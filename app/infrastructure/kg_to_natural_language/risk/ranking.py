"""三级风险指标知识关联度评分、排序与发布。"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
import uuid
from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import Any, BinaryIO, Callable, Coroutine, Iterator

import httpx

from .config import (
    DEFAULT_RISK_DATA_ROOT,
    DEFAULT_RISK_TEMP_ROOT,
    DEFAULT_RISK_TREE_PATH,
    RiskRelevanceConfigError,
    RiskRelevanceModelConfig,
)
from .indicator_tree import list_indicator_numbers, validate_indicator_number
from .generation import generate_risk_indicator_knowledge


logger = logging.getLogger(__name__)

_ATOMIC_REPLACE_MAX_ATTEMPTS = 8
_ATOMIC_REPLACE_INITIAL_DELAY_SECONDS = 0.05


class RiskKnowledgeRankingError(RuntimeError):
    """阶段二知识关联度评分失败。"""


class RiskKnowledgeBatchError(RiskKnowledgeRankingError):
    """全量三级指标知识生成、评分与排序失败。"""


class StageOneArtifactError(RiskKnowledgeRankingError):
    """阶段一 all.json 缺失或结构不合法。"""


class ModelResponseError(ValueError):
    """模型返回内容不符合关联度评分协议。"""


class _RetryableScoreError(RuntimeError):
    pass


class _NonRetryableScoreError(RuntimeError):
    pass


class _ScoreAttemptsExhausted(RiskKnowledgeRankingError):
    def __init__(self, message: str, attempts: int) -> None:
        super().__init__(message)
        self.attempts = attempts


_CODE_FENCE_PATTERN = re.compile(
    r"^\s*```(?:json)?\s*(.*?)\s*```\s*$",
    re.IGNORECASE | re.DOTALL,
)
_CHINESE_CHARACTER_PATTERN = re.compile(r"[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]")


def _now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("w", encoding="utf-8", newline="\n") as file:
            file.write(content)
        for attempt in range(_ATOMIC_REPLACE_MAX_ATTEMPTS):
            try:
                os.replace(temporary, path)
                break
            except PermissionError:
                if attempt + 1 >= _ATOMIC_REPLACE_MAX_ATTEMPTS:
                    raise
                delay = min(
                    _ATOMIC_REPLACE_INITIAL_DELAY_SECONDS * (2**attempt),
                    1.0,
                )
                logger.warning(
                    "文件暂时被占用，%ss后重试原子替换[%s/%s]：%s",
                    round(delay, 3),
                    attempt + 1,
                    _ATOMIC_REPLACE_MAX_ATTEMPTS,
                    path,
                )
                time.sleep(delay)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


@contextmanager
def _exclusive_process_lock(path: Path) -> Iterator[None]:
    """持有跨进程文件锁，避免多个全量任务同时写同一批成果。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_file: BinaryIO = path.open("a+b")
    locked = False
    try:
        lock_file.seek(0, os.SEEK_END)
        if lock_file.tell() == 0:
            lock_file.write(b"\0")
            lock_file.flush()
        lock_file.seek(0)
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            locked = True
        except OSError as exc:
            raise RiskKnowledgeBatchError(
                f"已有全量风险指标知识处理任务正在运行，不能重复启动：{path}"
            ) from exc
        yield
    finally:
        if locked:
            lock_file.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        lock_file.close()


def _atomic_write_json(path: Path, value: Any) -> None:
    content = json.dumps(value, ensure_ascii=False, indent=2) + "\n"
    _atomic_write_text(path, content)


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise StageOneArtifactError(
            f"阶段一成果不存在，请先生成指标知识：{path}"
        ) from exc
    except json.JSONDecodeError as exc:
        raise StageOneArtifactError(f"阶段一all.json无法解析：{exc}") from exc
    if not isinstance(value, dict):
        raise StageOneArtifactError("阶段一all.json根节点必须是JSON对象")
    return value


def _existing_final_indicator_output(
    sequence: str,
    *,
    data_dir: Path,
    temp_dir: Path,
) -> dict[str, Any] | None:
    """仅以最终 all.txt 是否存在判定指标是否已经生成。"""
    all_txt_path = data_dir / "all.txt"
    if not all_txt_path.is_file():
        return None

    try:
        with all_txt_path.open("r", encoding="utf-8") as file:
            knowledge_count = sum(1 for line in file if line.strip())
    except OSError:
        logger.warning("已有最终成果无法读取，将重新处理该指标：%s", all_txt_path)
        return None
    if knowledge_count == 0:
        logger.info("已有最终成果为空，将重新处理该指标：%s", all_txt_path)
        return None

    metadata: dict[str, Any] = {}
    all_json_path = temp_dir / "all.json"
    if all_json_path.is_file():
        try:
            value = json.loads(all_json_path.read_text(encoding="utf-8"))
            if isinstance(value, dict):
                indicator = value.get("indicator")
                ranking_summary = value.get("ranking_summary")
                if isinstance(indicator, dict):
                    metadata.update(
                        {
                            "indicator_name": str(indicator.get("name") or ""),
                            "risk_type": str(indicator.get("risk_type") or ""),
                        }
                    )
                if isinstance(ranking_summary, dict):
                    metadata.update(
                        {
                            "ranking_success_count": int(
                                ranking_summary.get("ranking_success_count") or 0
                            ),
                            "ranking_failure_count": int(
                                ranking_summary.get("ranking_failure_count") or 0
                            ),
                        }
                    )
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            logger.warning("已有临时成果无法读取元数据，但仍按最终TXT跳过：%s", all_json_path)

    return {
        "status": "skipped_existing",
        "indicator_number": sequence,
        "indicator_name": metadata.get("indicator_name", ""),
        "risk_type": metadata.get("risk_type", ""),
        "knowledge_count": knowledge_count,
        "ranking_success_count": metadata.get("ranking_success_count", 0),
        "ranking_failure_count": metadata.get("ranking_failure_count", 0),
        "all_json_path": str(all_json_path.resolve()) if all_json_path.is_file() else None,
        "all_txt_path": str(all_txt_path.resolve()),
    }


def _validate_stage_one_artifact(
    value: dict[str, Any],
    indicator_number: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    if value.get("stage") != "knowledge_generation" or value.get("status") != "success":
        raise StageOneArtifactError("阶段一all.json不是成功的knowledge_generation成果")

    indicator = value.get("indicator")
    if not isinstance(indicator, dict):
        raise StageOneArtifactError("阶段一all.json缺少indicator对象")
    if str(indicator.get("number") or "").strip() != indicator_number:
        raise StageOneArtifactError(
            f"阶段一成果指标序号与请求不一致：期望{indicator_number}"
        )
    description = str(indicator.get("description") or "").strip()
    if not description:
        raise StageOneArtifactError(f"三级指标{indicator_number}缺少正式指标描述")

    entries = value.get("knowledge_entries")
    if not isinstance(entries, list):
        raise StageOneArtifactError("阶段一all.json的knowledge_entries必须是数组")
    seen_ids: set[str] = set()
    for index, entry in enumerate(entries):
        if not isinstance(entry, dict):
            raise StageOneArtifactError(f"第{index + 1}条知识记录必须是JSON对象")
        knowledge_id = str(entry.get("knowledge_id") or "").strip()
        knowledge = str(entry.get("knowledge") or "").strip()
        if not knowledge_id:
            raise StageOneArtifactError(f"第{index + 1}条知识缺少knowledge_id")
        if knowledge_id in seen_ids:
            raise StageOneArtifactError(f"知识ID重复：{knowledge_id}")
        seen_ids.add(knowledge_id)
        if not knowledge:
            raise StageOneArtifactError(f"知识{knowledge_id}正文为空")
        original_order = entry.get("original_order")
        if isinstance(original_order, bool) or not isinstance(original_order, int):
            entry["original_order"] = index
        entry.setdefault("relevance_score", None)
        entry.setdefault("relevance_reason", None)
        entry.setdefault("ranking_status", "pending")
        entry.setdefault("ranking_error", None)
    return indicator, entries


def build_relevance_messages(
    indicator_description: str,
    knowledge: str,
) -> list[dict[str, str]]:
    """构造只含一个指标描述和一条知识的精简评分消息。"""
    system_prompt = (
        "你是企业合规风险知识关联度判定器。判断给定知识条目对三级指标描述的"
        "直接支撑程度。必须只依据知识条目明示内容，不得利用常识、文件背景或法规"
        "整体主题补充条目中没有的信息。评分口径：直接规定指标所问对象、行为、条件"
        "或义务为0.90至1.00；明确规定可直接用于指标判断的相近要求为0.70至0.89；"
        "只涉及相关领域、背景、适用范围或间接条件为0.30至0.69；仅有文件身份、"
        "制定依据、发布日期、一般性引用或宽泛主题为0.00至0.29；没有实质联系为"
        "0.00。若条目未明确涉及指标的核心对象或要求，不得仅凭可能存在联系给出"
        "0.70以上分数。reason必须指出条目中实际出现的依据，不得声称条目包含并未"
        "出现的内容。只输出一个JSON对象，不要输出Markdown、分析过程或其他文字。"
        "JSON必须包含relevance_score和reason；relevance_score必须是0.00到1.00之间"
        "的数值，reason必须是简短、非空的判断原因。不要改写知识条目。"
    )
    user_payload = {
        "indicator_description": str(indicator_description).strip(),
        "knowledge": str(knowledge).strip(),
    }
    return [
        {"role": "system", "content": system_prompt},
        {
            "role": "user",
            "content": json.dumps(user_payload, ensure_ascii=False),
        },
    ]


def parse_relevance_response(content: str) -> tuple[float, str]:
    """解析并严格校验模型返回的关联度JSON。"""
    text = str(content or "").strip()
    fenced = _CODE_FENCE_PATTERN.fullmatch(text)
    if fenced:
        text = fenced.group(1).strip()
    try:
        value = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ModelResponseError("模型响应不是有效JSON") from exc
    if not isinstance(value, dict):
        raise ModelResponseError("模型响应必须是JSON对象")
    if "relevance_score" not in value or "reason" not in value:
        raise ModelResponseError("模型响应缺少relevance_score或reason")

    score = value["relevance_score"]
    if isinstance(score, bool) or not isinstance(score, (int, float)):
        raise ModelResponseError("relevance_score必须是数值")
    numeric_score = float(score)
    if not 0.0 <= numeric_score <= 1.0:
        raise ModelResponseError("relevance_score必须位于0.00到1.00")
    reason = value["reason"]
    if not isinstance(reason, str) or not reason.strip():
        raise ModelResponseError("reason必须是非空文本")
    return round(numeric_score, 2), reason.strip()


def _extract_message_content(response: httpx.Response) -> str:
    try:
        payload = response.json()
        choices = payload["choices"]
        content = choices[0]["message"]["content"]
    except (ValueError, KeyError, IndexError, TypeError) as exc:
        raise ModelResponseError("模型HTTP响应缺少choices[0].message.content") from exc
    if not isinstance(content, str) or not content.strip():
        raise ModelResponseError("模型返回内容为空")
    return content


def _sanitize_error(exc: BaseException, api_key: str) -> str:
    message = re.sub(r"\s+", " ", str(exc or exc.__class__.__name__)).strip()
    if api_key:
        message = message.replace(api_key, "[REDACTED]")
    message = re.sub(
        r"(?i)(authorization\s*[:=]\s*bearer\s+)[^\s,;]+",
        r"\1[REDACTED]",
        message,
    )
    return (message or exc.__class__.__name__)[:500]


def _is_valid_success(entry: dict[str, Any]) -> bool:
    if entry.get("ranking_status") != "success":
        return False
    score = entry.get("relevance_score")
    reason = entry.get("relevance_reason")
    return (
        not isinstance(score, bool)
        and isinstance(score, (int, float))
        and 0.0 <= float(score) <= 1.0
        and isinstance(reason, str)
        and bool(reason.strip())
    )


class RiskRelevanceScorer:
    """使用共享HTTP客户端对单条知识执行受控评分。"""

    def __init__(
        self,
        config: RiskRelevanceModelConfig,
        client: httpx.AsyncClient,
        *,
        sleep: Callable[[float], Coroutine[Any, Any, None]] = asyncio.sleep,
    ) -> None:
        self.config = config
        self.client = client
        self.semaphore = asyncio.Semaphore(config.max_concurrency)
        self._sleep = sleep

    async def score_entry(
        self,
        index: int,
        indicator_description: str,
        entry: dict[str, Any],
    ) -> tuple[int, dict[str, Any]]:
        """评分一条知识，所有异常均转换为可回写结果。"""
        attempts = 0
        try:
            async with self.semaphore:
                score, reason, attempts = await asyncio.wait_for(
                    self._score_with_retries(indicator_description, entry["knowledge"]),
                    timeout=self.config.total_timeout_seconds,
                )
            return index, {
                "status": "success",
                "score": score,
                "reason": reason,
                "error": None,
                "attempts": attempts,
            }
        except asyncio.TimeoutError as exc:
            error = _sanitize_error(
                RiskKnowledgeRankingError("单条知识评分超过总时限"),
                self.config.api_key,
            )
        except Exception as exc:
            attempts = int(getattr(exc, "attempts", attempts) or 0)
            error = _sanitize_error(exc, self.config.api_key)
        return index, {
            "status": "failed",
            "score": None,
            "reason": None,
            "error": error,
            "attempts": attempts,
        }

    async def _score_with_retries(
        self,
        indicator_description: str,
        knowledge: str,
    ) -> tuple[float, str, int]:
        attempts = 0
        while True:
            attempts += 1
            try:
                score, reason = await self._request_score(
                    indicator_description,
                    knowledge,
                )
                return score, reason, attempts
            except _NonRetryableScoreError as exc:
                raise _ScoreAttemptsExhausted(str(exc), attempts) from exc
            except (httpx.RequestError, _RetryableScoreError, ModelResponseError) as exc:
                if attempts > self.config.max_retries:
                    raise _ScoreAttemptsExhausted(
                        f"模型评分在{attempts}次尝试后失败：{exc}",
                        attempts,
                    ) from exc
                await self._sleep(min(0.5 * (2 ** (attempts - 1)), 4.0))

    async def _request_score(
        self,
        indicator_description: str,
        knowledge: str,
    ) -> tuple[float, str]:
        payload = {
            "model": self.config.model_name,
            "messages": build_relevance_messages(indicator_description, knowledge),
            "temperature": 0,
            "max_tokens": 256,
        }
        try:
            response = await self.client.post(
                self.config.api_url,
                headers={
                    "Authorization": f"Bearer {self.config.api_key}",
                    "Content-Type": "application/json",
                },
                json=payload,
            )
        except httpx.RequestError:
            raise
        if response.status_code == 429 or response.status_code >= 500:
            raise _RetryableScoreError(f"模型服务返回HTTP {response.status_code}")
        if response.status_code >= 400:
            raise _NonRetryableScoreError(f"模型服务返回HTTP {response.status_code}")
        return parse_relevance_response(_extract_message_content(response))


def _create_async_client(config: RiskRelevanceModelConfig) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=httpx.Timeout(config.total_timeout_seconds),
        limits=httpx.Limits(
            max_connections=config.max_concurrency,
            max_keepalive_connections=config.max_concurrency,
        ),
    )


def _knowledge_language_priority(entry: dict[str, Any]) -> int:
    """中文知识返回0，其他语言返回1，供同分排序使用。"""
    knowledge = str(entry.get("knowledge") or "")
    return 0 if _CHINESE_CHARACTER_PATTERN.search(knowledge) else 1


def _entry_sort_key(entry: dict[str, Any]) -> tuple[Any, ...]:
    original_order = entry.get("original_order", 0)
    knowledge_id = str(entry.get("knowledge_id") or "")
    language_priority = _knowledge_language_priority(entry)
    if _is_valid_success(entry):
        return (
            0,
            -float(entry["relevance_score"]),
            language_priority,
            original_order,
            knowledge_id,
        )
    return (1, 0.0, language_priority, original_order, knowledge_id)


def _build_ranking_summary(
    *,
    config: RiskRelevanceModelConfig,
    entries: list[dict[str, Any]],
    requested_count: int,
    skipped_success_count: int,
    run_success_count: int,
    run_failure_count: int,
    retry_count: int,
    force: bool,
    started_at: str,
    elapsed_seconds: float,
    status: str,
) -> dict[str, Any]:
    success_count = sum(_is_valid_success(entry) for entry in entries)
    failure_count = len(entries) - success_count
    return {
        "status": status,
        "model_name": config.model_name,
        "api_url": config.sanitized_api_url,
        "max_concurrency": config.max_concurrency,
        "total_timeout_seconds": config.total_timeout_seconds,
        "max_retries": config.max_retries,
        "force": force,
        "knowledge_count": len(entries),
        "requested_count": requested_count,
        "skipped_success_count": skipped_success_count,
        "run_success_count": run_success_count,
        "run_failure_count": run_failure_count,
        "ranking_success_count": success_count,
        "ranking_failure_count": failure_count,
        "retry_count": retry_count,
        "started_at": started_at,
        "completed_at": _now_iso() if status != "running" else None,
        "elapsed_seconds": round(elapsed_seconds, 3),
    }


async def rank_risk_indicator_knowledge(
    indicator_number: str,
    *,
    input_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
    max_concurrency: int | None = None,
    force: bool = False,
    model_config: RiskRelevanceModelConfig | None = None,
) -> dict[str, Any]:
    """读取阶段一成果，评分、排序并发布一个指标的知识TXT。"""
    started_perf = time.perf_counter()
    started_at = _now_iso()
    sequence = validate_indicator_number(indicator_number)
    source_dir = (
        Path(input_dir)
        if input_dir is not None
        else DEFAULT_RISK_TEMP_ROOT / sequence
    )
    publish_dir = (
        Path(output_dir)
        if output_dir is not None
        else DEFAULT_RISK_DATA_ROOT / sequence
    )
    all_json_path = source_dir / "all.json"
    all_json = _load_json_object(all_json_path)
    indicator, entries = _validate_stage_one_artifact(all_json, sequence)

    try:
        if model_config is None:
            config = RiskRelevanceModelConfig.from_env(
                max_concurrency=max_concurrency,
            )
        else:
            config = (
                replace(model_config, max_concurrency=max_concurrency)
                if max_concurrency is not None
                else model_config
            )
            config.validate()
    except RiskRelevanceConfigError as exc:
        raise RiskKnowledgeRankingError(str(exc)) from exc

    if force:
        for entry in entries:
            entry.update(
                {
                    "relevance_score": None,
                    "relevance_reason": None,
                    "ranking_status": "pending",
                    "ranking_error": None,
                }
            )

    pending_indexes = [
        index
        for index, entry in enumerate(entries)
        if force or not _is_valid_success(entry)
    ]
    skipped_success_count = len(entries) - len(pending_indexes)
    logger.info(
        "风险指标阶段二开始：指标=%s，模型=%s，接口=%s，并发=%s，时限=%ss，"
        "知识=%s，待评分=%s，跳过=%s",
        sequence,
        config.model_name,
        config.sanitized_api_url,
        config.max_concurrency,
        config.total_timeout_seconds,
        len(entries),
        len(pending_indexes),
        skipped_success_count,
    )

    run_success_count = 0
    run_failure_count = 0
    retry_count = 0
    completed_count = 0
    all_json["ranking_summary"] = _build_ranking_summary(
        config=config,
        entries=entries,
        requested_count=len(pending_indexes),
        skipped_success_count=skipped_success_count,
        run_success_count=0,
        run_failure_count=0,
        retry_count=0,
        force=force,
        started_at=started_at,
        elapsed_seconds=0.0,
        status="running",
    )
    _atomic_write_json(all_json_path, all_json)

    if pending_indexes:
        async with _create_async_client(config) as client:
            scorer = RiskRelevanceScorer(config, client)
            tasks = [
                asyncio.create_task(
                    scorer.score_entry(
                        index,
                        str(indicator["description"]),
                        entries[index],
                    )
                )
                for index in pending_indexes
            ]
            for future in asyncio.as_completed(tasks):
                index, result = await future
                entry = entries[index]
                attempts = int(result["attempts"] or 0)
                retry_count += max(attempts - 1, 0)
                entry["ranking_model"] = config.model_name
                entry["ranking_attempts"] = attempts
                entry["ranked_at"] = _now_iso()
                if result["status"] == "success":
                    run_success_count += 1
                    entry.update(
                        {
                            "relevance_score": result["score"],
                            "relevance_reason": result["reason"],
                            "ranking_status": "success",
                            "ranking_error": None,
                        }
                    )
                else:
                    run_failure_count += 1
                    entry.update(
                        {
                            "relevance_score": None,
                            "relevance_reason": None,
                            "ranking_status": "failed",
                            "ranking_error": result["error"],
                        }
                    )
                completed_count += 1
                if completed_count % config.checkpoint_interval == 0:
                    all_json["ranking_summary"] = _build_ranking_summary(
                        config=config,
                        entries=entries,
                        requested_count=len(pending_indexes),
                        skipped_success_count=skipped_success_count,
                        run_success_count=run_success_count,
                        run_failure_count=run_failure_count,
                        retry_count=retry_count,
                        force=force,
                        started_at=started_at,
                        elapsed_seconds=time.perf_counter() - started_perf,
                        status="running",
                    )
                    _atomic_write_json(all_json_path, all_json)

    entries.sort(key=_entry_sort_key)
    final_status = "success" if all(_is_valid_success(entry) for entry in entries) else "partial_success"
    elapsed = time.perf_counter() - started_perf
    summary = _build_ranking_summary(
        config=config,
        entries=entries,
        requested_count=len(pending_indexes),
        skipped_success_count=skipped_success_count,
        run_success_count=run_success_count,
        run_failure_count=run_failure_count,
        retry_count=retry_count,
        force=force,
        started_at=started_at,
        elapsed_seconds=elapsed,
        status=final_status,
    )
    all_json["knowledge_entries"] = entries
    all_json["ranking_summary"] = summary
    all_json["ranked_at"] = summary["completed_at"]
    _atomic_write_json(all_json_path, all_json)

    knowledge_lines = [str(entry["knowledge"]).strip() for entry in entries if str(entry["knowledge"]).strip()]
    txt_path = publish_dir / "all.txt"
    _atomic_write_text(txt_path, "\n".join(knowledge_lines) + ("\n" if knowledge_lines else ""))

    response = {
        "status": final_status,
        "indicator_number": sequence,
        "indicator_name": str(indicator.get("name") or ""),
        "indicator_description": str(indicator.get("description") or ""),
        "risk_type": str(indicator.get("risk_type") or ""),
        **dict(all_json.get("file_summary") or {}),
        "knowledge_count": len(entries),
        "ranking_requested_count": len(pending_indexes),
        "ranking_skipped_success_count": skipped_success_count,
        "ranking_success_count": summary["ranking_success_count"],
        "ranking_failure_count": summary["ranking_failure_count"],
        "retry_count": retry_count,
        "all_json_path": str(all_json_path.resolve()),
        "all_txt_path": str(txt_path.resolve()),
        "elapsed_seconds": summary["elapsed_seconds"],
    }
    logger.info(
        "风险指标阶段二完成：指标=%s，状态=%s，知识=%s，成功=%s，失败=%s，"
        "重试=%s，耗时=%ss，TXT=%s",
        sequence,
        final_status,
        len(entries),
        response["ranking_success_count"],
        response["ranking_failure_count"],
        retry_count,
        response["elapsed_seconds"],
        response["all_txt_path"],
    )
    return response


async def process_risk_indicator_knowledge(
    indicator_number: str,
    *,
    risk_tree_path: str | Path | None = None,
    temp_output_dir: str | Path | None = None,
    data_output_dir: str | Path | None = None,
    max_concurrency: int | None = None,
    force: bool = False,
    model_config: RiskRelevanceModelConfig | None = None,
) -> dict[str, Any]:
    """依次执行阶段一知识生成和阶段二关联度排序。"""
    started = time.perf_counter()
    sequence = validate_indicator_number(indicator_number)
    stage_one = await asyncio.to_thread(
        generate_risk_indicator_knowledge,
        sequence,
        risk_tree_path=risk_tree_path,
        output_dir=temp_output_dir,
    )
    stage_two = await rank_risk_indicator_knowledge(
        sequence,
        input_dir=stage_one["output_directory"],
        output_dir=data_output_dir,
        max_concurrency=max_concurrency,
        force=force,
        model_config=model_config,
    )
    return {
        "status": stage_two["status"],
        "indicator_number": sequence,
        "indicator_name": stage_two["indicator_name"],
        "indicator_description": stage_two["indicator_description"],
        "risk_type": stage_two["risk_type"],
        "unique_found_file_count": stage_one["unique_found_file_count"],
        "matched_source_file_count": stage_one["matched_source_file_count"],
        "unmatched_file_count": stage_one["unmatched_file_count"],
        "knowledge_count": stage_two["knowledge_count"],
        "ranking_success_count": stage_two["ranking_success_count"],
        "ranking_failure_count": stage_two["ranking_failure_count"],
        "all_json_path": stage_two["all_json_path"],
        "all_txt_path": stage_two["all_txt_path"],
        "stage_one": stage_one,
        "stage_two": stage_two,
        "elapsed_seconds": round(time.perf_counter() - started, 3),
    }


async def _process_all_risk_indicator_knowledge_unlocked(
    *,
    risk_tree_path: str | Path | None = None,
    temp_output_root: str | Path | None = None,
    data_output_root: str | Path | None = None,
    max_concurrency: int | None = None,
    force: bool = False,
    continue_on_error: bool = True,
    model_config: RiskRelevanceModelConfig | None = None,
) -> dict[str, Any]:
    """为指标树中的全部三级指标生成、评分并排序映射文件知识条目。

    指标之间顺序执行，单个指标内部沿用知识条目并发评分机制。默认隔离
    单指标异常并继续执行，其间持续把处理进度写入正式输出根目录。
    """
    started_perf = time.perf_counter()
    started_at = _now_iso()
    tree_path = Path(risk_tree_path) if risk_tree_path is not None else None
    sequences = list_indicator_numbers(tree_path=tree_path)
    if not sequences:
        raise RiskKnowledgeBatchError("风险指标树中没有可处理的三级指标")

    temp_root = (
        Path(temp_output_root)
        if temp_output_root is not None
        else DEFAULT_RISK_TEMP_ROOT
    )
    data_root = (
        Path(data_output_root)
        if data_output_root is not None
        else DEFAULT_RISK_DATA_ROOT
    )
    summary_path = data_root / "处理汇总.json"

    try:
        if model_config is None:
            effective_model_config = RiskRelevanceModelConfig.from_env(
                max_concurrency=max_concurrency,
            )
        else:
            effective_model_config = (
                replace(model_config, max_concurrency=max_concurrency)
                if max_concurrency is not None
                else model_config
            )
            effective_model_config.validate()
    except RiskRelevanceConfigError as exc:
        raise RiskKnowledgeBatchError(str(exc)) from exc

    results: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    skipped_existing: list[dict[str, Any]] = []

    def build_summary(status: str) -> dict[str, Any]:
        success_count = sum(item["status"] == "success" for item in results)
        partial_count = sum(item["status"] != "success" for item in results)
        processed_count = len(results) + len(failures)
        completed_count = processed_count + len(skipped_existing)
        new_knowledge_count = sum(item["knowledge_count"] for item in results)
        existing_knowledge_count = sum(
            item["knowledge_count"] for item in skipped_existing
        )
        return {
            "status": status,
            "risk_tree_path": str(
                (tree_path or DEFAULT_RISK_TREE_PATH).resolve()
            ),
            "model_name": effective_model_config.model_name,
            "indicator_count": len(sequences),
            "completed_indicator_count": completed_count,
            "pending_indicator_count": len(sequences) - completed_count,
            "processed_indicator_count": processed_count,
            "skipped_existing_indicator_count": len(skipped_existing),
            "success_indicator_count": success_count,
            "partial_success_indicator_count": partial_count,
            "failure_indicator_count": len(failures),
            "knowledge_count": new_knowledge_count + existing_knowledge_count,
            "new_knowledge_count": new_knowledge_count,
            "existing_knowledge_count": existing_knowledge_count,
            "ranking_success_count": sum(
                item["ranking_success_count"] for item in results
            ) + sum(item["ranking_success_count"] for item in skipped_existing),
            "ranking_failure_count": sum(
                item["ranking_failure_count"] for item in results
            ) + sum(item["ranking_failure_count"] for item in skipped_existing),
            "started_at": started_at,
            "completed_at": _now_iso() if status != "running" else None,
            "elapsed_seconds": round(time.perf_counter() - started_perf, 3),
            "temp_output_root": str(temp_root.resolve()),
            "data_output_root": str(data_root.resolve()),
            "summary_path": str(summary_path.resolve()),
            "results": results,
            "skipped_existing": skipped_existing,
            "failures": failures,
        }

    logger.info(
        "全量风险指标知识处理开始：指标数=%s，模型=%s，单指标知识并发=%s",
        len(sequences),
        effective_model_config.model_name,
        effective_model_config.max_concurrency,
    )
    _atomic_write_json(summary_path, build_summary("running"))

    for position, sequence in enumerate(sequences, start=1):
        if not force:
            existing = _existing_final_indicator_output(
                sequence,
                data_dir=data_root / sequence,
                temp_dir=temp_root / sequence,
            )
            if existing is not None:
                skipped_existing.append(existing)
                logger.info(
                    "跳过三级指标[%s/%s]：%s，最终all.txt已存在，知识=%s",
                    position,
                    len(sequences),
                    sequence,
                    existing["knowledge_count"],
                )
                _atomic_write_json(summary_path, build_summary("running"))
                continue
        logger.info("处理三级指标[%s/%s]：%s", position, len(sequences), sequence)
        try:
            result = await process_risk_indicator_knowledge(
                sequence,
                risk_tree_path=tree_path,
                temp_output_dir=temp_root / sequence,
                data_output_dir=data_root / sequence,
                force=force,
                model_config=effective_model_config,
            )
            results.append(
                {
                    "status": result["status"],
                    "indicator_number": result["indicator_number"],
                    "indicator_name": result["indicator_name"],
                    "risk_type": result["risk_type"],
                    "unique_found_file_count": result["unique_found_file_count"],
                    "matched_source_file_count": result["matched_source_file_count"],
                    "unmatched_file_count": result["unmatched_file_count"],
                    "knowledge_count": result["knowledge_count"],
                    "ranking_success_count": result["ranking_success_count"],
                    "ranking_failure_count": result["ranking_failure_count"],
                    "all_json_path": result["all_json_path"],
                    "all_txt_path": result["all_txt_path"],
                    "elapsed_seconds": result["elapsed_seconds"],
                }
            )
        except Exception as exc:
            failure = {
                "indicator_number": sequence,
                "error_type": type(exc).__name__,
                "error": str(exc),
            }
            failures.append(failure)
            logger.exception("三级指标%s处理失败", sequence)
            if not continue_on_error:
                summary = build_summary("failed")
                _atomic_write_json(summary_path, summary)
                raise RiskKnowledgeBatchError(
                    f"三级指标{sequence}处理失败：{exc}"
                ) from exc
        _atomic_write_json(summary_path, build_summary("running"))

    if failures or any(item["status"] != "success" for item in results):
        final_status = "failed" if len(failures) == len(sequences) else "partial_success"
    else:
        final_status = "success"
    summary = build_summary(final_status)
    _atomic_write_json(summary_path, summary)
    logger.info(
        "全量风险指标知识处理完成：状态=%s，指标=%s，成功=%s，部分成功=%s，"
        "失败=%s，跳过已有=%s，知识=%s，耗时=%ss",
        final_status,
        summary["indicator_count"],
        summary["success_indicator_count"],
        summary["partial_success_indicator_count"],
        summary["failure_indicator_count"],
        summary["skipped_existing_indicator_count"],
        summary["knowledge_count"],
        summary["elapsed_seconds"],
    )
    return summary


async def process_all_risk_indicator_knowledge(
    *,
    risk_tree_path: str | Path | None = None,
    temp_output_root: str | Path | None = None,
    data_output_root: str | Path | None = None,
    max_concurrency: int | None = None,
    force: bool = False,
    continue_on_error: bool = True,
    model_config: RiskRelevanceModelConfig | None = None,
) -> dict[str, Any]:
    """为全部三级指标生成、评分并排序知识，同一输出目录只允许运行一个任务。"""
    temp_root = (
        Path(temp_output_root)
        if temp_output_root is not None
        else DEFAULT_RISK_TEMP_ROOT
    )
    with _exclusive_process_lock(temp_root / ".process_all.lock"):
        return await _process_all_risk_indicator_knowledge_unlocked(
            risk_tree_path=risk_tree_path,
            temp_output_root=temp_root,
            data_output_root=data_output_root,
            max_concurrency=max_concurrency,
            force=force,
            continue_on_error=continue_on_error,
            model_config=model_config,
        )


__all__ = [
    "ModelResponseError",
    "RiskKnowledgeBatchError",
    "RiskKnowledgeRankingError",
    "RiskRelevanceScorer",
    "StageOneArtifactError",
    "build_relevance_messages",
    "parse_relevance_response",
    "process_risk_indicator_knowledge",
    "process_all_risk_indicator_knowledge",
    "rank_risk_indicator_knowledge",
]
