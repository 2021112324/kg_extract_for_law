from __future__ import annotations

import asyncio
import functools
import json
from pathlib import Path
from typing import Any

import httpx
import pytest

from app.infrastructure import kg_to_natural_language as package
from app.infrastructure.kg_to_natural_language.risk_indicator_config import (
    RISK_RELEVANCE_MODEL_API_KEY,
    RiskRelevanceModelConfig,
)
from app.infrastructure.kg_to_natural_language import risk_knowledge_ranking as ranking


def _async_test(function):
    @functools.wraps(function)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        return asyncio.run(function(*args, **kwargs))

    return wrapper


def _config(**overrides: Any) -> RiskRelevanceModelConfig:
    values = {
        "model_name": "test-model",
        "api_url": "https://user:password@example.test/v1/chat/completions?token=x",
        "api_key": "secret-test-key",
        "max_concurrency": 100,
        "total_timeout_seconds": 1.0,
        "max_retries": 1,
        "checkpoint_interval": 2,
    }
    values.update(overrides)
    config = RiskRelevanceModelConfig(**values)
    config.validate()
    return config


def _response(score: float, reason: str = "相关") -> httpx.Response:
    content = json.dumps(
        {"relevance_score": score, "reason": reason},
        ensure_ascii=False,
    )
    return httpx.Response(
        200,
        json={"choices": [{"message": {"content": content}}]},
    )


def _write_all(path: Path) -> None:
    entries = [
        {
            "knowledge_id": f"risk-{index}",
            "knowledge": knowledge,
            "source_file": "测试法规.txt",
            "graph_category": "法律法规条款",
            "relevance_score": None,
            "relevance_reason": None,
            "ranking_status": "pending",
            "ranking_error": None,
            "original_order": index,
        }
        for index, knowledge in enumerate(["高相关一", "低相关", "高相关二", "请求失败"])
    ]
    value = {
        "schema_version": "1.0",
        "stage": "knowledge_generation",
        "status": "success",
        "indicator": {
            "number": "1.1.1.1",
            "name": "测试指标",
            "description": "企业是否履行测试合规义务",
            "risk_type": "企业关联方合规风险",
        },
        "file_summary": {
            "unique_found_file_count": 1,
            "matched_source_file_count": 1,
            "unmatched_file_count": 0,
        },
        "knowledge_count": len(entries),
        "knowledge_entries": entries,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")


def test_config_reads_key_at_call_time_and_sanitizes_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("RISK_RELEVANCE_MODEL_API_KEY", "runtime-secret")
    monkeypatch.setenv("RISK_RELEVANCE_MODEL_API_URL", "https://u:p@example.test/a?key=x")
    config = RiskRelevanceModelConfig.from_env(max_concurrency=7)
    assert config.api_key == "runtime-secret"
    assert config.max_concurrency == 7
    assert config.sanitized_api_url == "https://example.test/a"

    monkeypatch.delenv("RISK_RELEVANCE_MODEL_API_KEY")
    default_config = RiskRelevanceModelConfig.from_env()
    assert default_config.api_key == RISK_RELEVANCE_MODEL_API_KEY


def test_prompt_contains_exactly_one_indicator_and_one_knowledge() -> None:
    messages = ranking.build_relevance_messages("指标描述A", "知识条目B")
    assert len(messages) == 2
    payload = json.loads(messages[1]["content"])
    assert payload == {
        "indicator_description": "指标描述A",
        "knowledge": "知识条目B",
    }
    assert "relevance_score" in messages[0]["content"]
    assert "reason" in messages[0]["content"]


@pytest.mark.parametrize(
    "content,expected",
    [
        ('{"relevance_score": 0.856, "reason": "直接相关"}', (0.86, "直接相关")),
        (
            '```json\n{"relevance_score": 0, "reason": "无关"}\n```',
            (0.0, "无关"),
        ),
    ],
)
def test_parse_relevance_response(content: str, expected: tuple[float, str]) -> None:
    assert ranking.parse_relevance_response(content) == expected


@pytest.mark.parametrize(
    "content",
    [
        "not-json",
        "[]",
        '{"relevance_score": true, "reason": "错误"}',
        '{"relevance_score": "0.8", "reason": "错误"}',
        '{"relevance_score": 1.1, "reason": "错误"}',
        '{"relevance_score": 0.8, "reason": ""}',
        '{"relevance_score": 0.8}',
    ],
)
def test_parse_relevance_response_rejects_invalid_data(content: str) -> None:
    with pytest.raises(ranking.ModelResponseError):
        ranking.parse_relevance_response(content)


def test_atomic_write_retries_transient_permission_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "all.json"
    real_replace = ranking.os.replace
    attempts = 0

    def flaky_replace(source: str | Path, destination: str | Path) -> None:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise PermissionError(5, "文件暂时被占用")
        real_replace(source, destination)

    monkeypatch.setattr(ranking.os, "replace", flaky_replace)
    monkeypatch.setattr(ranking.time, "sleep", lambda _: None)

    ranking._atomic_write_text(target, "updated")

    assert attempts == 3
    assert target.read_text(encoding="utf-8") == "updated"


def test_sort_uses_chinese_priority_only_after_relevance_score() -> None:
    entries = [
        {
            "knowledge_id": "english-high",
            "knowledge": "A directly relevant English requirement.",
            "relevance_score": 0.95,
            "relevance_reason": "direct",
            "ranking_status": "success",
            "original_order": 0,
        },
        {
            "knowledge_id": "english-tie",
            "knowledge": "An English requirement with the same score.",
            "relevance_score": 0.8,
            "relevance_reason": "related",
            "ranking_status": "success",
            "original_order": 1,
        },
        {
            "knowledge_id": "chinese-tie",
            "knowledge": "同分的中文知识条目。",
            "relevance_score": 0.8,
            "relevance_reason": "相关",
            "ranking_status": "success",
            "original_order": 2,
        },
        {
            "knowledge_id": "chinese-low",
            "knowledge": "分数更低的中文知识条目。",
            "relevance_score": 0.7,
            "relevance_reason": "部分相关",
            "ranking_status": "success",
            "original_order": 3,
        },
    ]

    entries.sort(key=ranking._entry_sort_key)

    assert [entry["knowledge_id"] for entry in entries] == [
        "english-high",
        "chinese-tie",
        "english-tie",
        "chinese-low",
    ]


def test_failed_entries_also_prefer_chinese_before_original_order() -> None:
    entries = [
        {
            "knowledge_id": "english-failed",
            "knowledge": "Failed English knowledge.",
            "relevance_score": None,
            "relevance_reason": None,
            "ranking_status": "failed",
            "original_order": 0,
        },
        {
            "knowledge_id": "chinese-failed",
            "knowledge": "评分失败的中文知识。",
            "relevance_score": None,
            "relevance_reason": None,
            "ranking_status": "failed",
            "original_order": 1,
        },
    ]

    entries.sort(key=ranking._entry_sort_key)

    assert [entry["knowledge_id"] for entry in entries] == [
        "chinese-failed",
        "english-failed",
    ]


@_async_test
async def test_scorer_limits_in_flight_requests_to_100() -> None:
    active = 0
    maximum = 0
    seen_knowledge: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal active, maximum
        body = json.loads(request.content)
        user_payload = json.loads(body["messages"][1]["content"])
        seen_knowledge.append(user_payload["knowledge"])
        assert set(user_payload) == {"indicator_description", "knowledge"}
        active += 1
        maximum = max(maximum, active)
        await asyncio.sleep(0.005)
        active -= 1
        return _response(0.5)

    config = _config(max_concurrency=100, max_retries=0)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        scorer = ranking.RiskRelevanceScorer(config, client)
        results = await asyncio.gather(
            *[
                scorer.score_entry(
                    index,
                    "指标描述",
                    {"knowledge": f"知识{index}"},
                )
                for index in range(250)
            ]
        )

    assert len(results) == 250
    assert maximum == 100
    assert len(set(seen_knowledge)) == 250
    assert all(result[1]["status"] == "success" for result in results)


@_async_test
async def test_scorer_retries_invalid_json_and_5xx_then_succeeds() -> None:
    calls = 0

    async def handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(500)
        if calls == 2:
            return httpx.Response(
                200,
                json={"choices": [{"message": {"content": "bad-json"}}]},
            )
        return _response(0.75, "高度相关")

    async def no_sleep(_: float) -> None:
        return None

    config = _config(max_retries=2)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        scorer = ranking.RiskRelevanceScorer(config, client, sleep=no_sleep)
        _, result = await scorer.score_entry(0, "指标", {"knowledge": "知识"})

    assert calls == 3
    assert result == {
        "status": "success",
        "score": 0.75,
        "reason": "高度相关",
        "error": None,
        "attempts": 3,
    }


@_async_test
async def test_scorer_timeout_and_failure_do_not_fabricate_score() -> None:
    async def handler(_: httpx.Request) -> httpx.Response:
        await asyncio.sleep(0.2)
        return _response(1.0)

    config = _config(total_timeout_seconds=0.02, max_retries=0)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        scorer = ranking.RiskRelevanceScorer(config, client)
        _, result = await scorer.score_entry(0, "指标", {"knowledge": "知识"})

    assert result["status"] == "failed"
    assert result["score"] is None
    assert result["reason"] is None
    assert "总时限" in result["error"]


@_async_test
async def test_rank_function_sorts_publishes_and_resumes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    input_dir = tmp_path / "temp" / "1.1.1.1"
    output_dir = tmp_path / "data" / "1.1.1.1"
    _write_all(input_dir / "all.json")
    calls: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        knowledge = json.loads(payload["messages"][1]["content"])["knowledge"]
        calls.append(knowledge)
        if knowledge == "请求失败":
            return httpx.Response(500)
        score = {"高相关一": 0.92, "低相关": 0.5, "高相关二": 0.92}[knowledge]
        return _response(score, f"{knowledge}的原因")

    transport = httpx.MockTransport(handler)
    monkeypatch.setattr(
        ranking,
        "_create_async_client",
        lambda config: httpx.AsyncClient(transport=transport),
    )
    config = _config(max_retries=0, checkpoint_interval=1)
    result = await ranking.rank_risk_indicator_knowledge(
        "1.1.1.1",
        input_dir=input_dir,
        output_dir=output_dir,
        model_config=config,
    )

    assert result["status"] == "partial_success"
    assert result["ranking_success_count"] == 3
    assert result["ranking_failure_count"] == 1
    all_json = json.loads((input_dir / "all.json").read_text(encoding="utf-8"))
    assert [item["knowledge"] for item in all_json["knowledge_entries"]] == [
        "高相关一",
        "高相关二",
        "低相关",
        "请求失败",
    ]
    assert all_json["knowledge_entries"][-1]["relevance_score"] is None
    assert "secret-test-key" not in json.dumps(all_json, ensure_ascii=False)
    assert (output_dir / "all.txt").read_text(encoding="utf-8").splitlines() == [
        "高相关一",
        "高相关二",
        "低相关",
        "请求失败",
    ]

    calls.clear()
    rerun = await ranking.rank_risk_indicator_knowledge(
        "1.1.1.1",
        input_dir=input_dir,
        output_dir=output_dir,
        model_config=config,
    )
    assert calls == ["请求失败"]
    assert rerun["ranking_skipped_success_count"] == 3

    calls.clear()
    await ranking.rank_risk_indicator_knowledge(
        "1.1.1.1",
        input_dir=input_dir,
        output_dir=output_dir,
        model_config=config,
        force=True,
    )
    assert sorted(calls) == sorted(["高相关一", "高相关二", "低相关", "请求失败"])


@_async_test
async def test_rank_rejects_invalid_stage_before_creating_client(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    input_dir = tmp_path / "temp" / "1.1.1.1"
    input_dir.mkdir(parents=True)
    (input_dir / "all.json").write_text(
        json.dumps(
            {
                "stage": "knowledge_generation",
                "status": "failed",
                "indicator": {"number": "1.1.1.1"},
                "knowledge_entries": [],
            }
        ),
        encoding="utf-8",
    )
    called = False

    def fail_if_called(_: object) -> httpx.AsyncClient:
        nonlocal called
        called = True
        raise AssertionError("不应创建模型客户端")

    monkeypatch.setattr(ranking, "_create_async_client", fail_if_called)
    with pytest.raises(ranking.StageOneArtifactError):
        await ranking.rank_risk_indicator_knowledge(
            "1.1.1.1",
            input_dir=input_dir,
            output_dir=tmp_path / "data",
            model_config=_config(),
        )
    assert called is False


def test_package_exports_stage_two_and_end_to_end_functions() -> None:
    assert package.rank_risk_indicator_knowledge is ranking.rank_risk_indicator_knowledge
    assert package.process_risk_indicator_knowledge is ranking.process_risk_indicator_knowledge
    assert (
        package.process_all_risk_indicator_knowledge
        is ranking.process_all_risk_indicator_knowledge
    )


@_async_test
async def test_end_to_end_function_returns_complete_statistics(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stage_one_dir = tmp_path / "temp" / "1.1.1.1"
    calls: dict[str, Any] = {}

    def fake_generate(indicator_number: str, **kwargs: Any) -> dict[str, Any]:
        calls["generate"] = (indicator_number, kwargs)
        return {
            "status": "success",
            "indicator_number": indicator_number,
            "indicator_name": "测试指标",
            "indicator_description": "测试描述",
            "risk_type": "企业关联方合规风险",
            "unique_found_file_count": 3,
            "matched_source_file_count": 2,
            "unmatched_file_count": 1,
            "knowledge_count": 4,
            "output_directory": str(stage_one_dir),
        }

    async def fake_rank(indicator_number: str, **kwargs: Any) -> dict[str, Any]:
        calls["rank"] = (indicator_number, kwargs)
        return {
            "status": "partial_success",
            "indicator_number": indicator_number,
            "indicator_name": "测试指标",
            "indicator_description": "测试描述",
            "risk_type": "企业关联方合规风险",
            "knowledge_count": 4,
            "ranking_success_count": 3,
            "ranking_failure_count": 1,
            "all_json_path": str(stage_one_dir / "all.json"),
            "all_txt_path": str(tmp_path / "data" / "1.1.1.1" / "all.txt"),
        }

    monkeypatch.setattr(ranking, "generate_risk_indicator_knowledge", fake_generate)
    monkeypatch.setattr(ranking, "rank_risk_indicator_knowledge", fake_rank)
    result = await ranking.process_risk_indicator_knowledge(
        "1.1.1.1",
        risk_tree_path=tmp_path / "tree.json",
        temp_output_dir=stage_one_dir,
        data_output_dir=tmp_path / "data" / "1.1.1.1",
        max_concurrency=8,
        force=True,
        model_config=_config(),
    )

    assert result["status"] == "partial_success"
    assert result["unique_found_file_count"] == 3
    assert result["matched_source_file_count"] == 2
    assert result["unmatched_file_count"] == 1
    assert result["knowledge_count"] == 4
    assert result["ranking_success_count"] == 3
    assert result["ranking_failure_count"] == 1
    assert calls["rank"][1]["input_dir"] == str(stage_one_dir)
    assert calls["rank"][1]["max_concurrency"] == 8
    assert calls["rank"][1]["force"] is True


@_async_test
async def test_process_all_indicators_writes_progress_and_isolates_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sequences = ("1.1.1.1", "1.1.1.2", "2.1.1.1")
    calls: list[tuple[str, dict[str, Any]]] = []
    monkeypatch.setattr(ranking, "list_indicator_numbers", lambda **_: sequences)

    async def fake_process(indicator_number: str, **kwargs: Any) -> dict[str, Any]:
        calls.append((indicator_number, kwargs))
        if indicator_number == "1.1.1.2":
            raise RuntimeError("测试失败")
        return {
            "status": "success",
            "indicator_number": indicator_number,
            "indicator_name": f"指标{indicator_number}",
            "indicator_description": "测试描述",
            "risk_type": "测试风险",
            "unique_found_file_count": 2,
            "matched_source_file_count": 2,
            "unmatched_file_count": 0,
            "knowledge_count": 4,
            "ranking_success_count": 4,
            "ranking_failure_count": 0,
            "all_json_path": str(tmp_path / "temp" / indicator_number / "all.json"),
            "all_txt_path": str(tmp_path / "data" / indicator_number / "all.txt"),
            "elapsed_seconds": 0.1,
        }

    monkeypatch.setattr(ranking, "process_risk_indicator_knowledge", fake_process)
    result = await ranking.process_all_risk_indicator_knowledge(
        risk_tree_path=tmp_path / "tree.json",
        temp_output_root=tmp_path / "temp",
        data_output_root=tmp_path / "data",
        model_config=_config(max_concurrency=6),
    )

    assert [item[0] for item in calls] == list(sequences)
    assert result["status"] == "partial_success"
    assert result["indicator_count"] == 3
    assert result["completed_indicator_count"] == 3
    assert result["success_indicator_count"] == 2
    assert result["failure_indicator_count"] == 1
    assert result["knowledge_count"] == 8
    assert result["ranking_success_count"] == 8
    assert (tmp_path / "temp" / ".process_all.lock").is_file()
    assert calls[0][1]["temp_output_dir"] == tmp_path / "temp" / "1.1.1.1"
    assert calls[0][1]["data_output_dir"] == tmp_path / "data" / "1.1.1.1"

    summary_path = tmp_path / "data" / "处理汇总.json"
    persisted = json.loads(summary_path.read_text(encoding="utf-8"))
    assert persisted["status"] == "partial_success"
    assert persisted["failures"][0]["indicator_number"] == "1.1.1.2"


@_async_test
async def test_process_all_skips_final_output_but_reruns_temp_only_indicator(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sequences = ("1.1.1.1", "1.1.1.2")
    temp_root = tmp_path / "temp"
    data_root = tmp_path / "data"
    completed_dir = data_root / "1.1.1.1"
    completed_dir.mkdir(parents=True)
    (completed_dir / "all.txt").write_text("已有知识一\n已有知识二\n", encoding="utf-8")

    temp_only_dir = temp_root / "1.1.1.2"
    temp_only_dir.mkdir(parents=True)
    (data_root / "1.1.1.2").mkdir(parents=True)
    (temp_only_dir / "all.json").write_text('{"temporary": true}', encoding="utf-8")

    calls: list[str] = []
    monkeypatch.setattr(ranking, "list_indicator_numbers", lambda **_: sequences)

    async def fake_process(indicator_number: str, **_: Any) -> dict[str, Any]:
        calls.append(indicator_number)
        return {
            "status": "success",
            "indicator_number": indicator_number,
            "indicator_name": "待重跑指标",
            "indicator_description": "测试描述",
            "risk_type": "测试风险",
            "unique_found_file_count": 1,
            "matched_source_file_count": 1,
            "unmatched_file_count": 0,
            "knowledge_count": 4,
            "ranking_success_count": 4,
            "ranking_failure_count": 0,
            "all_json_path": str(temp_only_dir / "all.json"),
            "all_txt_path": str(data_root / indicator_number / "all.txt"),
            "elapsed_seconds": 0.1,
        }

    monkeypatch.setattr(ranking, "process_risk_indicator_knowledge", fake_process)
    result = await ranking.process_all_risk_indicator_knowledge(
        risk_tree_path=tmp_path / "tree.json",
        temp_output_root=temp_root,
        data_output_root=data_root,
        model_config=_config(),
    )

    assert calls == ["1.1.1.2"]
    assert result["status"] == "success"
    assert result["completed_indicator_count"] == 2
    assert result["processed_indicator_count"] == 1
    assert result["skipped_existing_indicator_count"] == 1
    assert result["new_knowledge_count"] == 4
    assert result["existing_knowledge_count"] == 2
    assert result["knowledge_count"] == 6
    assert result["skipped_existing"][0]["indicator_number"] == "1.1.1.1"

    calls.clear()
    await ranking.process_all_risk_indicator_knowledge(
        risk_tree_path=tmp_path / "tree.json",
        temp_output_root=temp_root,
        data_output_root=data_root,
        force=True,
        model_config=_config(),
    )
    assert calls == list(sequences)
