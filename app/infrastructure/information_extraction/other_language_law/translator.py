"""格式保持型法规翻译工具。

核心原则：
1. 原文件不修改，只生成临时中文译文。
2. 尽量保持原始行数、换行符、缩进、标题、列表和法律编号。
3. 只翻译自然语言正文，结构前缀和行尾空白原样回写。
"""

from __future__ import annotations

import hashlib
import json
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Protocol
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from app.infrastructure.information_extraction.other_language_law.config import (
    OTHER_LANGUAGE_MODEL,
    OTHER_LANGUAGE_MODEL_API_KEY,
    OTHER_LANGUAGE_MODEL_API_URL,
    SUPPORTED_FILE_SUFFIXES,
    TRANSLATION_MAX_QUERY_CHARS,
    TRANSLATION_MAX_RETRIES,
    TRANSLATION_MAX_TOKENS,
    TRANSLATION_SLEEP_SECONDS,
    TRANSLATION_SOURCE_LANG,
    TRANSLATION_TARGET_LANG,
    TRANSLATION_TEMPERATURE,
)


class TranslationError(RuntimeError):
    """翻译请求或翻译结果异常。"""


class TranslatorProtocol(Protocol):
    """翻译客户端协议，便于测试时注入 fake translator。"""

    model: str
    source_lang: str
    target_lang: str

    def translate(self, text: str) -> str:
        """翻译单段文本。"""


@dataclass(frozen=True)
class LineParts:
    """一行文本拆分后的结构。"""

    prefix: str
    body: str
    suffix: str
    newline: str


@dataclass
class FileTranslationResult:
    """单文件翻译结果。"""

    source_path: str
    target_path: str
    status: str
    source_line_count: int
    translated_line_count: int
    translated_line_count_by_llm: int = 0
    copied_line_count: int = 0
    cache_hit_count: int = 0
    error: str = ""

    def to_dict(self) -> dict:
        return {
            "source_path": self.source_path,
            "target_path": self.target_path,
            "status": self.status,
            "source_line_count": self.source_line_count,
            "translated_line_count": self.translated_line_count,
            "translated_line_count_by_llm": self.translated_line_count_by_llm,
            "copied_line_count": self.copied_line_count,
            "cache_hit_count": self.cache_hit_count,
            "error": self.error,
        }


@dataclass
class TranslationRunResult:
    """目录翻译结果。"""

    source_dir: str
    target_dir: str
    cache_path: str
    total_files: int
    success_files: int
    failed_files: int
    skipped_files: int
    files: list[FileTranslationResult] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "source_dir": self.source_dir,
            "target_dir": self.target_dir,
            "cache_path": self.cache_path,
            "total_files": self.total_files,
            "success_files": self.success_files,
            "failed_files": self.failed_files,
            "skipped_files": self.skipped_files,
            "files": [item.to_dict() for item in self.files],
        }


def normalize_newline(line: str) -> tuple[str, str]:
    """拆出正文和原始换行符。"""

    if line.endswith("\r\n"):
        return line[:-2], "\r\n"
    if line.endswith("\n"):
        return line[:-1], "\n"
    if line.endswith("\r"):
        return line[:-1], "\r"
    return line, ""


def is_probably_non_translatable(text: str) -> bool:
    """判断某段是否应原样保留。"""

    stripped = text.strip()
    if not stripped:
        return True
    if not re.search(r"[A-Za-zÀ-ÖØ-öø-ÿ]", stripped):
        return True
    if re.fullmatch(r"[-*_=`~#\[\]().,;:/$\\|<>{}\s0-9A-Za-zÀ-ÖØ-öø-ÿ]+", stripped):
        words = re.findall(r"[A-Za-zÀ-ÖØ-öø-ÿ]{3,}", stripped)
        if len(words) == 0:
            return True
    return False


def split_format_prefix(text: str) -> LineParts:
    """拆分行首格式前缀、可翻译正文、行尾空白和换行符。"""

    body, newline = normalize_newline(text)
    trailing_match = re.search(r"\s*$", body)
    trailing = trailing_match.group(0) if trailing_match else ""
    core = body[: len(body) - len(trailing)] if trailing else body

    prefix = ""
    rest = core

    leading = re.match(r"^\s*", rest).group(0)
    prefix += leading
    rest = rest[len(leading):]

    for pattern in (
        r"^(#{1,6}\s+)",
        r"^(>\s*)",
        r"^([-*+]\s+)",
        r"^(\d+[.)]\s+)",
    ):
        match = re.match(pattern, rest)
        if match:
            prefix += match.group(1)
            rest = rest[len(match.group(1)):]
            break

    for pattern in (
        r"^(§\s*[0-9A-Za-zÀ-ÖØ-öø-ÿ_.-]*\.?\s*)",
        r"^((?:第[零一二三四五六七八九十百千万\d]+\s*[编章节条款项]\s*)+)",
        r"^((?:\[[^\]]+\]\s*)+)",
        r"^((?:\([A-Za-z0-9ivxlcdmIVXLCDM]+\)\s*)+)",
    ):
        match = re.match(pattern, rest)
        if match:
            prefix += match.group(1)
            rest = rest[len(match.group(1)):]

    match = re.match(r"^([A-ZÀ-ÖØ-Þ][A-ZÀ-ÖØ-Þ0-9 ,'/&()\\-]{1,120}(?:\.|--|.—|.--)\s*)", rest)
    if match and len(match.group(1).split()) <= 12:
        prefix += match.group(1)
        rest = rest[len(match.group(1)):]

    return LineParts(prefix=prefix, body=rest, suffix=trailing, newline=newline)


def cleanup_translation(text: str) -> str:
    """清理模型可能额外输出的包裹格式。"""

    value = str(text or "").strip()
    value = re.sub(r"^```(?:\w+)?\s*", "", value)
    value = re.sub(r"\s*```$", "", value)
    value = re.sub(r"^(译文|翻译|Translation)\s*[:：]\s*", "", value, flags=re.IGNORECASE)
    return value.strip()


class JsonlTranslationCache:
    """JSONL 翻译缓存，避免重复请求相同文本。"""

    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)
        self.data: dict[str, str] = {}
        self._load()

    @staticmethod
    def key(text: str, model: str, source_lang: str, target_lang: str) -> str:
        raw = f"{model}\0{source_lang}\0{target_lang}\0{text}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _load(self) -> None:
        if not self.path.exists():
            return
        with self.path.open("r", encoding="utf-8") as file:
            for line in file:
                line = line.strip()
                if not line:
                    continue
                try:
                    item = json.loads(line)
                except json.JSONDecodeError:
                    continue
                key = item.get("key")
                value = item.get("value")
                if isinstance(key, str) and isinstance(value, str):
                    self.data[key] = value

    def get(self, text: str, model: str, source_lang: str, target_lang: str) -> str | None:
        return self.data.get(self.key(text, model, source_lang, target_lang))

    def set(self, text: str, translated: str, model: str, source_lang: str, target_lang: str) -> None:
        key = self.key(text, model, source_lang, target_lang)
        self.data[key] = translated
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as file:
            file.write(json.dumps({"key": key, "value": translated}, ensure_ascii=False) + "\n")


class LocalLLMTranslator:
    """OpenAI-compatible 本地大模型翻译客户端。"""

    def __init__(
        self,
        model: str = OTHER_LANGUAGE_MODEL,
        api_key: str = OTHER_LANGUAGE_MODEL_API_KEY,
        api_url: str = OTHER_LANGUAGE_MODEL_API_URL,
        source_lang: str = TRANSLATION_SOURCE_LANG,
        target_lang: str = TRANSLATION_TARGET_LANG,
        max_tokens: int = TRANSLATION_MAX_TOKENS,
        temperature: float = TRANSLATION_TEMPERATURE,
        sleep_seconds: float = TRANSLATION_SLEEP_SECONDS,
        max_retries: int = TRANSLATION_MAX_RETRIES,
        timeout: int = 180,
    ) -> None:
        self.model = model
        self.api_key = api_key
        self.api_url = api_url
        self.source_lang = source_lang
        self.target_lang = target_lang
        self.max_tokens = max_tokens
        self.temperature = temperature
        self.sleep_seconds = sleep_seconds
        self.max_retries = max_retries
        self.timeout = timeout

    def translate(self, text: str) -> str:
        if len(text) > TRANSLATION_MAX_QUERY_CHARS:
            raise TranslationError(f"single request text too long: {len(text)} chars")

        payload = {
            "model": self.model,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You are a professional legal translator. Translate the user's legal text "
                        "into Simplified Chinese. Return only the translation. Do not add explanations, "
                        "notes, markdown fences, headings, bullets, prefixes, or suffixes. Preserve legal "
                        "citations, article numbers, section numbers, paragraph markers, amounts, dates, "
                        "formulas, and punctuation as much as possible."
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"Translate from {self.source_lang} to {self.target_lang}. "
                        "Only translate natural-language text. Keep legal numbering unchanged.\n\n"
                        f"{text}"
                    ),
                },
            ],
            "temperature": self.temperature,
            "max_tokens": self.max_tokens,
        }
        encoded = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        last_error: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                request = Request(self.api_url, data=encoded, headers=headers, method="POST")
                with urlopen(request, timeout=self.timeout) as response:
                    result = json.loads(response.read().decode("utf-8"))
                choices = result.get("choices") or []
                if not choices:
                    raise TranslationError(f"missing choices in response: {result}")
                message = choices[0].get("message") or {}
                content = str(message.get("content") or "").strip()
                if not content:
                    raise TranslationError(f"empty translation response: {result}")
                if self.sleep_seconds > 0:
                    time.sleep(self.sleep_seconds)
                return cleanup_translation(content)
            except (HTTPError, URLError, TimeoutError, json.JSONDecodeError, TranslationError) as exc:
                last_error = exc
                if attempt < self.max_retries:
                    time.sleep(1.5 * attempt)
        raise TranslationError(f"translation failed after retries: {last_error}")


def split_long_body(text: str, max_chars: int = TRANSLATION_MAX_QUERY_CHARS) -> list[str]:
    """把超长单行切成多个请求片段，尽量在句号、分号、逗号或空格处切分。"""

    if len(text) <= max_chars:
        return [text]

    chunks: list[str] = []
    start = 0
    while start < len(text):
        end = min(start + max_chars, len(text))
        if end < len(text):
            window = text[start:end]
            cut = max(window.rfind(". "), window.rfind("; "), window.rfind(", "), window.rfind(" "))
            if cut > max_chars * 0.5:
                end = start + cut + 1
        chunks.append(text[start:end])
        start = end
    return chunks


def translate_body(
    body: str,
    translator: TranslatorProtocol,
    cache: JsonlTranslationCache,
    stats: dict[str, int] | None = None,
) -> str:
    """翻译一段正文，并缓存每个请求片段。"""

    if is_probably_non_translatable(body):
        return body

    translated_chunks: list[str] = []
    for chunk in split_long_body(body):
        cached = cache.get(chunk, translator.model, translator.source_lang, translator.target_lang)
        if cached is None:
            cached = translator.translate(chunk)
            cache.set(chunk, cached, translator.model, translator.source_lang, translator.target_lang)
            if stats is not None:
                stats["llm_calls"] = stats.get("llm_calls", 0) + 1
        elif stats is not None:
            stats["cache_hits"] = stats.get("cache_hits", 0) + 1
        translated_chunks.append(cached)
    return "".join(translated_chunks)


def translate_line(
    line: str,
    translator: TranslatorProtocol,
    cache: JsonlTranslationCache,
    stats: dict[str, int] | None = None,
) -> str:
    """翻译一行，同时保留结构前缀、行尾空白和换行符。"""

    parts = split_format_prefix(line)
    if is_probably_non_translatable(parts.body):
        if stats is not None:
            stats["copied_lines"] = stats.get("copied_lines", 0) + 1
        return line
    translated = translate_body(parts.body, translator, cache, stats)
    if stats is not None:
        stats["translated_lines"] = stats.get("translated_lines", 0) + 1
    return parts.prefix + translated + parts.suffix + parts.newline


def discover_law_files(source_dir: Path | str) -> list[Path]:
    """发现可处理的法规文件。"""

    root = Path(source_dir)
    return [
        item for item in sorted(root.rglob("*"), key=lambda path: path.as_posix().lower())
        if item.is_file() and item.suffix.lower() in SUPPORTED_FILE_SUFFIXES
    ]


def translate_file(
    source_path: Path | str,
    target_path: Path | str,
    translator: TranslatorProtocol,
    cache: JsonlTranslationCache,
    overwrite: bool = False,
) -> FileTranslationResult:
    """翻译单个文件。"""

    source = Path(source_path)
    target = Path(target_path)
    if target.exists() and not overwrite:
        text = target.read_text(encoding="utf-8", errors="ignore")
        return FileTranslationResult(
            source_path=str(source),
            target_path=str(target),
            status="skipped",
            source_line_count=len(source.read_text(encoding="utf-8", errors="ignore").splitlines()),
            translated_line_count=len(text.splitlines()),
        )

    text = source.read_text(encoding="utf-8", errors="ignore")
    lines = text.splitlines(keepends=True)
    output_lines: list[str] = []
    stats: dict[str, int] = {"translated_lines": 0, "copied_lines": 0, "cache_hits": 0, "llm_calls": 0}

    for index, line in enumerate(lines, start=1):
        try:
            output_lines.append(translate_line(line, translator, cache, stats))
        except Exception as exc:
            partial_path = target.with_name(target.stem + ".partial" + target.suffix)
            partial_path.parent.mkdir(parents=True, exist_ok=True)
            with partial_path.open("w", encoding="utf-8", newline="") as file:
                file.write("".join(output_lines))
            raise TranslationError(f"{source.name} line {index}/{len(lines)} translate failed: {exc}") from exc

    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8", newline="") as file:
        file.write("".join(output_lines))

    return FileTranslationResult(
        source_path=str(source),
        target_path=str(target),
        status="success",
        source_line_count=len(lines),
        translated_line_count=len(output_lines),
        translated_line_count_by_llm=stats["translated_lines"],
        copied_line_count=stats["copied_lines"],
        cache_hit_count=stats["cache_hits"],
    )


def translate_directory(
    source_dir: Path | str,
    target_dir: Path | str,
    cache_path: Path | str,
    translator: TranslatorProtocol | None = None,
    overwrite: bool = False,
    limit: int = 0,
) -> TranslationRunResult:
    """批量翻译目录。"""

    source_root = Path(source_dir)
    target_root = Path(target_dir)
    if not source_root.exists():
        raise FileNotFoundError(f"source dir does not exist: {source_root}")
    if not source_root.is_dir():
        raise NotADirectoryError(f"source path is not a dir: {source_root}")

    translator = translator or LocalLLMTranslator()
    cache = JsonlTranslationCache(cache_path)
    files = discover_law_files(source_root)
    if limit and limit > 0:
        files = files[:limit]

    results: list[FileTranslationResult] = []
    for source_path in files:
        relative = source_path.relative_to(source_root)
        target_path = target_root / relative
        try:
            result = translate_file(source_path, target_path, translator, cache, overwrite=overwrite)
        except Exception as exc:
            result = FileTranslationResult(
                source_path=str(source_path),
                target_path=str(target_path),
                status="failed",
                source_line_count=len(source_path.read_text(encoding="utf-8", errors="ignore").splitlines()),
                translated_line_count=0,
                error=str(exc),
            )
        results.append(result)

    return TranslationRunResult(
        source_dir=str(source_root),
        target_dir=str(target_root),
        cache_path=str(cache.path),
        total_files=len(files),
        success_files=sum(1 for item in results if item.status == "success"),
        failed_files=sum(1 for item in results if item.status == "failed"),
        skipped_files=sum(1 for item in results if item.status == "skipped"),
        files=results,
    )


def write_translation_outputs(run_result: TranslationRunResult, output_dir: Path | str) -> dict[str, str]:
    """保存翻译统计 JSON 和 Markdown 报告。"""

    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    json_path = output / "translation_summary.json"
    md_path = output / "translation_report.md"
    json_path.write_text(json.dumps(run_result.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        "# 其他语种法规格式保持翻译报告",
        "",
        f"- 源目录：`{run_result.source_dir}`",
        f"- 译文目录：`{run_result.target_dir}`",
        f"- 缓存文件：`{run_result.cache_path}`",
        f"- 文件总数：{run_result.total_files}",
        f"- 翻译成功：{run_result.success_files}",
        f"- 已跳过：{run_result.skipped_files}",
        f"- 翻译失败：{run_result.failed_files}",
        "",
        "| 文件 | 状态 | 原文行数 | 译文行数 | LLM翻译行 | 缓存命中 | 错误 |",
        "|---|---:|---:|---:|---:|---:|---|",
    ]
    for item in run_result.files:
        lines.append(
            f"| {Path(item.source_path).name} | {item.status} | {item.source_line_count} | "
            f"{item.translated_line_count} | {item.translated_line_count_by_llm} | "
            f"{item.cache_hit_count} | {item.error} |"
        )
    md_path.write_text("\n".join(lines), encoding="utf-8")
    return {"json": str(json_path), "markdown": str(md_path)}
