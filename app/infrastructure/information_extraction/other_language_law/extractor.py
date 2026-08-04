"""其他语种法规抽取编排器。

流程：
1. 对其他语种法规做格式保持型翻译。
2. 校验译文行数、编号和格式前缀。
3. 将译文交给中文 `ClauseExtractor` 生成中文知识图谱。
4. 保存中间结果、最终图谱和审查报告。
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from app.infrastructure.information_extraction.law_extract.clause_extract import ClauseExtractor
from app.infrastructure.information_extraction.other_language_law.config import (
    DEFAULT_CACHE_PATH,
    DEFAULT_REPORT_DIR,
    DEFAULT_TRANSLATED_DIR,
    TRANSLATION_OVERWRITE,
)
from app.infrastructure.information_extraction.other_language_law.quality import (
    validate_translation_directory,
    write_quality_outputs,
)
from app.infrastructure.information_extraction.other_language_law.normalizer import (
    normalize_translated_file_for_chinese_extraction,
)
from app.infrastructure.information_extraction.other_language_law.translator import (
    TranslationRunResult,
    TranslatorProtocol,
    discover_law_files,
    translate_directory,
    write_translation_outputs,
)


@dataclass
class ExtractionFileResult:
    """单文件中文抽取结果摘要。"""

    source_path: str
    translated_path: str
    chinese_input_path: str
    result_path: str
    status: str
    node_count: int = 0
    edge_count: int = 0
    error: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_path": self.source_path,
            "translated_path": self.translated_path,
            "chinese_input_path": self.chinese_input_path,
            "result_path": self.result_path,
            "status": self.status,
            "node_count": self.node_count,
            "edge_count": self.edge_count,
            "error": self.error,
        }


@dataclass
class OtherLanguageExtractionResult:
    """其他语种抽取总结果。"""

    source_dir: str
    translated_dir: str
    output_dir: str
    translation: dict[str, Any]
    quality: dict[str, Any]
    total_files: int
    success_files: int
    failed_files: int
    files: list[ExtractionFileResult] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_dir": self.source_dir,
            "translated_dir": self.translated_dir,
            "output_dir": self.output_dir,
            "translation": self.translation,
            "quality": self.quality,
            "total_files": self.total_files,
            "success_files": self.success_files,
            "failed_files": self.failed_files,
            "files": [item.to_dict() for item in self.files],
        }


def save_json(data: dict[str, Any], output_path: Path | str) -> str:
    """保存 JSON 文件。"""

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(path)


class OtherLanguageLawExtractor:
    """其他语种法规知识图谱抽取编排器。"""

    def __init__(
        self,
        clause_extractor: ClauseExtractor | None = None,
        translator: TranslatorProtocol | None = None,
        max_concurrent: int = 5,
    ) -> None:
        self.clause_extractor = clause_extractor or ClauseExtractor(max_concurrent=max_concurrent)
        self.translator = translator

    async def translate_and_check(
        self,
        source_dir: str | Path,
        translated_dir: str | Path = DEFAULT_TRANSLATED_DIR,
        cache_path: str | Path = DEFAULT_CACHE_PATH,
        output_dir: str | Path = DEFAULT_REPORT_DIR,
        overwrite: bool = TRANSLATION_OVERWRITE,
        limit: int = 0,
    ) -> dict[str, Any]:
        """只执行翻译和格式质量校验。"""

        output = Path(output_dir)
        translation_result = translate_directory(
            source_dir=source_dir,
            target_dir=translated_dir,
            cache_path=cache_path,
            translator=self.translator,
            overwrite=overwrite,
            limit=limit,
        )
        translation_outputs = write_translation_outputs(translation_result, output)
        quality_summary = validate_translation_directory(source_dir, translated_dir)
        quality_outputs = write_quality_outputs(quality_summary, output)
        return {
            "translation": translation_result.to_dict(),
            "translation_outputs": translation_outputs,
            "quality": quality_summary,
            "quality_outputs": quality_outputs,
        }

    async def extract_from_translated_dir(
        self,
        source_dir: str | Path,
        translated_dir: str | Path,
        output_dir: str | Path,
        limit: int = 0,
    ) -> list[ExtractionFileResult]:
        """对已翻译目录执行中文法规图谱抽取，并保存本地 JSON。"""

        source_root = Path(source_dir)
        translated_root = Path(translated_dir)
        output = Path(output_dir)
        kg_output_dir = output / "kg"
        chinese_input_dir = output / "translated_for_chinese_extraction"
        files = discover_law_files(translated_root)
        if limit and limit > 0:
            files = files[:limit]

        results: list[ExtractionFileResult] = []
        for translated_path in files:
            relative = translated_path.relative_to(translated_root)
            source_path = source_root / relative
            chinese_input_path = chinese_input_dir / relative
            result_path = kg_output_dir / relative.with_suffix(".kg.json")
            try:
                normalize_translated_file_for_chinese_extraction(translated_path, chinese_input_path)
                text = chinese_input_path.read_text(encoding="utf-8", errors="ignore")
                kg = await self.clause_extractor.extract_clauses(
                    filename=chinese_input_path.stem,
                    text=text,
                )
                kg.setdefault("metadata", {})
                kg["metadata"].update(
                    {
                        "source_path": str(source_path),
                        "translated_path": str(translated_path),
                        "chinese_input_path": str(chinese_input_path),
                        "source_language": "other",
                        "target_language": "Simplified Chinese",
                    }
                )
                save_json(kg, result_path)
                results.append(
                    ExtractionFileResult(
                        source_path=str(source_path),
                        translated_path=str(translated_path),
                        chinese_input_path=str(chinese_input_path),
                        result_path=str(result_path),
                        status="success",
                        node_count=len(kg.get("nodes") or []),
                        edge_count=len(kg.get("edges") or []),
                    )
                )
            except Exception as exc:
                logging.exception("其他语种译文抽取失败: %s", translated_path)
                results.append(
                    ExtractionFileResult(
                        source_path=str(source_path),
                        translated_path=str(translated_path),
                        chinese_input_path=str(chinese_input_path),
                        result_path=str(result_path),
                        status="failed",
                        error=str(exc),
                    )
                )
        return results

    async def extract_directory(
        self,
        source_dir: str | Path,
        translated_dir: str | Path = DEFAULT_TRANSLATED_DIR,
        cache_path: str | Path = DEFAULT_CACHE_PATH,
        output_dir: str | Path = DEFAULT_REPORT_DIR,
        overwrite: bool = TRANSLATION_OVERWRITE,
        limit: int = 0,
        run_extraction: bool = True,
    ) -> OtherLanguageExtractionResult:
        """执行完整流程：翻译、质量校验、中文抽取。"""

        output = Path(output_dir)
        check_result = await self.translate_and_check(
            source_dir=source_dir,
            translated_dir=translated_dir,
            cache_path=cache_path,
            output_dir=output,
            overwrite=overwrite,
            limit=limit,
        )

        extraction_results: list[ExtractionFileResult] = []
        if run_extraction:
            extraction_results = await self.extract_from_translated_dir(
                source_dir=source_dir,
                translated_dir=translated_dir,
                output_dir=output,
                limit=limit,
            )

        result = OtherLanguageExtractionResult(
            source_dir=str(source_dir),
            translated_dir=str(translated_dir),
            output_dir=str(output),
            translation=check_result["translation"],
            quality=check_result["quality"],
            total_files=len(extraction_results) if run_extraction else check_result["translation"]["total_files"],
            success_files=sum(1 for item in extraction_results if item.status == "success") if run_extraction else 0,
            failed_files=sum(1 for item in extraction_results if item.status == "failed") if run_extraction else 0,
            files=extraction_results,
        )
        self.write_extraction_report(result, output)
        return result

    @staticmethod
    def write_extraction_report(result: OtherLanguageExtractionResult, output_dir: Path | str) -> dict[str, str]:
        """保存完整流程报告。"""

        output = Path(output_dir)
        output.mkdir(parents=True, exist_ok=True)
        json_path = output / "other_language_extraction_summary.json"
        md_path = output / "other_language_extraction_report.md"
        json_path.write_text(json.dumps(result.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")

        lines = [
            "# 其他语种法规知识图谱抽取报告",
            "",
            f"- 源目录：`{result.source_dir}`",
            f"- 译文目录：`{result.translated_dir}`",
            f"- 输出目录：`{result.output_dir}`",
            f"- 翻译文件数：{result.translation.get('total_files')}",
            f"- 翻译成功：{result.translation.get('success_files')}",
            f"- 翻译跳过：{result.translation.get('skipped_files')}",
            f"- 翻译失败：{result.translation.get('failed_files')}",
            f"- 需人工审查译文：{result.quality.get('manual_review_count')}",
            f"- 抽取成功：{result.success_files}",
            f"- 抽取失败：{result.failed_files}",
            "",
            "| 文件 | 状态 | 节点数 | 关系数 | 中文抽取输入 | 结果文件 | 错误 |",
            "|---|---:|---:|---:|---|---|---|",
        ]
        for item in result.files:
            lines.append(
                f"| {Path(item.translated_path).name} | {item.status} | {item.node_count} | "
                f"{item.edge_count} | `{item.chinese_input_path}` | `{item.result_path}` | {item.error} |"
            )
        md_path.write_text("\n".join(lines), encoding="utf-8")
        return {"json": str(json_path), "markdown": str(md_path)}


def run_extract_directory_sync(**kwargs: Any) -> OtherLanguageExtractionResult:
    """同步脚本入口包装。"""

    return asyncio.run(OtherLanguageLawExtractor().extract_directory(**kwargs))
