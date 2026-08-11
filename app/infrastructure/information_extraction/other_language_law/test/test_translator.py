from __future__ import annotations

import asyncio
from pathlib import Path

from app.infrastructure.information_extraction.other_language_law.extractor import (
    OtherLanguageLawExtractor,
)
from app.infrastructure.information_extraction.other_language_law.normalizer import (
    normalize_translated_text_for_chinese_extraction,
)
from app.infrastructure.information_extraction.other_language_law.translator import (
    JsonlTranslationCache,
    split_format_prefix,
    translate_directory,
)


class FakeTranslator:
    model = "fake-model"
    source_lang = "German"
    target_lang = "Simplified Chinese"

    def translate(self, text: str) -> str:
        return f"译文({text})"


class FakeClauseExtractor:
    async def extract_clauses(self, filename: str, text: str) -> dict:
        return {
            "nodes": [
                {
                    "id": f"doc_{filename}",
                    "label": "法规文件",
                    "name": filename,
                    "原文": text,
                }
            ],
            "edges": [],
        }


class FakeResultStats:
    def __init__(self) -> None:
        self.error = 99
        self.error_msg = "old error"
        self.week_warning = 99
        self.week_warning_msg = "old warning"
        self.strong_warning = 99
        self.strong_warning_msg = "old strong warning"


class FakeClauseExtractorWithStats:
    def __init__(self) -> None:
        self.result_stats = FakeResultStats()

    async def extract_clauses(self, filename: str, text: str) -> dict:
        self.result_stats.error += 1
        self.result_stats.error_msg += f"{filename} extraction error\n"
        self.result_stats.week_warning += 2
        self.result_stats.week_warning_msg += f"{filename} weak warning\n"
        self.result_stats.strong_warning += 3
        self.result_stats.strong_warning_msg += f"{filename} strong warning\n"
        return {
            "nodes": [
                {
                    "id": f"doc_{filename}",
                    "label": "law_document",
                    "name": filename,
                }
            ],
            "edges": [],
        }


def test_split_format_prefix_preserves_markdown_section() -> None:
    parts = split_format_prefix("## § 1 Anwendungsbereich\n")
    assert parts.prefix == "## § 1 "
    assert parts.body == "Anwendungsbereich"
    assert parts.newline == "\n"


def test_split_format_prefix_does_not_protect_foreign_section_word() -> None:
    parts = split_format_prefix("## Abschnitt 2\n")
    assert parts.prefix == "## "
    assert parts.body == "Abschnitt 2"
    assert parts.newline == "\n"


def test_split_format_prefix_preserves_list_marker() -> None:
    parts = split_format_prefix("  1. ihre Hauptverwaltung\n")
    assert parts.prefix == "  1. "
    assert parts.body == "ihre Hauptverwaltung"
    assert parts.newline == "\n"


def test_translation_cache_roundtrip(tmp_path: Path) -> None:
    cache = JsonlTranslationCache(tmp_path / "cache.jsonl")
    cache.set("abc", "中文", "m", "German", "Simplified Chinese")
    loaded = JsonlTranslationCache(tmp_path / "cache.jsonl")
    assert loaded.get("abc", "m", "German", "Simplified Chinese") == "中文"


def test_translate_directory_with_fake_translator(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    target_dir = tmp_path / "target"
    source_dir.mkdir()
    (source_dir / "law.md").write_text("## § 1 Titel\n\n(1) Inhalt\n", encoding="utf-8")
    result = translate_directory(
        source_dir=source_dir,
        target_dir=target_dir,
        cache_path=tmp_path / "cache.jsonl",
        translator=FakeTranslator(),
        overwrite=True,
    )
    translated = (target_dir / "law.md").read_text(encoding="utf-8")
    assert result.success_files == 1
    assert "## § 1 译文(Titel)" in translated
    assert "(1) 译文(Inhalt)" in translated
    assert (source_dir / "law.md").read_text(encoding="utf-8") == "## § 1 Titel\n\n(1) Inhalt\n"


def test_normalize_translated_text_for_chinese_extraction() -> None:
    text = (
        "## § 1 适用范围\n\n"
        "(1) 本法适用于企业。\n"
        "## Abschnitt 2\n\n"
        "## Sorgfaltspflichten\n"
        "## Unterabschnitt 1 审计报告审查\n"
        "## Zivilprozess\n"
        "## 附件（第2条第1款）\n"
    )
    normalized = normalize_translated_text_for_chinese_extraction(text)
    assert "## 第1条 适用范围" in normalized
    assert "(1) 本法适用于企业。" in normalized
    assert "## 第2节 尽职义务" in normalized
    assert "## 尽职义务" not in normalized
    assert "## 第1节 审计报告审查" in normalized
    assert "小节" not in normalized
    assert "## 民事诉讼" in normalized
    assert "附件（第2条第1款）" in normalized
    assert "## 附件" not in normalized


def test_normalize_merges_numbered_structure_heading_with_next_title() -> None:
    text = (
        "## 第4节\n\n"
        "## 行政监管与执行\n"
        "## Unterabschnitt 1\n\n"
        "## 审计报告审查\n"
        "## 第2小节\n\n"
        "## 基于风险的控制\n"
        "## 第12条 报告的提交\n"
    )
    normalized = normalize_translated_text_for_chinese_extraction(text)
    assert "## 第4节 行政监管与执行" in normalized
    assert "## 第1节 审计报告审查" in normalized
    assert "## 第2节 基于风险的控制" in normalized
    assert "## 行政监管与执行" not in normalized
    assert "## 审计报告审查" not in normalized
    assert "小节" not in normalized
    assert "## 第12条 报告的提交" in normalized


def test_extract_uses_source_files_not_stale_translated_files(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    translated_dir = tmp_path / "translated"
    output_dir = tmp_path / "output"
    source_dir.mkdir()
    translated_dir.mkdir()
    (source_dir / "current.md").write_text("## § 1 Titel\n\n(1) Inhalt\n", encoding="utf-8")
    (translated_dir / "current.md").write_text("## § 1 标题\n\n(1) 内容\n", encoding="utf-8")
    (translated_dir / "stale.md").write_text("## § 1 旧标题\n\n(1) 旧内容\n", encoding="utf-8")
    stale_kg = output_dir / "kg" / "stale.kg.json"
    stale_kg.parent.mkdir(parents=True)
    stale_kg.write_text("{}", encoding="utf-8")

    extractor = OtherLanguageLawExtractor(clause_extractor=FakeClauseExtractor())
    results = asyncio.run(
        extractor.extract_from_translated_dir(
            source_dir=source_dir,
            translated_dir=translated_dir,
            output_dir=output_dir,
        )
    )

    assert [Path(item.translated_path).name for item in results] == ["current.md"]
    assert (output_dir / "kg" / "current.kg.json").exists()
    assert not stale_kg.exists()


def test_extract_directory_returns_real_clause_extraction_stats(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    translated_dir = tmp_path / "translated"
    output_dir = tmp_path / "output"
    source_dir.mkdir()
    (source_dir / "law.md").write_text("## Section 1\n\nText\n", encoding="utf-8")

    extractor = OtherLanguageLawExtractor(
        clause_extractor=FakeClauseExtractorWithStats(),
        translator=FakeTranslator(),
    )
    result = asyncio.run(
        extractor.extract_directory(
            source_dir=source_dir,
            translated_dir=translated_dir,
            output_dir=output_dir,
            cache_path=tmp_path / "cache.jsonl",
            overwrite=True,
        )
    )

    assert result.extraction_stats["error"] == 1
    assert result.extraction_stats["weak_warning"] == 2
    assert result.extraction_stats["strong_warning"] == 3
    assert "old error" not in result.extraction_stats["error_msg"]
