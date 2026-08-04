from __future__ import annotations

from pathlib import Path

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


def test_split_format_prefix_preserves_markdown_section() -> None:
    parts = split_format_prefix("## § 1 Anwendungsbereich\n")
    assert parts.prefix == "## § 1 "
    assert parts.body == "Anwendungsbereich"
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
    text = "## § 1 适用范围\n\n(1) 本法适用于企业。\n"
    normalized = normalize_translated_text_for_chinese_extraction(text)
    assert "## 第1条 适用范围" in normalized
    assert "(1) 本法适用于企业。" in normalized
