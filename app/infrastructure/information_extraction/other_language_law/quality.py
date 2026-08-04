"""其他语种法规翻译质量校验。"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path


STRUCTURE_PREFIX_RE = re.compile(
    r"^\s*(#{1,6}\s+|[-*+]\s+|>\s*|\d+[.)]\s+|(?:Article|ARTICLE|Art\.|Artikel|SECTION|SEC\.|Sec\.|Section|Abschnitt|§)\s*[0-9A-Za-z_.-]*|第[零一二三四五六七八九十百千万\d]+\s*[编章节条款项]|(?:\([A-Za-z0-9ivxlcdmIVXLCDM]+\)\s*)+)"
)
LEGAL_NUMBER_RE = re.compile(
    r"(§\s*\d+[A-Za-z0-9_.-]*|(?:Article|Art\.|Artikel|SECTION|SEC\.|Sec\.|Section|Abschnitt)\s*\d+[A-Za-z0-9_.-]*|第[零一二三四五六七八九十百千万\d]+\s*[编章节条款项])"
)


@dataclass
class FileQualityReport:
    """单文件翻译质量报告。"""

    source_path: str
    translated_path: str
    source_line_count: int
    translated_line_count: int
    line_count_match: bool
    source_legal_number_count: int
    translated_legal_number_count: int
    missing_prefix_lines: list[int] = field(default_factory=list)
    suspicious_untranslated_lines: list[int] = field(default_factory=list)
    requires_manual_review: bool = False

    def to_dict(self) -> dict:
        return {
            "source_path": self.source_path,
            "translated_path": self.translated_path,
            "source_line_count": self.source_line_count,
            "translated_line_count": self.translated_line_count,
            "line_count_match": self.line_count_match,
            "source_legal_number_count": self.source_legal_number_count,
            "translated_legal_number_count": self.translated_legal_number_count,
            "missing_prefix_lines": self.missing_prefix_lines,
            "suspicious_untranslated_lines": self.suspicious_untranslated_lines,
            "requires_manual_review": self.requires_manual_review,
        }


def _prefix(text: str) -> str:
    match = STRUCTURE_PREFIX_RE.match(text)
    return match.group(0).strip() if match else ""


def _contains_cjk(text: str) -> bool:
    return bool(re.search(r"[\u4e00-\u9fff]", text))


def validate_translated_file(source_path: Path | str, translated_path: Path | str) -> FileQualityReport:
    """校验单个译文文件是否保持基础结构。"""

    source = Path(source_path)
    translated = Path(translated_path)
    source_text = source.read_text(encoding="utf-8", errors="ignore")
    translated_text = translated.read_text(encoding="utf-8", errors="ignore") if translated.exists() else ""
    source_lines = source_text.splitlines()
    translated_lines = translated_text.splitlines()
    missing_prefix_lines: list[int] = []
    suspicious_untranslated_lines: list[int] = []

    for index, source_line in enumerate(source_lines, start=1):
        if index > len(translated_lines):
            break
        source_prefix = _prefix(source_line)
        if source_prefix and not translated_lines[index - 1].lstrip().startswith(source_prefix.lstrip()):
            missing_prefix_lines.append(index)
        translated_line = translated_lines[index - 1].strip()
        if translated_line and re.search(r"[A-Za-zÀ-ÖØ-öø-ÿ]{8,}", translated_line) and not _contains_cjk(translated_line):
            suspicious_untranslated_lines.append(index)

    source_legal_number_count = len(LEGAL_NUMBER_RE.findall(source_text))
    translated_legal_number_count = len(LEGAL_NUMBER_RE.findall(translated_text))
    line_count_match = len(source_lines) == len(translated_lines)
    requires_manual_review = (
        not line_count_match
        or bool(missing_prefix_lines)
        or translated_legal_number_count < max(1, int(source_legal_number_count * 0.75))
    )
    return FileQualityReport(
        source_path=str(source),
        translated_path=str(translated),
        source_line_count=len(source_lines),
        translated_line_count=len(translated_lines),
        line_count_match=line_count_match,
        source_legal_number_count=source_legal_number_count,
        translated_legal_number_count=translated_legal_number_count,
        missing_prefix_lines=missing_prefix_lines[:100],
        suspicious_untranslated_lines=suspicious_untranslated_lines[:100],
        requires_manual_review=requires_manual_review,
    )


def validate_translation_directory(source_dir: Path | str, translated_dir: Path | str) -> dict:
    """校验目录下所有已翻译文件。"""

    source_root = Path(source_dir)
    translated_root = Path(translated_dir)
    reports: list[FileQualityReport] = []
    for source_path in sorted(source_root.rglob("*"), key=lambda item: item.as_posix().lower()):
        if not source_path.is_file() or source_path.suffix.lower() not in {".md", ".txt"}:
            continue
        translated_path = translated_root / source_path.relative_to(source_root)
        reports.append(validate_translated_file(source_path, translated_path))
    return {
        "source_dir": str(source_root),
        "translated_dir": str(translated_root),
        "file_count": len(reports),
        "manual_review_count": sum(1 for item in reports if item.requires_manual_review),
        "files": [item.to_dict() for item in reports],
    }


def write_quality_outputs(summary: dict, output_dir: Path | str) -> dict[str, str]:
    """保存翻译质量校验结果。"""

    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    json_path = output / "translation_quality_summary.json"
    md_path = output / "translation_quality_report.md"
    json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        "# 其他语种法规翻译质量校验报告",
        "",
        f"- 源目录：`{summary.get('source_dir')}`",
        f"- 译文目录：`{summary.get('translated_dir')}`",
        f"- 文件数：{summary.get('file_count')}",
        f"- 需人工审查文件数：{summary.get('manual_review_count')}",
        "",
        "| 文件 | 行数一致 | 原文编号数 | 译文编号数 | 前缀异常行数 | 疑似未翻译行数 | 需审查 |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for item in summary.get("files") or []:
        lines.append(
            f"| {Path(item.get('source_path', '')).name} | {item.get('line_count_match')} | "
            f"{item.get('source_legal_number_count')} | {item.get('translated_legal_number_count')} | "
            f"{len(item.get('missing_prefix_lines') or [])} | "
            f"{len(item.get('suspicious_untranslated_lines') or [])} | "
            f"{item.get('requires_manual_review')} |"
        )
    md_path.write_text("\n".join(lines), encoding="utf-8")
    return {"json": str(json_path), "markdown": str(md_path)}
