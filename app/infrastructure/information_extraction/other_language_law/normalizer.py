"""译文到中文法规抽取输入的轻量归一化。"""

from __future__ import annotations

import re
from pathlib import Path


SECTION_TO_CHINESE_CLAUSE_RE = re.compile(
    r"^(?P<prefix>\s*#{1,6}\s*)§\s*(?P<number>\d+[A-Za-z]?)\.?\s*(?P<title>.*)$"
)
FOOTNOTE_HEADING_RE = re.compile(r"^\s*#{1,6}\s*(脚注|Footnote|Fußnote|Fussnote)\s*$", re.IGNORECASE)
MARKDOWN_HEADING_RE = re.compile(r"^\s*#{1,6}\s+")


def normalize_translated_text_for_chinese_extraction(text: str) -> str:
    """把保留外文结构编号的译文转换为中文 `law_extract` 可切分的形式。

    当前只做必要归一化：行首 `§ 1 标题` 转为 `第1条 标题`。
    不处理正文内部引用，避免污染引用语义。
    """

    output_lines: list[str] = []
    skipping_footnote = False
    for line in text.splitlines(keepends=True):
        newline = ""
        body = line
        if line.endswith("\r\n"):
            body, newline = line[:-2], "\r\n"
        elif line.endswith("\n"):
            body, newline = line[:-1], "\n"
        elif line.endswith("\r"):
            body, newline = line[:-1], "\r"

        if FOOTNOTE_HEADING_RE.match(body):
            skipping_footnote = True
            continue
        if skipping_footnote and MARKDOWN_HEADING_RE.match(body):
            skipping_footnote = False
        if skipping_footnote:
            continue

        match = SECTION_TO_CHINESE_CLAUSE_RE.match(body)
        if match:
            title = match.group("title").strip()
            normalized = f"{match.group('prefix')}第{match.group('number')}条"
            if title:
                normalized += f" {title}"
            output_lines.append(normalized + newline)
        else:
            output_lines.append(line)
    return "".join(output_lines)


def normalize_translated_file_for_chinese_extraction(
    translated_path: Path | str,
    output_path: Path | str,
) -> str:
    """生成中文法规抽取专用临时文件。"""

    source = Path(translated_path)
    target = Path(output_path)
    text = source.read_text(encoding="utf-8", errors="ignore")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(normalize_translated_text_for_chinese_extraction(text), encoding="utf-8")
    return str(target)
