"""译文到中文法规抽取输入的轻量归一化。"""

from __future__ import annotations

import re
from pathlib import Path


SECTION_TO_CHINESE_CLAUSE_RE = re.compile(
    r"^(?P<prefix>\s*#{1,6}\s*)§\s*(?P<number>\d+[A-Za-z]?)\.?\s*(?P<title>.*)$"
)
APPENDIX_HEADING_RE = re.compile(
    r"^\s*#{1,6}\s*(?P<head>附件|附录|附表)(?P<tail>.*)$"
)
CHINESE_STRUCTURE_HEADING_RE = re.compile(
    r"^(?P<prefix>\s*#{1,6}\s*)第(?P<number>[零一二三四五六七八九十百千万\d]+)\s*"
    r"(?P<kind>编|分编|章|节|小节)\s*(?P<title>.*)$"
)
CHINESE_ARTICLE_HEADING_RE = re.compile(
    r"^\s*#{1,6}\s*第[零一二三四五六七八九十百千万\d]+\s*条\b"
)
GERMAN_SECTION_RE = re.compile(
    r"^(?P<prefix>\s*#{1,6}\s*)Abschnitt\s+(?P<number>\d+[A-Za-z]?)\.?\s*(?P<title>.*)$",
    re.IGNORECASE,
)
GERMAN_SUBSECTION_RE = re.compile(
    r"^(?P<prefix>\s*#{1,6}\s*)Unterabschnitt\s+(?P<number>\d+[A-Za-z]?)\.?\s*(?P<title>.*)$",
    re.IGNORECASE,
)
FOOTNOTE_HEADING_RE = re.compile(r"^\s*#{1,6}\s*(脚注|Footnote|Fußnote|Fussnote)\s*$", re.IGNORECASE)
MARKDOWN_HEADING_RE = re.compile(r"^\s*#{1,6}\s+")

GERMAN_HEADING_TRANSLATIONS = {
    "Sorgfaltspflichten": "尽职义务",
    "Zivilprozess": "民事诉讼",
}


def _normalize_heading_text(body: str) -> str:
    """翻译/归一化仍残留的外文结构标题。"""

    match = APPENDIX_HEADING_RE.match(body)
    if match:
        return f"{match.group('head')}{match.group('tail')}"

    match = GERMAN_SECTION_RE.match(body)
    if match:
        title = match.group("title").strip()
        normalized = f"{match.group('prefix')}第{match.group('number')}节"
        if title:
            normalized += f" {GERMAN_HEADING_TRANSLATIONS.get(title, title)}"
        return normalized

    match = GERMAN_SUBSECTION_RE.match(body)
    if match:
        title = match.group("title").strip()
        # 中文 law_extract 只识别“节”作为法条边界；其他语种小节先降级为兼容边界。
        normalized = f"{match.group('prefix')}第{match.group('number')}节"
        if title:
            normalized += f" {GERMAN_HEADING_TRANSLATIONS.get(title, title)}"
        return normalized

    for source, translated in GERMAN_HEADING_TRANSLATIONS.items():
        pattern = re.compile(rf"^(?P<prefix>\s*#{{1,6}}\s*){re.escape(source)}\s*$", re.IGNORECASE)
        match = pattern.match(body)
        if match:
            return f"{match.group('prefix')}{translated}"

    return body


def _split_line_ending(line: str) -> tuple[str, str]:
    """Split one line into body and original newline."""

    if line.endswith("\r\n"):
        return line[:-2], "\r\n"
    if line.endswith("\n"):
        return line[:-1], "\n"
    if line.endswith("\r"):
        return line[:-1], "\r"
    return line, ""


def _markdown_heading_text(body: str) -> str | None:
    """Return markdown heading text without leading #, or None."""

    if not MARKDOWN_HEADING_RE.match(body):
        return None
    return re.sub(r"^\s*#{1,6}\s*", "", body).strip()


def _is_mergeable_structure_title(body: str) -> bool:
    """Whether a standalone heading can be merged as previous section title."""

    heading_text = _markdown_heading_text(body)
    if not heading_text:
        return False
    if CHINESE_ARTICLE_HEADING_RE.match(body):
        return False
    if APPENDIX_HEADING_RE.match(body):
        return False
    if SECTION_TO_CHINESE_CLAUSE_RE.match(body):
        return False
    if GERMAN_SECTION_RE.match(body) or GERMAN_SUBSECTION_RE.match(body):
        return False
    if CHINESE_STRUCTURE_HEADING_RE.match(body):
        return False
    return True


def _merge_numbered_structure_heading(body: str, next_body: str | None) -> tuple[str, bool]:
    """Merge `第X节` + next standalone heading into `第X节 标题`."""

    match = CHINESE_STRUCTURE_HEADING_RE.match(body)
    if not match:
        return body, False
    if match.group("title").strip():
        return body, False
    if next_body is None:
        return body, False
    normalized_next = _normalize_heading_text(next_body)
    if not _is_mergeable_structure_title(normalized_next):
        return body, False
    title = _markdown_heading_text(normalized_next)
    kind = "节" if match.group("kind") == "小节" else match.group("kind")
    merged = f"{match.group('prefix')}第{match.group('number')}{kind} {title}"
    return merged, True


def _normalize_structure_kind_for_chinese_splitter(body: str) -> str:
    """Convert structure headings unsupported by law_extract into compatible ones."""

    match = CHINESE_STRUCTURE_HEADING_RE.match(body)
    if not match or match.group("kind") != "小节":
        return body
    title = match.group("title").strip()
    normalized = f"{match.group('prefix')}第{match.group('number')}节"
    if title:
        normalized += f" {title}"
    return normalized


def _next_nonempty_body(lines: list[str], start_index: int) -> tuple[str | None, int | None]:
    """Find next non-empty line body from start_index."""

    for index in range(start_index, len(lines)):
        body, _ = _split_line_ending(lines[index])
        if body.strip():
            return body, index
    return None, None


def normalize_translated_text_for_chinese_extraction(text: str) -> str:
    """把保留外文结构编号的译文转换为中文 `law_extract` 可切分的形式。

    当前只做必要归一化：行首 `§ 1 标题` 转为 `第1条 标题`。
    不处理正文内部引用，避免污染引用语义。
    """

    output_lines: list[str] = []
    skipping_footnote = False
    lines = text.splitlines(keepends=True)
    index = 0
    while index < len(lines):
        line = lines[index]
        body, newline = _split_line_ending(line)

        if FOOTNOTE_HEADING_RE.match(body):
            skipping_footnote = True
            index += 1
            continue
        if skipping_footnote and MARKDOWN_HEADING_RE.match(body):
            skipping_footnote = False
        if skipping_footnote:
            index += 1
            continue

        body = _normalize_heading_text(body)
        next_body, next_body_index = _next_nonempty_body(lines, index + 1)
        body, merged_next = _merge_numbered_structure_heading(body, next_body)
        body = _normalize_structure_kind_for_chinese_splitter(body)
        match = SECTION_TO_CHINESE_CLAUSE_RE.match(body)
        if match:
            title = match.group("title").strip()
            normalized = f"{match.group('prefix')}第{match.group('number')}条"
            if title:
                normalized += f" {title}"
            output_lines.append(normalized + newline)
        else:
            output_lines.append(body + newline)
        index = (next_body_index + 1) if merged_next and next_body_index is not None else index + 1
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
