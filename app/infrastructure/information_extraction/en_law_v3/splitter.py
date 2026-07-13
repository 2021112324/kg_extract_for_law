"""格式三美国 CFR/USC `§` 文本的规则切分器。"""

from __future__ import annotations

import re
from typing import Any

from app.infrastructure.information_extraction.en_law_v3.config import (
    FORMAT_THREE_DOCUMENT_FORMAT,
    RISK_TYPE_VALUES,
)


RISK_HEADER_RE = re.compile(r"^\s*合规风险类型\s*[:：]\s*(?P<value>.+?)\s*$")
SECTION_HEADING_RE = re.compile(
    r"^\s*#{1,6}\s*§\s*(?P<number>[0-9][0-9A-Za-z_.-]*)\.?\s*(?P<tail>.*)$"
)
RESERVED_SECTION_RE = re.compile(r"^\s*#{0,6}\s*§{1,2}\s*[\dA-Za-z_.-]+(?:\s*[-–]\s*[\dA-Za-z_.-]+)?\s*\[Reserved\]\s*$", re.IGNORECASE)
SUPPLEMENT_RE = re.compile(r"^\s*#{1,6}\s*Supplement\s+No\.\s+\d+\s+to\s+Part\s+\d+\b.*$", re.IGNORECASE)
EDITORIAL_RE = re.compile(
    r"^\s*#{0,6}\s*(Editorial\s+Notes|EDITORIAL\s+NOTES|AMENDMENTS|CODIFICATION|PRIOR\s+PROVISIONS|REFERENCES\s+IN\s+TEXT)\b",
    re.IGNORECASE,
)
PART_RE = re.compile(r"^\s*#{0,6}\s*PART\s+(?P<number>[0-9A-Za-z_.-]+)\s*[—–-]?\s*(?P<name>.*)$", re.IGNORECASE)
SUBPART_RE = re.compile(r"^\s*#{0,6}\s*Subpart\s+(?P<number>[A-Z])\s*[—–-]?\s*(?P<name>.*)$", re.IGNORECASE)
TITLE_RE = re.compile(r"^\s*#{0,6}\s*(?:Title|TITLE)\s+(?P<number>[0-9A-ZIVXLCDM]+)\s*[—–-]?\s*(?P<name>.*)$", re.IGNORECASE)
SUBTITLE_RE = re.compile(r"^\s*#{0,6}\s*Subtitle\s+(?P<number>[0-9A-ZIVXLCDM]+)\s*[—–-]?\s*(?P<name>.*)$", re.IGNORECASE)
CHAPTER_RE = re.compile(r"^\s*#{0,6}\s*(?:Chapter|CHAPTER)\s+(?P<number>[0-9A-ZIVXLCDM]+)\s*[—–-]?\s*(?P<name>.*)$", re.IGNORECASE)
SUBCHAPTER_RE = re.compile(r"^\s*#{0,6}\s*Subchapter\s+(?P<number>[0-9A-ZIVXLCDM]+)\s*[—–-]?\s*(?P<name>.*)$", re.IGNORECASE)
AUTHORITY_RE = re.compile(r"^\s*Authority:\s*(?P<value>.+)$", re.IGNORECASE)
SOURCE_RE = re.compile(r"^\s*Source:\s*(?P<value>.+)$", re.IGNORECASE)
SUBSECTION_RE = re.compile(r"(?m)^\s*\((?P<number>[a-z])\)\s+")
NON_BODY_HEADING_RE = re.compile(
    r"^\s*#{1,6}\s*("
    r"Editorial\s+Notes|"
    r"AMENDMENTS|"
    r"CODIFICATION|"
    r"PRIOR\s+PROVISIONS|"
    r"REFERENCES\s+IN\s+TEXT|"
    r"HISTORICAL\s+AND\s+REVISION\s+NOTES|"
    r"LEGISLATIVE\s+STATEMENTS|"
    r"SENATE\s+REPORT\s+NO\.\s+[\d-]+|"
    r"HOUSE\s+REPORT\s+NO\.\s+[\d-]+|"
    r"STATUTORY\s+NOTES\s+AND\s+RELATED\s+SUBSIDIARIES|"
    r"EFFECTIVE\s+DATE(?:\s+OF\s+.+)?|"
    r"ADJUSTMENT\s+OF\s+DOLLAR\s+AMOUNTS"
    r")\b",
    re.IGNORECASE,
)
HTML_TABLE_RE = re.compile(r"<table\b.*?</table>", re.IGNORECASE | re.DOTALL)
IMAGE_RE = re.compile(r"!\[[^\]]*\]\([^)]+\)")


def normalize_newlines(text: str) -> str:
    """统一换行符。"""

    return str(text or "").replace("\r\n", "\n").replace("\r", "\n")


def clean_text(text: str) -> str:
    """清理多余空白，同时保留段落结构。"""

    value = normalize_newlines(text)
    value = re.sub(r"[ \t]+$", "", value, flags=re.MULTILINE)
    value = re.sub(r"\n{3,}", "\n\n", value)
    return value.strip()


def strip_markdown_heading(line: str) -> str:
    """去掉 Markdown 标题前缀。"""

    return re.sub(r"^\s*#{1,6}\s*", "", str(line or "").strip()).strip()


def parse_risk_header(lines: list[str]) -> tuple[dict[str, Any], list[str]]:
    """解析并移除文首合规风险类型。"""

    for index, line in enumerate(lines):
        if not line.strip():
            continue
        match = RISK_HEADER_RE.match(line)
        if not match:
            return {}, lines
        value = match.group("value").strip()
        return {
            "compliance_risk_type": value,
            "risk_header_line": line.strip(),
            "risk_header_line_number": index + 1,
            "risk_header_valid": value in RISK_TYPE_VALUES,
        }, lines[:index] + lines[index + 1 :]
    return {}, lines


def _parse_context_heading(line: str) -> tuple[str, str, str] | None:
    """识别 Title/Chapter/Part/Subpart 等层级标题。"""

    stripped = strip_markdown_heading(line)
    for level, pattern in (
        ("title", TITLE_RE),
        ("subtitle", SUBTITLE_RE),
        ("chapter", CHAPTER_RE),
        ("subchapter", SUBCHAPTER_RE),
        ("part", PART_RE),
        ("subpart", SUBPART_RE),
    ):
        match = pattern.match(stripped)
        if match:
            number = match.group("number").strip()
            name = (match.group("name") or "").strip()
            label = f"{level.capitalize()} {number}" if level != "part" else f"Part {number}"
            if level == "subpart":
                label = f"Subpart {number}"
            return level, label, name
    return None


def _update_context(context: dict[str, str], level: str, label: str, name: str) -> None:
    """更新层级上下文，并清空下级上下文。"""

    order = ["title", "subtitle", "chapter", "subchapter", "part", "subpart"]
    context[level] = label
    context[f"{level}_name"] = name
    if level in order:
        for child in order[order.index(level) + 1 :]:
            context[child] = ""
            context[f"{child}_name"] = ""


def _infer_document_title(file_header: str, filename: str = "") -> str:
    """从文件头推断文件级标题。"""

    for raw in file_header.splitlines():
        line = strip_markdown_heading(raw)
        if not line:
            continue
        lower = line.lower()
        if lower.startswith(("subtitle ", "chapter ", "subchapter ", "authority:", "source:")):
            continue
        if RESERVED_SECTION_RE.match(line):
            continue
        if len(line) <= 180:
            return line
    return filename.rsplit(".", 1)[0] if filename else ""


def _detect_document_type(filename: str, header: str, context: dict[str, str]) -> str:
    """粗略识别格式三文件类型。"""

    name = filename.lower()
    text = header.lower()
    if "itar" in name or "international traffic in arms regulations" in text:
        return "ITAR_Part"
    if "cfr" in name or context.get("part"):
        return "CFR_Part"
    if "u.s.c" in text or "united states code" in text or "usc" in name:
        return "USC_Compilation"
    if "delaware" in name:
        return "State_Law"
    return "US_Code_Section_Document"


def _match_section_heading(line: str) -> dict[str, str] | None:
    """匹配正式 `## § n` 条款标题。"""

    if RESERVED_SECTION_RE.match(line):
        return None
    match = SECTION_HEADING_RE.match(line)
    if not match:
        return None
    number = match.group("number").rstrip(".")
    tail = (match.group("tail") or "").strip()
    if re.search(r"\[Reserved\]", tail, re.IGNORECASE):
        return None
    heading = re.sub(r"^\.\s*", "", tail).strip()
    return {
        "article_number": f"§ {number}",
        "section_number": f"§ {number}",
        "section_index": number,
        "provision_full_ref": f"§ {number}",
        "inline_heading": heading,
        "raw_heading_line": line.strip(),
    }


def _next_nonblank_line(lines: list[str], start_index: int) -> str:
    """获取指定位置之后的下一行非空文本。"""

    for value in lines[start_index + 1:]:
        if str(value or "").strip():
            return value
    return ""


def _has_same_titled_section_ahead(lines: list[str], start_index: int, article_number: str, window: int = 120) -> bool:
    """判断无标题 Section 行后方是否存在同编号正式标题行。"""

    end_index = min(len(lines), start_index + window + 1)
    for value in lines[start_index + 1:end_index]:
        match = _match_section_heading(value)
        if not match:
            continue
        if match["article_number"] == article_number and match.get("inline_heading"):
            return True
        if match["article_number"] != article_number and match.get("inline_heading"):
            return False
    return False


def _is_non_body_boundary(line: str) -> bool:
    """判断是否遇到 Supplement 或编纂附注等非正文区域边界。"""

    return bool(SUPPLEMENT_RE.match(line) or EDITORIAL_RE.match(line) or NON_BODY_HEADING_RE.match(line))


def _remove_non_body_blocks(text: str) -> tuple[str, list[str]]:
    """移除不适合直接作为条文正文的 HTML 表格和图片块。"""

    warnings = []
    table_count = len(HTML_TABLE_RE.findall(text))
    if table_count:
        warnings.append(f"removed_html_tables:{table_count}")
        text = HTML_TABLE_RE.sub("\n", text)
    image_count = len(IMAGE_RE.findall(text))
    if image_count:
        warnings.append(f"removed_markdown_images:{image_count}")
        text = IMAGE_RE.sub("", text)
    return text, warnings


def clean_for_llm_input(text: str) -> str:
    """生成供大模型抽取使用的清洗文本，不改变正式入库的原始文本。"""

    cleaned, _warnings = _remove_non_body_blocks(text)
    return clean_text(cleaned)


def _clause_dedupe_key(clause: dict[str, Any]) -> tuple[str, str, str, str, str, str]:
    """生成同一法规上下文下 Section 去重键。"""

    context = clause.get("classification_context") or {}
    if not isinstance(context, dict):
        context = {}
    return (
        str(context.get("title") or ""),
        str(context.get("chapter") or ""),
        str(context.get("part") or ""),
        str(context.get("subpart") or ""),
        str(clause.get("article_number") or ""),
        str(clause.get("section_index") or ""),
    )


def _dedupe_duplicate_clauses(clauses: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    """删除同一上下文下重复的 Section，优先保留更像正文的条目。"""

    selected: dict[tuple[str, str, str, str, str, str], dict[str, Any]] = {}
    order: list[tuple[str, str, str, str, str, str]] = []
    dropped = 0
    for clause in clauses:
        key = _clause_dedupe_key(clause)
        existing = selected.get(key)
        if existing is None:
            selected[key] = clause
            order.append(key)
            continue
        dropped += 1
        existing_score = (
            1 if existing.get("article_heading") else 0,
            len(str(existing.get("content") or "")),
        )
        current_score = (
            1 if clause.get("article_heading") else 0,
            len(str(clause.get("content") or "")),
        )
        if current_score > existing_score:
            selected[key] = clause
    return [selected[key] for key in order], dropped


def _first_level_subsection_matches(content: str) -> list[re.Match[str]]:
    """只保留从 `(a)` 开始连续出现的行首一级 subsection 边界。"""

    candidates = list(SUBSECTION_RE.finditer(content or ""))
    if not candidates:
        return []

    matches: list[re.Match[str]] = []
    expected = ord("a")
    started = False
    for match in candidates:
        local = match.group("number")
        current = ord(local)
        if not started:
            if local != "a":
                continue
            started = True
            matches.append(match)
            expected = ord("b")
            continue
        if current == expected:
            matches.append(match)
            expected += 1
            continue
        if current < expected:
            # 已接受过的编号再次出现，通常是 notes 或嵌套说明，不作为同级边界。
            continue
        # 从已开始的一级序列跳号到更深层罗马编号，后续不再扩展一级边界。
        break
    return matches


def extract_provision_clauses(section: dict[str, Any]) -> list[dict[str, Any]]:
    """从 `§` 条款中抽取一级 `(a)/(b)/(c)` subsection。"""

    provision_number = str(section.get("article_number") or "").strip()
    content = section.get("content") or ""
    start_line = int(section.get("content_line_start") or section.get("line_start") or 1)
    matches = _first_level_subsection_matches(content)
    if not matches:
        stripped = clean_text(content)
        if not stripped:
            return []
        return [{
            "unit_number": provision_number,
            "unit_level": "subsection",
            "unit_content": content,
            "unit_content_clean": clean_for_llm_input(content),
            "source_article_number": provision_number,
            "clause_index": 1,
            "explicit_boundary": False,
            "line_start": start_line,
            "line_end": start_line + max(len(content.splitlines()), 1) - 1,
        }]

    clauses: list[dict[str, Any]] = []
    for index, match in enumerate(matches):
        next_start = matches[index + 1].start() if index + 1 < len(matches) else len(content)
        local = match.group("number")
        prefix_text = content[: match.start()]
        local_start_line = start_line + prefix_text.count("\n")
        unit_text = content[match.start() : next_start]
        if not clean_text(unit_text):
            continue
        clauses.append({
            "unit_number": f"{provision_number}({local})",
            "unit_level": "subsection",
            "unit_content": unit_text,
            "unit_content_clean": clean_for_llm_input(unit_text),
            "source_article_number": provision_number,
            "clause_index": len(clauses) + 1,
            "explicit_boundary": True,
            "line_start": local_start_line,
            "line_end": local_start_line + max(unit_text.count("\n"), 0),
        })
    return clauses


def split_v3_document(text: str, filename: str = "") -> dict[str, Any]:
    """按格式三 `§` 正式条款结构切分法规文本。"""

    normalized_text = normalize_newlines(text)
    _cleaned_text_for_warning, cleanup_warnings = _remove_non_body_blocks(normalized_text)
    lines = normalized_text.split("\n")
    risk_info, body_lines = parse_risk_header(lines)
    clauses: list[dict[str, Any]] = []
    context = {
        "title": "",
        "title_name": "",
        "subtitle": "",
        "subtitle_name": "",
        "chapter": "",
        "chapter_name": "",
        "subchapter": "",
        "subchapter_name": "",
        "part": "",
        "part_name": "",
        "subpart": "",
        "subpart_name": "",
    }
    file_header_lines: list[str] = []
    reserved_sections: list[dict[str, Any]] = []
    supplements: list[dict[str, Any]] = []
    authority_lines: list[str] = []
    source_lines: list[str] = []
    current: dict[str, Any] | None = None
    current_content: list[tuple[int, str]] = []
    in_non_body = False

    def close_current(end_line: int) -> None:
        nonlocal current, current_content
        if current is None:
            return
        content = "\n".join(line for _line_no, line in current_content)
        content_clean = clean_for_llm_input(content)
        current["content"] = content
        current["provision_content"] = content
        current["content_clean"] = content_clean
        current["provision_content_clean"] = content_clean
        current["content_line_start"] = current_content[0][0] if current_content else current["line_start"]
        current["content_line_end"] = current_content[-1][0] if current_content else current["line_start"]
        current["line_end"] = end_line
        current["provision_clauses"] = extract_provision_clauses(current)
        clauses.append(current)
        current = None
        current_content = []

    for offset, line in enumerate(body_lines):
        line_no = offset + 1
        if RESERVED_SECTION_RE.match(line):
            close_current(line_no - 1)
            reserved_sections.append({"line": line_no, "text": line.strip()})
            continue
        context_match = _parse_context_heading(line)
        if context_match:
            level, label, name = context_match
            if level in {"part", "subpart"}:
                in_non_body = False
            _update_context(context, level, label, name)
            if current is None:
                file_header_lines.append(line)
            else:
                current_content.append((line_no, line))
            continue
        authority_match = AUTHORITY_RE.match(line)
        if authority_match:
            authority_lines.append(authority_match.group("value").strip())
        source_match = SOURCE_RE.match(line)
        if source_match:
            source_lines.append(source_match.group("value").strip())

        section_match = _match_section_heading(line)
        if section_match:
            if current is not None and section_match["article_number"] == current.get("article_number"):
                current_content.append((line_no, line))
                continue
            if (
                not section_match.get("inline_heading")
                and _has_same_titled_section_ahead(body_lines, offset, section_match["article_number"])
            ):
                if current is None:
                    file_header_lines.append(line)
                else:
                    current_content.append((line_no, line))
                continue
            if (
                current is None
                and not section_match.get("inline_heading")
                and (
                    next_section_match := _match_section_heading(_next_nonblank_line(body_lines, offset))
                )
                and next_section_match["article_number"] == section_match["article_number"]
                and next_section_match.get("inline_heading")
            ):
                file_header_lines.append(line)
                continue
            close_current(line_no - 1)
            in_non_body = False
            current = {
                **section_match,
                "article_number": section_match["article_number"],
                "article_heading": section_match["inline_heading"],
                "section_heading": section_match["inline_heading"],
                "line_start": line_no,
                "classification_context": dict(context),
                "is_amendment_article": False,
            }
            continue

        if _is_non_body_boundary(line):
            close_current(line_no - 1)
            in_non_body = True
            if SUPPLEMENT_RE.match(line):
                supplements.append({"line": line_no, "heading": strip_markdown_heading(line)})
            continue

        if current is None:
            file_header_lines.append(line)
        elif not in_non_body:
            current_content.append((line_no, line))

    close_current(len(body_lines))
    clauses, duplicate_clause_count = _dedupe_duplicate_clauses(clauses)
    file_header = clean_text("\n".join(file_header_lines))
    fallback_metadata = {
        "document_name": _infer_document_title(file_header, filename),
        "document_type": _detect_document_type(filename, file_header, context),
        "document_number": "",
        "legal_basis_text": "; ".join(authority_lines),
        "source_text": "; ".join(source_lines),
        **{key: value for key, value in context.items() if value},
    }
    warnings = list(cleanup_warnings)
    if reserved_sections:
        warnings.append(f"reserved_sections_skipped:{len(reserved_sections)}")
    if supplements:
        warnings.append(f"supplements_detected:{len(supplements)}")
    if duplicate_clause_count:
        warnings.append(f"duplicate_sections_dropped:{duplicate_clause_count}")
    stats = {
        "section_count": len(clauses),
        "provision_clause_count": sum(len(item.get("provision_clauses") or []) for item in clauses),
        "reserved_section_count": len(reserved_sections),
        "supplement_count": len(supplements),
        "warning_count": len(warnings),
    }
    validation = {
        "has_sections": bool(clauses),
        "requires_manual_review": not clauses or bool(supplements),
        "review_reasons": (
            ([] if clauses else ["no_section_detected"])
            + (["supplements_detected"] if supplements else [])
        ),
    }
    return {
        "filename": filename,
        "document_format": FORMAT_THREE_DOCUMENT_FORMAT,
        "compliance_risk_type": risk_info.get("compliance_risk_type", ""),
        "risk_header_line": risk_info.get("risk_header_line", ""),
        "file_header": file_header,
        "fallback_metadata": fallback_metadata,
        "clauses": clauses,
        "reserved_sections": reserved_sections,
        "supplements_metadata": supplements,
        "annexes_metadata": supplements,
        "recitals": [],
        "selected_recitals": [],
        "stats": stats,
        "validation": validation,
        "warnings": warnings,
    }


def split_format_three_document(text: str, filename: str = "") -> dict[str, Any]:
    """兼容命名的格式三切分入口。"""

    return split_v3_document(text, filename=filename)
