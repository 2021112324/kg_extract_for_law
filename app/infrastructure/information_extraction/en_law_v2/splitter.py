"""格式二美国法案 Markdown 的规则切分器。

格式二的稳定主干是美国法案中的 `SECTION` / `SEC.` / `Sec.` 条款。
本切分器只用确定性规则生成 LegalProvision 和一级 ProvisionClause：

1. LegalProvision 对应正文中的正式 SECTION/SEC/Sec 条款。
2. 目录区中的 Sec. 条目只记录为 table_of_contents_items，不入正文条款。
3. ProvisionClause 对应条款下 `(a)`、`(b)`、`(c)` 小写字母 subsection。
4. 若条款中没有 `(a)` 层级，则整个条款正文作为隐式 ProvisionClause。
5. U.S.C. `§` 插入文本只做标记和统计，不在本轮作为主法案条款边界。
"""

from __future__ import annotations

import re
from copy import deepcopy
from typing import Any

from app.infrastructure.information_extraction.en_law_v2.config import (
    FORMAT_TWO_DOCUMENT_FORMAT,
    RISK_TYPE_VALUES,
)


RISK_HEADER_RE = re.compile(r"^\s*合规风险类型\s*[:：]\s*(?P<value>.+?)\s*$")
SECTION_RE = re.compile(
    r"^\s*(?P<md>#{1,6}\s*)?"
    r"(?P<label>SECTION|SEC\.|Sec\.)\s+"
    r"(?P<number>[0-9][0-9A-Za-z_.-]*)\.?"
    r"(?P<tail>.*)$"
)
TOC_HEADING_RE = re.compile(r"\b(TABLE\s+OF\s+CONTENTS|CONTENTS)\b", re.IGNORECASE)
STRUCTURE_PATTERNS = [
    ("title", re.compile(r"^\s*#{0,6}\s*TITLE\s+[A-ZIVXLCDM0-9]+\b.*$", re.IGNORECASE)),
    ("subtitle", re.compile(r"^\s*#{0,6}\s*Subtitle\s+[A-ZIVXLCDM0-9]+\b.*$")),
    ("part", re.compile(r"^\s*#{0,6}\s*PART\s+[A-ZIVXLCDM0-9]+\b.*$", re.IGNORECASE)),
    ("chapter", re.compile(r"^\s*#{0,6}\s*CHAPTER\s+\d+\b.*$", re.IGNORECASE)),
    ("subchapter", re.compile(r"^\s*#{0,6}\s*SUBCHAPTER\s+[A-ZIVXLCDM0-9]+\b.*$", re.IGNORECASE)),
]
SUBSECTION_RE = re.compile(r"(?<![A-Za-z0-9])\((?P<number>[a-z])\)\s+")
USC_SECTION_RE = re.compile(r"^\s*(?:#{1,6}\s*)?[\"“]?§\s*[0-9][0-9A-Za-z_.-]*\b", re.MULTILINE)
AMENDMENT_RE = re.compile(
    r"\b(is|are|shall be|has been|have been)\s+"
    r"(amended|repealed|inserted|struck|stricken|replaced)\b|"
    r"\bis amended\b|\bare amended\b|\bamended by\b|\bby inserting\b|\bby striking\b",
    re.IGNORECASE,
)


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


def match_section(line: str) -> dict[str, str] | None:
    """匹配正式 SECTION/SEC/Sec 候选行。"""

    match = SECTION_RE.match(line)
    if not match:
        return None
    label = match.group("label")
    number = match.group("number").rstrip(".")
    tail = (match.group("tail") or "").strip()
    tail = re.sub(r"^\.\s*", "", tail).strip()
    return {
        "section_number": f"{label} {number}",
        "section_index": number,
        "section_label": label,
        "inline_heading": tail,
        "raw_heading_line": line.strip(),
    }


def match_structure(line: str) -> tuple[str, str] | None:
    """匹配 TITLE/Subtitle/PART 等组织结构标题。"""

    stripped = strip_markdown_heading(line)
    for level, pattern in STRUCTURE_PATTERNS:
        if pattern.match(stripped):
            return level, stripped
    return None


def update_context(context: dict[str, str], level: str, value: str) -> None:
    """更新当前结构上下文。"""

    order = ["title", "subtitle", "part", "chapter", "subchapter"]
    context[level] = value
    if level in order:
        for child in order[order.index(level) + 1 :]:
            context[child] = ""


def _infer_document_title(file_header: str, filename: str = "") -> str:
    """从文件头推断法规标题。"""

    skip_prefixes = ("[", "【", "currency:", "note:", "public law", "chapter", "as amended")
    for raw in file_header.splitlines():
        line = strip_markdown_heading(raw)
        if not line:
            continue
        lower = line.lower()
        if lower.startswith(skip_prefixes):
            continue
        if "table of contents" in lower:
            break
        if len(line) <= 180 and re.search(r"\b(ACT|LAW|CODE|STATUTE)\b", line, re.IGNORECASE):
            return line
    return filename.rsplit(".", 1)[0] if filename else ""


def _infer_public_law_number(file_header: str) -> str:
    match = re.search(r"\bPublic\s+Law\s+\d+[-–]\d+\b|\bP\.L\.\s*\d+[-–]\d+\b", file_header, re.IGNORECASE)
    return match.group(0).strip() if match else ""


def _infer_enactment_date(file_header: str) -> str:
    match = re.search(
        r"\b(?:Enacted|Approved)\s+([A-Z][a-z]+\s+\d{1,2},\s+\d{4})\b|"
        r"\b([A-Z][a-z]+\.\s+\d{1,2},\s+\d{4})\b",
        file_header,
    )
    if not match:
        return ""
    return (match.group(1) or match.group(2) or "").strip()


def _line_looks_like_toc_item(line: str) -> bool:
    """判断 Sec. 候选是否像目录项。"""

    match = match_section(line)
    if not match:
        return False
    if match["section_label"] != "Sec.":
        return False
    tail = match.get("inline_heading") or ""
    return bool(tail) and len(tail) < 160 and not re.search(r"\b(shall|must|may|is|are|means)\b", tail, re.IGNORECASE)


def detect_toc_lines(lines: list[str]) -> set[int]:
    """识别目录区中的 Sec. 行号。"""

    toc_lines: set[int] = set()
    in_toc = False
    consecutive_toc_items = 0
    for index, line in enumerate(lines):
        stripped = strip_markdown_heading(line)
        if TOC_HEADING_RE.search(stripped):
            in_toc = True
            consecutive_toc_items = 0
            continue
        if not in_toc:
            continue
        if _line_looks_like_toc_item(line):
            toc_lines.add(index)
            consecutive_toc_items += 1
            continue
        if not line.strip():
            continue
        if match_structure(line):
            continue
        if consecutive_toc_items >= 2 and match_section(line) and match_section(line)["section_label"] != "Sec.":
            in_toc = False
            consecutive_toc_items = 0
            continue
        if consecutive_toc_items >= 2 and not _line_looks_like_toc_item(line):
            # 目录后常进入标题或正文；遇到非目录实体行后结束目录状态。
            if re.search(r"\bBe it enacted\b|\bSECTION\b|\bSEC\.\b", line):
                in_toc = False
    return toc_lines


def _split_heading_and_content(section_match: dict[str, str], following_lines: list[str]) -> tuple[str, list[str]]:
    """拆分条款标题和正文起始行。"""

    tail = section_match.get("inline_heading") or ""
    if not tail:
        return "", following_lines
    if re.search(r"\b(shall|must|may|means|is|are|amended|repealed|inserted)\b", tail, re.IGNORECASE):
        return "", [tail] + following_lines
    if len(tail.split()) <= 14:
        return tail, following_lines
    return "", [tail] + following_lines


def extract_provision_clauses(section: dict[str, Any]) -> list[dict[str, Any]]:
    """从 SECTION/SEC 条款中抽取一级 `(a)` subsection。"""

    provision_number = str(section.get("article_number") or section.get("section_number") or "").strip()
    content = section.get("content") or ""
    start_line = int(section.get("content_line_start") or section.get("line_start") or 1)
    matches = list(SUBSECTION_RE.finditer(content))
    if not matches:
        stripped = clean_text(content)
        if not stripped:
            return []
        return [{
            "unit_number": provision_number,
            "unit_level": "subsection",
            "unit_content": stripped,
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
        unit_content = clean_text(content[match.end() : next_start])
        if not unit_content:
            continue
        prefix_text = content[: match.start()]
        local_start_line = start_line + prefix_text.count("\n")
        unit_text = content[match.start() : next_start]
        clauses.append({
            "unit_number": f"{provision_number}({local})",
            "unit_level": "subsection",
            "unit_content": clean_text(unit_text),
            "source_article_number": provision_number,
            "clause_index": len(clauses) + 1,
            "explicit_boundary": True,
            "line_start": local_start_line,
            "line_end": local_start_line + max(unit_text.count("\n"), 0),
        })
    return clauses


def split_v2_document(text: str, filename: str = "") -> dict[str, Any]:
    """将格式二美国法案 Markdown 切分成文件头和 SECTION/SEC 条款。"""

    normalized = normalize_newlines(text)
    original_lines = normalized.split("\n")
    risk_metadata, lines = parse_risk_header(original_lines)
    toc_line_indexes = detect_toc_lines(lines)

    context = {"title": "", "subtitle": "", "part": "", "chapter": "", "subchapter": ""}
    sections: list[dict[str, Any]] = []
    toc_items: list[dict[str, Any]] = []
    skipped_section_candidates: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None
    current_lines: list[str] = []
    file_header_end_index = 0

    def finish_section(end_index: int) -> None:
        nonlocal current, current_lines
        if not current:
            return
        heading, body_lines = _split_heading_and_content(current, current_lines)
        content = clean_text("\n".join(body_lines))
        current["article_heading"] = heading
        current["section_heading"] = heading
        current["content"] = content
        current["line_end"] = max(current.get("line_start", 1), end_index + 1)
        current["content_line_start"] = current.get("_content_start_line")
        current["content_line_end"] = current["line_end"]
        current["is_amendment_article"] = bool(AMENDMENT_RE.search(content))
        current["contains_codified_section"] = bool(USC_SECTION_RE.search(content))
        current["provision_clauses"] = extract_provision_clauses(current)
        sections.append(current)
        current = None
        current_lines = []

    for index, line in enumerate(lines):
        section_match = match_section(line)
        if section_match:
            if index in toc_line_indexes:
                toc_items.append({
                    **section_match,
                    "line_start": index + 1,
                    "is_table_of_contents_item": True,
                    "context": deepcopy(context),
                })
                continue
            # 空 Sec. 或页码型标题暂不作为正文条款，记录异常候选。
            if section_match["section_label"] == "Sec." and not section_match.get("inline_heading"):
                skipped_section_candidates.append({
                    **section_match,
                    "line_start": index + 1,
                    "skip_reason": "empty_sec_candidate",
                })
                continue
            finish_section(index - 1)
            if not sections:
                file_header_end_index = index
            current = {
                **section_match,
                "article_number": section_match["section_number"],
                "article_index": section_match["section_index"],
                "raw_heading_line": section_match["raw_heading_line"],
                "line_start": index + 1,
                "_content_start_line": index + 2,
                "classification_context": deepcopy(context),
                **context,
            }
            current_lines = []
            if section_match.get("inline_heading"):
                current_lines.append(section_match["inline_heading"])
            continue

        structure_match = match_structure(line)
        if structure_match and current is None:
            level, value = structure_match
            update_context(context, level, value)
        elif structure_match and current is not None:
            # 条款内部出现 TITLE/PART，通常是修订插入文本，保留为条款内容，不更新外层上下文。
            current_lines.append(line)
            continue

        if current is not None:
            current_lines.append(line)

    finish_section(len(lines) - 1)

    file_header = clean_text("\n".join(lines[:file_header_end_index or len(lines)]))
    title = _infer_document_title(file_header, filename)
    fallback_metadata = {
        "document_name": title,
        "document_type": "Act",
        "document_number": _infer_public_law_number(file_header),
        "publication_date": _infer_enactment_date(file_header),
        "issuing_authority": "United States Congress" if title else "",
    }

    warnings: list[str] = []
    if not risk_metadata:
        warnings.append("missing_compliance_risk_type_header")
    elif not risk_metadata.get("risk_header_valid"):
        warnings.append("unknown_compliance_risk_type")
    if not sections:
        warnings.append("no_formal_section_detected")
    if skipped_section_candidates:
        warnings.append("skipped_empty_sec_candidates")

    amendment_count = sum(1 for item in sections if item.get("is_amendment_article"))
    codified_count = sum(1 for item in sections if item.get("contains_codified_section"))
    if amendment_count:
        warnings.append("contains_amendment_sections")
    if codified_count:
        warnings.append("contains_codified_us_code_sections")

    return {
        **risk_metadata,
        "filename": filename,
        "document_format": FORMAT_TWO_DOCUMENT_FORMAT,
        "file_info": file_header,
        "file_header": file_header,
        "recitals": [],
        "selected_recitals": [],
        "clauses": sections,
        "table_of_contents_items": toc_items,
        "skipped_section_candidates": skipped_section_candidates,
        "annexes": [],
        "annexes_metadata": [],
        "fallback_metadata": fallback_metadata,
        "stats": {
            "section_count": len(sections),
            "table_of_contents_item_count": len(toc_items),
            "skipped_section_candidate_count": len(skipped_section_candidates),
            "amendment_section_count": amendment_count,
            "codified_section_count": codified_count,
            "provision_clause_count": sum(len(item.get("provision_clauses") or []) for item in sections),
        },
        "validation": {
            "is_complete_uniform_candidate": amendment_count == 0 and codified_count == 0,
            "requires_manual_review": bool(amendment_count or codified_count or skipped_section_candidates),
            "review_reasons": [
                reason for reason, enabled in [
                    ("contains_amendment_sections", bool(amendment_count)),
                    ("contains_codified_us_code_sections", bool(codified_count)),
                    ("skipped_empty_sec_candidates", bool(skipped_section_candidates)),
                ] if enabled
            ],
        },
        "warnings": warnings,
    }
