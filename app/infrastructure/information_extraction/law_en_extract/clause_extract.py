"""
English legal clause splitter.

This module implements task 1 for the English legal extractor: split foreign
legal texts into document header information and section-level provisions by
using only stable structural patterns. It intentionally does not infer open
semantic information such as legal basis, purpose, obligations, or subjects;
those belong to later LLM-based extraction steps.
"""

import json
import os
import re
from copy import deepcopy
from typing import Optional


HIERARCHY_LEVELS = [
    "title",
    "subtitle",
    "chapter",
    "subchapter",
    "part",
    "subpart",
]


# Foreign legal materials in the current data set mainly use two styles:
# 1. U.S.C./Act sections: "§ 1701. ..." or "SEC. 1701. ..."
# 2. CFR sections: "§ 123.1 ..."
# The expressions below only identify section-level numbering, not legal
# meaning. Parenthesized subsection references such as "§ 126.9(b) of this
# subchapter" are intentionally excluded because they are cross-references,
# not standalone section headings.
SECTION_PATTERNS = [
    re.compile(
        r"^(?P<label>§\s*§|§§)\s*"
        r"(?P<number>[0-9A-Za-z](?:[0-9A-Za-z.\-–—]*[0-9A-Za-z])?)"
        r"\s+(?P<heading>.+)$",
        re.IGNORECASE,
    ),
    re.compile(
        r"^(?P<label>§)\s*"
        r"(?P<number>[0-9A-Za-z](?:[0-9A-Za-z.\-–—]*[0-9A-Za-z])?)"
        r"(?:\s*[.\-–—]\s+|\s+)"
        r"(?P<heading>.+)$",
        re.IGNORECASE,
    ),
    re.compile(
        r"^(?P<label>Sec\.|SEC\.)\s*"
        r"(?P<number>[0-9A-Za-z](?:[0-9A-Za-z.\-–—]*[0-9A-Za-z])?)"
        r"(?:\s*[.\-–—]\s+|\s+)"
        r"(?P<heading>.+)$",
        re.IGNORECASE,
    ),
]


HIERARCHY_PATTERNS = [
    ("title", re.compile(r"^(TITLE|Title)\s+.+$", re.IGNORECASE)),
    ("subtitle", re.compile(r"^Subtitle\s+.+$", re.IGNORECASE)),
    ("chapter", re.compile(r"^Chapter\s+.+$", re.IGNORECASE)),
    ("subchapter", re.compile(r"^Subchapter\s+.+$", re.IGNORECASE)),
    ("subpart", re.compile(r"^Subpart\s+.+$", re.IGNORECASE)),
    ("part", re.compile(r"^(PART|Part)\s+.+$", re.IGNORECASE)),
]

END_MARKERS = (
    "Appendix",
    "Attachment",
    "Annex",
    "Table of Contents",
    "Editorial Notes",
    "Effective Date Note",
    "Historical and Statutory Notes",
    "List of Subjects",
    "Supplementary Information",
)


def clean_line(line: str) -> str:
    """Normalize one physical line while preserving legal text content."""
    return str(line or "").strip().lstrip(" \t\r\n\f\v#-*")


def clean_text(text: str) -> str:
    """Normalize whitespace for JSON output without changing wording."""
    if text is None:
        return ""
    cleaned = str(text).replace("\r\n", "\n").replace("\r", "\n")
    cleaned = re.sub(r"[ \t]+$", "", cleaned, flags=re.MULTILINE)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned.strip()


def normalize_key(value: str) -> str:
    """Build a loose key used only for duplicate table-of-contents detection."""
    normalized = str(value or "").lower()
    normalized = normalized.replace("§", " section ")
    normalized = re.sub(r"\b(sec|section)\.?\b", "section", normalized)
    normalized = re.sub(r"[^a-z0-9]+", " ", normalized)
    return re.sub(r"\s+", " ", normalized).strip()


def match_section(line: str) -> Optional[dict]:
    """Return normalized section metadata when a line starts a legal provision."""
    cleaned = clean_line(line)
    for pattern in SECTION_PATTERNS:
        match = pattern.match(cleaned)
        if not match:
            continue
        label = re.sub(r"\s+", "", match.group("label"))
        number = match.group("number").strip()
        heading = clean_text(match.group("heading"))
        if re.match(r"^\([A-Za-z0-9]+\)(?:\s|$)", heading):
            return None
        if label.startswith("§§") or label == "§§":
            section_number = f"§§ {number}"
            section_type = "section_range"
        elif label == "§":
            section_number = f"§ {number}"
            section_type = "section"
        else:
            normalized_label = "Section" if label.lower().startswith("section") else "Sec."
            section_number = f"{normalized_label} {number}"
            section_type = "section"
        return {
            "section_number": section_number,
            "section_key": normalize_key(number),
            "section_heading": heading,
            "section_type": section_type,
            "raw_heading_line": cleaned,
        }
    return None


def match_hierarchy(line: str) -> Optional[tuple[str, str]]:
    """Identify Title/Chapter/Subchapter/Part/Subpart headings."""
    cleaned = clean_line(line)
    for level, pattern in HIERARCHY_PATTERNS:
        if pattern.match(cleaned):
            return level, cleaned
    return None


def update_hierarchy(hierarchy: dict, level: str, value: str):
    """Update current hierarchy and clear lower levels."""
    hierarchy[level] = value
    level_index = HIERARCHY_LEVELS.index(level)
    for child in HIERARCHY_LEVELS[level_index + 1:]:
        hierarchy[child] = ""

    # Some CFR table-of-contents lines combine Part and Subpart:
    # "Part 120 Purpose and Definitions Subpart A General Information".
    if level == "part":
        subpart_match = re.search(r"\bSubpart\s+[A-Z0-9]+.+$", value, flags=re.IGNORECASE)
        if subpart_match:
            hierarchy["part"] = value[:subpart_match.start()].strip()
            hierarchy["subpart"] = subpart_match.group(0).strip()


def find_section_matches(lines: list[str]) -> list[dict]:
    """Collect all section-like headings with their line positions."""
    matches = []
    for index, line in enumerate(lines):
        section = match_section(line)
        if section:
            section["line_index"] = index
            matches.append(section)
    return matches


def find_body_start(section_matches: list[dict]) -> int:
    """
    Locate the first real body section.

    CFR and public-law texts often start with a table of contents whose section
    lines are repeated later in the body. If repeated numbers exist, the first
    second occurrence is treated as the body start. If not, the first section is
    already body text, as in standalone U.S.C. extracts.
    """
    if not section_matches:
        raise ValueError("No English legal section heading found in text")

    seen = set()
    duplicate_starts = []
    for section in section_matches:
        key = section["section_key"]
        if key in seen:
            duplicate_starts.append(section["line_index"])
        seen.add(key)
    return min(duplicate_starts) if duplicate_starts else section_matches[0]["line_index"]


def infer_document_type(file_info: str, clauses: list[dict]) -> str:
    """Infer a broad structural type from stable headings."""
    info = file_info or ""
    if re.search(r"\bCFR\b|Code of Federal Regulations|^Title\s+\d+\s+[—-]", info, re.IGNORECASE | re.MULTILINE):
        return "cfr_part"
    if any(clause.get("section_number", "").startswith("Sec.") for clause in clauses):
        return "public_law"
    if any(clause.get("section_number", "").startswith("§") for clause in clauses):
        return "usc_or_statute"
    return "unknown"


def split_heading_from_content(section_number: str, heading_and_content: str) -> str:
    """
    Best-effort section heading extraction.

    The full first-line text is still kept in clause_content. This helper only
    fills section_heading for convenience. CFR headings commonly end with a
    period before body text starts on the same line.
    """
    content = clean_text(heading_and_content)
    if not content:
        return ""

    if re.match(r"^§\s+\d+\.\d+", section_number):
        sentence_match = re.match(r"^(.+?\.)\s+[A-Z0-9(]", content)
        if sentence_match:
            return sentence_match.group(1).strip()
    return content


def build_clause(section: dict, hierarchy: dict, content_lines: list[str], start_line: int, end_line: int) -> dict:
    """Assemble one section-level clause record."""
    raw_content = "\n".join(content_lines)
    clause_content = clean_text(raw_content)
    section_heading = split_heading_from_content(
        section["section_number"],
        section.get("section_heading", ""),
    )
    classification = {key: value for key, value in hierarchy.items() if value}
    return {
        "title": hierarchy.get("title", ""),
        "subtitle": hierarchy.get("subtitle", ""),
        "chapter": hierarchy.get("chapter", ""),
        "subchapter": hierarchy.get("subchapter", ""),
        "part": hierarchy.get("part", ""),
        "subpart": hierarchy.get("subpart", ""),
        "classification": classification,
        "section_number": section["section_number"],
        "section_heading": section_heading,
        "section_type": section.get("section_type", "section"),
        "raw_heading_line": section.get("raw_heading_line", ""),
        "clause_content": clause_content,
        "source_start_line": start_line + 1,
        "source_end_line": end_line + 1,
    }


def split_clause(text: str) -> dict:
    """
    Split an English legal document into document header information and clauses.

    Return shape:
    {
        "file_info": "...",
        "document_type": "cfr_part | public_law | usc_or_statute | unknown",
        "clauses": [
            {
                "title": "...",
                "chapter": "...",
                "part": "...",
                "section_number": "§ 123.1",
                "section_heading": "Requirement for export or temporary import licenses.",
                "clause_content": "Requirement ...\\n(a) ...",
            }
        ]
    }
    """
    if not text or not str(text).strip():
        raise ValueError("English legal text is empty")

    lines = str(text).replace("\r\n", "\n").replace("\r", "\n").split("\n")
    section_matches = find_section_matches(lines)
    body_start = find_body_start(section_matches)

    hierarchy = {level: "" for level in HIERARCHY_LEVELS}
    clauses = []
    current_section = None
    current_hierarchy = deepcopy(hierarchy)
    current_lines = []
    current_start = body_start
    tail_info = ""

    def append_current(end_index: int):
        nonlocal current_section, current_lines, current_hierarchy, current_start
        if not current_section:
            return
        clauses.append(
            build_clause(
                section=current_section,
                hierarchy=current_hierarchy,
                content_lines=current_lines,
                start_line=current_start,
                end_line=max(current_start, end_index),
            )
        )
        current_section = None
        current_lines = []

    for index, raw_line in enumerate(lines):
        line = clean_line(raw_line)
        if not line:
            if current_section:
                current_lines.append("")
            continue

        hierarchy_match = match_hierarchy(line)
        if hierarchy_match:
            if index >= body_start and current_section:
                append_current(index - 1)
            level, value = hierarchy_match
            update_hierarchy(hierarchy, level, value)
            continue

        if index < body_start:
            continue

        if any(line.startswith(marker) for marker in END_MARKERS):
            append_current(index - 1)
            tail_info = clean_text("\n".join(lines[index:]))
            break

        section = match_section(line)
        if section:
            append_current(index - 1)
            current_section = section
            current_hierarchy = deepcopy(hierarchy)
            current_start = index
            current_lines = [section["section_heading"]]
            continue

        if current_section:
            current_lines.append(raw_line.strip())

    append_current(len(lines) - 1)

    if not clauses:
        raise ValueError("No English legal clauses were extracted")

    file_info = clean_text("\n".join(lines[:body_start]))
    return {
        "file_info": file_info,
        "tail_info": tail_info,
        "document_type": infer_document_type(file_info, clauses),
        "clauses": clauses,
    }


def save_clause_result(result: dict, output_dir: str, filename: str) -> str:
    """Save split result for debugging or offline inspection."""
    os.makedirs(output_dir, exist_ok=True)
    base_name = os.path.splitext(os.path.basename(filename))[0]
    output_path = os.path.join(output_dir, f"{base_name}_clauses.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    return output_path


def split_clause_file(input_path: str, output_dir: str = os.path.join("data", "temp")) -> tuple[dict, str]:
    """Read a local English legal text file, split it, and save JSON output."""
    with open(input_path, "r", encoding="utf-8") as f:
        text = f.read()
    result = split_clause(text)
    output_path = save_clause_result(result, output_dir, input_path)
    return result, output_path


class ClauseEnExtractor:
    """Small wrapper matching the extractor-style call used by later tasks."""

    async def split_clause(self, text: str) -> dict:
        return split_clause(text)

    async def split_clause_to_json(
            self,
            input_path: str,
            output_dir: str = os.path.join("data", "temp"),
    ) -> dict:
        result, output_path = split_clause_file(input_path, output_dir)
        return {
            "output_path": output_path,
            **result,
        }
