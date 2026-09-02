"""知识条目来源名称清理与语境前缀工具。"""

from __future__ import annotations

import re
from pathlib import PurePath, PureWindowsPath
from typing import Any


_INVALID_SOURCE_NAMES = {
    "",
    "none",
    "null",
    "unknown",
    "unknown source",
    "未知",
    "未知来源",
    "无",
}
_FILE_SUFFIXES = {
    ".csv",
    ".doc",
    ".docx",
    ".htm",
    ".html",
    ".json",
    ".md",
    ".pdf",
    ".txt",
    ".xml",
}


def clean_source_name(value: Any) -> str:
    """将来源值清理为可展示的文件名称。"""
    if isinstance(value, (list, tuple, set)):
        value = next((item for item in value if item is not None), "")
    text = str(value or "").strip().strip('"\'')
    if not text:
        return ""
    is_windows_path = "\\" in text or bool(re.match(r"^[A-Za-z]:", text))
    path = PureWindowsPath(text) if is_windows_path else PurePath(text)
    is_file_path = is_windows_path or text.startswith("/") or path.suffix.lower() in _FILE_SUFFIXES
    name = path.name if is_file_path else text
    if is_file_path and path.suffix.lower() in _FILE_SUFFIXES:
        name = path.stem
    name = name.strip().strip("《》").strip()
    if name.lower() in _INVALID_SOURCE_NAMES:
        return ""
    return name


def add_chinese_source_context(
    content: str,
    source_name: Any,
    source_location: Any = "",
) -> str:
    """为中文知识添加一次规范文件来源及条款定位前缀。"""
    body = str(content or "").strip()
    name = clean_source_name(source_name)
    location = str(source_location or "").strip()
    if not body:
        return body

    if not name:
        if location and not body.startswith(location):
            return f"{location}规定：{body}"
        return body

    generic_prefix = f"在《{name}》中，"
    prefix = f"在《{name}》{location}中，" if location else generic_prefix
    if body.startswith(prefix):
        return body

    if location and body.startswith(generic_prefix):
        remainder = body[len(generic_prefix):]
        if remainder.startswith(location):
            return body
        return f"{prefix}{remainder}"

    if location and body.startswith(location):
        return f"{generic_prefix}{body}"
    return f"{prefix}{body}"


def add_english_source_context(content: str, source_name: Any) -> str:
    """为英文知识添加一次 ``Under <document>,`` 来源前缀。"""
    body = str(content or "").strip()
    name = clean_source_name(source_name)
    if not body or not name:
        return body
    prefix = f"Under {name}, "
    if body.casefold().startswith(prefix.casefold()):
        return body
    existing = re.match(r"^Under\s+.+?,\s+", body, flags=re.IGNORECASE)
    if existing:
        body = body[existing.end():]
    return f"{prefix}{body}"


__all__ = [
    "add_chinese_source_context",
    "add_english_source_context",
    "clean_source_name",
]
