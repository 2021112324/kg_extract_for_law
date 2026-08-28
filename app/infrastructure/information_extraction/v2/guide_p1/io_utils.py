"""Input discovery and stage artifact helpers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

from .config import DEFAULT_CONFIG


class InputDiscoveryError(ValueError):
    """Raised when an explicit input path cannot produce supported files."""


def discover_input_files(
    input_path: str | Path,
    supported_suffixes: Iterable[str] | None = None,
) -> list[Path]:
    source = Path(input_path)
    if not source.exists():
        raise FileNotFoundError(f"分点格式合规指引输入路径不存在: {source}")
    suffixes = {item.lower() for item in (supported_suffixes or DEFAULT_CONFIG.supported_suffixes)}
    if source.is_file():
        if source.suffix.lower() not in suffixes:
            raise InputDiscoveryError(f"不支持的分点格式合规指引文件: {source}")
        return [source]
    files = sorted(
        (path for path in source.rglob("*") if path.is_file() and path.suffix.lower() in suffixes),
        key=lambda item: item.as_posix().lower(),
    )
    if not files:
        raise InputDiscoveryError(f"输入目录中没有受支持的 .txt/.md 文件，且不会回退父目录: {source}")
    return files


def load_text(path: str | Path) -> str:
    source = Path(path)
    try:
        return source.read_text(encoding="utf-8-sig")
    except UnicodeDecodeError:
        return source.read_text(encoding="gb18030")


def save_json(data: Any, path: str | Path) -> str:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(target)


def safe_stem(path_or_name: str | Path) -> str:
    stem = Path(path_or_name).stem
    forbidden = '<>:"/\\|?*'
    cleaned = "".join("_" if char in forbidden or ord(char) < 32 else char for char in stem)
    return cleaned.strip(" .") or "未命名合规指引"


def stage_output_path(input_path: str | Path, output_dir: str | Path, stage: str) -> Path:
    return Path(output_dir) / f"{safe_stem(input_path)}_{stage}.json"
