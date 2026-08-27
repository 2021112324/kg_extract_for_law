"""File and stage-result helpers for V2 Chinese regulation extraction."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


SUPPORTED_SUFFIXES = {".md", ".txt"}


def load_text(path: str | Path) -> str:
    source = Path(path)
    if source.suffix.lower() not in SUPPORTED_SUFFIXES:
        raise ValueError(f"Unsupported Chinese regulation file: {source}")
    return source.read_text(encoding="utf-8", errors="ignore")


def save_json(data: Any, path: str | Path) -> str:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(target)


def output_path(input_path: str | Path, output_dir: str | Path, stage: str) -> Path:
    source = Path(input_path)
    return Path(output_dir) / f"{source.stem}_{stage}.json"

