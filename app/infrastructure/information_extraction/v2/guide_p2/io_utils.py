"""Reuse source-preserving IO; do not duplicate file or quantity parsers."""
from ..rule.io_utils import compact, load_text, redact, save_json, stable_id

__all__ = ["compact", "load_text", "redact", "save_json", "stable_id"]
