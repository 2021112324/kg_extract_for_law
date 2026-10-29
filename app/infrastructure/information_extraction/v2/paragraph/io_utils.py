"""Existing identities/atomic JSON plus streaming, redacted local audit."""
import json
import logging
from pathlib import Path
from ..rule.io_utils import compact, redact, stable_id
from app.infrastructure.risk_map.common.io import atomic_json as save_json
from app.infrastructure.graph_storage.diagnostics import redact_diagnostics


class RedactedFormatter(logging.Formatter):
    def __init__(self, secrets=()):
        super().__init__("%(asctime)s %(levelname)s %(name)s %(message)s")
        self.secrets = tuple(s for s in secrets if s)

    def format(self, record):
        return redact_diagnostics(super().format(record), self.secrets)


def redact_tree(value, secrets=()):
    if isinstance(value, str):
        for secret in secrets:
            if secret:
                value = value.replace(secret, "[REDACTED]")
        return value
    if isinstance(value, dict):
        return {key: redact_tree(item, secrets) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [redact_tree(item, secrets) for item in value]
    return value


def append_audit(path, record, secrets=()):
    if path is None:
        return
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    from ..guide_p1.validator import to_plain
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(redact_tree(to_plain(record), secrets), ensure_ascii=False) + "\n")
        stream.flush()


def quote_range(quote, text, offset=0):
    """Resolve a whitespace-normalized quote to its actual source characters."""
    if not isinstance(quote, str) or not compact(quote):
        raise ValueError("Evidence must be a nonempty source quote")
    positions = [i for i, char in enumerate(text) if not char.isspace() and char != "\ufeff"]
    normalized = "".join(text[i] for i in positions)
    key = compact(quote).replace("\ufeff", "")
    start = normalized.find(key)
    if start < 0:
        raise ValueError("Evidence is outside its authorized source")
    if normalized.find(key, start + 1) >= 0:
        raise ValueError("Evidence is ambiguous; return a longer unique quote")
    return {"start": offset + positions[start], "end": offset + positions[start + len(key) - 1] + 1}


def supported(quote, source):
    return isinstance(quote, str) and bool(compact(quote)) and compact(quote) in compact(source)
