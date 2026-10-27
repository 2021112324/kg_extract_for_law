"""Source-preserving IO and deterministic identities."""
from hashlib import sha256
import json
from pathlib import Path
import re


def stable_id(prefix, *parts):
    value = "\x1f".join(str(p) for p in parts)
    return prefix + "_" + sha256(value.encode("utf-8")).hexdigest()[:28]


def compact(value):
    return re.sub(r"\s+", "", str(value or ""))


def load_text(path):
    path = Path(path)
    if path.suffix.lower() not in {".txt", ".md"}:
        raise ValueError("rule only supports .txt and .md")
    text = path.read_bytes().decode("utf-8-sig")
    if not text.strip() or "\x00" in text:
        raise ValueError("Input must be nonempty UTF-8 text")
    return text


def save_json(value, path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)
    return str(path)


def redact(value, secrets=()):
    text = str(value)
    for secret in secrets:
        if secret:
            text = text.replace(secret, "[REDACTED]")
    text = re.sub(r"(?i)(bearer\s+|api[_-]?key[=:]\s*)[^\s,;]+", r"\1[REDACTED]", text)
    return text[:2000]
