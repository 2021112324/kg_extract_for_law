"""Shared JSON I/O and hashes; no extraction or model dependencies."""

import hashlib
import json
import os
from pathlib import Path
import tempfile

from .models import RiskMapInputError


def content_hash(value) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                     separators=(",", ":")).encode("utf-8")).hexdigest()


def file_hash(path) -> str:
    with Path(path).open("rb") as stream:
        digest = hashlib.sha256()
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_json(path):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8-sig"))
    except (OSError, ValueError) as exc:
        raise RiskMapInputError(f"JSON输入读取失败 {path}: {exc}") from exc


def atomic_json(data, path, protected_paths=(), *, durable=False):
    output = Path(path).resolve()
    for source in protected_paths:
        source = Path(source).resolve()
        if source == output or (source.exists() and output.exists() and os.path.samefile(source, output)):
            raise RiskMapInputError("匹配输出禁止覆盖受保护输入")
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=output.parent,
                                         prefix=".match-", suffix=".tmp", delete=False) as stream:
            temporary = Path(stream.name)
            json.dump(data, stream, ensure_ascii=False, indent=2, allow_nan=False)
            if durable:
                stream.flush()
                os.fsync(stream.fileno())
        os.replace(temporary, output)
        if durable and os.name != "nt":
            descriptor = os.open(output.parent, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()
    return str(output)


def atomic_bytes(data, path):
    output = Path(path).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="wb", dir=output.parent, prefix=".input-", suffix=".tmp", delete=False) as stream:
            temporary = Path(stream.name)
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, output)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()
    return str(output)
