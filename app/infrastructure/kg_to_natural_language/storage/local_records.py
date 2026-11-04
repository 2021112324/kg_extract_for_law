"""Atomic local run evidence and an independent one-minute heartbeat."""

import json
import logging
import os
import re
import tempfile
import threading
import time
import uuid
from contextlib import contextmanager
from pathlib import Path

from .records import KnowledgeStorageError, canonical, json_default, now_cn

logger = logging.getLogger(__name__)


def redact(text):
    text = re.sub(r"(\w+://[^\s:/]+:)[^@\s]+@", r"\1[REDACTED]@", str(text))
    return re.sub(r"(?i)((?:password|api_key|authorization)\s*[=:]\s*)[^\s,;]+", r"\1[REDACTED]", text)


class RunRecords:
    def __init__(self, root, run_id=None, *, resume=False):
        self.run_id = run_id or uuid.uuid4().hex
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", self.run_id):
            raise KnowledgeStorageError("Invalid run_id")
        self.directory = Path(root).resolve() / "runs" / self.run_id
        if resume:
            if not self.directory.is_dir():
                raise KnowledgeStorageError("Run directory does not exist")
        else:
            self.directory.mkdir(parents=True, exist_ok=False)
        self._event_lock = threading.Lock()

    @classmethod
    def open(cls, directory):
        directory = Path(directory).resolve()
        if directory.parent.name != "runs":
            raise KnowledgeStorageError("Expected an output/runs/run_id directory")
        return cls(directory.parent.parent, directory.name, resume=True)

    def write(self, name, value):
        if Path(name).name != name:
            raise KnowledgeStorageError("Run artifact name must not contain a path")
        handle = tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=self.directory,
                                             prefix=".pending-", suffix=".tmp", delete=False)
        temporary = Path(handle.name)
        try:
            with handle:
                if name.endswith(".jsonl"):
                    for row in value:
                        handle.write(canonical(row) + "\n")
                else:
                    json.dump(value, handle, ensure_ascii=False, indent=2, allow_nan=False, default=json_default)
                    handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.directory / name)
        finally:
            temporary.unlink(missing_ok=True)

    def read(self, name):
        with (self.directory / name).open(encoding="utf-8") as file:
            return json.load(file)

    @contextmanager
    def writer_lock(self):
        """OS-owned local run lock; process exit releases it without stale TTLs."""
        with (self.directory / "writer.lock").open("a+b") as handle:
            if handle.seek(0, os.SEEK_END) == 0:
                handle.write(b"0")
                handle.flush()
            handle.seek(0)
            acquired = False
            try:
                if os.name == "nt":
                    import msvcrt
                    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                acquired = True
                yield
            except OSError as exc:
                if not acquired:
                    raise KnowledgeStorageError("Another process is already writing this local run") from exc
                raise
            finally:
                if acquired:
                    handle.seek(0)
                    if os.name == "nt":
                        import msvcrt
                        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                    else:
                        import fcntl
                        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def event(self, event_type, **fields):
        # Store execution metadata, never SQL parameter dumps or database URLs.
        row = {"at": now_cn().isoformat(), "event_type": event_type, **fields}
        safe_line = redact(canonical(row))
        with self._event_lock:
            with (self.directory / "events.jsonl").open("a", encoding="utf-8") as file:
                file.write(safe_line + "\n")
                file.flush()
        logger.info("[knowledge_storage] %s", safe_line)


class Heartbeat:
    def __init__(self, records, interval=60):
        self.records, self.interval = records, interval
        self.stage, self.done, self.total = "starting", 0, 0
        self.stop = threading.Event()
        self.thread = None
        self.record_error = None

    def progress(self, stage, done=0, total=0):
        self.stage, self.done, self.total = stage, done, total
        self.records.event("progress", stage=stage, done=done, total=total)

    def __enter__(self):
        def beat():
            began = time.monotonic()
            while not self.stop.wait(self.interval):
                try:
                    self.records.event("heartbeat", stage=self.stage, done=self.done,
                                       total=self.total, elapsed_seconds=round(time.monotonic() - began, 1))
                except Exception as exc:
                    self.record_error = type(exc).__name__
                    logger.error("Knowledge heartbeat record failed: %s", type(exc).__name__)
        self.thread = threading.Thread(target=beat, name="knowledge-storage-heartbeat", daemon=True)
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.stop.set()
        self.thread.join()
