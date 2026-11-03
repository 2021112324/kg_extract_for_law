"""Local-only copy receipt and append-only batch intentions/acknowledgements."""

from contextlib import contextmanager
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import re
import uuid

from .common.io import atomic_json, content_hash, read_json
from .config import PHASES, PROTOCOL

logger = logging.getLogger(__name__)


def now():
    return datetime.now(timezone.utc).isoformat()


def unresolved_runs(root, connection, target):
    """A fresh run must not bypass an earlier ambiguous write in the same local root."""
    blocked = []
    for path in (Path(root) / "runs").glob("*/run.json"):
        run = read_json(path)
        if run.get("connection") != connection or run.get("target_label") != target:
            continue
        folder = path.parent
        if (folder / "copy_intent.json").exists():
            result_path = folder / "copy_result.json"
            result = read_json(result_path) if result_path.exists() else {}
            if not result.get("commit_confirmed") and not (
                    result.get("status") == "failed" and result.get("rollback_confirmed") is True):
                blocked.append(folder.name)
                continue
        if any(not p.with_name(p.name.replace("_intent.json", "_ack.json")).exists()
               for p in (folder / "checkpoints").glob("*_intent.json")):
            blocked.append(folder.name)
    return blocked


@contextmanager
def target_lock(root, identity):
    """OS releases the local lock on process exit; this is NOT a distributed lock."""
    folder = Path(root) / "locks"
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / (content_hash(identity) + ".lock")
    with path.open("a+b") as stream:
        stream.seek(0, 2)
        if stream.tell() == 0:
            stream.write(b"\0")
            stream.flush()
        stream.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise ValueError("同一输出根的目标已有运行任务") from exc
        try:
            yield
        finally:
            stream.seek(0)
            if os.name == "nt":
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(stream, fcntl.LOCK_UN)


class RunRecords:
    def __init__(self, root, run_id=None, *, resume=False):
        self.run_id = run_id or uuid.uuid4().hex
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", self.run_id):
            raise ValueError("run_id不合法")
        base = Path(root).resolve()
        self.path = (base / "runs" / self.run_id).resolve()
        if not self.path.is_relative_to(base):
            raise ValueError("运行记录路径越出输出根")
        if resume:
            if not self.path.is_dir():
                raise ValueError("恢复记录不存在")
        else:
            self.path.mkdir(parents=True, exist_ok=False)

    def save(self, name, value):
        return atomic_json(value, self.path / name, durable=True)

    def read(self, name):
        return read_json(self.path / name)

    def event(self, phase, record):
        path = self.path / phase / "events.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps({"time": now(), "run_id": self.run_id, **record}, ensure_ascii=False) + "\n")
            stream.flush()

    def pending_batches(self):
        """Discover missing acknowledgements without loading batch payloads."""
        return [str(path) for path in sorted((self.path / "checkpoints").glob("*_intent.json"))
                if not path.with_name(path.name.replace("_intent.json", "_ack.json")).exists()]

    def iter_batches(self):
        """Validate one checkpoint at a time; retain only phase membership sets."""
        confirmed = {}
        for path in sorted((self.path / "checkpoints").glob("*_intent.json")):
            try:
                intent = read_json(path)
            except MemoryError as exc:
                detail = (f"读取批次记录内存不足: path={path} file_bytes={path.stat().st_size} "
                          f"confirmed_tasks={sum(len(v) for v in confirmed.values())}")
                logger.error("[本地记录内存不足] %s", detail)
                raise MemoryError(detail) from exc
            if (intent.get("protocol") != PROTOCOL or intent.get("run_id") != self.run_id
                    or intent.get("phase") not in PHASES
                    or len(intent.get("rows", [])) != len(intent.get("results", []))):
                raise ValueError("批次意图协议、身份或任务数量不一致")
            ack_path = path.with_name(path.name.replace("_intent.json", "_ack.json"))
            if not ack_path.exists():
                yield path, intent, False
                continue
            ack = read_json(ack_path)
            if ack.get("intent_hash") != content_hash(intent) or ack.get("status") != "committed":
                raise ValueError("批次回执校验失败")
            rows = intent["rows"]
            if sorted(ack.get("ids", [])) != sorted(row["id"] for row in rows):
                raise ValueError("批次回执成员不一致")
            done = confirmed.setdefault(intent["phase"], set())
            for row, result in zip(rows, intent["results"]):
                if (row["id"] in done or row["id"] != result["id"]
                        or row["properties"] != result["properties"]):
                    raise ValueError("阶段逻辑任务重复或错位")
                previous_phase = {"matching": "initialization"}.get(intent["phase"])
                if previous_phase and row["id"] not in confirmed.get(previous_phase, {}):
                    raise ValueError("阶段前置任务缺失，禁止重新初始化已处理节点")
                done.add(row["id"])
            yield path, intent, True

    def iter_patches(self):
        """Recheck disk evidence without rebuilding all result dictionaries."""
        for path, intent, committed in self.iter_batches():
            if not committed:
                raise ValueError("存在未确认批次意图: " + str(path))
            yield from intent["rows"]

    def batches(self, *, include_patches=True):
        confirmed, pending, patches = {}, [], []
        for path, intent, committed in self.iter_batches():
            if not committed:
                pending.append(str(path))
                continue
            rows = intent["rows"]
            done = confirmed.setdefault(intent["phase"], {})
            for row, result in zip(rows, intent["results"]):
                done[row["id"]] = result
                # rows and results carry the same property payload in the JSON
                # protocol; keep one in-memory copy after integrity validation.
                row["properties"] = result["properties"]
            if include_patches:
                patches.extend(rows)
        return confirmed, pending, patches

    def intent(self, phase, rows, results):
        if phase not in PHASES:
            raise ValueError("当前协议不支持此阶段: " + phase)
        sequences = [int(p.name.split("_", 1)[0]) for p in (self.path / "checkpoints").glob("*_intent.json")]
        count = max(sequences, default=-1) + 1
        key = f"{count:08d}_{phase}"
        value = {"protocol": PROTOCOL, "run_id": self.run_id, "phase": phase,
                 "rows": rows, "results": results, "created_at": now()}
        self.save("checkpoints/" + key + "_intent.json", value)
        return key, content_hash(value)

    def acknowledge(self, key, intent_hash, receipt):
        self.save("checkpoints/" + key + "_ack.json", {**receipt, "intent_hash": intent_hash, "time": now()})
