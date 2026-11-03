"""Read-only completion verification for new-protocol final-replay MemoryError.

Run this file directly after installing the checkpoint memory fix. It neither
resumes extraction nor changes the old run identity or any graph property.
"""

from collections import Counter
from datetime import datetime
import json
import logging
from pathlib import Path
import struct
import sys
import uuid

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.infrastructure.graph_storage.diagnostics import exception_details
from app.infrastructure.graph_storage.factory import GraphStorageFactory
from app.infrastructure.graph_storage.subgraph_records import decode, snapshot_hash
from app.infrastructure.risk_map.common.io import content_hash, file_hash
from app.infrastructure.risk_map.common.progress import ExecutionMonitor, set_step, update_progress
from app.infrastructure.risk_map.config import PHASES, PROTOCOL
from app.infrastructure.risk_map.local_records import RunRecords, now, target_lock
from app.infrastructure.risk_map.neo4j_bridge import freeze_articles, verify_target

RUN_ID = None  # Fill in a new expert-reference run ID; old review runs are rejected.
logger = logging.getLogger("app.infrastructure.risk_map.final_verification")


def validate_frozen_inputs(records, run):
    manifest = records.read("inputs/manifest.json")
    if content_hash(manifest) != run.get("input_manifest_hash"):
        raise ValueError("冻结输入清单校验失败")
    for name, expected in manifest["files"].items():
        if not isinstance(name, str) or Path(name).name != name or "\\" in name or name in {".", ".."}:
            raise ValueError("冻结输入文件名不合法")
        path = records.path / "inputs" / name
        if not path.is_file() or file_hash(path) != expected:
            raise ValueError("冻结输入文件校验失败: " + name)
    if (manifest.get("tree_hash") != run.get("tree_hash")
            or manifest.get("provenance") != run.get("provenance")
            or manifest["files"].get("indicator_tree.json") != run.get("tree_hash")
            or "code_identity.json" not in manifest["files"]):
        raise ValueError("冻结指标树、来源或代码身份缺失/不一致")
    identity = records.read("inputs/code_identity.json")
    if (identity.get("runtime_hash") != run.get("runtime_hash")
            or content_hash(identity["files"]) != run.get("runtime_hash")):
        raise ValueError("原任务代码身份记录校验失败")


def finished_counts(records, article_ids):
    seen = {phase: set() for phase in PHASES}
    statuses = {}
    failed = 0
    pending = records.pending_batches()
    if pending:
        raise ValueError(f"存在{len(pending)}个未确认批次；不自动重放，不认定完成")
    for number, (_, intent, committed) in enumerate(records.iter_batches(), 1):
        if not committed:
            raise ValueError("发现未确认批次；不认定完成")
        phase = intent["phase"]
        for result in intent["results"]:
            aid = result["id"]
            if aid not in article_ids:
                raise ValueError("批次包含冻结法条清单之外的节点")
            seen[phase].add(aid)
            statuses[aid] = result["status"]
            if phase != "initialization":
                failed += bool(result.get("errors"))
        if number % 100 == 0:
            update_progress(emit=True, 已校验批次=number, 已定位处理法条=len(seen["matching"]))
    missing = {phase: len(article_ids - ids) for phase, ids in seen.items()}
    if any(missing.values()):
        raise ValueError("任务尚未全部完成；缺少已确认法条: " + json.dumps(missing, ensure_ascii=False))
    return {"confirmed_counts": {phase: len(ids) for phase, ids in seen.items()},
            "article_statuses": dict(Counter(statuses.values())), "failed_stage_tasks": failed,
            "pending_batches": 0, "unprocessed": 0}


def verify_finished_run(config, adapter, run_id=RUN_ID):
    """Check old immutable inputs and receipts, then read the target once.

    Unlike resume, this verifier does not execute the old model algorithm or
    accept a changed runtime as its replacement. The original runtime identity
    remains frozen and is checked for internal integrity, not rewritten.
    """
    config.validate(writing=False)
    records = RunRecords(config.output_root, run_id, resume=True)
    connection = content_hash([getattr(adapter, "uri", "injected"),
                               getattr(adapter, "username", ""), adapter.database])
    with target_lock(config.output_root, [connection, config.target_label]):
        set_step("核对故障运行和冻结输入")
        run = records.read("run.json")
        if run.get("protocol") != PROTOCOL:
            raise ValueError("此收尾入口仅支持新专家依据定位协议；旧语义审查运行请使用匹配的历史代码核验")
        expected = {"protocol": PROTOCOL, "run_id": run_id, "connection": connection,
                    "source_label": config.source_label, "target_label": config.target_label,
                    "config_hash": config.semantic_hash}
        changed = [key for key, value in expected.items() if run.get(key) != value]
        if changed:
            raise ValueError("运行配置与原任务不一致: " + ", ".join(changed))
        validate_frozen_inputs(records, run)
        # Bind this special entry to the diagnosed final-replay failure, not an
        # arbitrary interrupted extraction or an uncertain database write.
        failure = records.read("run_failure.json")
        if (failure.get("run_id") != run_id or failure.get("error_type") != "MemoryError"
                or failure.get("source_label") != config.source_label
                or failure.get("target_label") != config.target_label):
            raise ValueError("故障记录不是本任务的MemoryError；不使用专用收尾入口")
        manifest = records.read("copy_manifest.json")
        if (not manifest.get("commit_confirmed")
                or any(manifest.get(key) != value for key, value in run.items()
                       if key not in {"run_id", "input_manifest_hash"})):
            raise ValueError("原任务基础复制回执不可信")
        set_step("读取并校验原任务源快照")
        source = decode(records.read("source_snapshot.json"))
        if snapshot_hash(source) != manifest.get("source_hash"):
            raise ValueError("源快照与原任务复制回执不一致")
        frozen = records.read("article_inventory.json")
        items = frozen["items"]
        if (content_hash(items) != frozen.get("hash")
                or content_hash(manifest) != frozen.get("copy_hash")
                or items != freeze_articles(source, manifest)):
            raise ValueError("冻结法条清单校验失败")
        article_ids = {item["id"] for item in items}
        del frozen, items
        set_step("逐批核对初始化与定位已确认记录", 法条数=len(article_ids))
        counts = finished_counts(records, article_ids)
        set_step("只读核验现有目标图谱", 目标=config.target_label)
        target = adapter.read_subgraph_records(config.target_label, **dict(config.read_options))
        inspection = verify_target(target, source, manifest, records.iter_patches())
        del target, source
        # Records may have changed while the database was read. Never issue a
        # success report if a later unacknowledged batch has appeared.
        if records.pending_batches():
            raise ValueError("核验期间出现未确认批次；不认定完成")
        report = {"time": now(), "run_id": run_id, "source_label": config.source_label,
                  "target_label": config.target_label, "mode": "read_only_final_verification",
                  "status": "verified_with_errors" if counts["failed_stage_tasks"] else "verified",
                  "inspection": inspection, "articles": len(article_ids), **counts,
                  "database_writes": 0, "model_requests": 0,
                  "original_runtime_hash": run["runtime_hash"],
                  "original_run_records_modified": False}
        name = "final_verification_" + datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:8] + ".json"
        report["report_path"] = str(records.path / name)
        records.save(name, report)
        return report, 2 if counts["failed_stage_tasks"] else 0


def main():
    # Reuse this computer's runner configuration; never copy another machine's
    # credentials and never invoke its main()/map entry.
    from app.infrastructure.risk_map import run_mapping as settings

    adapter = None
    handlers = []
    previous = []
    monitor = None
    try:
        if not RUN_ID:
            raise ValueError("请先填写新专家依据定位协议运行的RUN_ID；不再默认核验历史审查任务")
        if struct.calcsize("P") != 8:
            raise ValueError("请使用64位Python运行，当前解释器为32位")
        if not all(hasattr(RunRecords, name) for name in ("iter_batches", "iter_patches", "pending_batches")):
            raise ValueError("尚未安装内存修复：请先替换同目录的local_records.py和pipeline.py")
        settings.ACTION = "inspect"
        config = settings.build_config()
        log_folder = Path(config.output_root) / "logs"
        log_folder.mkdir(parents=True, exist_ok=True)
        log_path = log_folder / ("finish_memory_error_" + datetime.now().strftime("%Y%m%d_%H%M%S")
                                 + "_" + uuid.uuid4().hex[:8] + ".log")
        handlers = [logging.StreamHandler(), logging.FileHandler(log_path, encoding="utf-8")]
        for handler in handlers:
            handler.setFormatter(settings.RedactedFormatter([settings.API_KEY, settings.NEO4J_PASSWORD]))
        for name in ("app.infrastructure.risk_map", "app.infrastructure.graph_storage"):
            log = logging.getLogger(name)
            previous.append((log, log.level, log.propagate))
            log.setLevel(logging.INFO)
            log.propagate = False
            for handler in handlers:
                log.addHandler(handler)
        monitor = ExecutionMonitor(action="read_only_final_verification", source_mode="neo4j", interval=60).start()
        logger.info("[只读收尾] run_id=%s 日志=%s；不调用模型，不写图谱，不修改原运行身份", RUN_ID, log_path)
        adapter = GraphStorageFactory.create("neo4j", {"uri": settings.NEO4J_URI,
            "username": settings.NEO4J_USER, "password": settings.NEO4J_PASSWORD,
            "database": settings.NEO4J_DATABASE, "driver_options": settings.DRIVER_OPTIONS})
        if not adapter.connect():
            raise ConnectionError("Neo4j连接失败")
        adapter.verify_subgraph_support()
        report, code = verify_finished_run(config, adapter)
        monitor.result(report["status"], code)
        print(json.dumps(report, ensure_ascii=False, indent=2))
        print("核验通过，已有图谱与确认记录一致。" if not code else "图谱核验通过，但原抽取记录仍有技术错误，详见failed_stage_tasks。")
        return code
    except KeyboardInterrupt:
        if monitor:
            monitor.result("interrupted", 130)
        print("只读核验已中断；未改写图谱或原任务记录。")
        return 130
    except Exception as exc:
        if monitor:
            monitor.result("verification_failed", 2)
        detail = exception_details(exc, (settings.API_KEY, settings.NEO4J_PASSWORD))
        logger.error("[收尾核验失败] %s", json.dumps(detail, ensure_ascii=False))
        print(json.dumps({"status": "verification_failed", **detail}, ensure_ascii=False, indent=2))
        return 2
    finally:
        try:
            if adapter:
                adapter.disconnect()
        finally:
            if monitor:
                monitor.close(sys.exc_info()[0].__name__ if sys.exc_info()[0] else None)
            for log, level, propagate in previous:
                for handler in handlers:
                    log.removeHandler(handler)
                log.setLevel(level)
                log.propagate = propagate
            for handler in handlers:
                handler.close()


if __name__ == "__main__":
    raise SystemExit(main())
