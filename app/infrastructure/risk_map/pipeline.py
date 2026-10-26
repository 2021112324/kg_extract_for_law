"""The only business workflow: committed copy -> all candidates -> all reviews."""

import asyncio
from collections import Counter
import hashlib
import json
import logging
from pathlib import Path
import time

from ..graph_storage.diagnostics import exception_details, redact_diagnostics
from ..graph_storage.subgraph_records import decode, digest, encode, snapshot_hash
from .common.io import content_hash, file_hash
from .common.model_client import ModelSession, OpenAICompatibleClient
from .common.progress import set_step, update_progress
from .common.run_inputs import code_identity, empty_attachments, freeze_inputs, provenance_identity, skip_summary
from .common.stats import Diagnostics
from .config import CANDIDATES, FINAL, MANAGED, PROTOCOL, REVIEW_PROTOCOL
from .local_records import RunRecords, now, target_lock, unresolved_runs
from .matching.location import locate_article, SYSTEM as MATCH_SYSTEM
from .neo4j_bridge import copy_plan, freeze_articles, verify_target
from .preparation.tree_index import attach_provenance, build_tree_index
from .review.review import EXAMPLES, review_article, SYSTEM as REVIEW_SYSTEM

logger = logging.getLogger(__name__)


class WriteUncertain(RuntimeError):
    pass


def persist_diagnostic(records, name, detail, secrets):
    """Keep the original failure visible even if local diagnostic persistence fails."""
    detail = redact_diagnostics({"time": now(), **detail}, secrets)
    logger.error("[故障详情] %s", json.dumps(detail, ensure_ascii=False))
    errors = []
    for destination in ("file", "event"):
        try:
            if destination == "file":
                records.save(name, detail)
            else:
                records.event("failures", detail)
        except OSError as exc:
            failure = exception_details(exc, secrets)
            errors.append({"destination": destination, **failure})
            logger.error("[故障记录保存失败] destination=%s path=%s detail=%s",
                         destination, records.path, json.dumps(failure, ensure_ascii=False))
    return detail, errors


async def db_call(function, *args, **kwargs):
    # Drain an in-flight operation before honoring cancellation; never abandon a commit.
    task = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        result = await task
        asyncio.current_task().cancel()
        return result


async def execute_mapping(config, adapter, *, action="map", run_id=None, client=None):
    """Injected adapter/client are supported for isolated tests. Source is never written."""
    if action not in {"prepare", "map", "resume", "inspect"}:
        raise ValueError("仅支持Neo4j prepare/map/resume/inspect；旧JSON图谱和排名入口已移除")
    config.validate(writing=action in {"map", "resume"})
    if action in {"resume", "inspect"} and not run_id:
        raise ValueError("resume/inspect必须提供本次新协议的RUN_ID")
    root, tree = Path(config.output_root).resolve(), Path(config.tree_path).resolve()
    source_path = Path(config.provenance_path).resolve() if config.provenance_path is not None else None
    for input_path in (tree, source_path):
        if input_path is not None and (input_path == root or root in input_path.parents or input_path in root.parents):
            raise ValueError("输出目录必须与指标树及来源明细输入分离")
    if source_path == tree:
        raise ValueError("来源明细不能与指标树使用同一文件")
    tree_bytes = tree.read_bytes()
    tree_digest = hashlib.sha256(tree_bytes).hexdigest()
    diagnostics = Diagnostics()
    index = build_tree_index(json.loads(tree_bytes.decode("utf-8-sig")), diagnostics)
    provenance_bytes = None
    if source_path is not None:
        try:
            provenance_bytes = source_path.read_bytes()
            source_details = json.loads(provenance_bytes.decode("utf-8-sig"))
        except (OSError, UnicodeError, ValueError) as exc:
            raise ValueError("来源明细无法读取或解析: " + type(exc).__name__) from None
        attach_provenance(index, source_details, diagnostics)
    identity = code_identity()
    skipped = empty_attachments(index, config.region)
    connection = content_hash([getattr(adapter, "uri", "injected"), getattr(adapter, "username", ""), adapter.database])
    binding = {"protocol": PROTOCOL, "config_hash": config.semantic_hash,
               "tree_hash": tree_digest, "connection": connection,
               "prompt_hash": content_hash([MATCH_SYSTEM, REVIEW_SYSTEM, EXAMPLES]), "review_protocol": REVIEW_PROTOCOL,
               "runtime_hash": identity["runtime_hash"]}
    binding["provenance"] = provenance_identity(provenance_bytes, config)
    own_client = None
    session = None
    records = None
    record_writable = action == "map"
    summary = {"copy_status": "not_started", "mapping_status": "not_started",
               "target_label": config.target_label, "source_label": config.source_label,
               "input_diagnostics": index.input_stats}
    secrets = (config.model.api_key, getattr(adapter, "password", ""))
    logger.info("[输入准备] 指标=%s 挂接=%s 含别名挂接=%s 来源核验=%s",
                len(index.indicators), len(index.attachments), index.input_stats.get("alias_attachments", 0),
                index.input_stats["provenance"])
    options = dict(config.read_options)
    timeout = options.get("timeout", 600)
    with target_lock(root, [connection, config.target_label]):
        try:
            if action == "prepare":
                set_step("只读检查源图谱与目标范围")
                source = await db_call(adapter.read_subgraph_records, config.source_label, **options)
                if not source["nodes"]:
                    raise ValueError("源图谱为空，请检查源标签")
                await db_call(adapter.check_subgraph_destination, config.target_label)
                return {**summary, "status": "prepared", "nodes": len(source["nodes"]),
                        "relationships": len(source["edges"]), "indicators": len(index.indicators),
                        "tree_diagnostics": diagnostics.to_dict()}, 0
            if action == "map":
                if unresolved_runs(root, connection, config.target_label):
                    raise WriteUncertain("同一目标存在历史未决本地写入；禁止通过新run_id绕过核对")
                records = RunRecords(root, run_id)
                summary["run_id"] = records.run_id
                input_hash = freeze_inputs(records, tree_bytes, config, {"matching": MATCH_SYSTEM, "review": REVIEW_SYSTEM,
                                                           "review_examples": EXAMPLES},
                              identity, skipped, provenance_bytes=provenance_bytes,
                              verification=[{"attachment_id": aid, **a.source_verification}
                                            for aid, a in enumerate(index.attachments)])
                records.save("run.json", {**binding, "run_id": records.run_id,
                    "source_label": config.source_label, "target_label": config.target_label,
                    "input_manifest_hash": input_hash})
                logger.info("[输入冻结] 空依据挂接=%s（全树本地区，不计失败）；实际影响范围见结束摘要", len(skipped))
                set_step("读取源快照、核对目标为空")
                await db_call(adapter.check_subgraph_destination, config.target_label)
                source = await db_call(adapter.read_subgraph_records, config.source_label, **options)
                if not source["nodes"]:
                    raise ValueError("源图谱为空，请检查源标签")
                plan = copy_plan(source, config.source_label, config.target_label)
                records.save("source_snapshot.json", encode(source))
                records.save("copy_intent.json", {**binding, "source_hash": snapshot_hash(source),
                                                  "target_hash": digest(plan)})
                stable = await db_call(adapter.read_subgraph_records, config.source_label, **options)
                if snapshot_hash(stable) != snapshot_hash(source):
                    raise ValueError("复制前源图谱发生变化")
                del stable
                set_step("原子复制完整基础图谱", 节点数=len(source["nodes"]), 关系数=len(source["edges"]))
                receipt = await db_call(adapter.create_subgraph_copy, plan, config.target_label,
                    source_label=config.source_label, source_hash=snapshot_hash(source), publish_id=records.run_id,
                    completeness="complete", preserve_extra_labels=True,
                    **{k: v for k, v in options.items() if k != "fetch_size"})
                summary["copy_status"] = ("committed" if receipt.get("commit_confirmed") else receipt["status"])
                records.save("copy_result.json", receipt)
                if not receipt.get("commit_confirmed"):
                    raise WriteUncertain("基础复制提交未确认") if receipt["status"] == "commit_unknown" else RuntimeError("基础复制失败")
                manifest = {**receipt, **binding}
                records.save("copy_manifest.json", manifest)
                inventory = freeze_articles(source, manifest)
                records.save("article_inventory.json", {"items": inventory, "hash": content_hash(inventory),
                                                         "copy_hash": content_hash(manifest)})
                del plan
                logger.info("[基础图谱已保存] target=%s nodes=%s relationships=%s 映射尚未完成",
                            config.target_label, len(source["nodes"]), len(source["edges"]))
            else:
                records = RunRecords(root, run_id, resume=True)
                summary["run_id"] = records.run_id
                run = records.read("run.json")
                changed = [k for k, v in binding.items() if run.get(k) != v]
                if changed:
                    raise ValueError("恢复身份不一致: " + ", ".join(changed))
                inputs = records.read("inputs/manifest.json")
                if (content_hash(inputs) != run.get("input_manifest_hash") or inputs["tree_hash"] != tree_digest
                        or inputs.get("provenance") != binding["provenance"]
                        or ("provenance.json" in inputs["files"]) != binding["provenance"]["enabled"]
                        or any(not (records.path / "inputs" / name).is_file()
                            or file_hash(records.path / "inputs" / name) != expected for name, expected in inputs["files"].items())):
                    raise ValueError("冻结输入快照校验失败")
                if (records.path / "copy_result.json").is_file():
                    copy_result = records.read("copy_result.json")
                    summary["copy_status"] = "committed" if copy_result.get("commit_confirmed") else copy_result.get("status", "not_started")
                manifest = records.read("copy_manifest.json")
                if not manifest.get("commit_confirmed") or any(manifest.get(k) != v for k, v in binding.items()):
                    raise ValueError("缺少可信基础复制回执")
                summary["copy_status"] = "committed"
                source = decode(records.read("source_snapshot.json"))
                if snapshot_hash(source) != manifest["source_hash"]:
                    raise ValueError("源快照与复制回执不一致")
                frozen = records.read("article_inventory.json")
                inventory = frozen["items"]
                if (frozen["hash"] != content_hash(inventory) or frozen["copy_hash"] != content_hash(manifest)
                        or inventory != freeze_articles(source, manifest)):
                    raise ValueError("冻结法条清单校验失败")
                record_writable = True
            summary["articles"] = len(inventory)
            confirmed, pending, patches = records.batches()
            if pending:
                raise WriteUncertain("存在未确认批次意图，禁止自动重放或认领")
            set_step("核对基础图谱与已确认映射增量")
            target = await db_call(adapter.read_subgraph_records, config.target_label, **options)
            verification = verify_target(target, source, manifest, patches)
            del target
            if action == "inspect":
                previous = records.read("summary.json")
                return {**previous, "inspection": verification}, 0
            summary["mapping_status"] = "running"
            summary["articles"] = len(inventory)
            for phase in ("initialization", "matching", "review"):
                set_step({"initialization": "初始化目标映射字段", "matching": "第一轮：多对多候选定位",
                          "review": "第二轮：不合理指标剔除"}[phase], 法条总数=len(inventory))
                done = confirmed.setdefault(phase, {})
                remaining = [a for a in inventory if a["id"] not in done]
                for offset in range(0, len(remaining), 20):
                    batch = remaining[offset:offset + 20]
                    current = await db_call(adapter.read_subgraph_articles, config.target_label,
                                            [a["id"] for a in batch], timeout=timeout)
                    if phase == "initialization":
                        results = [{"id": a["id"], "status": "未处理",
                                    "old_values": {k: current[a["id"]][k] for k in MANAGED if k in current[a["id"]]},
                                    "properties": {k: None for k in MANAGED}} for a in batch]
                    else:
                        if client is None:
                            own_client = OpenAICompatibleClient(config.model)
                            client = own_client
                        if session is None:
                            session = ModelSession(config.model, client, root / "cache" / PROTOCOL, records.event,
                                                   scope=content_hash(binding), run_id=records.run_id)
                        if phase == "matching":
                            async def work(article):
                                if not isinstance(article["text"], str) or not article["text"].strip():
                                    return {"id": article["id"], "status": "匹配失败", "coverage_complete": False,
                                            "errors": ["法条全文缺失"], "properties": {CANDIDATES: [], FINAL: None}}
                                return await locate_article(article, index, config.region, session)
                        else:
                            async def work(article):
                                return await review_article({**article, "region": config.region}, current[article["id"]],
                                    confirmed["matching"][article["id"]], session)
                        tasks = [asyncio.create_task(work(a)) for a in batch]
                        try:
                            results = await asyncio.gather(*tasks)
                        except BaseException:
                            for task in tasks:
                                task.cancel()
                            await asyncio.gather(*tasks, return_exceptions=True)
                            raise
                    rows = [{"id": r["id"], "properties": r["properties"]} for r in results]
                    key, intent_hash = records.intent(phase, rows, results)
                    context = {"run_id": records.run_id, "phase": phase, "batch": key,
                        "intent_hash": intent_hash, "database": adapter.database, "uri": getattr(adapter, "uri", "injected"),
                        "source_label": config.source_label, "target_label": config.target_label,
                        "row_count": len(rows), "article_ids": [a["id"] for a in batch],
                        "articles": [{"id": a["id"], "number": a["number"], "filenames": a["filenames"]}
                                     for a in batch],
                        "property_keys": sorted({k for row in rows for k in row["properties"]}),
                        "payload_bytes": len(json.dumps(rows, ensure_ascii=False).encode("utf-8")),
                        "timeout_seconds": timeout}
                    logger.info("[映射批次写入开始] %s", json.dumps(redact_diagnostics(context, secrets), ensure_ascii=False))
                    write_started = time.monotonic()
                    try:
                        receipt = await db_call(adapter.update_subgraph_article_properties,
                                                config.target_label, rows, timeout=timeout)
                    except Exception as exc:
                        receipt = {"status": "exception", "commit_confirmed": False,
                                   "failure_stage": "adapter_call", **exception_details(exc, secrets)}
                    if not isinstance(receipt, dict):
                        receipt = {"status": "invalid_receipt", "commit_confirmed": False,
                                   "failure_stage": "adapter_return", "error_type": type(receipt).__name__}
                    receipt = redact_diagnostics(receipt, secrets)
                    if receipt.get("status") != "committed" or not receipt.get("commit_confirmed"):
                        name = "checkpoints/" + key + "_failure.json"
                        detail, errors = persist_diagnostic(records, name, {
                            **context, "event_type": "database_write_failure", "diagnostic_file": name,
                            "elapsed_seconds": round(time.monotonic() - write_started, 3),
                            "receipt": receipt}, secrets)
                        summary["write_failure"] = detail
                        if errors:
                            summary["diagnostic_record_errors"] = errors
                        raise WriteUncertain("属性写入未确认；停止后续写入并保留基础图谱；"
                            f"phase={phase} batch={key} receipt_status={receipt.get('status')} "
                            f"stage={receipt.get('failure_stage', 'unknown')} "
                            f"error_type={receipt.get('error_type', 'unknown')}")
                    try:
                        records.acknowledge(key, intent_hash, receipt)
                    except OSError as exc:
                        name = "checkpoints/" + key + "_failure.json"
                        detail, errors = persist_diagnostic(records, name, {
                            **context, "event_type": "acknowledgement_persistence_failure",
                            "diagnostic_file": name, "failure_stage": "save_acknowledgement",
                            "receipt": receipt, **exception_details(exc, secrets)}, secrets)
                        summary["write_failure"] = detail
                        if errors:
                            summary["diagnostic_record_errors"] = errors
                        raise WriteUncertain(f"数据库提交已确认但本地回执保存失败；batch={key}；禁止自动重放") from exc
                    logger.info("[映射批次写入已确认] run_id=%s phase=%s batch=%s rows=%s elapsed=%.3fs cleanup_error=%s",
                                records.run_id, phase, key, len(rows), time.monotonic() - write_started,
                                bool(receipt.get("cleanup_error")))
                    done.update((r["id"], r) for r in results)
                    if phase == "review":
                        removed_count = sum(len(group["removed"]) for r in results for group in r["groups"]
                                            if group["confirmed"])
                        records.event("review", {"event_type": "review_applied", "batch": key,
                            "intent_hash": intent_hash, "review_protocol": REVIEW_PROTOCOL,
                            "article_ids": [r["id"] for r in results], "removed_count": removed_count})
                        logger.info("[审查确认] 批次=%s 状态=已应用 法条数=%s 已应用剔除项数=%s",
                                    key, len(results), removed_count)
                    update_progress(emit=True, 阶段=phase, 已确认法条=len(done), 法条总数=len(inventory),
                                    当前批次条数=len(batch), 当前批次失败=sum(bool(r.get("errors")) for r in results))
                    for r in results:
                        records.event(phase, {"event_type": "article_result", "batch": key,
                                              "article_id": r["id"], "status": r["status"]})
                        if r.get("errors"):
                            records.event("failures", {"phase": phase, "id": r["id"], "errors": r["errors"]})
            confirmed, pending, patches = records.batches()
            set_step("结束核验：原始成员保真与映射增量")
            target = await db_call(adapter.read_subgraph_records, config.target_label, **options)
            summary["inspection"] = verify_target(target, source, manifest, patches)
            failures = sum(bool(result.get("errors")) for phase in ("matching", "review")
                           for result in confirmed.get(phase, {}).values())
            summary["mapping_status"] = "completed_with_errors" if failures else "completed"
            summary["failed_stage_tasks"] = failures
            summary["model"] = session.counters if session else {}
            code = 2 if failures else 0
        except (asyncio.CancelledError, KeyboardInterrupt) as exc:
            summary["mapping_status"] = "interrupted"
            summary["status"] = "interrupted"
            summary["interruption"] = exception_details(exc, secrets)
            logger.warning("[执行取消或键盘中断] %s", json.dumps(summary["interruption"], ensure_ascii=False))
            if records and record_writable:
                try:
                    records.save("summary.json", summary)
                except OSError:
                    logger.error("[摘要保存失败] 基础复制状态=%s", summary["copy_status"])
            raise
        except Exception as exc:
            summary["error"] = redact_diagnostics(str(exc), secrets)
            summary["error_type"] = type(exc).__name__
            summary["exception"] = exception_details(exc, secrets)
            logger.error("[运行异常堆栈] %s", json.dumps(summary["exception"], ensure_ascii=False))
            if records and record_writable and not isinstance(exc, WriteUncertain):
                _, errors = persist_diagnostic(records, "run_failure.json", {
                    "event_type": "run_failure", "run_id": records.run_id,
                    "source_label": config.source_label, "target_label": config.target_label,
                    **summary["exception"]}, secrets)
                if errors:
                    summary["diagnostic_record_errors"] = errors
            summary["mapping_status"] = "write_unknown" if isinstance(exc, WriteUncertain) else "interrupted"
            code = 2 if isinstance(exc, WriteUncertain) or summary["copy_status"] in {"committed", "commit_unknown"} else 1
            logger.error("[运行停止] copy_status=%s mapping_status=%s error=%s",
                         summary["copy_status"], summary["mapping_status"], summary["error"])
        finally:
            if own_client:
                try:
                    await own_client.aclose()
                except Exception:
                    summary["client_cleanup_error"] = True
        summary["status"] = summary["mapping_status"]
        if records and record_writable:
            summary["record_directory"] = str(records.path)
            try:
                confirmed, pending, _ = records.batches()
                summary["confirmed_counts"] = {phase: len(rows) for phase, rows in confirmed.items()}
                local_statuses = {}
                for phase in ("initialization", "matching", "review"):
                    local_statuses.update({key: r["status"] for key, r in confirmed.get(phase, {}).items()})
                summary["article_statuses"] = dict(Counter(local_statuses.values()))
                summary["basis_skips"] = skip_summary(index, confirmed.get("matching", {}))
                reviewed_results = confirmed.get("review", {}).values()
                summary["review_audit"] = {"review_protocol": REVIEW_PROTOCOL,
                    "confirmed_removals": sum(len(group["removed"]) for r in reviewed_results
                                              for group in r["groups"] if group["confirmed"]),
                    "confirmed_groups": sum(group["confirmed"] for r in reviewed_results for group in r["groups"]),
                    "unreviewed_candidates": sum(len(r.get("unreviewed_numbers", [])) for r in reviewed_results)}
                summary["pending_batches"] = len(pending)
                summary["unprocessed"] = max(0, summary.get("articles", 0) - len(confirmed.get("review", {})))
            except (ValueError, OSError, KeyError):
                summary["record_validation_error"] = True
                summary["unprocessed"] = None
            summary["model"] = session.counters if session else {}
            summary["tree_diagnostics"] = diagnostics.to_dict()
            try:
                records.save("summary.json", summary)
            except OSError:
                summary["summary_record_error"] = True
                code = 2 if summary["copy_status"] == "committed" else 1
        logger.info("[结束摘要] 基础复制=%s 目标=%s 映射=%s 未处理=%s",
                    summary["copy_status"], config.target_label, summary["mapping_status"], summary.get("unprocessed"))
        return summary, code
