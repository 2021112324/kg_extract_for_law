"""Run local files explicitly; verify new-label writes without modifying old graphs."""
import asyncio
from collections import Counter
import logging
from pathlib import Path
import re
import sys
import uuid

from app.infrastructure.risk_map.common.io import content_hash
from .config import ParagraphConfig
from .extractor import ParagraphExtractor
from .io_utils import save_json, redact, RedactedFormatter
from .stats import ResultStats

logger = logging.getLogger(__name__)


async def storage_call(function, *args, **kwargs):
    task = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        await task
        raise


def verify_snapshot(snapshot, expected_nodes, expected_edges, label):
    if snapshot.get("boundary_count", 0):
        raise ValueError("Unexpected cross-graph relationship")
    physical = {}
    observed = {}
    for row in snapshot["nodes"]:
        node = row["n"]
        props = node["properties"]
        identifier = props.get("id")
        if identifier in observed or props.get("graph_tag") != label or label not in node["labels"]:
            raise ValueError("Duplicate node or target scope mismatch")
        business = {k: v for k, v in props.items() if k not in {"id", "name", "label", "graph_tag", "graph_level", "filename"}}
        observed[identifier] = content_hash({"name": props.get("name"), "type": props.get("label"), "properties": business})
        physical[node["elementId"]] = identifier
    if observed != expected_nodes:
        raise ValueError("Written node identities/properties differ from expected graph")
    actual_edges = Counter()
    for row in snapshot["edges"]:
        p = row["p"]
        if p["properties"].get("graph_tag") != label:
            raise ValueError("Relationship target scope mismatch")
        actual_edges[(physical[p["startNodeElementId"]], p["type"], physical[p["endNodeElementId"]])] += 1
    if actual_edges != expected_edges:
        raise ValueError("Written relationship identities/counts differ from expected graph")
    if snapshot.get("scope_counts", {"nodes": len(observed), "relationships": sum(actual_edges.values())}) != {
            "nodes": len(observed), "relationships": sum(actual_edges.values())}:
        raise ValueError("Extra target graph members")
    return {"status": "verified", "nodes": len(observed), "relationships": sum(actual_edges.values())}


class ParagraphStandaloneRunner:
    def __init__(self, config=None, graph_storage=None, extractor_factory=ParagraphExtractor):
        self.config = config or ParagraphConfig()
        self.graph_storage = graph_storage
        self.extractor_factory = extractor_factory

    async def run(self, input_path, output_dir, mode="parse", target_label=None):
        source, output = Path(input_path).resolve(), Path(output_dir).resolve()
        if mode not in {"parse", "extract", "write"}:
            raise ValueError("mode must be parse, extract or write")
        if not source.exists():
            raise FileNotFoundError(source)
        if output == source or source.is_file() and output == source.parent:
            raise ValueError("Outputs must not replace the input location")
        files = [source] if source.is_file() else sorted(p for p in source.rglob("*")
            if p.is_file() and p.suffix.lower() in {".txt", ".md"} and output not in p.resolve().parents)
        if not files:
            raise ValueError("No TXT/Markdown input files")
        if mode == "write" and (self.graph_storage is None or not target_label or
                not re.fullmatch(r"[^\W\d]\w*", target_label) or target_label.startswith("_")):
            raise ValueError("Explicit storage and a new valid target label are required")
        run_id = uuid.uuid4().hex
        folder = output/run_id
        folder.mkdir(parents=True, exist_ok=False)
        handler = logging.FileHandler(folder/"run.log", encoding="utf-8")
        handler.setFormatter(RedactedFormatter([self.config.api_key, getattr(self.graph_storage, "password", "")]))
        logs = [logging.getLogger("app.infrastructure.information_extraction.v2.paragraph"),
                logging.getLogger("app.infrastructure.risk_map.progress")]
        previous = [(log, log.level) for log in logs]
        for log in logs:
            log.addHandler(handler)
            log.setLevel(logging.INFO)
        stats = ResultStats()
        stats.counts["input_files"] = len(files)
        summary = {"run_id": run_id, "mode": mode, "publication": self.config.publication,
                   "target_label": target_label, "record_directory": str(folder), "files": []}
        connected, writing_stopped = False, False
        expected_nodes, expected_edges, seen_sources = {}, Counter(), set()
        try:
            if mode == "write":
                connected = await storage_call(self.graph_storage.connect) is True
                if not connected:
                    raise ConnectionError("Neo4j connection failed")
                await storage_call(self.graph_storage.check_subgraph_destination, target_label)
            for index, path in enumerate(files, 1):
                row = {"filename": path.name, "source_path": str(path), "storage_status": "not_requested"}
                if str(path.resolve()) in seen_sources:
                    row["status"] = "duplicate_source_skipped"
                    stats.counts["duplicate_source_paths"] += 1
                    summary["files"].append(row)
                    continue
                seen_sources.add(str(path.resolve()))
                logger.info("paragraph file=%s/%s name=%s mode=%s", index, len(files), path.name, mode)
                writing = False
                try:
                    extractor = self.extractor_factory(config=self.config)
                    result = await extractor.extract_file(path, folder/"files", run_llm=mode != "parse")
                    stats.merge(result["stats"])
                    row.update(status=result["status"], record_directory=result["record_directory"],
                               formal_graph_eligible=result["formal_graph_eligible"], publish_graph_eligible=result["publish_graph_eligible"])
                    if mode == "write":
                        if not result["publish_graph_eligible"]:
                            row["storage_status"] = "skipped_ineligible"
                        elif writing_stopped:
                            row["storage_status"] = "skipped_after_storage_error"
                        else:
                            writing = True
                            graph = result["neo4j_graph"]
                            for node in graph["nodes"]:
                                value = content_hash({"name": node["node_name"], "type": node["node_type"], "properties": node["properties"]})
                                if node["node_id"] in expected_nodes and expected_nodes[node["node_id"]] != value:
                                    raise ValueError("Node identity collision before writing")
                                expected_nodes[node["node_id"]] = value
                            expected_edges.update((e["source_id"], e["relation_type"], e["target_id"]) for e in graph["edges"])
                            from app.infrastructure.risk_map.common.progress import ExecutionMonitor, set_step
                            monitor = ExecutionMonitor(action="paragraph_write", source_mode="neo4j", interval=self.config.heartbeat_seconds).start()
                            try:
                                set_step("新标签写入及关键属性读回核验", 文件=path.name, 目标=target_label)
                                saved = await storage_call(self.graph_storage.add_subgraph_with_merge,
                                    graph, target_label, "DomainLevel", path.name)
                                if saved is not True:
                                    raise RuntimeError("Storage failed or commit outcome unknown; no automatic replay")
                                snapshot = await storage_call(self.graph_storage.read_subgraph_records, target_label,
                                    max_bytes=536870912, timeout=600)
                                row["inspection"] = verify_snapshot(snapshot, expected_nodes, expected_edges, target_label)
                                row["storage_status"] = "confirmed"
                                save_json({"run_id": run_id, "target_label": target_label,
                                    "source_sha256": result["parse_result"]["source_sha256"], "inspection": row["inspection"]},
                                    Path(result["record_directory"])/"storage_receipt.json")
                                stats.counts["storage_success_files"] += 1
                                monitor.result("confirmed", 0)
                            finally:
                                monitor.close(sys.exc_info()[0].__name__ if sys.exc_info()[0] else None)
                except Exception as exc:
                    message = redact(exc, (self.config.api_key, getattr(self.graph_storage, "password", "")))
                    stats.add_error("storage" if writing else "file_processing", str(path), message)
                    row.update(status="failed", error=message)
                    if writing:
                        row["storage_status"], writing_stopped = "failed_or_unknown", True
                        stats.counts["storage_failed_or_unknown"] += 1
                    logger.error("paragraph failed file=%s stage=%s error=%s", path.name, "storage" if writing else "extraction", message)
                summary["files"].append(row)
        except Exception as exc:
            stats.add_error("storage" if mode == "write" else "file_processing", "batch",
                            redact(exc, (self.config.api_key, getattr(self.graph_storage, "password", ""))))
        except asyncio.CancelledError:
            stats.add_error("interruption", "batch", "cancelled; existing writes are not automatically replayed or rolled back")
            raise
        finally:
            if connected:
                try:
                    await storage_call(self.graph_storage.disconnect)
                except Exception as exc:
                    stats.warn("disconnect_failed", redact(exc, (self.config.api_key,)))
            summary["stats"] = stats.to_dict()
            summary["status"] = "failed" if stats.errors else "needs_review" if any(
                r["status"] not in {"structure_only", "success", "no_knowledge", "duplicate_source_skipped"} for r in summary["files"]) else "completed"
            summary["storage_status"] = "not_requested" if mode != "write" else "failed_or_incomplete" if stats.errors else (
                "confirmed" if any(r["storage_status"] == "confirmed" for r in summary["files"]) else "not_written")
            save_json(summary, folder/"service_summary.json")
            for log, level in previous:
                log.removeHandler(handler)
                log.setLevel(level)
            handler.close()
        return summary
