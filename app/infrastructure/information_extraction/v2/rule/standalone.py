"""Independent local-file runner using the existing project graph storage."""
import asyncio
import logging
from pathlib import Path
import re
import uuid

from .config import RuleConfig
from .extractor import RuleExtractor
from .io_utils import redact, save_json, stable_id
from .stats import ResultStats
from .structure_parser import RuleStructureParser


logger = logging.getLogger(__name__)


class RuleStandaloneRunner:
    def __init__(self, config=None, graph_storage=None, extractor_factory=RuleExtractor):
        self.config = config or RuleConfig()
        self.graph_storage = graph_storage
        self.extractor_factory = extractor_factory

    async def run(self, input_path, output_dir, mode="parse", target_label=None):
        if mode not in {"parse", "extract", "write"}:
            raise ValueError("mode must be parse, extract or write")
        source, output = Path(input_path).resolve(), Path(output_dir).resolve()
        if not source.exists():
            raise FileNotFoundError(source)
        if output == source or source.is_file() and output == source.parent:
            raise ValueError("Output must not overwrite the input location")
        files = [source] if source.is_file() else sorted(p for p in source.rglob("*") if p.suffix.lower() in {".txt", ".md"} and output not in p.parents)
        if not files:
            raise ValueError("No supported source files")
        if mode == "write" and (not target_label or not re.fullmatch(r"[^\W\d]\w*", target_label) or target_label.startswith("_")):
            raise ValueError("A new explicit legal target label is required")
        if mode == "write" and self.graph_storage is None:
            raise ValueError("Explicit project graph_storage is required for write")
        output.mkdir(parents=True, exist_ok=True)
        stats = ResultStats()
        stats.counts["input_files"] = len(files)
        summary = {"run_id": uuid.uuid4().hex, "mode": mode, "target_label": target_label,
                   "files": [], "source_aliases": {}, "storage_status": "not_requested"}
        seen, connected = {}, False
        stop_writing = False
        handler = logging.FileHandler(output / "run.log", encoding="utf-8")
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
        log = logging.getLogger("app.infrastructure.information_extraction.v2.rule")
        previous_level = log.level
        log.setLevel(logging.INFO)
        log.addHandler(handler)
        try:
            if mode == "write":
                if await asyncio.to_thread(self.graph_storage.connect) is not True:
                    raise RuntimeError("Neo4j connection failed")
                connected = True
                await asyncio.to_thread(self.graph_storage.check_subgraph_destination, target_label)
            for index, path in enumerate(files, 1):
                logger.info("rule file=%s/%s path=%s mode=%s", index, len(files), path.name, mode)
                extractor = self.extractor_factory(config=self.config)
                row = {"filename": path.name, "path": str(path), "storage_status": "not_requested"}
                try:
                    parsed = RuleStructureParser().parse_file(path)
                    fingerprint = parsed["body_fingerprint"]
                    if fingerprint in seen and parsed["validation"]["passed"]:
                        row.update(status="duplicate_skipped", duplicate_of=seen[fingerprint])
                        summary["source_aliases"].setdefault(seen[fingerprint], []).append(str(path))
                        stats.counts["duplicate_files"] += 1
                        summary["files"].append(row)
                        continue
                    result = await extractor.extract_file(path, output, run_llm=mode != "parse")
                    stats.merge(result["stats"])
                    row["status"] = result["status"]
                    if result["status"] in {"structure_only", "success"}:
                        seen[fingerprint] = str(path)
                    if mode == "write" and result["formal_graph_eligible"] and not stop_writing:
                        extractor.progress["stage"] = "Neo4j write and verification"
                        async with extractor.heartbeat():
                            saved = await asyncio.to_thread(self.graph_storage.add_subgraph_with_merge,
                                result["neo4j_graph"], target_label, "DomainLevel", path.name)
                            if saved is not True:
                                raise RuntimeError("Neo4j write failed or was not confirmed")
                            ids = [n["node_id"] for n in result["graph"]["nodes"]]
                            confirmed = await asyncio.to_thread(self.graph_storage.run_query,
                                f"MATCH (n:`{target_label}`) WHERE n.id IN $ids RETURN count(n) AS count", {"ids": ids})
                            edge_confirmed = await asyncio.to_thread(self.graph_storage.run_query,
                                f"MATCH (a:`{target_label}`)-[r]->(b:`{target_label}`) WHERE a.id IN $ids AND b.id IN $ids RETURN count(r) AS count", {"ids": ids})
                            if not confirmed or confirmed[0]["count"] != len(ids) or not edge_confirmed or edge_confirmed[0]["count"] != len(result["graph"]["edges"]):
                                raise RuntimeError("Neo4j write verification did not confirm all nodes and edges")
                        row["storage_status"] = "confirmed"
                        stats.counts["storage_success_files"] += 1
                    elif mode == "write":
                        row["storage_status"] = "skipped_ineligible" if not result["formal_graph_eligible"] else "skipped_after_storage_error"
                except Exception as exc:
                    message = redact(type(exc).__name__ + ": " + str(exc), (self.config.api_key,))
                    is_storage = extractor.progress.get("stage", "").startswith("Neo4j")
                    stats.add_error("storage" if is_storage else "file_processing", path.name, message)
                    row.update(status="failed", error=message)
                    if is_storage:
                        stop_writing = True
                        row["storage_status"] = "failed_or_unknown"
                    logger.error("rule failed file=%s error=%s", path.name, message)
                summary["files"].append(row)
        except Exception as exc:
            stats.add_error("storage" if mode == "write" else "file_processing", "batch", redact(exc, (self.config.api_key,)))
            summary["batch_error"] = redact(exc, (self.config.api_key,))
        finally:
            if connected:
                try:
                    await asyncio.to_thread(self.graph_storage.disconnect)
                except Exception as exc:
                    stats.warn("disconnect_failed", redact(exc, (self.config.api_key,)))
            summary["stats"] = stats.to_dict()
            summary["status"] = "failed" if stats.errors else "needs_review" if any(f["status"] in {"needs_review", "incomplete", "failed"} for f in summary["files"]) else "completed"
            if mode == "write":
                summary["storage_status"] = "failed_or_incomplete" if stats.errors or any(f["storage_status"] not in {"confirmed", "not_requested"} for f in summary["files"]) else "confirmed"
            save_json(summary, output / "service_summary.json")
            logger.info("rule complete status=%s stats=%s", summary["status"], dict(stats.counts))
            log.removeHandler(handler)
            log.setLevel(previous_level)
            handler.close()
        return summary
