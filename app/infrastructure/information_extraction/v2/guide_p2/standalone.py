"""Independent directory runner using existing storage, never existing labels."""
import asyncio
import logging
from pathlib import Path
import re
import uuid
from .config import GuideP2Config
from .extractor import GuideP2Extractor
from .io_utils import redact, save_json
from .stats import ResultStats

logger = logging.getLogger(__name__)


class GuideP2StandaloneRunner:
    def __init__(self, config=None, graph_storage=None, extractor_factory=GuideP2Extractor):
        self.config = config or GuideP2Config()
        self.graph_storage = graph_storage
        self.extractor_factory = extractor_factory

    async def run(self, input_path, output_dir, mode="parse", target_label=None):
        source, output = Path(input_path).resolve(), Path(output_dir).resolve()
        if mode not in {"parse", "extract", "write"}:
            raise ValueError("mode must be parse, extract or write")
        if not source.exists():
            raise FileNotFoundError(source)
        if source == output or source.is_file() and source.parent == output:
            raise ValueError("Output must not overwrite source location")
        files = [source] if source.is_file() else sorted(p for p in source.rglob("*") if p.suffix.lower() in {".txt", ".md"} and output not in p.parents)
        if not files:
            raise ValueError("No .txt/.md inputs")
        if mode == "write" and (self.graph_storage is None or not target_label or not re.fullmatch(r"[^\W\d]\w*", target_label) or target_label.startswith("_")):
            raise ValueError("Explicit storage and a new valid target label are required")
        output.mkdir(parents=True, exist_ok=True)
        handler = logging.FileHandler(output / "run.log", encoding="utf-8")
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        log = logging.getLogger("app.infrastructure.information_extraction.v2.guide_p2")
        previous = log.level
        log.setLevel(logging.INFO)
        log.addHandler(handler)
        summary = {"run_id": uuid.uuid4().hex, "mode": mode, "target_label": target_label, "files": [], "source_aliases": {}}
        stats, seen, connected, stop = ResultStats(), {}, False, False
        stats.counts["input_files"] = len(files)
        try:
            if mode == "write":
                connected = await asyncio.to_thread(self.graph_storage.connect) is True
                if not connected:
                    raise RuntimeError("Neo4j connection failed")
                await asyncio.to_thread(self.graph_storage.check_subgraph_destination, target_label)
            for i, path in enumerate(files, 1):
                logger.info("guide_p2 file=%s/%s name=%s mode=%s", i, len(files), path.name, mode)
                extractor = self.extractor_factory(config=self.config)
                row = {"filename": path.name, "path": str(path), "storage_status": "not_requested"}
                try:
                    parsed = extractor.structure_parser.parse_file(path)
                    fingerprint = parsed["body_fingerprint"]
                    if parsed["validation"]["passed"] and fingerprint in seen:
                        row.update(status="duplicate_skipped", duplicate_of=seen[fingerprint])
                        summary["source_aliases"].setdefault(seen[fingerprint], []).append(str(path))
                        stats.counts["duplicate_files"] += 1
                    else:
                        result = await extractor.extract_file(path, output, run_llm=mode != "parse")
                        stats.merge(result["stats"])
                        row["status"] = result["status"]
                        if result["status"] in {"structure_only", "success"}:
                            seen[fingerprint] = str(path)
                        if mode == "write" and result["formal_graph_eligible"] and not stop:
                            extractor.progress["stage"] = "Neo4j write and verification"
                            async with extractor.heartbeat():
                                saved = await asyncio.to_thread(self.graph_storage.add_subgraph_with_merge, result["neo4j_graph"], target_label, "DomainLevel", path.name)
                                if saved is not True:
                                    raise RuntimeError("Storage failed or write outcome unknown")
                                ids = [n["node_id"] for n in result["graph"]["nodes"]]
                                nodes = await asyncio.to_thread(self.graph_storage.run_query, f"MATCH(n:`{target_label}`) WHERE n.id IN $ids RETURN count(n) AS count", {"ids": ids})
                                edges = await asyncio.to_thread(self.graph_storage.run_query, f"MATCH(a:`{target_label}`)-[r]->(b:`{target_label}`) WHERE a.id IN $ids AND b.id IN $ids RETURN count(r) AS count", {"ids": ids})
                                if not nodes or not edges or nodes[0]["count"] != len(ids) or edges[0]["count"] != len(result["graph"]["edges"]):
                                    raise RuntimeError("Write verification unconfirmed")
                            row["storage_status"] = "confirmed"
                            stats.counts["storage_success_files"] += 1
                        elif mode == "write":
                            row["storage_status"] = "skipped_ineligible" if not result["formal_graph_eligible"] else "skipped_after_storage_error"
                except Exception as exc:
                    storage_error = extractor.progress["stage"].startswith("Neo4j")
                    error = redact(exc, (self.config.api_key,))
                    stats.add_error("storage" if storage_error else "file_processing", path.name, error)
                    row.update(status="failed", error=error)
                    if storage_error:
                        row["storage_status"], stop = "failed_or_unknown", True
                    logger.error("guide_p2 failed file=%s error=%s", path.name, error)
                summary["files"].append(row)
        except Exception as exc:
            stats.add_error("storage" if mode == "write" else "file_processing", "batch", redact(exc, (self.config.api_key,)))
        finally:
            if connected:
                try:
                    await asyncio.to_thread(self.graph_storage.disconnect)
                except Exception as exc:
                    stats.warn("disconnect_failed", redact(exc, (self.config.api_key,)))
            summary["stats"] = stats.to_dict()
            summary["status"] = "failed" if stats.errors else "needs_review" if any(r["status"] not in {"structure_only", "success", "duplicate_skipped"} for r in summary["files"]) else "completed"
            summary["storage_status"] = "not_requested" if mode != "write" else "confirmed" if summary["status"] == "completed" else "failed_or_incomplete"
            save_json(summary, output / "service_summary.json")
            logger.info("guide_p2 complete status=%s counts=%s", summary["status"], dict(stats.counts))
            log.removeHandler(handler)
            log.setLevel(previous)
            handler.close()
        return summary
