"""Two-stage contextual extraction with per-entry cumulative retry budgets."""
import asyncio
from collections import Counter
from contextlib import asynccontextmanager
import json
import logging
from pathlib import Path
import time

from ..rule.extractor import RuleExtractor
from .config import GuideP2Config
from .graph_builder import GuideP2GraphBuilder, prepare_graph_for_neo4j
from .io_utils import redact, save_json, stable_id
from .prompt.schema import schema_for_content_model, schema_for_file_info, RISK_TYPE_VALUES
from .prompt.prompt import CONTENT_PROMPT, FILE_PROMPT
from .prompt.example import CONTENT_EXAMPLES, FILE_EXAMPLES
from .stats import ResultStats
from .structure_parser import GuideP2StructureParser
from .unit_planner import GuideP2UnitPlanner
from .validator import validate_file, validate_batch

logger = logging.getLogger(__name__)


class GuideP2Extractor(RuleExtractor):
    def __init__(self, config=None, llm_extractor=None):
        super().__init__(config or GuideP2Config(), llm_extractor)
        self.structure_parser = GuideP2StructureParser()
        self.planner = GuideP2UnitPlanner(self.config)

    def _client(self):
        if self._llm is None and not all((self.config.model_name, self.config.api_url, self.config.api_key)):
            raise ValueError("Configure GUIDE_P2_MODEL, GUIDE_P2_API_URL and GUIDE_P2_API_KEY")
        return super()._client()

    @asynccontextmanager
    async def heartbeat(self):
        async def beat():
            while True:
                await asyncio.sleep(self.config.heartbeat_seconds)
                logger.info("guide_p2 heartbeat file=%s stage=%s entries=%s/%s failed=%s", self.progress.get("filename"),
                            self.progress["stage"], self.progress["completed"], self.progress.get("total", 0), self.progress["failed"])
        task = asyncio.create_task(beat())
        try:
            yield
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def _request(self, task_id, payload, prompt, schema, examples, validate, feedback=""):
        raw, started = None, time.perf_counter()
        phase = "model"
        self.result_stats.counts["requests"] += 1
        self.result_stats.counts["request_retries"] += bool(feedback)
        logger.info("guide_p2 request task=%s entries=%s max_attempts_per_target=%s", task_id, len(payload.get("entries", [])), self.config.max_attempts)
        try:
            async with self.semaphore:
                raw = await asyncio.wait_for(self._client().entity_and_relationship_extract(
                    user_prompt=prompt + ("\n上次失败，请修正：" + feedback if feedback else ""), schema=schema,
                    input_text=json.dumps(payload, ensure_ascii=False), examples=examples), self.config.timeout)
            if isinstance(raw, str):
                phase = "parse"
                from app.infrastructure.risk_map.common.json_output import parse_output, reset_audit, parse_audit
                reset_audit()
                try:
                    raw = parse_output(raw)
                finally:
                    repair = parse_audit()
                    if repair:
                        self.result_stats.counts["json_repair_attempts"] += 1
                        self.result_stats.counts["json_repair_successes"] += int(repair["success"])
            phase = "validation"
            value = validate(raw)
            self.result_stats.counts["model_success_requests"] += 1
            self._record_attempt({"task_id": task_id, "status": "success", "input": payload, "validated": value, "elapsed_seconds": round(time.perf_counter()-started, 3)})
            return value, ""
        except Exception as exc:
            detail = "request exceeded " + str(self.config.timeout) + " seconds" if isinstance(exc, asyncio.TimeoutError) else str(exc)
            error = redact(type(exc).__name__ + ": " + detail, (self.config.api_key,))
            self.result_stats.counts["attempt_errors"] += 1
            self.result_stats.counts[phase + "_attempt_errors"] += 1
            self._record_attempt({"task_id": task_id, "status": "attempt_failed", "input": payload, "raw_response": raw,
                                  "phase": phase, "error": error, "elapsed_seconds": round(time.perf_counter()-started, 3)})
            logger.warning("guide_p2 task=%s error=%s", task_id, error)
            return None, error

    async def extract_from_parse_result(self, parsed, run_llm=False, max_entries=None, entry_ids=None):
        started = time.perf_counter()
        self.result_stats, self.audit = ResultStats(), []
        self.progress = {"filename": parsed["filename"], "stage": "planning", "completed": 0, "failed": 0}
        plan = self.planner.plan(parsed)
        self.result_stats.counts.update(plan["counts"])
        self.result_stats.counts["total_files"] = 1
        file_result, results = None, []
        units = plan["units"]
        eligible_units = units if entry_ids is None else [u for u in units if u["entry_id"] in entry_ids]
        if entry_ids is not None and set(entry_ids) - {u["entry_id"] for u in units}:
            raise ValueError("Unknown selected entry ID")
        selected_ids = {u["entry_id"] for u in (eligible_units if max_entries is None else eligible_units[:max(0, max_entries)])}
        partial = len(selected_ids) < len(units)
        self.progress["total"] = len(selected_ids)
        self.result_stats.counts["unexecuted_entries"] = len(units) if not run_llm else len(units)-len(selected_ids)
        explicit = [r for r in RISK_TYPE_VALUES if r in parsed["file_header"]]
        attempts = Counter()
        async with self.heartbeat():
            if not plan["formal_plan_eligible"]:
                self.result_stats.add_error("file_processing", parsed["filename"], str(plan["errors"] or parsed["diagnostics"]))
            elif run_llm:
                self.progress["stage"] = "file_info"
                source = parsed["file_header"] + "\n" + parsed["title"]
                for attempt in range(1, self.config.max_attempts + 1):
                    self.result_stats.counts["retries"] += int(attempt > 1)
                    file_result, error = await self._request("file_info:"+str(attempt), {"title": parsed["title"], "source": source},
                        FILE_PROMPT, schema_for_file_info, FILE_EXAMPLES, lambda raw: validate_file(raw, parsed["title"], source, explicit),
                        error if attempt > 1 else "")
                    if file_result:
                        break
                    if attempt < self.config.max_attempts:
                        await asyncio.sleep(self.config.retry_delay)
                if file_result is None:
                    self.result_stats.add_error("extraction", "file_info", error)
                else:
                    self.progress["stage"] = "entries"
                    by_id = {u["entry_id"]: u for u in units}

                    async def execute(entries, feedback=""):
                        for e in entries:
                            key = e["entry_id"]
                            if attempts[key] >= self.config.max_attempts:
                                raise AssertionError("Retry budget exceeded")
                            self.result_stats.counts["entry_retries"] += int(attempts[key] > 0)
                            attempts[key] += 1
                        task_id = ",".join(e["entry_id"][-8:] + ":" + str(attempts[e["entry_id"]]) for e in entries)
                        value, error = await self._request(task_id, {"entries": entries, "explicit_risk_types": explicit}, CONTENT_PROMPT,
                            schema_for_content_model, CONTENT_EXAMPLES, lambda raw: validate_batch(raw, entries, explicit), feedback)
                        if value is not None:
                            for result in value:
                                unit = by_id[result["entry_id"]]
                                decision = result["decision"]
                                result["status"] = "success" if decision == "知识" else "needs_review" if decision == "不确定" or unit["high_signal"] else "reasonable_empty"
                                if result["status"] == "needs_review":
                                    self.result_stats.warn("uncertain_or_suspicious_empty_entry", result["entry_id"] + ": " + str(result["reason"]), strong=True)
                                results.append(result)
                                self.result_stats.counts["entries_" + result["status"]] += 1
                                self.progress["completed"] += 1
                                logger.info("guide_p2 entry=%s status=%s progress=%s/%s", unit["number"] or unit["entry_id"], result["status"], self.progress["completed"], self.progress["total"])
                            return
                        if len(entries) > 1:
                            self.result_stats.counts["split_batches"] += 1
                        pending = []
                        for e in entries:
                            if attempts[e["entry_id"]] < self.config.max_attempts:
                                pending.append(e)
                            else:
                                self.result_stats.add_error("extraction", e["entry_id"], error)
                                self.progress["failed"] += 1
                                results.append({"entry_id": e["entry_id"], "status": "failed", "error": error})
                        if pending:
                            await asyncio.sleep(self.config.retry_delay)
                            await asyncio.gather(*(execute([e], error) for e in pending))

                    batches = [[e for e in b["entries"] if e["entry_id"] in selected_ids] for b in plan["batches"]]
                    await asyncio.gather(*(execute(b) for b in batches if b))
        status = "structure_only" if not run_llm else "success"
        if self.result_stats.errors:
            status = "failed" if not run_llm else "incomplete"
        elif run_llm and any(r["status"] == "needs_review" for r in results):
            status = "needs_review"
        elif run_llm and partial:
            status = "partial_test"
        elif run_llm and (file_result is None or not results):
            status = "failed"
        graph = GuideP2GraphBuilder().build(parsed, plan, file_result, results)
        counts = self.result_stats.counts
        for row in parsed["excluded"]:
            counts["excluded_" + row["reason"]] += 1
        counts["coverage_targets"] = len(plan["coverage"])
        counts["unexecuted_entries"] = len(units) - len(results)
        counts["nodes"], counts["edges"] = len(graph["nodes"]), len(graph["edges"])
        counts["elapsed_ms"] = round((time.perf_counter()-started)*1000)
        counts[status + "_files"] += 1
        for n in graph["nodes"]:
            counts["nodes_" + n["node_type"]] += 1
            if n["node_type"] == "指引知识单元":
                counts["knowledge_" + n["properties"]["知识类型"]] += 1
        self.progress["stage"] = "finished"
        return {"filename": parsed["filename"], "status": status, "formal_graph_eligible": status == "success",
                "parse_result": parsed, "plan": plan, "extraction_result": {"file_info": file_result, "entries": results,
                "attempts_by_entry": dict(attempts), "audit": self.audit}, "graph": graph,
                "neo4j_graph": prepare_graph_for_neo4j(graph), "stats": self.result_stats.to_dict()}

    async def extract_text(self, text, filename="memory.txt", run_llm=False, max_entries=None, entry_ids=None):
        return await self.extract_from_parse_result(self.structure_parser.parse_text(text, filename), run_llm, max_entries, entry_ids)

    async def extract_file(self, path, output_dir=None, run_llm=False, max_entries=None, entry_ids=None):
        directory = Path(output_dir) / (Path(path).stem + "_" + stable_id("source", str(Path(path).resolve()))[-8:]) if output_dir else None
        if directory:
            self.audit_path = directory / "attempts.jsonl"
        parsed = self.structure_parser.parse_file(path)
        result = await self.extract_from_parse_result(parsed, run_llm, max_entries, entry_ids)
        if directory:
            for name, value in [("split", parsed), ("units", result["plan"]), ("extraction", result["extraction_result"]),
                                ("kg", {"status": result["status"], "formal_graph_eligible": result["formal_graph_eligible"], **result["graph"]}), ("summary", result["stats"])]:
                save_json(value, directory / (name + ".json"))
        return result
