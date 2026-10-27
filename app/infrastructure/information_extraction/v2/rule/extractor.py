"""Two-stage rule extraction with bounded retries and no semantic fallback."""
import asyncio
from contextlib import asynccontextmanager
import json
import logging
import re
import time

from .config import RuleConfig
from .graph_builder import RuleGraphBuilder, prepare_graph_for_neo4j
from .io_utils import compact, redact, save_json, stable_id
from .prompt.schema import schema_for_boundary, schema_for_clause, schema_for_file_info, schema_for_group
from .prompt.prompt import BOUNDARY_PROMPT, CLAUSE_PROMPT, FILE_PROMPT
from .prompt.example import example_for_boundary, example_for_clause, example_for_file_info, example_for_group, example_for_context
from .stats import ResultStats
from .structure_parser import RuleStructureParser
from .unit_planner import RuleUnitPlanner, boundary_candidates, descendants
from .validator import normalize_extraction_result, validate_result, supported
from ..guide_p1.validator import to_plain


logger = logging.getLogger(__name__)


class RuleExtractor:
    def __init__(self, config=None, llm_extractor=None):
        self.config = config or RuleConfig()
        self._llm = llm_extractor
        self.structure_parser = RuleStructureParser()
        self.planner = RuleUnitPlanner(self.config.block_chars, self.config.context_chars)
        self.result_stats = ResultStats()
        self.semaphore = asyncio.Semaphore(self.config.max_concurrent)
        self.progress = {"stage": "idle", "completed": 0, "failed": 0}
        self.audit = []
        self.audit_path = None

    def _record_attempt(self, event):
        serialized = json.dumps(to_plain(event), ensure_ascii=False, default=str)
        if self.config.api_key:
            serialized = serialized.replace(self.config.api_key, "[REDACTED]")
        self.audit.append(json.loads(serialized))
        if self.audit_path:
            from pathlib import Path
            path = Path(self.audit_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as stream:
                stream.write(serialized + "\n")

    def _client(self):
        if self._llm is None:
            if not all((self.config.model_name, self.config.api_url, self.config.api_key)):
                raise ValueError("Set RULE_MODEL, RULE_API_URL and RULE_API_KEY before model extraction")
            from app.infrastructure.information_extraction.factory import InformationExtractionFactory
            from app.infrastructure.information_extraction.method.base import LangextractConfig
            config = LangextractConfig(model_name=self.config.model_name, api_key=self.config.api_key,
                api_url=self.config.api_url, config={"timeout": self.config.timeout},
                max_char_buffer=max(20000, self.config.block_chars + self.config.context_chars + 4000),
                batch_length=1, max_workers=1, debug=False, max_output_tokens=8192)
            # Factory adapter uses range(max_retries); outer layer owns all five attempts.
            self._llm = InformationExtractionFactory.create("langextract", max_retries=1, config=config)
        return self._llm

    @asynccontextmanager
    async def heartbeat(self):
        async def beat():
            while True:
                await asyncio.sleep(self.config.heartbeat_seconds)
                logger.info("rule heartbeat file=%s stage=%s model_tasks_completed=%s failed=%s provisions=%s/%s", self.progress.get("filename"),
                            self.progress["stage"], self.progress["completed"], self.progress["failed"],
                            self.progress.get("provisions_completed", 0), self.progress.get("provisions_total", 0))
        task = asyncio.create_task(beat())
        try:
            yield
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def _task(self, task_id, prompt, schema, payload, examples, validator):
        error = ""
        for attempt in range(1, self.config.max_attempts + 1):
            raw = None
            attempt_started = time.perf_counter()
            self.result_stats.counts["requests"] += 1
            self.result_stats.counts["retries"] += int(attempt > 1)
            logger.info("rule model task=%s attempt=%s/%s", task_id, attempt, self.config.max_attempts)
            try:
                async with self.semaphore:
                    raw = await asyncio.wait_for(self._client().entity_and_relationship_extract(
                        user_prompt=prompt + ("\n上次结果不可用，请修正：" + error if error else ""),
                        schema=schema, input_text=json.dumps(payload, ensure_ascii=False), examples=examples),
                        timeout=self.config.timeout)
                if isinstance(raw, str):
                    from app.infrastructure.risk_map.common.json_output import parse_output, reset_audit, parse_audit
                    reset_audit()
                    try:
                        raw = parse_output(raw)
                    finally:
                        repair = parse_audit()
                        if repair:
                            self.result_stats.counts["json_repair_attempts"] += 1
                            self.result_stats.counts["json_repair_successes"] += int(repair["success"])
                validated = validator(raw)
                self._record_attempt({"task_id": task_id, "attempt": attempt, "status": "success", "input": payload,
                                      "validated": validated, "elapsed_seconds": round(time.perf_counter() - attempt_started, 3)})
                self.result_stats.counts["model_success_tasks"] += 1
                self.progress["completed"] += 1
                return validated
            except Exception as exc:
                error = redact(type(exc).__name__ + ": " + str(exc), (self.config.api_key,))
                self.result_stats.counts["attempt_errors"] += 1
                self._record_attempt({"task_id": task_id, "attempt": attempt, "status": "attempt_failed", "error": error,
                                      "input": payload, "raw_response": raw, "elapsed_seconds": round(time.perf_counter() - attempt_started, 3)})
                logger.warning("rule model task=%s attempt=%s error=%s", task_id, attempt, error)
                if attempt < self.config.max_attempts:
                    await asyncio.sleep(self.config.retry_delay)
        self.progress["failed"] += 1
        self.result_stats.counts["model_failed_tasks"] += 1
        self.result_stats.add_error("extraction", task_id, error)
        return None

    @staticmethod
    def _boundary_validator(node, choices):
        def check(raw):
            entities = normalize_extraction_result(raw)["entities"]
            if len(entities) != 1 or entities[0]["entity_type"] != "承载判定":
                raise ValueError("Expected exactly one boundary decision")
            props = entities[0]["properties"]
            if set(props) - {"处理方式", "成员编号", "证据原文"}:
                raise ValueError("Unknown boundary properties")
            modes = {"整组": "group", "继承": "inherit", "独立": "independent"}
            if props.get("处理方式") not in modes or not supported(props.get("证据原文"), node["own_text"]):
                raise ValueError("Unsupported boundary decision")
            members = props.get("成员编号", [])
            if not isinstance(members, list) or any(not isinstance(n, str) for n in members) or len(set(members)) != len(members):
                raise ValueError("Boundary members must be unique string identifiers")
            if any(n not in choices for n in members):
                raise ValueError("Invented boundary member")
            mode = modes[props["处理方式"]]
            if mode == "group" and not members or mode != "group" and members:
                raise ValueError("Boundary mode and members inconsistent")
            if re_conjunction(node["body_text"]) and (mode != "group" or set(members) != set(choices)):
                raise ValueError("All conjunctive conditions must remain together")
            return {"mode": mode, "member_ids": [choices[n] for n in members], "evidence": props["证据原文"]}
        return check

    async def extract_from_parse_result(self, parsed, run_llm=False, max_blocks=None):
        started = time.perf_counter()
        self.result_stats = ResultStats()
        self.audit = []
        self.progress = {"filename": parsed["filename"], "stage": "planning", "completed": 0, "failed": 0}
        self.result_stats.counts["total_files"] = 1
        plan = self.planner.plan(parsed)
        file_result, results, boundary_failed = None, [], False
        partial = False
        async with self.heartbeat():
            if not parsed["validation"]["passed"]:
                self.result_stats.add_error("file_processing", parsed["filename"], "Structure validation failed")
            elif not run_llm and plan["errors"]:
                self.result_stats.add_error("file_processing", parsed["filename"], str(plan["errors"]))
            elif run_llm:
                self.progress["stage"] = "boundary_confirmation"
                by_id = {n["node_id"]: n for n in parsed["nodes"]}
                decisions = {}
                for n in boundary_candidates(parsed):
                    children = [c for c in descendants(n, by_id) if c["kind"] == "numeric"]
                    choices = {c["number"]: c["node_id"] for c in children}
                    payload = {"number": n["number"], "target": n["own_text"],
                               "children": [{"number": c["number"], "text": c["own_text"]} for c in children]}
                    if len(json.dumps(payload, ensure_ascii=False)) > self.config.context_chars + self.config.block_chars:
                        self.result_stats.add_error("file_processing", n["number"], "Boundary context too long; requires review")
                        boundary_failed = True
                        continue
                    value = await self._task(n["node_id"], BOUNDARY_PROMPT, schema_for_boundary, payload,
                                             example_for_boundary, self._boundary_validator(n, choices))
                    if value is None:
                        boundary_failed = True
                    else:
                        decisions[n["node_id"]] = value
                plan = self.planner.plan(parsed, decisions)
                if plan["errors"]:
                    self.result_stats.add_error("file_processing", parsed["filename"], str(plan["errors"]))
                if plan["formal_plan_eligible"] and not boundary_failed:
                    self.progress["stage"] = "file_info"
                    file_source = parsed["file_header"]
                    # Source excerpts supply purposes/scope, not fabricated metadata.
                    if plan["units"]:
                        file_source += "\n" + plan["units"][0]["text"] + "\n" + plan["units"][-1]["text"]
                    payload = {"title": parsed["title"], "source": file_source}
                    file_result = await self._task("file_info", FILE_PROMPT, schema_for_file_info, payload,
                        example_for_file_info, lambda raw: validate_result(raw, source=file_source, title=parsed["title"]))
                    if file_result:
                        self.progress["stage"] = "provisions"
                        jobs = [(u, i, c) for u in plan["units"] for i, c in enumerate(u["chunks"])]
                        partial = max_blocks is not None and max_blocks < len(jobs)
                        selected = jobs if max_blocks is None else jobs[:max(0, max_blocks)]
                        self.progress.update(provisions_completed=0, provisions_total=len(selected))
                        self.result_stats.counts["planned_blocks"] = len(jobs)
                        self.result_stats.counts["unexecuted_blocks"] = len(jobs) - len(selected)

                        async def extract(unit, i, chunk):
                            payload = {"number": unit["number"], "member_numbers": unit["member_numbers"],
                                       "mode": unit["mode"], "target": chunk["text"], "context": unit["context"], "path": unit["path"]}
                            from .validator import RISK_TYPES
                            payload["explicit_risk_types"] = [risk for risk in RISK_TYPES if risk in parsed["file_header"]]
                            task_id = unit["unit_id"] + ":" + str(i)
                            examples = example_for_group if unit["mode"] == "group" else example_for_context if unit["mode"] == "context" else example_for_clause
                            schema = schema_for_group if unit["mode"] == "group" else schema_for_clause
                            value = await self._task(task_id, CLAUSE_PROMPT, schema, payload,
                                examples, lambda raw: validate_result(raw, source=payload["target"] + "\n" + payload["context"] + "\n" + "\n".join(payload["explicit_risk_types"]), unit=payload))
                            status = "success" if value else "failed"
                            if value and not any(e["entity_type"] == "条款单元" for e in value["entities"]):
                                self.result_stats.counts["empty_blocks"] += 1
                                risky = bool(re.search(r"应当|必须|不得|禁止|有权|应|须", chunk["text"]))
                                self.result_stats.counts["risky_empty_blocks" if risky else "reasonable_empty_blocks"] += 1
                                self.result_stats.warn("no_semantic_units", unit["number"], strong=risky)
                            self.progress["provisions_completed"] += 1
                            logger.info("rule provision number=%s chunk=%s status=%s completed=%s/%s", unit["number"], i+1, status,
                                        self.progress["provisions_completed"], len(selected))
                            return {"unit_id": unit["unit_id"], "chunk_index": i, "status": status, "validated": value}
                        results = await asyncio.gather(*(extract(*job) for job in selected))
        counts = self.result_stats.counts
        counts.update(plan["counts"])
        counts["structure_nodes"] = len(parsed["nodes"])
        counts["excluded_ranges"] = len(parsed["excluded"])
        for row in plan["coverage"]:
            counts["coverage_" + row["kind"]] += 1
        for row in parsed["excluded"]:
            counts["excluded_" + row["reason"]] += 1
        for d in parsed["diagnostics"]:
            if d["severity"] != "error":
                self.result_stats.warn(d["code"], str(d))
        if not run_llm:
            status = "structure_only" if parsed["validation"]["passed"] and not plan["errors"] else "failed"
        elif self.result_stats.errors or boundary_failed or not plan["formal_plan_eligible"]:
            status = "incomplete"
        elif partial:
            status = "partial_test"
        elif file_result is None or not plan["units"]:
            status = "failed"
        elif counts["risky_empty_blocks"]:
            status = "needs_review"
        else:
            status = "success"
        graph = RuleGraphBuilder().build(parsed, plan, file_result, results)
        counts["nodes"] = len(graph["nodes"])
        counts["edges"] = len(graph["edges"])
        for node in graph["nodes"]:
            counts["nodes_" + node["node_type"]] += 1
        counts["elapsed_ms"] = round((time.perf_counter() - started) * 1000)
        file_counter = {"success": "success_files", "structure_only": "structure_only_files", "partial_test": "partial_test_files",
                        "needs_review": "needs_review_files", "failed": "failed_files"}.get(status, "incomplete_files")
        counts[file_counter] += 1
        self.progress["stage"] = "finished"
        return {"filename": parsed["filename"], "status": status, "formal_graph_eligible": status == "success",
                "elapsed_seconds": round(time.perf_counter() - started, 3),
                "parse_result": parsed, "plan": plan, "extraction_result": {"file_info": file_result, "content": results, "audit": self.audit},
                "graph": graph, "neo4j_graph": prepare_graph_for_neo4j(graph), "stats": self.result_stats.to_dict()}

    async def extract_text(self, text, filename="memory.txt", run_llm=False, max_blocks=None):
        return await self.extract_from_parse_result(self.structure_parser.parse_text(text, filename), run_llm, max_blocks)

    async def extract_file(self, path, output_dir=None, run_llm=False, max_blocks=None):
        if output_dir:
            from pathlib import Path
            directory = Path(output_dir) / (Path(path).stem + "_" + stable_id("source", str(Path(path).resolve()))[-8:])
            self.audit_path = directory / "attempts.jsonl"
        parsed = self.structure_parser.parse_file(path)
        result = await self.extract_from_parse_result(parsed, run_llm, max_blocks)
        if output_dir:
            from pathlib import Path
            for suffix, value in [("split", parsed), ("units", result["plan"]), ("extraction", result["extraction_result"]),
                                  ("kg", {"status": result["status"], "formal_graph_eligible": result["formal_graph_eligible"], **result["graph"]}), ("summary", result["stats"])]:
                save_json(value, directory / (suffix + ".json"))
        return result


def re_conjunction(text):
    import re
    return bool(re.search(r"同时满足|以下全部|下列全部", text))
