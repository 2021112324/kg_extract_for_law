"""Two-stage model extraction, bounded attempts, streaming audit and independent heartbeat."""
import asyncio
from collections import Counter
import json
import logging
from pathlib import Path
import time
import sys
import uuid

from app.infrastructure.risk_map.common.progress import ExecutionMonitor, set_step, update_progress
from app.infrastructure.risk_map.common.io import file_hash
from .config import ParagraphConfig
from .graph_builder import ParagraphGraphBuilder, prepare_graph_for_neo4j
from .io_utils import append_audit, save_json, redact
from .prompt.schema import FILE_SCHEMA, CONTENT_SCHEMA, RISK_TYPE_VALUES
from .prompt.prompt import FILE_PROMPT, CONTENT_PROMPT
from .prompt.example import FILE_EXAMPLES, CONTENT_EXAMPLES
from .stats import ResultStats
from .structure_parser import ParagraphStructureParser
from .unit_planner import ParagraphUnitPlanner
from .validator import validate_file, validate_content, file_properties, ReviewRequired

logger = logging.getLogger(__name__)
PACKAGE = Path(__file__).resolve().parent
RUNTIME_IDENTITY = {p.relative_to(PACKAGE).as_posix(): file_hash(p)
                    for p in sorted(list(PACKAGE.glob("*.py")) + list((PACKAGE/"prompt").glob("*.py")))}


def require_documents(documents):
    if not documents:
        raise ValueError("Empty model document list")


class ParagraphExtractor:
    def __init__(self, config=None, llm_extractor=None):
        self.config = config or ParagraphConfig()
        self._llm = llm_extractor
        self.structure_parser = ParagraphStructureParser(self.config)
        self.planner = ParagraphUnitPlanner(self.config)
        self.stats = ResultStats()
        self.audit_path = None
        self.semaphore = asyncio.Semaphore(self.config.max_concurrent)

    def _client(self):
        if self._llm is None:
            if not all((self.config.model_name, self.config.api_url, self.config.api_key)):
                raise ValueError("Configure PARAGRAPH_MODEL, PARAGRAPH_API_URL and PARAGRAPH_API_KEY")
            from app.infrastructure.information_extraction.factory import InformationExtractionFactory
            from app.infrastructure.information_extraction.method.base import LangextractConfig
            model = LangextractConfig(model_name=self.config.model_name, api_url=self.config.api_url,
                api_key=self.config.api_key, config={"timeout": self.config.timeout},
                max_char_buffer=self.config.request_chars, max_output_tokens=self.config.max_output_tokens,
                batch_length=1, max_workers=1, debug=False)
            self._llm = InformationExtractionFactory.create("langextract", max_retries=1, config=model)
        return self._llm

    async def _task(self, task_id, payload, prompt, schema, examples, validator):
        feedback = ""
        for attempt in range(1, self.config.max_attempts + 1):
            raw, phase = None, "request"
            instruction = prompt + ("\n上次输出未通过，请根据错误修正："+feedback if feedback else "")
            try:
                self.planner.check_request(payload, instruction, schema, examples)
            except ValueError as exc:
                self.stats.warn("request_budget", str(exc), strong=True)
                return {"status": "needs_review", "reason": str(exc)}
            started = time.perf_counter()
            try:
                async with self.semaphore:
                    self.stats.counts["requests"] += 1
                    self.stats.counts["retries"] += attempt > 1
                    logger.info("paragraph model task=%s attempt=%s/%s", task_id, attempt, self.config.max_attempts)
                    raw = await asyncio.wait_for(self._client().entity_and_relationship_extract(
                        user_prompt=instruction, schema=schema, input_text=json.dumps(payload, ensure_ascii=False), examples=examples,
                        document_validator=require_documents), self.config.timeout)
                if isinstance(raw, str):
                    phase = "parse"
                    from app.infrastructure.risk_map.common.json_output import parse_output, reset_audit, parse_audit
                    reset_audit()
                    try:
                        raw = parse_output(raw)
                    finally:
                        repair = parse_audit()
                        if repair:
                            self.stats.counts["json_repair_attempts"] += 1
                            self.stats.counts["json_repair_successes"] += int(repair["success"])
                phase = "validation"
                value = validator(raw)
                self.stats.counts["model_success_tasks"] += 1
                append_audit(self.audit_path, {"task_id": task_id, "attempt": attempt, "status": "success",
                    "input": payload, "raw_response": raw, "validated": value,
                    "elapsed_seconds": round(time.perf_counter()-started, 3)}, (self.config.api_key,))
                return {"status": "validated", "value": value}
            except ReviewRequired as exc:
                self.stats.warn("semantic_review", str(exc), strong=True)
                if "news" in str(exc).lower() or "source classification" in str(exc).lower():
                    self.stats.counts["source_conflicts"] += 1
                append_audit(self.audit_path, {"task_id": task_id, "attempt": attempt, "status": "needs_review",
                    "input": payload, "raw_response": raw, "reason": str(exc)}, (self.config.api_key,))
                return {"status": "needs_review", "reason": str(exc)}
            except Exception as exc:
                detail = str(exc) or (f"timeout after {self.config.timeout}s" if isinstance(exc, asyncio.TimeoutError) else type(exc).__name__)
                feedback = redact(type(exc).__name__+": "+detail, (self.config.api_key,))
                self.stats.counts["attempt_errors"] += 1
                self.stats.counts[phase+"_attempt_errors"] += 1
                if phase == "validation" and any(token in str(exc) for token in ("Unknown Schema", "Unknown entity", "Illegal relationship")):
                    self.stats.counts["schema_rejections"] += 1
                append_audit(self.audit_path, {"task_id": task_id, "attempt": attempt, "status": "attempt_failed",
                    "phase": phase, "error": feedback, "raw_response": raw, "input": payload}, (self.config.api_key,))
                logger.warning("paragraph task=%s attempt=%s phase=%s error=%s", task_id, attempt, phase, feedback)
                if attempt < self.config.max_attempts:
                    await asyncio.sleep(self.config.retry_delay)
        self.stats.add_error("extraction", task_id, feedback)
        return {"status": "failed", "error": feedback}

    async def extract_from_parse_result(self, parsed, run_llm=False, max_targets=None):
        self.stats = ResultStats()
        self.stats.counts.update(filtered_fields=0, schema_rejections=0, source_conflicts=0,
                                 json_repair_attempts=0, json_repair_successes=0, requests=0,
                                 retries=0, attempt_errors=0, field_normalizations=0, error=0)
        monitor = ExecutionMonitor(action="paragraph_extract" if run_llm else "paragraph_parse",
                                   source_mode="text", interval=self.config.heartbeat_seconds).start()
        try:
            set_step("段落与上下文规划", 文件=parsed["filename"])
            plan = self.planner.plan(parsed)
            self.stats.counts.update(plan["counts"])
            for diagnostic in parsed["diagnostics"]:
                self.stats.warn(diagnostic["code"], diagnostic["code"], diagnostic["severity"] == "review")
            self.stats.counts["files"] = 1
            file_result, results = None, []
            total = len(plan["units"])
            selected = plan["units"] if max_targets is None else plan["units"][:max(0, max_targets)]
            partial = len(selected) != total
            status = "structure_only"
            if not plan["formal_plan_eligible"]:
                status = "needs_review"
            elif run_llm:
                set_step("文件来源及引用抽取", 文件=parsed["filename"])
                file_task = await self._task("file_info", plan["file_payload"], FILE_PROMPT, FILE_SCHEMA, FILE_EXAMPLES,
                                             lambda raw: validate_file(raw, parsed, plan["file_payload"]))
                if file_task["status"] != "validated":
                    status = "incomplete" if file_task["status"] == "failed" else "needs_review"
                else:
                    file_result = file_task["value"]
                    source_props = file_properties(file_result)
                    explicit = [r for r in RISK_TYPE_VALUES if r in parsed["raw_text"]]
                    set_step("逐目标知识抽取", 文件=parsed["filename"], 目标数=len(selected))
                    completed = 0

                    async def work(unit):
                        nonlocal completed
                        payload = {**unit, "source_type": source_props["来源性质"], "title": parsed["title"], "explicit_risk_types": explicit}
                        task = await self._task(unit["target_id"], payload, CONTENT_PROMPT, CONTENT_SCHEMA, CONTENT_EXAMPLES,
                            lambda raw: validate_content(raw, unit, source_props, explicit))
                        if task["status"] == "validated":
                            value = task["value"]
                            for correction in value.get("normalizations", []):
                                self.stats.counts["field_normalizations"] += 1
                                self.stats.warn("field_normalized", correction["field"] + ": " + correction["reason"])
                                logger.info("paragraph normalization task=%s detail=%s", unit["target_id"],
                                            json.dumps(correction, ensure_ascii=False))
                            value["status"] = ("success" if value["decision"] == "知识" else
                                               "needs_review" if value["decision"] == "不确定" or unit["high_signal"] else "reasonable_empty")
                            if value["status"] == "needs_review":
                                self.stats.warn("uncertain_or_suspicious_empty", value["reason"], strong=True)
                        else:
                            value = {"target_id": unit["target_id"], **task}
                        value["paragraph_id"] = unit["paragraph_id"]
                        completed += 1
                        self.stats.counts["targets_"+value["status"]] += 1
                        update_progress(emit=True, 已完成目标=completed, 目标数=len(selected), 当前状态=value["status"])
                        return value

                    results = await asyncio.gather(*(work(u) for u in selected))
                    status = "incomplete" if self.stats.errors else "needs_review" if any(r["status"] == "needs_review" for r in results) else "partial_test" if partial else "success"
            self.stats.counts["unexecuted_targets"] = total - len(results)
            builder = ParagraphGraphBuilder()
            try:
                graph = builder.build(parsed, file_result, results)
                normative = builder.build(parsed, file_result, results, "规范知识")
                reference = builder.build(parsed, file_result, results, "参考知识")
            except ReviewRequired as exc:
                status = "needs_review"
                self.stats.warn("claim_identity_conflict", str(exc), strong=True)
                graph = normative = reference = {"nodes": [], "edges": [], "metadata": {"semantic_success": False}}
            kind_counts = Counter(n["properties"].get("使用定位") for n in graph["nodes"] if n["node_type"] == "段落知识单元")
            self.stats.counts.update(normative_knowledge=kind_counts["规范知识"], reference_knowledge=kind_counts["参考知识"],
                                     nodes=len(graph["nodes"]), edges=len(graph["edges"]))
            if status == "success" and not sum(kind_counts.values()):
                status = "no_knowledge"
            complete = run_llm and status in {"success", "no_knowledge"} and file_result is not None
            selected_graph = normative if self.config.publication == "normative" else reference
            eligible = complete and bool(kind_counts["规范知识" if self.config.publication == "normative" else "参考知识"])
            if len(results) < total and run_llm and status == "success":
                status, eligible = "partial_test", False
            by_target = {r["target_id"]: r["status"] for r in results}
            coverage = [{**r, "semantic_status": "not_a_target" if r["role"] != "target" else
                         [by_target.get(u["target_id"], "unexecuted") for u in plan["units"] if u["paragraph_id"] == r["paragraph_id"]] or ["needs_review"]}
                        for r in plan["coverage"]]
            result = {"status": status, "parse_result": parsed, "plan": plan, "file_result": file_result,
                      "results": results, "coverage": coverage, "graph": graph, "structure_graph": builder.structure(parsed),
                      "normative_graph": normative, "reference_graph": reference,
                      "publication_graph": selected_graph, "publication": self.config.publication,
                      "neo4j_graph": prepare_graph_for_neo4j(selected_graph),
                      "semantic_complete": complete, "formal_graph_eligible": complete and bool(kind_counts["规范知识"]),
                      "publish_graph_eligible": eligible, "stats": self.stats.to_dict(),
                      "configuration": {"protocol": "paragraph-grounded-v1", "model": self.config.model_name,
                          "max_attempts": self.config.max_attempts, "max_concurrent": self.config.max_concurrent,
                          "timeout": self.config.timeout, "paragraph_mode": self.config.paragraph_mode,
                          "target_chars": self.config.block_chars, "context_chars": self.config.context_chars,
                          "request_chars": self.config.request_chars, "publication": self.config.publication},
                      "runtime_identity": RUNTIME_IDENTITY}
            monitor.result(status, 0 if status in {"success", "no_knowledge", "structure_only"} else 1)
            return result
        finally:
            monitor.close(sys.exc_info()[0].__name__ if sys.exc_info()[0] else None)

    async def extract_text(self, text, filename="material.txt", source_path="", run_llm=False, max_targets=None):
        parsed = self.structure_parser.parse_text(text, filename, source_path)
        return await self.extract_from_parse_result(parsed, run_llm, max_targets)

    async def extract_file(self, path, output_dir=None, run_llm=False, max_targets=None):
        parsed = self.structure_parser.parse_file(path)
        folder = None
        if output_dir is not None:
            folder = Path(output_dir)/ (parsed["document_id"]+"_"+uuid.uuid4().hex[:8])
            folder.mkdir(parents=True, exist_ok=False)
            self.audit_path = folder/"attempts.jsonl"
        result = await self.extract_from_parse_result(parsed, run_llm, max_targets)
        if folder is not None:
            for name, value in (("parse", parsed), ("plan", result["plan"]), ("coverage", result["coverage"]),
                                ("extraction", {"file": result["file_result"], "results": result["results"]}),
                                ("graph", result["graph"]), ("summary", {k: result[k] for k in
                                  ("status", "semantic_complete", "formal_graph_eligible", "publish_graph_eligible", "publication", "stats")})):
                save_json(value, folder/(name+".json"))
            save_json({"configuration": result["configuration"], "runtime_identity": result["runtime_identity"]}, folder/"configuration.json")
            result["record_directory"] = str(folder)
        return result
