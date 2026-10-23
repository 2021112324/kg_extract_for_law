"""Opt-in LOCAL Neo4j regression and recorded, optional live-model checks."""
# ruff: noqa: E402

import argparse
import asyncio
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime
import io
import json
import logging
import os
from pathlib import Path
import sys
import time
import unittest
import uuid

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT))

from app.infrastructure.graph_storage.subgraph_records import digest, identifier, snapshot_hash
from app.infrastructure.risk_map import MappingConfig, ModelConfig
from app.infrastructure.risk_map import run_mapping as runner
from app.infrastructure.risk_map.common.indicators import decode_candidates
from app.infrastructure.risk_map.common.io import atomic_json, file_hash, read_json
from app.infrastructure.risk_map.common.model_client import redact
from app.infrastructure.risk_map.config import CANDIDATES, FINAL
from app.infrastructure.risk_map.local_records import RunRecords
from app.infrastructure.risk_map.preparation.config import DEFAULT_TREE_PATH
from app.infrastructure.risk_map.test.run_real_model_validation import RecordedLiveClient
from app.infrastructure.risk_map.test.run_validation import connect_local, database_state
from app.infrastructure.risk_map.test.test_copy_first import KEEP_ALL, payload_data
from app.infrastructure.risk_map.test.test_scoped_aliases import files, named_tree, provenance_for

PACKAGE = Path(__file__).resolve().parents[1]
TEXT = "第一条 企业应当建立信息安全管理制度，定期开展风险评估，保存评估记录并采取整改措施。"
BASIS = "企业必须制定信息安全管理规则，按期识别评估风险，留存评估材料并落实问题整改。"
DESCRIPTIONS = [
    "企业是否建立信息安全管理制度并向主管部门报送制度备案材料",
    "企业是否定期开展风险评估并委托独立机构审计",
    "企业是否保存评估记录并每年公开披露风险报告",
    "企业是否针对风险评估发现的问题采取整改措施并公示整改结果",
    "企业是否建立信息安全管理制度并安排人员参加专项培训",
    "企业是否定期开展风险评估并向行业协会申报评估等级",
    "企业是否保存风险评估记录并向境外监管机构报送记录",
]
CASE_NAMES = (
    "local_alias_only", "local_bilingual_and_resume", "local_shared_alias_conflict",
    "local_empty_upstream_ambiguity", "live_seven_partial_support",
    "live_mixed_support_and_exclusion", "live_actual_law_current_full_tree",
)


class ControlledClient:
    def __init__(self):
        self.records = []

    async def complete(self, payload):
        data = payload_data(payload)
        response = KEEP_ALL if "candidates" in data else '{"match":"是","reason":"受控测试：内容对应"}'
        self.records.append({"payload": payload, "response": response})
        return response


def synthetic(count=7):
    data = named_tree(count, ["Example Information Security Act"], BASIS)
    for i in range(1, count + 1):
        data["产品法律风险"]["一级"]["二级"][f"指标{i}"]["指标描述"] = DESCRIPTIONS[i - 1]
    return data


def graph_signature(snapshot, label, *, mapped=False):
    """Compare properties and edge multiplicity independently of physical IDs."""
    nodes, edges, slots = [], [], {}
    for row in snapshot["nodes"]:
        n = row["n"]
        props = dict(n["properties"])
        if mapped and props.get("label") == "法条":
            props.pop(CANDIDATES, None)
            props.pop(FINAL, None)
        props.pop("graph_tag", None)
        slots[n["elementId"]] = props["test_slot"]
        nodes.append((props["test_slot"], digest(props), sorted(set(n["labels"]) - {label})))
    for row in snapshot["edges"]:
        p = row["p"]
        props = dict(p["properties"])
        props.pop("graph_tag", None)
        edges.append((slots[p["startNodeElementId"]], p["type"], slots[p["endNodeElementId"]], digest(props)))
    return sorted(nodes), sorted(edges)


async def run_case(adapter, output, name, data, article_text, filenames, model, *, live=False,
                   expected=None, expected_code=0, source_status=None, check_resume=False,
                   tree_path=None, provenance_path=None, article_number="第一条"):
    case = output / name
    case.mkdir()
    if tree_path is None:
        tree_path, provenance_path = case / "tree.json", case / "provenance.json"
        atomic_json(data, tree_path)
        atomic_json(provenance_for(data, source_status), provenance_path)
    hashes = {str(p): file_hash(p) for p in (tree_path, provenance_path)}
    prefix = "RiskMapAliasTest_" + uuid.uuid4().hex
    source, target = prefix + "_source", prefix + "_target"
    config = MappingConfig(source, target, tree_path=tree_path, provenance_path=provenance_path,
        output_root=case / "run", model=model, write_enabled=True, exclusive_target=True)
    client = RecordedLiveClient(model, case, limit=30) if live else ControlledClient()
    result = {"name": name, "live_model": live, "source_label": source, "target_label": target,
              "expected_final_numbers": expected, "expected_exit_code": expected_code,
              "input_hashes": hashes, "checks": {}}
    try:
        rows = [
            {"id": "file", "label": "法规文件", "name": filenames[0], "filename": filenames,
             "graph_tag": source, "test_slot": 0},
            {"id": "article", "label": "法条", "name": article_number, "条": article_number,
             "法条全文": article_text, "filename": filenames, "graph_tag": source, "test_slot": 1,
             "zero": 0, "false": False, "empty": []},
            {"id": "orphan", "label": "测试孤立节点", "graph_tag": source, "test_slot": 2},
        ]
        with adapter.driver.session(database=adapter.database) as session:
            session.run(f"UNWIND $rows AS row CREATE (n:{identifier(source)}) SET n=row", rows=rows).consume()
            for _ in range(2):
                session.run(f"MATCH (a:{identifier(source)} {{test_slot:0}}),(b:{identifier(source)} {{test_slot:1}}) "
                            "CREATE (a)-[r:包含]->(b) SET r.graph_tag=$source, r.test_value=0", source=source).consume()
            session.run(f"MATCH (a:{identifier(source)} {{test_slot:1}}) CREATE (a)-[r:测试自环]->(a) "
                        "SET r.graph_tag=$source", source=source).consume()
        before = adapter.read_subgraph_records(source)
        summary, code = await runner.execute(config, adapter, client=client)
        result.update(summary=summary, exit_code=code)
        actual = adapter.read_subgraph_records(target)
        # Plain Neo4j properties in these fixtures need no private-value serializer.
        atomic_json(actual, case / "target_graph.json")
        source_after = adapter.read_subgraph_records(source)
        props = next(row["n"]["properties"] for row in actual["nodes"] if row["n"]["properties"]["test_slot"] == 1)
        final = [c["指标序号"] for c in decode_candidates(props.get(FINAL, []))]
        candidate = [c["指标序号"] for c in decode_candidates(props.get(CANDIDATES, []))]
        records = RunRecords(config.output_root, summary["run_id"], resume=True)
        confirmed, _, _ = records.batches()
        matching = list(confirmed["matching"].values())
        review = list(confirmed["review"].values())
        atomic_json({"matching": matching, "review": review}, case / "stage_records.json")
        result.update(final_numbers=final, candidate_numbers=candidate,
                      nodes=len(actual["nodes"]), relationships=len(actual["edges"]),
                      physical_model_requests=len(client.records) if live else 0,
                      controlled_requests=0 if live else len(client.records))
        checks = result["checks"]
        checks.update(exit_code=code == expected_code,
            source_unchanged=snapshot_hash(before) == snapshot_hash(source_after),
            complete_independent_copy=graph_signature(before, source) == graph_signature(actual, target, mapped=True),
            no_shared_physical_nodes=not ({r["n"]["elementId"] for r in before["nodes"]} &
                                         {r["n"]["elementId"] for r in actual["nodes"]}),
            no_diagnostic_graph_fields=not ({"三级指标映射状态", "source_verification", "name_hits"} & props.keys()),
            frozen_provenance_exact=(records.path / "inputs/provenance.json").read_bytes() == provenance_path.read_bytes(),
            input_files_unchanged=hashes == {str(p): file_hash(p) for p in (tree_path, provenance_path)},
            local_name_trace=bool(matching and matching[0].get("file_match", {}).get("name_hits")))
        if expected is not None:
            checks["expected_final_numbers"] = set(final) == set(expected)
        if source_status:
            checks["empty_basis_does_not_call_model"] = not client.records
            checks["source_ambiguity_recorded_locally"] = "source_ambiguity" in summary.get("basis_skips", {}).get("by_reason", {})
        if check_resume:
            target_hash = snapshot_hash(actual)
            summary_file = records.path / "summary.json"
            summary_bytes = summary_file.read_bytes()
            original = provenance_path.read_bytes()
            calls = len(client.records)
            try:
                # Deliberate mutation of this synthetic fixture ONLY.
                provenance_path.write_bytes(original + b"\n")
                rejected, reject_code = await runner.execute(config, adapter, action="resume",
                    run_id=summary["run_id"], client=client)
                atomic_json(rejected, case / "rejected_resume.json")
                checks["changed_provenance_rejects_resume"] = reject_code != 0 and "provenance" in rejected.get("error", "")
                checks["rejected_resume_preserves_target"] = target_hash == snapshot_hash(adapter.read_subgraph_records(target))
                checks["rejected_resume_preserves_summary"] = summary_file.read_bytes() == summary_bytes
                checks["rejected_resume_no_model_calls"] = len(client.records) == calls
            finally:
                provenance_path.write_bytes(original)
        result["passed"] = all(checks.values())
    except Exception as exc:
        result.update(passed=False, error=redact(str(exc), model.api_key), error_type=type(exc).__name__)
    finally:
        if live:
            await client.aclose()
        with adapter.driver.session(database=adapter.database) as session:
            for label in (source, target):
                if not label.startswith(prefix + "_"):
                    raise AssertionError("cleanup ownership mismatch")
                session.run(f"MATCH (n:{identifier(label)}) DETACH DELETE n").consume()
            result["cleanup_remaining_nodes"] = session.run(
                "MATCH (n) WHERE any(label IN labels(n) WHERE label STARTS WITH $prefix) RETURN count(n) AS n",
                prefix=prefix + "_").single()["n"]
        result["passed"] = result.get("passed", False) and result["cleanup_remaining_nodes"] == 0
        atomic_json(result, case / "result.json", durable=True)
    print(json.dumps({k: result.get(k) for k in ("name", "passed", "candidate_numbers", "final_numbers", "physical_model_requests", "error")}, ensure_ascii=False), flush=True)
    return result


async def cases(adapter, output, live, model, selected=None):
    result = []
    control = ModelConfig(model="controlled-no-network", retry_delay=0)
    numbers = [f"2.1.1.{i}" for i in range(1, 8)]
    selected = set(selected or CASE_NAMES)
    if "local_alias_only" in selected:
        result.append(await run_case(adapter, output, "local_alias_only", synthetic(), TEXT,
            ["Example Information Security Act.md"], control, expected=numbers))
    if "local_bilingual_and_resume" in selected:
        result.append(await run_case(adapter, output, "local_bilingual_and_resume", synthetic(), TEXT,
            ["测试法.txt", "Example Information Security Act"], control, expected=numbers, check_resume=True))
    if "local_shared_alias_conflict" in selected:
        ambiguous = synthetic(1)
        files(ambiguous)["另一部法"] = {"原始文本": "另一部法", "依据文件名": "另一部法",
            "别名": ["Example Information Security Act"], "已找到": "", "知识依据": BASIS}
        result.append(await run_case(adapter, output, "local_shared_alias_conflict", ambiguous, TEXT,
            ["Example Information Security Act"], control, expected=[], expected_code=2))
    if "local_empty_upstream_ambiguity" in selected:
        result.append(await run_case(adapter, output, "local_empty_upstream_ambiguity",
            named_tree(1, ["Example Information Security Act"], ""), TEXT,
            ["Example Information Security Act"], control, expected=[], source_status="匹配歧义"))
    if live:
        if not model.api_key or not model.api_url or not model.model:
            raise ValueError("Live model configuration is incomplete; no credentials were printed")
        if "live_seven_partial_support" in selected:
            result.append(await run_case(adapter, output, "live_seven_partial_support", synthetic(), TEXT,
                ["Example Information Security Act.md"], model, live=True, expected=numbers))
        if "live_mixed_support_and_exclusion" in selected:
            mixed = synthetic(3)
            mixed["产品法律风险"]["一级"]["二级"]["指标3"]["指标描述"] = "企业是否按月足额缴纳员工社会保险费"
            result.append(await run_case(adapter, output, "live_mixed_support_and_exclusion", mixed, TEXT,
                ["测试法.txt", "Example Information Security Act"], model, live=True, expected=numbers[:2]))
        # An existing saved source-law excerpt, not invented source text. The full
        # current tree and its real provenance remain read-only in this case.
        if "live_actual_law_current_full_tree" in selected:
            sample_path = PACKAGE / "task/数据审查/审查结果/审查结果1/人工文本复核样例.json"
            sample = read_json(sample_path)[0]["saved_link"]
            case = await run_case(adapter, output, "live_actual_law_current_full_tree", None,
                sample["text"], sample["file"], model, live=True, article_number=sample["number"],
                tree_path=DEFAULT_TREE_PATH, provenance_path=DEFAULT_TREE_PATH.with_name("条款知识来源明细.json"))
            case["sample_source"] = str(sample_path)
            case["expected_supported_indicator"] = sample["indicator"]["number"]
            case["checks"]["known_supported_indicator_retained"] = sample["indicator"]["number"] in case.get("final_numbers", [])
            case["passed"] = case["passed"] and case["checks"]["known_supported_indicator_retained"]
            atomic_json(case, output / case["name"] / "result.json", durable=True)
            result.append(case)
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True, help="New evidence folder; never overwrite existing results")
    parser.add_argument("--live", action="store_true", help="Allow real model calls using current runner model settings")
    parser.add_argument("--model", default=runner.MODEL_NAME)
    parser.add_argument("--case", action="append", choices=CASE_NAMES, help="Repeat to select cases; database regression always runs")
    args = parser.parse_args()
    if not args.live and args.case and any(name.startswith("live_") for name in args.case):
        parser.error("Live cases require --live")
    args.output.mkdir(parents=True, exist_ok=False)
    os.environ["RISK_MAP_LOCAL_NEO4J_TEST"] = "1"
    model = ModelConfig(model=args.model, api_url=runner.MODEL_API_URL, api_key=runner.API_KEY,
                        request_timeout=90, max_attempts=5, retry_delay=1, max_tokens=4096)
    adapter = connect_local()  # Rejects non-loopback endpoints; does not use runner DB settings.
    log = io.StringIO()
    handler = logging.StreamHandler(log)
    logger = logging.getLogger("app.infrastructure.risk_map")
    logger.addHandler(handler)
    started = time.monotonic()
    summary = {"started": datetime.now().astimezone().isoformat(), "local_database_only": True,
               "real_model_enabled": args.live, "model": model.model, "model_endpoint": model.api_url}
    try:
        before = database_state(adapter)
        summary.update(database_before=before, database_version=adapter.verify_subgraph_support())
        suite = unittest.defaultTestLoader.loadTestsFromName("app.infrastructure.risk_map.test.test_neo4j_integration")
        stream = io.StringIO()
        with redirect_stdout(stream), redirect_stderr(stream):
            tests = unittest.TextTestRunner(stream=stream, verbosity=2).run(suite)
        (args.output / "local_unittest.log").write_text(stream.getvalue(), encoding="utf-8")
        summary["local_regression"] = {"tests": tests.testsRun, "failures": len(tests.failures),
                                      "errors": len(tests.errors), "skipped": len(tests.skipped)}
        print(json.dumps(summary["local_regression"]), flush=True)
        summary["cases"] = asyncio.run(cases(adapter, args.output, args.live, model, args.case))
        summary["passed"] = tests.wasSuccessful() and all(c["passed"] for c in summary["cases"])
    except Exception as exc:
        summary.update(passed=False, error=redact(str(exc), model.api_key), error_type=type(exc).__name__)
    finally:
        summary["database_after"] = database_state(adapter)
        summary["database_members_and_schema_unchanged"] = summary.get("database_before") == summary["database_after"]
        summary["passed"] = summary.get("passed", False) and summary["database_members_and_schema_unchanged"]
        summary["seconds"] = round(time.monotonic() - started, 2)
        summary["real_model_requests"] = sum(c.get("physical_model_requests", 0) for c in summary.get("cases", []))
        atomic_json(summary, args.output / "validation_results.json", durable=True)
        (args.output / "execution.log").write_text(redact(log.getvalue(), model.api_key), encoding="utf-8")
        logger.removeHandler(handler)
        adapter.disconnect()
    print(json.dumps({k: summary.get(k) for k in ("passed", "seconds", "real_model_requests",
          "database_members_and_schema_unchanged", "error")}, ensure_ascii=False), flush=True)
    return 0 if summary["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
