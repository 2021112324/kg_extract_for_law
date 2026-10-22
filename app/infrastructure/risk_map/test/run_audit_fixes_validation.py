"""Audit fixtures, optional real model, and explicitly local database validation."""

import argparse
import asyncio
from datetime import date
import json
from pathlib import Path
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.infrastructure.graph_storage.subgraph_records import identifier, snapshot_hash
from app.infrastructure.risk_map import MappingConfig, ModelConfig, execute_mapping
from app.infrastructure.risk_map.common.io import atomic_json, content_hash, file_hash, read_json
from app.infrastructure.risk_map.common.model_client import ModelSession, OpenAICompatibleClient, redact
from app.infrastructure.risk_map.common.stats import Diagnostics
from app.infrastructure.risk_map.config import CANDIDATES, FINAL, PROTOCOL
from app.infrastructure.risk_map.matching.location import attachment_ids, locate_article
from app.infrastructure.risk_map.preparation.tree_index import build_tree_index
from app.infrastructure.risk_map.review.review import EXAMPLES, SYSTEM, review_data, validate_removals
from app.infrastructure.risk_map.test.run_validation import connect_local, database_state
from app.infrastructure.risk_map.test.test_audit_fixes import rename_tree
from app.infrastructure.risk_map.test.test_copy_first import TEXT, tree

PACKAGE = Path(__file__).resolve().parents[1]
EVIDENCE = PACKAGE / "task/数据审查/审查结果/审查结果2/行政监管规则_指标映射_20261021"
HISTORICAL_HASH = "f52d28d030cce3918b744f09749b695129e6951954601a4b2703f78461c8ae54"


def semantic_cases():
    cases = []
    for row in read_json(EVIDENCE / "定向文本审查样例及判定.json"):
        if row["judgment"].startswith("高风险"):
            continue
        cases.append({"case_id": row["target_element_id"], "kind": "historical_regression",
            "article": {"text": row["article_text"], "number": row["article_number"],
                        "filenames": row["filename"], "region": "中国"},
            "candidates": [row["indicator"]], "expected_removed": [] if row["judgment"] != "剔除合理"
                else [row["indicator"]["指标序号"]], "expected_basis": row["audit_reason"]})
    heldout = [
        ("经营者应当保存交易记录不少于两年。", "交易记录管理", "企业是否保存交易记录并按期报送", True),
        ("有重大失信行为的企业，一年内不得评为最高信用等级。", "企业信用等级", "企业是否达到最高信用等级，是否有失信记录", True),
        ("行政许可有效期届满需要继续经营的，应当在届满前申请延续。", "许可有效性管理", "企业许可证是否持续有效，变更是否及时办理", True),
        ("本办法自公布之日起施行。", "经营档案管理", "企业是否完整保存经营活动档案", False),
    ]
    for i, (text, name, description, keep) in enumerate(heldout):
        number = f"9.1.1.{i + 1}"
        cases.append({"case_id": "heldout_" + str(i), "kind": "synthetic_not_verbatim_prompt_example",
            "article": {"text": text, "number": "测试条", "filenames": ["独立测试材料"], "region": "中国"},
            "candidates": [{"指标序号": number, "指标名": name, "指标描述": description}],
            "expected_removed": [] if keep else [number], "expected_basis": "按部分规范支持口径的测试预期，非专家金标准"})
    for row in read_json(EVIDENCE / "技术失败法条.json"):
        cases.append({"case_id": row["target_element_id"], "kind": "historical_protocol_failure",
            "article": {"text": row["article_text"], "number": row["article_number"],
                        "filenames": row["filename"], "region": "中国"},
            "candidates": row["candidates"], "expected_removed": None,
            "expected_basis": "验证完整协议审查成功，不要求所有候选保留"})
    return cases


async def historical_checks(output):
    path = EVIDENCE / "历史指标映射树_审查用.json"
    assert file_hash(path) == HISTORICAL_HASH
    index = build_tree_index(read_json(path), Diagnostics())
    files = ["特别纳税调整实施办法（试行）", "产品质量监督抽查管理暂行办法"]
    recalled = {name: len(attachment_ids(index, [name], "中国")) for name in files}
    assert all(recalled.values())
    results = []
    class ControlledNonOverlap:
        async def complete(self, payload):
            return {"match": "否", "reason": "离线控制只核对自动原文重合，其他描述不作语义判定"}
    session = ModelSession(ModelConfig(retry_delay=0), ControlledNonOverlap(), output / "fixture_cache", lambda *a: None)
    for row in read_json(EVIDENCE / "文件名前缀差异导致的可恢复候选.json"):
        # Use the complete hash-exact historical index, not a hand-made attachment.
        article = {"id": row["target_element_id"], "text": row["article_text"],
                   "number": row["article_number"], "filenames": row["filename"]}
        result = await locate_article(article, index, "中国", session)
        numbers = {json.loads(v)["指标序号"] for v in result["properties"][CANDIDATES]}
        # Nonverbatim descriptions receive controlled negatives; this is a
        # recall regression, not a semantic judgment on all other attachments.
        results.append({"article_id": article["id"], "expected": row["indicator"]["指标序号"],
                        "candidate_recalled": row["indicator"]["指标序号"] in numbers,
                        "file_match": result.get("file_match"), "errors": result["errors"]})
    atomic_json({"tree_hash": HISTORICAL_HASH, "file_attachments": recalled,
                 "known_candidates": results, "controlled_client_calls": session.counters["requests"],
                 "real_model_calls": 0}, output / "historical_checks.json")
    return {"file_recall_passed": all(recalled.values()),
            "known_candidates_passed": all(row["candidate_recalled"] for row in results)}


async def real_model_checks(output, cases, rounds):
    from app.infrastructure.risk_map import run_mapping as runner
    model = ModelConfig(model=runner.MODEL_NAME, api_url=runner.MODEL_API_URL, api_key=runner.API_KEY,
                        request_timeout=90, max_attempts=5, retry_delay=0.2, max_tokens=4096)
    client = OpenAICompatibleClient(model)
    rows, counters = [], []
    atomic_json(cases, output / "semantic_cases.json")
    atomic_json({"system": SYSTEM, "examples": EXAMPLES, "hash": content_hash([SYSTEM, EXAMPLES]), "model": model.semantic,
                 "rounds": rounds, "gold_standard": False}, output / "model_identity.json")
    def event(phase, row):
        folder = output / "model_events"
        folder.mkdir(exist_ok=True)
        with (folder / (phase + ".jsonl")).open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(redact(row, model.api_key), ensure_ascii=False) + "\n")
    try:
        for trial in range(rounds):
            session = ModelSession(model, client, output / "cache", event, scope="audit_trial_" + str(trial))
            async def check(case):
                try:
                    answer = await session.ask("review", case["case_id"], SYSTEM,
                        review_data(case["article"], case["candidates"]),
                        lambda r: validate_removals(r, case["candidates"]), examples=EXAMPLES)
                    expected = case["expected_removed"]
                    return {"trial": trial + 1, "case_id": case["case_id"], "kind": case["kind"],
                        "protocol_passed": True, "semantic_passed": set(answer["removed_indicator_numbers"]) == set(expected)
                            if expected is not None else None, "response": answer}
                except Exception as exc:
                    return {"trial": trial + 1, "case_id": case["case_id"], "kind": case["kind"],
                            "protocol_passed": False, "semantic_passed": False if case["expected_removed"] is not None else None,
                            "error": redact(str(exc), model.api_key)}
            results = await asyncio.gather(*(check(case) for case in cases))
            rows.extend(results)
            counters.append(dict(session.counters))
            atomic_json(rows, output / "real_model_results.json")
            print(f"real model trial {trial + 1}/{rounds}: protocol={sum(r['protocol_passed'] for r in results)}/{len(results)}", flush=True)
    finally:
        await client.aclose()
    firm = [r for r in rows if r["semantic_passed"] is not None]
    return {"model": model.model, "cases_per_round": len(cases), "rounds": rounds,
            "protocol_passed": sum(r["protocol_passed"] for r in rows), "protocol_total": len(rows),
            "semantic_passed": sum(r["semantic_passed"] for r in firm), "semantic_total": len(firm),
            "counters": counters, "full_corpus_accuracy": None,
            "passed": all(r["protocol_passed"] for r in rows) and all(r["semantic_passed"] for r in firm)}


async def local_checks(output):
    adapter = connect_local()
    prefix = "RiskMapAudit_" + uuid.uuid4().hex
    source, target = prefix + "_source", prefix + "_target"
    before = database_state(adapter)
    try:
        data = rename_tree(tree(), "中华人民共和国测试法")
        mapping = data["产品法律风险"]["一级"]["二级"]["指标1"]["映射文件"]["中国"]
        mapping["空依据法"] = {"原始文本": "空依据法", "依据文件名": "空依据法", "已找到": "", "知识依据": " "}
        atomic_json(data, output / "local_tree.json")
        with adapter.driver.session(database=adapter.database) as session:
            session.run(f"CREATE (f:{identifier(source)} {{label:'法规文件', name:'测试法', graph_tag:$source}})", source=source).consume()
            for slot, filename in enumerate(("测试法(2019改编).txt", "测试法.txt", "空依据法.txt")):
                session.run(f"MATCH (f:{identifier(source)} {{label:'法规文件'}}) "
                    f"CREATE (a:{identifier(source)}), (f)-[:包含]->(a) SET a=$props",
                    props={"label": "法条", "id": "test_" + str(slot), "法条全文": TEXT, "条": "第一条",
                           "filename": [filename], "graph_tag": source, "slot": slot}).consume()
        original = adapter.read_subgraph_records(source)
        class Controlled:
            calls = 0
            async def complete(self, payload):
                self.calls += 1
                return '{"removed_indicator_numbers":[],"reasons":{},}'
        client = Controlled()
        config = MappingConfig(source, target, tree_path=output / "local_tree.json", output_root=output / "local_run",
            write_enabled=True, exclusive_target=True, model=ModelConfig(retry_delay=0))
        summary, code = await execute_mapping(config, adapter, client=client)
        copied = adapter.read_subgraph_records(target)
        records = [r["n"]["properties"] for r in copied["nodes"] if r["n"]["properties"].get("label") == "法条"]
        result = {"exit_code": code, "summary": summary, "target_nodes": len(copied["nodes"]),
            "target_relationships": len(copied["edges"]), "source_unchanged": snapshot_hash(original) == snapshot_hash(adapter.read_subgraph_records(source)),
            "no_graph_status": all("三级指标映射状态" not in p for p in records),
            "mapping_counts": sorted(len(p[FINAL]) for p in records), "controlled_model_calls": client.calls,
            "real_model_calls": 0, "test_labels": [source, target]}
        assert code == 0 and result["source_unchanged"] and result["no_graph_status"]
        assert result["mapping_counts"] == [0, 7, 7]
        assert summary["model"]["json_repair_successes"] >= 1
        assert summary["basis_skips"]["all_empty_articles"] == 1
    finally:
        with adapter.driver.session(database=adapter.database) as session:
            for label in (source, target):
                assert label.startswith(prefix + "_")
                session.run(f"MATCH (n:{identifier(label)}) DETACH DELETE n").consume()
        after = database_state(adapter)
        adapter.disconnect()
    result["database_before"], result["database_after"] = before, after
    result["database_members_and_schema_unchanged"] = before == after
    atomic_json(result, output / "local_results.json")
    return {"passed": before == after, "source_unchanged": result["source_unchanged"],
            "database_members_and_schema_unchanged": before == after}


async def run(args):
    output = args.output
    output.mkdir(parents=True, exist_ok=False)
    report = {"date": date.today().isoformat(), "protocol": PROTOCOL, "started": time.time()}
    try:
        report["historical"] = await historical_checks(output)
        if args.local:
            report["local"] = await local_checks(output)
        if args.real_model:
            report["real_model"] = await real_model_checks(output, semantic_cases(), args.rounds)
    finally:
        report["seconds"] = round(time.time() - report["started"], 2)
        atomic_json(report, output / "summary.json")
    print(json.dumps({"output": str(output), **report}, ensure_ascii=False, indent=2))
    return 0 if all(v.get("passed", all(v.values())) for v in
        (report.get("historical", {}), report.get("local", {}), report.get("real_model", {}))) else 1


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--local", action="store_true")
    parser.add_argument("--real-model", action="store_true")
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not 1 <= args.rounds <= 3:
        parser.error("rounds must be 1..3")
    return asyncio.run(run(args))


if __name__ == "__main__":
    raise SystemExit(main())
