"""Opt-in live model validation; database writes are restricted to owned LOCAL labels."""

import asyncio
import argparse
import json
from pathlib import Path
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.infrastructure.graph_storage.subgraph_records import identifier, snapshot_hash
from app.infrastructure.risk_map import MappingConfig, ModelConfig
from app.infrastructure.risk_map.common.indicators import decode_candidates
from app.infrastructure.risk_map.common.io import atomic_json, content_hash, file_hash, read_json
from app.infrastructure.risk_map.common.model_client import ModelFailure, ModelSession, OpenAICompatibleClient, redact
from app.infrastructure.risk_map.common.stats import Diagnostics
from app.infrastructure.risk_map.config import CANDIDATES, FINAL, REVIEW_PROTOCOL
from app.infrastructure.risk_map.local_records import RunRecords
from app.infrastructure.risk_map.matching.location import SYSTEM as MATCH_SYSTEM, validate_match
from app.infrastructure.risk_map.preparation.config import DEFAULT_TREE_PATH
from app.infrastructure.risk_map.preparation.tree_index import build_tree_index
from app.infrastructure.risk_map.review.review import SYSTEM as REVIEW_SYSTEM, validate_removals, review_data
from app.infrastructure.risk_map import run_mapping as runner
from app.infrastructure.risk_map.test.run_validation import connect_local, database_state

PACKAGE = Path(__file__).resolve().parents[1]
PARAPHRASES = {
    0: "勘察设计单位需具备相应资质，不允许转包或违法分包已经承揽的工程。",
    1: "单位必须保障达到连续工作年限条件的职工享受带薪年休假。",
    2: "没有取得排污许可证的经营者不能排放污染物。",
    3: "单位欠缴社保时可责令补足，并依法采取划拨账户资金等措施追缴。",
    5: "受到强迫劳动或单位不支付劳动报酬等情形下，劳动者能够随时通知解除劳动合同。",
    6: "将特定形式标志用于指定中国的商标保护申请，需要按规定通过代理机构及时补交材料。",
}


class RecordedLiveClient:
    """Pass-through recorder, never generates or substitutes a model response."""

    def __init__(self, config, output, limit=80):
        self.config, self.output, self.limit = config, output, limit
        self.inner = OpenAICompatibleClient(config)
        self.records = []
        self.active = self.maximum = 0

    async def complete(self, payload):
        if len(self.records) >= self.limit:
            raise ModelFailure("live_test_request_limit_reached")
        record = {"request": len(self.records) + 1, "payload": redact(payload, self.config.api_key)}
        self.records.append(record)
        self.active += 1
        self.maximum = max(self.maximum, self.active)
        start = time.monotonic()
        try:
            response = await self.inner.complete(payload)
            record.update(status="received", response=redact(response, self.config.api_key))
            return response
        except BaseException as exc:
            record.update(status="failed", error=str(exc) if isinstance(exc, ModelFailure) else type(exc).__name__)
            raise
        finally:
            record["seconds"] = round(time.monotonic() - start, 3)
            self.active -= 1
            atomic_json(self.records, self.output / "live_requests.json", durable=True)

    async def aclose(self):
        await self.inner.aclose()


def authoritative_candidate(index, number):
    item = index.indicators[number]
    return {"指标名": item.name, "指标序号": number, "指标描述": item.description}


def fixture_tree(index, articles):
    result = {}
    for article in articles:
        for number in article["numbers"]:
            indicator = index.indicators[number]
            cursor = result
            for name, code in indicator.path[:-1]:
                cursor = cursor.setdefault(name, {"序号": code})
            leaf = cursor.setdefault(indicator.name, {"序号": number, "指标描述": indicator.description, "映射文件": {"中国": {}}})
            leaf["映射文件"]["中国"][article["filename"]] = {
                "原始文本": article["filename"], "已找到": article["filename"],
                "依据文件名": article["filename"], "知识依据": article["description"]}
    return result


async def diagnose_reviews(samples, index, output, model):
    """Probe the production response protocol; do not replace it with a test prompt."""
    client = RecordedLiveClient(model, output)
    session = ModelSession(model, client, output / "cache", lambda *a: None, scope=output.name)
    results = []
    try:
        for i in (0, 1, 5, 6):
            link = samples[i]["saved_link"]
            group = [authoritative_candidate(index, link["indicator"]["number"])]
            data = review_data({"text": link["text"], "number": link["number"], "filenames": link["file"],
                                "region": "中国"}, group)
            response = await session.ask("review", str(i), REVIEW_SYSTEM, data, lambda r: validate_removals(r, group))
            results.append({"sample": i, "data": data, "response": response})
    finally:
        await client.aclose()
        atomic_json(results, output / "diagnosis.json", durable=True)
    return results


async def live_checks(samples, versions, index, adapter, output, model):
    client = RecordedLiveClient(model, output)
    events = []
    session = ModelSession(model, client, output / "cache", lambda phase, row: events.append({"phase": phase, **row}),
                           scope="live_" + output.name)
    report = {"model": model.model, "endpoint": model.api_url, "database": "LOCAL Neo4j",
              "started": time.strftime("%Y-%m-%d %H:%M:%S"), "scope": "small_sample_not_production_accuracy",
              "review_protocol": REVIEW_PROTOCOL, "prompt_hash": content_hash(REVIEW_SYSTEM), "model_config": model.semantic}
    prefix = "RiskMapLive_" + uuid.uuid4().hex
    source, target = prefix + "_source", prefix + "_target"
    created = False
    before = database_state(adapter)
    try:
        # First actual request doubles as a connectivity and minimal protocol check.
        match_cases = [{"case": f"positive_{i}", "text": samples[i]["saved_link"]["text"],
                        "description": PARAPHRASES[i], "expected": "是"} for i in (0, 1, 2, 3)]
        match_cases += [
            {"case": "different_normative_topic", "text": samples[0]["saved_link"]["text"],
             "description": PARAPHRASES[1], "expected": "否"},
            {"case": "same_number_opposite_obligation", "text": "第一条 企业应当按时足额支付劳动报酬。",
             "description": "第一条 本条允许企业无理由拖欠劳动报酬。", "expected": "否", "controlled_text": True},
            {"case": "short_complete_clause", "text": "不得泄密。", "description": "本条禁止泄露秘密。",
             "expected": "是", "controlled_text": True},
            {"case": "worker_vs_employer_dismissal", "text": samples[5]["saved_link"]["text"],
             "description": "本条规定企业在海外解雇员工时应履行通知及经济补偿程序。", "expected": "否"},
        ]
        outcomes = []
        for case in match_cases:
            try:
                answer = await session.ask("matching", case["case"], MATCH_SYSTEM,
                    {"article_text": case["text"], "description": case["description"]}, validate_match)
                outcomes.append({**case, "answer": answer, "agrees": answer["match"] == case["expected"]})
            except ModelFailure as exc:
                outcomes.append({**case, "error": str(exc), "agrees": False})
                if len(outcomes) == 1:
                    report["match_cases"] = outcomes
                    report["status"] = "model_unavailable"
                    return report
        report["match_cases"] = outcomes
        review_cases = []
        for i, sample in enumerate(samples):
            link = sample["saved_link"]
            number = link["indicator"]["number"]
            candidate = authoritative_candidate(index, number)
            expected = "keep" if i < 4 else "remove" if i in (5, 6) else "manual_review"
            entry = {"sample": i, "article_text": link["text"], "candidate": candidate,
                     "reference_reason": sample["reason"], "expected": expected}
            try:
                removals = await session.ask("review", "historical_" + str(i), REVIEW_SYSTEM,
                    review_data({"text": link["text"], "number": link["number"], "filenames": link["file"], "region": "中国"}, [candidate]),
                    lambda response: validate_removals(response, [candidate]))
                entry["answer"] = removals
                numbers = removals["removed_indicator_numbers"]
                entry["agrees"] = (number not in numbers) if expected == "keep" else (
                    number in numbers if expected == "remove" else None)
            except ModelFailure as exc:
                entry.update(error=str(exc), agrees=False)
            review_cases.append(entry)
        report["review_cases"] = review_cases

        control_candidates = [authoritative_candidate(index, samples[i]["saved_link"]["indicator"]["number"])
                              for i in range(7)]
        all_supported = "\n".join(samples[i]["saved_link"]["text"] for i in range(4)) + (
            "\n企业收集、存储、使用、删除美欧用户个人信息，应符合相应当地隐私保护法规。"
            "\n企业在境外解雇员工，应符合当地法定通知期、经济补偿等解雇程序。"
            "\n企业在美欧注册商标、专利，应按当地规定合规注册、办理海关备案并及时维护有效期。")
        controls = [
            {"case": "seven_candidates_one_relevant", "text": samples[0]["saved_link"]["text"],
             "filenames": samples[0]["saved_link"]["file"], "region": "中国",
             "expected_removed": [c["指标序号"] for c in control_candidates[1:]], "controlled_text": False},
            {"case": "seven_candidates_all_supported", "text": all_supported,
             "filenames": ["协议控制文本_非真实法规.txt"], "region": "", "expected_removed": [], "controlled_text": True},
        ]
        for control in controls:
            try:
                removed = await session.ask("review", control["case"], REVIEW_SYSTEM,
                    review_data(control, control_candidates), lambda response: validate_removals(response, control_candidates))
                control["answer"] = removed
                control["agrees"] = set(removed["removed_indicator_numbers"]) == set(control["expected_removed"])
            except ModelFailure as exc:
                control.update(error=str(exc), agrees=False)
        report["review_controls"] = controls

        waste = versions[3]
        holdouts = [
            {"case": "real_waste_partial_support", "text": waste["text"], "number": waste["number"],
             "filenames": waste["file"], "region": "中国", "controlled_text": False,
             "candidate_numbers": ["2.2.3.2", "4.2.3.1"], "expected_removed": ["4.2.3.1"],
             "reference_reason": "废物运输限制支持按规定处理废弃物这一部分事项，但不支持年休假。"},
            {"case": "explicit_region_conflict", "text": "本条仅适用于中国境内的劳动合同解除，规定境内用人单位的通知和补偿程序，不适用于境外雇佣关系。",
             "filenames": ["地域边界控制文本_非真实法规.txt"], "region": "中国", "controlled_text": True,
             "candidate_numbers": ["4.11.3.2"], "expected_removed": ["4.11.3.2"],
             "reference_reason": "明确排除境外雇佣，不能直接作为海外当地解雇合规的依据。"},
        ]
        for case in holdouts:
            group = [authoritative_candidate(index, n) for n in case["candidate_numbers"]]
            case["candidates"] = group
            try:
                answer = await session.ask("review", case["case"], REVIEW_SYSTEM,
                                           review_data(case, group), lambda r: validate_removals(r, group))
                case["answer"] = answer
                case["agrees"] = set(answer["removed_indicator_numbers"]) == set(case["expected_removed"])
            except ModelFailure as exc:
                case.update(error=str(exc), agrees=False)
        report["holdout_reviews"] = holdouts

        articles = []
        for i in (0, 1, 2, 3, 5, 6):
            link = samples[i]["saved_link"]
            articles.append({"key": "historical_" + str(i), "text": link["text"], "number": link["number"],
                "filename": link["file"][0], "description": PARAPHRASES[i],
                "numbers": [link["indicator"]["number"]], "expected_final": [] if i in (5, 6) else [link["indicator"]["number"]]})
        for i, row in enumerate(versions[:2]):
            articles.append({"key": "revision_" + str(i), "text": row["text"], "number": row["number"],
                "filename": row["file"][0], "description": row["basis"], "numbers": row["indicator_numbers"],
                "expected_final": None, "actual_tree_filename": row["tree_file_identity"][1]})
        tree = fixture_tree(index, articles)
        # Version cases use the actual tree title, not the graph's suffixed filename.
        for article in articles:
            if "actual_tree_filename" in article:
                for root in tree.values():
                    def rename(node):
                        for child in list(node.values()):
                            if isinstance(child, dict):
                                rename(child)
                        files = node.get("映射文件", {}).get("中国", {})
                        if article["filename"] in files:
                            value = files.pop(article["filename"])
                            value["依据文件名"] = article["actual_tree_filename"]
                            files[article["actual_tree_filename"]] = value
                    rename(root)
        atomic_json(tree, output / "diagnostic_tree.json", durable=True)
        atomic_json(articles, output / "diagnostic_articles.json", durable=True)
        rows = [{"id": item["key"], "label": "法条", "graph_tag": source,
                 "条": item["number"], "法条全文": item["text"], "filename": [item["filename"]]}
                for item in articles]
        with adapter.driver.session(database=adapter.database) as db:
            db.run(f"UNWIND $rows AS row CREATE (n:{identifier(source)}) SET n=row", rows=rows).consume()
        created = True
        source_hash = snapshot_hash(adapter.read_subgraph_records(source))
        config = MappingConfig(source, target, tree_path=output / "diagnostic_tree.json", output_root=output / "pipeline",
            model=model, write_enabled=True, exclusive_target=True)
        summary, code = await runner.execute(config, adapter, client=client)
        actual = adapter.read_subgraph_records(target)
        records = RunRecords(config.output_root, summary["run_id"], resume=True)
        confirmed, pending, _ = records.batches()
        statuses = {row["n"]["properties"]["id"]: confirmed.get("review", {}).get(row["n"]["elementId"], {}).get("status")
                    for row in actual["nodes"]}
        by_key = {row["n"]["properties"]["id"]: row["n"]["properties"] for row in actual["nodes"]}
        pipeline_checks = []
        for article in articles:
            props = by_key.get(article["key"], {})
            final = decode_candidates(props.get(FINAL, [])) if FINAL in props else None
            candidate_objects = decode_candidates(props.get(CANDIDATES, []))
            recalled = set(article["numbers"]) <= {item["指标序号"] for item in candidate_objects}
            numbers = [item["指标序号"] for item in final] if final is not None else None
            pipeline_checks.append({"case": article["key"], "status": statuses.get(article["key"]),
                "candidates": candidate_objects, "candidate_recalled": recalled, "final": final,
                "expected_final": article["expected_final"],
                "agrees": recalled and set(numbers or []) == set(article["expected_final"]) if article["expected_final"] is not None and numbers is not None else None})
        report.update(pipeline_summary=summary, pipeline_exit_code=code, pipeline_checks=pipeline_checks,
            source_unchanged=source_hash == snapshot_hash(adapter.read_subgraph_records(source)),
            graph_complete=len(actual["nodes"]) == len(rows), diagnostic_tree_not_authoritative_mapping=True,
            status="tested")
        inspected, inspect_code = await runner.execute(config, adapter, action="inspect", run_id=summary["run_id"])
        report["inspect_verified"] = inspect_code == 0 and inspected.get("inspection", {}).get("status") == "verified"
        report["pipeline_review_records"] = list(confirmed.get("review", {}).values())
        report["reasons_local_only"] = all("reasons" not in row["n"]["properties"] and
                                            "review_protocol" not in row["n"]["properties"] and
                                            "三级指标映射状态" not in row["n"]["properties"] for row in actual["nodes"])
        firm = [item["agrees"] for section in (outcomes, review_cases, controls, holdouts, pipeline_checks)
                for item in section if item.get("agrees") is not None]
        report["semantic_checks"] = {"passed": sum(value is True for value in firm), "total": len(firm),
                                     "not_expert_gold_standard": True}
        pairs = []
        cases = [("historical_" + str(case["sample"]), [case["candidate"]],
                  [] if case["expected"] == "keep" else [case["candidate"]["指标序号"]], case.get("answer"))
                 for case in review_cases if case["expected"] != "manual_review"]
        cases += [(case["case"], case.get("candidates", control_candidates), case["expected_removed"], case.get("answer"))
                  for case in controls + holdouts]
        for name, group, expected_removed, answer in cases:
            for candidate in group:
                number = candidate["指标序号"]
                actual = number in answer["removed_indicator_numbers"] if answer is not None else None
                expected = number in expected_removed
                pairs.append({"case": name, "indicator_number": number, "indicator_name": candidate["指标名"],
                              "expected_removed": expected, "actual_removed": actual,
                              "false_rejection": actual is True and not expected,
                              "false_retention": actual is False and expected,
                              "reason": answer["reasons"].get(number) if answer else None})
        report["review_pair_diagnostics"] = {"pairs": pairs, "not_independent_accuracy_estimate": True,
            "false_rejections": sum(p["false_rejection"] for p in pairs),
            "false_retentions": sum(p["false_retention"] for p in pairs),
            "technical_missing": sum(p["actual_removed"] is None for p in pairs)}
        report["status"] = "passed_small_sample" if (code == 0 and all(firm) and report["source_unchanged"]
                                                       and report["graph_complete"] and report["inspect_verified"]) else "semantic_findings"
    finally:
        if created:
            with adapter.driver.session(database=adapter.database) as db:
                for label in (source, target):
                    if not label.startswith(prefix + "_"):
                        raise AssertionError("Refusing cleanup outside owned namespace")
                    db.run(f"MATCH (n:{identifier(label)}) DETACH DELETE n").consume()
        report.update(physical_requests=len(client.records), max_concurrent_requests=client.maximum,
            request_attempt_errors=session.counters["attempt_errors"], database_unchanged=before == database_state(adapter))
        review_events = [e for e in events if e["phase"] == "review" and "status" in e]
        report["direct_review_protocol"] = {"accepted": sum(e["status"] == "confirmed" for e in review_events),
                                            "failed_attempts": sum(e["status"] == "failed" for e in review_events)}
        atomic_json(events, output / "direct_protocol_events.json", durable=True)
        await client.aclose()
        atomic_json(report, output / "results.json", durable=True)
    return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", default=runner.MODEL_NAME, help="Explicit live-test-only model override")
    parser.add_argument("--diagnose-review", action="store_true", help="Alias for production-protocol review probe")
    parser.add_argument("--probe-review", action="store_true", help="Four production-protocol review probes")
    args = parser.parse_args()
    output = PACKAGE / "task" / "测试报告" / "任务2-优化三_真实模型验证" / ("run_" + uuid.uuid4().hex[:12])
    output.mkdir(parents=True, exist_ok=False)
    evidence = PACKAGE / "task" / "数据审查" / "审查结果" / "审查结果1"
    samples = read_json(evidence / "人工文本复核样例.json")
    versions = read_json(evidence / "版本后缀导致漏召回且正文相同明细.json")
    index = build_tree_index(read_json(DEFAULT_TREE_PATH), Diagnostics())
    atomic_json({"tree_hash": file_hash(DEFAULT_TREE_PATH), "historical_samples": str(evidence),
                 "review_protocol": REVIEW_PROTOCOL, "prompt_hash": content_hash(REVIEW_SYSTEM),
                 "notes": "Small diagnostic subset; expectations are engineering review, not expert gold labels."}, output / "inputs.json")
    # This does not use runner.NEO4J_URI or runner.main().
    adapter = connect_local()
    model = ModelConfig(model=args.model, api_url=runner.MODEL_API_URL, api_key=runner.API_KEY,
                        request_timeout=60, max_attempts=5, retry_delay=1, max_tokens=4096)
    try:
        if args.diagnose_review or args.probe_review:
            results = asyncio.run(diagnose_reviews(samples, index, output, model))
            print(json.dumps({"output": str(output), "diagnosis": results}, ensure_ascii=False, indent=2))
            return 0
        result = asyncio.run(live_checks(samples, versions, index, adapter, output, model))
        print(json.dumps({"output": str(output), "status": result.get("status"),
              "physical_requests": result["physical_requests"], "pipeline_exit_code": result.get("pipeline_exit_code"),
              "match_agreements": [r["agrees"] for r in result.get("match_cases", [])],
              "review_agreements": [r["agrees"] for r in result.get("review_cases", [])],
              "pipeline_agreements": [r["agrees"] for r in result.get("pipeline_checks", [])],
              "source_unchanged": result.get("source_unchanged"), "database_unchanged": result["database_unchanged"]}, ensure_ascii=False, indent=2))
        return 0 if result.get("status") == "passed_small_sample" and result.get("database_unchanged") else 2
    finally:
        adapter.disconnect()


if __name__ == "__main__":
    raise SystemExit(main())
