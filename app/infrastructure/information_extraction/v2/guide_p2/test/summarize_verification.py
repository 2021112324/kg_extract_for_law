"""Consolidate saved evidence and revalidate sampled model outputs."""
import json
from pathlib import Path
import sys
import xml.etree.ElementTree as ET

REPO = next(p for p in Path(__file__).resolve().parents if (p / "app/infrastructure").is_dir())
sys.path.insert(0, str(REPO))
from app.infrastructure.information_extraction.v2.guide_p2.io_utils import save_json
from app.infrastructure.information_extraction.v2.guide_p2.validator import validate_batch

ROOT = REPO / "app/infrastructure/information_extraction/v2/task/测试报告/guide_p2_测试记录"


def read(path):
    return json.loads(path.read_text(encoding="utf-8"))


def main():
    suite = ET.parse(ROOT/"离线及旧流程回归.xml").getroot().find("testsuite")
    tests = {k: int(suite.attrib[k]) for k in ("tests", "errors", "failures", "skipped")}
    modules = {}
    for case in suite.findall("testcase"):
        name = case.attrib["classname"].split(".v2.")[1].split(".")[0]
        modules[name] = modules.get(name, 0)+1
    tests["by_module"] = modules
    offline = read(ROOT/"真实文件离线/真实文件离线校验.json")
    local = read(ROOT/"本地Neo4j/本地Neo4j测试结果.json")
    samples = []
    checks = {}
    for folder, prefix in [("范围完整性复测", "境外投资"), ("条件连接词复测", "第一批")]:
        directory = next(p.parent for p in (ROOT/"真实模型"/folder).rglob("kg.json") if p.parent.name.startswith(prefix))
        graph = read(directory/"kg.json")
        extraction = read(directory/"extraction.json")
        units = {u["entry_id"]: u for u in read(directory/"units.json")["units"]}
        for result in extraction["entries"]:
            assert result["status"] == "success"
            receipt = {"name": "verification_receipt", "entity_type": "条目处理", "properties": {
                "目标条目ID": result["entry_id"], "处理状态": result["decision"], "理由": result["reason"]}}
            validate_batch({"entities": [receipt]+result["entities"], "relations": result["relations"]}, [units[result["entry_id"]]])
        assert all(a <= 5 for a in extraction["attempts_by_entry"].values())
        assert not any(n["node_type"] == "条目处理" or "目标条目ID" in n["properties"] for n in graph["nodes"])
        knowledge = [n["properties"] for n in graph["nodes"] if n["node_type"] == "指引知识单元"]
        if prefix == "境外投资":
            checks["industry_no_universal_ban"] = all("禁止" not in k["知识内容"] and "不得" not in k["知识内容"] for k in knowledge)
            checks["industry_version_retained"] = all("2018" in k["知识内容"]+str(k.get("适用条件", "")) for k in knowledge)
            checks["industry_first_three_not_restricted"] = all("限制" not in k["知识内容"] for k in knowledge if k["原文依据"].startswith(("一、", "二、", "三、")))
        else:
            fridge = [k for k in knowledge if k["知识类型"] == "目录归属" and "家用电冰箱" in str(k.get("适用对象", ""))]
            checks["fridge_membership_keeps_capacity"] = bool(fridge) and all("500" in k["知识内容"]+str(k.get("适用条件", "")) for k in fridge)
            checks["product_is_partial_not_formal"] = graph["status"] == "partial_test" and not graph["formal_graph_eligible"]
        samples.append({"directory": str(directory.relative_to(ROOT)), "status": graph["status"], "executed_entries": len(extraction["entries"]),
                        "nodes": len(graph["nodes"]), "edges": len(graph["edges"]), "current_validator_passed": True})
    summary = {"regression": tests, "offline_sources": [{k: row[k] for k in ("filename", "counts", "source_unchanged", "source_offsets_verified")} for row in offline],
               "real_model_samples": samples, "targeted_semantic_checks": checks, "local_neo4j": local,
               "full_corpus_semantic_acceptance": False, "limitations": ["产品目录只执行3个范围，其余40个未执行", "模型知识粒度与复杂语义仍需复核", "未进行全量人工验收或现行效力核查"]}
    summary["all_scoped_checks_passed"] = tests["errors"] == tests["failures"] == 0 and all(checks.values()) and local["passed"] and all(row["source_unchanged"] and row["source_offsets_verified"] for row in offline)
    save_json(summary, ROOT/"最终验证汇总.json")
    print(json.dumps({"regression": tests, "checks": checks, "all_scoped_checks_passed": summary["all_scoped_checks_passed"]}, ensure_ascii=False))
    if not summary["all_scoped_checks_passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
