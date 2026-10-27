"""Consolidate actual verification artifacts without hiding earlier failures."""
import json
from pathlib import Path
import sys
import xml.etree.ElementTree as ET

REPO = next(p for p in Path(__file__).resolve().parents if (p / "app/infrastructure").is_dir())
sys.path.insert(0, str(REPO))
from app.infrastructure.information_extraction.v2.rule.io_utils import save_json

ROOT = REPO / "app/infrastructure/information_extraction/v2/task/测试报告/rule_测试记录"


def read(path):
    return json.loads(path.read_text(encoding="utf-8"))


def main():
    suite = ET.parse(ROOT / "离线及旧流程回归.xml").getroot().find("testsuite")
    tests = {k: int(suite.attrib[k]) for k in ("tests", "failures", "errors", "skipped")}
    counts = {}
    for case in suite.findall("testcase"):
        module = case.attrib["classname"].split(".v2.")[1].split(".")[0]
        counts[module] = counts.get(module, 0) + 1
    tests["by_module"] = counts
    model_root = ROOT / "真实模型"
    samples = []
    for name in ["父子独立", "全部条件整组", "继承前提", "定义与数量", "内部引用", "例外条件"]:
        folder = "编号与例外定向复测" if name == "例外条件" else "修订后复测"
        path = model_root / folder / (name + ".json")
        result = read(path)
        samples.append({"name": name, "passed": result["status"] == "success", "evidence": str(path.relative_to(ROOT))})
    for folder, prefix in [("修订后复测", "上海证券"), ("编号与例外定向复测", "电子商业")]:
        path = next(p for p in (model_root / folder / "实际条款抽样").rglob("kg.json") if p.parent.name.startswith(prefix))
        result = read(path)
        samples.append({"name": prefix + "首条抽样", "passed": result["status"] == "partial_test" and not result["formal_graph_eligible"],
                        "evidence": str(path.relative_to(ROOT))})
    normative = model_root / "真实规范条款抽样"
    for path in [normative / "单位复测/2.1.6.json", normative / "2.1.2.json"]:
        result = read(path)
        passed = result["passed"] and result["source_unchanged"] and not result["formal_graph_eligible"]
        if result["number"] == "2.1.6":
            conditions = [n["properties"].get("量化条件") for n in result["graph"]["nodes"] if n["node_type"] == "条款单元"]
            passed = passed and any(c and c.get("单位") == "交易日" for c in conditions)
        samples.append({"name": result["filename"] + " " + result["number"], "passed": bool(passed), "evidence": str(path.relative_to(ROOT))})
    local = read(ROOT / "本地Neo4j/本地Neo4j测试结果.json")
    offline = read(ROOT / "真实文件离线校验.json")
    rows = offline if isinstance(offline, list) else offline["files"]
    summary = {"offline_regression": tests, "real_model_samples": samples,
               "real_file_count": len(rows), "real_file_source_checks_passed": all(r["source_unchanged"] and r["source_offsets_verified"] for r in rows),
               "local_neo4j_passed": local["passed"], "original_graph_fingerprints": local["original_graph_fingerprints"],
               "full_corpus_semantic_acceptance": False,
               "remaining_limits": ["结构识别启发式仍需抽样检查", "复杂复合量化和周期频次不保证完整解析", "未执行全量真实文件模型及逐条人工验收"]}
    summary["all_scoped_checks_passed"] = tests["failures"] == tests["errors"] == 0 and all(r["passed"] for r in samples) and summary["real_file_source_checks_passed"] and local["passed"]
    save_json(summary, ROOT / "最终验证汇总.json")
    print(json.dumps({"tests": tests, "model_samples": len(samples), "all_scoped_checks_passed": summary["all_scoped_checks_passed"]}, ensure_ascii=False))
    if not summary["all_scoped_checks_passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
