"""Review real normative provisions without claiming whole-file extraction."""
import asyncio
import logging
import os
from pathlib import Path
import sys

REPO = next(p for p in Path(__file__).resolve().parents if (p / "app/infrastructure").is_dir())
sys.path.insert(0, str(REPO))
from app.infrastructure.information_extraction.v2.rule import RuleExtractor
from app.infrastructure.information_extraction.v2.rule.graph_builder import RuleGraphBuilder
from app.infrastructure.information_extraction.v2.rule.io_utils import save_json
from app.infrastructure.information_extraction.v2.rule.prompt.schema import schema_for_file_info, schema_for_clause
from app.infrastructure.information_extraction.v2.rule.prompt.prompt import FILE_PROMPT, CLAUSE_PROMPT
from app.infrastructure.information_extraction.v2.rule.prompt.example import example_for_file_info, example_for_clause
from app.infrastructure.information_extraction.v2.rule.validator import validate_result
from app.infrastructure.information_extraction.v2.rule.test.run_real_model_test import project_model_config

ROOT = Path(r"D:\CogmAIT\8.1项目\8.1数据\合规风险指标映射数据\v1_923\清洗名称后的纯文本数据\其他格式")
OUTPUT = REPO / "app/infrastructure/information_extraction/v2/task/测试报告/rule_测试记录/真实模型/真实规范条款抽样"


async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    config = project_model_config()
    rows = []
    output = OUTPUT / os.getenv("RULE_NORMATIVE_OUTPUT_SUBDIR", "本轮")
    for filename, number in [("上海证券交易所上市公司自律监管指引第1号——规范运作.txt", "2.1.6"),
                             ("电子商业汇票系统危机处置预案.txt", "2.1.2")]:
        if os.getenv("RULE_TEST_ACTUAL_FILES") and filename not in os.environ["RULE_TEST_ACTUAL_FILES"].split("|"):
            continue
        source = ROOT / filename
        before = source.read_bytes()
        extractor = RuleExtractor(config)
        extractor.audit_path = output / (number + "_attempts.jsonl")
        parsed = extractor.structure_parser.parse_file(source)
        plan = extractor.planner.plan(parsed)
        assert plan["formal_plan_eligible"]
        unit = next(u for u in plan["units"] if u["number"] == number)
        assert len(unit["chunks"]) == 1 and unit["mode"] == "independent"
        extractor.progress.update(filename=filename, stage="selected_real_provision")
        file_source = parsed["file_header"] + "\n" + plan["units"][0]["text"] + "\n" + plan["units"][-1]["text"]
        payload = {"number": number, "member_numbers": unit["member_numbers"], "mode": unit["mode"],
                   "target": unit["text"], "context": unit["context"], "path": unit["path"], "explicit_risk_types": []}
        async with extractor.heartbeat():
            file_info = await extractor._task("file_info", FILE_PROMPT, schema_for_file_info,
                {"title": parsed["title"], "source": file_source}, example_for_file_info,
                lambda raw: validate_result(raw, source=file_source, title=parsed["title"]))
            semantic = await extractor._task(number, CLAUSE_PROMPT, schema_for_clause, payload, example_for_clause,
                lambda raw: validate_result(raw, source=payload["target"] + "\n" + payload["context"], unit=payload))
        passed = bool(file_info and semantic and any(e["entity_type"] == "条款单元" for e in semantic["entities"]))
        content = [{"unit_id": unit["unit_id"], "chunk_index": 0, "status": "success", "validated": semantic}] if passed else []
        graph = RuleGraphBuilder().build(parsed, plan, file_info, content)
        row = {"filename": filename, "number": number, "passed": passed, "model": config.model_name,
               "status": "partial_test", "formal_graph_eligible": False, "source_sha256": parsed["source_sha256"],
               "source_unchanged": source.read_bytes() == before, "selected_unit": unit, "graph": graph,
               "stats": extractor.result_stats.to_dict(), "audit": extractor.audit}
        save_json(row, output / (number + ".json"))
        rows.append({k: row[k] for k in ("filename", "number", "passed", "status", "source_unchanged")})
        print(rows[-1])
    save_json({"samples": rows, "all_expected_passed": bool(rows) and all(r["passed"] and r["source_unchanged"] for r in rows)}, output / "汇总.json")
    if not all(r["passed"] and r["source_unchanged"] for r in rows):
        raise SystemExit(1)


if __name__ == "__main__":
    asyncio.run(main())
