"""Explicit real-model sample checks; never writes a database."""
import asyncio
import logging
import os
from pathlib import Path
import sys

REPO = next(p for p in Path(__file__).resolve().parents if (p / "app/infrastructure").is_dir())
sys.path.insert(0, str(REPO))
from app.infrastructure.information_extraction.v2.guide_p2 import GuideP2Config, GuideP2Extractor
from app.infrastructure.information_extraction.v2.guide_p2.io_utils import save_json
from app.infrastructure.information_extraction.v2.rule.test.run_real_model_test import project_model_config
from app.infrastructure.information_extraction.v2.guide_p2.test.run_actual_samples import ROOT, FILES

OUTPUT = REPO / "app/infrastructure/information_extraction/v2/task/测试报告/guide_p2_测试记录/真实模型"


async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    base = project_model_config()
    config = GuideP2Config(model_name=os.getenv("GUIDE_P2_MODEL") or base.model_name,
                          api_url=os.getenv("GUIDE_P2_API_URL") or base.api_url,
                          api_key=os.getenv("GUIDE_P2_API_KEY") or base.api_key)
    output = OUTPUT/os.getenv("GUIDE_P2_TEST_RUN", "首次")
    rows = []
    for filename in FILES:
        source = ROOT/filename
        if os.getenv("GUIDE_P2_TEST_FILE") and os.environ["GUIDE_P2_TEST_FILE"] != filename:
            continue
        before = source.read_bytes()
        extractor = GuideP2Extractor(config)
        parsed = extractor.structure_parser.parse_file(source)
        plan = extractor.planner.plan(parsed)
        selected = None if filename == FILES[0] else [u["entry_id"] for u in plan["units"] if
            u["number"] == "八、" or "1.家用电冰箱" in u["target"] or u["target"].startswith("根据《")]
        if selected is not None:
            assert selected
        result = await extractor.extract_file(source, output, run_llm=True, entry_ids=selected)
        expected = "success" if selected is None else "partial_test"
        rows.append({"filename": filename, "model": config.model_name, "status": result["status"], "expected_status": expected,
                     "formal_graph_eligible": result["formal_graph_eligible"], "source_unchanged": before == source.read_bytes(),
                     "counts": result["stats"]["counts"], "errors": result["stats"]["errors"]})
        print(rows[-1])
    summary = {"samples": rows, "all_expected_passed": bool(rows) and all(r["status"] == r["expected_status"] and r["source_unchanged"] for r in rows),
               "full_corpus_semantic_acceptance": False}
    save_json(summary, output/"真实模型测试汇总.json")
    if not summary["all_expected_passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    asyncio.run(main())
