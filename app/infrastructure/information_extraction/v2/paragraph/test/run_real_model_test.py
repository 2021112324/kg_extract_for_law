"""Explicit bounded real-model acceptance, never a database write."""
import asyncio
from hashlib import sha256
import json
import logging
import os
from pathlib import Path
import sys
import uuid

REPO = next(p for p in Path(__file__).resolve().parents if (p/"app/infrastructure").is_dir())
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from app.infrastructure.information_extraction.v2.paragraph import ParagraphConfig, ParagraphExtractor
from app.infrastructure.information_extraction.v2.paragraph.io_utils import save_json, RedactedFormatter
from app.infrastructure.information_extraction.v2.paragraph.test.run_actual_samples import actual_samples, OUTPUT
from app.infrastructure.information_extraction.v2.rule.test.run_real_model_test import project_model_config


class RecordedSuccessClient:
    """Explicit test-only reuse of actual successful responses, always revalidated."""
    def __init__(self, backend, records):
        self.backend, self.records = backend, records
        self.new_calls, self.reused = 0, 0

    async def entity_and_relationship_extract(self, **kwargs):
        data = json.loads(kwargs["input_text"])
        key = json.dumps(data, ensure_ascii=False, sort_keys=True)
        if key in self.records:
            self.reused += 1
            return self.records.pop(key)
        self.new_calls += 1
        return await self.backend.entity_and_relationship_extract(**kwargs)


async def main():
    base = project_model_config()
    config = ParagraphConfig(model_name=os.getenv("PARAGRAPH_MODEL") or "glm-5.3-flash",
        api_url=os.getenv("PARAGRAPH_API_URL") or base.api_url,
        api_key=os.getenv("PARAGRAPH_API_KEY") or base.api_key, timeout=float(os.getenv("PARAGRAPH_TEST_TIMEOUT", "180")))
    if not all((config.model_name, config.api_url, config.api_key)):
        raise ValueError("Real model tests require explicit or existing project credentials")
    folder = OUTPUT/"真实模型"/uuid.uuid4().hex
    folder.mkdir(parents=True, exist_ok=False)
    handler = logging.StreamHandler()
    handler.setFormatter(RedactedFormatter([config.api_key]))
    file_handler = logging.FileHandler(folder/"run.log", encoding="utf-8")
    file_handler.setFormatter(RedactedFormatter([config.api_key]))
    logging.basicConfig(level=logging.INFO, handlers=[handler, file_handler])
    rows = []
    reuse_directory = os.getenv("PARAGRAPH_TEST_REUSE_DIR")
    records = {}
    if reuse_directory:
        for path in Path(reuse_directory).glob("*_attempts.jsonl"):
            with path.open(encoding="utf-8") as stream:
                for line in stream:
                    item = json.loads(line)
                    if item["status"] == "success" and item.get("validated", {}).get("decision") != "不确定":
                        records[json.dumps(item["input"], ensure_ascii=False, sort_keys=True)] = item["raw_response"]
    samples = actual_samples()
    controls = [
        ("测试指引.txt", "测试指引\n\n发布单位：示例机构\n\n本指引适用于各类企业。\n\n企业应当保存业务记录。\n\n但依法豁免的情形除外。", "condition_and_exception"),
        ("测试数量规定.txt", "测试数量规定\n\n发布单位：示例机构\n\n企业应当缴纳五千元以上一万元以下的罚款。", "quantitative_object"),
        ("测试观点说明.txt", "测试观点说明\n\n作者表示：甲专家认为，企业可以优先完善内部审计。", "attributed_opinion"),
    ]
    samples.extend((name, text, "synthetic:"+name, {"actual_source": False, "control": control}) for name, text, control in controls)
    only = os.getenv("PARAGRAPH_TEST_CASE")
    for filename, text, identity, source in samples:
        if only and only not in filename and only != source.get("control"):
            continue
        extractor = ParagraphExtractor(config)
        client = None
        if records:
            client = RecordedSuccessClient(extractor._client(), records)
            extractor._llm = client
        extractor.audit_path = folder/(filename+"_attempts.jsonl")
        result = await extractor.extract_text(text, filename, identity, run_llm=True)
        knowledge = [n["properties"] for n in result["graph"]["nodes"] if n["node_type"] == "段落知识单元"]
        checks = {"processing_complete": result["status"] in {"success", "no_knowledge"},
                  "no_fake_articles": not any(n["node_type"] in {"法条", "条款单元", "段落处理"} for n in result["graph"]["nodes"])}
        if source.get("selection", "").startswith("full_current_news"):
            checks.update(news_not_formal=not result["formal_graph_eligible"],
                          retained_reference=bool(knowledge) and all(p["使用定位"] == "参考知识" for p in knowledge),
                          publication_background_not_knowledge=not any(p["知识类型"] == "事实陈述" and "印发" in p["知识内容"] for p in knowledge))
        if source.get("selection", "").startswith("formal_source"):
            checks["formal_prose_has_normative_knowledge"] = bool(result["formal_graph_eligible"])
        if source.get("control") == "quantitative_object":
            checks["quantity_preserved"] = any(isinstance(p.get("量化条件"), dict) and p["量化条件"].get("最小值") == 5000
                                              and p["量化条件"].get("最大值") == 10000 for p in knowledge)
        if source.get("control") == "condition_and_exception":
            checks["exception_retained_with_normative_knowledge"] = any(p["使用定位"] == "规范知识" and "依法豁免" in
                (p["知识内容"] + str(p.get("适用前提", "")) + str(p.get("例外情形", ""))) for p in knowledge)
        if source.get("control") == "attributed_opinion":
            checks["opinion_not_obligation"] = bool(knowledge) and all(p["使用定位"] == "参考知识" and p.get("功能类型") is None for p in knowledge)
        if source.get("actual_source"):
            checks["source_unchanged"] = sha256(Path(source["original_path"]).read_bytes()).hexdigest() == source["original_sha256"]
        row = {**source, "filename": filename, "status": result["status"], "model": config.model_name,
               "checks": checks, "counts": result["stats"]["counts"], "errors": result["stats"]["errors"],
               "warnings": result["stats"]["warnings"], "passed": all(checks.values())}
        row["configuration"] = result["configuration"]
        row["runtime_identity"] = result["runtime_identity"]
        row["model_execution"] = {"new_backend_calls": client.new_calls if client else result["stats"]["counts"]["requests"],
                                  "reused_successful_responses": client.reused if client else 0,
                                  "reuse_directory": reuse_directory}
        rows.append(row)
        save_json(result, folder/(filename+"_result.json"))
        print(filename, result["status"], "passed=", row["passed"])
        save_json({"model": config.model_name, "samples": rows, "all_expected_passed": all(r["passed"] for r in rows),
                   "database_writes": 0, "full_corpus_semantic_acceptance": False}, folder/"真实模型测试汇总.json")
    print("record_directory:", folder)
    return 0 if rows and all(r["passed"] for r in rows) else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
