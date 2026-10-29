"""Real loopback Neo4j, injected control model; clean only the unique test scope."""
import asyncio
from hashlib import sha256
import json
import os
from pathlib import Path
import sys
import tempfile
from urllib.parse import urlsplit
import uuid

REPO = next(p for p in Path(__file__).resolve().parents if (p/"app/infrastructure").is_dir())
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from app.infrastructure.graph_storage.neo4j_adapter import Neo4jAdapter
from app.infrastructure.information_extraction.v2.paragraph import ParagraphConfig, ParagraphExtractor, ParagraphStandaloneRunner
from app.infrastructure.information_extraction.v2.paragraph.io_utils import save_json
from app.infrastructure.information_extraction.v2.paragraph.test.test_paragraph import FakeModel
from app.infrastructure.information_extraction.v2.paragraph.test.run_actual_samples import OUTPUT
from app.infrastructure.information_extraction.v2.rule.test.run_local_neo4j_test import counts, source_fingerprint


def quantity_model(value, data):
    if "target_id" in data and "五千元" in data["target"]:
        for e in value["entities"]:
            if e["type"] == "段落知识单元":
                e["attributes"].update(量化特征="定量", 量化条件={"原文件": "五千元以上一万元以下的罚款",
                    "量化值类型": "金额", "最小值": 5000, "最大值": 10000, "单位": "元", "约束关系": "区间"})
    return value


async def main():
    uri = os.getenv("PARAGRAPH_LOCAL_NEO4J_URI", "bolt://127.0.0.1:7687")
    endpoint = urlsplit(uri)
    if endpoint.hostname not in {"127.0.0.1", "localhost", "::1"} or endpoint.username or endpoint.password:
        raise ValueError("This test only accepts explicit loopback Neo4j URIs without embedded credentials")
    storage = Neo4jAdapter(uri=uri, username=os.getenv("PARAGRAPH_LOCAL_NEO4J_USERNAME", "neo4j"),
        password=os.getenv("PARAGRAPH_LOCAL_NEO4J_PASSWORD"), database=os.getenv("PARAGRAPH_LOCAL_NEO4J_DATABASE", "neo4j"),
        driver_options={"connection_timeout": 5})
    label = "paragraph_local_test_"+uuid.uuid4().hex
    folder = OUTPUT/"本地Neo4j"/label
    folder.mkdir(parents=True, exist_ok=False)
    before = after = fingerprint_before = None
    checked_empty = False
    try:
        if storage.connect() is not True:
            raise ConnectionError("Local Neo4j is unavailable; no remote fallback")
        storage.check_subgraph_destination(label)
        checked_empty = True
        before, fingerprint_before = counts(storage), source_fingerprint(storage, label)
        storage.disconnect()
        with tempfile.TemporaryDirectory(prefix="paragraph_fixture_", dir=folder) as directory:
            directory = Path(directory)
            assert directory.resolve().is_relative_to(folder.resolve())
            source = directory/"示例规定.txt"
            text = "示例规定\n\n发布单位：示例机构\n\n企业应当保存业务记录，并缴纳五千元以上一万元以下的罚款。"
            source.write_text(text, encoding="utf-8")
            source_hash = sha256(source.read_bytes()).hexdigest()
            config = ParagraphConfig(retry_delay=0)
            runner = ParagraphStandaloneRunner(config, storage, lambda config: ParagraphExtractor(config, FakeModel(transform=quantity_model)))
            result = await runner.run(source, folder/"run", "write", label)
            assert result["storage_status"] == "confirmed", result
            assert sha256(source.read_bytes()).hexdigest() == source_hash
        assert storage.connect() is True
        after = counts(storage)
        assert source_fingerprint(storage, label) == fingerprint_before
        snapshot = storage.read_subgraph_records(label)
        knowledge = [n["n"]["properties"] for n in snapshot["nodes"] if n["n"]["properties"].get("label") == "段落知识单元"]
        assert len(knowledge) == 1 and json.loads(knowledge[0]["量化条件"])["最大值"] == 10000
        assert after["constraints"] == before["constraints"] and after["indexes"] == before["indexes"]
        record = {"passed": True, "uri": uri, "test_label": label, "before": before, "after_write": after,
                  "source_unchanged": True, "original_graph_fingerprint": fingerprint_before,
                  "original_graph_unchanged_after_write": True, "no_new_schema_objects": True,
                  "semantic_model": "injected test oracle; real model tested separately"}
    except Exception as exc:
        save_json({"passed": False, "error_type": type(exc).__name__, "error": str(exc), "test_label": label}, folder/"测试失败.json")
        raise
    finally:
        if checked_empty and before is not None:
            if storage.driver is None:
                assert storage.connect() is True
            assert label.startswith("paragraph_local_test_") and len(label) == len("paragraph_local_test_")+32
            storage.delete_subgraph(label)
            cleaned = counts(storage)
            assert cleaned == before and source_fingerprint(storage, label) == fingerprint_before
            storage.disconnect()
    record.update(after_cleanup=cleaned, original_counts_restored=True)
    save_json(record, folder/"本地Neo4j测试结果.json")
    print(record)


if __name__ == "__main__":
    asyncio.run(main())
