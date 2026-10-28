"""Real local storage test; temporary label only, existing adapter and credentials."""
import asyncio
from pathlib import Path
import sys
import tempfile
import uuid

REPO = next(p for p in Path(__file__).resolve().parents if (p / "app/infrastructure").is_dir())
sys.path.insert(0, str(REPO))
from app.infrastructure.graph_storage.neo4j_adapter import Neo4jAdapter
from app.infrastructure.information_extraction.v2.guide_p2 import GuideP2Config, GuideP2Extractor
from app.infrastructure.information_extraction.v2.guide_p2.standalone import GuideP2StandaloneRunner
from app.infrastructure.information_extraction.v2.guide_p2.io_utils import save_json
from app.infrastructure.information_extraction.v2.guide_p2.test.test_guide_p2 import FakeModel
from app.infrastructure.information_extraction.v2.rule.test.run_local_neo4j_test import counts, source_fingerprint

OUTPUT = REPO / "app/infrastructure/information_extraction/v2/task/测试报告/guide_p2_测试记录/本地Neo4j"


async def main():
    OUTPUT.mkdir(parents=True, exist_ok=True)
    label = "p2_local_test_" + uuid.uuid4().hex
    storage = Neo4jAdapter(uri="bolt://127.0.0.1:7687", username="neo4j", database="neo4j", driver_options={"connection_timeout": 5})
    before = None
    try:
        assert storage.uri == "bolt://127.0.0.1:7687" and storage.connect() is True
        storage.check_subgraph_destination(label)
        before = counts(storage)
        fingerprint_before = source_fingerprint(storage, label)
        storage.disconnect()
        with tempfile.TemporaryDirectory(prefix="fixture_", dir=OUTPUT) as directory:
            source = Path(directory)/"本地测试目录.txt"
            assert source.resolve().is_relative_to(OUTPUT.resolve())
            text = "本地测试目录\n一、甲行业\n（一）甲产品"
            source.write_text(text, encoding="utf-8")
            runner = GuideP2StandaloneRunner(GuideP2Config(retry_delay=0), storage, lambda config: GuideP2Extractor(config, FakeModel()))
            summary = await runner.run(source, OUTPUT/label, "write", label)
            assert summary["storage_status"] == "confirmed", summary
            assert source.read_text(encoding="utf-8") == text
        assert storage.connect() is True
        after_write = counts(storage)
        fingerprint_after_write = source_fingerprint(storage, label)
        assert fingerprint_before == fingerprint_after_write
        rows = storage.run_query(f"MATCH(n:`{label}`) RETURN n.label AS type, n.知识类型 AS knowledge_type, n.约束强度 AS strength, n.上下文依据 AS evidence")
        knowledge = [r for r in rows if r["type"] == "指引知识单元"]
        assert len(knowledge) == 2 and all(r["knowledge_type"] == "目录归属" and r["strength"] == "不适用" and isinstance(r["evidence"], list) for r in knowledge)
        assert after_write["nodes"]-before["nodes"] == summary["stats"]["counts"]["nodes"]
        assert after_write["edges"]-before["edges"] == summary["stats"]["counts"]["edges"]
        assert after_write["constraints"] == before["constraints"] and after_write["indexes"] == before["indexes"]
    finally:
        if before is not None:
            if storage.driver is None:
                storage.connect()
            assert label.startswith("p2_local_test_") and len(label) == len("p2_local_test_")+32
            storage.delete_subgraph(label)
            after_cleanup = counts(storage)
            fingerprint_after_cleanup = source_fingerprint(storage, label)
            storage.disconnect()
    assert before == after_cleanup and fingerprint_before == fingerprint_after_cleanup
    record = {"passed": True, "uri": "bolt://127.0.0.1:7687", "label": label,
              "semantic_model": "fake; real model tested separately", "before": before, "after_write": after_write, "after_cleanup": after_cleanup,
              "source_unchanged": True, "no_new_schema_objects": True,
              "original_graph_fingerprints": {"before": fingerprint_before, "after_write": fingerprint_after_write, "after_cleanup": fingerprint_after_cleanup}}
    save_json(record, OUTPUT/"本地Neo4j测试结果.json")
    print(record)


if __name__ == "__main__":
    asyncio.run(main())
