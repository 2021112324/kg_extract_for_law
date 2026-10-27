"""Isolated real-local-Neo4j test, using the project's adapter and fake semantics."""
import asyncio
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import uuid

REPO = next(p for p in Path(__file__).resolve().parents if (p / "app/infrastructure").is_dir())
sys.path.insert(0, str(REPO))
from app.infrastructure.graph_storage.neo4j_adapter import Neo4jAdapter
from app.infrastructure.information_extraction.v2.rule import RuleConfig, RuleExtractor
from app.infrastructure.information_extraction.v2.rule.standalone import RuleStandaloneRunner
from app.infrastructure.information_extraction.v2.rule.io_utils import save_json
from app.infrastructure.information_extraction.v2.rule.test.test_rule import FakeModel

OUTPUT = REPO / "app/infrastructure/information_extraction/v2/task/测试报告/rule_测试记录/本地Neo4j"


def counts(storage):
    return {"nodes": storage.run_query("MATCH(n) RETURN count(n) AS count")[0]["count"],
            "edges": storage.run_query("MATCH()-[r]->() RETURN count(r) AS count")[0]["count"],
            "constraints": len(storage.run_query("SHOW CONSTRAINTS")), "indexes": len(storage.run_query("SHOW INDEXES"))}


def source_fingerprint(storage, label):
    digest = hashlib.sha256()
    queries = [
        "MATCH(n) WHERE NOT ($label IN labels(n)) RETURN elementId(n) AS id, labels(n) AS labels, properties(n) AS properties ORDER BY id",
        "MATCH(a)-[r]->(b) WHERE NOT ($label IN labels(a)) AND NOT ($label IN labels(b)) "
        "RETURN elementId(r) AS id,elementId(a) AS source,elementId(b) AS target,type(r) AS type,properties(r) AS properties ORDER BY id",
    ]
    for query in queries:
        for row in storage.run_query(query, {"label": label}):
            if "labels" in row:
                row["labels"] = sorted(row["labels"])
            digest.update(json.dumps(row, sort_keys=True, ensure_ascii=False, default=str).encode("utf-8"))
            digest.update(b"\n")
    return digest.hexdigest()


async def main():
    OUTPUT.mkdir(parents=True, exist_ok=True)
    label = "rule_local_test_" + uuid.uuid4().hex
    storage = Neo4jAdapter(uri="bolt://127.0.0.1:7687", username="neo4j", database="neo4j",
                           driver_options={"connection_timeout": 5})
    # Explicit URI prevents any remote default from being used.
    assert storage.uri == "bolt://127.0.0.1:7687"
    before = after_write = after_cleanup = None
    fingerprint_before = fingerprint_after_write = fingerprint_after_cleanup = None
    summary = None
    try:
        assert storage.connect() is True
        before = counts(storage)
        storage.check_subgraph_destination(label)
        fingerprint_before = source_fingerprint(storage, label)
        storage.disconnect()
        with tempfile.TemporaryDirectory(prefix="rule_fixture_", dir=OUTPUT) as directory:
            directory = Path(directory)
            assert directory.resolve().is_relative_to(OUTPUT.resolve())
            source = directory / "示例规则.txt"
            content = "示例规则\n1.1 企业应建立制度。\n1.1.1 企业应定期评估制度。"
            source.write_text(content, encoding="utf-8")
            runner = RuleStandaloneRunner(RuleConfig(retry_delay=0), storage,
                lambda config: RuleExtractor(config, FakeModel()))
            summary = await runner.run(source, OUTPUT / label, "write", label)
            assert summary["storage_status"] == "confirmed", summary
            assert source.read_text(encoding="utf-8") == content
        assert storage.connect() is True
        after_write = counts(storage)
        fingerprint_after_write = source_fingerprint(storage, label)
        assert fingerprint_after_write == fingerprint_before
        rows = storage.run_query(f"MATCH(n:`{label}`) RETURN n.label AS type,n.条 AS number")
        assert sorted(r["number"] for r in rows if r["type"] == "法条") == ["1.1", "1.1.1"]
        assert after_write["nodes"] - before["nodes"] == summary["stats"]["counts"]["nodes"]
        assert after_write["edges"] - before["edges"] == summary["stats"]["counts"]["edges"]
        assert after_write["constraints"] == before["constraints"] and after_write["indexes"] == before["indexes"]
    finally:
        if before is not None:
            if storage.driver is None:
                storage.connect()
            # A unique label was checked empty before this test; only its nodes are removed.
            assert label.startswith("rule_local_test_") and len(label) == len("rule_local_test_") + 32
            storage.delete_subgraph(label)
            after_cleanup = counts(storage)
            fingerprint_after_cleanup = source_fingerprint(storage, label)
            storage.disconnect()
    assert after_cleanup == before, (before, after_cleanup)
    assert fingerprint_before == fingerprint_after_cleanup
    record = {"passed": True, "uri": "bolt://127.0.0.1:7687", "test_label": label,
              "before": before, "after_write": after_write, "after_cleanup": after_cleanup,
              "source_unchanged": True, "semantic_model": "fake; real model is tested separately",
              "original_database_counts_restored": True, "no_new_schema_objects": True}
    record["original_graph_fingerprints"] = {"before": fingerprint_before, "after_write": fingerprint_after_write,
                                            "after_cleanup": fingerprint_after_cleanup}
    save_json(record, OUTPUT / "本地Neo4j测试结果.json")
    print(record)


if __name__ == "__main__":
    asyncio.run(main())
