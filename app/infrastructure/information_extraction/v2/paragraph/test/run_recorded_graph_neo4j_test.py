"""Write an explicitly selected real-result view to a unique local test label."""
import argparse
from collections import Counter
from hashlib import sha256
import json
import os
from pathlib import Path
import sys
from urllib.parse import urlsplit
import uuid

REPO = next(p for p in Path(__file__).resolve().parents if (p/"app/infrastructure").is_dir())
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from app.infrastructure.graph_storage.neo4j_adapter import Neo4jAdapter
from app.infrastructure.risk_map.common.io import content_hash
from app.infrastructure.information_extraction.v2.paragraph.extractor import RUNTIME_IDENTITY
from app.infrastructure.information_extraction.v2.paragraph.graph_builder import prepare_graph_for_neo4j
from app.infrastructure.information_extraction.v2.paragraph.io_utils import save_json, redact
from app.infrastructure.information_extraction.v2.paragraph.standalone import verify_snapshot
from app.infrastructure.information_extraction.v2.paragraph.test.run_actual_samples import OUTPUT
from app.infrastructure.information_extraction.v2.rule.test.run_local_neo4j_test import counts, source_fingerprint


def main(path, publication):
    result = json.loads(path.read_text(encoding="utf-8"))
    if not result["semantic_complete"] or result["status"] != "success":
        raise ValueError("Only complete successful real results may be tested")
    if result["runtime_identity"] != RUNTIME_IDENTITY:
        raise ValueError("Result code identity differs; re-extract or explicitly revalidate first")
    view = result[publication+"_graph"]
    if not any(n["node_type"] == "段落知识单元" for n in view["nodes"]):
        raise ValueError("The explicitly selected view has no knowledge")
    graph = prepare_graph_for_neo4j(view)
    uri = os.getenv("PARAGRAPH_LOCAL_NEO4J_URI", "bolt://127.0.0.1:7687")
    endpoint = urlsplit(uri)
    if endpoint.hostname not in {"localhost", "127.0.0.1", "::1"} or endpoint.username or endpoint.password:
        raise ValueError("Local-only Neo4j test; no embedded credentials")
    storage = Neo4jAdapter(uri=uri, username=os.getenv("PARAGRAPH_LOCAL_NEO4J_USERNAME", "neo4j"),
        password=os.getenv("PARAGRAPH_LOCAL_NEO4J_PASSWORD"), database=os.getenv("PARAGRAPH_LOCAL_NEO4J_DATABASE", "neo4j"),
        driver_options={"connection_timeout": 5})
    label = "paragraph_real_local_test_"+uuid.uuid4().hex
    folder = OUTPUT/"本地Neo4j"/label
    folder.mkdir(parents=True, exist_ok=False)
    safe_cleanup, before = False, None
    source = Path(result["parse_result"]["source_path"])
    source_hash = sha256(source.read_bytes()).hexdigest() if source.is_file() else None
    try:
        if storage.connect() is not True:
            raise ConnectionError("Local Neo4j unavailable; no remote fallback")
        storage.check_subgraph_destination(label)
        before, fingerprint = counts(storage), source_fingerprint(storage, label)
        safe_cleanup = True
        if storage.add_subgraph_with_merge(graph, label, "DomainLevel", result["parse_result"]["filename"]) is not True:
            raise RuntimeError("Test write failed or unknown; never replay")
        nodes = {n["node_id"]: content_hash({"name": n["node_name"], "type": n["node_type"], "properties": n["properties"]})
                 for n in graph["nodes"]}
        edges = Counter((e["source_id"], e["relation_type"], e["target_id"]) for e in graph["edges"])
        inspection = verify_snapshot(storage.read_subgraph_records(label), nodes, edges, label)
        after_write = counts(storage)
        assert after_write["constraints"] == before["constraints"] and after_write["indexes"] == before["indexes"]
        assert source_fingerprint(storage, label) == fingerprint
        if source_hash:
            assert sha256(source.read_bytes()).hexdigest() == source_hash
        record = {"passed": True, "test_label": label, "publication": publication,
            "real_model_result": str(path.resolve()), "real_model_result_sha256": sha256(path.read_bytes()).hexdigest(),
            "semantic_model": result["configuration"]["model"], "before": before, "after_write": after_write,
            "inspection": inspection, "original_graph_unchanged_after_write": True,
            "source_unchanged": True if source_hash else "not_a_direct_file_path",
            "no_new_schema_objects": True, "new_model_calls": 0}
    except Exception as exc:
        save_json({"passed": False, "error_type": type(exc).__name__, "error": redact(exc, (storage.password,)),
                   "test_label": label}, folder/"测试失败.json")
        raise
    finally:
        if safe_cleanup:
            assert label.startswith("paragraph_real_local_test_") and len(label) == len("paragraph_real_local_test_")+32
            storage.delete_subgraph(label)
            after_cleanup = counts(storage)
            assert after_cleanup == before and source_fingerprint(storage, label) == fingerprint
        storage.disconnect()
    record.update(after_cleanup=after_cleanup, original_counts_restored=True)
    save_json(record, folder/"真实抽取图谱本地测试结果.json")
    print(json.dumps(record, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("result_path", type=Path)
    parser.add_argument("--publication", choices=("normative", "reference"), required=True)
    args = parser.parse_args()
    main(args.result_path, args.publication)
