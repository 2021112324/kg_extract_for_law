"""Read-only real-export sampling and optional isolated local MySQL validation."""

import argparse
import hashlib
import json
import sys
import uuid
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.infrastructure.kg_to_natural_language.storage.config import SourceConfig, StorageConfig
from app.infrastructure.kg_to_natural_language.storage.pipeline import persist_knowledge, prepare_knowledge
from app.infrastructure.kg_to_natural_language.storage.records import digest


def run(nodes_path, edges_path, output, credentials=None):
    node_bytes, edge_bytes = Path(nodes_path).read_bytes(), Path(edges_path).read_bytes()
    nodes = json.loads(node_bytes.decode("utf-8-sig"))
    edges = json.loads(edge_bytes.decode("utf-8-sig"))
    by_element = {r["n"].get("elementId") for r in nodes}
    by_identity = {r["n"].get("identity") for r in nodes}
    def exists(edge, which):
        return edge.get(which + "NodeElementId") in by_element or edge.get(which) in by_identity
    dangling = sum(not exists(r["p"], "start") or not exists(r["p"], "end") for r in edges)
    filenames = Counter()
    for row in nodes:
        value = row["n"]["properties"].get("filename", [])
        for name in [value] if isinstance(value, str) else value:
            filenames[name] += 1
    if not filenames:
        raise ValueError("No source filenames for an explicit closed-file validation scope")
    name = sorted(filenames, key=lambda x: (-filenames[x], x))[0]
    selected = []
    for row in nodes:
        value = row["n"]["properties"].get("filename", [])
        if name in ([value] if isinstance(value, str) else value):
            selected.append(row)
    elements = {r["n"].get("elementId") for r in selected}
    identities = {r["n"].get("identity") for r in selected}
    def inside(edge, which):
        return edge.get(which + "NodeElementId") in elements or edge.get(which) in identities
    internal = [r for r in edges if inside(r["p"], "start") and inside(r["p"], "end")]
    snapshot = {"nodes": selected, "edges": internal, "sampling": {"method": "largest_exact_filename_closed_subset",
                "filename": name, "original_dangling_relationships": dangling,
                "original_nodes_sha256": hashlib.sha256(node_bytes).hexdigest(),
                "original_edges_sha256": hashlib.sha256(edge_bytes).hexdigest()}}
    label = selected[0]["n"].get("labels", ["法律法规条款"])[0]
    source = SourceConfig("chinese_law", "法律法规条款", label, str(nodes_path), str(edges_path),
                          graph_source="explicit-closed-validation-subset")
    config = StorageConfig("validation_" + uuid.uuid4().hex, [source], str(output))
    result = {"scope": snapshot["sampling"], "original": {"nodes": len(nodes), "edges": len(edges)},
              "selected": {"nodes": len(selected), "edges": len(internal)}, "database_test": "not_requested"}
    prepared = prepare_knowledge(config, snapshots=[snapshot])
    result["preparation"] = prepared
    if credentials:
        c = json.loads(Path(credentials).read_text(encoding="utf-8-sig"))
        if c.get("host") not in {"localhost", "127.0.0.1", "::1"}:
            raise ValueError("Only explicit local MySQL test connections are allowed")
        from sqlalchemy import create_engine, inspect, text
        from sqlalchemy.engine import URL
        from app.infrastructure.kg_to_natural_language.storage.repository import KnowledgeRepository
        from app.infrastructure.kg_to_natural_language.storage.query import QuerySpec, query_knowledge
        url = URL.create("mysql+pymysql", username=c["username"], password=c["password"], host=c["host"],
                         port=int(c["port"]), query={"charset": "utf8mb4"})
        admin = create_engine(url, hide_parameters=True)
        database = "risk_knowledge_test_" + uuid.uuid4().hex
        engine = None
        try:
            with admin.begin() as connection:
                connection.execute(text("CREATE DATABASE `" + database + "` CHARACTER SET utf8mb4"))
            engine = create_engine(url.set(database=database), hide_parameters=True)
            repository = KnowledgeRepository(engine)
            result["persistence"] = persist_knowledge(prepared["record_directory"], repository=repository)
            if not result["persistence"]["database_published"]:
                raise ValueError("Real-data test publication failed: " + str(result["persistence"].get("error_type")))
            result["query_count"] = query_knowledge(QuerySpec(config.dataset_version, page_size=1), engine)["total"]
            result["tables"] = inspect(engine).get_table_names()
            result["rerun"] = persist_knowledge(prepared["record_directory"], repository=repository)["status"]
            assert result["query_count"] == prepared["records"]
            assert result["tables"] == ["t_natural_knowledge_entry"]
            assert result["rerun"] == "already_published"
            result["database_test"] = "verified_then_test_database_removed"
        finally:
            if engine is not None:
                engine.dispose()
            with admin.begin() as connection:
                connection.execute(text("DROP DATABASE IF EXISTS `" + database + "`"))
            admin.dispose()
    if hashlib.sha256(Path(nodes_path).read_bytes()).hexdigest() != snapshot["sampling"]["original_nodes_sha256"]:
        raise ValueError("Original node file changed during validation")
    if hashlib.sha256(Path(edges_path).read_bytes()).hexdigest() != snapshot["sampling"]["original_edges_sha256"]:
        raise ValueError("Original relationship file changed during validation")
    result["original_files_unchanged"] = True
    target = Path(prepared["record_directory"]) / "real_validation.json"
    target.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--nodes", required=True)
    parser.add_argument("--edges", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--mysql-credentials")
    args = parser.parse_args()
    print(json.dumps(run(args.nodes, args.edges, args.output, args.mysql_credentials), ensure_ascii=False, indent=2))
