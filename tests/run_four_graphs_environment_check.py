"""Opt-in: read real Neo4j samples and verify the runner on isolated local MySQL."""

import argparse
from collections import Counter
import json
import logging
from pathlib import Path
import sys
import uuid
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def read_samples():
    from neo4j import GraphDatabase, Query, READ_ACCESS
    from app.core.config import settings
    from app.infrastructure.graph_storage.subgraph_records import node_to_dict, relationship_to_dict
    from app.infrastructure.kg_to_natural_language import run_four_graphs_to_mysql as runner

    snapshots, statistics = [], {}
    file_types = {"chinese_law": ["法规文件"], "national_standard": ["标准文件"],
                  "guide": ["合规指引文件", "法规文件"]}
    driver = GraphDatabase.driver(settings.NEO4J_URI,
                                 auth=(settings.NEO4J_USERNAME, settings.NEO4J_PASSWORD),
                                 connection_timeout=10, max_transaction_retry_time=0)
    try:
        driver.verify_connectivity()
        with driver.session(database=settings.NEO4J_DATABASE, default_access_mode=READ_ACCESS) as session:
            for label, converter in runner.GRAPH_LABELS:
                quoted = "`" + label.replace("`", "``") + "`"
                types = [dict(r) for r in session.run(Query(
                    f"MATCH (n:{quoted}) RETURN n.label AS node_type, count(*) AS nodes, "
                    "count(n.`三级指标映射列表`) AS with_formal_indicators", timeout=30))]
                candidates = [dict(r) for r in session.run(Query(
                    f"MATCH (f:{quoted}) WHERE f.label IN $types "
                    "UNWIND f.filename AS filename RETURN filename,collect(DISTINCT f.label) AS types",
                    timeout=30), types=file_types[converter])]
                sizes = {r["filename"]: r["nodes"] for r in session.run(Query(
                    f"MATCH (n:{quoted}) UNWIND n.filename AS filename "
                    "RETURN filename,count(*) AS nodes", timeout=30))}
                chosen = []
                for file_type in file_types[converter]:
                    eligible = [r["filename"] for r in candidates if file_type in r["types"]
                                and 2 <= sizes.get(r["filename"], 0) <= 1200]
                    if not eligible:
                        raise ValueError("No bounded real-file sample for " + label + ": " + file_type)
                    chosen.append(sorted(eligible, key=lambda n: (-sizes[n], n))[0])
                nodes = [{"n": node_to_dict(r["n"])} for r in session.run(Query(
                    f"MATCH (n:{quoted}) WHERE any(f IN $files WHERE f IN n.filename) RETURN n",
                    timeout=30), files=chosen)]
                ids = [r["n"]["elementId"] for r in nodes]
                edges = [{"p": relationship_to_dict(r["r"])} for r in session.run(Query(
                    f"MATCH (a:{quoted})-[r]->(b:{quoted}) "
                    "WHERE elementId(a) IN $ids AND elementId(b) IN $ids RETURN r",
                    timeout=30), ids=ids)]
                snapshots.append({"nodes": nodes, "edges": edges})
                statistics[label] = {"whole_label_node_types": types, "sample_files": chosen,
                                     "sample_nodes": len(nodes), "sample_relationships": len(edges)}
                print(json.dumps({"sample_ready": label, "nodes": len(nodes), "edges": len(edges)},
                                 ensure_ascii=False), flush=True)
    finally:
        driver.close()
    return snapshots, statistics


def run(credentials_path, output):
    from sqlalchemy import create_engine, inspect, text
    from sqlalchemy.engine import URL
    from app.infrastructure.kg_to_natural_language import run_four_graphs_to_mysql as runner
    from app.infrastructure.kg_to_natural_language.storage import pipeline
    from app.infrastructure.kg_to_natural_language.storage.local_records import RunRecords
    from app.infrastructure.kg_to_natural_language.storage.query import QuerySpec, query_knowledge
    from app.infrastructure.kg_to_natural_language.storage.records import from_json_record
    from app.infrastructure.kg_to_natural_language.storage.repository import KnowledgeRepository

    credentials = json.loads(Path(credentials_path).read_text(encoding="utf-8-sig"))
    if credentials["host"] not in {"localhost", "127.0.0.1", "::1"}:
        raise ValueError("Only an explicitly isolated local MySQL test connection is allowed")
    name = "risk_knowledge_test_four_" + uuid.uuid4().hex
    url = URL.create("mysql+pymysql", username=credentials["username"], password=credentials["password"],
                     host=credentials["host"], port=int(credentials["port"]), query={"charset": "utf8mb4"})
    admin = create_engine(url, hide_parameters=True)
    engine = None
    snapshots, source_stats = read_samples()
    original_prepare = pipeline.prepare_knowledge
    report = {"scope": "closed file samples, not a full-graph import", "sources": source_stats}
    try:
        with patch.multiple(runner, OUTPUT_ROOT=Path(output),
                            DATASET_VERSION="envcheck_20261104_" + uuid.uuid4().hex,
                            MYSQL_HOST=credentials["host"], MYSQL_PORT=int(credentials["port"]),
                            MYSQL_USER=credentials["username"], MYSQL_DATABASE=name), patch.object(
                                pipeline, "prepare_knowledge",
                                side_effect=lambda config: original_prepare(config, snapshots=snapshots)):
            first = runner.run(credentials["password"])
            if first["status"] != "published":
                raise RuntimeError("Sample publication failed: " + str(first.get("error_type")))
            second = runner.run(credentials["password"])
            if second["status"] != "already_published":
                raise RuntimeError("Repeat execution did not verify an already-published version")
            run_records = RunRecords.open(first["record_directory"])
            with (run_records.directory / "records.jsonl").open(encoding="utf-8") as stream:
                rows = [from_json_record(json.loads(line)) for line in stream if line.strip()]
            engine = create_engine(url.set(database=name), hide_parameters=True)
            repository = KnowledgeRepository(engine)
            repository.verify(first["dataset_version"], rows)
            assert inspect(engine).get_table_names() == ["t_natural_knowledge_entry"]
            queried = query_knowledge(QuerySpec(first["dataset_version"], page_size=1), engine=engine)
            assert queried["total"] == len(rows)
            by_category = Counter(r["knowledge_category"] for r in rows)
            assert set(by_category) == {label for label, _ in runner.GRAPH_LABELS}
            assert all(r["content"].strip() and r["sources"] for r in rows)
            with engine.connect() as connection:
                json_types = [dict(r) for r in connection.execute(text(
                    "SELECT JSON_TYPE(sources) AS sources_type,JSON_TYPE(document_indicators) AS documents_type,"
                    "JSON_TYPE(field_states) AS states_type,count(*) AS records "
                    "FROM t_natural_knowledge_entry GROUP BY 1,2,3")).mappings()]
            assert all(r["sources_type"] == r["documents_type"] == "ARRAY"
                       and r["states_type"] == "OBJECT" for r in json_types)
            report.update(first=first, rerun_status=second["status"], query_count=queried["total"],
                          by_category=dict(by_category), json_types=json_types,
                          with_formal_indicators=sum(bool(r["indicators"]) for r in rows),
                          field_states={field: dict(Counter(r["field_states"][field]["state"] for r in rows))
                                        for field in ("sources", "indicators", "document_indicators",
                                                      "economic_industries", "risk_types")},
                          row_hash_verification="passed", source_neo4j_access="read_only")
    finally:
        if engine is not None:
            engine.dispose()
        with admin.begin() as connection:
            connection.execute(text("DROP DATABASE IF EXISTS `" + name + "`"))
        admin.dispose()
    report["temporary_database_removed"] = True
    run_records.write("environment_validation.json", report)
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--credentials", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(json.dumps(run(args.credentials, args.output), ensure_ascii=False, indent=2))
