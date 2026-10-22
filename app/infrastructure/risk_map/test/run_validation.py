"""Reproducible validation with optional LOCAL Neo4j and historical failure replay."""

import argparse
import asyncio
import io
import json
import logging
import os
from pathlib import Path
import sys
import time
import unittest
from urllib.parse import urlsplit
import uuid

ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.infrastructure.graph_storage.factory import GraphStorageFactory
from app.infrastructure.graph_storage.subgraph_records import identifier, snapshot_hash
from app.infrastructure.risk_map import MappingConfig, ModelConfig, execute_mapping
from app.infrastructure.risk_map.common.io import atomic_json, file_hash, read_json
from app.infrastructure.risk_map.common.stats import Diagnostics
from app.infrastructure.risk_map.matching.location import attachment_ids, body
from app.infrastructure.risk_map.preparation.config import DEFAULT_TREE_PATH
from app.infrastructure.risk_map.preparation.tree_index import build_tree_index
from app.infrastructure.risk_map.test.test_copy_first import tree

PACKAGE = Path(__file__).resolve().parents[1]


def connect_local():
    uri = os.environ["RISK_MAP_TEST_NEO4J_URI"]
    if urlsplit(uri).hostname not in {"localhost", "127.0.0.1", "::1"}:
        raise ValueError("只允许本地Neo4j验证")
    adapter = GraphStorageFactory.create("neo4j", {"uri": uri,
        "username": os.environ["RISK_MAP_TEST_NEO4J_USER"],
        "password": os.environ["RISK_MAP_TEST_NEO4J_PASSWORD"],
        "database": os.environ["RISK_MAP_TEST_NEO4J_DATABASE"]})
    if not adapter.connect():
        adapter.disconnect()
        raise RuntimeError("本地Neo4j连接失败")
    return adapter


def database_state(adapter):
    with adapter.driver.session(database=adapter.database) as session:
        nodes = session.run("MATCH (n) RETURN count(n) AS count").single()["count"]
        edges = session.run("MATCH ()-[r]->() RETURN count(r) AS count").single()["count"]
        constraints = [dict(r) for r in session.run("SHOW CONSTRAINTS YIELD name, type, labelsOrTypes, properties RETURN name,type,labelsOrTypes,properties ORDER BY name")]
        indexes = [dict(r) for r in session.run("SHOW INDEXES YIELD name,type,labelsOrTypes,properties RETURN name,type,labelsOrTypes,properties ORDER BY name")]
    return {"nodes": nodes, "relationships": edges, "constraints": constraints, "indexes": indexes}


def historical_audit(adapter, output):
    evidence = PACKAGE / "task" / "数据审查" / "审查结果" / "审查结果1"
    failures_path = evidence / "技术失败法条明细.json"
    versions_path = evidence / "版本后缀导致漏召回且正文相同明细.json"
    if not all(p.is_file() for p in (failures_path, versions_path, DEFAULT_TREE_PATH)):
        return {"status": "not_available", "semantic_accuracy_tested": False}
    failures, versions = read_json(failures_path), read_json(versions_path)
    index = build_tree_index(read_json(DEFAULT_TREE_PATH), Diagnostics())
    version_result = []
    for row in versions:
        ids = attachment_ids(index, row["file"], "中国")
        numbers = {index.attachments[i].indicator_number for i in ids}
        version_result.append({"article_id": row["article_id"], "files": row["file"],
            "file_family_recalled": bool(ids), "expected_indicators_recalled": set(row["indicator_numbers"]) <= numbers,
            "same_body_still_locatable": bool(body(row["body"])) and body(row["body"]) in body(row["text"]),
            "version_validity_verified": False})
    result = {"historical_failure_articles": len(failures), "version_pairs": len(versions),
              "version_details": version_result, "real_model_calls": 0, "semantic_accuracy_tested": False}
    if adapter is None:
        return {**result, "database_replay": "not_run"}
    prefix = "RiskMapTask2Replay_" + uuid.uuid4().hex
    source, target = prefix + "_source", prefix + "_target"
    # A synthetic one-description-per-file tree exercises all old failed articles.
    # It is not a replacement for the authoritative tree or a semantic gold standard.
    data = tree(1, "第一条 企业应当履行待模型核对的专项义务。")
    files = data["产品法律风险"]["一级"]["二级"]["指标1"]["映射文件"]["中国"]
    files.clear()
    rows = []
    for i, row in enumerate(failures):
        names = row.get("file", [])
        for name in names:
            files[name] = {"原始文本": name, "依据文件名": name, "已找到": name,
                           "知识依据": "第一条 企业应当履行待模型核对的专项义务。"}
        rows.append({"id": row["article_id"], "label": "法条", "法条全文": row["text"],
                     "条": row["number"], "filename": names, "graph_tag": source, "slot": i})
    fixture = output / "historical_replay" / "fixture_tree.json"
    atomic_json(data, fixture)
    class Unavailable:
        calls = 0
        async def complete(self, payload):
            self.calls += 1
            await asyncio.sleep(0)
            raise RuntimeError("controlled_legacy_failure_replay")
    client = Unavailable()
    try:
        with adapter.driver.session(database=adapter.database) as session:
            session.run(f"UNWIND $rows AS row CREATE (n:{identifier(source)}) SET n=row", rows=rows).consume()
        before = snapshot_hash(adapter.read_subgraph_records(source))
        config = MappingConfig(source, target, tree_path=fixture, output_root=output / "historical_replay" / "run",
            write_enabled=True, exclusive_target=True, model=ModelConfig(retry_delay=0))
        summary, code = asyncio.run(execute_mapping(config, adapter, client=client))
        target_snapshot = adapter.read_subgraph_records(target)
        result.update(database_replay="local_controlled_failure", replay_exit_code=code,
                      replay_summary=summary, replay_model_attempts=client.calls,
                      target_nodes=len(target_snapshot["nodes"]),
                      source_unchanged=before == snapshot_hash(adapter.read_subgraph_records(source)))
        assert code == 2 and summary["copy_status"] == "committed"
        assert len(target_snapshot["nodes"]) == len(rows) and result["source_unchanged"]
        assert summary["article_statuses"] == {"匹配失败": len(rows)}
        assert all("三级指标映射状态" not in row["n"]["properties"] for row in target_snapshot["nodes"])
        result["all_147_failures_preserved"] = len(rows) == 147
    finally:
        with adapter.driver.session(database=adapter.database) as session:
            for label in (source, target):
                if not label.startswith(prefix + "_"):
                    raise AssertionError("cleanup ownership mismatch")
                session.run(f"MATCH (n:{identifier(label)}) DETACH DELETE n").consume()
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--local", action="store_true", help="only LOCAL Neo4j; explicit env credentials required")
    parser.add_argument("--output", type=Path, help="New evidence directory; existing results are never overwritten")
    args = parser.parse_args()
    if args.local:
        os.environ["RISK_MAP_LOCAL_NEO4J_TEST"] = "1"
    output = args.output or PACKAGE / "task" / "测试报告" / "映射状态仅本地记录_验证证据" / ("run_" + uuid.uuid4().hex[:12])
    output.mkdir(parents=True, exist_ok=False)
    log = io.StringIO()
    handler = logging.StreamHandler(log)
    logger = logging.getLogger("app.infrastructure.risk_map")
    logger.addHandler(handler)
    old_level = logger.level
    logger.setLevel(logging.WARNING)
    adapter = connect_local() if args.local else None
    started = time.monotonic()
    try:
        before = database_state(adapter) if adapter else None
        suite = unittest.defaultTestLoader.loadTestsFromNames([
            "app.infrastructure.risk_map.test.test_copy_first",
            "app.infrastructure.risk_map.test.test_matching_index",
            "app.infrastructure.risk_map.test.test_storage_records",
            "app.infrastructure.risk_map.test.test_progress",
            "app.infrastructure.risk_map.test.test_review_reasons",
            "app.infrastructure.risk_map.test.test_audit_fixes",
            "app.infrastructure.risk_map.test.test_neo4j_integration"])
        stream = io.StringIO()
        result = unittest.TextTestRunner(stream=stream, verbosity=2).run(suite)
        audit = historical_audit(adapter, output)
        after = database_state(adapter) if adapter else None
        summary = {"date": time.strftime("%Y-%m-%d"), "tests_run": result.testsRun, "failures": len(result.failures),
            "errors": len(result.errors), "skipped": len(result.skipped),
            "seconds": round(time.monotonic() - started, 2), "local_neo4j": bool(adapter),
            "database_version": adapter.verify_subgraph_support() if adapter else None,
            "database_before": before, "database_after": after,
            "database_members_and_schema_unchanged": before == after if adapter else None,
            "real_model_calls": 0, "historical_audit": audit}
        atomic_json(summary, output / "validation_results.json", durable=True)
        # Generated test transcripts, not source edits.
        (output / "unittest.log").write_text(stream.getvalue(), encoding="utf-8")
        (output / "execution.log").write_text(log.getvalue(), encoding="utf-8")
        print(json.dumps({"output": str(output), **{k: v for k, v in summary.items() if k not in {"historical_audit", "database_before", "database_after"}}}, ensure_ascii=False, indent=2))
        return 0 if result.wasSuccessful() and (adapter is None or before == after) else 1
    finally:
        if adapter:
            adapter.disconnect()
        logger.removeHandler(handler)
        logger.setLevel(old_level)


if __name__ == "__main__":
    raise SystemExit(main())
