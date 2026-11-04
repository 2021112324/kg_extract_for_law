"""Offline provenance and recovery tests; not a substitute for MySQL JSON tests."""

import copy
import json
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

import pytest

from app.infrastructure.kg_to_natural_language.storage.config import SourceConfig, StorageConfig
from app.infrastructure.kg_to_natural_language.storage.enrichment import GraphContext, decode_indicators, enrich_entry
from app.infrastructure.kg_to_natural_language.storage.local_records import Heartbeat, RunRecords
from app.infrastructure.kg_to_natural_language.storage.pipeline import load_prepared, persist_knowledge, prepare_knowledge
from app.infrastructure.kg_to_natural_language.storage.records import (KnowledgeStorageError, digest, record_digest,
                                                                     validate_record)


def indicator(number="3.1.2"):
    return {"指标序号": number, "指标名": "示例指标", "指标描述": "示例描述"}


def node(i, label, **props):
    return {"n": {"identity": i, "elementId": "n:" + str(i), "labels": ["测试图谱"],
                  "properties": {"id": "business:" + str(i), "label": label, "name": label + str(i),
                                 "filename": ["示例法规.txt"], **props}}}


def edge(i, a, b, kind="包含"):
    return {"p": {"identity": i, "elementId": "e:" + str(i), "start": a, "end": b,
                  "startNodeElementId": "n:" + str(a), "endNodeElementId": "n:" + str(b), "type": kind,
                  "properties": {}}}


def sample(tmp_path, formal="present"):
    props = {"三级指标映射列表": [json.dumps(indicator(), ensure_ascii=False)]}
    if formal == "empty":
        props = {"三级指标映射列表": []}
    elif formal == "missing":
        props = {}
    elif formal == "invalid":
        props = {"三级指标映射列表": ["{bad"]}
    nodes = [node(1, "法规文件", 文件全称="示例法规", 时效性="现行有效", 发布日期="2026年11月03日",
                  经济行业=["制造业"], 文件指标=[json.dumps(indicator("3.1.3"), ensure_ascii=False)]),
             node(2, "法条", 条="第一条", 法条全文="企业应当履行义务。", **props),
             node(3, "条款单元", 条款单元内容="企业应当履行义务。", 合规风险类型=["产品法律风险"])]
    edges = [edge(1, 1, 2), edge(2, 2, 3)]
    snapshot = {"nodes": nodes, "edges": edges, "snapshot": digest([nodes, edges]),
                "source": {"converter": "chinese_law", "category": "法律法规条款", "graph_label": "测试图谱", "graph_source": "fixture"}}
    config = StorageConfig("v1", [SourceConfig("chinese_law", "法律法规条款", "测试图谱", "nodes.json", "edges.json")],
                           str(tmp_path), file_indicator_properties={"法规文件": "文件指标"}, batch_size=1)
    entry = {"content": "企业应当履行义务。", "knowledge_type": "规则知识", "source_node_ids": ["business:3"],
             "source_location": "第一条", "metadata": {}}
    return snapshot, config, entry


def enriched(tmp_path, formal="present"):
    snapshot, config, entry = sample(tmp_path, formal)
    return enrich_entry(entry, GraphContext(snapshot), config, "run1", "binding", "local-log")


def test_indicators_and_file_fallback_are_independent(tmp_path):
    row = enriched(tmp_path)
    validate_record(row)
    assert row["indicators"][0]["indicator_number"] == "3.1.2"
    assert row["document_indicators"][0]["indicators"][0]["indicator_number"] == "3.1.3"
    assert isinstance(row["indicators"][0], dict)
    assert row["validity_status"] == "现行有效"
    assert row["sources"][0]["publication_date"] == "2026-11-03"
    assert row["economic_industries"] == ["制造业"]
    assert row["indicators"][0]["association_method"] == "沿所属法条继承"


@pytest.mark.parametrize("state,expected", [("empty", []), ("missing", None), ("invalid", None)])
def test_formal_states_do_not_turn_into_fallback_mappings(tmp_path, state, expected):
    row = enriched(tmp_path, state)
    assert row["indicators"] == expected
    assert row["field_states"]["indicators"]["state"] == state
    assert row["document_indicators"][0]["mapping_state"] == "present"


def test_missing_file_attribute_not_derived_from_article(tmp_path):
    snapshot, config, entry = sample(tmp_path)
    config.file_indicator_properties = {}
    row = enrich_entry(entry, GraphContext(snapshot), config, "run1", "binding", "log")
    assert row["document_indicators"][0]["indicators"] is None
    assert row["document_indicators"][0]["mapping_state"] == "missing"
    assert row["field_states"]["document_indicators"]["state"] == "missing"


def test_self_definition_keeps_edge_but_not_ownership_loop(tmp_path):
    snapshot, config, entry = sample(tmp_path)
    for record, label in zip(snapshot["nodes"], ("标准文件", "标准结构节点", "标准要求")):
        record["n"]["properties"]["label"] = label
    snapshot["edges"].append(edge(99, 2, 2, "定义"))
    original = copy.deepcopy(snapshot)
    context = GraphContext(snapshot)
    assert context.files_for("n:3") == (["n:1"], False)
    assert len(context.edges) == 3
    assert context.ownership_diagnostics[0]["relationship_type"] == "定义"
    assert context.ownership_diagnostics[0]["node_id"] == "n:2"
    assert len(context.ownership_diagnostics) == 1
    assert snapshot == original
    row = enrich_entry(entry, context, config, "run1", "binding", "log")
    assert row["sources"][0]["source_file_snapshot_key"] == "n:1"


def test_multi_node_ownership_cycle_is_still_rejected(tmp_path):
    snapshot, _, _ = sample(tmp_path)
    snapshot["edges"].append(edge(99, 3, 2))
    with pytest.raises(KnowledgeStorageError, match="Cyclic owning"):
        GraphContext(snapshot).ancestors("n:2")


def test_prepare_records_ignored_ownership_edges(tmp_path):
    snapshot, config, entry = sample(tmp_path)
    snapshot["edges"].append(edge(99, 2, 2, "定义"))
    with patch("app.infrastructure.kg_to_natural_language.storage.adapters.generate_entries", return_value=[entry]):
        result = prepare_knowledge(config, snapshots=[snapshot])
    assert result["status"] == "prepared"
    assert result["ownership_edges_ignored"] == 1
    directory = Path(result["record_directory"])
    diagnostics = json.loads((directory / "ownership_diagnostics.json").read_text(encoding="utf-8"))
    assert diagnostics[0]["node_id"] == "n:2"
    assert diagnostics[0]["graph_label"] == "测试图谱"
    events = (directory / "events.jsonl").read_text(encoding="utf-8")
    assert '"event_type":"ownership_edges_ignored"' in events


@pytest.mark.parametrize("value,expected", [
    (None, "missing"), ([], "empty"), (["not-json"], "invalid"),
])
def test_file_indicator_summary_matches_actual_mapping_state(tmp_path, value, expected):
    snapshot, config, entry = sample(tmp_path)
    snapshot["nodes"][0]["n"]["properties"]["文件指标"] = value
    row = enrich_entry(entry, GraphContext(snapshot), config, "run1", "binding", "log")
    assert row["document_indicators"][0]["mapping_state"] == expected
    assert row["field_states"]["document_indicators"]["state"] == expected
    validate_record(row)


def test_citation_does_not_inherit_target_file_validity_or_indicators(tmp_path):
    snapshot, config, entry = sample(tmp_path)
    snapshot["nodes"].append(node(4, "法规文件", filename=["被引用文件.txt"], 时效性="已失效", 经济行业=["其他"]))
    snapshot["edges"].append(edge(3, 3, 4, "引用"))
    row = enrich_entry(entry, GraphContext(snapshot), config, "run1", "binding", "log")
    assert row["validity_status"] == "现行有效"
    assert row["economic_industries"] == ["制造业"]
    assert len(row["sources"]) == 1


def test_unknown_industry_and_nullable_validity(tmp_path):
    snapshot, config, entry = sample(tmp_path)
    p = snapshot["nodes"][0]["n"]["properties"]
    p.pop("时效性")
    p["经济行业"] = ["建筑业"]
    row = enrich_entry(entry, GraphContext(snapshot), config, "run1", "binding", "log")
    assert row["validity_status"] is None
    assert row["economic_industries"] is None
    assert row["field_states"]["economic_industries"]["state"] == "invalid"
    config.industry_mapping = {"建筑业": "其他"}
    mapped = enrich_entry(entry, GraphContext(snapshot), config, "run1", "binding", "log")
    assert mapped["economic_industries"] == ["其他"]


def test_source_scope_mismatch_is_not_silently_relabelled(tmp_path):
    snapshot, _, _ = sample(tmp_path)
    snapshot["source"]["graph_label"] = "another-graph"
    with pytest.raises(KnowledgeStorageError, match="source graph"):
        GraphContext(snapshot)


def test_placeholder_validity_remains_nullable(tmp_path):
    snapshot, config, entry = sample(tmp_path)
    snapshot["nodes"][0]["n"]["properties"]["时效性"] = "无"
    row = enrich_entry(entry, GraphContext(snapshot), config, "run1", "binding", "log")
    assert row["validity_status"] is None


def test_neo4j_temporal_value_is_frozen_without_mutating_input(tmp_path):
    from neo4j.time import Date
    snapshot, config, entry = sample(tmp_path)
    snapshot["nodes"][0]["n"]["properties"]["发布日期"] = Date(2026, 11, 3)
    with patch("app.infrastructure.kg_to_natural_language.storage.adapters.generate_entries", return_value=[entry]):
        result = prepare_knowledge(config, snapshots=[snapshot])
    assert isinstance(snapshot["nodes"][0]["n"]["properties"]["发布日期"], Date)
    saved = RunRecords.open(result["record_directory"]).read("inputs.json")
    assert saved[0]["nodes"][0]["n"]["properties"]["发布日期"] == "2026-11-03"


def test_conflicting_indicator_definitions_rejected():
    with pytest.raises(KnowledgeStorageError):
        decode_indicators([indicator(), {**indicator(), "指标名": "冲突名称"}])


def test_same_file_name_different_editions_not_same_identity(tmp_path):
    snap, conf, entry = sample(tmp_path)
    a = enrich_entry(entry, GraphContext(snap), conf, "run", "binding", "log")
    snap["nodes"][0]["n"]["properties"]["修订版本"] = "2025"
    b = enrich_entry(entry, GraphContext(snap), conf, "run", "binding", "log")
    assert a["knowledge_id"] != b["knowledge_id"]


def test_record_hash_detects_metadata_change(tmp_path):
    row = enriched(tmp_path)
    row["tags"] = ["changed"]
    with pytest.raises(KnowledgeStorageError, match="digest"):
        validate_record(row)


class MemoryRepository:
    """Fault-injection double. No SQL or JSON-dialect verification is claimed."""
    def __init__(self):
        self.rows = {}
        self.fail_after_commit = False
        self.fail_before_commit = False
        self.fail_publish_ack = False
        self.locked = False

    def preflight(self):
        pass

    def initialize(self):
        pass

    @contextmanager
    def lock(self, version):
        self.locked = True
        try:
            yield
        finally:
            self.locked = False

    def assert_lock(self):
        assert self.locked

    def read_version(self, version):
        return copy.deepcopy([r for (v, _), r in self.rows.items() if v == version])

    def write_batch(self, rows):
        self.assert_lock()
        if self.fail_before_commit:
            raise OSError("before commit")
        for r in rows:
            key = (r["dataset_version"], r["knowledge_id"])
            if key in self.rows:
                assert record_digest(self.rows[key]) == r["record_hash"]
            else:
                self.rows[key] = copy.deepcopy(r)
        if self.fail_after_commit:
            self.fail_after_commit = False
            raise OSError("commit ack lost")

    def verify(self, version, rows):
        got = self.read_version(version)
        assert {(r["knowledge_id"], record_digest(r)) for r in got} == {(r["knowledge_id"], r["record_hash"]) for r in rows}

    def publish(self, version, rows):
        self.assert_lock()
        self.verify(version, rows)
        for key, row in self.rows.items():
            if key[0] == version:
                row["publication_status"] = "published"
        if self.fail_publish_ack:
            self.fail_publish_ack = False
            raise OSError("publication ack lost")


def prepared(tmp_path):
    snap, conf, entry = sample(tmp_path)
    with patch("app.infrastructure.kg_to_natural_language.storage.adapters.generate_entries", return_value=[entry]):
        summary = prepare_knowledge(conf, snapshots=[snap])
    return summary["record_directory"]


def test_prepare_is_offline_and_frozen_tampering_is_rejected(tmp_path):
    directory = prepared(tmp_path)
    records = RunRecords.open(directory)
    manifest, rows = load_prepared(records)
    assert manifest["record_count"] == len(rows) == 1
    raw = records.read("inputs.json")
    raw[0]["nodes"][0]["n"]["properties"]["name"] = "tampered"
    records.write("inputs.json", raw)
    with pytest.raises(KnowledgeStorageError, match="changed"):
        load_prepared(records)


def test_idempotence_and_uncertain_batch_ack_recovery(tmp_path):
    directory = prepared(tmp_path)
    repo = MemoryRepository()
    repo.fail_after_commit = True
    result = persist_knowledge(directory, repository=repo)
    assert result["status"] == "published"
    assert len(repo.rows) == 1
    again = persist_knowledge(directory, repository=repo)
    assert again["status"] == "already_published"
    assert len(repo.rows) == 1


def test_interruption_does_not_publish_and_can_resume(tmp_path):
    directory = prepared(tmp_path)
    repo = MemoryRepository()
    repo.fail_before_commit = True
    failed = persist_knowledge(directory, repository=repo)
    assert failed["status"] == "failed_or_unconfirmed"
    assert not failed["database_published"]
    repo.fail_before_commit = False
    assert persist_knowledge(directory, repository=repo)["database_published"]


def test_publication_commit_ack_is_reconciled(tmp_path):
    directory = prepared(tmp_path)
    repo = MemoryRepository()
    repo.fail_publish_ack = True
    assert persist_knowledge(directory, repository=repo)["database_published"]


def test_local_summary_error_does_not_claim_database_rollback(tmp_path):
    directory = prepared(tmp_path)
    repo = MemoryRepository()
    original = RunRecords.write
    def failing_write(self, name, value):
        if name == "summary.json" and value.get("database_published"):
            raise OSError("local disk write failed")
        return original(self, name, value)
    with patch.object(RunRecords, "write", failing_write):
        result = persist_knowledge(directory, repository=repo)
    assert result["database_published"] and result["status"] == "record_error"
    assert all(r["publication_status"] == "published" for r in repo.rows.values())


def test_heartbeat_is_independent_and_stops(tmp_path):
    import time
    records = RunRecords(tmp_path)
    with Heartbeat(records, interval=0.01) as heartbeat:
        heartbeat.progress("blocked_db", 2, 5)
        time.sleep(0.05)
    assert not heartbeat.thread.is_alive()
    assert '"event_type":"heartbeat"' in (records.directory / "events.jsonl").read_text(encoding="utf-8")


def test_user_interruption_is_recorded_and_not_completed(tmp_path):
    directory = prepared(tmp_path)
    class Interrupted(MemoryRepository):
        def write_batch(self, rows):
            raise KeyboardInterrupt()
    with pytest.raises(KeyboardInterrupt):
        persist_knowledge(directory, repository=Interrupted())
    assert RunRecords.open(directory).read("summary.json")["status"] == "interrupted"


def test_duplicate_json_indicator_keys_are_not_silently_accepted():
    with pytest.raises(KnowledgeStorageError, match="Duplicate"):
        decode_indicators(['{"指标序号":"3.1.2","指标序号":"3.1.3"}'])


def test_same_local_run_cannot_be_written_concurrently(tmp_path):
    records = RunRecords(tmp_path)
    second = RunRecords.open(records.directory)
    with records.writer_lock():
        with pytest.raises(KnowledgeStorageError, match="already writing"):
            with second.writer_lock():
                pass
    with second.writer_lock():
        pass


def test_read_only_graph_input_uses_explicit_transaction_and_keeps_driver_open():
    from app.infrastructure.kg_to_natural_language.graph.exporter import read_knowledge_snapshot
    from contextlib import nullcontext
    calls = []
    class Tx:
        def run(self, query):
            calls.append(query)
            return []
    class Session:
        def begin_transaction(self):
            return nullcontext(Tx())
    class Driver:
        def session(self, **options):
            assert options["default_access_mode"] == "READ"
            assert options["database"] == "explicit-test"
            return nullcontext(Session())
        def close(self):
            pytest.fail("An injected driver must not be closed")
    assert read_knowledge_snapshot("任意_标签", driver=Driver(), database="explicit-test") == ([], [])
    assert len(calls) == 2 and all(q.startswith("MATCH") for q in calls)
    assert "(q:`任意_标签`)" in calls[1]


def test_json_size_budget_is_checked_before_loading(tmp_path):
    from app.infrastructure.kg_to_natural_language.storage.inputs import read_source
    n, e = tmp_path / "nodes.json", tmp_path / "edges.json"
    n.write_text("[]", encoding="utf-8")
    e.write_text("[]", encoding="utf-8")
    source = SourceConfig("chinese_law", "法律法规条款", "tag", str(n), str(e), max_bytes=1)
    with pytest.raises(KnowledgeStorageError, match="max_bytes"):
        read_source(source)


def test_import_package_does_not_load_project_mysql_module():
    import subprocess
    import sys
    code = ("import socket,sys; "
            "socket.create_connection=lambda *a,**k: (_ for _ in ()).throw(AssertionError('network')); "
            "import app.infrastructure.kg_to_natural_language.storage; "
            "assert 'app.db.base' not in sys.modules")
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_file_groups_with_same_logical_identity_do_not_overwrite(tmp_path):
    snapshot, config, entry = sample(tmp_path)
    second = copy.deepcopy(snapshot["nodes"][0])
    second["n"].update(identity=10, elementId="n:10")
    second["n"]["properties"]["id"] = "business:10"
    second["n"]["properties"]["文件指标"] = [json.dumps(indicator("9.9.9"), ensure_ascii=False)]
    # The second semantic node explicitly belongs to the second duplicate file.
    unit = node(11, "条款单元", 条款单元内容="企业应当履行义务。")
    snapshot["nodes"].extend([second, unit])
    snapshot["edges"].append(edge(10, 10, 11))
    entry["source_node_ids"].append("n:11")
    row = enrich_entry(entry, GraphContext(snapshot), config, "run1", "binding", "log")
    assert len(row["document_indicators"]) == 2
    assert {g["source_file_snapshot_key"] for g in row["document_indicators"]} == {"n:1", "n:10"}
    assert all(g["mapping_state"] == "conflict" for g in row["document_indicators"])
    assert row["field_states"]["document_indicators"]["state"] == "conflict"


def test_merge_detects_indicator_definition_conflicts(tmp_path):
    from app.infrastructure.kg_to_natural_language.storage.pipeline import merge_records
    first = enriched(tmp_path)
    other = copy.deepcopy(first)
    other["indicators"][0]["indicator_name"] = "另一名称"
    other["record_hash"] = record_digest(other)
    merged = merge_records(first, other)
    assert merged["indicators"] is None
    assert merged["field_states"]["indicators"]["state"] == "conflict"
    validate_record(merged)


def test_industry_filename_fallback_is_per_generating_source(tmp_path):
    snapshot, config, entry = sample(tmp_path)
    snapshot["nodes"].extend([node(20, "法规文件", filename=["other.md"]),
                              node(21, "条款单元", filename=["other.md"])])
    snapshot["edges"].append(edge(20, 20, 21))
    entry["source_node_ids"].append("n:21")
    config.filename_industries = {"other.md": "其他"}
    row = enrich_entry(entry, GraphContext(snapshot), config, "run1", "binding", "log")
    assert row["economic_industries"] == ["其他", "制造业"]


def test_duplicate_business_id_formal_provenance_keeps_snapshot_keys(tmp_path):
    snapshot, config, entry = sample(tmp_path)
    second = copy.deepcopy(snapshot["nodes"][2])
    second["n"].update(identity=30, elementId="n:30")
    second["n"]["properties"]["三级指标映射列表"] = [json.dumps(indicator(), ensure_ascii=False)]
    snapshot["nodes"].append(second)
    snapshot["edges"].append(edge(30, 2, 30))
    entry["source_node_ids"] = ["n:3", "n:30"]
    row = enrich_entry(entry, GraphContext(snapshot), config, "run1", "binding", "log")
    assert {item["source_reference"]["source_snapshot_key"] for item in row["indicators"]} == {"n:2", "n:30"}


def test_zero_relationship_identity_is_valid_and_unknown_rejected(tmp_path):
    snapshot, config, entry = sample(tmp_path)
    snapshot["edges"][0]["p"].pop("elementId")
    snapshot["edges"][0]["p"]["identity"] = 0
    entry["source_relationship_ids"] = ["0"]
    row = enrich_entry(entry, GraphContext(snapshot), config, "run1", "binding", "log")
    assert row["sources"][0]["source_relationship_ids"] == ["0"]
    entry["source_relationship_ids"] = ["no-such-relationship"]
    with pytest.raises(KnowledgeStorageError, match="relationship reference"):
        enrich_entry(entry, GraphContext(snapshot), config, "run1", "binding", "log")
