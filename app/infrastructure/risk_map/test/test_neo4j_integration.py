"""Opt-in real LOCAL database tests. Every created business label is unique and owned."""

import asyncio
import copy
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import json
import os
from pathlib import Path
import tempfile
import threading
import unittest
from urllib.parse import urlsplit
import uuid
from unittest.mock import patch

from neo4j.exceptions import ServiceUnavailable
from neo4j.spatial import CartesianPoint, WGS84Point
from neo4j.time import Date, DateTime, Duration, Time

from app.infrastructure.graph_storage.factory import GraphStorageFactory
from app.infrastructure.graph_storage.subgraph_records import SubgraphError, digest, identifier, snapshot_hash
from app.infrastructure.risk_map import MappingConfig, ModelConfig, execute_mapping
from app.infrastructure.risk_map.config import CANDIDATES, FINAL
from app.infrastructure.risk_map.neo4j_bridge import copy_plan
from app.infrastructure.risk_map.common.io import atomic_json
from app.infrastructure.risk_map.test.test_copy_first import tree, TEXT, KEEP_ALL, removal_response
from app.infrastructure.risk_map.local_records import RunRecords


ENABLED = os.getenv("RISK_MAP_LOCAL_NEO4J_TEST") == "1"


class SessionProxy:
    def __init__(self, session, fault):
        self.session, self.fault = session, fault

    def __enter__(self):
        self.session.__enter__()
        return self

    def __exit__(self, *args):
        return self.session.__exit__(*args)

    def __getattr__(self, name):
        return getattr(self.session, name)

    def begin_transaction(self, **kwargs):
        return TransactionProxy(self.session.begin_transaction(**kwargs), self.fault)


class TransactionProxy:
    def __init__(self, tx, fault):
        self.tx, self.fault = tx, fault

    def __getattr__(self, name):
        return getattr(self.tx, name)

    def __enter__(self):
        self.tx.__enter__()
        return self

    def __exit__(self, *args):
        return self.tx.__exit__(*args)

    def commit(self):
        if self.fault == "commit_before":
            raise ServiceUnavailable("controlled missing commit acknowledgement")
        self.tx.commit()
        if self.fault == "commit_after":
            raise ServiceUnavailable("controlled lost commit acknowledgement")

    def rollback(self):
        if self.fault == "rollback_failure":
            raise ServiceUnavailable("controlled rollback acknowledgement loss")
        return self.tx.rollback()

    def run(self, query, *args, **kwargs):
        result = self.tx.run(query, *args, **kwargs)
        if self.fault and self.fault in query:
            raise SubgraphError("controlled_failure_after_batch")
        return result


@unittest.skipUnless(ENABLED, "local Neo4j tests require explicit opt-in and credentials")
class LocalNeo4jTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        uri = os.environ["RISK_MAP_TEST_NEO4J_URI"]
        if urlsplit(uri).hostname not in {"localhost", "127.0.0.1", "::1"}:
            raise AssertionError("Refusing integration tests against a non-local database")
        cls.adapter = GraphStorageFactory.create("neo4j", dict(
            uri=uri, username=os.environ["RISK_MAP_TEST_NEO4J_USER"],
            password=os.environ["RISK_MAP_TEST_NEO4J_PASSWORD"],
            database=os.environ["RISK_MAP_TEST_NEO4J_DATABASE"],
            driver_options={"connection_timeout": 10, "max_transaction_retry_time": 3},
        ))
        if not cls.adapter.connect():
            cls.adapter.disconnect()
            raise AssertionError("Local Neo4j login failed")
        cls.server_version = cls.adapter.verify_subgraph_support()

    @classmethod
    def tearDownClass(cls):
        cls.adapter.disconnect()

    def setUp(self):
        self.prefix = "RiskMapTest_" + uuid.uuid4().hex
        self.source = self.prefix + "_source"
        self.outside = self.prefix + "_outside"
        self.labels = {self.source, self.outside}
        self.snap = None
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.tree_path = self.root / "tree.json"
        atomic_json(tree(), self.tree_path)
        props = [
            {"id": "file", "name": "测试法", "label": "法规文件", "文件全称": "测试法", "filename": ["测试法.txt"]},
            {"id": "article", "name": "第一条", "label": "法条", "条": "第一条", "法条全文": TEXT, "filename": ["测试法.txt"]},
            {"id": "duplicate", "name": "isolated", "label": "其他", "empty": [], "zero": 0, "false": False,
             "date": Date(2026, 10, 16), "time": Time(3, 4, 5.123456789),
             "datetime": DateTime(2026, 10, 16, 3, 4, 5.123456789),
             "duration": Duration(months=1, days=2, seconds=3, nanoseconds=4),
             "cartesian": CartesianPoint((1, 2)), "location": WGS84Point((120, 30)), "bytes": bytearray(b"\x00\xff")},
            {"id": "duplicate", "name": "isolated-again", "label": "其他", "特殊`属性": "preserved"},
        ]
        with self.adapter.driver.session(database=self.adapter.database) as session:
            def create(tx):
                for i, p in enumerate(props):
                    tx.run(f"CREATE (n:{identifier(self.source)}) SET n=$props", props={**p, "graph_tag": self.source, "slot": i}).consume()
                for i in range(2):
                    tx.run(f"MATCH (a:{identifier(self.source)} {{slot:0}}), (b:{identifier(self.source)} {{slot:1}}) "
                           "CREATE (a)-[r:包含]->(b) SET r=$props", props={"graph_tag": self.source, "value": "same"}).consume()
                tx.run(f"MATCH (a:{identifier(self.source)} {{slot:1}}) "
                       f"CREATE (a)-[r:{identifier('自环`测试')}]->(a) SET r.graph_tag=$tag", tag=self.source).consume()
                tx.run(f"CREATE (b:{identifier(self.outside)} {{owner:$owner}})", owner=self.prefix).consume()
                tx.run(f"MATCH (a:{identifier(self.source)} {{slot:0}}), (b:{identifier(self.outside)}) "
                       "CREATE (a)-[:边界]->(b)").consume()
            session.execute_write(create)
        self.snap = self.adapter.read_subgraph_records(self.source)

    def tearDown(self):
        try:
            # Only labels from this test's random namespace are ever eligible for cleanup.
            with self.adapter.driver.session(database=self.adapter.database) as session:
                def clean(tx):
                    for label in self.labels:
                        if not label.startswith(self.prefix + "_"):
                            raise AssertionError("Cleanup ownership mismatch")
                        tx.run(f"MATCH (n:{identifier(label)}) DETACH DELETE n").consume()
                session.execute_write(clean)
        finally:
            self.temp.cleanup()

    def target(self):
        target = self.prefix + "_target_" + uuid.uuid4().hex[:8]
        self.labels.add(target)
        return target

    def plan(self, target):
        snapshot = copy.deepcopy(self.snap)
        for row in snapshot["nodes"]:
            row["n"]["labels"] = [target]
            row["n"]["properties"]["graph_tag"] = target
        for row in snapshot["edges"]:
            row["p"]["properties"]["graph_tag"] = target
        snapshot["boundary_count"] = 0
        return snapshot

    def publish(self, target, publication=None):
        return self.adapter.create_subgraph_copy(self.plan(target), target, source_label=self.source,
                    source_hash=snapshot_hash(self.snap), publish_id=publication or uuid.uuid4().hex, batch_size=1)

    def assert_source_unchanged(self):
        self.assertEqual(snapshot_hash(self.snap), snapshot_hash(self.adapter.read_subgraph_records(self.source)))

    def proxy_driver(self, fault):
        real = self.adapter.driver
        class Proxy:
            def session(inner, **kwargs):
                return SessionProxy(real.session(**kwargs), fault)
        return Proxy()

    def test_complete_read_and_independent_copy_keep_types_and_parallel_edges(self):
        self.assertEqual((len(self.snap["nodes"]), len(self.snap["edges"]), self.snap["boundary_count"]), (4, 3, 1))
        target = self.target()
        result = self.publish(target)
        self.assertEqual(result["status"], "committed_partial")
        actual = self.adapter.read_subgraph_records(target)
        self.assertEqual((len(actual["nodes"]), len(actual["edges"]), actual["boundary_count"]), (4, 3, 0))
        self.assertFalse({n["n"]["elementId"] for n in self.snap["nodes"]} &
                         {n["n"]["elementId"] for n in actual["nodes"]})
        by_slot = {n["n"]["properties"]["slot"]: n["n"] for n in actual["nodes"]}
        for old in self.snap["nodes"]:
            expected = {**old["n"]["properties"], "graph_tag": target}
            self.assertEqual(digest(by_slot[expected["slot"]]["properties"]), digest(expected))
        self.assertEqual(self.adapter.inspect_subgraph_publication(target, manifest=result)["inspection_status"], "verified")
        self.assert_source_unchanged()

    def test_read_budgets_and_scope_counts(self):
        for options in ({"max_nodes": 1}, {"max_relationships": 1}, {"max_bytes": 10}):
            with self.subTest(options=options), self.assertRaises(SubgraphError):
                self.adapter.read_subgraph_records(self.source, **options)

    def test_same_publication_is_not_recreated_and_inspect_is_read_only(self):
        target = self.target()
        first = self.publish(target)
        again = self.publish(target, first["publish_id"])
        self.assertEqual(again["status"], "failed")
        self.assertTrue(again["rollback_confirmed"])
        self.assertEqual(self.adapter.get_subgraph_stats(target).node_count, 4)
        self.assertEqual(self.adapter.inspect_subgraph_publication(target, manifest=first)["inspection_status"], "verified")

    def test_source_target_same_rejected(self):
        with self.assertRaises(SubgraphError):
            self.publish(self.source)
        self.assert_source_unchanged()

    def test_failure_after_each_batch_rolls_back_entire_graph(self):
        for stage in ("risk_map:create_nodes", "risk_map:create_edges"):
            target = self.target()
            with patch.object(self.adapter, "driver", self.proxy_driver(stage)):
                result = self.publish(target)
            self.assertEqual(result["status"], "failed")
            self.assertTrue(result["rollback_confirmed"])
            diagnostic = self.adapter.inspect_subgraph_publication(target)
            self.assertEqual(diagnostic["diagnostic_counts"], {"nodes": 0, "relationships": 0})
        self.assert_source_unchanged()

    def test_validation_failure_rolls_back_all_created_objects(self):
        target = self.target()
        with patch.object(self.adapter, "_verify_copy_tx", side_effect=SubgraphError("controlled_validation_failure")):
            result = self.publish(target)
        self.assertEqual(result["status"], "failed")
        self.assertEqual(self.adapter.inspect_subgraph_publication(target)["diagnostic_counts"]["nodes"], 0)

    def test_lost_commit_acknowledgement_keeps_unknown_even_when_target_exists(self):
        target = self.target()
        with patch.object(self.adapter, "driver", self.proxy_driver("commit_after")):
            result = self.publish(target)
        self.assertEqual(result["status"], "commit_unknown")
        self.assertEqual(self.adapter.get_subgraph_stats(target).node_count, 4)
        self.assertEqual(self.adapter.inspect_subgraph_publication(target)["inspection_status"], "unverified")

    def test_lost_commit_acknowledgement_keeps_unknown_when_target_is_empty(self):
        target = self.target()
        with patch.object(self.adapter, "driver", self.proxy_driver("commit_before")):
            result = self.publish(target)
        self.assertEqual(result["status"], "commit_unknown")
        self.assertEqual(self.adapter.inspect_subgraph_publication(target)["diagnostic_counts"]["nodes"], 0)
        self.assertNotIn("safe_to_retry", result)

    def test_no_managed_write_retry_or_registry_queries(self):
        target = self.target()
        queries = []
        real = self.adapter.driver
        class TxProxy(TransactionProxy):
            def run(inner, query, *args, **kwargs):
                queries.append(query)
                if any(word in query.upper() for word in ("_RISKMAPPUBLICATION", "SHOW CONSTRAINT", "CREATE CONSTRAINT", "CREATE INDEX")):
                    raise AssertionError("Forbidden management query")
                return super().run(query, *args, **kwargs)
        class Session(SessionProxy):
            def begin_transaction(inner, **kwargs):
                return TxProxy(inner.session.begin_transaction(**kwargs), None)
            def execute_write(inner, *args, **kwargs):
                raise AssertionError("Managed write retry must not run")
            def execute_read(inner, callback):
                return inner.session.execute_read(lambda tx: callback(TxProxy(tx, None)))
        class Driver:
            def session(inner, **kwargs):
                return Session(real.session(**kwargs), None)
        with patch.object(self.adapter, "driver", Driver()):
            result = self.publish(target)
            inspected = self.adapter.inspect_subgraph_publication(target, manifest=result)
        self.assertEqual(result["status"], "committed_partial")
        self.assertEqual(inspected["inspection_status"], "verified")
        self.assertTrue(queries)
        self.assertNotIn("publish_id", json.dumps(self.adapter.read_subgraph_records(target), default=str))

    def test_session_close_error_does_not_negate_acknowledged_commit(self):
        target = self.target()
        real = self.adapter.driver
        class Session(SessionProxy):
            def __exit__(inner, *args):
                inner.session.__exit__(*args)
                raise ServiceUnavailable("controlled close failure")
        class Driver:
            def session(inner, **kwargs):
                return Session(real.session(**kwargs), None)
        with patch.object(self.adapter, "driver", Driver()):
            result = self.publish(target)
        self.assertEqual(result["status"], "committed_partial")
        self.assertTrue(result["session_cleanup_error"])
        self.assertEqual(self.adapter.get_subgraph_stats(target).node_count, 4)

    def test_verification_rejects_tampered_target(self):
        target = self.target()
        result = self.publish(target)
        with self.adapter.driver.session(database=self.adapter.database) as session:
            session.run(f"MATCH (n:{identifier(target)} {{slot:0}}) SET n.changed='bad'").consume()
        with self.assertRaises(SubgraphError):
            self.adapter.inspect_subgraph_publication(target, manifest=result)

    def test_graph_tag_only_extra_object_is_detected(self):
        target = self.target()
        result = self.publish(target)
        with self.adapter.driver.session(database=self.adapter.database) as session:
            session.run(f"CREATE (n:{identifier(self.outside)} {{graph_tag:$tag}})", tag=target).consume()
        with self.assertRaises(SubgraphError):
            self.adapter.inspect_subgraph_publication(target, manifest=result)

    def test_bridge_real_database_with_controlled_model_and_no_source_mutation(self):
        target = self.target()
        config = MappingConfig(source_label=self.source, target_label=target, write_enabled=True,
                    exclusive_target=True, tree_path=self.tree_path, output_root=self.root / "mapped",
                    model=ModelConfig(retry_delay=0))
        adapter = self.adapter
        class ReviewClient:
            async def complete(inner, payload):
                self.assertEqual(adapter.get_subgraph_stats(target).node_count, 4)
                return KEEP_ALL
        result, code = asyncio.run(execute_mapping(config, self.adapter, client=ReviewClient()))
        self.assertEqual((code, result["copy_status"]), (0, "committed"))
        inspected, _ = asyncio.run(execute_mapping(config, self.adapter, action="inspect", run_id=result["run_id"]))
        self.assertEqual(inspected["inspection"]["status"], "verified")
        actual = self.adapter.read_subgraph_records(target)
        props = next(n["n"]["properties"] for n in actual["nodes"] if n["n"]["properties"].get("label") == "法条")
        self.assertEqual(len(props[FINAL]), 7)
        self.assert_source_unchanged()

    def test_new_copy_preserves_extra_semantic_labels(self):
        target = self.target()
        with self.adapter.driver.session(database=self.adapter.database) as session:
            session.run(f"MATCH (n:{identifier(self.source)} {{slot:1}}) SET n:SemanticArticle").consume()
        self.snap = self.adapter.read_subgraph_records(self.source)
        plan = copy_plan(self.snap, self.source, target)
        result = self.adapter.create_subgraph_copy(plan, target, source_label=self.source,
            source_hash=snapshot_hash(self.snap), publish_id=uuid.uuid4().hex, preserve_extra_labels=True)
        self.assertTrue(result["commit_confirmed"])
        actual = self.adapter.read_subgraph_records(target)
        article = next(n["n"] for n in actual["nodes"] if n["n"]["properties"]["slot"] == 1)
        self.assertIn("SemanticArticle", article["labels"])
        self.assertNotIn(self.source, article["labels"])
        self.assert_source_unchanged()

    def test_restricted_patch_physical_id_lists_and_whitelist(self):
        target = self.target()
        result = self.publish(target)
        ids = dict(result["node_map"])
        source_article = next(n["n"]["elementId"] for n in self.snap["nodes"] if n["n"]["properties"]["slot"] == 1)
        physical = ids[source_article]
        rows = [{"id": physical, "properties": {CANDIDATES: [], FINAL: []}}]
        patched = self.adapter.update_subgraph_article_properties(target, rows)
        self.assertEqual(patched["status"], "committed")
        patched = self.adapter.update_subgraph_article_properties(target, rows)
        self.assertEqual(self.adapter.read_subgraph_articles(target, [physical])[physical][FINAL], [])
        for bad in ([{"id": physical, "properties": {"other": "not allowed"}}],
                    [{"id": physical, "properties": {FINAL: [{}]}}],
                    [{"id": physical, "properties": {"三级指标映射状态": "禁止写入"}}]):
            with self.assertRaises(SubgraphError):
                self.adapter.update_subgraph_article_properties(target, bad)
        wrong = self.adapter.update_subgraph_article_properties(target, [{"id": source_article, "properties": {FINAL: []}}])
        self.assertEqual(wrong["status"], "failed")
        self.assert_source_unchanged()

    def test_patch_ack_loss_not_replayed(self):
        target = self.target()
        result = self.publish(target)
        article = next(n["n"]["elementId"] for n in self.snap["nodes"] if n["n"]["properties"]["slot"] == 1)
        physical = dict(result["node_map"])[article]
        with patch.object(self.adapter, "driver", self.proxy_driver("commit_after")):
            receipt = self.adapter.update_subgraph_article_properties(target, [{"id": physical, "properties": {FINAL: ["测试"]}}])
        self.assertEqual(receipt["status"], "commit_unknown")
        self.assertEqual(self.adapter.read_subgraph_articles(target, [physical])[physical][FINAL], ["测试"])
        self.assert_source_unchanged()

    def test_local_pipeline_failures_preserve_complete_base(self):
        target = self.target()
        config = MappingConfig(self.source, target, tree_path=self.tree_path, output_root=self.root / "failure",
                    write_enabled=True, exclusive_target=True, model=ModelConfig(retry_delay=0))
        calls = []
        class Unavailable:
            async def complete(inner, payload):
                calls.append(1)
                raise RuntimeError("controlled_model_unavailable")
        result, code = asyncio.run(execute_mapping(config, self.adapter, client=Unavailable()))
        self.assertEqual(code, 2)
        self.assertEqual(result["copy_status"], "committed")
        self.assertEqual(len(calls), 5)
        snapshot = self.adapter.read_subgraph_records(target)
        self.assertEqual((len(snapshot["nodes"]), len(snapshot["edges"])), (4, 3))
        props = next(n["n"]["properties"] for n in snapshot["nodes"] if n["n"]["properties"].get("label") == "法条")
        self.assertEqual(result["article_statuses"], {"审查失败": 1})
        self.assertNotIn("三级指标映射状态", props)
        self.assertEqual(len(props[CANDIDATES]), 7)
        self.assertNotIn(FINAL, props)
        self.assert_source_unchanged()

    def test_local_removal_reasons_are_durable_but_not_graph_properties(self):
        with self.adapter.driver.session(database=self.adapter.database) as session:
            session.run(f"MATCH (n:{identifier(self.source)} {{slot:1}}) SET n.三级指标映射状态='历史状态'").consume()
        self.snap = self.adapter.read_subgraph_records(self.source)
        target = self.target()
        config = MappingConfig(self.source, target, tree_path=self.tree_path, output_root=self.root / "reasons",
                    write_enabled=True, exclusive_target=True, model=ModelConfig(retry_delay=0))
        class RejectOne:
            async def complete(inner, payload):
                return removal_response(["2.1.1.1"])
        result, code = asyncio.run(execute_mapping(config, self.adapter, client=RejectOne()))
        self.assertEqual(code, 0)
        records = RunRecords(config.output_root, result["run_id"], resume=True)
        confirmed, pending, _ = records.batches()
        self.assertFalse(pending)
        group = next(iter(confirmed["review"].values()))["groups"][0]
        self.assertEqual(group["reasons"], {"2.1.1.1": "测试中的明确规范冲突"})
        actual = self.adapter.read_subgraph_records(target)
        props = next(n["n"]["properties"] for n in actual["nodes"] if n["n"]["properties"].get("label") == "法条")
        self.assertEqual(len(props[CANDIDATES]), 7)
        self.assertEqual(len(props[FINAL]), 6)
        self.assertNotIn("reasons", props)
        self.assertNotIn("review_protocol", props)
        self.assertNotIn("三级指标映射状态", props)
        self.assertEqual(next(iter(confirmed["review"].values()))["status"], "已审查")
        self.assertEqual((len(actual["nodes"]), len(actual["edges"])), (4, 3))
        self.assert_source_unchanged()

    def test_local_cancel_after_candidates_resume_no_recopy(self):
        target = self.target()
        config = MappingConfig(self.source, target, tree_path=self.tree_path, output_root=self.root / "resume",
                    write_enabled=True, exclusive_target=True, model=ModelConfig(retry_delay=0))
        async def interrupt():
            reached = asyncio.Event()
            class Pausing:
                async def complete(inner, payload):
                    reached.set()
                    await asyncio.Event().wait()
            task = asyncio.create_task(execute_mapping(config, self.adapter, client=Pausing()))
            await asyncio.wait_for(reached.wait(), 20)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        asyncio.run(interrupt())
        before = self.adapter.read_subgraph_records(target)
        before_ids = {n["n"]["elementId"] for n in before["nodes"]}
        run_id = next((config.output_root / "runs").iterdir()).name
        class AcceptAll:
            async def complete(inner, payload):
                return KEEP_ALL
        result, code = asyncio.run(execute_mapping(config, self.adapter, action="resume", run_id=run_id, client=AcceptAll()))
        self.assertEqual(code, 0)
        after = self.adapter.read_subgraph_records(target)
        self.assertEqual(before_ids, {n["n"]["elementId"] for n in after["nodes"]})
        self.assertEqual(result["confirmed_counts"], {"initialization": 1, "matching": 1, "review": 1})
        self.assert_source_unchanged()


if __name__ == "__main__":
    unittest.main()
