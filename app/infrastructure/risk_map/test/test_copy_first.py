"""New contracts; all clients/adapters here are isolated, never use runner credentials."""

import asyncio
import copy
from dataclasses import replace
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from app.infrastructure.graph_storage.subgraph_records import digest, graph_content, snapshot_hash
from app.infrastructure.risk_map import MappingConfig, ModelConfig, execute_mapping
from app.infrastructure.risk_map.common.indicators import decode_candidates
from app.infrastructure.risk_map.common.io import atomic_json
from app.infrastructure.risk_map.common.model_client import ModelFailure, ModelSession
from app.infrastructure.risk_map.config import CANDIDATES, FINAL
from app.infrastructure.risk_map.local_records import RunRecords
from app.infrastructure.risk_map.matching.location import exact_location, validate_match
from app.infrastructure.risk_map.neo4j_bridge import copy_plan, verify_target
from app.infrastructure.risk_map.review.review import split_groups, validate_removals

TEXT = "第一条 企业应当建立管理制度。"
KEEP_ALL = '{"removed_indicator_numbers":[],"reasons":{}}'


def removal_response(numbers):
    return json.dumps({"removed_indicator_numbers": numbers,
                       "reasons": {number: "测试中的明确规范冲突" for number in numbers}}, ensure_ascii=False)


def payload_data(payload):
    for message in reversed(payload["messages"]):
        if message["role"] != "user":
            continue
        try:
            value = json.loads(message["content"])
        except ValueError:
            continue
        if isinstance(value, dict):
            return value
    raise AssertionError("no task JSON in request")


def local_status(summary, article_id="ts0"):
    records = RunRecords(Path(summary["record_directory"]).parents[1], summary["run_id"], resume=True)
    confirmed, _, _ = records.batches()
    return confirmed["matching"][article_id]["status"]


def tree(count=7, description=TEXT):
    leaves = {f"指标{i}": {"序号": f"2.1.1.{i}", "指标描述": f"制度管理要求{i}",
              "映射文件": {"中国": {"测试法": {"原始文本": "《测试法》", "已找到": "测试法.txt",
                       "知识依据": description, "依据文件名": "测试法"}}}} for i in range(1, count + 1)}
    return {"产品法律风险": {"序号": "2", "一级": {"序号": "2.1", "二级": {"序号": "2.1.1", **leaves}}}}


def source_graph(count):
    nodes = [{"n": {"elementId": "s" + str(i), "identity": i, "labels": ["source", "Semantic"],
              "properties": {"id": "duplicate", "label": "法条", "graph_tag": "source",
                             "法条全文": TEXT, "filename": ["测试法（2024年修订）.txt"], "条文编号": "第一条",
                             "flag": False, "zero": 0, "empty": [], "三级指标映射Top5": "historical"}}}
             for i in range(count)]
    if not nodes:
        nodes = [{"n": {"elementId": "metadata", "identity": -1, "labels": ["source"],
                        "properties": {"label": "法规文件", "graph_tag": "source"}}}]
    edges = [{"p": {"elementId": "r" + str(i), "identity": i, "type": "自环",
                      "startNodeElementId": "s0", "endNodeElementId": "s0",
                      "properties": {"graph_tag": "source", "i": i}}} for i in range(2)] if count else []
    return {"database": "neo4j", "source_label": "source", "nodes": nodes, "edges": edges, "boundary_count": 0}


class FakeAdapter:
    database, uri, username = "neo4j", "fake://isolated", "test"

    def __init__(self, count):
        self.source = source_graph(count)
        self.target = None
        self.calls = []
        self.unknown = False

    def check_subgraph_destination(self, target):
        if self.target is not None:
            raise ValueError("target_graph_already_exists")

    def read_subgraph_records(self, label, **kwargs):
        return copy.deepcopy(self.source if label == "source" else self.target)

    def create_subgraph_copy(self, plan, target_label, **kwargs):
        self.calls.append("copy")
        self.target = copy.deepcopy(plan)
        nmap = {row["n"]["elementId"]: "t" + row["n"]["elementId"] for row in plan["nodes"]}
        emap = {row["p"]["elementId"]: "t" + row["p"]["elementId"] for row in plan["edges"]}
        for row in self.target["nodes"]:
            row["n"]["elementId"] = nmap[row["n"]["elementId"]]
        for row in self.target["edges"]:
            p = row["p"]
            p["elementId"] = emap[p["elementId"]]
            p["startNodeElementId"] = nmap[p["startNodeElementId"]]
            p["endNodeElementId"] = nmap[p["endNodeElementId"]]
        return {"status": "committed_complete", "commit_confirmed": True,
                "source_label": kwargs["source_label"], "target_label": target_label,
                "source_hash": kwargs["source_hash"], "target_hash": digest(graph_content(plan)),
                "node_map": list(nmap.items()), "edge_map": list(emap.items()),
                "node_count": len(nmap), "relationship_count": len(emap), "database": self.database}

    def read_subgraph_articles(self, label, ids, **kwargs):
        self.calls.append(("read", len(ids)))
        return {row["n"]["elementId"]: copy.deepcopy(row["n"]["properties"])
                for row in self.target["nodes"] if row["n"]["elementId"] in ids}

    def update_subgraph_article_properties(self, label, rows, **kwargs):
        self.calls.append(("update", len(rows)))
        for row in rows:
            props = next(n["n"]["properties"] for n in self.target["nodes"] if n["n"]["elementId"] == row["id"])
            for key, value in row["properties"].items():
                if value is None:
                    props.pop(key, None)
                else:
                    props[key] = copy.deepcopy(value)
        if self.unknown:
            return {"status": "commit_unknown", "commit_confirmed": False}
        return {"status": "committed", "commit_confirmed": True, "ids": sorted(row["id"] for row in rows)}


class Client:
    def __init__(self, adapter, response=KEEP_ALL, failure=False):
        self.adapter, self.response, self.failure = adapter, response, failure
        self.calls, self.active, self.maximum = 0, 0, 0
        self.payloads = []

    async def complete(self, payload):
        assert self.adapter.target is not None
        self.calls += 1
        self.active += 1
        self.maximum = max(self.maximum, self.active)
        self.payloads.append(payload)
        try:
            await asyncio.sleep(0.001)
            if self.failure:
                raise ModelFailure("simulated_500")
            data = payload_data(payload)
            return self.response if "candidates" in data else '{"match":"是","reason":"局部内容对应"}'
        finally:
            self.active -= 1


class CopyFirstTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.root = Path(self.folder.name)
        self.tree = self.root / "tree.json"
        atomic_json(tree(), self.tree)
        self.config = MappingConfig("source", "target", tree_path=self.tree, output_root=self.root / "out",
              write_enabled=True, exclusive_target=True, model=ModelConfig(retry_delay=0))

    def tearDown(self):
        self.folder.cleanup()

    async def run_case(self, count=1, response=KEEP_ALL, failure=False, description=TEXT):
        atomic_json(tree(description=description), self.tree)
        adapter = FakeAdapter(count)
        client = Client(adapter, response, failure)
        before = snapshot_hash(adapter.source)
        summary, code = await execute_mapping(self.config, adapter, client=client)
        self.assertEqual(before, snapshot_hash(adapter.source))
        return adapter, client, summary, code

    async def test_seven_candidates_all_retained_copy_before_model(self):
        adapter, client, summary, code = await self.run_case()
        props = adapter.target["nodes"][0]["n"]["properties"]
        self.assertEqual(code, 0)
        self.assertEqual(len(props[CANDIDATES]), 7)
        self.assertEqual(props[CANDIDATES], props[FINAL])
        self.assertEqual(local_status(summary), "定位完成")
        self.assertNotIn("三级指标映射状态", props)
        self.assertNotIn("三级指标映射Top5", props)
        self.assertEqual(adapter.calls[0], "copy")
        self.assertEqual(len(adapter.target["edges"]), 2)
        self.assertEqual(adapter.target["nodes"][0]["n"]["labels"], ["Semantic", "target"])
        self.assertEqual(summary["copy_status"], "committed")

    async def test_exact_location_needs_no_model_even_when_model_unavailable(self):
        adapter, client, summary, code = await self.run_case(failure=True)
        props = adapter.target["nodes"][0]["n"]["properties"]
        self.assertEqual(code, 0)
        self.assertEqual(client.calls, 0)
        self.assertEqual(len(props[CANDIDATES]), 7)
        self.assertEqual(props[CANDIDATES], props[FINAL])
        self.assertEqual(local_status(summary), "定位完成")
        self.assertEqual(summary["copy_status"], "committed")

    async def test_first_phase_failure_is_not_no_match(self):
        adapter, client, summary, code = await self.run_case(failure=True, description="企业应当开展专项风险评估。")
        props = adapter.target["nodes"][0]["n"]["properties"]
        self.assertEqual(client.calls, 5)
        self.assertEqual(code, 2)
        self.assertEqual(local_status(summary), "匹配失败")
        self.assertNotIn(FINAL, props)

    async def test_batch_boundaries_and_frozen_inventory(self):
        for count in (0, 1, 19, 20, 21, 41):
            with self.subTest(count=count):
                config = replace(self.config, target_label="target" + str(count), output_root=self.root / str(count))
                adapter, client = FakeAdapter(count), None
                client = Client(adapter)
                summary, code = await execute_mapping(config, adapter, client=client)
                self.assertEqual(code, 0)
                sizes = [min(20, count - i) for i in range(0, count, 20)]
                self.assertEqual([n for kind, n in adapter.calls[1:] if kind == "update"], sizes * 2)
                self.assertLessEqual(client.maximum, 20)
                self.assertEqual(summary["confirmed_counts"].get("matching", 0), count)
                self.assertNotIn("review", summary["confirmed_counts"])

    async def test_unknown_update_blocks_resume_without_replay(self):
        adapter = FakeAdapter(1)
        adapter.unknown = True
        summary, code = await execute_mapping(self.config, adapter, client=Client(adapter))
        self.assertEqual(summary["copy_status"], "committed")
        self.assertEqual(summary["mapping_status"], "write_unknown")
        self.assertEqual(code, 2)
        calls = len(adapter.calls)
        summary, code = await execute_mapping(self.config, adapter, action="resume", run_id=summary["run_id"], client=Client(adapter))
        self.assertEqual(summary["mapping_status"], "write_unknown")
        self.assertEqual(len(adapter.calls), calls)
        adapter.target = None  # Even an empty target must not bypass an unknown commit.
        summary, code = await execute_mapping(self.config, adapter, client=Client(adapter))
        self.assertEqual(code, 2)
        self.assertEqual(len(adapter.calls), calls)

    async def test_inspect_and_resume_confirmed_no_replay(self):
        adapter, client, summary, _ = await self.run_case()
        before = len(adapter.calls)
        result, code = await execute_mapping(self.config, adapter, action="resume", run_id=summary["run_id"], client=client)
        self.assertEqual(code, 0)
        self.assertEqual(len(adapter.calls), before)
        result, code = await execute_mapping(self.config, adapter, action="inspect", run_id=summary["run_id"])
        self.assertEqual(result["inspection"]["status"], "verified")
        adapter.target["nodes"][0]["n"]["properties"]["flag"] = True
        result, code = await execute_mapping(self.config, adapter, action="inspect", run_id=summary["run_id"])
        self.assertEqual(code, 2)

    async def test_missing_initialization_history_cannot_reset_finished_node(self):
        adapter, client, summary, _ = await self.run_case()
        before = copy.deepcopy(adapter.target)
        directory = Path(summary["record_directory"]) / "checkpoints"
        for path in directory.glob("*_initialization_*.json"):
            path.unlink()  # Deliberate local ledger corruption, not production cleanup.
        result, code = await execute_mapping(self.config, adapter, action="resume", run_id=summary["run_id"], client=client)
        self.assertEqual(code, 2)
        self.assertTrue(result["record_validation_error"])
        self.assertEqual(adapter.target, before)

    async def test_legacy_unknown_removal_response_is_never_requested(self):
        adapter, client, _, code = await self.run_case(response=removal_response(["9.9.9.9"]))
        props = adapter.target["nodes"][0]["n"]["properties"]
        self.assertEqual(code, 0)
        self.assertEqual(client.calls, 0)
        self.assertEqual(len(props[FINAL]), 7)

    async def test_legacy_all_removed_response_cannot_delete_expert_associations(self):
        adapter, client, summary, code = await self.run_case(response=removal_response([f"2.1.1.{i}" for i in range(1, 8)]))
        props = adapter.target["nodes"][0]["n"]["properties"]
        self.assertEqual(code, 0)
        self.assertEqual(len(props[FINAL]), 7)
        self.assertEqual(local_status(summary), "定位完成")
        self.assertEqual(client.calls, 0)

    async def test_cancel_after_candidates_resume_does_not_reset(self):
        atomic_json(tree(description="企业应当开展专项风险评估。"), self.tree)
        adapter = FakeAdapter(21)
        client = Client(adapter)
        reached = asyncio.Event()
        class PausingClient(Client):
            async def complete(inner, payload):
                reached.set()
                await asyncio.Event().wait()
        task = asyncio.create_task(execute_mapping(self.config, adapter, client=PausingClient(adapter)))
        await asyncio.wait_for(reached.wait(), 10)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        run_id = next((self.config.output_root / "runs").iterdir()).name
        self.assertNotIn(CANDIDATES, adapter.target["nodes"][0]["n"]["properties"])
        previous_updates = len([call for call in adapter.calls if isinstance(call, tuple) and call[0] == "update"])
        result, code = await execute_mapping(self.config, adapter, action="resume", run_id=run_id, client=client)
        self.assertEqual(code, 0)
        props = adapter.target["nodes"][0]["n"]["properties"]
        self.assertEqual(len(props[CANDIDATES]), 7)
        self.assertEqual(props[CANDIDATES], props[FINAL])
        self.assertEqual(len([call for call in adapter.calls if isinstance(call, tuple) and call[0] == "update"]), previous_updates + 2)

    async def test_one_article_failure_other_articles_continue(self):
        atomic_json(tree(description="企业应当开展专项风险评估。"), self.tree)
        adapter = FakeAdapter(21)
        for i, row in enumerate(adapter.source["nodes"]):
            row["n"]["properties"]["法条全文"] += f"第{i}组事实。"
        class OneFailure(Client):
            async def complete(inner, payload):
                if "第3组事实" in payload_data(payload)["article_text"]:
                    inner.calls += 1
                    raise ModelFailure("only_one_article_failed")
                return await super().complete(payload)
        client = OneFailure(adapter)
        summary, code = await execute_mapping(self.config, adapter, client=client)
        self.assertEqual(code, 2)
        self.assertEqual(summary["article_statuses"], {"定位完成": 20, "匹配失败": 1})
        self.assertEqual(summary["confirmed_counts"]["matching"], 21)

    async def test_known_associations_saved_with_empty_basis_skipped(self):
        data = tree()
        data["产品法律风险"]["一级"]["二级"]["指标7"]["映射文件"]["中国"]["测试法"]["知识依据"] = ""
        atomic_json(data, self.tree)
        adapter = FakeAdapter(1)
        summary, code = await execute_mapping(self.config, adapter, client=Client(adapter))
        props = adapter.target["nodes"][0]["n"]["properties"]
        self.assertEqual(code, 0)
        self.assertEqual(len(props[FINAL]), 6)
        self.assertEqual(local_status(summary), "定位完成")
        self.assertEqual(summary["basis_skips"]["unique_empty_attachments"], 1)
        self.assertEqual(summary["failed_stage_tasks"], 0)

    async def test_many_indicators_preserved_without_review_groups(self):
        data = tree(30)
        atomic_json(data, self.tree)
        adapter = FakeAdapter(1)
        config = replace(self.config, model=ModelConfig(review_max_group_candidates=3, retry_delay=0))
        class GroupFailure(Client):
            async def complete(inner, payload):
                values = payload_data(payload)
                if "candidates" in values and values["candidates"][0]["指标序号"] == "2.1.1.1":
                    raise ModelFailure("first_group_failed")
                return await super().complete(payload)
        summary, code = await execute_mapping(config, adapter, client=GroupFailure(adapter))
        props = adapter.target["nodes"][0]["n"]["properties"]
        self.assertEqual(code, 0)
        self.assertEqual(len(props[CANDIDATES]), 30)
        self.assertEqual(len(props[FINAL]), 30)
        self.assertEqual(local_status(summary), "定位完成")

    async def test_full_article_over_budget_is_not_truncated(self):
        atomic_json(tree(description="企业应当开展专项风险评估。"), self.tree)
        adapter = FakeAdapter(1)
        adapter.source["nodes"][0]["n"]["properties"]["法条全文"] = TEXT + "完整法条。" * 1000
        original = snapshot_hash(adapter.source)
        config = replace(self.config, model=ModelConfig(max_input_chars=1250, retry_delay=0))
        client = Client(adapter)
        summary, code = await execute_mapping(config, adapter, client=client)
        self.assertEqual(code, 2)
        self.assertEqual(client.calls, 0)
        self.assertEqual(snapshot_hash(adapter.source), original)
        self.assertEqual(local_status(summary), "匹配失败")

    async def test_copy_failures_no_models_and_receipt_failure_keeps_commit(self):
        for outcome in ("failed", "commit_unknown"):
            adapter = FakeAdapter(1)
            client = Client(adapter)
            async_config = replace(self.config, output_root=self.root / outcome)
            with patch.object(adapter, "create_subgraph_copy", return_value={"status": outcome, "commit_confirmed": False}):
                summary, code = await execute_mapping(async_config, adapter, client=client)
            self.assertEqual(summary["copy_status"], outcome)
            self.assertEqual(client.calls, 0)
        adapter, client = FakeAdapter(1), None
        client = Client(adapter)
        original_save = RunRecords.save
        def fail_manifest(records, name, value):
            if name == "copy_manifest.json":
                raise OSError("controlled_local_receipt_failure")
            return original_save(records, name, value)
        with patch.object(RunRecords, "save", fail_manifest):
            summary, code = await execute_mapping(self.config, adapter, client=client)
        self.assertEqual(summary["copy_status"], "committed")
        self.assertIsNotNone(adapter.target)
        self.assertEqual(client.calls, 0)

    async def test_prepare_read_only_occupied_target_and_legacy_rejected(self):
        adapter = FakeAdapter(1)
        summary, code = await execute_mapping(self.config, adapter, action="prepare")
        self.assertEqual(code, 0)
        self.assertIsNone(adapter.target)
        summary, code = await execute_mapping(self.config, adapter, client=Client(adapter))
        before = copy.deepcopy(adapter.target)
        result, code = await execute_mapping(self.config, adapter, client=Client(adapter))
        self.assertEqual(code, 1)
        self.assertEqual(adapter.target, before)
        from app.infrastructure.risk_map.__main__ import main
        self.assertEqual(main(["file", "old.json"]), 64)
        with self.assertRaisesRegex(ValueError, "run_id"):
            await execute_mapping(self.config, adapter, action="resume", run_id="../old")

    def test_groups_cover_every_candidate_once_and_do_not_truncate(self):
        candidates = [{"指标名": f"指标{i}", "指标序号": f"2.1.1.{i}", "指标描述": "完整描述" * 20} for i in range(30)]
        groups = split_groups({"text": TEXT}, candidates, ModelConfig(review_max_group_candidates=3))
        self.assertGreater(len(groups), 1)
        self.assertEqual([c for group in groups for c in group], candidates)

    def test_revision_family_does_not_strip_identity_parentheses(self):
        from app.infrastructure.risk_map.preparation.document_identity import normalize_document_name, revision_family
        self.assertEqual(revision_family(normalize_document_name("《测试法》（2024年修订）.txt"))[0], "测试法")
        self.assertNotEqual(revision_family(normalize_document_name("测试法（试行）"))[0], "测试法")

    def test_minimal_protocols_and_short_text(self):
        self.assertEqual(validate_match('```json\n{"match":"是","reason":"对应"}\n```')["match"], "是")
        for value in ({"match": "是", "reason": "对应", "extra": 1}, {"match": "是", "reason": ""}):
            with self.assertRaises(ModelFailure):
                validate_match(value)
        for value in (["2.1.1.1", "2.1.1.1"], [1], ["9.9.9.9"], {}):
            with self.assertRaises(ModelFailure):
                validate_removals(value, [{"指标序号": "2.1.1.1"}])
        self.assertTrue(exact_location({"text": "不得泄密。"}, "不得泄密。"))
        self.assertFalse(exact_location({"text": TEXT}, "第一条"))

    def test_review_payload_preserves_file_and_region_context(self):
        from app.infrastructure.risk_map.review.review import review_data
        article = {"text": TEXT, "number": "第一条", "filenames": ["测试法.txt"], "region": "中国"}
        data = review_data(article, [])
        self.assertEqual(data["article_text"], TEXT)
        self.assertEqual(data["article_number"], "第一条")
        self.assertEqual(data["source_filenames"], article["filenames"])
        self.assertEqual(data["source_region"], "中国")

    async def test_prompt_change_cannot_silently_resume_old_results(self):
        adapter, client, summary, _ = await self.run_case()
        before = copy.deepcopy(adapter.target)
        with patch("app.infrastructure.risk_map.pipeline.MATCH_SYSTEM", "changed location policy"):
            result, code = await execute_mapping(self.config, adapter, action="resume", run_id=summary["run_id"], client=client)
        self.assertNotEqual(code, 0)
        self.assertEqual(adapter.target, before)


if __name__ == "__main__":
    unittest.main()
