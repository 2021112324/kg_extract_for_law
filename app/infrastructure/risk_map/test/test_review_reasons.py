"""Explainable review protocol, bounded retries, local audit and commit ordering."""

import asyncio
import copy
from dataclasses import replace
import io
import json
import logging
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from app.infrastructure.risk_map import MappingConfig, ModelConfig, execute_mapping
from app.infrastructure.risk_map.common.io import atomic_json, content_hash, read_json
from app.infrastructure.risk_map.common.model_client import ModelFailure, ModelSession, OpenAICompatibleClient, request_messages
from app.infrastructure.risk_map.config import CANDIDATES, FINAL, MANAGED, REVIEW_PROTOCOL
from app.infrastructure.risk_map.local_records import RunRecords
from app.infrastructure.risk_map.review.review import EXAMPLES, SYSTEM, review_article, review_data, split_groups, validate_removals
from app.infrastructure.risk_map.test.test_copy_first import Client, FakeAdapter, KEEP_ALL, TEXT, removal_response, tree


def candidates(count=2):
    return [{"指标名": f"指标{i}", "指标序号": f"1.1.{i}", "指标描述": "管理事项甲及乙"} for i in range(count)]


class ReviewResponseTests(unittest.TestCase):
    def test_graph_adapter_refuses_nonnull_status_without_contacting_database(self):
        from app.infrastructure.graph_storage.neo4j_adapter import Neo4jAdapter
        from app.infrastructure.graph_storage.subgraph_records import SubgraphError
        adapter = object.__new__(Neo4jAdapter)
        with self.assertRaisesRegex(SubgraphError, "removal_only"):
            adapter.update_subgraph_article_properties("target", [{"id": "article", "properties": {"三级指标映射状态": "已审查"}}])

    def test_empty_partial_and_all_removals(self):
        group = candidates()
        for numbers in ([], ["1.1.0"], ["1.1.0", "1.1.1"]):
            with self.subTest(numbers=numbers):
                response = removal_response(numbers)
                expected = json.loads(response)
                self.assertEqual(validate_removals(response, group), expected)
                self.assertEqual(validate_removals("```json\n" + response + "\n```", group), expected)
                reversed_fields = {"reasons": expected["reasons"], "removed_indicator_numbers": numbers}
                self.assertEqual(validate_removals(reversed_fields, group), expected)

    def test_invalid_types_and_sets_are_not_repaired(self):
        group = candidates()
        cases = [[], "[]", {}, None,
            {"removed_indicator_numbers": "1.1.0", "reasons": {}},
            {"removed_indicator_numbers": [1], "reasons": {}},
            {"removed_indicator_numbers": [None], "reasons": {}},
            {"removed_indicator_numbers": ["1.1.0", "1.1.0"], "reasons": {"1.1.0": "重复"}},
            {"removed_indicator_numbers": ["other"], "reasons": {"other": "未知"}},
            {"removed_indicator_numbers": ["1.1.0"], "reasons": {}},
            {"removed_indicator_numbers": [], "reasons": {"1.1.0": "多余"}},
            {"removed_indicator_numbers": [], "reasons": []},
            {"removed_indicator_numbers": [], "reasons": {}, "extra": 1},
        ]
        cases.extend({"removed_indicator_numbers": ["1.1.0"], "reasons": {"1.1.0": value}}
                     for value in ("", " \n\t", None, 5, False, [], {}))
        for response in cases:
            with self.subTest(response=response), self.assertRaises(ModelFailure):
                validate_removals(response, group)

    def test_duplicate_keys_multiple_values_and_truncation(self):
        cases = [
            '{"removed_indicator_numbers":[],"removed_indicator_numbers":[],"reasons":{}}',
            '{"removed_indicator_numbers":["1.1.0"],"reasons":{"1.1.0":"甲","1.1.0":"乙"}}',
            KEEP_ALL + "\n" + KEEP_ALL,
            KEEP_ALL + "\n说明",
            '{"removed_indicator_numbers":["1.1.0"],"reasons":',
        ]
        for response in cases:
            with self.subTest(response=response), self.assertRaises(ModelFailure):
                validate_removals(response, candidates())

    def test_group_cap_and_input_budget_cover_once(self):
        values = candidates(31)
        config = ModelConfig(review_max_group_candidates=8)
        groups = split_groups({"text": TEXT}, values, config)
        self.assertEqual([len(g) for g in groups], [8, 8, 8, 7])
        self.assertEqual([c for group in groups for c in group], values)
        article = {"text": TEXT}
        payload = {"model": config.model, "temperature": 0, "max_tokens": config.max_tokens,
                   "messages": request_messages(SYSTEM, review_data(article, values[-1:]), EXAMPLES)}
        budget = len(json.dumps(payload, ensure_ascii=False)) + 256
        groups = split_groups(article, values, replace(config, max_input_chars=budget))
        self.assertTrue(all(len(g) == 1 for g in groups))
        self.assertEqual([c for g in groups for c in g], values)
        with self.assertRaises(ModelFailure):
            split_groups(article, values, replace(config, max_input_chars=budget - 1))

    def test_legacy_group_configuration_does_not_affect_current_workflow(self):
        for value in (0, -1, True, 1.5, "8"):
            with self.subTest(value=value):
                ModelConfig(review_max_group_candidates=value).validate()
        self.assertEqual(ModelConfig().semantic, ModelConfig(review_max_group_candidates=7).semantic)
        self.assertNotIn("review_protocol", ModelConfig().semantic)


class ReviewSessionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.events = []

    def tearDown(self):
        self.temp.cleanup()

    def session(self, replies, config=None):
        class ReplyClient:
            def __init__(inner):
                inner.payloads = []

            async def complete(inner, payload):
                inner.payloads.append(copy.deepcopy(payload))
                return replies[min(len(inner.payloads) - 1, len(replies) - 1)]
        client = ReplyClient()
        return ModelSession(config or ModelConfig(retry_delay=0), client, self.root / "cache",
                            lambda phase, row: self.events.append({"phase": phase, **row}), run_id="test_run")

    async def review(self, session, count=2):
        values = candidates(count)
        props = {CANDIDATES: [json.dumps(c, ensure_ascii=False) for c in values]}
        original = copy.deepcopy(props)
        result = await review_article({"id": "article", "text": TEXT, "number": "第一条", "filenames": ["测试法.txt"]},
                                      props, {"coverage_complete": True}, session)
        self.assertEqual(props, original)
        self.assertLessEqual(set(result["properties"]), set(MANAGED))
        return result

    async def test_retry_specific_reason_error_then_success(self):
        missing = '{"removed_indicator_numbers":["1.1.0"],"reasons":{}}'
        session = self.session([missing, removal_response(["1.1.0"])])
        result = await self.review(session)
        self.assertEqual(len(session.client.payloads), 2)
        self.assertIn("reason_numbers_mismatch", session.client.payloads[1]["messages"][-1]["content"])
        self.assertIn("1.1.0", session.client.payloads[1]["messages"][-1]["content"])
        self.assertEqual(result["groups"][0]["reasons"], {"1.1.0": "测试中的明确规范冲突"})
        self.assertEqual(result["groups"][0]["attempt"], 2)
        self.assertEqual(self.events[0]["response"], missing)
        self.assertEqual([json.loads(item)["指标序号"] for item in result["properties"][FINAL]], ["1.1.1"])

    async def test_five_invalid_or_truncated_responses_never_mean_keep_all(self):
        for response in ("[]", '{"removed_indicator_numbers":["1.1.0"],"reasons":'):
            session = self.session([response], ModelConfig(retry_delay=0, max_tokens=1))
            result = await self.review(session)
            self.assertEqual(len(session.client.payloads), 5)
            self.assertEqual(result["properties"], {FINAL: None})
            self.assertEqual(result["status"], "审查失败")
            self.assertEqual(result["unreviewed_numbers"], ["1.1.0", "1.1.1"])
            self.assertFalse(any(e.get("event_type") == "review_decision" for e in self.events))

    async def test_cache_replays_reasons_and_origin_in_current_audit(self):
        session = self.session([removal_response(["1.1.0"])])
        first = await self.review(session)
        second = await self.review(session)
        self.assertEqual(len(session.client.payloads), 1)
        self.assertEqual(first["groups"][0]["origin"], "model")
        self.assertEqual(second["groups"][0]["origin"], "cache")
        decisions = [e for e in self.events if e.get("event_type") == "review_decision"]
        self.assertEqual(len(decisions), 2)
        self.assertEqual(decisions[1]["run_id"], "test_run")
        self.assertEqual(decisions[1]["reasons"], decisions[0]["reasons"])

    async def test_old_cache_is_not_given_invented_reasons(self):
        session = self.session([removal_response(["1.1.0"])])
        await self.review(session)
        cache = next((self.root / "cache").glob("*.json"))
        atomic_json({"response": '["1.1.0"]'}, cache)
        result = await self.review(session)
        self.assertEqual(len(session.client.payloads), 2)
        self.assertEqual(session.counters["cache_hits"], 0)
        self.assertEqual(result["groups"][0]["origin"], "model")

    async def test_partial_group_failure_preserves_full_candidates(self):
        session = self.session(["[]", "[]", "[]", "[]", "[]", KEEP_ALL],
                               ModelConfig(retry_delay=0, review_max_group_candidates=1))
        result = await self.review(session)
        self.assertEqual(result["status"], "部分完成")
        self.assertEqual(len(result["properties"][FINAL]), 1)
        self.assertEqual(result["unreviewed_numbers"], ["1.1.0"])
        self.assertFalse(result["groups"][0]["confirmed"])
        self.assertEqual(result["groups"][1]["reasons"], {})

    async def test_all_31_candidates_kept_without_top_cap(self):
        session = self.session([KEEP_ALL])
        result = await self.review(session, 31)
        self.assertEqual(len(result["properties"][FINAL]), 31)
        self.assertEqual(len(result["groups"]), 4)
        self.assertEqual(result["status"], "已审查")

    async def test_empty_candidate_status_stays_local_for_every_terminal_case(self):
        for status in ("无匹配", "文件未匹配", "依据不足", "匹配失败", "文件歧义"):
            with self.subTest(status=status):
                session = self.session([KEEP_ALL])
                matching = {"status": status, "coverage_complete": status in {"无匹配", "文件未匹配"},
                            "properties": {CANDIDATES: [], FINAL: [] if status == "无匹配" else None}}
                result = await review_article({"id": "article", "text": TEXT}, {CANDIDATES: []}, matching, session)
                self.assertEqual(result["status"], status)
                self.assertEqual(result["properties"], {FINAL: matching["properties"][FINAL]})
                self.assertEqual(session.client.payloads, [])

    async def test_log_and_checkpoint_result_are_redacted_and_terminal_escaped(self):
        secret = "test-secret-never-log"
        response = {"removed_indicator_numbers": ["1.1.0"],
                    "reasons": {"1.1.0": "具体冲突\n伪造日志\x1b[31m " + secret}}
        session = self.session([json.dumps(response, ensure_ascii=False)], ModelConfig(api_key=secret, retry_delay=0))
        output = io.StringIO()
        handler = logging.StreamHandler(output)
        logger = logging.getLogger("app.infrastructure.risk_map.review.review")
        old_level = logger.level
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)
        try:
            result = await self.review(session)
        finally:
            logger.removeHandler(handler)
            logger.setLevel(old_level)
        self.assertNotIn(secret, json.dumps([self.events, result], ensure_ascii=False))
        self.assertNotIn(secret, output.getvalue())
        self.assertEqual(len(output.getvalue().splitlines()), 1)
        self.assertIn("\\u000a", output.getvalue())
        self.assertIn("\\u001b", output.getvalue())
        self.assertIn("\n", result["groups"][0]["reasons"]["1.1.0"])

    async def test_audit_io_failure_is_not_retried_as_model_error(self):
        session = self.session([KEEP_ALL])
        def fail(phase, row):
            if row.get("event_type") == "review_decision":
                raise OSError("controlled audit failure")
        session.events = fail
        with self.assertRaises(OSError):
            await self.review(session)
        self.assertEqual(len(session.client.payloads), 1)
        # The cached path must not swallow callback errors either.
        with self.assertRaises(OSError):
            await self.review(session)
        self.assertEqual(len(session.client.payloads), 1)


    async def test_finish_reason_length_rejects_even_valid_json(self):
        import httpx
        cfg = ModelConfig(model="test", api_url="http://localhost/completions", api_key="fake", retry_delay=0)
        client = OpenAICompatibleClient(cfg)
        await client.http.aclose()
        client.http = httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(200,
            json={"choices": [{"finish_reason": "length", "message": {"content": KEEP_ALL}}]})))
        session = ModelSession(cfg, client, self.root / "cache", lambda phase, row: self.events.append(row))
        try:
            with self.assertRaisesRegex(ModelFailure, "incomplete_model_output"):
                await session.ask("review", "task", SYSTEM, {"text": TEXT}, lambda r: validate_removals(r, candidates()))
        finally:
            await client.aclose()
        self.assertEqual(session.counters["requests"], 5)
        failures = [e for e in self.events if e.get("status") == "failed"]
        self.assertEqual(len(failures), 5)
        self.assertTrue(all(e["response"] == KEEP_ALL for e in failures))


class LocationCommitTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        atomic_json(tree(), self.root / "tree.json")
        self.config = MappingConfig("source", "target", tree_path=self.root / "tree.json", output_root=self.root / "out",
                                    write_enabled=True, exclusive_target=True, model=ModelConfig(retry_delay=0))

    def tearDown(self):
        self.temp.cleanup()

    async def run_case(self, fault=None):
        adapter = FakeAdapter(1)
        client = Client(adapter, removal_response(["2.1.1.1"]))
        original_intent, original_ack, original_event = RunRecords.intent, RunRecords.acknowledge, RunRecords.event
        applied = []
        def intent(records, phase, rows, results):
            if fault == "intent" and phase == "matching":
                raise OSError("controlled intent failure")
            return original_intent(records, phase, rows, results)
        def ack(records, key, digest, receipt):
            if fault == "ack" and "matching" in key:
                raise OSError("controlled ack failure")
            return original_ack(records, key, digest, receipt)
        def event(records, phase, row):
            if row.get("event_type") == "location_applied":
                self.assertTrue((records.path / "checkpoints" / (row["batch"] + "_ack.json")).exists())
                if fault == "applied_event":
                    raise OSError("controlled applied event failure")
                applied.append(row)
            return original_event(records, phase, row)
        original_update = adapter.update_subgraph_article_properties
        def update(label, rows, **kwargs):
            if fault == "unknown" and rows[0]["properties"].get(FINAL):
                adapter.unknown = True
            return original_update(label, rows, **kwargs)
        with patch.object(RunRecords, "intent", intent), patch.object(RunRecords, "acknowledge", ack), \
                patch.object(RunRecords, "event", event), patch.object(adapter, "update_subgraph_article_properties", update):
            summary, code = await execute_mapping(self.config, adapter, client=client)
        return adapter, client, summary, code, applied

    async def test_locations_are_durable_before_apply_and_not_graph_properties(self):
        adapter, client, summary, code, applied = await self.run_case()
        self.assertEqual(code, 0)
        self.assertEqual(len(applied), 1)
        records = RunRecords(self.config.output_root, summary["run_id"], resume=True)
        confirmed, pending, patches = records.batches()
        result = confirmed["matching"]["ts0"]
        self.assertEqual(result["sources"][0]["description"], TEXT)
        self.assertFalse(pending)
        self.assertNotIn("review_audit", summary)
        self.assertEqual(summary["location"]["saved_links"], 7)
        self.assertNotIn("reasons", adapter.target["nodes"][0]["n"]["properties"])
        result, code = await execute_mapping(self.config, adapter, action="resume", run_id=summary["run_id"], client=client)
        self.assertEqual(code, 0)
        self.assertEqual(result["location"]["saved_links"], 7)
        self.assertEqual(client.calls, 0)

    async def test_legacy_status_removed_only_from_copy_and_statuses_recover_locally(self):
        adapter = FakeAdapter(1)
        adapter.source["nodes"][0]["n"]["properties"]["三级指标映射状态"] = "历史状态"
        source_before = copy.deepcopy(adapter.source)
        client = Client(adapter)
        summary, code = await execute_mapping(self.config, adapter, client=client)
        self.assertEqual(code, 0)
        self.assertEqual(adapter.source, source_before)
        self.assertNotIn("三级指标映射状态", adapter.target["nodes"][0]["n"]["properties"])
        records = RunRecords(self.config.output_root, summary["run_id"], resume=True)
        confirmed, pending, patches = records.batches()
        self.assertFalse(pending)
        self.assertEqual([confirmed[p]["ts0"]["status"] for p in ("initialization", "matching")],
                         ["未处理", "定位完成"])
        self.assertTrue(all(row["properties"].get("三级指标映射状态") is None for row in patches))
        events = [json.loads(line) for line in (records.path / "matching" / "events.jsonl").read_text(encoding="utf-8").splitlines()]
        self.assertTrue(any(e.get("event_type") == "article_result" and e["status"] == "定位完成" for e in events))
        self.assertEqual(summary["article_statuses"], {"定位完成": 1})
        resumed, code = await execute_mapping(self.config, adapter, action="resume", run_id=summary["run_id"], client=client)
        self.assertEqual(code, 0)
        self.assertEqual(resumed["article_statuses"], summary["article_statuses"])
        self.assertEqual(client.calls, 0)

    async def test_intent_failure_does_not_write_location_patch(self):
        adapter, _, summary, code, applied = await self.run_case("intent")
        self.assertEqual(code, 2)
        self.assertNotIn(FINAL, adapter.target["nodes"][0]["n"]["properties"])
        self.assertEqual(len([c for c in adapter.calls if c[0] == "update"]), 1)
        self.assertFalse(applied)
        self.assertEqual(summary["location"]["saved_links"], 0)

    async def test_unknown_or_missing_ack_never_claims_applied_or_replays(self):
        for fault in ("unknown", "ack"):
            with self.subTest(fault=fault):
                self.config = replace(self.config, output_root=self.root / fault)
                adapter, client, summary, code, applied = await self.run_case(fault)
                self.assertEqual(code, 2)
                self.assertFalse(applied)
                self.assertEqual(summary["pending_batches"], 1)
                self.assertEqual(summary["location"]["saved_links"], 0)
                before = len(adapter.calls)
                result, code = await execute_mapping(self.config, adapter, action="resume", run_id=summary["run_id"], client=client)
                self.assertEqual(result["mapping_status"], "write_unknown")
                self.assertEqual(len(adapter.calls), before)

    async def test_ack_then_log_failure_preserves_confirmed_update(self):
        adapter, client, summary, code, applied = await self.run_case("applied_event")
        self.assertEqual(code, 2)
        self.assertEqual(summary["mapping_status"], "interrupted")
        self.assertEqual(summary["pending_batches"], 0)
        self.assertEqual(summary["location"]["saved_links"], 7)
        before = len(adapter.calls)
        result, code = await execute_mapping(self.config, adapter, action="resume", run_id=summary["run_id"], client=client)
        self.assertEqual(code, 0)
        self.assertEqual(len(adapter.calls), before)

    async def test_old_protocol_rejected_but_unused_group_option_can_change(self):
        adapter, client, summary, _, _ = await self.run_case()
        records = RunRecords(self.config.output_root, summary["run_id"], resume=True)
        before = copy.deepcopy(adapter.target)
        run = records.read("run.json")
        self.assertNotIn("review_protocol", run)
        legacy = dict(run)
        legacy["protocol"] = "copy-first-many-to-many-v3-audit-fixes"
        records.save("run.json", legacy)
        with self.assertRaisesRegex(ValueError, "旧语义审查"):
            await execute_mapping(self.config, adapter, action="resume", run_id=summary["run_id"], client=client)
        self.assertEqual(before, adapter.target)
        records.save("run.json", run)
        changed = replace(self.config, model=replace(self.config.model, review_max_group_candidates=7))
        result, code = await execute_mapping(changed, adapter, action="resume", run_id=summary["run_id"], client=client)
        self.assertEqual(code, 0)
        self.assertEqual(before, adapter.target)


if __name__ == "__main__":
    unittest.main()
