"""Audit regression contracts, independent of production connections."""

import asyncio
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from app.infrastructure.risk_map import MappingConfig, ModelConfig, execute_mapping
from app.infrastructure.risk_map.common.io import atomic_json, file_hash, read_json
from app.infrastructure.risk_map.common.json_output import parse_audit, reset_audit
from app.infrastructure.risk_map.common.model_client import ModelFailure, ModelSession
from app.infrastructure.risk_map.common.stats import Diagnostics
from app.infrastructure.risk_map.config import CANDIDATES, FINAL, PROTOCOL
from app.infrastructure.risk_map.local_records import RunRecords
from app.infrastructure.risk_map.matching.location import attachment_ids, locate_article, resolve_attachments, validate_match
from app.infrastructure.risk_map.preparation.document_identity import normalize_document_name, revision_family
from app.infrastructure.risk_map.preparation.tree_index import build_tree_index
from app.infrastructure.risk_map.review.review import EXAMPLES, validate_removals
from app.infrastructure.risk_map.test.test_copy_first import Client, FakeAdapter, KEEP_ALL, TEXT, tree


def group():
    return [{"指标序号": "2.2.1.1", "指标名": "管理", "指标描述": "保存与报送"}]


def rename_tree(data, title):
    for node in data["产品法律风险"]["一级"]["二级"].values():
        if not isinstance(node, dict):
            continue
        files = node["映射文件"]["中国"]
        leaf = files.pop("测试法")
        leaf["依据文件名"] = title
        files[title] = leaf
    return data


class RepairTests(unittest.TestCase):
    def test_syntax_only_punctuation_and_quotes(self):
        for response in ('{"match":"是","reason":"应当保存",}',
                         '{"match":"是" "reason":"应当保存"}',
                         "{'match':'是','reason':'应当保存'}"):
            with self.subTest(response=response):
                reset_audit()
                self.assertEqual(validate_match(response), {"match": "是", "reason": "应当保存"})
                self.assertTrue(parse_audit()["success"])

    def test_review_repair_preserves_all_values(self):
        response = '{"removed_indicator_numbers":["2.2.1.1",],"reasons":{"2.2.1.1":"无对应",},}'
        self.assertEqual(validate_removals(response, group())["reasons"], {"2.2.1.1": "无对应"})

    def test_duplicates_in_both_stages_and_combined_malformed_input(self):
        for response in ('{"match":"是","match":"否","reason":"理由"}',
                         '{"match":"是","match":"否","reason":"理由",}'):
            with self.subTest(response=response), self.assertRaises(ModelFailure):
                validate_match(response)
        for response in ('{"removed_indicator_numbers":[],"reasons":{"x":"甲","x":"乙",}}',
                         '{"removed_indicator_numbers":[],"removed_indicator_numbers":[],"reasons":{},}'):
            with self.subTest(response=response), self.assertRaises(ModelFailure):
                validate_removals(response, group())

    def test_missing_values_truncation_multiple_and_comments_rejected(self):
        for response in ('{"match":"是","reason":}', '{"match":"是","reason":"x"',
                         '{"match":"是","reason":"x"}{}',
                         '{"match":"是","reason":"x"}说明',
                         '{"match":"是",/*注释*/"reason":"x"}'):
            with self.subTest(response=response), self.assertRaises(ModelFailure):
                validate_match(response)

    def test_repair_does_not_fix_protocol_fields(self):
        with self.assertRaisesRegex(ModelFailure, "reason_numbers_mismatch"):
            validate_removals('{"removed_indicator_numbers":["2.2.1.1"],"reasons":{},}', group())
        with self.assertRaises(ModelFailure):
            validate_match('{"match":true,"reason":"x",}')

    def test_library_failure_does_not_bypass_validation(self):
        with patch("json_repair.repair_json", side_effect=ValueError("controlled")), self.assertRaises(ModelFailure):
            validate_match('{"match":"是","reason":"x",}')

    def test_library_value_changes_are_rejected(self):
        with patch("json_repair.repair_json", return_value='{"match":"否","reason":"x"}'), \
                self.assertRaisesRegex(ModelFailure, "changes_business_tokens"):
            validate_match('{"match":"是","reason":"x",}')


class FilenameTests(unittest.TestCase):
    def test_country_prefix_both_directions_same_region(self):
        for title, query in (("中华人民共和国测试法", "测试法.txt"), ("测试法", "中华人民共和国测试法.txt")):
            index = build_tree_index(rename_tree(tree(1), title), Diagnostics())
            ids, info = resolve_attachments(index, [query], "中国")
            self.assertEqual(len(ids), 1)
            self.assertEqual(info["method"], "country_prefix")
            self.assertEqual(attachment_ids(index, [query], "美国"), [])

    def test_revision_metadata_and_qualifiers(self):
        index = build_tree_index(tree(1), Diagnostics())
        ids, info = resolve_attachments(index, ["《测试法》（2019改编）.txt"], "中国")
        self.assertEqual(len(ids), 1)
        self.assertEqual(info["sources"][0]["revision_annotations"], ("(2019改编)",))
        for name in ("测试法（试行）", "测试法（2024暂行）", "测试法（草案）"):
            self.assertEqual(attachment_ids(index, [name], "中国"), [])
        self.assertEqual(revision_family(normalize_document_name("GB/T 4754-2017"))[0], "gb/t4754-2017")
        self.assertEqual(revision_family(normalize_document_name("测试(2019修订)管理办法"))[0], "测试(2019修订)管理办法")

    def test_exact_hit_does_not_expand_to_other_revision(self):
        data = tree(1)
        files = data["产品法律风险"]["一级"]["二级"]["指标1"]["映射文件"]["中国"]
        files["测试法(2020修订)"] = {**files["测试法"], "依据文件名": "测试法(2020修订)"}
        index = build_tree_index(data, Diagnostics())
        ids, info = resolve_attachments(index, ["测试法.txt"], "中国")
        self.assertEqual(len(ids), 1)
        self.assertEqual(info["method"], "exact")
        from app.infrastructure.risk_map.matching.location import DocumentAmbiguity
        with self.assertRaises(DocumentAmbiguity):
            attachment_ids(index, ["测试法(2024修订)"], "中国")

    def test_conflicting_alias_is_not_arbitrarily_chosen(self):
        from app.infrastructure.risk_map.matching.location import DocumentAmbiguity
        data = tree(1)
        files = data["产品法律风险"]["一级"]["二级"]["指标1"]["映射文件"]["中国"]
        files["中华人民共和国测试法"] = {**files["测试法"], "依据文件名": "另一部法"}
        files["中华人民共和国测试法.txt"] = {**files["测试法"], "依据文件名": "第三部法"}
        index = build_tree_index(data, Diagnostics())
        with self.assertRaises(DocumentAmbiguity):
            resolve_attachments(index, ["中华人民共和国测试法"], "中国")


class SessionFixTests(unittest.IsolatedAsyncioTestCase):
    async def test_fewshot_examples_precede_actual_task_and_enter_cache_key(self):
        calls = []
        class Keep:
            async def complete(self, payload):
                calls.append(payload)
                return KEEP_ALL
        with tempfile.TemporaryDirectory() as folder:
            session = ModelSession(ModelConfig(retry_delay=0), Keep(), Path(folder), lambda *a: None)
            for examples in (EXAMPLES, EXAMPLES, EXAMPLES[:-1]):
                await session.ask("review", "a", "test", {"candidates": group()},
                    lambda r: validate_removals(r, group()), examples=examples)
        self.assertEqual(len(calls), 2)
        self.assertEqual(json.loads(calls[0]["messages"][-1]["content"]), {"candidates": group()})
        self.assertEqual(sum(m["role"] == "assistant" for m in calls[0]["messages"]), len(EXAMPLES))

    async def test_concurrent_parse_audits_do_not_leak(self):
        events = []
        class Mixed:
            async def complete(self, payload):
                key = json.loads(payload["messages"][-1]["content"])["key"]
                await asyncio.sleep(0)
                return '{"match":"是","reason":"依据",}' if key % 2 else '{"match":"否","reason":"无依据"}'
        with tempfile.TemporaryDirectory() as folder:
            session = ModelSession(ModelConfig(retry_delay=0), Mixed(), Path(folder), lambda p, r: events.append(r))
            await asyncio.gather(*(session.ask("matching", str(i), "test", {"key": i}, validate_match) for i in range(20)))
        self.assertEqual(session.counters["json_repair_successes"], 10)
        self.assertEqual({e["task"] for e in events if e.get("event_type") == "json_repair"},
                         {str(i) for i in range(20) if i % 2})

    async def test_retry_budget_and_secret_redaction(self):
        calls, events = [], []
        secret = "secret-only-for-test"
        class Fix:
            async def complete(self, payload):
                calls.append(payload)
                if len(calls) == 1:
                    raise ModelFailure("invalid_json:" + secret + "X" * 4000)
                return {"match": "否", "reason": "无对应"}
        with tempfile.TemporaryDirectory() as folder:
            config = ModelConfig(retry_delay=0, max_input_chars=600, api_key=secret)
            session = ModelSession(config, Fix(), Path(folder), lambda p, r: events.append(r))
            await session.ask("matching", "a", "test", {"text": "完整原始输入"}, validate_match)
        self.assertEqual(len(calls), 2)
        self.assertTrue(all(len(json.dumps(p, ensure_ascii=False)) <= 600 for p in calls))
        self.assertNotIn(secret, json.dumps([calls, events], ensure_ascii=False))
        self.assertEqual(calls[0]["messages"][:2], calls[1]["messages"][:2])

    async def test_repair_once_without_extra_call_and_cache(self):
        calls, events = [], []
        class RepairClient:
            async def complete(self, payload):
                calls.append(payload)
                return '{"match":"是","reason":"依据",}'
        with tempfile.TemporaryDirectory() as folder:
            session = ModelSession(ModelConfig(retry_delay=0), RepairClient(), Path(folder), lambda p, r: events.append(r))
            for _ in range(2):
                self.assertEqual((await session.ask("matching", "a", "test", {}, validate_match))["match"], "是")
        self.assertEqual(len(calls), 1)
        self.assertEqual(session.counters["json_repair_successes"], 1)
        self.assertEqual(session.counters["cache_hits"], 1)
        self.assertTrue(any(e.get("event_type") == "json_repair" and e["accepted"] for e in events))

    async def test_feedback_has_missing_extra_and_original_group(self):
        calls = []
        class Correcting:
            async def complete(self, payload):
                calls.append(copy.deepcopy(payload))
                if len(calls) == 1:
                    return {"removed_indicator_numbers": ["2.2.1.1"], "reasons": {"2.1.1.1": "错误序号"}}
                return {"removed_indicator_numbers": ["2.2.1.1"], "reasons": {"2.2.1.1": "无对应"}}
        with tempfile.TemporaryDirectory() as folder:
            session = ModelSession(ModelConfig(retry_delay=0), Correcting(), Path(folder), lambda *a: None)
            await session.ask("review", "a", "test", {"candidates": group()}, lambda r: validate_removals(r, group()))
        self.assertEqual(calls[0]["messages"][:2], calls[1]["messages"][:2])
        feedback = calls[1]["messages"][-1]["content"]
        for expected in ("2.2.1.1", "2.1.1.1", "missing", "extra", "完整结果"):
            self.assertIn(expected, feedback)

    async def test_five_attempts_and_repair_rejection_are_not_success(self):
        calls = []
        class Bad:
            async def complete(self, payload):
                calls.append(payload)
                return '{"match":"是","match":"否","reason":"依据",}'
        with tempfile.TemporaryDirectory() as folder:
            session = ModelSession(ModelConfig(retry_delay=0), Bad(), Path(folder), lambda *a: None)
            with self.assertRaises(ModelFailure):
                await session.ask("matching", "a", "test", {}, validate_match)
            self.assertFalse(list(Path(folder).glob("*.json")))
        self.assertEqual(len(calls), 5)
        self.assertEqual(session.counters["json_repair_successes"], 0)


class SkipAndSnapshotTests(unittest.IsolatedAsyncioTestCase):
    async def test_all_empty_no_model_and_unique_counts(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            atomic_json(tree(2, " \r\n\t"), root / "tree.json")
            adapter = FakeAdapter(21)
            client = Client(adapter)
            config = MappingConfig("source", "target", tree_path=root / "tree.json", output_root=root / "out",
                write_enabled=True, exclusive_target=True, model=ModelConfig(api_key="never-in-snapshot", retry_delay=0))
            summary, code = await execute_mapping(config, adapter, client=client)
            self.assertEqual(code, 0)
            self.assertEqual(client.calls, 0)
            self.assertEqual(summary["article_statuses"], {"无可用依据": 21})
            self.assertEqual(summary["failed_stage_tasks"], 0)
            for key, value in {"unique_empty_attachments": 2, "document_families": 1,
                               "affected_articles": 21, "all_empty_articles": 21}.items():
                self.assertEqual(summary["basis_skips"][key], value)
            self.assertEqual(summary["basis_skips"]["by_reason"]["empty_reason_unverified"]["unique_empty_attachments"], 2)
            records = RunRecords(config.output_root, summary["run_id"], resume=True)
            inputs = records.path / "inputs"
            self.assertEqual((root / "tree.json").read_bytes(), (inputs / "indicator_tree.json").read_bytes())
            manifest = read_json(inputs / "manifest.json")
            self.assertEqual(manifest["tree_hash"], file_hash(root / "tree.json"))
            self.assertNotIn("never-in-snapshot", (inputs / "configuration.json").read_text(encoding="utf-8"))
            for row in adapter.target["nodes"]:
                self.assertEqual(row["n"]["properties"][FINAL], [])
                self.assertNotIn("三级指标映射状态", row["n"]["properties"])
            (inputs / "prompts.json").write_text("{}", encoding="utf-8")
            again, code = await execute_mapping(config, adapter, action="resume", run_id=summary["run_id"], client=client)
            self.assertNotEqual(code, 0)
            self.assertIn("冻结输入", again["error"])

    async def test_real_failure_is_not_hidden_by_empty_skip(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            data = tree(2, "企业必须开展专项风险评估。")
            data["产品法律风险"]["一级"]["二级"]["指标2"]["映射文件"]["中国"]["测试法"]["知识依据"] = ""
            atomic_json(data, root / "tree.json")
            adapter = FakeAdapter(1)
            client = Client(adapter, failure=True)
            config = MappingConfig("source", "target", tree_path=root / "tree.json", output_root=root / "out",
                write_enabled=True, exclusive_target=True, model=ModelConfig(retry_delay=0))
            summary, code = await execute_mapping(config, adapter, client=client)
            self.assertEqual(code, 2)
            self.assertEqual(summary["article_statuses"], {"匹配失败": 1})
            self.assertEqual(summary["basis_skips"]["unique_empty_attachments"], 1)
            self.assertEqual(client.calls, 5)

    async def test_invalid_knowledge_type_still_rejected(self):
        for invalid in (None, [], {}):
            data = tree(1)
            data["产品法律风险"]["一级"]["二级"]["指标1"]["映射文件"]["中国"]["测试法"]["知识依据"] = invalid
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                build_tree_index(data, Diagnostics())

    async def test_v2_run_does_not_resume_as_v3(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            atomic_json(tree(1), root / "tree.json")
            adapter = FakeAdapter(1)
            config = MappingConfig("source", "target", tree_path=root / "tree.json", output_root=root / "out",
                write_enabled=True, exclusive_target=True, model=ModelConfig(retry_delay=0))
            summary, _ = await execute_mapping(config, adapter, client=Client(adapter))
            records = RunRecords(config.output_root, summary["run_id"], resume=True)
            saved = records.read("run.json")
            self.assertEqual(saved["protocol"], PROTOCOL)
            records.save("run.json", {**saved, "protocol": "copy-first-many-to-many-v2-local-status"})
            before = copy.deepcopy(adapter.target)
            _, code = await execute_mapping(config, adapter, action="resume", run_id=summary["run_id"])
            self.assertNotEqual(code, 0)
            self.assertEqual(before, adapter.target)


if __name__ == "__main__":
    unittest.main()
