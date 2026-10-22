"""File-family, numbering, partial-evidence and model retry contracts."""

import asyncio
import copy
import json
from pathlib import Path
import tempfile
import unittest

from app.infrastructure.risk_map.common.model_client import ModelSession, ModelFailure
from app.infrastructure.risk_map.common.stats import Diagnostics
from app.infrastructure.risk_map.config import CANDIDATES, ModelConfig
from app.infrastructure.risk_map.matching.location import DocumentAmbiguity, attachment_ids, exact_location, locate_article, validate_match
from app.infrastructure.risk_map.preparation.document_identity import normalize_document_name, revision_family
from app.infrastructure.risk_map.preparation.tree_index import build_tree_index
from app.infrastructure.risk_map.test.test_copy_first import tree, TEXT


class IndexTests(unittest.TestCase):
    def test_regions_and_identity_qualifiers(self):
        index = build_tree_index(tree(), Diagnostics())
        self.assertEqual(len(attachment_ids(index, ["《测试法》（2024年修订）.txt"], "中国")), 7)
        self.assertEqual(attachment_ids(index, ["测试法（试行）.txt"], "中国"), [])
        self.assertEqual(attachment_ids(index, ["测试法.txt"], "美国"), [])
        self.assertNotEqual(revision_family(normalize_document_name("测试法（2024年试行）"))[0], "测试法")

    def test_multiple_versions_all_sources_merge_not_random(self):
        data = tree()
        mapping = data["产品法律风险"]["一级"]["二级"]["指标1"]["映射文件"]["中国"]
        mapping["测试法（2023年修订）"] = {"原始文本": "旧版本", "已找到": "", "依据文件名": "测试法（2023年修订）", "知识依据": TEXT}
        index = build_tree_index(data, Diagnostics())
        ids = attachment_ids(index, ["测试法（2024修正）.txt"], "中国")
        self.assertEqual(len(ids), 8)
        self.assertEqual(len({index.attachments[i].indicator_number for i in ids}), 7)

    def test_conflicting_formal_indicator_number_rejected(self):
        data = tree()
        level = data["产品法律风险"]["一级"]["二级"]
        level["冲突指标"] = copy.deepcopy(level["指标1"])
        level["冲突指标"]["指标描述"] = "不同含义"
        with self.assertRaises(ValueError):
            build_tree_index(data, Diagnostics())

    def test_conflicting_source_files_not_silently_union(self):
        data = tree()
        mapping = data["产品法律风险"]["一级"]["二级"]["指标1"]["映射文件"]["中国"]
        mapping["不同法规"] = {"原始文本": "不同法规", "已找到": "", "依据文件名": "不同法规", "知识依据": TEXT}
        index = build_tree_index(data, Diagnostics())
        with self.assertRaises(DocumentAmbiguity):
            attachment_ids(index, ["测试法.txt", "不同法规.txt"], "中国")

    def test_short_local_excerpt_different_number_can_match(self):
        self.assertTrue(exact_location({"text": "第二条 不得泄密。"}, "第一条 不得泄密。"))
        self.assertIsNone(exact_location({"text": "第一条 企业应当申报。"}, "第一条 企业不得销毁资料。"))


class ModelProtocolTests(unittest.IsolatedAsyncioTestCase):
    async def test_five_invalid_responses_include_specific_feedback(self):
        responses = []
        class Invalid:
            async def complete(inner, payload):
                responses.append(payload)
                return {"match": "也许", "reason": "不确定"}
        with tempfile.TemporaryDirectory() as folder:
            session = ModelSession(ModelConfig(retry_delay=0), Invalid(), Path(folder), lambda *a: None)
            with self.assertRaises(ModelFailure):
                await session.ask("matching", "article", "test", {"text": TEXT}, validate_match)
        self.assertEqual(len(responses), 5)
        self.assertIn("expected_match", responses[1]["messages"][-1]["content"])

    async def test_different_number_conflicting_body_reaches_model(self):
        data = tree(description="第一条 企业不得销毁资料。")
        index = build_tree_index(data, Diagnostics())
        calls = []
        class Decide:
            async def complete(inner, payload):
                calls.append(payload)
                return {"match": "是", "reason": "历史编号不同但描述存在局部对应"}
        with tempfile.TemporaryDirectory() as folder:
            session = ModelSession(ModelConfig(retry_delay=0), Decide(), Path(folder), lambda *a: None)
            result = await locate_article({"id": "target", "text": "第二条 企业应当保存档案。", "number": "第二条",
                                          "filenames": ["测试法"]}, index, "中国", session)
        self.assertEqual(len(calls), 1)
        self.assertEqual(len(result["properties"][CANDIDATES]), 7)

    async def test_cache_scope_isolation(self):
        calls = []
        class Decide:
            async def complete(inner, payload):
                calls.append(payload)
                return {"match": "否", "reason": "不对应"}
        with tempfile.TemporaryDirectory() as folder:
            for scope in ("A", "A", "B"):
                session = ModelSession(ModelConfig(retry_delay=0), Decide(), Path(folder), lambda *a: None, scope=scope)
                await session.ask("matching", "task", "test", {"text": TEXT}, validate_match)
        self.assertEqual(len(calls), 2)


if __name__ == "__main__":
    unittest.main()
