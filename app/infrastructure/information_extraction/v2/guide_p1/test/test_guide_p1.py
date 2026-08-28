from __future__ import annotations

import asyncio
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from app.infrastructure.information_extraction.v2.guide_p1.config import DEFAULT_CONFIG
from app.infrastructure.information_extraction.v2.guide_p1.extractor import GuideP1Extractor
from app.infrastructure.information_extraction.v2.guide_p1.graph_builder import GuideP1GraphBuilder, prepare_guide_p1_kg_for_neo4j
from app.infrastructure.information_extraction.v2.guide_p1.io_utils import InputDiscoveryError, discover_input_files
from app.infrastructure.information_extraction.v2.guide_p1.structure_parser import GuideP1StructureParser
from app.infrastructure.information_extraction.v2.guide_p1.validator import SCHEMA_CONTRACT, validate_extraction_result


SAMPLE_TEXT = """合规风险类型：产品法律风险 企业国际化经营合规风险
示例部门关于印发《示例发展规划》的通知
示发〔2026〕1号
示例部门
2026年1月2日
示例发展规划
一、总体要求
（一）指导思想。
近年来产业基础持续增强。
二、重点任务
（一）推进绿色改造。推动重点设备升级，到2028年能耗降低10%。（示例部门牵头负责）
专栏1重点工程
建设示范项目，完善配套能力。
一、组织实施
（一）强化监督。严禁违规建设，确保任务落实。
附件1
重点项目清单
序号 项目 单位
1 项目甲 单位甲
"""


class TrackingMockExtractor:
    def __init__(self, fail_token: str = "", empty_token: str = "近年来") -> None:
        self.fail_token = fail_token
        self.empty_token = empty_token
        self.active = 0
        self.max_active = 0

    async def entity_and_relationship_extract(self, *, schema: str, input_text: str, **_: object) -> dict:
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            await asyncio.sleep(0.01)
            if self.fail_token and self.fail_token in input_text:
                raise RuntimeError("all retries failed")
            if "## 合规指引文件" in schema:
                return {
                    "entities": [
                        {
                            "name": "示例发展规划",
                            "entity_type": "合规指引文件",
                            "properties": {
                                "文件全称": "示例发展规划",
                                "文件类型": "规划",
                                "合规风险类型": ["产品法律风险"],
                            },
                        }
                    ],
                    "relations": [],
                }
            if self.empty_token and self.empty_token in input_text:
                return {"entities": [], "relations": []}
            return {
                "entities": [
                    {
                        "name": "推进示例任务",
                        "entity_type": "指引知识单元",
                        "properties": {
                            "知识内容": "推进示例任务",
                            "知识类型": "行动任务",
                            "约束强度": "指导性要求",
                            "原文依据": "",
                            "是否知识条目候选": True,
                            "动态错误字段": "应删除",
                        },
                    },
                    {
                        "name": "示例部门",
                        "entity_type": "责任主体",
                        "properties": {"主体名称": "示例部门", "责任角色": "牵头"},
                    },
                ],
                "relations": [
                    {"source": "推进示例任务", "type": "由其负责", "target": "示例部门", "properties": {}}
                ],
            }
        finally:
            self.active -= 1


class StructureParserTests(unittest.TestCase):
    def test_repeated_numbering_columns_and_attachment_skip(self) -> None:
        result = GuideP1StructureParser().parse_text(SAMPLE_TEXT, filename="示例发展规划.txt")
        self.assertTrue(result["validation"]["passed"])
        top_nodes = [node for node in result["structure_nodes"] if node["node_type"] == "一级标题"]
        self.assertEqual(3, len(top_nodes))
        self.assertEqual(2, sum(node["number"] == "一、" for node in top_nodes))
        self.assertEqual(1, sum(node["node_type"] == "专栏" for node in result["structure_nodes"]))
        self.assertEqual(1, len(result["skipped_attachments"]))
        self.assertEqual("skipped", result["skipped_attachments"][0]["status"])
        self.assertFalse(any("重点项目清单" in block["text"] for block in result["semantic_blocks"]))

    def test_inline_point_and_inherited_context(self) -> None:
        result = GuideP1StructureParser().parse_text(SAMPLE_TEXT, filename="示例发展规划.txt")
        block = next(item for item in result["semantic_blocks"] if "重点设备升级" in item["text"])
        self.assertIn("二、重点任务", block["ancestor_context"])
        self.assertIn("推进绿色改造", block["source_path"])
        self.assertTrue(block["high_knowledge_signal"])

    def test_long_block_is_bounded(self) -> None:
        config = replace(DEFAULT_CONFIG, semantic_block_chars=500)
        text = "合规风险类型：产品法律风险\n示例规划\n一、任务\n（一）推进工作。" + "推进设备升级。" * 200
        result = GuideP1StructureParser(config).parse_text(text, filename="示例规划.txt")
        self.assertGreater(len(result["semantic_blocks"]), 1)
        self.assertTrue(all(len(item["text"]) <= 500 for item in result["semantic_blocks"]))

    def test_empty_directory_does_not_fall_back(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(InputDiscoveryError):
                discover_input_files(directory)


class SchemaAndGraphTests(unittest.TestCase):
    def test_schema_contract_contains_expected_rules(self) -> None:
        self.assertIn("指引知识单元", SCHEMA_CONTRACT.allowed_properties)
        self.assertIn(("指引知识单元", "由其负责", "责任主体"), SCHEMA_CONTRACT.legal_triples)

    def test_dynamic_property_and_illegal_relation_are_removed(self) -> None:
        raw = {
            "entities": [
                {
                    "name": "任务",
                    "entity_type": "指引知识单元",
                    "properties": {"知识内容": "推进任务", "动态字段": "错误", "知识类型": "随意类型"},
                },
                {"name": "主体", "entity_type": "责任主体", "properties": {"主体名称": "主体"}},
            ],
            "relations": [{"source": "主体", "type": "设定目标", "target": "任务", "properties": {}}],
        }
        validated = validate_extraction_result(raw, source_text="推进任务")
        self.assertNotIn("动态字段", validated["entities"][0]["properties"])
        self.assertEqual("", validated["entities"][0]["properties"]["知识类型"])
        self.assertEqual([], validated["relations"])
        self.assertGreaterEqual(len(validated["warnings"]), 3)

    def test_framework_prefixed_relation_endpoints_are_resolved(self) -> None:
        raw = {
            "entities": [
                {
                    "name": "推进任务",
                    "entity_type": "指引知识单元",
                    "properties": {"知识内容": "推进任务", "知识类型": "行动任务"},
                },
                {
                    "name": "10%",
                    "entity_type": "量化目标",
                    "properties": {
                        "原始文本": "下降10%",
                        "量化值类型": "比例",
                        "最小值": 10,
                        "最大值": 10,
                        "单位": "%",
                        "约束关系": "等于",
                    },
                },
            ],
            "relations": [
                {
                    "source": "指引知识单元_推进任务",
                    "type": "设定目标",
                    "target": "量化目标_10%",
                    "properties": {},
                }
            ],
        }
        validated = validate_extraction_result(raw, source_text="推进任务，下降10%")
        self.assertEqual(
            [{"source": "推进任务", "target": "10%", "type": "设定目标", "properties": {}}],
            validated["relations"],
        )
        self.assertFalse(any("端点无法解析" in warning for warning in validated["warnings"]))

    def test_graph_binds_source_and_deduplicates_responsible_body(self) -> None:
        parse_result = GuideP1StructureParser().parse_text(SAMPLE_TEXT, filename="示例发展规划.txt")
        blocks = parse_result["semantic_blocks"][:2]
        content_items = []
        for index, block in enumerate(blocks):
            content_items.append(
                {
                    "status": "success",
                    "block": block,
                    "validated": {
                        "entities": [
                            {
                                "name": f"任务{index}",
                                "entity_type": "指引知识单元",
                                "properties": {"知识内容": f"任务{index}", "知识类型": "行动任务"},
                            },
                            {
                                "name": "示例部门",
                                "entity_type": "责任主体",
                                "properties": {"主体名称": "示例部门", "责任角色": "牵头"},
                            },
                        ],
                        "relations": [],
                        "warnings": [],
                    },
                }
            )
        graph = GuideP1GraphBuilder("示例发展规划.txt", parse_result).build({"content_blocks": content_items})
        bodies = [node for node in graph["nodes"] if node["node_type"] == "责任主体"]
        units = [node for node in graph["nodes"] if node["node_type"] == "指引知识单元"]
        self.assertEqual(1, len(bodies))
        self.assertEqual(2, len(units))
        self.assertTrue(all(node["properties"].get("source_structure_id") for node in units))
        neo4j_graph = prepare_guide_p1_kg_for_neo4j(graph)
        self.assertEqual(len(graph["nodes"]), len(neo4j_graph["nodes"]))


class ExtractorTests(unittest.IsolatedAsyncioTestCase):
    async def test_success_empty_and_concurrency_bound(self) -> None:
        mock = TrackingMockExtractor()
        config = replace(DEFAULT_CONFIG, max_concurrent=2, strict_mode=True)
        extractor = GuideP1Extractor(config=config, llm_extractor=mock)
        result = await extractor.extract_text(SAMPLE_TEXT, filename="示例发展规划.txt")
        self.assertEqual("success", result["status"])
        self.assertLessEqual(mock.max_active, 2)
        self.assertGreater(result["run_stats"]["llm_empty_count"], 0)
        self.assertGreater(result["run_stats"]["knowledge_unit_count"], 0)
        self.assertEqual(0, result["run_stats"]["extraction_error"])

    async def test_strict_failure_has_no_parser_semantic_fallback(self) -> None:
        mock = TrackingMockExtractor(fail_token="严禁违规建设")
        config = replace(DEFAULT_CONFIG, max_concurrent=3, strict_mode=True)
        extractor = GuideP1Extractor(config=config, llm_extractor=mock)
        result = await extractor.extract_text(SAMPLE_TEXT, filename="示例发展规划.txt")
        self.assertEqual("incomplete", result["status"])
        self.assertFalse(result["formal_graph_eligible"])
        self.assertGreater(result["run_stats"]["extraction_error"], 0)
        failed_source_ids = {
            item["block"]["source_node_id"] for item in result["extraction_result"]["failed_blocks"]
        }
        semantic_sources = {
            node["properties"].get("source_structure_id")
            for node in result["graph"]["nodes"]
            if node["node_type"] == "指引知识单元"
        }
        self.assertTrue(failed_source_ids.isdisjoint(semantic_sources))

    async def test_stage_artifacts_are_written(self) -> None:
        mock = TrackingMockExtractor(empty_token="")
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "示例规划.txt"
            output = Path(directory) / "output"
            source.write_text(SAMPLE_TEXT, encoding="utf-8")
            extractor = GuideP1Extractor(llm_extractor=mock)
            await extractor.extract_file(source, output_dir=output)
            for stage in ("split", "extraction", "kg", "summary"):
                self.assertTrue((output / f"示例规划_{stage}.json").exists())


if __name__ == "__main__":
    unittest.main()
