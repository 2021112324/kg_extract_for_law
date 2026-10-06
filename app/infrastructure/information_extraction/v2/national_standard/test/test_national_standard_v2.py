from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.infrastructure.information_extraction.v2.national_standard.graph_extract import (
    CORE_ENTITY_TYPES,
    CORE_RELATION_TRIPLES,
    NODE_ALLOWED_PROPERTIES,
    NationalStandardGraphExtractor,
    _GraphBuilder,
)
from app.infrastructure.information_extraction.v2.national_standard.national_standard_extract import (
    NationalStandardExtractor,
    StandardBlock,
    public_national_standard_parse_result,
)
from app.infrastructure.information_extraction.v2.national_standard.stats import ResultStats
from app.infrastructure.information_extraction.v2.national_standard.standalone import (
    extract_and_store_national_standard,
)


def _parse_result_with_resources() -> dict:
    blocks = [
        StandardBlock("title", "title", "1 技术要求"),
        StandardBlock("text", "paragraph", "产品的标识应清晰可见。"),
        StandardBlock("image", "image", "图1 产品标识示意图", path="images/a.png"),
        StandardBlock("caption", "paragraph", "图1 产品标识示意图"),
        StandardBlock("table", "table", "|项目|限值|\n|铅|0.1%|"),
        StandardBlock("flow", "flowchart", "flowchart TD\nA-->B"),
    ]
    return NationalStandardExtractor(include_resource_descriptions=True).extract_from_blocks(
        blocks,
        {"root": Path("sample")},
    )


def test_v2_schema_only_allows_core_nodes_and_resource_description_properties() -> None:
    assert CORE_ENTITY_TYPES == {
        "标准文件",
        "标准依据",
        "标准结构节点",
        "术语定义",
        "标准要求",
        "指标限值",
        "试验检测方法",
        "引用标准",
    }
    assert {"图片", "表格", "流程图"}.isdisjoint(NODE_ALLOWED_PROPERTIES)
    assert {"表格描述", "流程图描述"} <= NODE_ALLOWED_PROPERTIES["标准结构节点"]
    assert all(source not in {"图片", "表格", "流程图"} for source, _, _ in CORE_RELATION_TRIPLES)
    assert all(target not in {"图片", "表格", "流程图"} for _, _, target in CORE_RELATION_TRIPLES)


def test_first_stage_discards_images_and_keeps_resources_private() -> None:
    result = _parse_result_with_resources()
    node = result["body_tree"][0]
    stats = result["validation"]["resource_stats"]

    assert "产品的标识应清晰可见" in node["content"]
    assert "产品标识示意图" not in node["content"]
    assert stats["skipped_image_blocks"] == 1
    assert stats["skipped_image_texts"] == 2
    assert stats["table_contexts"] == 1
    assert stats["flowchart_contexts"] == 1
    assert len(result["_resource_contexts"]) == 2

    public = public_national_standard_parse_result(result)
    serialized = json.dumps(public, ensure_ascii=False)
    assert "_resource_contexts" not in public
    assert "resource_context_ids" not in serialized
    assert "|项目|限值|" not in serialized
    assert "flowchart TD" not in serialized
    assert "images/a.png" not in serialized


def test_markdown_image_is_removed_but_same_line_text_is_kept(tmp_path: Path) -> None:
    source = tmp_path / "full.md"
    source.write_text(
        "# 1 要求\n产品应符合规定。![图1 示意图](images/a.png)\n",
        encoding="utf-8",
    )
    blocks = NationalStandardExtractor()._load_markdown_blocks(source)
    paragraphs = [item.text for item in blocks if item.block_type == "paragraph"]
    images = [item for item in blocks if item.block_type == "image"]
    assert paragraphs == ["产品应符合规定。"]
    assert len(images) == 1
    assert images[0].text == "图1 示意图"


def test_graph_merges_descriptions_into_structure_node_and_filters_resource_nodes() -> None:
    parse_result = _parse_result_with_resources()
    stats = ResultStats()
    builder = _GraphBuilder("测试标准", stats, include_resource_descriptions=True)
    builder.add_context_graph(parse_result)
    builder.add_llm_extractions(
        [
            {
                "scope": "tree_node",
                "node_number": "1",
                "node_title": "技术要求",
                "status": "success",
                "entities": [
                    {
                        "name": "1 技术要求",
                        "type": "标准结构节点",
                        "properties": {
                            "节点编号": "1",
                            "表格描述": [
                                "该表说明产品中铅含量的限值。",
                                "该表说明产品中铅含量的限值。",
                                "<table><tr><td>原始表格</td></tr></table>",
                            ],
                            "流程图描述": "该流程说明产品检验与判定的先后步骤。",
                        },
                    },
                    {
                        "name": "表1",
                        "type": "表格",
                        "properties": {"表格描述": "该表补充说明限值对应的检测项目。", "表格内容": "原始表格"},
                    },
                    {
                        "name": "图1",
                        "type": "图片",
                        "properties": {"图片链接": "images/a.png"},
                    },
                    {
                        "name": "1要求1",
                        "type": "标准要求",
                        "properties": {
                            "要求内容": "产品的标识应清晰可见。",
                            "约束强度": "强制",
                            "约束强度原文": "产品的标识应清晰可见。",
                            "行为描述": "保持产品标识清晰可见",
                            "量化特征": "定性",
                        },
                    },
                ],
                "relations": [
                    {
                        "source": "标准结构节点_1 技术要求",
                        "target": "标准要求_1要求1",
                        "type": "规定",
                        "properties": {},
                    },
                    {
                        "source": "标准结构节点_1 技术要求",
                        "target": "表格_表1",
                        "type": "包含",
                        "properties": {},
                    },
                ],
            }
        ]
    )
    graph = builder.to_graph()
    builder.validate_and_report()

    assert {node["node_type"] for node in graph["nodes"]} <= CORE_ENTITY_TYPES
    structure = next(node for node in graph["nodes"] if node["node_type"] == "标准结构节点")
    assert structure["properties"]["表格描述"] == [
        "该表说明产品中铅含量的限值。",
        "该表补充说明限值对应的检测项目。",
    ]
    assert structure["properties"]["流程图描述"] == ["该流程说明产品检验与判定的先后步骤。"]
    assert all(edge["relation_type"] != "包含" or not edge["target_id"].startswith("表格_") for edge in graph["edges"])
    assert stats.filtered_nodes == 2
    assert stats.valid_descriptions == 3
    assert stats.invalid_descriptions >= 2


@pytest.mark.asyncio
async def test_all_llm_failures_are_reported_without_semantic_fallback() -> None:
    parse_result = _parse_result_with_resources()
    extractor = NationalStandardGraphExtractor.__new__(NationalStandardGraphExtractor)
    extractor.include_validation_failed = True
    extractor.include_resource_descriptions = True
    extractor.max_tree_nodes = None
    extractor.result_stats = ResultStats()

    async def failed_extract(*_args, **_kwargs):
        return [{"scope": "file_info", "status": "failed", "error": "API 500", "entities": [], "relations": []}]

    extractor._extract_with_llm = failed_extract
    result = await extractor.extract_from_parse_result(parse_result, "测试标准")

    assert result["status"] == "llm_partial_failed"
    assert result["stats"]["llm_failed_count"] == 1
    assert extractor.result_stats.extraction_errors == 1
    assert all(node["source"] == "first_stage_context" for node in result["graph"]["nodes"])
    assert not any(node["node_type"] in {"术语定义", "标准要求", "指标限值", "试验检测方法", "引用标准"} for node in result["graph"]["nodes"])


@pytest.mark.asyncio
async def test_validation_failure_skips_llm_and_reports_parse_error() -> None:
    parse_result = _parse_result_with_resources()
    parse_result["validation"]["passed"] = False
    extractor = NationalStandardGraphExtractor.__new__(NationalStandardGraphExtractor)
    extractor.include_validation_failed = False
    extractor.result_stats = ResultStats()
    result = await extractor.extract_from_parse_result(parse_result, "测试标准")
    assert result["status"] == "skipped_validation_failed"
    assert result["graph"] == {"nodes": [], "edges": []}
    assert extractor.result_stats.error == 1


@pytest.mark.asyncio
async def test_empty_adapter_result_is_not_reported_as_llm_success() -> None:
    class EmptyAdapter:
        async def entity_and_relationship_extract(self, **_kwargs):
            return {}

    extractor = NationalStandardGraphExtractor.__new__(NationalStandardGraphExtractor)
    extractor.semaphore = __import__("asyncio").Semaphore(1)
    extractor.extractor = EmptyAdapter()
    with pytest.raises(RuntimeError, match="抽取器返回空结果"):
        await extractor._llm_extract_file_info(_parse_result_with_resources(), "测试标准")


@pytest.mark.asyncio
async def test_storage_failure_is_counted_and_raised() -> None:
    class FakeExtractor:
        def __init__(self) -> None:
            self.result_stats = ResultStats()

        async def extract(self, _input_path):
            return {
                "filename": "测试标准",
                "status": "success",
                "graph": {"nodes": [{"node_type": "标准文件"}], "edges": []},
            }

    class FailedStorage:
        disconnected = False

        def connect(self):
            return None

        def add_subgraph_with_merge(self, *_args, **_kwargs):
            return False

        def disconnect(self):
            self.disconnected = True

    extractor = FakeExtractor()
    storage = FailedStorage()
    with pytest.raises(RuntimeError, match="保存到 Neo4j 失败"):
        await extract_and_store_national_standard("sample", "test_graph", storage, extractor=extractor)
    assert extractor.result_stats.storage_errors == 1
    assert extractor.result_stats.error == 1
    assert storage.disconnected is True


@pytest.mark.asyncio
async def test_storage_success_uses_existing_graph_contract() -> None:
    class FakeExtractor:
        def __init__(self) -> None:
            self.result_stats = ResultStats()

        async def extract(self, _input_path):
            return {
                "filename": "测试标准",
                "status": "success",
                "graph": {"nodes": [{"node_type": "标准文件"}], "edges": []},
            }

    class Storage:
        call = None

        def connect(self):
            return None

        def add_subgraph_with_merge(self, *args, **kwargs):
            self.call = (args, kwargs)
            return True

        def disconnect(self):
            return None

    storage = Storage()
    result = await extract_and_store_national_standard(
        "sample", "test_graph", storage, extractor=FakeExtractor()
    )
    assert result["status"] == "success"
    assert storage.call[0][1:3] == ("test_graph", "DomainLevel")
