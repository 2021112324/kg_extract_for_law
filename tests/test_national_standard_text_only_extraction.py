import inspect
import os
from types import SimpleNamespace

os.environ.setdefault("MYSQL_SKIP_INIT", "true")
os.environ.setdefault("MINIO_SKIP_INIT", "true")

import pytest
from fastapi import BackgroundTasks

from app.api.v1.endpoints.kg import national_standard_extract_by_dir_standalone
from app.infrastructure.information_extraction.national_standard.graph_extract import (
    CORE_ENTITY_TYPES,
    CORE_RELATION_TRIPLES,
    FORBIDDEN_RESOURCE_PROPERTIES,
    ResultStats,
    _GraphBuilder,
    _schema_without_resource_entities,
)
from app.infrastructure.information_extraction.national_standard.national_standard_extract import (
    NationalStandardExtractor,
    public_national_standard_parse_result,
)
from app.infrastructure.information_extraction.national_standard.prompt.schema import (
    schema_for_standard_tree_node,
)
from app.services.core import kg_service as kg_service_module
from app.services.core.kg_service import KGService


SAMPLE_MARKDOWN = """# 示例标准

# 1 范围
资源前正文应保留，温度为 $17^{\\circ}C$，浓度不大于 5 mg/m³。

![图1 试验装置](images/figure.png)

| 指标 | 限值 |
| --- | --- |
| 温度 | 17 ℃ |

资源后正文也应保留。

```mermaid
flowchart LR
A[开始] --> B[结束]
```

# 2 规范性引用文件
GB/T 1.1 标准化工作导则
"""


def _write_standard(tmp_path, name="示例标准"):
    standard_dir = tmp_path / name
    standard_dir.mkdir()
    (standard_dir / "full.md").write_text(SAMPLE_MARKDOWN, encoding="utf-8")
    return standard_dir


def _walk_nodes(nodes):
    for node in nodes:
        yield node
        yield from _walk_nodes(node.get("children", []))


def test_first_stage_strict_mode_drops_resources_and_preserves_text(tmp_path):
    standard_dir = _write_standard(tmp_path)
    result = NationalStandardExtractor().extract(standard_dir)

    assert "tables" not in result
    assert "images" not in result
    assert result["_resource_contexts"] == []
    assert all(block["type"] not in {"image", "table", "flowchart"} for block in result["blocks"])

    content = "\n".join(str(node.get("content", "")) for node in _walk_nodes(result["body_tree"]))
    assert "资源前正文应保留" in content
    assert "资源后正文也应保留" in content
    assert "17^{\\circ}C" in content
    assert "5 mg/m³" in content
    assert "| 指标 | 限值 |" not in content
    assert "flowchart LR" not in content
    assert "images/figure.png" not in content

    stats = result["validation"]["resource_stats"]
    assert stats["skipped_image_blocks"] == 1
    assert stats["skipped_table_blocks"] == 1
    assert stats["skipped_flowchart_blocks"] == 1


def test_description_mode_keeps_only_ephemeral_context(tmp_path):
    result = NationalStandardExtractor(include_resource_descriptions=True).extract(_write_standard(tmp_path))

    contexts = result["_resource_contexts"]
    assert {item["resource_type"] for item in contexts} == {"table", "flowchart"}
    assert any("| 指标 | 限值 |" in item["resource_text"] for item in contexts)
    assert any("flowchart LR" in item["resource_text"] for item in contexts)

    public_result = public_national_standard_parse_result(result)
    assert "_resource_contexts" not in public_result
    serialized = str(public_result)
    assert "resource_context_ids" not in serialized
    assert "flowchart LR" not in serialized
    assert "| 指标 | 限值 |" not in serialized


def test_html_resources_are_skipped_without_losing_surrounding_text(tmp_path):
    standard_dir = tmp_path / "HTML标准"
    standard_dir.mkdir()
    (standard_dir / "full.md").write_text(
        "# 1 范围\n前文。\n<table><tr><td>限值</td></tr></table>\n"
        '<img src="images/a.png"/>图2 装置说明\n后文。',
        encoding="utf-8",
    )

    result = NationalStandardExtractor().extract(standard_dir)
    content = "\n".join(str(node.get("content", "")) for node in _walk_nodes(result["body_tree"]))
    stats = result["validation"]["resource_stats"]

    assert "前文" in content
    assert "图2 装置说明" in content
    assert "后文" in content
    assert "<table>" not in content
    assert "<img" not in content
    assert stats["skipped_table_blocks"] == 1
    assert stats["skipped_image_blocks"] == 1


def test_strict_schema_and_contract_exclude_resource_entities():
    strict_schema = _schema_without_resource_entities(schema_for_standard_tree_node)

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
    assert "## 表格" not in strict_schema
    assert "## 流程图" not in strict_schema
    assert ("标准结构节点", "规定", "标准要求") in CORE_RELATION_TRIPLES
    assert {"table_id", "表格内容", "流程图内容", "图片链接"} <= FORBIDDEN_RESOURCE_PROPERTIES


def _old_parse_result(with_description_contexts=False):
    result = {
        "file_info": {"标准中文名称": "示例标准", "标准编号": "GB/T 1—2026"},
        "body_tree": [
            {
                "number": "1",
                "title": "范围",
                "level": 1,
                "path": "1",
                "full_path_title": "1 范围",
                "content": "本标准规定了文本要求。",
                "children": [],
                "tables": ["old_table"],
                "images": ["old_image"],
                "resource_context_ids": [],
            }
        ],
        "appendix_tree": [],
        "tables": [{"table_id": "old_table", "table_markdown": "|A|B|"}],
        "images": [{"image_id": "old_image", "minio_url": "http://example/image.png"}],
    }
    if with_description_contexts:
        result["body_tree"][0]["resource_context_ids"] = ["table_context", "flow_context"]
        result["_resource_contexts"] = [
            {"context_id": "table_context", "resource_type": "table", "resource_text": "|A|B|"},
            {"context_id": "flow_context", "resource_type": "flowchart", "resource_text": "flowchart LR"},
        ]
    return result


def test_old_parse_result_resources_cannot_enter_strict_graph():
    stats = ResultStats()
    builder = _GraphBuilder("示例标准", stats, include_resource_descriptions=False)
    builder.add_context_graph(_old_parse_result())
    builder.add_llm_extractions(
        [
            {
                "scope": "tree_node",
                "node_number": "1",
                "node_title": "范围",
                "entities": [
                    {"name": "图片1", "type": "图片", "properties": {"图片链接": "http://example/image.png"}},
                    {
                        "name": "表1",
                        "type": "表格",
                        "properties": {"表格描述": "表格描述", "表格内容": "|A|B|", "table_id": "old_table"},
                    },
                    {
                        "name": "流程1",
                        "type": "流程图",
                        "properties": {"流程图描述": "流程描述", "流程图内容": "flowchart LR"},
                    },
                ],
                "relations": [],
            }
        ]
    )
    graph = builder.to_graph()

    assert {node["node_type"] for node in graph["nodes"]} == {"标准文件", "标准结构节点"}
    assert all(not (set(node["properties"]) & FORBIDDEN_RESOURCE_PROPERTIES) for node in graph["nodes"])
    assert stats.filtered_nodes == 3


def test_description_mode_keeps_descriptions_but_filters_payloads():
    stats = ResultStats()
    builder = _GraphBuilder("示例标准", stats, include_resource_descriptions=True)
    builder.add_context_graph(_old_parse_result(with_description_contexts=True))
    builder.add_llm_extractions(
        [
            {
                "scope": "tree_node",
                "node_number": "1",
                "node_title": "范围",
                "entities": [
                    {
                        "name": "表1",
                        "type": "表格",
                        "properties": {
                            "表号": "表1",
                            "表题": "指标表",
                            "表格描述": "该表概述主要指标。",
                            "表格内容": "|A|B|",
                            "table_id": "old_table",
                        },
                    },
                    {
                        "name": "流程1",
                        "type": "流程图",
                        "properties": {
                            "流程图标题": "处理流程",
                            "流程图描述": "该流程说明处理顺序。",
                            "流程图内容": "flowchart LR",
                        },
                    },
                ],
                "relations": [
                    {
                        "source": "1 范围",
                        "target": "表1",
                        "type": "包含",
                        "properties": {"主体": "标准结构节点_1 范围", "谓词": "包含", "客体": "表格_表1"},
                    },
                    {
                        "source": "1 范围",
                        "target": "流程1",
                        "type": "包含",
                        "properties": {"主体": "标准结构节点_1 范围", "谓词": "包含", "客体": "流程图_流程1"},
                    },
                ],
            }
        ]
    )
    graph = builder.to_graph()
    by_type = {node["node_type"]: node for node in graph["nodes"]}

    assert set(by_type["表格"]["properties"]) == {"表号", "表题", "表格描述"}
    assert set(by_type["流程图"]["properties"]) == {"流程图标题", "流程图描述"}
    assert len([edge for edge in graph["edges"] if edge["relation_type"] == "包含"]) == 3
    assert stats.description_nodes == 2


def test_description_without_current_resource_context_is_rejected():
    stats = ResultStats()
    builder = _GraphBuilder("示例标准", stats, include_resource_descriptions=True)
    builder.add_context_graph(_old_parse_result())
    builder.add_llm_extractions(
        [
            {
                "scope": "tree_node",
                "node_number": "1",
                "node_title": "范围",
                "entities": [
                    {"name": "表1", "type": "表格", "properties": {"表格描述": "无资源依据的描述"}},
                ],
                "relations": [],
            }
        ]
    )

    graph = builder.to_graph()
    assert all(node["node_type"] != "表格" for node in graph["nodes"])
    assert stats.description_nodes == 0


class FakeResultStats:
    error = 0
    week_warning = 0
    strong_warning = 0
    skipped_resource_blocks = 3
    filtered_nodes = 1
    filtered_properties = 2
    filtered_relations = 0
    description_nodes = 0
    error_msg = ""
    week_warning_msg = ""
    strong_warning_msg = ""


class FakeNationalStandardExtractor:
    def __init__(self, *args, **kwargs):
        self.result_stats = FakeResultStats()
        self.log_calls = 0

    async def extract(self, standard_dir):
        if standard_dir.name == "失败标准":
            raise RuntimeError("model request failed")
        return {
            "status": "success",
            "filename": standard_dir.name,
            "graph": {
                "nodes": [{"node_id": "file_1", "node_name": standard_dir.name, "node_type": "标准文件", "properties": {}}],
                "edges": [],
            },
        }

    async def logging_result_stats(self):
        self.log_calls += 1


class FakeGraphStorage:
    def __init__(self, save_result=True):
        self.save_result = save_result
        self.saved = []
        self.merged = []
        self.deleted = []

    def connect(self):
        return True

    def disconnect(self):
        return None

    def add_subgraph_with_merge(self, graph, graph_name, graph_level, **kwargs):
        self.saved.append((graph_name, graph, graph_level, kwargs))
        return self.save_result

    def merge_graphs(self, source, target):
        self.merged.append((source, target))
        return SimpleNamespace(error=None)

    def delete_subgraph(self, graph_name):
        self.deleted.append(graph_name)
        return True


@pytest.mark.asyncio
async def test_standalone_endpoint_has_no_database_dependency(tmp_path):
    signature = inspect.signature(national_standard_extract_by_dir_standalone)
    assert "db" not in signature.parameters

    tasks = BackgroundTasks()
    response = await national_standard_extract_by_dir_standalone(
        background_tasks=tasks,
        data_dir=str(tmp_path),
        if_del_task=True,
    )

    assert response["code"] == 200
    assert response["data"]["use_mysql"] is False
    assert response["data"]["use_minio"] is False
    assert response["data"]["include_resource_descriptions"] is False
    params = tasks.tasks[0].args[1]
    assert params["db"] is None
    assert params["use_mysql"] is False


@pytest.mark.asyncio
async def test_standalone_service_merges_success_and_reports_failure(tmp_path, monkeypatch):
    _write_standard(tmp_path, "成功标准")
    _write_standard(tmp_path, "失败标准")
    monkeypatch.setattr(kg_service_module, "NationalStandardGraphExtractor", FakeNationalStandardExtractor)

    service = KGService.__new__(KGService)
    service.graph_storage = FakeGraphStorage()
    summary = await service.national_standard_extract_by_local_dir(
        standard_data_dir=str(tmp_path),
        if_del_task=True,
        db=None,
        use_mysql=False,
        kg_graph_name="target_standard_graph",
    )

    assert summary["use_mysql"] is False
    assert summary["use_minio"] is False
    assert summary["total"] == 2
    assert summary["success"] == 1
    assert summary["failed"] == 1
    assert summary["errors"][0]["file"] == "失败标准"
    assert service.graph_storage.merged == [(service.graph_storage.saved[0][0], "target_standard_graph")]
    assert service.graph_storage.deleted == [service.graph_storage.saved[0][0]]


@pytest.mark.asyncio
async def test_standalone_service_rejects_database_session(tmp_path):
    _write_standard(tmp_path)
    service = KGService.__new__(KGService)

    with pytest.raises(ValueError, match="不得传入数据库会话"):
        await service.national_standard_extract_by_local_dir(
            standard_data_dir=str(tmp_path),
            if_del_task=False,
            db=object(),
            use_mysql=False,
        )


@pytest.mark.asyncio
async def test_standalone_service_reports_neo4j_write_failure(tmp_path, monkeypatch):
    _write_standard(tmp_path)
    monkeypatch.setattr(kg_service_module, "NationalStandardGraphExtractor", FakeNationalStandardExtractor)
    service = KGService.__new__(KGService)
    service.graph_storage = FakeGraphStorage(save_result=False)

    with pytest.raises(RuntimeError, match="没有成功入库"):
        await service.national_standard_extract_by_local_dir(
            standard_data_dir=str(tmp_path),
            if_del_task=False,
            db=None,
            use_mysql=False,
            kg_graph_name="target_standard_graph",
        )
