from __future__ import annotations

import csv
import inspect
import json
from pathlib import Path
from typing import Any

from app.infrastructure.kg_to_natural_language import (
    convert_json_to_text,
    convert_json_to_text_v2,
    convert_national_standard_json_to_text,
)


def _node(identity: int, label: str, filename: str, **properties: Any) -> dict[str, Any]:
    node_properties = {
        "id": properties.pop("id", f"node_{identity}"),
        "name": properties.pop("name", f"{label}_{identity}"),
        "label": label,
        "filename": [filename],
        **properties,
    }
    return {
        "n": {
            "identity": identity,
            "labels": ["国家标准测试图谱"],
            "properties": node_properties,
            "elementId": f"4:test:{identity}",
        }
    }


def _edge(
    identity: int,
    start: int,
    end: int,
    predicate: str,
    **properties: Any,
) -> dict[str, Any]:
    return {
        "p": {
            "identity": identity,
            "start": start,
            "end": end,
            "type": predicate,
            "properties": properties,
            "elementId": f"5:test:{identity}",
            "startNodeElementId": f"4:test:{start}",
            "endNodeElementId": f"4:test:{end}",
        }
    }


def _write_graph(input_dir: Path, nodes: list[dict[str, Any]], edges: list[dict[str, Any]]) -> None:
    input_dir.mkdir(parents=True, exist_ok=True)
    (input_dir / "node.json").write_text(
        json.dumps(nodes, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (input_dir / "edge.json").write_text(
        json.dumps(edges, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _read_jsonl(path: str) -> list[dict[str, Any]]:
    return [json.loads(line) for line in Path(path).read_text(encoding="utf-8").splitlines() if line]


def _convert(
    tmp_path: Path,
    nodes: list[dict[str, Any]],
    edges: list[dict[str, Any]],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    _write_graph(input_dir, nodes, edges)
    result = convert_national_standard_json_to_text(
        "国家标准测试",
        input_dir=input_dir,
        output_dir=output_dir,
    )
    return result, _read_jsonl(result["jsonl_path"])


def test_converts_core_knowledge_and_references(tmp_path: Path) -> None:
    filename = "测试产品安全标准"
    nodes = [
        _node(
            1,
            "标准文件",
            filename,
            name=filename,
            标准中文名称=filename,
            标准英文名称="Test product safety standard",
            标准编号="GB/T 1000—2026",
            标准性质="推荐性国家标准",
            发布单位="国家标准化管理部门",
            发布日期="2026-01-01",
            实施日期="2026-07-01",
            标准状态="现行",
            ICS="01.100",
            CCS="A00",
            合规风险类型=["产品法律风险"],
            适用范围摘要="适用于测试产品。",
        ),
        _node(
            2,
            "标准结构节点",
            filename,
            name="1 范围",
            节点编号="1",
            节点标题="范围",
            节点路径="1 范围",
            节点类型="范围节点",
            节点内容="本文件规定了测试产品的安全要求，适用于测试产品的设计和检验。",
        ),
        _node(
            3,
            "标准结构节点",
            filename,
            name="3.1 安全装置",
            节点编号="3.1",
            节点标题="安全装置",
            节点路径="3 要求 > 3.1 安全装置",
            节点类型="要求节点",
            节点内容="测试产品应安装安全装置。",
        ),
        _node(
            4,
            "术语定义",
            filename,
            术语编号="2.1",
            中文术语="测试产品",
            英文术语="test product",
            定义内容="用于验证标准转换行为的产品。",
        ),
        _node(
            5,
            "标准要求",
            filename,
            要求内容="测试产品应安装安全装置。",
            要求类型="安全要求",
            约束强度="强制",
            适用对象="测试产品",
            行为描述="安装安全装置",
            合规风险类型="产品法律风险",
        ),
        _node(
            6,
            "指标限值",
            filename,
            指标名称="安全距离",
            限值="不小于5 mm",
            单位="mm",
            适用对象="测试产品",
            来源文本="测试产品的安全距离应不小于5 mm。",
        ),
        _node(
            7,
            "试验检测方法",
            filename,
            方法名称="安全距离测定方法",
            方法内容="使用校准后的量具测定测试产品的安全距离。",
            适用对象="测试产品",
            操作步骤="使用量具测定",
        ),
        _node(
            8,
            "标准依据",
            filename,
            标准编号="GB/T 1.1—2020",
            标准名称="标准化工作导则",
            依据类型="引用",
        ),
        _node(
            9,
            "引用标准",
            filename,
            标准编号="GB/T 2000—2025",
            标准名称="外部检测标准",
            引用说明="安全装置的检验应符合GB/T 2000—2025。",
            是否内部引用="否",
            是否本标准自身引用="否",
        ),
        _node(
            10,
            "标准结构节点",
            filename,
            name="4.2",
            节点编号="4.2",
            节点标题="内部判定要求",
            节点路径="4 检验 > 4.2 内部判定要求",
            节点类型="正文节点",
            节点内容="判定时还应符合本文件3.1的规定。",
        ),
    ]
    edges = [
        _edge(101, 1, 2, "包含"),
        _edge(102, 1, 3, "包含"),
        _edge(103, 3, 4, "定义"),
        _edge(104, 3, 5, "规定"),
        _edge(105, 3, 6, "规定"),
        _edge(106, 3, 7, "规定"),
        _edge(107, 1, 8, "依据"),
        _edge(108, 5, 9, "引用"),
        _edge(109, 1, 10, "包含"),
        _edge(110, 5, 10, "引用", 引用说明="安全装置还应符合本文件4.2。"),
    ]

    result, entries = _convert(tmp_path, nodes, edges)
    contents = [entry["content"] for entry in entries]
    types = {entry["knowledge_type"] for entry in entries}

    assert result["source_files"] == 1
    assert {
        "标准文件档案知识",
        "结构节点原文知识",
        "术语定义知识",
        "标准要求知识",
        "指标限值知识",
        "试验检测方法知识",
        "引用知识",
    } <= types
    assert any("测试产品应安装安全装置" in content for content in contents)
    assert any("安全距离应不小于5 mm" in content for content in contents)
    assert any("使用校准后的量具测定" in content for content in contents)
    assert any("安全装置的检验应符合GB/T 2000—2025" in content for content in contents)
    assert not any("包含标准结构节点" in content for content in contents)
    assert sum("测试产品应安装安全装置" in content for content in contents) == 1
    assert all(entry["source_file"] == filename for entry in entries)
    assert all(entry["knowledge_id"].startswith("ns_") for entry in entries)


def test_resource_knowledge_uses_descriptions_only(tmp_path: Path) -> None:
    filename = "资源测试标准"
    mermaid = "flowchart TD\nA[开始] --> B[结束]"
    table_html = "<table><tr><td>不得进入知识正文</td></tr></table>"
    image_url = "https://example.test/image.png"
    nodes = [
        _node(1, "标准文件", filename, name=filename, 标准中文名称=filename, 标准编号="GB 2—2026"),
        _node(
            2,
            "标准结构节点",
            filename,
            节点编号="5",
            节点标题="资源说明",
            节点路径="5 资源说明",
            节点类型="正文节点",
            节点内容=(
                "本章给出了配套资源的用途说明。\n"
                "<details><summary>flowchart</summary>\n"
                f"```mermaid\n{mermaid}\n```\n</details>"
            ),
        ),
        _node(
            3,
            "表格",
            filename,
            表号="表1",
            表题="性能指标",
            table_id="table-1",
            表格描述="该表规定了产品性能指标的分类和用途。",
            表格内容=table_html,
            表格HTML=table_html,
        ),
        _node(
            4,
            "表格",
            filename,
            表号="表2",
            表题="仅有标题",
            table_id="table-2",
            表格内容="不应输出",
        ),
        _node(
            5,
            "流程图",
            filename,
            流程图编号="图1",
            流程图标题="处理流程",
            流程图描述="该流程图说明了从开始到结束的处理顺序。",
            流程图内容=mermaid,
            流程图语法类型="flowchart TD",
            流程节点摘要="开始、结束",
            流程关系摘要="开始指向结束",
        ),
        _node(
            6,
            "流程图",
            filename,
            流程图编号="图2",
            流程图标题="无描述流程",
            流程图内容=mermaid,
        ),
        _node(7, "图片", filename, 图片链接=image_url, 图片类型="示意图"),
    ]
    edges = [
        _edge(101, 1, 2, "包含"),
        _edge(102, 2, 3, "包含"),
        _edge(103, 2, 4, "包含"),
        _edge(104, 2, 5, "包含"),
        _edge(105, 2, 6, "包含"),
        _edge(106, 2, 7, "包含"),
    ]

    result, entries = _convert(tmp_path, nodes, edges)
    contents = [entry["content"] for entry in entries]

    assert sum(entry["knowledge_type"] == "表格索引知识" for entry in entries) == 1
    assert sum(entry["knowledge_type"] == "流程图描述知识" for entry in entries) == 1
    assert any("该表规定了产品性能指标的分类和用途" in content for content in contents)
    assert any("该流程图说明了从开始到结束的处理顺序" in content for content in contents)
    forbidden = [table_html, "不得进入知识正文", mermaid, "开始、结束", "开始指向结束"]
    assert all(token not in "\n".join(contents) for token in forbidden)
    assert all(entry["source_node_type"] != "图片" for entry in entries)
    structure_entry = next(entry for entry in entries if entry["knowledge_type"] == "结构节点原文知识")
    assert structure_entry["attachments"] == [image_url]
    assert result["skip_reason_counts"]["表格索引知识:表格描述为空"] == 1
    assert result["skip_reason_counts"]["流程图描述知识:流程图描述为空"] == 1
    assert len(Path(result["txt_path"]).read_text(encoding="utf-8").splitlines()) == result["total_entries"]


def test_filters_invalid_candidates_deduplicates_and_isolates_files(tmp_path: Path) -> None:
    file_a = "标准甲"
    file_b = "标准乙"
    shared_text = "产品应按规定进行安全检测。"
    nodes = [
        _node(1, "标准文件", file_a, name=file_a, 标准中文名称=file_a, 标准编号="GB 10—2026"),
        _node(
            2,
            "标准结构节点",
            file_a,
            name="3.1",
            节点编号="3.1",
            节点标题="安全检测",
            节点路径="3.1 安全检测",
            节点类型="要求节点",
            节点内容=shared_text,
        ),
        _node(3, "标准要求", file_a, name="相同名称", 要求内容=shared_text, 要求类型="检测要求"),
        _node(
            4,
            "试验检测方法",
            file_a,
            name="相同名称",
            方法名称="安全检测方法",
            方法内容=shared_text,
            适用对象="产品",
        ),
        _node(
            5,
            "指标限值",
            file_a,
            指标名称="在线监测项目",
            来源文本="主要水质指标应安装在线监测装置。",
        ),
        _node(
            6,
            "试验检测方法",
            file_a,
            方法名称="在线监测装置安装",
            方法内容="主要水质指标应安装在线监测装置。",
            适用对象="在线监测装置",
        ),
        _node(7, "标准文件", file_b, name=file_b, 标准中文名称=file_b, 标准编号="GB 11—2026"),
        _node(
            8,
            "标准结构节点",
            file_b,
            name="3.1",
            节点编号="3.1",
            节点标题="安全检测",
            节点路径="3.1 安全检测",
            节点类型="要求节点",
            节点内容=shared_text,
        ),
        _node(9, "标准要求", file_b, name="相同名称", 要求内容=shared_text, 要求类型="检测要求"),
        _node(
            10,
            "指标限值",
            file_a,
            指标名称="触摸电压",
            限值=r"不超过 $\frac{50}{U_0} \times Z_{\mathrm{s}}$",
            适用对象="保护联结电路",
            来源文本=r"保护联结电路阻抗不超过 $\frac{50}{U_0} \times Z_{\mathrm{s}}$。",
        ),
    ]
    edges = [
        _edge(101, 1, 2, "包含"),
        _edge(102, 2, 3, "规定"),
        _edge(103, 2, 4, "规定"),
        _edge(104, 2, 5, "规定"),
        _edge(105, 2, 6, "规定"),
        _edge(106, 7, 8, "包含"),
        _edge(107, 8, 9, "规定"),
        _edge(108, 3, 3, "引用"),
        _edge(110, 2, 10, "规定"),
        {
            "p": {
                "identity": 109,
                "start": 3,
                "end": 999,
                "type": "引用",
                "properties": {},
            }
        },
    ]

    result, entries = _convert(tmp_path, nodes, edges)
    shared_entries = [entry for entry in entries if shared_text.rstrip("。") in entry["content"]]

    assert len(shared_entries) == 2
    assert {entry["source_file"] for entry in shared_entries} == {file_a, file_b}
    entry_a = next(entry for entry in shared_entries if entry["source_file"] == file_a)
    assert set(entry_a["semantic_types"]) == {"标准要求知识", "试验检测方法知识"}
    assert sum(entry["knowledge_type"] == "指标限值知识" for entry in entries) == 1
    assert any("保护联结电路阻抗不超过" in entry["content"] for entry in entries)
    assert result["skip_reason_counts"]["指标限值知识:缺少有效数值边界"] == 1
    assert result["skip_reason_counts"]["试验检测方法知识:不满足检测方法语义"] == 1
    assert result["relationship_issue_counts"]["自循环关系"] == 1
    assert result["relationship_issue_counts"]["关系端点无法解析"] == 1
    assert result["skip_reason_counts"]["结构节点原文知识:已有高优先级语义实体"] == 2

    txt_lines = Path(result["txt_path"]).read_text(encoding="utf-8").splitlines()
    jsonl_lines = Path(result["jsonl_path"]).read_text(encoding="utf-8").splitlines()
    assert len(txt_lines) == len(jsonl_lines) == result["total_entries"]
    with Path(result["csv_path"]).open(encoding="utf-8-sig", newline="") as file:
        csv_rows = list(csv.reader(file))
    assert ["总计", "国家标准测试", str(result["total_entries"])] in csv_rows


def test_public_entry_does_not_change_regulation_converter_signatures() -> None:
    assert list(inspect.signature(convert_json_to_text).parameters) == ["graph_type"]
    assert list(inspect.signature(convert_json_to_text_v2).parameters) == ["graph_type"]
    assert list(inspect.signature(convert_national_standard_json_to_text).parameters) == [
        "graph_type",
        "input_dir",
        "output_dir",
    ]
