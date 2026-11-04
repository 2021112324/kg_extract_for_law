from __future__ import annotations

import json
import re
from collections import Counter
from copy import deepcopy
from pathlib import Path

import pytest

from app.infrastructure.kg_to_natural_language.converters import (
    chinese_law, compliance_guide, english_law, national_standard,
)
from app.infrastructure.kg_to_natural_language.storage.adapters import generate_entries


def node(identity, label, **props):
    return {"n": {"identity": identity, "elementId": f"n:{identity}",
                  "properties": {"id": f"business:{identity}", "label": label,
                                 "name": f"name:{identity}", "filename": ["source.md"], **props}}}


def edge(identity, start, end, predicate, **props):
    return {"p": {"identity": identity, "elementId": f"e:{identity}", "type": predicate,
                  "start": start, "end": end, "startNodeElementId": f"n:{start}",
                  "endNodeElementId": f"n:{end}", "properties": props}}


def sample(kind):
    if kind in {"chinese_law", "rule"}:
        nodes = [node(1, "法规文件", name="测试规定", 文件全称="测试规定", 文件性质="规范性文件"),
                 node(2, "法条", name="1.1", 条="1.1", 效力范围="适用于经营者"),
                 node(3, "条款单元", name="1.1（1）", 单元编号="1.1（1）",
                      条款单元内容="经营者应保存完整交易记录。", 上下文依据="经营者开展交易时"),
                 node(4, "引用依据", name="记录办法", 文件全称="记录办法", 引用类型="条款",
                      条款编号="2.1", 引用关系="引用", 引用目的="明确保存要求")]
        return nodes, [edge(1, 1, 2, "包含"), edge(2, 2, 3, "包含"), edge(3, 3, 4, "引用")]
    if kind == "guide":
        nodes = [node(1, "合规指引文件", name="合规目录", 文件全称="合规目录", 文件类型="目录"),
                 node(2, "指引结构节点", name="1.1", 显示编号="1.1", 节点标题="装备",
                      完整路径="一 目录 > 1.1 装备"),
                 node(3, "指引知识单元", 知识内容="智能装备列入本目录支持范围。", 知识类型="目录归属"),
                 node(4, "引用依据", name="上位目录", 文件全称="上位目录")]
        return nodes, [edge(1, 1, 2, "包含"), edge(2, 2, 3, "提出"), edge(3, 3, 4, "引用")]
    if kind == "national_standard":
        nodes = [node(1, "标准文件", name="测试标准", 标准中文名称="测试标准", 标准编号="GB/T 1-2026"),
                 node(2, "标准结构节点", name="3.1", 节点编号="3.1", 节点路径="3 安全 > 3.1"),
                 node(3, "标准要求", 要求内容="测试设备应当安装安全装置。", 要求类型="安全要求"),
                 node(4, "引用标准", name="检测标准", 标准编号="GB/T 2-2026", 标准名称="检测标准",
                      引用说明="安全装置应符合检测标准要求。", 是否内部引用="否")]
        return nodes, [edge(1, 1, 2, "包含"), edge(2, 2, 3, "规定"), edge(3, 3, 4, "引用")]
    nodes = [node(1, "LegalDocument", name="Example Act", document_name="Example Act", document_type="Act"),
             node(2, "LegalProvision", name="Article 1", provision_number="Article 1",
                  provision_content="Operators shall retain records."),
             node(3, "ProvisionTextParagraph", name="Article 1(1)", unit_number="Article 1(1)",
                  unit_content="Operators shall retain records."),
             node(22, "ProvisionClause", name="Article 1(1)", unit_number="Article 1(1)"),
             node(4, "Citation", name="Records Act", citation_text="Records Act",
                  is_internal_reference="No")]
    return nodes, [edge(1, 1, 2, "CONTAINS"), edge(2, 2, 22, "CONTAINS"),
                   edge(22, 22, 3, "CONTAINS"), edge(3, 3, 4, "CITES")]


@pytest.mark.parametrize("kind", ["chinese_law", "rule", "national_standard", "guide", "english_law"])
def test_adapter_matches_legacy_text_order_and_statistics(kind, tmp_path):
    nodes, edges = sample(kind)
    original = deepcopy((nodes, edges))
    entries = generate_entries(kind, nodes, edges, graph_label="test")
    assert (nodes, edges) == original
    assert entries
    for entry in entries:
        assert entry["source_node_ids"]
        assert entry["semantic_types"]
        assert entry["metadata"]["graph_label"] == "test"
        assert entry["source_node_keys_scope"] == "snapshot"
        json.dumps(entry, ensure_ascii=False)
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    (input_dir / "node.json").write_text(json.dumps(nodes, ensure_ascii=False), encoding="utf-8")
    (input_dir / "edge.json").write_text(json.dumps(edges, ensure_ascii=False), encoding="utf-8")
    converter = {"chinese_law": chinese_law.convert_json_to_text_v2,
                 "rule": chinese_law.convert_json_to_text_v2,
                 "national_standard": national_standard.convert_national_standard_json_to_text,
                 "guide": compliance_guide.convert_compliance_guide_json_to_text,
                 "english_law": english_law.convert_english_law_json_to_text}[kind]
    result = converter("test", input_dir=input_dir, output_dir=tmp_path / "output")
    expected = Path(result["txt_path"]).read_text(encoding="utf-8").splitlines()
    assert [re.sub(r"\s*\n\s*", " ", e["content"]).strip() for e in entries] == expected
    assert result["total_entries"] == len(entries)


@pytest.mark.parametrize("kind", ["chinese_law", "rule", "national_standard", "guide", "english_law"])
def test_relationship_provenance_distinguishes_generating_nodes_and_endpoints(kind):
    nodes, edges = sample(kind)
    entries = generate_entries(kind, nodes, edges, "test")
    reference = next(e for e in entries if e["metadata"].get("relationship"))
    assert reference["source_node_ids"] == ["n:3"]
    assert set(reference["related_node_ids"]) == {"n:3", "n:4"}
    assert reference["source_relationship_ids"] == ["e:3"]
    assert [r["business_id"] for r in reference["source_refs"] if r["role"] == "primary"] == ["business:3"]
    assert all(r["role"] == "related" for r in reference["source_refs"] if r["node_id"] == "n:4")


@pytest.mark.parametrize("kind", ["national_standard", "guide", "english_law"])
def test_duplicate_relationships_preserve_all_edges_and_target_refs(kind):
    nodes, edges = sample(kind)
    edges.append(edge(33, 3, 4, edges[-1]["p"]["type"]))
    target = deepcopy(nodes[-1])
    target["n"].update(identity=44, elementId="n:44")
    target["n"]["properties"]["id"] = "business:44"
    nodes.append(target)
    edges.append(edge(34, 3, 44, edges[-1]["p"]["type"]))
    entries = generate_entries(kind, nodes, edges, "test")
    reference = next(e for e in entries if e["metadata"].get("relationship"))
    assert set(reference["source_relationship_ids"]) == {"e:3", "e:33", "e:34"}
    assert set(reference["related_node_ids"]) == {"n:3", "n:4", "n:44"}


@pytest.mark.parametrize("kind", ["national_standard", "guide"])
def test_same_body_dedup_retains_both_generating_nodes(kind):
    nodes, edges = sample(kind)
    duplicate = deepcopy(nodes[2])
    duplicate["n"].update(identity=30, elementId="n:30")
    duplicate["n"]["properties"]["id"] = "business:30"
    duplicate["n"]["properties"]["filename"] = ["source.md", "second-source.md"]
    nodes.append(duplicate)
    edges.append(edge(30, 2, 30, "规定" if kind == "national_standard" else "提出"))
    entries = generate_entries(kind, nodes, edges, "test")
    content = next(e for e in entries if "n:30" in e["source_node_ids"])
    assert set(content["source_node_ids"]) == {"n:3", "n:30"}
    assert "second-source.md" in content["metadata"]["source_filenames"]


def test_english_cross_level_dedup_keeps_ancestor_and_child_generators():
    nodes, edges = sample("english_law")
    entries = generate_entries("english_law", nodes, edges, "test")
    provision = next(e for e in entries if e["knowledge_type"] == "legal_provision")
    assert set(provision["source_node_ids"]) == {"n:2", "n:3"}
    assert provision["source_node_id"] == "business:3"
    assert {r["source_location"] for r in provision["source_refs"]} == {"Article 1", "Article 1(1)"}


def test_chinese_collector_does_not_change_text_counts_or_structured_switch(monkeypatch):
    nodes, _ = sample("chinese_law")
    nodes[2]["n"]["properties"].update(适用主体="经营者", 行为描述="保存记录", 法律后果="承担责任")
    monkeypatch.setattr(chinese_law, "ENABLE_STRUCTURED_RULE_ASSEMBLY", True)
    plain, captured, collector = [], [], []
    plain_count, captured_count = Counter(), Counter()
    names = chinese_law.build_regulation_source_names(nodes)
    chinese_law.convert_clause_units(nodes, plain, plain_count, names)
    chinese_law.convert_clause_units(nodes, captured, captured_count, names, collector=collector)
    assert plain == captured == [e["content"] for e in collector]
    assert plain_count == captured_count
    assert [e["knowledge_type"] for e in collector] == ["条款单元内容知识", "规则知识", "规则知识"]
    assert all(e["source_location"] == "1.1（1）" for e in collector)


def test_rule_preserves_real_numeric_location_and_authorized_context():
    nodes, edges = sample("rule")
    entry = next(e for e in generate_entries("rule", nodes, edges, "test")
                 if e["knowledge_type"] == "条款单元内容知识")
    assert entry["source_location"] == "1.1（1）"
    assert "第" not in entry["content"]
    assert entry["metadata"]["rule_source_context"][0]["上下文依据"] == "经营者开展交易时"


@pytest.mark.parametrize("knowledge_type", ["目录归属", "范围界定"])
def test_guide_p2_preserves_non_behavior_knowledge(knowledge_type):
    nodes, edges = sample("guide")
    nodes[2]["n"]["properties"]["知识类型"] = knowledge_type
    entry = next(e for e in generate_entries("guide", nodes, edges, "guide_p2")
                 if e["knowledge_type"] == "指引内容知识")
    assert entry["metadata"]["knowledge_type"] == knowledge_type
    assert "禁止" not in entry["content"]
    assert entry["source_location"] == "一 目录 > 1.1 装备"


@pytest.mark.parametrize("nature", ["新闻报道", "解读材料", "正式文件", "未知"])
def test_paragraph_keeps_attribution_and_reference_usage(nature):
    nodes = [node(1, "来源文件", 文件名称="当前报道", 来源性质=nature),
             node(2, "段落节点", 段落原文="某机构指出应加强审查。", 顺序=2,
                  所属标题=["审查"], 原文范围=[10, 24]),
             node(3, "段落知识单元", 知识内容="应加强审查。", 知识类型="观点转述",
                  使用定位="规范知识", 归属说明="某机构", 目标ID="p:2", 原文依据="某机构指出应加强审查。")]
    edges = [edge(1, 1, 2, "包含"), edge(2, 2, 3, "承载")]
    entries = generate_entries("paragraph", nodes, edges, "test")
    assert len(entries) == 1
    entry = entries[0]
    assert entry["source_node_ids"] == ["n:3"]
    assert entry["knowledge_type"] == "观点转述"
    assert entry["metadata"]["使用定位"] == "参考知识"
    assert entry["metadata"]["归属说明"] == "某机构"
    assert "某机构" in entry["content"] and "当前报道" in entry["content"]
    assert entry["metadata"]["paragraph_locations"][0]["原文范围"] == [10, 24]
    assert entry["source_location"] == "p:2"


def test_paragraph_only_generates_extracted_knowledge_not_properties_or_original_paragraph():
    nodes = [node(1, "来源文件", 文件名称="说明", 来源性质="正式文件", 时效性="现行有效"),
             node(2, "段落节点", 段落原文="长段原文", 顺序=1),
             node(3, "段落知识单元", 适用主体="企业", 经济行业=["制造业"])]
    assert generate_entries("paragraph", nodes, [], "test") == []
    nodes[2]["n"]["properties"].update(知识内容="经营者应保存记录。", 知识类型="规范要求", 使用定位="规范知识")
    entries = generate_entries("paragraph", nodes, [edge(1, 1, 2, "包含"), edge(2, 2, 3, "承载")], "test")
    assert entries[0]["metadata"]["使用定位"] == "规范知识"
    assert entries[0]["source_location"] == "段落1"


def test_physical_id_zero_and_missing_business_id_are_auditable():
    nodes = [node(0, "条款单元", 单元编号="1.1", 条款单元内容="经营者应当保存原始记录。")]
    del nodes[0]["n"]["properties"]["id"]
    del nodes[0]["n"]["elementId"]
    entry = generate_entries("rule", nodes, [], "test")[0]
    assert entry["source_node_ids"] == ["0"]
    assert entry["source_refs"][0]["identity"] == 0
    assert entry["source_refs"][0]["business_id"] is None


@pytest.mark.parametrize("kind", ["chinese_law", "national_standard", "guide", "english_law"])
def test_relationship_without_id_uses_explicit_snapshot_position(kind):
    nodes, edges = sample(kind)
    for record in edges:
        del record["p"]["identity"]
        del record["p"]["elementId"]
    entries = generate_entries(kind, nodes, edges, "test")
    reference = next(e for e in entries if e["metadata"].get("relationship"))
    assert reference["source_relationship_ids"] == [f"relationship:{len(edges) - 1}"]


def test_duplicate_business_ids_keep_distinct_physical_generators():
    nodes, edges = sample("guide")
    duplicate = deepcopy(nodes[2])
    duplicate["n"].update(identity=30, elementId="n:30")
    nodes.append(duplicate)
    edges.append(edge(30, 2, 30, "提出"))
    entries = generate_entries("guide", nodes, edges, "test")
    content = next(e for e in entries if e["knowledge_type"] == "指引内容知识")
    assert set(content["source_node_ids"]) == {"n:3", "n:30"}
    assert [r["business_id"] for r in content["source_refs"]] == ["business:3", "business:3"]


@pytest.mark.parametrize("kind", ["chinese_law", "rule", "national_standard", "guide", "english_law", "paragraph"])
def test_empty_graph(kind):
    assert generate_entries(kind, [], [], "test") == []


def test_unsupported_type_and_structure_are_rejected():
    with pytest.raises(ValueError, match="Unsupported converter_type"):
        generate_entries("auto", [], [], "test")
    with pytest.raises(ValueError, match="Unsupported paragraph"):
        generate_entries("paragraph", [node(1, "未知类型", 任意属性="不应成为知识")], [], "test")
    with pytest.raises(ValueError, match="n.properties"):
        generate_entries("paragraph", [{"content": "pure text"}], [], "test")


@pytest.mark.parametrize("kind,label", [("chinese_law", "法规文件"), ("rule", "法条"),
                                      ("national_standard", "标准文件"), ("guide", "合规指引文件"),
                                      ("english_law", "LegalDocument"), ("paragraph", "来源文件")])
def test_mixed_known_and_unknown_labels_are_rejected(kind, label):
    with pytest.raises(ValueError, match="Unsupported.*未知语义节点"):
        generate_entries(kind, [node(1, label), node(2, "未知语义节点")], [], "test")


def test_national_standard_resource_duplicate_merges_sources_after_priority_replacement():
    def entry(type_, ref, resource=""):
        return national_standard.KnowledgeEntry(
            knowledge_type=type_, content="相同的安全要求。", source_file="source.md",
            standard_name="测试标准", source_location="3.1", resource_key=resource,
            source_refs=[{"node_id": ref, "role": "primary"}], dedup_text="安全要求")
    first = entry("表格索引知识", "table:1", "same-table")
    priority = entry("标准要求知识", "requirement:1")
    duplicate = entry("表格索引知识", "table:2", "same-table")
    result = national_standard._deduplicate_entries(
        [first, priority, duplicate], national_standard.ConversionStats())
    assert len(result) == 1
    assert result[0].knowledge_type == "标准要求知识"
    assert {r["node_id"] for r in result[0].source_refs} == {"table:1", "table:2", "requirement:1"}


@pytest.mark.parametrize("kind,label", [("national_standard", "图片"), ("national_standard", "标准依据"),
                                      ("guide", "量化目标"), ("guide", "责任主体"),
                                      ("english_law", "LegalBasis"), ("english_law", "Citation"),
                                      ("paragraph", "引用依据")])
def test_known_auxiliary_labels_are_accepted(kind, label):
    assert isinstance(generate_entries(kind, [node(1, label)], [], "test"), list)
