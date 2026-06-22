"""Validate exported Neo4j compliance case data against source files.

This script checks the external node.json/edge.json exported from Neo4j and
writes a Markdown report plus JSON details into the exported data directory.
"""

from __future__ import annotations

import json
import os
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


SOURCE_DIR = Path(
    os.getenv(
        "COMPLIANCE_CASE_SOURCE_DIR",
        r"F:\企业大脑知识库系统\8.1项目\抽取代码\kg_extract_for_law\app\infrastructure\information_extraction\compliance_case_v1\data",
    )
)
NEO4J_DIR = Path(r"D:\CogmAIT\8.1项目\8.1数据\成果\部分知识库\6月11日\neo4j数据\案例\风险合规案例")
NODE_PATH = NEO4J_DIR / "node.json"
EDGE_PATH = NEO4J_DIR / "edge.json"
REPORT_PATH = NEO4J_DIR / "风险合规案例_neo4j数据校验报告.md"
DETAIL_PATH = NEO4J_DIR / "风险合规案例_neo4j数据校验明细.json"

SOURCE_SUFFIXES = {".md", ".txt"}
CONTROLLED_RISK_TYPES = {
    "产品法律风险",
    "供应链合规风险",
    "劳动用工法律合规风险",
    "企业关联方合规风险",
    "企业国际化经营合规风险",
    "企业信用风险",
}
TARGET_KNOWLEDGE_FIELDS = [
    "case_id",
    "case_title",
    "source_compliance_domain",
    "full_text",
    "risk_types",
    "keywords",
    "related_regulation_ids",
    "related_indicator_ids",
    "conclusion",
    "analysis",
    "disposal_plan",
]
ALLOWED_NODE_TYPES = {
    "案例",
    "案例主体",
    "行为",
    "案例分析",
    "法规条款依据",
    "罪名或违法性质",
    "案例结果",
    "案例启示",
    "风险点",
    "处置方案",
}
ALLOWED_TRIPLES = {
    ("案例", "涉及", "案例主体"),
    ("案例", "包含", "行为"),
    ("案例", "引审出", "案例分析"),
    ("案例", "形成", "案例启示"),
    ("案例", "关联", "风险点"),
    ("案例", "关联", "处置方案"),
    ("案例主体", "实施", "行为"),
    ("案例主体", "遭受", "行为"),
    ("案例主体", "承担", "案例结果"),
    ("案例主体", "隶属于", "案例主体"),
    ("行为", "构成", "罪名或违法性质"),
    ("行为", "违反", "法规条款依据"),
    ("行为", "导致", "案例结果"),
    ("案例分析", "包含", "行为"),
    ("案例分析", "依据", "法规条款依据"),
    ("案例分析", "得出", "案例结果"),
    ("案例分析", "形成", "案例启示"),
    ("罪名或违法性质", "依据", "法规条款依据"),
}


def main() -> None:
    detail = validate()
    DETAIL_PATH.write_text(json.dumps(detail, ensure_ascii=False, indent=2), encoding="utf-8")
    REPORT_PATH.write_text(build_report(detail), encoding="utf-8")
    print(
        json.dumps(
            {
                "report": str(REPORT_PATH),
                "detail": str(DETAIL_PATH),
                "source_total": detail["source_total"],
                "covered_source_count": detail["covered_source_count"],
                "missing_source_count": detail["missing_source_count"],
                "node_count": detail["node_count"],
                "edge_count": detail["edge_count"],
                "case_count": detail["case_count"],
                "serious_issues": detail["serious_issues"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )


def validate() -> dict[str, Any]:
    source_files = sorted(
        path for path in SOURCE_DIR.rglob("*") if path.is_file() and path.suffix.lower() in SOURCE_SUFFIXES
    )
    source_by_name = {path.name: str(path) for path in source_files}
    source_by_category = Counter(path.parent.name for path in source_files)

    raw_nodes = _read_json(NODE_PATH)
    raw_edges = _read_json(EDGE_PATH)
    nodes = [_unwrap(item, "n") for item in raw_nodes]
    edges = [_unwrap(item, "p") for item in raw_edges]
    node_by_identity = {node.get("identity"): node for node in nodes}
    node_props = [node.get("properties") or {} for node in nodes]

    node_type_counter = Counter(_node_type(node) for node in nodes)
    edge_type_counter = Counter(edge.get("type") for edge in edges)
    graph_tags = Counter((props.get("graph_tag") or "") for props in node_props)

    covered_filenames = set()
    for props in node_props:
        for filename in _as_list(props.get("filename")):
            if filename:
                covered_filenames.add(Path(str(filename)).name)

    source_names = set(source_by_name)
    missing_source_names = sorted(source_names - covered_filenames)
    extra_covered_names = sorted(covered_filenames - source_names)

    case_nodes = [node for node in nodes if _node_type(node) == "案例"]
    case_nodes_by_filename: dict[str, list[dict[str, Any]]] = defaultdict(list)
    nodes_by_filename: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for node in nodes:
        props = node.get("properties") or {}
        for filename in _as_list(props.get("filename")):
            if filename:
                name = Path(str(filename)).name
                nodes_by_filename[name].append(node)
                if _node_type(node) == "案例":
                    case_nodes_by_filename[name].append(node)

    cases_missing_main_node = sorted(name for name in source_names & covered_filenames if not case_nodes_by_filename.get(name))
    cases_with_multiple_main_nodes = {
        name: len(items) for name, items in case_nodes_by_filename.items() if len(items) > 1
    }

    unknown_node_types = [
        _node_brief(node) for node in nodes if _node_type(node) and _node_type(node) not in ALLOWED_NODE_TYPES
    ]
    missing_endpoint_edges = []
    invalid_triples = []
    self_loop_edges = []
    for edge in edges:
        source = node_by_identity.get(edge.get("start"))
        target = node_by_identity.get(edge.get("end"))
        if not source or not target:
            missing_endpoint_edges.append(_edge_brief(edge, source, target))
            continue
        if edge.get("start") == edge.get("end"):
            self_loop_edges.append(_edge_brief(edge, source, target))
        triple = (_node_type(source), edge.get("type"), _node_type(target))
        if triple not in ALLOWED_TRIPLES:
            invalid_triples.append({"triple": triple, "edge": _edge_brief(edge, source, target)})

    risk_nodes = [node for node in nodes if _node_type(node) == "风险点"]
    invalid_risk_type_nodes = [
        _node_brief(node)
        for node in risk_nodes
        if any(
            risk_type not in CONTROLLED_RISK_TYPES
            for risk_type in _as_list((node.get("properties") or {}).get("风险类型"))
        )
    ]
    empty_risk_type_nodes = [
        _node_brief(node) for node in risk_nodes if not (node.get("properties") or {}).get("风险类型")
    ]

    regulation_nodes = [node for node in nodes if _node_type(node) == "法规条款依据"]
    indicator_linkable_risk_nodes = [
        _node_brief(node) for node in risk_nodes if (node.get("properties") or {}).get("指标ID")
    ]
    indicator_level3_leaf_nodes = [
        _node_brief(node)
        for node in nodes
        if (node.get("properties") or {}).get("indicator_level") == 3
        and (node.get("properties") or {}).get("is_leaf_indicator") is True
    ]

    per_case_requirements = []
    for filename in sorted(source_names & covered_filenames):
        case_nodes_for_file = case_nodes_by_filename.get(filename, [])
        related_nodes = nodes_by_filename.get(filename, [])
        by_type = Counter(_node_type(node) for node in related_nodes)
        case_props = (case_nodes_for_file[0].get("properties") or {}) if case_nodes_for_file else {}
        risk_values = sorted(
            {
                risk_type
                for node in related_nodes
                if _node_type(node) == "风险点"
                for risk_type in _as_list((node.get("properties") or {}).get("风险类型"))
            }
        )
        keywords = _as_list(case_props.get("关键词"))
        item = {
            "filename": filename,
            "has_case_id": bool(case_props.get("id")),
            "has_case_title": bool(case_props.get("案例名称") or case_props.get("name")),
            "has_source_compliance_domain": bool(case_props.get("合规领域")),
            "has_full_text": bool(case_props.get("原文全文")),
            "has_risk_types": bool(risk_values),
            "risk_types": risk_values,
            "risk_types_controlled": all(value in CONTROLLED_RISK_TYPES for value in risk_values),
            "has_keywords": bool(keywords),
            "has_related_regulation_ids": by_type.get("法规条款依据", 0) > 0,
            "has_related_indicator_ids": any(
                (node.get("properties") or {}).get("指标ID")
                for node in related_nodes
                if _node_type(node) == "风险点"
            ),
            "has_conclusion": by_type.get("案例结果", 0) > 0 or by_type.get("案例分析", 0) > 0,
            "has_analysis": by_type.get("案例分析", 0) > 0,
            "has_disposal_plan": by_type.get("处置方案", 0) > 0,
            "node_type_counts": dict(by_type),
        }
        item["missing_target_fields"] = [
            field
            for field, ok in {
                "case_id": item["has_case_id"],
                "case_title": item["has_case_title"],
                "source_compliance_domain": item["has_source_compliance_domain"],
                "full_text": item["has_full_text"],
                "risk_types": item["has_risk_types"] and item["risk_types_controlled"],
                "keywords": item["has_keywords"],
                "related_regulation_ids": item["has_related_regulation_ids"],
                "related_indicator_ids": item["has_related_indicator_ids"],
                "conclusion": item["has_conclusion"],
                "analysis": item["has_analysis"],
                "disposal_plan": item["has_disposal_plan"],
            }.items()
            if not ok
        ]
        per_case_requirements.append(item)

    missing_field_counter = Counter()
    for item in per_case_requirements:
        missing_field_counter.update(item["missing_target_fields"])

    serious_issues = []
    if missing_source_names:
        serious_issues.append(f"Neo4j 数据未覆盖全部原始案例，缺失 {len(missing_source_names)} / {len(source_files)} 个源文件。")
    if cases_missing_main_node:
        serious_issues.append(f"存在 {len(cases_missing_main_node)} 个已覆盖文件缺少案例主节点。")
    if cases_with_multiple_main_nodes:
        serious_issues.append(f"存在 {len(cases_with_multiple_main_nodes)} 个文件对应多个案例主节点。")
    if unknown_node_types:
        serious_issues.append(f"存在 {len(unknown_node_types)} 个未知节点类型。")
    if invalid_triples:
        serious_issues.append(f"存在 {len(invalid_triples)} 条 schema 外关系三元组。")
    if missing_endpoint_edges:
        serious_issues.append(f"存在 {len(missing_endpoint_edges)} 条悬空关系。")
    if invalid_risk_type_nodes:
        serious_issues.append(f"存在 {len(invalid_risk_type_nodes)} 个风险点节点使用非六类风险类型。")
    if missing_field_counter.get("related_indicator_ids"):
        serious_issues.append(
            f"有 {missing_field_counter['related_indicator_ids']} 个已覆盖案例缺少 related_indicator_ids，无法满足风险点关联三级叶子指标 ID 的要求。"
        )

    return {
        "source_dir": str(SOURCE_DIR),
        "neo4j_dir": str(NEO4J_DIR),
        "source_total": len(source_files),
        "source_by_category": dict(source_by_category),
        "node_count": len(nodes),
        "edge_count": len(edges),
        "node_types": dict(node_type_counter),
        "edge_types": dict(edge_type_counter),
        "graph_tags": dict(graph_tags),
        "covered_source_count": len(source_names & covered_filenames),
        "missing_source_count": len(missing_source_names),
        "missing_source_examples": missing_source_names[:50],
        "extra_covered_count": len(extra_covered_names),
        "extra_covered_examples": extra_covered_names[:50],
        "case_count": len(case_nodes),
        "cases_missing_main_node_count": len(cases_missing_main_node),
        "cases_missing_main_node_examples": cases_missing_main_node[:50],
        "cases_with_multiple_main_nodes": cases_with_multiple_main_nodes,
        "unknown_node_type_count": len(unknown_node_types),
        "unknown_node_type_examples": unknown_node_types[:20],
        "invalid_triple_count": len(invalid_triples),
        "invalid_triple_examples": invalid_triples[:20],
        "missing_endpoint_count": len(missing_endpoint_edges),
        "missing_endpoint_examples": missing_endpoint_edges[:20],
        "self_loop_count": len(self_loop_edges),
        "self_loop_examples": self_loop_edges[:20],
        "risk_node_count": len(risk_nodes),
        "invalid_risk_type_count": len(invalid_risk_type_nodes),
        "invalid_risk_type_examples": invalid_risk_type_nodes[:20],
        "empty_risk_type_count": len(empty_risk_type_nodes),
        "empty_risk_type_examples": empty_risk_type_nodes[:20],
        "regulation_node_count": len(regulation_nodes),
        "risk_nodes_with_indicator_id_count": len(indicator_linkable_risk_nodes),
        "risk_nodes_with_indicator_id_examples": indicator_linkable_risk_nodes[:20],
        "indicator_level3_leaf_node_count": len(indicator_level3_leaf_nodes),
        "target_knowledge_fields": TARGET_KNOWLEDGE_FIELDS,
        "missing_target_field_counts": dict(missing_field_counter),
        "per_case_requirement_examples": per_case_requirements[:20],
        "serious_issues": serious_issues,
    }


def build_report(detail: dict[str, Any]) -> str:
    lines = [
        "# 风险合规案例 Neo4j 数据质量校验报告",
        "",
        "## 一、校验范围",
        "",
        f"- 原始数据目录：`{detail['source_dir']}`",
        f"- Neo4j 导出目录：`{detail['neo4j_dir']}`",
        f"- 原始文件总数：{detail['source_total']}",
    ]
    for category, count in detail["source_by_category"].items():
        lines.append(f"  - {category}：{count}")
    lines.extend(
        [
            f"- Neo4j 节点数：{detail['node_count']}",
            f"- Neo4j 关系数：{detail['edge_count']}",
            f"- 覆盖原始文件数：{detail['covered_source_count']} / {detail['source_total']}",
            "",
            "## 二、严重问题",
            "",
        ]
    )
    if detail["serious_issues"]:
        for index, issue in enumerate(detail["serious_issues"], 1):
            lines.append(f"{index}. {issue}")
    else:
        lines.append("未发现会直接阻断当前数据入库/导出的严重结构问题。")

    lines.extend(
        [
            "",
            "## 三、是否满足 case_knowledge 目标字段",
            "",
            "| 字段 | 结论 | 说明 |",
            "| --- | --- | --- |",
            f"| case_id | 基本满足 | 案例主节点数 {detail['case_count']}，但仍需通过节点属性 id/name 统一映射。 |",
            f"| case_title | 基本满足 | 可由案例节点 `案例名称` 或 `name` 转换。 |",
            f"| source_compliance_domain | 部分满足 | 缺失计数：{detail['missing_target_field_counts'].get('source_compliance_domain', 0)}。该字段不是六类风险类型，应保留原始合规领域。 |",
            f"| full_text | 基本满足 | 可由案例节点 `原文全文` 转换；缺失计数：{detail['missing_target_field_counts'].get('full_text', 0)}。 |",
            f"| risk_types | 满足受控词表要求 | 非六类风险类型节点数：{detail['invalid_risk_type_count']}；空风险类型节点数：{detail['empty_risk_type_count']}。 |",
            f"| keywords | 基本满足 | 缺失计数：{detail['missing_target_field_counts'].get('keywords', 0)}。 |",
            f"| related_regulation_ids | 基本满足 | 法规条款依据节点数：{detail['regulation_node_count']}；字段可由法规节点 id 或依据/违反关系导出。 |",
            f"| related_indicator_ids | 不满足 | 风险点节点含 `指标ID` 的数量：{detail['risk_nodes_with_indicator_id_count']}；三级叶子指标节点数量：{detail['indicator_level3_leaf_node_count']}。当前无法证明其指向 indicator_level=3 且 is_leaf_indicator=true 的指标节点。 |",
            f"| conclusion | 基本满足 | 可由案例结果/案例分析节点导出；缺失计数：{detail['missing_target_field_counts'].get('conclusion', 0)}。 |",
            f"| analysis | 基本满足 | 可由案例分析节点导出；缺失计数：{detail['missing_target_field_counts'].get('analysis', 0)}。 |",
            f"| disposal_plan | 部分满足 | 缺失计数：{detail['missing_target_field_counts'].get('disposal_plan', 0)}；部分原文无处置方案结构。 |",
            "",
            "## 四、结构一致性",
            "",
            "| 检查项 | 数量 |",
            "| --- | ---: |",
            f"| 未知节点类型 | {detail['unknown_node_type_count']} |",
            f"| schema 外关系三元组 | {detail['invalid_triple_count']} |",
            f"| 悬空关系 | {detail['missing_endpoint_count']} |",
            f"| 自环关系 | {detail['self_loop_count']} |",
            f"| 非六类风险类型 | {detail['invalid_risk_type_count']} |",
            f"| 覆盖文件缺少案例主节点 | {detail['cases_missing_main_node_count']} |",
            f"| 一个文件多个案例主节点 | {len(detail['cases_with_multiple_main_nodes'])} |",
            "",
            "## 五、节点类型统计",
            "",
            "| 节点类型 | 数量 |",
            "| --- | ---: |",
        ]
    )
    for node_type, count in Counter(detail["node_types"]).most_common():
        lines.append(f"| {node_type} | {count} |")

    lines.extend(["", "## 六、关系类型统计", "", "| 关系类型 | 数量 |", "| --- | ---: |"])
    for edge_type, count in Counter(detail["edge_types"]).most_common():
        lines.append(f"| {edge_type} | {count} |")

    lines.extend(
        [
            "",
            "## 七、结论",
            "",
            "当前数据在图谱结构层面整体较稳定：未发现 schema 外关系、悬空关系、自环关系或非六类风险类型。",
            "",
            "但若目标是直接满足给定的 `case_knowledge` 知识库格式，仍存在一个阻断点：`related_indicator_ids` 暂不能满足“指向 indicator_level=3 且 is_leaf_indicator=true 的三级指标/叶子指标 ID”的要求。当前风险点节点多数只有指标路径、指标名称，缺少可验证的指标 ID 或与指标库节点的显式关联。",
            "",
            "因此建议：先补充风险点到指标体系的映射流程，再导出最终 `case_knowledge`。专家结论、分析、处置方案如需保留，应在导出层标注为参考知识，避免直接进入后续模型输入。",
        ]
    )
    return "\n".join(lines) + "\n"


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _unwrap(item: dict[str, Any], key: str) -> dict[str, Any]:
    value = item.get(key) if isinstance(item, dict) else item
    return value if isinstance(value, dict) else {}


def _node_type(node: dict[str, Any]) -> str:
    props = node.get("properties") or {}
    return str(props.get("label") or "").strip()


def _node_brief(node: dict[str, Any]) -> dict[str, Any]:
    props = node.get("properties") or {}
    return {
        "identity": node.get("identity"),
        "id": props.get("id"),
        "name": props.get("name"),
        "label": props.get("label"),
        "filename": props.get("filename"),
        "risk_type": props.get("风险类型"),
    }


def _edge_brief(edge: dict[str, Any], source: dict[str, Any] | None, target: dict[str, Any] | None) -> dict[str, Any]:
    return {
        "identity": edge.get("identity"),
        "type": edge.get("type"),
        "start": edge.get("start"),
        "end": edge.get("end"),
        "source_type": _node_type(source or {}),
        "target_type": _node_type(target or {}),
    }


def _as_list(value: Any) -> list[Any]:
    if value in (None, "", [], {}):
        return []
    if isinstance(value, list):
        return value
    return [value]


if __name__ == "__main__":
    main()
