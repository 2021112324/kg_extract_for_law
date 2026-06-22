"""Validate compliance_case_v1 result quality against source data.

This script does not call the LLM. It checks the current result directory
against source files and writes a Markdown report plus JSON details.
"""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
RESULT_DIR = BASE_DIR / "result"
REPORT_PATH = RESULT_DIR / "result_quality_recheck_report.md"
DETAIL_PATH = RESULT_DIR / "result_quality_recheck_detail.json"

AGGREGATE_FILES = {"summary.json", "node.json", "edge.json", "case_knowledge.json"}
SOURCE_SUFFIXES = {".md", ".txt"}
CONTROLLED_RISK_TYPES = {
    "产品法律风险",
    "供应链合规风险",
    "劳动用工法律合规风险",
    "企业关联方合规风险",
    "企业国际化经营合规风险",
    "企业信用风险",
}

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


def validate_result_quality() -> dict[str, Any]:
    original_files = sorted(
        p for p in DATA_DIR.rglob("*") if p.is_file() and p.suffix.lower() in SOURCE_SUFFIXES
    )
    original_by_category = Counter(p.parent.name for p in original_files)

    summary = _read_json(RESULT_DIR / "summary.json", {})
    nodes = _read_json(RESULT_DIR / "node.json", [])
    edges = _read_json(RESULT_DIR / "edge.json", [])
    case_knowledge = _read_json(RESULT_DIR / "case_knowledge.json", [])
    single_results = _load_single_result_files()

    node_by_id = {node.get("node_id"): node for node in nodes}
    node_type_counter = Counter(node.get("node_type") for node in nodes)
    edge_type_counter = Counter(edge.get("relation_type") for edge in edges)

    unknown_node_types = [node for node in nodes if node.get("node_type") not in ALLOWED_NODE_TYPES]
    missing_endpoint_edges: list[dict[str, Any]] = []
    invalid_triples: list[dict[str, Any]] = []
    self_loop_edges: list[dict[str, Any]] = []
    empty_property_edges: list[dict[str, Any]] = []
    missing_evidence_edges: list[dict[str, Any]] = []

    for edge in edges:
        source = node_by_id.get(edge.get("source_id"))
        target = node_by_id.get(edge.get("target_id"))
        if not source or not target:
            missing_endpoint_edges.append(edge)
            continue
        if edge.get("source_id") == edge.get("target_id"):
            self_loop_edges.append(edge)
        triple = (source.get("node_type"), edge.get("relation_type"), target.get("node_type"))
        if triple not in ALLOWED_TRIPLES:
            invalid_triples.append({"triple": triple, "edge": edge})
        properties = edge.get("properties") or {}
        if not properties:
            empty_property_edges.append(edge)
        if not properties.get("证据文本"):
            missing_evidence_edges.append(edge)

    case_nodes = [node for node in nodes if node.get("node_type") == "案例"]
    case_ids = [node.get("case_id") for node in case_nodes]
    case_id_counter = Counter(case_ids)
    duplicate_case_nodes = {case_id: count for case_id, count in case_id_counter.items() if count != 1}

    node_type_by_case: dict[str, Counter[str]] = defaultdict(Counter)
    for node in nodes:
        node_type_by_case[node.get("case_id")][node.get("node_type")] += 1
    missing_by_type = {
        node_type: [case_id for case_id in case_ids if node_type_by_case[case_id].get(node_type, 0) == 0]
        for node_type in ["案例结果", "案例启示", "法规条款依据", "风险点", "处置方案", "案例分析"]
    }

    edge_key_counter = Counter(
        (edge.get("source_id"), edge.get("relation_type"), edge.get("target_id")) for edge in edges
    )
    duplicate_edges = [
        {"source_id": key[0], "relation_type": key[1], "target_id": key[2], "count": count}
        for key, count in edge_key_counter.items()
        if count > 1
    ]

    original_resolved = {str(path.resolve()) for path in original_files}
    covered_paths: set[str] = set()
    per_file = []
    for result_path, result in single_results:
        parsed = result.get("parsed_document") or {}
        source_path = Path(parsed.get("source_path") or "")
        if not source_path.is_absolute():
            source_path = (BASE_DIR / source_path).resolve()
        else:
            source_path = source_path.resolve()
        covered_paths.add(str(source_path))

        graph = result.get("graph") or {}
        graph_nodes = graph.get("nodes") or []
        graph_edges = graph.get("edges") or []
        case_node = [node for node in graph_nodes if node.get("node_type") == "案例"]
        source_type = parsed.get("source_type", "")
        case_properties = case_node[0].get("properties", {}) if case_node else {}
        errors = []
        if not source_path.exists():
            errors.append("source_path_not_exists")
        if len(case_node) != 1:
            errors.append(f"case_node_count={len(case_node)}")
        if source_type and case_properties.get("数据来源类型") and case_properties.get("数据来源类型") != source_type:
            errors.append(f"source_type_mismatch:{case_properties.get('数据来源类型')}!={source_type}")
        for node_type in ["案例结果", "案例启示", "法规条款依据"]:
            if not any(node.get("node_type") == node_type for node in graph_nodes):
                errors.append(f"missing_{node_type}")

        per_file.append(
            {
                "result_file": result_path.name,
                "case_id": result.get("case_id"),
                "status": result.get("status"),
                "source_path": str(source_path),
                "source_exists": source_path.exists(),
                "source_type": source_type,
                "node_count": len(graph_nodes),
                "edge_count": len(graph_edges),
                "errors": errors,
            }
        )

    knowledge_empty = {
        "related_indicator_ids": sum(1 for item in case_knowledge if not item.get("related_indicator_ids")),
        "related_regulation_ids": sum(1 for item in case_knowledge if not item.get("related_regulation_ids")),
        "risk_types": sum(1 for item in case_knowledge if not item.get("risk_types")),
        "source_compliance_domain": sum(1 for item in case_knowledge if not item.get("source_compliance_domain")),
        "case_summary": sum(1 for item in case_knowledge if not item.get("case_summary")),
        "lessons": sum(1 for item in case_knowledge if not item.get("lessons")),
        "disposal_suggestions": sum(1 for item in case_knowledge if not item.get("disposal_suggestions")),
    }
    invalid_risk_types = [
        {"case_id": item.get("case_id"), "risk_type": risk_type}
        for item in case_knowledge
        for risk_type in (item.get("risk_types") or [])
        if risk_type not in CONTROLLED_RISK_TYPES
    ]
    invalid_graph_risk_types = [
        {
            "case_id": node.get("case_id"),
            "node_id": node.get("node_id"),
            "node_name": node.get("node_name"),
            "risk_type": (node.get("properties") or {}).get("风险类型"),
        }
        for node in nodes
        if node.get("node_type") == "风险点"
        and any(
            risk_type not in CONTROLLED_RISK_TYPES
            for risk_type in _as_list((node.get("properties") or {}).get("风险类型"))
        )
    ]

    serious_issues: list[str] = []
    if len(single_results) < len(original_files):
        serious_issues.append(
            f"结果目录仅覆盖 {len(single_results)} / {len(original_files)} 个原始文件，当前是抽样结果，不是全量抽取结果。"
        )
    if invalid_triples:
        serious_issues.append(f"存在 {len(invalid_triples)} 条 schema 外关系三元组。")
    if missing_endpoint_edges:
        serious_issues.append(f"存在 {len(missing_endpoint_edges)} 条悬空关系。")
    if self_loop_edges:
        serious_issues.append(f"存在 {len(self_loop_edges)} 条自环关系。")
    if unknown_node_types:
        serious_issues.append(f"存在 {len(unknown_node_types)} 个未知节点类型。")
    if empty_property_edges:
        serious_issues.append(f"存在 {len(empty_property_edges)} 条空属性关系。")
    if missing_by_type["案例结果"]:
        serious_issues.append(f"{len(missing_by_type['案例结果'])} 个案例缺少案例结果节点。")
    if missing_by_type["法规条款依据"]:
        serious_issues.append(f"{len(missing_by_type['法规条款依据'])} 个案例缺少法规条款依据节点。")
    if invalid_risk_types:
        serious_issues.append(f"存在 {len(invalid_risk_types)} 个非受控 risk_types 值。")
    if invalid_graph_risk_types:
        serious_issues.append(f"存在 {len(invalid_graph_risk_types)} 个风险点节点使用非受控风险类型。")

    detail = {
        "original_total": len(original_files),
        "original_by_category": dict(original_by_category),
        "result_single_case_files": len(single_results),
        "summary": summary,
        "aggregate_nodes": len(nodes),
        "aggregate_edges": len(edges),
        "case_knowledge_count": len(case_knowledge),
        "node_types": dict(node_type_counter),
        "edge_types": dict(edge_type_counter),
        "unknown_node_type_count": len(unknown_node_types),
        "invalid_triple_count": len(invalid_triples),
        "invalid_triple_examples": invalid_triples[:20],
        "missing_endpoint_count": len(missing_endpoint_edges),
        "self_loop_count": len(self_loop_edges),
        "empty_edge_property_count": len(empty_property_edges),
        "missing_evidence_count": len(missing_evidence_edges),
        "duplicate_case_nodes": duplicate_case_nodes,
        "duplicate_edge_count": len(duplicate_edges),
        "duplicate_edge_examples": duplicate_edges[:20],
        "missing_by_type": missing_by_type,
        "knowledge_empty_fields": knowledge_empty,
        "invalid_risk_type_count": len(invalid_risk_types),
        "invalid_risk_type_examples": invalid_risk_types[:20],
        "invalid_graph_risk_type_count": len(invalid_graph_risk_types),
        "invalid_graph_risk_type_examples": invalid_graph_risk_types[:20],
        "missing_original_count": len(original_resolved - covered_paths),
        "result_input_missing": sorted(covered_paths - original_resolved),
        "per_file": per_file,
        "serious_issues": serious_issues,
    }

    DETAIL_PATH.write_text(json.dumps(detail, ensure_ascii=False, indent=2), encoding="utf-8")
    REPORT_PATH.write_text(_build_report(detail), encoding="utf-8")
    return {"report": str(REPORT_PATH), "detail": str(DETAIL_PATH), **detail}


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _load_single_result_files() -> list[tuple[Path, dict[str, Any]]]:
    results = []
    for path in sorted(RESULT_DIR.glob("*.json")):
        if path.name in AGGREGATE_FILES or "质量" in path.name or "recheck" in path.name:
            continue
        data = _read_json(path, {})
        if isinstance(data, dict) and "parsed_document" in data and "graph" in data:
            results.append((path, data))
    return results


def _build_report(detail: dict[str, Any]) -> str:
    lines = [
        "# compliance_case_v1 结果质量复核报告",
        "",
        "## 一、校验范围",
        "",
        f"- 原始数据目录：`{DATA_DIR}`",
        f"- 结果目录：`{RESULT_DIR}`",
        f"- 原始文件总数：{detail['original_total']}",
    ]
    for category, count in detail["original_by_category"].items():
        lines.append(f"  - {category}：{count}")
    lines.extend(
        [
            f"- 结果单案例 JSON 数：{detail['result_single_case_files']}",
            f"- 汇总节点数：{detail['aggregate_nodes']}",
            f"- 汇总关系数：{detail['aggregate_edges']}",
            "",
            "## 二、严重问题判断",
            "",
        ]
    )
    if detail["serious_issues"]:
        for index, issue in enumerate(detail["serious_issues"], 1):
            lines.append(f"{index}. {issue}")
    else:
        lines.append("未发现会直接阻断当前样本入图的严重结构问题。")

    lines.extend(
        [
            "",
            "## 三、结构一致性复核",
            "",
            "| 检查项 | 数量 |",
            "| --- | ---: |",
            f"| 未知节点类型 | {detail['unknown_node_type_count']} |",
            f"| schema 外关系三元组 | {detail['invalid_triple_count']} |",
            f"| 悬空关系 | {detail['missing_endpoint_count']} |",
            f"| 自环关系 | {detail['self_loop_count']} |",
            f"| 空属性关系 | {detail['empty_edge_property_count']} |",
            f"| 缺少证据文本关系 | {detail['missing_evidence_count']} |",
            f"| 重复案例主节点 | {len(detail['duplicate_case_nodes'])} |",
            f"| 重复关系 | {detail['duplicate_edge_count']} |",
            f"| 非受控 risk_types | {detail['invalid_risk_type_count']} |",
            f"| 风险点节点非受控风险类型 | {detail['invalid_graph_risk_type_count']} |",
            "",
            "## 四、关键节点覆盖",
            "",
            "| 节点类型 | 缺失案例数 |",
            "| --- | ---: |",
        ]
    )
    for node_type, cases in detail["missing_by_type"].items():
        lines.append(f"| {node_type} | {len(cases)} |")

    lines.extend(["", "## 五、节点类型统计", "", "| 节点类型 | 数量 |", "| --- | ---: |"])
    for node_type, count in Counter(detail["node_types"]).most_common():
        lines.append(f"| {node_type} | {count} |")

    lines.extend(["", "## 六、关系类型统计", "", "| 关系类型 | 数量 |", "| --- | ---: |"])
    for edge_type, count in Counter(detail["edge_types"]).most_common():
        lines.append(f"| {edge_type} | {count} |")

    lines.extend(["", "## 七、case_knowledge 空字段统计", "", "| 字段 | 空值数量 |", "| --- | ---: |"])
    for field, count in detail["knowledge_empty_fields"].items():
        lines.append(f"| {field} | {count} |")

    lines.extend(["", "## 八、结论", ""])
    if detail["result_single_case_files"] < detail["original_total"]:
        lines.append(
            f"当前 result 目录是抽样测试结果，仅能说明这 {detail['result_single_case_files']} 个样本的图谱结构质量，不能代表 data 下全部原始案例已经完成抽取。若目标是全量知识图谱，需要重新运行全量批处理。"
        )
    if not any(
        [
            detail["invalid_triple_count"],
            detail["missing_endpoint_count"],
            detail["self_loop_count"],
            detail["unknown_node_type_count"],
            detail["empty_edge_property_count"],
        ]
    ) and not detail["missing_by_type"]["案例结果"] and not detail["missing_by_type"]["法规条款依据"]:
        lines.append("在当前样本结果内，未发现关系结构、端点完整性、主节点数量、关键结果/法规依据覆盖方面的严重问题。")
    if detail["knowledge_empty_fields"].get("related_indicator_ids"):
        lines.append(
            "`related_indicator_ids` 仍全部为空，这属于外部指标映射缺失问题；若后续知识库必须绑定指标 ID，需要接入指标体系映射或单独做指标匹配。"
        )
    return "\n".join(lines) + "\n"


def main() -> None:
    result = validate_result_quality()
    print(
        json.dumps(
            {
                "report": result["report"],
                "detail": result["detail"],
                "original_total": result["original_total"],
                "result_single_case_files": result["result_single_case_files"],
                "serious_issues": result["serious_issues"],
                "invalid_triple_count": result["invalid_triple_count"],
                "missing_endpoint_count": result["missing_endpoint_count"],
                "self_loop_count": result["self_loop_count"],
                "empty_edge_property_count": result["empty_edge_property_count"],
                "missing_evidence_count": result["missing_evidence_count"],
                "missing_by_type": {key: len(value) for key, value in result["missing_by_type"].items()},
                "knowledge_empty_fields": result["knowledge_empty_fields"],
                "invalid_risk_type_count": result["invalid_risk_type_count"],
                "invalid_graph_risk_type_count": result["invalid_graph_risk_type_count"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )


def _as_list(value: Any) -> list[Any]:
    if value in (None, "", [], {}):
        return []
    if isinstance(value, list):
        return value
    return [value]


if __name__ == "__main__":
    main()
