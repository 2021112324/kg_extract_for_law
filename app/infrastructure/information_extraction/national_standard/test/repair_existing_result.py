"""Repair existing national_standard result node/edge JSON exports.

The graph extraction code fixes future outputs. This script cleans the already
exported Neo4j-style result files in ``national_standard/result`` without
calling the LLM.
"""

from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[5]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.infrastructure.information_extraction.national_standard.graph_extract import (
    INTERNAL_REFERENCE_RE,
    STANDARD_PREFIX_RE,
    _looks_like_path_or_url,
    _normalize_constraint_strength,
    _normalize_quant_condition,
)


BASE_DIR = Path(__file__).resolve().parents[1]
RESULT_DIR = BASE_DIR / "result"
NODE_PATH = RESULT_DIR / "node.json"
EDGE_PATH = RESULT_DIR / "edge.json"
REPORT_PATH = RESULT_DIR / "result_repair_report.md"


def repair_existing_result(result_dir: Path = RESULT_DIR) -> dict[str, Any]:
    node_path = result_dir / "node.json"
    edge_path = result_dir / "edge.json"
    nodes = json.loads(node_path.read_text(encoding="utf-8"))
    edges = json.loads(edge_path.read_text(encoding="utf-8"))

    stats: Counter[str] = Counter()
    removed_identity: set[Any] = set()
    removed_element_id: set[str] = set()

    for item in nodes:
        node = item.get("n", item)
        props = node.get("properties", {}) or {}
        label = props.get("label") or _first_label(node)
        filename = _first_value(props.get("filename"))

        if "节点层级" in props:
            props.pop("节点层级", None)
            stats["removed_node_level"] += 1
        if "相关图片地址" in props:
            props.pop("相关图片地址", None)
            stats["removed_image_url_property"] += 1

        if label == "标准文件" and filename:
            if props.get("name") != filename:
                props["name"] = filename
                stats["standard_file_name_fixed"] += 1
            if props.get("标准中文名称") != filename:
                props["标准中文名称"] = filename
                stats["standard_chinese_name_fixed"] += 1

        if label == "表格":
            new_name = _table_display_name(props, filename)
            if new_name and _is_bad_display_name(props.get("name")):
                props["name"] = new_name
                stats["table_name_fixed"] += 1

        if label == "图片":
            new_name = _image_display_name(props, filename)
            if new_name and _is_bad_display_name(props.get("name")):
                props["name"] = new_name
                stats["image_name_fixed"] += 1

        if label == "流程图":
            new_name = _flowchart_display_name(props, filename)
            if new_name and _is_bad_display_name(props.get("name")):
                props["name"] = new_name
                stats["flowchart_name_fixed"] += 1

        if label == "标准要求":
            normalized, raw = _normalize_constraint_strength(props.get("约束强度", ""), props.get("要求内容", ""))
            if props.get("约束强度") != normalized:
                props["约束强度"] = normalized
                if raw:
                    props.setdefault("约束强度原文", raw)
                stats["constraint_strength_fixed"] += 1
            quant = _normalize_quant_condition(props.get("量化条件"))
            if props.get("量化条件") != quant:
                props["量化条件"] = quant
                props["量化特征"] = "定量" if quant else "定性"
                stats["quant_condition_fixed"] += 1

        if label == "引用标准" and not _is_valid_reference_props(props):
            removed_identity.add(node.get("identity"))
            if node.get("elementId"):
                removed_element_id.add(str(node.get("elementId")))
            stats["invalid_reference_removed"] += 1

    if removed_identity or removed_element_id:
        nodes = [
            item
            for item in nodes
            if item.get("n", item).get("identity") not in removed_identity
            and str(item.get("n", item).get("elementId", "")) not in removed_element_id
        ]
        edges = [
            item
            for item in edges
            if _edge_start(item) not in removed_identity
            and _edge_end(item) not in removed_identity
            and str(_edge_start_element_id(item)) not in removed_element_id
            and str(_edge_end_element_id(item)) not in removed_element_id
        ]

    node_path.write_text(json.dumps(nodes, ensure_ascii=False, indent=2), encoding="utf-8")
    edge_path.write_text(json.dumps(edges, ensure_ascii=False, indent=2), encoding="utf-8")
    report = _build_report(stats, len(nodes), len(edges))
    (result_dir / "result_repair_report.md").write_text(report, encoding="utf-8")
    return {"node_count": len(nodes), "edge_count": len(edges), "stats": dict(stats)}


def _first_label(node: dict[str, Any]) -> str:
    labels = node.get("labels") or []
    return labels[0] if labels else ""


def _first_value(value: Any) -> str:
    if isinstance(value, list):
        return str(value[0]) if value else ""
    return str(value or "")


def _is_bad_display_name(value: Any) -> bool:
    text = str(value or "").strip()
    return (
        not text
        or _looks_like_path_or_url(text)
        or bool(__import__("re").match(r"^(?:表格|图片|流程图)?[_-]?[0-9a-f]{16,}$", text, flags=__import__("re").I))
        or text.startswith(("table_id_", "image_id_"))
    )


def _table_display_name(props: dict[str, Any], filename: str) -> str:
    number = str(props.get("表号") or props.get("table_number") or "").strip()
    title = str(props.get("表题") or props.get("table_caption") or "").strip()
    if number and title:
        return f"{number} {title}"
    return title or number or f"{filename} 表格"


def _image_display_name(props: dict[str, Any], filename: str) -> str:
    title = str(props.get("图片标题") or props.get("caption") or props.get("图片说明") or "").strip()
    if title and not _looks_like_path_or_url(title):
        return title
    section = str(props.get("节点标题") or props.get("related_section_title") or "").strip()
    return f"{section} 图片" if section else f"{filename} 图片"


def _flowchart_display_name(props: dict[str, Any], filename: str) -> str:
    number = str(props.get("流程图编号") or "").strip()
    title = str(props.get("流程图标题") or "").strip()
    if number and title:
        return f"{number} {title}"
    return title or number or f"{filename} 流程图"


def _is_valid_reference_props(props: dict[str, Any]) -> bool:
    number = str(props.get("标准编号") or props.get("name") or "").strip()
    name = str(props.get("标准名称") or "").strip()
    text = f"{number} {name} {props.get('引用说明', '')}".strip()
    if props.get("是否内部引用") == "是":
        return False
    if INTERNAL_REFERENCE_RE.fullmatch(number) or INTERNAL_REFERENCE_RE.fullmatch(name):
        return False
    return bool(STANDARD_PREFIX_RE.search(text) or __import__("re").search(r"《[^》]{2,}》", text))


def _edge_start(item: dict[str, Any]) -> Any:
    edge = item.get("p", item)
    return edge.get("start") or edge.get("source_id")


def _edge_end(item: dict[str, Any]) -> Any:
    edge = item.get("p", item)
    return edge.get("end") or edge.get("target_id")


def _edge_start_element_id(item: dict[str, Any]) -> str:
    return str(item.get("p", item).get("startNodeElementId", ""))


def _edge_end_element_id(item: dict[str, Any]) -> str:
    return str(item.get("p", item).get("endNodeElementId", ""))


def _build_report(stats: Counter[str], node_count: int, edge_count: int) -> str:
    lines = [
        "# national_standard 既有结果修复报告",
        "",
        f"- 修复后节点数：{node_count}",
        f"- 修复后关系数：{edge_count}",
        "",
        "## 修复统计",
        "",
        "| 项目 | 数量 |",
        "| --- | ---: |",
    ]
    for key, value in sorted(stats.items()):
        lines.append(f"| {key} | {value} |")
    return "\n".join(lines) + "\n"


def main() -> None:
    print(json.dumps(repair_existing_result(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
