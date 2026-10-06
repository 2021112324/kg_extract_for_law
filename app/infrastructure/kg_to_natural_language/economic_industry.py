"""按经济行业分类知识条目。

基于 ``data/json/filename到经济行业映射.json``，通过节点的 ``filename`` 获取经济行业，
将五类图谱（法律法规条款 / 国家标准 / 行政监管规则 / 英文法规 / 合规指引）的知识条目
按行业分类保存到 ``data/type/经济行业/<行业>.txt``。

用法：
    python -m app.infrastructure.kg_to_natural_language.economic_industry
    或
    from app.infrastructure.kg_to_natural_language import classify_knowledge_by_industry
"""

from __future__ import annotations

import json
import logging
import tempfile
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

from .batch.categories import TYPED_KNOWLEDGE_CATEGORIES
from .converters.base import (
    convert_json_to_text,
    extract_edge,
    extract_node,
    first_filename,
    load_json_array,
)
from .converters.chinese_law import convert_json_to_text_v2
from .converters.compliance_guide import convert_compliance_guide_json_to_text
from .converters.english_law import convert_english_law_json_to_text
from .converters.national_standard import convert_national_standard_json_to_text
from .graph.exporter import export_graph_to_directory

logger = logging.getLogger(__name__)

_DEFAULT_MAPPING_PATH = Path(__file__).resolve().parent / "data" / "json" / "filename到经济行业映射.json"
_DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parent / "data" / "type" / "经济行业"
_UNCLASSIFIED = "未分类"

_CONVERTER_BY_NAME = {
    "chinese_law": convert_json_to_text_v2,
    "national_standard": convert_national_standard_json_to_text,
    "english_law": convert_english_law_json_to_text,
    "guide": convert_compliance_guide_json_to_text,
    "generic": convert_json_to_text,
}


def load_industry_mapping(mapping_path: str | Path | None = None) -> dict[str, str]:
    """读取 filename → 经济行业 映射。

    Args:
        mapping_path: 映射文件路径；默认 data/json/filename到经济行业映射.json

    Returns:
        dict[str, str]: 文件名（含扩展名）到经济行业的映射
    """
    path = Path(mapping_path) if mapping_path is not None else _DEFAULT_MAPPING_PATH
    if not path.is_file():
        raise FileNotFoundError(f"未找到映射文件：{path}")
    with path.open("r", encoding="utf-8") as file:
        value = json.load(file)
    if not isinstance(value, dict):
        raise ValueError(f"映射文件根节点必须是 JSON 对象：{path}")
    return {str(key): str(val) for key, val in value.items()}


def _industry_of(properties: dict[str, Any], mapping: dict[str, str]) -> str:
    """根据节点 properties 的 filename 获取经济行业。"""
    filename = first_filename(properties)
    if not filename:
        return _UNCLASSIFIED
    return mapping.get(filename, _UNCLASSIFIED)


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as file:
        json.dump(value, file, ensure_ascii=False, indent=2)
        file.write("\n")


def _converter_for(name: str):
    """按转换器标识返回转换函数。"""
    converter = _CONVERTER_BY_NAME.get(name)
    if converter is None:
        raise ValueError(f"未配置转换器标识：{name}")
    return converter


def classify_knowledge_by_industry(
    *,
    mapping_path: str | Path | None = None,
    output_dir: str | Path | None = None,
) -> dict[str, Any]:
    """从 Neo4j 导出五类图谱，按经济行业分类知识条目并保存。

    Args:
        mapping_path: filename→经济行业 映射文件路径；默认使用内置映射
        output_dir: 输出根目录；默认 data/type/经济行业

    Returns:
        dict: 包含各行业知识条数、源文件数及输出路径的统计信息
    """
    mapping = load_industry_mapping(mapping_path)
    output_root = Path(output_dir) if output_dir is not None else _DEFAULT_OUTPUT_DIR
    output_root.mkdir(parents=True, exist_ok=True)

    industry_lines: dict[str, list[str]] = defaultdict(list)
    industry_files: dict[str, set[str]] = defaultdict(set)

    with tempfile.TemporaryDirectory(prefix="industry_") as tmp:
        work_root = Path(tmp)
        for category in TYPED_KNOWLEDGE_CATEGORIES:
            graph_dir = work_root / category.name / "json"
            export_graph_to_directory(category.label, graph_dir)
            node_records = load_json_array(graph_dir / "node.json")
            edge_records = load_json_array(graph_dir / "edge.json")

            # 按 filename → 经济行业 分组节点
            grouped_nodes: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for record in node_records:
                node = extract_node(record)
                if node is None:
                    continue
                properties = node.get("properties") or {}
                if not isinstance(properties, dict):
                    continue
                industry = _industry_of(properties, mapping)
                grouped_nodes[industry].append(record)
                industry_files[industry].add(first_filename(properties))

            converter = _converter_for(category.converter)
            for industry, records in grouped_nodes.items():
                # 构建该行业节点的 identity 集合，过滤出两端都在集合内的关系
                identity_set = {
                    node.get("identity")
                    for record in records
                    if (node := extract_node(record)) is not None
                    and node.get("identity") is not None
                }
                sub_edges: list[dict[str, Any]] = []
                for record in edge_records:
                    edge = extract_edge(record)
                    if edge is None:
                        continue
                    if edge.get("start") in identity_set and edge.get("end") in identity_set:
                        sub_edges.append(record)

                sub_dir = work_root / category.name / "industry" / industry
                _write_json(sub_dir / "node.json", records)
                _write_json(sub_dir / "edge.json", sub_edges)

                try:
                    convert_stats = converter(
                        category.name,
                        input_dir=sub_dir,
                        output_dir=sub_dir / "conversion",
                    )
                except Exception as exc:
                    logger.warning("类别 %s / 行业 %s 转换失败：%s", category.name, industry, exc)
                    continue

                txt_path = Path(str(convert_stats.get("txt_path") or ""))
                if txt_path.is_file():
                    lines = [
                        line.strip()
                        for line in txt_path.read_text(encoding="utf-8").splitlines()
                        if line.strip()
                    ]
                    industry_lines[industry].extend(lines)

    # 清理上次运行残留、本次不再产出的行业文件
    current_industries = set(industry_lines.keys())
    for stale in output_root.glob("*.txt"):
        if stale.stem not in current_industries:
            stale.unlink(missing_ok=True)

    # 写入各行业知识文件
    statistics: dict[str, dict[str, int]] = {}
    for industry, lines in sorted(industry_lines.items()):
        target = output_root / f"{industry}.txt"
        with target.open("w", encoding="utf-8", newline="\n") as file:
            file.write("\n".join(lines) + ("\n" if lines else ""))
        statistics[industry] = {
            "knowledge_count": len(lines),
            "source_file_count": len(industry_files.get(industry, set())),
        }

    summary = {
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "mapping_count": len(mapping),
        "industries": statistics,
        "total_knowledge_count": sum(item["knowledge_count"] for item in statistics.values()),
        "output_directory": str(output_root.resolve()),
    }
    _write_json(output_root / "分类统计.json", summary)

    logger.info("按经济行业分类完成，总计 %s 条", summary["total_knowledge_count"])
    return summary


__all__ = [
    "classify_knowledge_by_industry",
    "load_industry_mapping",
]


if __name__ == "__main__":
    import sys

    sys.stdout.reconfigure(encoding="utf-8")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    print(json.dumps(classify_knowledge_by_industry(), ensure_ascii=False, indent=2))
