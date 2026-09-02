"""
统一转换入口

自动识别图谱类型（中文法规 / 国家标准 / 英文法规），并分发到对应转换器。
调用方无需预先知道图谱类型，只需传入数据目录即可。
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from .base import extract_node, load_json_array
from .chinese_law import convert_json_to_text_v2
from .national_standard import convert_national_standard_json_to_text
from .english_law import convert_english_law_json_to_text
from .compliance_guide import convert_compliance_guide_json_to_text

logger = logging.getLogger(__name__)

_TEMP_DIR = Path(__file__).resolve().parents[1] / "temp"

# 各类图谱的节点 label 集合（用于自动识别）
# 合规指引（分点格式）的独有节点 label，不包含与中文法规共用的"引用依据"。
COMPLIANCE_GUIDE_LABELS = {"合规指引文件", "指引知识单元", "指引结构节点", "责任主体", "量化目标"}
CHINESE_LAW_LABELS = {"法规文件", "法条", "条款单元", "法规依据", "引用依据"}
NATIONAL_STANDARD_LABELS = {"术语定义", "标准要求", "指标限值", "试验检测方法", "表格", "流程图", "图片"}
ENGLISH_LAW_LABELS = {
    "LegalDocument",
    "LegalBasis",
    "LegalProvision",
    "ProvisionClause",
    "ProvisionTextParagraph",
    "Citation",
}


def detect_graph_type(node_records: list[dict[str, Any]]) -> str:
    """抽样检查节点 label，自动识别图谱类型。

    Args:
        node_records: 导出的节点 JSON 记录列表

    Returns:
        str: "compliance_guide" / "chinese_law" / "national_standard" / "english_law" / "unknown"
    """
    labels: set[str] = set()
    for record in node_records[:200]:  # 抽样前 200 个节点即可
        node = extract_node(record)
        if not node:
            continue
        properties = node.get("properties") or {}
        label = properties.get("label", "")
        if label:
            labels.add(label)

    if labels & ENGLISH_LAW_LABELS:
        return "english_law"
    if labels & COMPLIANCE_GUIDE_LABELS:
        return "compliance_guide"
    if labels & NATIONAL_STANDARD_LABELS:
        return "national_standard"
    if labels & CHINESE_LAW_LABELS:
        return "chinese_law"
    return "unknown"


def convert_graph_to_text(
    graph_type: str = "知识库",
    *,
    input_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
) -> dict[str, Any]:
    """统一入口：自动识别图谱类型 → 分发到对应转换器。

    Args:
        graph_type: 输出文件使用的图谱类型名称（如"行政监管规则"、"国家标准"等）
        input_dir: 包含 node.json 和 edge.json 的目录；默认使用模块 temp/json
        output_dir: 输出根目录；默认使用模块 temp

    Returns:
        dict: 对应转换器返回的结果字典
    """
    input_root = Path(input_dir) if input_dir is not None else _TEMP_DIR / "json"
    node_records = load_json_array(input_root / "node.json")

    kind = detect_graph_type(node_records)
    logger.info("识别到图谱类型：%s", kind)

    if kind == "english_law":
        return convert_english_law_json_to_text(
            graph_type, input_dir=input_dir, output_dir=output_dir
        )
    if kind == "compliance_guide":
        return convert_compliance_guide_json_to_text(
            graph_type, input_dir=input_dir, output_dir=output_dir
        )
    if kind == "national_standard":
        return convert_national_standard_json_to_text(
            graph_type, input_dir=input_dir, output_dir=output_dir
        )
    if kind == "chinese_law":
        return convert_json_to_text_v2(
            graph_type, input_dir=input_dir, output_dir=output_dir
        )
    raise ValueError(
        "无法识别图谱类型，请确认 node.json 中的节点 label 属于"
        "合规指引/中文法规/国家标准/英文法规 之一。"
    )


__all__ = ["detect_graph_type", "convert_graph_to_text"]
