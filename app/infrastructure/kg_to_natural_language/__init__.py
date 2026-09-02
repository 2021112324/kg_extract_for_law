"""
知识图谱导出与自然语言转换模块

提供四个核心功能：
1. 从 Neo4j 按标签导出图谱数据为 JSON
2. 将导出的 JSON 转换为自然语言知识条目
3. 批量生成五类固定标签的知识条目及统计
4. 按三级风险指标生成、评分并排序映射文件知识条目
5. 统计三级及上级风险指标的知识条目数量

主入口函数: process_graph(tag, graph_type)、process_graphs_by_type()、
generate_risk_indicator_knowledge(indicator_number)、
rank_risk_indicator_knowledge(indicator_number)、
process_risk_indicator_knowledge(indicator_number)、
process_all_risk_indicator_knowledge()、
build_risk_indicator_knowledge_statistics()
"""

import sys

from .batch import categories as typed_categories
from .batch import processor as typed_batch
from .converters import base as converter
from .converters import chinese_law as converter_v2
from .converters import compliance_guide as converter_compliance_guide
from .converters import english_law as converter_english_law
from .converters import national_standard as converter_national_standard
from .converters import router as converter_unified
from .converters import source_context
from .graph import exporter
from .risk import config as risk_indicator_config
from .risk import generation as risk_knowledge_generation
from .risk import graph_query as risk_graph_query
from .risk import indicator_tree as risk_indicator_tree
from .risk import ranking as risk_knowledge_ranking
from .risk import statistics as risk_knowledge_statistics
from .risk import tree as risk_tree

clear_temp_dir = exporter.clear_temp_dir
export_graph = exporter.export_graph
export_graph_to_directory = exporter.export_graph_to_directory
convert_json_to_text = converter.convert_json_to_text
convert_json_to_text_v2 = converter_v2.convert_json_to_text_v2
convert_national_standard_json_to_text = (
    converter_national_standard.convert_national_standard_json_to_text
)
convert_english_law_json_to_text = (
    converter_english_law.convert_english_law_json_to_text
)
convert_compliance_guide_json_to_text = (
    converter_compliance_guide.convert_compliance_guide_json_to_text
)
convert_graph_to_text = converter_unified.convert_graph_to_text
detect_graph_type = converter_unified.detect_graph_type
process_typed_knowledge_entries = typed_batch.process_typed_knowledge_entries
generate_risk_indicator_knowledge = (
    risk_knowledge_generation.generate_risk_indicator_knowledge
)
rank_risk_indicator_knowledge = (
    risk_knowledge_ranking.rank_risk_indicator_knowledge
)
process_risk_indicator_knowledge = (
    risk_knowledge_ranking.process_risk_indicator_knowledge
)
process_all_risk_indicator_knowledge = (
    risk_knowledge_ranking.process_all_risk_indicator_knowledge
)
build_risk_indicator_knowledge_statistics = (
    risk_knowledge_statistics.build_risk_indicator_knowledge_statistics
)

# Keep existing imports working while callers migrate to the organized packages.
_LEGACY_MODULE_ALIASES = {
    "converter": converter,
    "converter_v2": converter_v2,
    "converter_national_standard": converter_national_standard,
    "converter_english_law": converter_english_law,
    "converter_compliance_guide": converter_compliance_guide,
    "converter_unified": converter_unified,
    "source_context": source_context,
    "exporter": exporter,
    "typed_batch": typed_batch,
    "typed_categories": typed_categories,
    "risk_graph_query": risk_graph_query,
    "risk_indicator_config": risk_indicator_config,
    "risk_indicator_tree": risk_indicator_tree,
    "risk_knowledge_generation": risk_knowledge_generation,
    "risk_knowledge_ranking": risk_knowledge_ranking,
    "risk_knowledge_statistics": risk_knowledge_statistics,
    "risk_tree": risk_tree,
}
for _legacy_name, _module in _LEGACY_MODULE_ALIASES.items():
    sys.modules[f"{__name__}.{_legacy_name}"] = _module

__all__ = [
    "export_graph",
    "export_graph_to_directory",
    "clear_temp_dir",
    "convert_json_to_text",
    "convert_json_to_text_v2",
    "convert_national_standard_json_to_text",
    "convert_english_law_json_to_text",
    "convert_compliance_guide_json_to_text",
    "convert_graph_to_text",
    "detect_graph_type",
    "process_graph",
    "process_graph_v2",
    "process_graphs_by_type",
    "generate_risk_indicator_knowledge",
    "rank_risk_indicator_knowledge",
    "process_risk_indicator_knowledge",
    "process_all_risk_indicator_knowledge",
    "build_risk_indicator_knowledge_statistics",
]


def process_graphs_by_type() -> dict:
    """从 Neo4j 导出并转换五类固定知识图谱。

    最终在 ``data/type`` 下发布五个分类 TXT 和
    ``知识条目统计.json``。该流程只访问 Neo4j 和本地文件系统。
    """
    return process_typed_knowledge_entries()


def process_graph(tag: str, graph_type: str) -> dict:
    """
    完整处理流程：导出 Neo4j 图谱数据 → 自动识别图谱类型 → 转换为自然语言知识。

    自动识别图谱类型（中文法规 / 国家标准 / 英文法规），并分发到对应转换器。

    Args:
        tag: Neo4j 节点标签（图谱标签），如 "e1_行政监管规则_kg_586736132520148992"
        graph_type: 知识图谱类型名称（用于输出文件命名），如 "行政监管规则"

    Returns:
        dict: 包含 export_stats 和 convert_stats 的结果信息
    """
    # 先清空临时目录
    clear_temp_dir()

    # 步骤1：从 Neo4j 导出图谱数据
    print(f"正在从 Neo4j 导出图谱数据，标签: {tag}")
    export_stats = export_graph(tag)
    print(f"导出完成：{export_stats['node_count']} 个节点，{export_stats['edge_count']} 个关系")

    # 步骤2：自动识别图谱类型 → JSON → 自然语言知识转换
    print(f"正在自动识别图谱类型并转换为自然语言知识，输出名称: {graph_type}")
    convert_stats = convert_graph_to_text(graph_type)
    print(f"转换完成：{convert_stats['total_entries']} 条自然语言知识")

    return {
        "tag": tag,
        "graph_type": graph_type,
        "export": export_stats,
        "convert": convert_stats,
    }


def process_graph_v2(tag: str, graph_type: str) -> dict:
    """
    完整处理流程：导出 Neo4j 图谱数据 → 转换为自然语言知识（v2 过滤+组装）。

    Args:
        tag: Neo4j 节点标签（图谱标签），如 "e1_行政监管规则_kg_586736132520148992"
        graph_type: 知识图谱类型，如 "行政监管规则"

    Returns:
        dict: 包含 export_stats 和 convert_stats 的结果信息
    """
    # 先清空临时目录
    clear_temp_dir()

    # 步骤1：从 Neo4j 导出图谱数据
    print(f"正在从 Neo4j 导出图谱数据，标签: {tag}")
    export_stats = export_graph(tag)
    print(f"导出完成：{export_stats['node_count']} 个节点，{export_stats['edge_count']} 个关系")

    # 步骤2：JSON → 自然语言知识转换（v2）
    print(f"正在转换为自然语言知识（v2），类型: {graph_type}")
    convert_stats = convert_json_to_text_v2(graph_type)
    print(f"转换完成：{convert_stats['total_entries']} 条自然语言知识")

    return {
        "tag": tag,
        "graph_type": graph_type,
        "export": export_stats,
        "convert": convert_stats,
    }
