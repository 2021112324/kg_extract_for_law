"""
知识图谱导出与自然语言转换模块

提供两个核心功能：
1. 从 Neo4j 按标签导出图谱数据为 JSON
2. 将导出的 JSON 转换为自然语言知识条目

主入口函数: process_graph(tag, graph_type)
"""

from .exporter import export_graph, clear_temp_dir
from .converter import convert_json_to_text
from .converter_v2 import convert_json_to_text_v2
from .converter_national_standard import convert_national_standard_json_to_text

__all__ = [
    "export_graph",
    "clear_temp_dir",
    "convert_json_to_text",
    "convert_json_to_text_v2",
    "convert_national_standard_json_to_text",
    "process_graph",
    "process_graph_v2",
]


def process_graph(tag: str, graph_type: str) -> dict:
    """
    完整处理流程：导出 Neo4j 图谱数据 → 转换为自然语言知识（v1 属性转写）。

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

    # 步骤2：JSON → 自然语言知识转换
    print(f"正在转换为自然语言知识，类型: {graph_type}")
    convert_stats = convert_json_to_text(graph_type)
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
