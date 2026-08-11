"""
Neo4j 知识图谱导出模块

通过 Cypher 查询将指定标签的图谱数据导出为 node.json 和 edge.json，
保存在 temp/json/ 目录下。
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

from neo4j import Driver, GraphDatabase
from neo4j.graph import Node, Relationship

from app.core.config import settings

logger = logging.getLogger(__name__)

# 临时文件输出目录
_TEMP_JSON_DIR = Path(__file__).resolve().parent / "temp" / "json"


def _get_driver() -> Driver:
    """创建 Neo4j driver 连接。"""
    uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    username = os.getenv("NEO4J_USERNAME", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "neo4j")
    database = os.getenv("NEO4J_DATABASE", "neo4j")
    return GraphDatabase.driver(uri, auth=(username, password))


def _node_to_dict(node: Node) -> dict[str, Any]:
    """将 Neo4j Node 对象转换为导出格式的字典。"""
    return {
        "identity": node.id,
        "labels": list(node.labels),
        "properties": dict(node.items()),
        "elementId": node.element_id,
    }


def _relationship_to_dict(rel: Relationship) -> dict[str, Any]:
    """将 Neo4j Relationship 对象转换为导出格式的字典。"""
    return {
        "identity": rel.id,
        "start": rel.start_node.id,
        "end": rel.end_node.id,
        "type": rel.type,
        "properties": dict(rel.items()),
        "elementId": rel.element_id,
        "startNodeElementId": rel.start_node.element_id,
        "endNodeElementId": rel.end_node.element_id,
    }


def clear_temp_dir() -> None:
    """清空并重建 temp 临时目录。"""
    temp_dir = _TEMP_JSON_DIR.parent
    for sub_dir_name in ("json", "txt", "csv"):
        sub_dir = temp_dir / sub_dir_name
        if sub_dir.exists():
            for file in sub_dir.iterdir():
                if file.is_file():
                    file.unlink()
        sub_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"已清空临时目录: {temp_dir}")


def export_graph(tag: str) -> dict[str, Any]:
    """
    从 Neo4j 导出指定标签的图谱数据为 JSON 文件。

    Args:
        tag: Neo4j 节点标签（图谱标签），如 "e1_行政监管规则_kg_586736132520148992"

    Returns:
        dict: 包含 node_count、edge_count 的统计信息

    Raises:
        ConnectionError: Neo4j 不可达时抛出
    """
    clear_temp_dir()
    _TEMP_JSON_DIR.mkdir(parents=True, exist_ok=True)

    driver = _get_driver()

    try:
        # 验证连接
        driver.verify_connectivity()
    except Exception as e:
        driver.close()
        raise ConnectionError(f"无法连接 Neo4j 数据库: {e}") from e

    try:
        with driver.session(database=os.getenv("NEO4J_DATABASE", "neo4j")) as session:
            # 导出节点
            node_query = f"MATCH (n:`{tag}`) RETURN n"
            node_result = session.run(node_query)
            nodes = []
            for record in node_result:
                node = record["n"]
                nodes.append({"n": _node_to_dict(node)})

            node_path = _TEMP_JSON_DIR / "node.json"
            with open(node_path, "w", encoding="utf-8") as f:
                json.dump(nodes, f, ensure_ascii=False, indent=2)
            logger.info(f"已导出 {len(nodes)} 个节点 -> {node_path}")

            # 导出关系
            edge_query = f"MATCH (n:`{tag}`)-[p]->(q) RETURN p"
            edge_result = session.run(edge_query)
            edges = []
            for record in edge_result:
                rel = record["p"]
                edges.append({"p": _relationship_to_dict(rel)})

            edge_path = _TEMP_JSON_DIR / "edge.json"
            with open(edge_path, "w", encoding="utf-8") as f:
                json.dump(edges, f, ensure_ascii=False, indent=2)
            logger.info(f"已导出 {len(edges)} 个关系 -> {edge_path}")

            return {
                "node_count": len(nodes),
                "edge_count": len(edges),
                "node_path": str(node_path),
                "edge_path": str(edge_path),
            }
    except ConnectionError:
        raise
    except Exception as e:
        raise RuntimeError(f"导出图谱数据失败: {e}") from e
    finally:
        driver.close()
