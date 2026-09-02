"""
Neo4j 知识图谱导出模块

通过 Cypher 查询将指定标签的图谱数据导出为 node.json 和 edge.json，
保存在 temp/json/ 目录下。
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from neo4j import Driver, GraphDatabase
from neo4j.graph import Node, Relationship

from app.core.config import settings

from ..batch.categories import ALLOWED_TYPED_GRAPH_LABELS

logger = logging.getLogger(__name__)

# 临时文件输出目录
_TEMP_JSON_DIR = Path(__file__).resolve().parents[1] / "temp" / "json"


def _get_driver() -> Driver:
    """创建 Neo4j driver 连接。"""
    return GraphDatabase.driver(
        settings.NEO4J_URI,
        auth=(settings.NEO4J_USERNAME, settings.NEO4J_PASSWORD),
    )


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


def _write_json(path: Path, records: list[dict[str, Any]]) -> None:
    """以 UTF-8 写入 Neo4j 导出数组。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file:
        json.dump(records, file, ensure_ascii=False, indent=2)


def _export_graph_to_directory(
    tag: str,
    output_dir: Path,
    *,
    internal_relationships_only: bool,
) -> dict[str, Any]:
    """执行实际导出，并返回文件路径及数量。"""
    output_dir.mkdir(parents=True, exist_ok=True)
    driver = _get_driver()

    try:
        driver.verify_connectivity()
    except Exception as exc:
        driver.close()
        raise ConnectionError(f"无法连接 Neo4j 数据库: {exc}") from exc

    try:
        with driver.session(database=settings.NEO4J_DATABASE) as session:
            node_query = f"MATCH (n:`{tag}`) RETURN n"
            nodes = [
                {"n": _node_to_dict(record["n"])}
                for record in session.run(node_query)
            ]
            node_path = output_dir / "node.json"
            _write_json(node_path, nodes)
            logger.info("已导出 %s 个节点 -> %s", len(nodes), node_path)

            if internal_relationships_only:
                edge_query = f"MATCH (n:`{tag}`)-[p]->(q:`{tag}`) RETURN p"
            else:
                edge_query = f"MATCH (n:`{tag}`)-[p]->(q) RETURN p"
            edges = [
                {"p": _relationship_to_dict(record["p"])}
                for record in session.run(edge_query)
            ]
            edge_path = output_dir / "edge.json"
            _write_json(edge_path, edges)
            logger.info("已导出 %s 个关系 -> %s", len(edges), edge_path)

            return {
                "node_count": len(nodes),
                "edge_count": len(edges),
                "node_path": str(node_path),
                "edge_path": str(edge_path),
            }
    except ConnectionError:
        raise
    except Exception as exc:
        raise RuntimeError(f"导出图谱数据失败: {exc}") from exc
    finally:
        driver.close()


def export_graph_to_directory(tag: str, output_dir: str | Path) -> dict[str, Any]:
    """将固定类别标签及其类别内部关系导出到指定目录。

    与兼容入口 :func:`export_graph` 不同，本函数只接受批处理注册表中的
    五个固定标签，且仅保留起止节点都带当前标签的关系。
    """
    if tag not in ALLOWED_TYPED_GRAPH_LABELS:
        allowed = "、".join(sorted(ALLOWED_TYPED_GRAPH_LABELS))
        raise ValueError(f"不支持的分类图谱标签: {tag}；允许值: {allowed}")
    return _export_graph_to_directory(
        tag,
        Path(output_dir),
        internal_relationships_only=True,
    )


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
    return _export_graph_to_directory(
        tag,
        _TEMP_JSON_DIR,
        internal_relationships_only=False,
    )


__all__ = ["clear_temp_dir", "export_graph", "export_graph_to_directory"]
