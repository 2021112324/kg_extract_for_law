"""按指标映射文件查询五类 Neo4j 图谱并构建文件子图。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from neo4j import Driver, GraphDatabase

from app.core.config import settings

from .indicator_tree import MappedFile, filename_query_variants, normalize_filename
from ..batch.categories import TYPED_KNOWLEDGE_CATEGORIES


class RiskGraphQueryError(RuntimeError):
    """Neo4j 风险文件查询失败。"""


@dataclass
class GraphFilePartition:
    normalized_key: str
    graph_category: str
    graph_label: str
    graph_filenames: tuple[str, ...]
    node_records: list[dict[str, Any]]
    edge_records: list[dict[str, Any]]

    @property
    def graph_filename(self) -> str:
        return self.graph_filenames[0] if self.graph_filenames else self.normalized_key


@dataclass
class GraphQueryResult:
    partitions: list[GraphFilePartition]
    unmatched_keys: list[str]
    cross_label_matches: dict[str, list[str]]
    relationship_issue_count: int
    queried_labels: list[str]


def _get_driver() -> Driver:
    return GraphDatabase.driver(
        settings.NEO4J_URI,
        auth=(settings.NEO4J_USERNAME, settings.NEO4J_PASSWORD),
    )


def _node_to_dict(node: Any) -> dict[str, Any]:
    if isinstance(node, dict):
        if "properties" in node:
            return dict(node)
        raise TypeError("Neo4j节点字典缺少properties")
    return {
        "identity": node.id,
        "labels": list(node.labels),
        "properties": dict(node.items()),
        "elementId": node.element_id,
    }


def _relationship_to_dict(relationship: Any) -> dict[str, Any]:
    if isinstance(relationship, dict):
        if "startNodeElementId" in relationship and "endNodeElementId" in relationship:
            return dict(relationship)
        raise TypeError("Neo4j关系字典缺少起止节点elementId")
    return {
        "identity": relationship.id,
        "start": relationship.start_node.id,
        "end": relationship.end_node.id,
        "type": relationship.type,
        "properties": dict(relationship.items()),
        "elementId": relationship.element_id,
        "startNodeElementId": relationship.start_node.element_id,
        "endNodeElementId": relationship.end_node.element_id,
    }


def _record_value(record: Any, key: str) -> Any:
    if isinstance(record, dict):
        return record[key]
    return record[key]


def _filename_values(node: dict[str, Any]) -> list[str]:
    value = (node.get("properties") or {}).get("filename")
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item or "").strip()]
    if value is None:
        return []
    text = str(value).strip()
    return [text] if text else []


def _node_element_id(node: dict[str, Any]) -> str:
    value = node.get("elementId")
    if value is None:
        raise TypeError("Neo4j节点缺少elementId")
    return str(value)


def _query_label_records(
    session: Any,
    label: str,
    query_variants: set[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    node_query = f"""
        MATCH (n:`{label}`)
        WHERE n.filename IS NOT NULL
        WITH n,
             CASE WHEN n.filename IS :: LIST<ANY>
                  THEN n.filename ELSE [n.filename] END AS filenames
        WHERE any(filename IN filenames WHERE
            toLower(trim(last(split(replace(toString(filename), $backslash, '/'), '/'))))
            IN $filename_variants)
        RETURN n
    """
    nodes = [
        _node_to_dict(_record_value(record, "n"))
        for record in session.run(
            node_query,
            filename_variants=sorted(query_variants),
            backslash="\\",
        )
    ]
    if not nodes:
        return [], []

    element_ids = sorted({_node_element_id(node) for node in nodes})
    edge_query = f"""
        MATCH (start:`{label}`)-[relationship]->(end:`{label}`)
        WHERE elementId(start) IN $node_ids AND elementId(end) IN $node_ids
        RETURN relationship
    """
    edges = [
        _relationship_to_dict(_record_value(record, "relationship"))
        for record in session.run(edge_query, node_ids=element_ids)
    ]
    return nodes, edges


def _partition_label_records(
    label: str,
    category_name: str,
    nodes: Iterable[dict[str, Any]],
    edges: Iterable[dict[str, Any]],
    target_keys: set[str],
) -> tuple[list[GraphFilePartition], int]:
    node_partitions: dict[str, dict[str, dict[str, Any]]] = {
        key: {} for key in target_keys
    }
    graph_filenames: dict[str, set[str]] = {key: set() for key in target_keys}
    for node in nodes:
        element_id = _node_element_id(node)
        for filename in _filename_values(node):
            key = normalize_filename(filename)
            if key in target_keys:
                node_partitions[key][element_id] = node
                graph_filenames[key].add(filename)

    edge_partitions: dict[str, list[dict[str, Any]]] = {
        key: [] for key in target_keys
    }
    relationship_issues = 0
    for edge in edges:
        start_id = str(edge.get("startNodeElementId") or "")
        end_id = str(edge.get("endNodeElementId") or "")
        assigned = False
        for key, partition_nodes in node_partitions.items():
            if start_id in partition_nodes and end_id in partition_nodes:
                edge_partitions[key].append(edge)
                assigned = True
        if not assigned:
            relationship_issues += 1

    partitions: list[GraphFilePartition] = []
    for key in sorted(target_keys):
        partition_nodes = node_partitions[key]
        if not partition_nodes:
            continue
        partitions.append(
            GraphFilePartition(
                normalized_key=key,
                graph_category=category_name,
                graph_label=label,
                graph_filenames=tuple(sorted(graph_filenames[key], key=str.casefold)),
                node_records=[
                    {"n": node}
                    for _, node in sorted(partition_nodes.items(), key=lambda item: item[0])
                ],
                edge_records=[
                    {"p": edge}
                    for edge in sorted(
                        edge_partitions[key],
                        key=lambda item: str(item.get("elementId") or item.get("identity") or ""),
                    )
                ],
            )
        )
    return partitions, relationship_issues


def query_mapped_file_subgraphs(
    mapped_files: Iterable[MappedFile],
    *,
    driver: Driver | None = None,
) -> GraphQueryResult:
    files = list(mapped_files)
    target_keys = {item.normalized_key for item in files}
    if not target_keys:
        return GraphQueryResult([], [], {}, 0, [])

    variants: set[str] = set()
    for item in files:
        variants.update(filename_query_variants(item.found_filename))

    owns_driver = driver is None
    active_driver = driver or _get_driver()
    all_partitions: list[GraphFilePartition] = []
    relationship_issue_count = 0
    queried_labels: list[str] = []
    try:
        active_driver.verify_connectivity()
        with active_driver.session(database=settings.NEO4J_DATABASE) as session:
            for category in TYPED_KNOWLEDGE_CATEGORIES:
                queried_labels.append(category.label)
                nodes, edges = _query_label_records(
                    session,
                    category.label,
                    variants,
                )
                partitions, issues = _partition_label_records(
                    category.label,
                    category.name,
                    nodes,
                    edges,
                    target_keys,
                )
                all_partitions.extend(partitions)
                relationship_issue_count += issues
    except Exception as exc:
        raise RiskGraphQueryError(f"风险映射文件Neo4j查询失败：{exc}") from exc
    finally:
        if owns_driver:
            active_driver.close()

    category_order = {
        category.name: index for index, category in enumerate(TYPED_KNOWLEDGE_CATEGORIES)
    }
    file_order = {item.normalized_key: index for index, item in enumerate(files)}
    all_partitions.sort(
        key=lambda item: (
            file_order.get(item.normalized_key, len(file_order)),
            category_order.get(item.graph_category, len(category_order)),
        )
    )

    matches_by_key: dict[str, list[str]] = {}
    for partition in all_partitions:
        matches_by_key.setdefault(partition.normalized_key, []).append(
            partition.graph_category
        )
    unmatched = [item.normalized_key for item in files if item.normalized_key not in matches_by_key]
    cross_label = {
        key: categories
        for key, categories in matches_by_key.items()
        if len(set(categories)) > 1
    }
    return GraphQueryResult(
        partitions=all_partitions,
        unmatched_keys=unmatched,
        cross_label_matches=cross_label,
        relationship_issue_count=relationship_issue_count,
        queried_labels=queried_labels,
    )


__all__ = [
    "GraphFilePartition",
    "GraphQueryResult",
    "RiskGraphQueryError",
    "query_mapped_file_subgraphs",
]
