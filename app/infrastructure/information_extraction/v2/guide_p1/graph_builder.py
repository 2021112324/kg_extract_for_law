"""Graph assembly and Neo4j-safe serialization for guide_p1."""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any

from .prompt.schema import RISK_TYPE_VALUES


def _stable_id(prefix: str, *parts: Any) -> str:
    raw = "\x1f".join(str(part or "") for part in parts)
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]
    return f"{prefix}_{digest}"


def _normalize_name(value: Any) -> str:
    return re.sub(r"[\s《》“”\"'‘’：:，,。.;；（）()]", "", str(value or "")).lower()


def _edge_id(source_id: str, relation_type: str, target_id: str) -> str:
    return _stable_id("guide_edge", source_id, relation_type, target_id)


class GuideP1GraphBuilder:
    def __init__(self, filename: str, parse_result: dict[str, Any]) -> None:
        self.filename = filename
        self.parse_result = parse_result
        self.nodes: list[dict[str, Any]] = []
        self.edges: list[dict[str, Any]] = []
        self.warnings: list[str] = []
        self._nodes_by_id: dict[str, dict[str, Any]] = {}
        self._edge_keys: set[tuple[str, str, str]] = set()
        self._dedup: dict[tuple[str, str], str] = {}
        self.document_id = ""

    def build(self, extraction_result: dict[str, Any] | None = None) -> dict[str, Any]:
        self._add_context_graph()
        if extraction_result:
            self._add_file_extraction(extraction_result.get("file_info") or {})
            for item in extraction_result.get("content_blocks") or []:
                if item.get("status") in {"success", "empty"}:
                    self._add_content_extraction(item)
        metadata = self.parse_result.get("metadata_candidates", {}) or {}
        knowledge_count = sum(1 for node in self.nodes if node["node_type"] == "指引知识单元")
        return {
            "nodes": self.nodes,
            "edges": self.edges,
            "metadata": {
                "filename": self.filename,
                "document_type": "point_form_compliance_guide",
                "compliance_risk_types": metadata.get("risk_types", []),
                "skipped_attachments": self.parse_result.get("skipped_attachments", []),
                "warnings": self.warnings,
                "knowledge_unit_count": knowledge_count,
            },
        }

    def _add_context_graph(self) -> None:
        metadata = self.parse_result.get("metadata_candidates", {}) or {}
        title = metadata.get("document_title") or self.filename
        self.document_id = _stable_id("guide_document", self.filename, title)
        document = {
            "node_id": self.document_id,
            "node_name": title,
            "node_type": "合规指引文件",
            "properties": {
                "文件全称": title,
                "文号": "；".join(metadata.get("document_numbers", [])),
                "发布单位": "；".join(metadata.get("issuer_candidates", [])),
                "发布日期": metadata.get("dates", [""])[-1] if metadata.get("dates") else "",
                "文件类型": metadata.get("document_type", "合规指引"),
                "合规风险类型": [risk for risk in metadata.get("risk_types", []) if risk in RISK_TYPE_VALUES],
                "source_file": self.filename,
                "source_path": self.parse_result.get("source_path", ""),
            },
            "filename": self.filename,
        }
        self._add_node(document)
        for structure in self.parse_result.get("structure_nodes", []) or []:
            node_id = structure["node_id"]
            node = {
                "node_id": node_id,
                "node_name": f"{structure.get('number', '')}{structure.get('title', '')}".strip(),
                "node_type": "指引结构节点",
                "properties": {
                    "结构类型": structure.get("node_type", ""),
                    "显示编号": structure.get("number", ""),
                    "节点标题": structure.get("title", ""),
                    "节点内容": structure.get("content", ""),
                    "完整路径": structure.get("complete_path", ""),
                    "顺序": structure.get("order"),
                    "起始行": structure.get("line_start"),
                    "结束行": structure.get("line_end"),
                    "source_file": self.filename,
                },
                "filename": self.filename,
            }
            self._add_node(node)
        for structure in self.parse_result.get("structure_nodes", []) or []:
            parent_id = structure.get("parent_id") or self.document_id
            if parent_id in self._nodes_by_id:
                self._add_edge(parent_id, "包含", structure["node_id"], {"source": "structure_parser"})

    def _add_file_extraction(self, item: dict[str, Any]) -> None:
        if item.get("status") not in {"success", "empty"}:
            return
        validated = item.get("validated") or {}
        self.warnings.extend(validated.get("warnings", []))
        local_names: dict[str, str] = {}
        for entity in validated.get("entities", []) or []:
            entity_type = entity.get("entity_type")
            if entity_type == "合规指引文件":
                props = entity.get("properties") or {}
                for key, value in props.items():
                    if value not in (None, "", [], {}):
                        self._nodes_by_id[self.document_id]["properties"][key] = value
                local_names[entity.get("name", "")] = self.document_id
            elif entity_type == "引用依据":
                node_id = self._semantic_node(entity, source_block=None)
                local_names[entity.get("name", "")] = node_id
        self._add_validated_relations(validated.get("relations", []), local_names)

    def _add_content_extraction(self, item: dict[str, Any]) -> None:
        if item.get("status") == "empty":
            return
        block = item.get("block") or {}
        validated = item.get("validated") or {}
        self.warnings.extend(validated.get("warnings", []))
        local_names: dict[str, str] = {}
        knowledge_ids: list[str] = []
        for entity in validated.get("entities", []) or []:
            node_id = self._semantic_node(entity, source_block=block)
            local_names[entity.get("name", "")] = node_id
            if entity.get("entity_type") == "指引知识单元":
                knowledge_ids.append(node_id)
                source_node_id = block.get("source_node_id")
                if source_node_id in self._nodes_by_id:
                    self._add_edge(source_node_id, "提出", node_id, {"source": "validated_llm_block"})
        self._add_validated_relations(validated.get("relations", []), local_names)
        if len(knowledge_ids) == 1:
            only_knowledge = knowledge_ids[0]
            for entity in validated.get("entities", []) or []:
                target_id = local_names.get(entity.get("name", ""), "")
                if entity.get("entity_type") == "责任主体" and target_id:
                    self._add_edge(only_knowledge, "由其负责", target_id, {"source": "single_unit_binding"})
                elif entity.get("entity_type") == "量化目标" and target_id:
                    self._add_edge(only_knowledge, "设定目标", target_id, {"source": "single_unit_binding"})
                elif entity.get("entity_type") == "引用依据" and target_id:
                    self._add_edge(only_knowledge, "引用", target_id, {"source": "single_unit_binding"})

    def _semantic_node(self, entity: dict[str, Any], source_block: dict[str, Any] | None) -> str:
        entity_type = entity.get("entity_type", "")
        name = entity.get("name", "") or entity_type
        dedup_types = {"责任主体", "引用依据"}
        dedup_key = (entity_type, _normalize_name(name))
        if entity_type in dedup_types and dedup_key in self._dedup:
            return self._dedup[dedup_key]
        source_id = source_block.get("block_id", "file") if source_block else "file"
        node_id = _stable_id("guide_semantic", self.filename, source_id, entity_type, name)
        props = dict(entity.get("properties") or {})
        if source_block:
            props.update(
                {
                    "source_file": self.filename,
                    "source_block_id": source_block.get("block_id", ""),
                    "source_structure_id": source_block.get("source_node_id", ""),
                    "source_structure_path": source_block.get("source_path", ""),
                    "source_line_start": source_block.get("line_start"),
                    "source_line_end": source_block.get("line_end"),
                    "来源原文": source_block.get("text", ""),
                }
            )
        else:
            props.update({"source_file": self.filename})
        node = {
            "node_id": node_id,
            "node_name": name,
            "node_type": entity_type,
            "properties": props,
            "filename": self.filename,
        }
        self._add_node(node)
        if entity_type in dedup_types:
            self._dedup[dedup_key] = node_id
        return node_id

    def _add_validated_relations(self, relations: list[dict[str, Any]], local_names: dict[str, str]) -> None:
        for relation in relations:
            source_id = local_names.get(relation.get("source", ""), "")
            target_id = local_names.get(relation.get("target", ""), "")
            if not source_id or not target_id:
                self.warnings.append(
                    f"构图时关系端点未找到: {relation.get('source')}-{relation.get('type')}-{relation.get('target')}"
                )
                continue
            self._add_edge(source_id, relation.get("type", ""), target_id, relation.get("properties") or {})

    def _add_node(self, node: dict[str, Any]) -> None:
        node_id = node["node_id"]
        if node_id in self._nodes_by_id:
            return
        self.nodes.append(node)
        self._nodes_by_id[node_id] = node

    def _add_edge(self, source_id: str, relation_type: str, target_id: str, properties: dict[str, Any]) -> None:
        key = (source_id, relation_type, target_id)
        if not source_id or not target_id or not relation_type or key in self._edge_keys:
            return
        self._edge_keys.add(key)
        self.edges.append(
            {
                "edge_id": _edge_id(source_id, relation_type, target_id),
                "source_id": source_id,
                "target_id": target_id,
                "relation_type": relation_type,
                "directionality": "single",
                "properties": dict(properties or {}),
                "filename": self.filename,
            }
        )


def _neo4j_safe_value(value: Any) -> Any:
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    if isinstance(value, (list, tuple)):
        if all(isinstance(item, (str, int, float, bool)) or item is None for item in value):
            return [item for item in value if item is not None]
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return value


def prepare_guide_p1_kg_for_neo4j(kg: dict[str, Any]) -> dict[str, Any]:
    graph = kg.get("graph") if "graph" in kg else kg
    nodes = []
    for node in graph.get("nodes", []) or []:
        properties = {
            key: _neo4j_safe_value(value)
            for key, value in (node.get("properties") or {}).items()
            if value not in (None, "", [], {})
        }
        nodes.append({**node, "properties": properties})
    edges = []
    for edge in graph.get("edges", []) or []:
        properties = {
            key: _neo4j_safe_value(value)
            for key, value in (edge.get("properties") or {}).items()
            if value not in (None, "", [], {}) and key != "source"
        }
        edges.append({**edge, "properties": properties})
    return {"nodes": nodes, "edges": edges, "metadata": graph.get("metadata", {})}
