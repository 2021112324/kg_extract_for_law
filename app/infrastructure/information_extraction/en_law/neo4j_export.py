"""英文法规知识图谱 Neo4j 入库辅助工具。

本模块只处理“正式入库前”的轻量转换，不改变抽取阶段 `_kg.json` 的审查数据。
设计目的：
1. 删除不适合长期存入 Neo4j 节点属性的调试/定位字段。
2. 将嵌套结构换成已准备好的扁平字段或 JSON 字符串字段。
3. 为接口批量抽取提供错误、警告和入库统计。
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


# Article 在源文件中的行号、原文兜底全文、嵌套上下文等只适合审查 JSON，不适合 Neo4j 节点属性。
NODE_PROPERTIES_EXCLUDED_FROM_NEO4J = {
    "line_start",
    "line_end",
    "article_text",
    "classification_context",
    "quantitative_condition",
}

# 边上的 source 目前只说明该边是否由构图补充逻辑生成，属于构图审查信息，不作为业务关系属性入库。
EDGE_PROPERTIES_EXCLUDED_FROM_NEO4J = {
    "source",
}


@dataclass
class EnLawNeo4jRunStats:
    """英文法规目录级抽取与入库统计。"""

    total_files: int = 0
    success_files: int = 0
    error: int = 0
    extraction_error: int = 0
    storage_error: int = 0
    file_processing_error: int = 0
    warning: int = 0
    strong_warning: int = 0
    error_messages: list[str] = field(default_factory=list)
    warning_messages: list[str] = field(default_factory=list)
    strong_warning_messages: list[str] = field(default_factory=list)

    def add_extraction_error(self, filename: str, message: str) -> None:
        """记录抽取阶段错误。"""
        self.error += 1
        self.extraction_error += 1
        self.error_messages.append(f"{filename}: {message}")

    def add_storage_error(self, filename: str, message: str) -> None:
        """记录 Neo4j 保存阶段错误。"""
        self.error += 1
        self.storage_error += 1
        self.error_messages.append(f"{filename}: {message}")

    def add_file_processing_error(self, filename: str, message: str) -> None:
        """记录单文件整体处理错误。"""
        self.error += 1
        self.file_processing_error += 1
        self.error_messages.append(f"{filename}: {message}")

    def add_kg_warnings(self, filename: str, kg: dict[str, Any]) -> None:
        """从构图结果 metadata 与 raw 统计中汇总警告。"""
        metadata = kg.get("metadata") or {}
        warnings = metadata.get("warnings") or []
        self.warning += len(warnings)
        self.warning_messages.extend(f"{filename}: {item}" for item in warnings)

        raw = kg.get("raw_llm_result") or {}
        failed_articles = raw.get("failed_article_extractions") or []
        if failed_articles:
            self.strong_warning += len(failed_articles)
            self.strong_warning_messages.extend(
                f"{filename}: failed Article {item.get('article_number') or item.get('article_heading') or 'unknown'}"
                for item in failed_articles
            )

    def to_dict(self) -> dict[str, Any]:
        """转换为可 JSON 序列化的统计字典。"""
        return {
            "total_files": self.total_files,
            "success_files": self.success_files,
            "error": self.error,
            "extraction_error": self.extraction_error,
            "storage_error": self.storage_error,
            "file_processing_error": self.file_processing_error,
            "warning": self.warning,
            "strong_warning": self.strong_warning,
            "error_messages": self.error_messages,
            "warning_messages": self.warning_messages,
            "strong_warning_messages": self.strong_warning_messages,
        }


def discover_en_law_files(root: Path) -> list[Path]:
    """递归发现英文法规输入文件。"""
    if root.is_file() and root.suffix.lower() in {".md", ".txt"}:
        return [root]
    files = [
        path
        for path in root.rglob("*")
        if path.is_file() and path.suffix.lower() in {".md", ".txt"}
    ]
    return sorted(files, key=lambda item: item.as_posix().lower())


def _clean_properties(
    properties: dict[str, Any],
    excluded_keys: set[str],
) -> dict[str, Any]:
    """删除入库不需要的属性，并跳过空值。"""
    cleaned = {}
    for key, value in dict(properties or {}).items():
        if key in excluded_keys:
            continue
        if value in (None, "", [], {}):
            continue
        if key == "quantitative_indicator" and isinstance(value, (dict, list)):
            cleaned[key] = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            continue
        cleaned[key] = value
    return cleaned


def prepare_en_law_kg_for_neo4j(kg: dict[str, Any]) -> dict[str, Any]:
    """生成适合 Neo4j 入库的英文法规 KG。

    抽取结果中的 `split_result`、`raw_llm_result` 和详细定位属性仍保留在本地 JSON，
    但不进入 Neo4j，避免节点属性过大或混入审查字段。
    """
    nodes = []
    for node in kg.get("nodes", []) or []:
        nodes.append(
            {
                "node_id": node.get("node_id"),
                "node_name": node.get("node_name"),
                "node_type": node.get("node_type"),
                "properties": _clean_properties(
                    node.get("properties") or {},
                    NODE_PROPERTIES_EXCLUDED_FROM_NEO4J,
                ),
                "filename": node.get("filename"),
            }
        )

    edges = []
    for edge in kg.get("edges", []) or []:
        edges.append(
            {
                "source_id": edge.get("source_id"),
                "target_id": edge.get("target_id"),
                "relation_type": edge.get("relation_type"),
                "directionality": edge.get("directionality", "single"),
                "properties": _clean_properties(
                    edge.get("properties") or {},
                    EDGE_PROPERTIES_EXCLUDED_FROM_NEO4J,
                ),
                "filename": edge.get("filename"),
            }
        )

    return {
        "nodes": nodes,
        "edges": edges,
        "metadata": {
            "filename": (kg.get("metadata") or {}).get("filename"),
            "document_format": (kg.get("metadata") or {}).get("document_format"),
            "compliance_risk_type": (kg.get("metadata") or {}).get("compliance_risk_type"),
        },
    }
