"""固定知识类别及转换器路由配置。"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TypedKnowledgeCategory:
    """一个固定 Neo4j 标签对应的知识条目类别。"""

    name: str
    label: str
    converter: str
    output_filename: str


TYPED_KNOWLEDGE_CATEGORIES: tuple[TypedKnowledgeCategory, ...] = (
    TypedKnowledgeCategory("法律法规条款", "法律法规条款", "chinese_law", "法律法规条款.txt"),
    TypedKnowledgeCategory("国家标准", "国家标准", "national_standard", "国家标准.txt"),
    TypedKnowledgeCategory("行政监管规则", "行政监管规则", "chinese_law", "行政监管规则.txt"),
    TypedKnowledgeCategory("英文法规", "英文法规", "english_law", "英文法规.txt"),
    TypedKnowledgeCategory("合规指引", "合规指引", "guide", "合规指引.txt"),
)

ALLOWED_TYPED_GRAPH_LABELS = frozenset(
    category.label for category in TYPED_KNOWLEDGE_CATEGORIES
)


__all__ = [
    "ALLOWED_TYPED_GRAPH_LABELS",
    "TYPED_KNOWLEDGE_CATEGORIES",
    "TypedKnowledgeCategory",
]
