"""合规指引 Neo4j 图谱数据转自然语言知识条目。

依据《合规指引知识条目转化规则设计》，读取 node.json/edge.json，输出逐行
TXT、可审计 JSONL 和统计 CSV。核心原则：转化规则以"节点类型 + 产出条件"
为唯一依据，满足条件即生成知识，不依据图谱类型、抽取 Schema 或数据格式
（分点格式 / 条款格式）决定规则。

产出的知识类型：

1. 文件档案知识（合规指引文件 / 法规文件）
2. 指引内容知识（指引知识单元 / 条款单元）
3. 量化目标知识（量化目标 / 条款单元量化条件）
4. 责任分工知识（责任主体 / 条款单元责任角色）
5. 结构节点原文知识（指引结构节点）
6. 引用知识（引用依据 / 法规依据）
7. 法条效力知识（法条）
"""

from __future__ import annotations

import csv
import hashlib
import json
import logging
import os
import re
import tempfile
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

from .base import (
    extract_edge,
    extract_node,
    first_filename,
    is_empty_value,
    load_json_array,
    sanitize_filename,
    value_to_text,
)
from .source_context import add_chinese_source_context, clean_source_name

logger = logging.getLogger(__name__)

ENABLE_GUIDE_SEMANTIC_KNOWLEDGE = True
ENABLE_SEMANTIC_ROLE_VALIDATION = True
ENABLE_GRAPH_NORMALIZATION = True
ENABLE_STRUCTURE_TEXT_FALLBACK = True
ENABLE_QUANTITATIVE_TARGET_KNOWLEDGE = True
ENABLE_RESPONSIBILITY_KNOWLEDGE = True

_TEMP_DIR = Path(__file__).resolve().parents[1] / "temp"

ALLOWED_RISK_TYPES = {
    "产品法律风险",
    "供应链合规风险",
    "劳动用工法律合规风险",
    "企业关联方合规风险",
    "企业国际化经营合规风险",
    "企业信用风险",
}

# 文件级节点：分点格式（合规指引文件）与条款格式（法规文件）统一处理。
FILE_LABELS = {"合规指引文件", "法规文件"}
# 内容单元：分点格式（指引知识单元）与条款格式（条款单元）统一处理。
CONTENT_LABELS = {"指引知识单元", "条款单元"}
# 引用目标：分点格式（引用依据）与条款格式（引用依据 / 法规依据）统一处理。
REFERENCE_TARGET_LABELS = {"引用依据", "法规依据"}
# 结构节点：仅分点格式。
STRUCTURE_LABELS = {"指引结构节点"}
# 条文节点：仅条款格式。
ARTICLE_LABELS = {"法条"}
# 子实体：仅分点格式。
QUANTITATIVE_TARGET_LABELS = {"量化目标"}
RESPONSIBILITY_LABELS = {"责任主体"}

# 组织/结构关系，不直接生成关系句，仅用于恢复上下文。
ORGANIZATION_RELATIONSHIPS = {"包含", "提出", "由其负责", "设定目标"}

PRIMARY_TYPE_PRIORITY = {
    "量化目标知识": 0,
    "责任分工知识": 1,
    "指引内容知识": 2,
    "结构节点原文知识": 3,
    "法条效力知识": 4,
    "引用知识": 5,
    "文件档案知识": 6,
}

_QUANTITATIVE_RE = re.compile(
    r"(?:[<>≤≥=±]|不(?:少|小|多|大|超过|低|高)于|超过|少于|大于|小于|高于|低于|"
    r"至少|至多|以上|以下|以内|范围|"
    r"\d+(?:\.\d+)?\s*(?:%|％|元|万元|亿元|家|件|次|倍|年|月|日|小时|分钟|秒)|\d+(?:\.\d+)?\s*(?:~|～|—|-|:|∶)\s*\d+)",
    re.IGNORECASE,
)


@dataclass
class NodeInfo:
    """归一化后的 Neo4j 节点。"""

    key: str
    identity: Any
    element_id: str
    business_id: str
    name: str
    label: str
    filename: str
    properties: dict[str, Any]


@dataclass
class ResolvedEdge:
    """已解析端点的 Neo4j 关系。"""

    predicate: str
    start: NodeInfo
    end: NodeInfo
    properties: dict[str, Any]


@dataclass
class KnowledgeEntry:
    """知识条目及其可审计来源信息。"""

    knowledge_type: str
    content: str
    source_file: str
    guide_name: str
    source_node_id: str = ""
    source_node_type: str = ""
    source_location: str = ""
    risk_types: list[str] = field(default_factory=list)
    attachments: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    semantic_types: list[str] = field(default_factory=list)
    knowledge_id: str = ""
    dedup_text: str = field(default="", repr=False)
    structure_key: str = field(default="", repr=False)

    def __post_init__(self) -> None:
        if not self.semantic_types:
            self.semantic_types = [self.knowledge_type]

    def to_dict(self) -> dict[str, Any]:
        return {
            "knowledge_id": self.knowledge_id,
            "knowledge_type": self.knowledge_type,
            "semantic_types": self.semantic_types,
            "content": self.content,
            "source_file": self.source_file,
            "guide_name": self.guide_name,
            "source_node_id": self.source_node_id,
            "source_node_type": self.source_node_type,
            "source_location": self.source_location,
            "risk_types": self.risk_types,
            "attachments": self.attachments,
            "metadata": self.metadata,
        }


@dataclass
class ConversionStats:
    """转换统计及过滤原因。"""

    skip_reasons: Counter[str] = field(default_factory=Counter)
    relationship_issues: Counter[str] = field(default_factory=Counter)
    source_context_issues: Counter[str] = field(default_factory=Counter)

    def skip(self, knowledge_type: str, reason: str, count: int = 1) -> None:
        self.skip_reasons[f"{knowledge_type}:{reason}"] += count

    def relationship_issue(self, reason: str, count: int = 1) -> None:
        self.relationship_issues[reason] += count

    def source_context_issue(self, reason: str, count: int = 1) -> None:
        self.source_context_issues[reason] += count


class GuideGraphContext:
    """合规指引节点、关系及来源上下文索引。"""

    def __init__(
        self,
        node_records: list[dict[str, Any]],
        edge_records: list[dict[str, Any]],
        stats: ConversionStats,
    ) -> None:
        self.stats = stats
        self.nodes: list[NodeInfo] = []
        self.nodes_by_key: dict[str, NodeInfo] = {}
        self.identity_index: dict[Any, NodeInfo] = {}
        self.element_id_index: dict[str, NodeInfo] = {}
        self.nodes_by_business_id: dict[str, NodeInfo] = {}
        self.nodes_by_file: dict[str, list[NodeInfo]] = defaultdict(list)
        self.guide_by_file: dict[str, NodeInfo] = {}
        self.edges: list[ResolvedEdge] = []
        self.incoming: dict[str, list[ResolvedEdge]] = defaultdict(list)
        self.outgoing: dict[str, list[ResolvedEdge]] = defaultdict(list)
        self.structure_parent: dict[str, NodeInfo] = {}
        self.children: dict[str, list[NodeInfo]] = defaultdict(list)
        self._parse_nodes(node_records)
        self._parse_edges(edge_records)
        self._build_context_indexes()

    def _parse_nodes(self, records: list[dict[str, Any]]) -> None:
        for index, record in enumerate(records):
            node = extract_node(record)
            if node is None:
                self.stats.skip("节点", "导出结构无n对象")
                continue
            properties = node.get("properties") or {}
            if not isinstance(properties, dict):
                self.stats.skip("节点", "properties不是对象")
                continue
            identity = node.get("identity")
            element_id = str(node.get("elementId") or "")
            business_id = _text(properties.get("id"))
            filename = first_filename(properties)
            fallback_id = element_id or str(identity) or f"record-{index}"
            business_id = business_id or fallback_id
            key = f"{filename}\x1f{business_id}"
            info = NodeInfo(
                key=key,
                identity=identity,
                element_id=element_id,
                business_id=business_id,
                name=_text(properties.get("name")),
                label=_text(properties.get("label")),
                filename=filename,
                properties=properties,
            )
            self.nodes.append(info)
            self.nodes_by_key[key] = info
            self.nodes_by_business_id[business_id] = info
            self.nodes_by_file[filename].append(info)
            if identity is not None:
                self.identity_index[identity] = info
            if element_id:
                self.element_id_index[element_id] = info
            if info.label in FILE_LABELS and filename not in self.guide_by_file:
                self.guide_by_file[filename] = info

    def _parse_edges(self, records: list[dict[str, Any]]) -> None:
        seen: set[tuple[str, str, str]] = set()
        for record in records:
            edge = extract_edge(record)
            if edge is None:
                self.stats.relationship_issue("导出结构无p对象")
                continue
            predicate = _text(edge.get("type"))
            if not predicate:
                self.stats.relationship_issue("关系类型为空")
                continue
            start = self._find_node(edge.get("start"), edge.get("startNodeElementId"))
            end = self._find_node(edge.get("end"), edge.get("endNodeElementId"))
            if start is None or end is None:
                self.stats.relationship_issue("关系端点无法解析")
                continue
            if start.key == end.key:
                self.stats.relationship_issue("自循环关系")
                continue
            edge_key = (start.key, predicate, end.key)
            if edge_key in seen:
                self.stats.relationship_issue("重复关系")
                continue
            seen.add(edge_key)
            properties = edge.get("properties") or {}
            resolved = ResolvedEdge(
                predicate=predicate,
                start=start,
                end=end,
                properties=properties if isinstance(properties, dict) else {},
            )
            self.edges.append(resolved)
            self.outgoing[start.key].append(resolved)
            self.incoming[end.key].append(resolved)

    def _find_node(self, identity: Any, element_id: Any) -> NodeInfo | None:
        if identity in self.identity_index:
            return self.identity_index[identity]
        if element_id and str(element_id) in self.element_id_index:
            return self.element_id_index[str(element_id)]
        return None

    def _build_context_indexes(self) -> None:
        for edge in self.edges:
            start_label = self.effective_label(edge.start)
            end_label = self.effective_label(edge.end)
            if edge.predicate == "包含":
                if start_label in FILE_LABELS and end_label in STRUCTURE_LABELS:
                    self.structure_parent[edge.end.key] = edge.start
                    self.children[edge.start.key].append(edge.end)
                elif start_label in STRUCTURE_LABELS and end_label in STRUCTURE_LABELS:
                    self.structure_parent[edge.end.key] = edge.start
                    self.children[edge.start.key].append(edge.end)
            elif edge.predicate == "提出" and start_label in STRUCTURE_LABELS:
                self.structure_parent[edge.end.key] = edge.start
                self.children[edge.start.key].append(edge.end)

    def effective_label(self, node: NodeInfo) -> str:
        return node.label

    def structure_for(self, node: NodeInfo) -> NodeInfo | None:
        """返回节点所属的指引结构节点（用于恢复来源位置）。"""
        if self.effective_label(node) in STRUCTURE_LABELS:
            return node
        parent = self.structure_parent.get(node.key)
        if parent is not None:
            return parent
        structure_id = node.properties.get("source_structure_id")
        if not is_empty_value(structure_id):
            structure = self.nodes_by_business_id.get(_text(structure_id))
            if structure is not None and structure.label in STRUCTURE_LABELS:
                return structure
        return None

    def guide_for(self, node: NodeInfo) -> NodeInfo | None:
        guide = self.guide_by_file.get(node.filename)
        if guide is not None:
            return guide
        structure = self.structure_for(node)
        if structure is not None:
            return self.guide_by_file.get(structure.filename)
        return None

    def guide_name(self, node: NodeInfo) -> str:
        guide = self.guide_for(node)
        if guide is not None:
            return (
                _text(guide.properties.get("文件全称"))
                or guide.name
                or clean_source_name(node.filename)
            )
        return clean_source_name(node.filename)

    def source_location(self, node: NodeInfo) -> tuple[str, str, str]:
        """返回 (编号, 标题, 路径)。条款单元使用自身单元编号。"""
        if node.label == "条款单元":
            number = _text(node.properties.get("单元编号"))
            title = _text(node.properties.get("条款主旨"))
            return number, title, number
        structure = self.structure_for(node)
        target = structure or node
        props = target.properties
        number = _text(props.get("显示编号")) or _text(props.get("节点编号"))
        title = _text(props.get("节点标题"))
        path = (
            _text(props.get("完整路径"))
            or _text(node.properties.get("source_structure_path"))
            or number
        )
        return number, title, path

    def risk_types_for(self, node: NodeInfo) -> list[str]:
        guide = self.guide_for(node)
        guide_risks = guide.properties.get("合规风险类型") if guide else None
        return _risk_types(node.properties.get("合规风险类型"), guide_risks)


def _text(value: Any) -> str:
    if is_empty_value(value):
        return ""
    return value_to_text(value).strip()


def _as_text_list(value: Any) -> list[str]:
    if is_empty_value(value):
        return []
    if isinstance(value, (list, tuple, set)):
        return [_text(item) for item in value if _text(item)]
    text = _text(value)
    if not text:
        return []
    return [part.strip() for part in re.split(r"[，,、;；\n]+", text) if part.strip()]


def _risk_types(*values: Any) -> list[str]:
    result: list[str] = []
    for value in values:
        for item in _as_text_list(value):
            if item in ALLOWED_RISK_TYPES and item not in result:
                result.append(item)
    return result


def _normalize_text(value: str) -> str:
    text = value.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[　\t ]+", " ", text)
    text = re.sub(r"\s*\n\s*", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _comparison_text(value: str) -> str:
    return re.sub(r"[\s，,。；;：:]", "", _normalize_text(value))


def _ensure_sentence(value: str) -> str:
    text = _normalize_text(value)
    if text and text[-1] not in "。！？；.!?;：:":
        return f"{text}。"
    return text


def _location_prefix(number: str, title: str = "") -> str:
    prefix = ""
    if number:
        prefix += number
    if title:
        prefix += f"“{title}”"
    return prefix


def _node_id(node: NodeInfo) -> str:
    return node.business_id or node.element_id or str(node.identity or "")


def _metadata(**values: Any) -> dict[str, Any]:
    return {key: value for key, value in values.items() if not is_empty_value(value)}


def _make_entry(
    context: GuideGraphContext,
    node: NodeInfo,
    knowledge_type: str,
    content: str,
    *,
    dedup_text: str = "",
    metadata: dict[str, Any] | None = None,
    source_structure: NodeInfo | None = None,
) -> KnowledgeEntry:
    structure = source_structure or context.structure_for(node)
    number, _title, path = context.source_location(structure or node)
    body = _ensure_sentence(content)
    guide_name = context.guide_name(node)
    source_node_type = context.effective_label(node)
    rendered_content = body
    if source_node_type not in FILE_LABELS:
        rendered_content = add_chinese_source_context(body, guide_name)
        if rendered_content == body and not clean_source_name(guide_name):
            context.stats.source_context_issue("非文件节点缺少有效指引名称")
    return KnowledgeEntry(
        knowledge_type=knowledge_type,
        content=rendered_content,
        source_file=node.filename,
        guide_name=guide_name,
        source_node_id=_node_id(node),
        source_node_type=source_node_type,
        source_location=path or number,
        risk_types=context.risk_types_for(node),
        attachments=[],
        metadata=metadata or {},
        dedup_text=dedup_text or content,
        structure_key=structure.key if structure else "",
    )


# ---------------------------------------------------------------------------
# 文件节点 → 文件档案知识
# ---------------------------------------------------------------------------


def _generate_file_entries(
    context: GuideGraphContext,
    stats: ConversionStats,
) -> list[KnowledgeEntry]:
    entries: list[KnowledgeEntry] = []
    for node in context.nodes:
        if node.label not in FILE_LABELS:
            continue
        props = node.properties
        name = _text(props.get("文件全称")) or node.name
        if not name:
            stats.skip("文件档案知识", "文件全称为空")
            continue
        filename = node.filename
        nature = _text(props.get("文件类型")) or _text(props.get("文件性质"))
        wenhao = _text(props.get("文号"))
        publisher = _text(props.get("发布单位"))

        def emit(content: str, metadata: dict[str, Any] | None = None) -> None:
            entries.append(
                _make_entry(
                    context,
                    node,
                    "文件档案知识",
                    content,
                    metadata=metadata,
                )
            )

        # 1. 文件身份
        identity_parts: list[str] = [name]
        if wenhao:
            identity_parts.append(f"（{wenhao}）")
        identity_parts.append("是")
        if publisher:
            identity_parts.append(f"{publisher}发布的")
        if nature:
            identity_parts.append(nature)
        emit("".join(identity_parts), _metadata(file_type=nature))

        # 2. 发布实施
        publication_parts: list[str] = []
        if publisher:
            publication_parts.append(f"由{publisher}发布")
        publish_date = _text(props.get("发布日期"))
        if publish_date:
            publication_parts.append(f"发布日期为{publish_date}")
        effective_date = _text(props.get("生效日期")) or _text(props.get("实施日期"))
        if effective_date:
            publication_parts.append(f"实施日期为{effective_date}")
        status = _text(props.get("时效性"))
        if status:
            publication_parts.append(f"当前状态为{status}")
        if publication_parts:
            emit(f"{name}{'，'.join(publication_parts)}")

        # 3. 制定目的
        purpose = _text(props.get("制定目的"))
        if purpose:
            emit(f"{name}的制定目的是{purpose}", _metadata(summary_kind="制定目的"))

        # 4. 适用范围
        scope = _text(props.get("适用范围")) or _text(props.get("应用范围"))
        if scope:
            emit(f"{name}的适用范围是{scope}", _metadata(summary_kind="适用范围"))
    return entries


# ---------------------------------------------------------------------------
# 内容单元 → 指引内容知识
# ---------------------------------------------------------------------------


def _content_body(node: NodeInfo) -> str:
    props = node.properties
    if node.label == "条款单元":
        return _text(props.get("条款单元内容")) or node.name
    return _text(props.get("知识内容")) or node.name


def _content_verb(node: NodeInfo) -> str:
    return "规定" if node.label == "条款单元" else "明确"


def _is_meaningful_content(body: str) -> bool:
    if len(_comparison_text(body)) < 5:
        return False
    return True


def _generate_content_entries(
    context: GuideGraphContext,
    stats: ConversionStats,
) -> list[KnowledgeEntry]:
    entries: list[KnowledgeEntry] = []
    for node in context.nodes:
        if node.label not in CONTENT_LABELS:
            continue
        props = node.properties
        # 候选标志：分点格式中为 false 时不产出
        if node.label == "指引知识单元" and _text(props.get("是否知识条目候选")) == "False":
            stats.skip("指引内容知识", "非知识条目候选")
            continue
        body = _content_body(node)
        if not _is_meaningful_content(body):
            stats.skip("指引内容知识", "内容为空或不完整")
            continue
        number, title, _path = context.source_location(node)
        verb = _content_verb(node)
        prefix = _location_prefix(number, title)
        sentence = f"{prefix}{verb}：{body}" if prefix else f"{verb}：{body}"
        entries.append(
            _make_entry(
                context,
                node,
                "指引内容知识",
                sentence,
                dedup_text=body,
                metadata=_metadata(
                    knowledge_type=_text(props.get("知识类型"))
                    or _text(props.get("功能类型")),
                    constraint_strength=_text(props.get("约束强度")),
                    applicable_object=_text(props.get("适用对象"))
                    or _text(props.get("适用主体")),
                    behavior=_text(props.get("行为描述")),
                    applicable_condition=_text(props.get("适用条件"))
                    or _text(props.get("适用前提")),
                    time_requirement=_text(props.get("时间要求"))
                    or _text(props.get("时间限制")),
                ),
            )
        )
    return entries


# ---------------------------------------------------------------------------
# 量化目标 → 量化目标知识
# ---------------------------------------------------------------------------


def _has_quantitative_boundary(value: str) -> bool:
    if not value:
        return False
    plain = value
    if _QUANTITATIVE_RE.search(plain):
        return True
    has_number = bool(re.search(r"\d", plain))
    has_relation = bool(
        re.search(
            r"[<>≤≥=±~～×]|不(?:少|小|多|大|超过|低|高)于|超过|少于|大于|小于|高于|低于|至少|至多|以上|以下|以内",
            plain,
        )
    )
    return has_number and has_relation


def _generate_quantitative_target_entries(
    context: GuideGraphContext,
    stats: ConversionStats,
) -> list[KnowledgeEntry]:
    if not ENABLE_QUANTITATIVE_TARGET_KNOWLEDGE:
        return []
    entries: list[KnowledgeEntry] = []
    for node in context.nodes:
        if node.label not in QUANTITATIVE_TARGET_LABELS:
            continue
        props = node.properties
        minimum = _text(props.get("最小值"))
        maximum = _text(props.get("最大值"))
        relation = _text(props.get("约束关系"))
        if not (minimum or maximum) or not relation:
            stats.skip("量化目标知识", "缺少有效数值或约束关系")
            continue
        source_text = _text(props.get("原始文本"))
        condition = _text(props.get("适用条件"))
        unit = _text(props.get("单位"))
        number, title, _path = context.source_location(node)
        if source_text and _has_quantitative_boundary(source_text):
            # 原文优先；原文为片段且未包含适用对象时，前置适用条件补全语义。
            if condition and _comparison_text(condition) not in _comparison_text(source_text):
                body = f"{condition}{source_text}"
            else:
                body = source_text
        else:
            if minimum and maximum and minimum != maximum:
                value_expr = f"{minimum}～{maximum}"
            else:
                value_expr = minimum or maximum
            body = f"{condition}达到{value_expr}"
            if unit and unit not in body:
                body += unit
        prefix = _location_prefix(number, title)
        sentence = f"{prefix}提出：{body}" if prefix else f"提出：{body}"
        entries.append(
            _make_entry(
                context,
                node,
                "量化目标知识",
                sentence,
                dedup_text=body,
                metadata=_metadata(
                    quantitative_value_type=_text(props.get("量化值类型")),
                    minimum=minimum,
                    maximum=maximum,
                    unit=_text(props.get("单位")),
                    constraint_relation=relation,
                    applicable_condition=_text(props.get("适用条件")),
                ),
            )
        )
    return entries


# ---------------------------------------------------------------------------
# 责任主体 → 责任分工知识
# ---------------------------------------------------------------------------


def _generate_responsibility_entries(
    context: GuideGraphContext,
    stats: ConversionStats,
) -> list[KnowledgeEntry]:
    if not ENABLE_RESPONSIBILITY_KNOWLEDGE:
        return []
    entries: list[KnowledgeEntry] = []
    for node in context.nodes:
        if node.label not in RESPONSIBILITY_LABELS:
            continue
        props = node.properties
        subject = _text(props.get("主体名称")) or node.name
        if not subject:
            stats.skip("责任分工知识", "主体名称为空")
            continue
        role = _text(props.get("责任角色"))
        # 主体名称已含"按职责分工负责"等完整表述时，即使角色为空仍可产出。
        if not role and "按职责分工" not in subject and "负责" not in subject:
            stats.skip("责任分工知识", "责任角色为空且主体不能独立表达责任")
            continue
        number, title, _path = context.source_location(node)
        prefix = _location_prefix(number, title)
        if role:
            body = f"{subject}承担{role}责任"
        else:
            body = subject
        sentence = f"{prefix}明确，{body}" if prefix else f"明确，{body}"
        entries.append(
            _make_entry(
                context,
                node,
                "责任分工知识",
                sentence,
                dedup_text=subject,
                metadata=_metadata(
                    responsibility_role=role,
                    subject_name=subject,
                ),
            )
        )
    return entries


# ---------------------------------------------------------------------------
# 结构节点 → 结构节点原文知识
# ---------------------------------------------------------------------------


def _is_meaningful_structure_text(node: NodeInfo) -> bool:
    props = node.properties
    content = _text(props.get("节点内容"))
    title = _text(props.get("节点标题"))
    node_type = _text(props.get("结构类型"))
    if not content or len(_comparison_text(content)) < 6:
        return False
    if node_type in {"一级标题", "章"} and not _comparison_text(content).strip(
        "一二三四五六七八九十（）、（）"
    ):
        return False
    if title and _comparison_text(content) == _comparison_text(title):
        return False
    return True


def _generate_structure_entries(
    context: GuideGraphContext,
    valid_semantic_entries: list[KnowledgeEntry],
    stats: ConversionStats,
) -> list[KnowledgeEntry]:
    if not ENABLE_STRUCTURE_TEXT_FALLBACK:
        return []
    entries: list[KnowledgeEntry] = []
    covered_structure_keys = {
        entry.structure_key
        for entry in valid_semantic_entries
        if entry.knowledge_type in {"指引内容知识", "量化目标知识", "责任分工知识"}
        and entry.structure_key
    }
    for node in context.nodes:
        if context.effective_label(node) not in STRUCTURE_LABELS:
            continue
        if node.key in covered_structure_keys:
            stats.skip("结构节点原文知识", "已有高优先级语义实体")
            continue
        if not _is_meaningful_structure_text(node):
            stats.skip("结构节点原文知识", "内容为空、不完整或仅标题")
            continue
        props = node.properties
        content = _text(props.get("节点内容"))
        number = _text(props.get("显示编号"))
        title = _text(props.get("节点标题"))
        prefix = _location_prefix(number, title)
        sentence = f"{prefix}提出：{content}" if prefix else f"提出：{content}"
        entries.append(
            _make_entry(
                context,
                node,
                "结构节点原文知识",
                sentence,
                dedup_text=content,
                metadata=_metadata(structure_type=_text(props.get("结构类型"))),
            )
        )
    return entries


# ---------------------------------------------------------------------------
# 法条 → 法条效力知识
# ---------------------------------------------------------------------------


def _generate_article_entries(
    context: GuideGraphContext,
    stats: ConversionStats,
) -> list[KnowledgeEntry]:
    entries: list[KnowledgeEntry] = []
    for node in context.nodes:
        if node.label not in ARTICLE_LABELS:
            continue
        props = node.properties
        scope = _text(props.get("效力范围"))
        if not scope:
            stats.skip("法条效力知识", "效力范围为空")
            continue
        subject = node.name or _text(props.get("条"))
        entries.append(
            _make_entry(
                context,
                node,
                "法条效力知识",
                f"{subject}的效力范围是{scope}",
                dedup_text=scope,
                metadata=_metadata(core_topic=_text(props.get("核心主题"))),
            )
        )
    return entries


# ---------------------------------------------------------------------------
# 引用关系 → 引用知识
# ---------------------------------------------------------------------------


def _reference_target(node: NodeInfo) -> str:
    return _text(node.properties.get("文件全称")) or node.name


def _generate_reference_entries(
    context: GuideGraphContext,
    stats: ConversionStats,
) -> list[KnowledgeEntry]:
    entries: list[KnowledgeEntry] = []
    for edge in context.edges:
        if edge.predicate in ORGANIZATION_RELATIONSHIPS:
            continue
        if edge.predicate not in {"引用", "依据"}:
            stats.relationship_issue("非组织且非引用关系")
            continue
        start_label = context.effective_label(edge.start)
        end_label = context.effective_label(edge.end)
        if end_label not in REFERENCE_TARGET_LABELS:
            stats.relationship_issue("引用目标类型不支持")
            continue
        target = _reference_target(edge.end)
        if not target or target in {"本标准", "本指引", "本规范", "见下文", "本法"}:
            stats.skip("引用知识", "引用目标不明确")
            continue
        number, title, _path = context.source_location(edge.start)
        prefix = _location_prefix(number, title)
        is_file_level = start_label in FILE_LABELS
        if is_file_level:
            # 文件级引用：正文包含文件全称（_make_entry 不会为文件节点加来源前缀）。
            guide = context.guide_name(edge.start)
            if edge.predicate == "依据":
                body = f"{guide}依据{target}制定"
            else:
                body = f"{guide}引用{target}"
            sentence = body
        else:
            if edge.predicate == "依据":
                body = f"依据{target}"
            else:
                body = f"引用{target}"
            sentence = f"{prefix}{body}" if prefix else body
        entries.append(
            _make_entry(
                context,
                edge.start,
                "引用知识",
                sentence,
                dedup_text=body,
                metadata=_metadata(
                    reference_scope="文件级" if is_file_level else "内容级",
                    target=target,
                    reference_verb="依据" if edge.predicate == "依据" else "引用",
                ),
            )
        )
    return entries


# ---------------------------------------------------------------------------
# 去重与输出
# ---------------------------------------------------------------------------


def _entry_priority(entry: KnowledgeEntry) -> int:
    return PRIMARY_TYPE_PRIORITY.get(entry.knowledge_type, 99)


def _deduplicate_entries(
    entries: Iterable[KnowledgeEntry],
    stats: ConversionStats,
) -> list[KnowledgeEntry]:
    selected: dict[tuple[str, str, str], KnowledgeEntry] = {}
    for entry in entries:
        normalized_body = _comparison_text(entry.dedup_text or entry.content)
        if not normalized_body:
            stats.skip(entry.knowledge_type, "正文为空")
            continue
        key = (entry.source_file, entry.source_location, normalized_body)
        existing = selected.get(key)
        if existing is None:
            selected[key] = entry
            continue
        semantic_types = sorted(
            set(existing.semantic_types) | set(entry.semantic_types),
            key=lambda item: PRIMARY_TYPE_PRIORITY.get(item, 99),
        )
        if _entry_priority(entry) < _entry_priority(existing):
            entry.semantic_types = semantic_types
            selected[key] = entry
        else:
            existing.semantic_types = semantic_types
        stats.skip(entry.knowledge_type, "跨类型或同类型正文重复")

    exact_selected: dict[tuple[str, str, str], KnowledgeEntry] = {}
    for entry in selected.values():
        exact_key = (
            entry.source_file,
            entry.source_location,
            _comparison_text(entry.content),
        )
        existing = exact_selected.get(exact_key)
        if existing is None:
            exact_selected[exact_key] = entry
            continue
        semantic_types = sorted(
            set(existing.semantic_types) | set(entry.semantic_types),
            key=lambda item: PRIMARY_TYPE_PRIORITY.get(item, 99),
        )
        if _entry_priority(entry) < _entry_priority(existing):
            entry.semantic_types = semantic_types
            exact_selected[exact_key] = entry
        else:
            existing.semantic_types = semantic_types
        stats.skip(entry.knowledge_type, "最终正文重复")

    result = list(exact_selected.values())
    result.sort(
        key=lambda item: (
            item.source_file,
            item.source_location,
            _entry_priority(item),
            item.content,
        )
    )
    for entry in result:
        raw_id = "\x1f".join(
            (
                entry.source_file,
                entry.knowledge_type,
                entry.source_node_id,
                entry.source_location,
                _normalize_text(entry.content),
            )
        )
        entry.knowledge_id = f"gd_{hashlib.sha256(raw_id.encode('utf-8')).hexdigest()[:24]}"
    return result


def _atomic_text_write(path: Path, lines: Iterable[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        newline="\n",
        delete=False,
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
    )
    temp_name = handle.name
    try:
        with handle:
            for line in lines:
                handle.write(line)
                handle.write("\n")
        os.replace(temp_name, path)
    except Exception:
        Path(temp_name).unlink(missing_ok=True)
        raise


def _write_outputs(
    entries: list[KnowledgeEntry],
    stats: ConversionStats,
    graph_type: str,
    output_root: Path,
) -> tuple[Path, Path, Path, dict[str, int], dict[str, int]]:
    safe_name = sanitize_filename(graph_type)
    txt_path = output_root / "txt" / f"{safe_name}.txt"
    jsonl_path = output_root / "jsonl" / f"{safe_name}.jsonl"
    csv_path = output_root / "csv" / f"{safe_name}_知识条目统计.csv"

    _atomic_text_write(
        txt_path,
        (re.sub(r"\s*\n\s*", " ", entry.content).strip() for entry in entries),
    )
    _atomic_text_write(
        jsonl_path,
        (json.dumps(entry.to_dict(), ensure_ascii=False) for entry in entries),
    )

    type_counts = Counter(entry.knowledge_type for entry in entries)
    file_counts = Counter(entry.source_file for entry in entries)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8-sig",
        newline="",
        delete=False,
        dir=csv_path.parent,
        prefix=f".{csv_path.name}.",
        suffix=".tmp",
    )
    temp_name = handle.name
    try:
        with handle:
            writer = csv.writer(handle)
            writer.writerow(["统计维度", "名称", "数量"])
            writer.writerow(["总计", graph_type, len(entries)])
            for name, count in sorted(type_counts.items()):
                writer.writerow(["知识类型", name, count])
            for name, count in sorted(file_counts.items()):
                writer.writerow(["来源文件", name, count])
            for name, count in sorted(stats.skip_reasons.items()):
                writer.writerow(["跳过原因", name, count])
            for name, count in sorted(stats.relationship_issues.items()):
                writer.writerow(["关系问题", name, count])
        os.replace(temp_name, csv_path)
    except Exception:
        Path(temp_name).unlink(missing_ok=True)
        raise

    return txt_path, jsonl_path, csv_path, dict(type_counts), dict(file_counts)


def convert_compliance_guide_json_to_text(
    graph_type: str = "合规指引",
    *,
    input_dir: Path | None = None,
    output_dir: Path | None = None,
) -> dict[str, Any]:
    """将合规指引 Neo4j 导出 JSON 转换为自然语言知识条目。

    Args:
        graph_type: 输出文件使用的图谱类型名称。
        input_dir: 包含 node.json 和 edge.json 的目录；默认使用模块 temp/json。
        output_dir: 输出根目录；默认使用模块 temp。

    Returns:
        包含知识数量、来源数量、分类统计、跳过统计和输出路径的字典。
    """

    input_root = Path(input_dir) if input_dir is not None else _TEMP_DIR / "json"
    output_root = Path(output_dir) if output_dir is not None else _TEMP_DIR
    node_records = load_json_array(input_root / "node.json")
    edge_records = load_json_array(input_root / "edge.json")

    stats = ConversionStats()
    context = GuideGraphContext(node_records, edge_records, stats)

    candidates: list[KnowledgeEntry] = []
    candidates.extend(_generate_file_entries(context, stats))
    if ENABLE_GUIDE_SEMANTIC_KNOWLEDGE:
        semantic_entries: list[KnowledgeEntry] = []
        semantic_entries.extend(_generate_content_entries(context, stats))
        semantic_entries.extend(_generate_quantitative_target_entries(context, stats))
        semantic_entries.extend(_generate_responsibility_entries(context, stats))
        candidates.extend(semantic_entries)
        candidates.extend(_generate_structure_entries(context, semantic_entries, stats))
    candidates.extend(_generate_article_entries(context, stats))
    candidates.extend(_generate_reference_entries(context, stats))

    entries = _deduplicate_entries(candidates, stats)
    txt_path, jsonl_path, csv_path, type_counts, file_counts = _write_outputs(
        entries,
        stats,
        graph_type,
        output_root,
    )

    logger.info("合规指引自然语言知识总条数：%s", len(entries))
    logger.info("合规指引知识来源文件数：%s", len(file_counts))
    logger.info("合规指引知识类型统计：%s", type_counts)
    logger.info("合规指引知识跳过原因统计：%s", dict(stats.skip_reasons))
    logger.info("合规指引关系问题统计：%s", dict(stats.relationship_issues))
    logger.info("合规指引来源语境问题统计：%s", dict(stats.source_context_issues))
    logger.info("合规指引知识 TXT：%s", txt_path)
    logger.info("合规指引知识 JSONL：%s", jsonl_path)
    logger.info("合规指引知识统计 CSV：%s", csv_path)

    return {
        "total_entries": len(entries),
        "source_files": len(file_counts),
        "knowledge_type_counts": type_counts,
        "source_file_counts": file_counts,
        "skip_reason_counts": dict(stats.skip_reasons),
        "relationship_issue_counts": dict(stats.relationship_issues),
        "source_context_issue_counts": dict(stats.source_context_issues),
        "txt_path": str(txt_path),
        "jsonl_path": str(jsonl_path),
        "csv_path": str(csv_path),
    }


__all__ = [
    "KnowledgeEntry",
    "convert_compliance_guide_json_to_text",
]
