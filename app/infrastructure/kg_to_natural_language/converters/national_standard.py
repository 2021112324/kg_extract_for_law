"""国家标准 Neo4j 图谱数据转自然语言知识条目。

该模块与法规转换器并存，按国家标准专用规则读取 node.json/edge.json，
输出逐行 TXT、可审计 JSONL 和统计 CSV。表格正文、Mermaid 代码和图片
资源不会被直接写入自然语言知识正文。
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

ENABLE_STANDARD_SEMANTIC_KNOWLEDGE = True
ENABLE_SEMANTIC_ROLE_VALIDATION = True
ENABLE_GRAPH_NORMALIZATION = True
ENABLE_STRUCTURE_TEXT_FALLBACK = True
ENABLE_TABLE_INDEX_KNOWLEDGE = True
ENABLE_FLOWCHART_DESCRIPTION_KNOWLEDGE = True
ENABLE_IMAGE_AS_STANDALONE_KNOWLEDGE = False

_TEMP_DIR = Path(__file__).resolve().parents[1] / "temp"

ALLOWED_RISK_TYPES = {
    "产品法律风险",
    "供应链合规风险",
    "劳动用工法律合规风险",
    "企业关联方合规风险",
    "企业国际化经营合规风险",
    "企业信用风险",
}

SEMANTIC_LABELS = {"术语定义", "标准要求", "指标限值", "试验检测方法"}
RESOURCE_LABELS = {"表格", "流程图", "图片"}
ORGANIZATION_RELATIONSHIPS = {"包含", "定义", "规定"}

PRIMARY_TYPE_PRIORITY = {
    "指标限值知识": 0,
    "试验检测方法知识": 1,
    "标准要求知识": 2,
    "术语定义知识": 3,
    "结构节点原文知识": 4,
    "表格索引知识": 5,
    "流程图描述知识": 6,
    "标准范围与目的知识": 7,
    "标准文件档案知识": 8,
    "引用知识": 9,
}

_CHAPTER_NUMBER_RE = re.compile(
    r"^(?:第?[一二三四五六七八九十百千\d]+[章节条款项]?|附录\s*[A-ZＡ-Ｚ]|[A-ZＡ-Ｚ]?\.?\d+(?:\.\d+)*(?:\([a-zA-Z0-9]+\))?)$"
)
_QUANTITATIVE_RE = re.compile(
    r"(?:[<>≤≥=±]|不(?:少|小|多|大|超过|低|高)于|超过|少于|大于|小于|高于|低于|"
    r"至少|至多|以上|以下|以内|范围|"
    r"\d+(?:\.\d+)?\s*(?:%|％|mm|cm|km|m|μm|nm|mg|kg|g|mL|L|s|min|h|d|a|℃|°C|"
    r"次|倍|年|月|日|小时|分钟|秒)|\d+(?:\.\d+)?\s*(?:~|～|—|-|:|∶)\s*\d+)",
    re.IGNORECASE,
)
_METHOD_ACTION_RE = re.compile(
    r"试验|实验|检测|测定|测量|测试|检验|监测|判定|验证|确认|评估|检查|目测|采样|取样|制样|计算|分析|校准"
)
_STRONG_METHOD_ACTION_RE = re.compile(
    r"试验|实验|检测|测定|测量|测试|检验|判定|验证|确认|评估|检查|目测|采样|取样|制样|计算|分析|校准"
)
_INSTALL_MONITOR_RE = re.compile(r"(?:安装|设置|配备).{0,12}监测(?:装置|设备|设施|系统|仪器)")
_TABLE_RE = re.compile(r"<table\b|</table>|^\s*\|.+\|\s*$|^\s*表\s*[A-ZＡ-Ｚ]?\.?\d+\s*[^。；]*$", re.I | re.M)
_FLOW_DETAILS_RE = re.compile(
    r"<details>\s*<summary>\s*flowchart\s*</summary>.*?</details>",
    re.IGNORECASE | re.DOTALL,
)
_MERMAID_BLOCK_RE = re.compile(r"```\s*mermaid\b.*?```", re.IGNORECASE | re.DOTALL)
_ISOLATED_POINTER_RE = re.compile(
    r"^\s*(?:见|参见|详见|按|符合|执行)\s*(?:图|表|附录|第?\s*[A-ZＡ-Ｚ\d一二三四五六七八九十])[^。；]{0,40}[。；]?\s*$"
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
    standard_name: str
    standard_number: str = ""
    source_node_id: str = ""
    source_node_type: str = ""
    source_location: str = ""
    risk_types: list[str] = field(default_factory=list)
    attachments: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    semantic_types: list[str] = field(default_factory=list)
    knowledge_id: str = ""
    dedup_text: str = field(default="", repr=False)
    resource_key: str = field(default="", repr=False)
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
            "standard_name": self.standard_name,
            "standard_number": self.standard_number,
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


class StandardGraphContext:
    """国家标准节点、关系及来源上下文索引。"""

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
        self.nodes_by_file: dict[str, list[NodeInfo]] = defaultdict(list)
        self.standard_by_file: dict[str, NodeInfo] = {}
        self.edges: list[ResolvedEdge] = []
        self.incoming: dict[str, list[ResolvedEdge]] = defaultdict(list)
        self.outgoing: dict[str, list[ResolvedEdge]] = defaultdict(list)
        self.structure_parent: dict[str, NodeInfo] = {}
        self.standard_parent: dict[str, NodeInfo] = {}
        self.children: dict[str, list[NodeInfo]] = defaultdict(list)
        self.structure_proxies: set[str] = set()
        self.attachments_by_structure: dict[str, list[str]] = defaultdict(list)
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
            self.nodes_by_file[filename].append(info)
            if identity is not None:
                self.identity_index[identity] = info
            if element_id:
                self.element_id_index[element_id] = info
            if info.label == "标准文件" and filename not in self.standard_by_file:
                self.standard_by_file[filename] = info

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
                if start_label == "标准文件" and end_label == "标准结构节点":
                    self.standard_parent[edge.end.key] = edge.start
                    self.children[edge.start.key].append(edge.end)
                elif start_label == "标准结构节点" and end_label == "标准结构节点":
                    self.structure_parent[edge.end.key] = edge.start
                    self.children[edge.start.key].append(edge.end)
                elif start_label == "标准结构节点" and end_label in RESOURCE_LABELS:
                    self.structure_parent[edge.end.key] = edge.start
                    self.children[edge.start.key].append(edge.end)
            elif edge.predicate in {"定义", "规定"} and start_label == "标准结构节点":
                self.structure_parent[edge.end.key] = edge.start
                self.children[edge.start.key].append(edge.end)

        if ENABLE_GRAPH_NORMALIZATION:
            self._find_structure_proxies()
        self._index_attachments()

    def _find_structure_proxies(self) -> None:
        for node in self.nodes:
            if node.label != "引用标准":
                continue
            number = _text(node.properties.get("标准编号")) or node.name
            if not _CHAPTER_NUMBER_RE.fullmatch(number.strip()):
                continue
            has_structure_role = any(
                edge.predicate in ORGANIZATION_RELATIONSHIPS
                and edge.end.label in SEMANTIC_LABELS | RESOURCE_LABELS | {"标准结构节点"}
                for edge in self.outgoing.get(node.key, [])
            )
            if has_structure_role:
                self.structure_proxies.add(node.key)

        for proxy_key in self.structure_proxies:
            proxy = self.nodes_by_key[proxy_key]
            for edge in self.outgoing.get(proxy_key, []):
                if edge.predicate in ORGANIZATION_RELATIONSHIPS:
                    self.structure_parent.setdefault(edge.end.key, proxy)
                    self.children[proxy.key].append(edge.end)

    def _index_attachments(self) -> None:
        for node in self.nodes:
            if self.effective_label(node) == "标准结构节点":
                for url in _as_text_list(node.properties.get("相关图片地址")):
                    _append_unique(self.attachments_by_structure[node.key], url)
            if node.label != "图片":
                continue
            parent = self.structure_for(node)
            if parent is None:
                continue
            for key in ("图片链接", "图片地址", "图片路径", "image_url", "image_path", "url"):
                for url in _as_text_list(node.properties.get(key)):
                    _append_unique(self.attachments_by_structure[parent.key], url)

    def effective_label(self, node: NodeInfo) -> str:
        if node.key in self.structure_proxies:
            return "标准结构节点"
        return node.label

    def structure_for(self, node: NodeInfo) -> NodeInfo | None:
        if self.effective_label(node) == "标准结构节点":
            return node
        return self.structure_parent.get(node.key)

    def standard_for(self, node: NodeInfo) -> NodeInfo | None:
        standard = self.standard_by_file.get(node.filename)
        if standard is not None:
            return standard
        structure = self.structure_for(node)
        if structure is not None:
            return self.standard_parent.get(structure.key)
        return None

    def standard_name(self, node: NodeInfo) -> str:
        standard = self.standard_for(node)
        if standard is not None:
            return (
                _text(standard.properties.get("标准中文名称"))
                or standard.name
                or clean_source_name(node.filename)
            )
        return clean_source_name(node.filename)

    def standard_number(self, node: NodeInfo) -> str:
        standard = self.standard_for(node)
        return _text(standard.properties.get("标准编号")) if standard else ""

    def source_location(self, node: NodeInfo) -> tuple[str, str, str]:
        structure = self.structure_for(node)
        target = structure or node
        props = target.properties
        number = (
            _text(props.get("节点编号"))
            or _text(props.get("术语编号"))
            or _text(props.get("指标编号"))
            or _text(props.get("方法编号"))
            or _text(props.get("要求编号"))
            or (_text(props.get("标准编号")) if target.key in self.structure_proxies else "")
        )
        if not number:
            display_name = _text(props.get("节点展示名称")) or target.name
            match = re.match(
                r"^(?:国家标准序号)?(附录\s*[A-ZＡ-Ｚ]|[A-ZＡ-Ｚ]?\.?\d+(?:\.\d+)*(?:\([a-zA-Z0-9]+\))?)",
                display_name,
            )
            if match:
                number = match.group(1)
        title = _text(props.get("节点标题"))
        path = _text(props.get("节点路径")) or _text(props.get("完整路径标题")) or number
        return number, title, path

    def attachments_for(self, node: NodeInfo) -> list[str]:
        structure = self.structure_for(node)
        if structure is None:
            return []
        return list(self.attachments_by_structure.get(structure.key, []))


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


def _append_unique(items: list[str], value: str) -> None:
    if value and value not in items:
        items.append(value)


def _risk_types(*values: Any) -> list[str]:
    result: list[str] = []
    for value in values:
        for item in _as_text_list(value):
            if item in ALLOWED_RISK_TYPES:
                _append_unique(result, item)
    return result


def _normalize_text(value: str) -> str:
    text = value.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[\u3000\t ]+", " ", text)
    text = re.sub(r"\s*\n\s*", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _comparison_text(value: str) -> str:
    return re.sub(r"[\s，,。；;：:]", "", _normalize_text(value))


def _strip_mermaid_resources(value: str) -> str:
    text = _FLOW_DETAILS_RE.sub("", value)
    text = _MERMAID_BLOCK_RE.sub("", text)
    return _normalize_text(text)


def _ensure_sentence(value: str) -> str:
    text = _normalize_text(value)
    if text and text[-1] not in "。！？；.!?;：:":
        return f"{text}。"
    return text


def _location_prefix(_standard_name: str, number: str, title: str = "") -> str:
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
    context: StandardGraphContext,
    node: NodeInfo,
    knowledge_type: str,
    content: str,
    *,
    dedup_text: str = "",
    resource_key: str = "",
    metadata: dict[str, Any] | None = None,
    source_structure: NodeInfo | None = None,
) -> KnowledgeEntry:
    structure = source_structure or context.structure_for(node)
    number, _title, path = context.source_location(structure or node)
    standard = context.standard_for(node)
    standard_risks = standard.properties.get("合规风险类型") if standard else None
    body = _ensure_sentence(content)
    source_node_type = context.effective_label(node)
    standard_name = context.standard_name(node)
    rendered_content = body
    if source_node_type != "标准文件":
        rendered_content = add_chinese_source_context(body, standard_name)
        if rendered_content == body and not clean_source_name(standard_name):
            stats = context.stats
            stats.source_context_issue("非文件节点缺少有效标准名称")
    return KnowledgeEntry(
        knowledge_type=knowledge_type,
        content=rendered_content,
        source_file=node.filename,
        standard_name=standard_name,
        standard_number=context.standard_number(node),
        source_node_id=_node_id(node),
        source_node_type=source_node_type,
        source_location=path or number,
        risk_types=_risk_types(node.properties.get("合规风险类型"), standard_risks),
        attachments=context.attachments_for(node),
        metadata=metadata or {},
        dedup_text=dedup_text or content,
        resource_key=resource_key,
        structure_key=structure.key if structure else "",
    )


def _generate_standard_file_entries(
    context: StandardGraphContext,
    stats: ConversionStats,
) -> list[KnowledgeEntry]:
    entries: list[KnowledgeEntry] = []
    for node in context.nodes:
        if node.label != "标准文件":
            continue
        props = node.properties
        name = _text(props.get("标准中文名称")) or node.name
        if not name:
            stats.skip("标准文件档案知识", "标准名称为空")
            continue
        number = _text(props.get("标准编号"))
        nature = _text(props.get("标准性质"))
        english = _text(props.get("标准英文名称"))
        if number or nature or english:
            if nature:
                content = f"{name}{f'（{number}）' if number else ''}是{nature}"
            elif number:
                content = f"{name}的标准编号为{number}"
            else:
                content = name
            if english:
                content += f"，英文名称为“{english}”"
            entries.append(
                _make_entry(
                    context,
                    node,
                    "标准文件档案知识",
                    content,
                    metadata=_metadata(standard_nature=nature),
                )
            )

        publication_parts: list[str] = []
        publisher = _text(props.get("发布单位"))
        publish_date = _text(props.get("发布日期"))
        effective_date = _text(props.get("实施日期"))
        status = _text(props.get("标准状态"))
        if publisher:
            publication_parts.append(f"由{publisher}发布")
        if publish_date:
            publication_parts.append(f"发布日期为{publish_date}")
        if effective_date:
            publication_parts.append(f"实施日期为{effective_date}")
        if status:
            publication_parts.append(f"当前状态为{status}")
        if publication_parts:
            entries.append(
                _make_entry(
                    context,
                    node,
                    "标准文件档案知识",
                    f"{name}{'，'.join(publication_parts)}",
                )
            )

        ics = _text(props.get("ICS"))
        ccs = _text(props.get("CCS"))
        classification: list[str] = []
        if ics:
            classification.append(f"国际标准分类号为{ics}")
        if ccs:
            classification.append(f"中国标准文献分类号为{ccs}")
        if classification:
            entries.append(
                _make_entry(
                    context,
                    node,
                    "标准文件档案知识",
                    f"{name}的{'，'.join(classification)}",
                )
            )

        replaced = _text(props.get("代替标准"))
        if replaced:
            entries.append(
                _make_entry(
                    context,
                    node,
                    "标准文件档案知识",
                    f"{name}{f'（{number}）' if number else ''}代替{replaced}",
                    metadata={"revision_kind": "代替标准"},
                )
            )
        first_publish = _text(props.get("首次发布日期"))
        if first_publish:
            entries.append(
                _make_entry(
                    context,
                    node,
                    "标准文件档案知识",
                    f"{name}首次发布于{first_publish}",
                    metadata={"revision_kind": "首次发布日期"},
                )
            )
        changes = _text(props.get("标准主要变化"))
        if changes:
            entries.append(
                _make_entry(
                    context,
                    node,
                    "标准文件档案知识",
                    f"{name}的主要变化为：{changes}",
                    metadata={"revision_kind": "主要变化"},
                )
            )

        organization_parts: list[str] = []
        for prop_name, phrase in (
            ("提出单位", "由{}提出"),
            ("归口单位", "由{}归口"),
            ("起草单位", "由{}负责起草"),
            ("参与起草单位", "参与起草单位包括{}"),
            ("主要起草人", "主要起草人包括{}"),
        ):
            value = _text(props.get(prop_name))
            if value:
                organization_parts.append(phrase.format(value))
        if organization_parts:
            entries.append(
                _make_entry(
                    context,
                    node,
                    "标准文件档案知识",
                    f"{name}{'，'.join(organization_parts)}",
                )
            )

        structure_nodes = context.nodes_by_file.get(node.filename, [])
        has_scope_original = any(
            candidate.label == "标准结构节点"
            and _text(candidate.properties.get("节点类型")) == "范围节点"
            and _is_meaningful_structure_text(candidate)
            for candidate in structure_nodes
        )
        has_intro_original = any(
            candidate.label == "标准结构节点"
            and _text(candidate.properties.get("节点类型")) == "引言节点"
            and _is_meaningful_structure_text(candidate)
            for candidate in structure_nodes
        )
        scope = _text(props.get("适用范围摘要"))
        if scope and not has_scope_original:
            entries.append(
                _make_entry(
                    context,
                    node,
                    "标准范围与目的知识",
                    f"{name}的适用范围是{scope}",
                    metadata={"summary_kind": "适用范围"},
                )
            )
        if not has_intro_original:
            background = _text(props.get("制定背景"))
            purpose = _text(props.get("制定目的"))
            if background:
                entries.append(
                    _make_entry(
                        context,
                        node,
                        "标准范围与目的知识",
                        f"{name}的制定背景是{background}",
                        metadata={"summary_kind": "制定背景"},
                    )
                )
            if purpose:
                entries.append(
                    _make_entry(
                        context,
                        node,
                        "标准范围与目的知识",
                        f"{name}的制定目的是{purpose}",
                        metadata={"summary_kind": "制定目的"},
                    )
                )
    return entries


def _generate_term_entries(
    context: StandardGraphContext,
    stats: ConversionStats,
) -> list[KnowledgeEntry]:
    entries: list[KnowledgeEntry] = []
    for node in context.nodes:
        if node.label != "术语定义":
            continue
        props = node.properties
        chinese = _text(props.get("中文术语"))
        english = _text(props.get("英文术语"))
        definition = _text(props.get("定义内容"))
        if not (chinese or english) or not definition:
            stats.skip("术语定义知识", "术语名称或定义内容为空")
            continue
        number, _title, _path = context.source_location(node)
        term = chinese or english
        if chinese and english:
            term = f"{chinese}（{english}）"
        content = f"{_location_prefix(context.standard_name(node), number)}将“{term}”定义为：{definition}"
        source_note = _text(props.get("来源说明"))
        if source_note and _comparison_text(source_note) not in _comparison_text(definition):
            content += f"。{source_note}"
        entries.append(
            _make_entry(
                context,
                node,
                "术语定义知识",
                content,
                dedup_text=definition,
                metadata=_metadata(term_number=number, source_note=source_note),
            )
        )
    return entries


def _generate_requirement_entries(
    context: StandardGraphContext,
    stats: ConversionStats,
) -> list[KnowledgeEntry]:
    entries: list[KnowledgeEntry] = []
    for node in context.nodes:
        if node.label != "标准要求":
            continue
        props = node.properties
        content = _text(props.get("要求内容"))
        if len(_comparison_text(content)) < 5:
            stats.skip("标准要求知识", "要求内容为空或不完整")
            continue
        number, title, _path = context.source_location(node)
        sentence = f"{_location_prefix(context.standard_name(node), number, title)}规定：{content}"
        entries.append(
            _make_entry(
                context,
                node,
                "标准要求知识",
                sentence,
                dedup_text=content,
                metadata=_metadata(
                    requirement_type=_text(props.get("要求类型")),
                    constraint_strength=_text(props.get("约束强度")),
                    applicable_object=_text(props.get("适用对象")),
                    applicable_condition=_text(props.get("适用条件")),
                    behavior=_text(props.get("行为描述")),
                    quantitative_feature=_text(props.get("量化特征")),
                    quantitative_condition=_text(props.get("量化条件")),
                ),
            )
        )
    return entries


def _has_quantitative_boundary(value: str) -> bool:
    if not value:
        return False
    plain = value
    for source, target in (
        (r"\leqslant", "≤"),
        (r"\leq", "≤"),
        (r"\geqslant", "≥"),
        (r"\geq", "≥"),
        (r"\lt", "<"),
        (r"\gt", ">"),
        (r"\sim", "~"),
        (r"\times", "×"),
        (r"\cdot", "×"),
        (r"\circ", "°"),
    ):
        plain = plain.replace(source, target)
    plain = re.sub(r"\\(?:left|right|mathrm|text|operatorname|tag|quad|qquad)", "", plain)
    plain = re.sub(r"\\[,;! ]", "", plain)
    plain = plain.replace("$", "").replace("{", "").replace("}", "")
    if _QUANTITATIVE_RE.search(plain):
        return True
    has_number = bool(re.search(r"\d", plain))
    has_relation = bool(
        re.search(
            r"[<>≤≥=±~～×]|不(?:少|小|多|大|超过|低|高)于|超过|少于|大于|小于|高于|低于|至少|至多|以上|以下|以内",
            plain,
        )
    )
    has_unit = bool(
        re.search(
            r"(?:%|％|mm|cm|km|μm|nm|mg|kg|mL|℃|°C|Va\.c\.|Vd\.c\.|V|A|Hz|h-?1)\b",
            plain,
            re.IGNORECASE,
        )
    )
    return has_number and (has_relation or has_unit)


def _generate_indicator_entries(
    context: StandardGraphContext,
    stats: ConversionStats,
) -> list[KnowledgeEntry]:
    entries: list[KnowledgeEntry] = []
    for node in context.nodes:
        if node.label != "指标限值":
            continue
        props = node.properties
        name = _text(props.get("指标名称"))
        limit = _text(props.get("限值"))
        source_text = _text(props.get("来源文本"))
        if not name:
            stats.skip("指标限值知识", "指标名称为空")
            continue
        if not limit or not _has_quantitative_boundary(f"{limit} {source_text}"):
            stats.skip("指标限值知识", "缺少有效数值边界")
            continue
        number, _title, _path = context.source_location(node)
        if source_text and _has_quantitative_boundary(source_text):
            body = source_text
        else:
            applicable_object = _text(props.get("适用对象"))
            condition = _text(props.get("测定条件"))
            unit = _text(props.get("单位"))
            body = f"{f'在{condition}下，' if condition else ''}{applicable_object}{name}{limit}"
            if unit and unit not in limit:
                body += f"，单位为{unit}"
        entries.append(
            _make_entry(
                context,
                node,
                "指标限值知识",
                f"{_location_prefix(context.standard_name(node), number)}规定：{body}",
                dedup_text=body,
                metadata=_metadata(
                    indicator_name=name,
                    limit=limit,
                    unit=_text(props.get("单位")),
                    applicable_object=_text(props.get("适用对象")),
                    measurement_condition=_text(props.get("测定条件")),
                ),
            )
        )
    return entries


def _is_valid_test_method(node: NodeInfo) -> bool:
    props = node.properties
    name = _text(props.get("方法名称"))
    content = _text(props.get("方法内容"))
    applicable_object = _text(props.get("适用对象"))
    reference = _text(props.get("引用标准"))
    combined = f"{name} {content} {reference}"
    if not name or not content:
        return False
    if not _METHOD_ACTION_RE.search(combined):
        return False
    if _INSTALL_MONITOR_RE.search(content) and not _STRONG_METHOD_ACTION_RE.search(content):
        return False
    if not applicable_object and not reference and len(_comparison_text(content)) < 10:
        return False
    return True


def _generate_method_entries(
    context: StandardGraphContext,
    stats: ConversionStats,
) -> list[KnowledgeEntry]:
    entries: list[KnowledgeEntry] = []
    for node in context.nodes:
        if node.label != "试验检测方法":
            continue
        if ENABLE_SEMANTIC_ROLE_VALIDATION and not _is_valid_test_method(node):
            stats.skip("试验检测方法知识", "不满足检测方法语义")
            continue
        props = node.properties
        method_name = _text(props.get("方法名称"))
        method_content = _text(props.get("方法内容"))
        number, _title, _path = context.source_location(node)
        sentence = (
            f"{_location_prefix(context.standard_name(node), number)}规定的{method_name}为：{method_content}"
        )
        entries.append(
            _make_entry(
                context,
                node,
                "试验检测方法知识",
                sentence,
                dedup_text=method_content,
                metadata=_metadata(
                    method_name=method_name,
                    applicable_object=_text(props.get("适用对象")),
                    operation_steps=_text(props.get("操作步骤")),
                    referenced_standard=_text(props.get("引用标准")),
                ),
            )
        )
    return entries


def _is_meaningful_structure_text(node: NodeInfo) -> bool:
    props = node.properties
    content = _strip_mermaid_resources(_text(props.get("节点内容")))
    title = _text(props.get("节点标题"))
    node_type = _text(props.get("节点类型"))
    if not content or len(_comparison_text(content)) < 6:
        return False
    if node_type in {"目录节点", "封面节点", "表格节点"}:
        return False
    if title and _comparison_text(content) == _comparison_text(title):
        return False
    if _TABLE_RE.search(content) or _ISOLATED_POINTER_RE.fullmatch(content):
        return False
    if re.fullmatch(r"(?:第?\s*\d+\s*页|\d+)", content):
        return False
    return True


def _generate_structure_entries(
    context: StandardGraphContext,
    valid_semantic_entries: list[KnowledgeEntry],
    stats: ConversionStats,
) -> list[KnowledgeEntry]:
    if not ENABLE_STRUCTURE_TEXT_FALLBACK:
        return []
    entries: list[KnowledgeEntry] = []
    covered_structure_keys = {
        entry.structure_key
        for entry in valid_semantic_entries
        if entry.knowledge_type in {
            "术语定义知识",
            "标准要求知识",
            "指标限值知识",
            "试验检测方法知识",
        }
        and entry.structure_key
    }
    for node in context.nodes:
        if context.effective_label(node) != "标准结构节点":
            continue
        if node.key in covered_structure_keys:
            stats.skip("结构节点原文知识", "已有高优先级语义实体")
            continue
        if not _is_meaningful_structure_text(node):
            stats.skip("结构节点原文知识", "内容为空、不完整或属于资源正文")
            continue
        props = node.properties
        content = _strip_mermaid_resources(_text(props.get("节点内容")))
        number = _text(props.get("节点编号"))
        title = _text(props.get("节点标题"))
        sentence = f"{_location_prefix(context.standard_name(node), number, title)}规定：{content}"
        entries.append(
            _make_entry(
                context,
                node,
                "结构节点原文知识",
                sentence,
                dedup_text=content,
                metadata=_metadata(node_type=_text(props.get("节点类型"))),
            )
        )
    return entries


def _generate_table_entries(
    context: StandardGraphContext,
    stats: ConversionStats,
) -> list[KnowledgeEntry]:
    if not ENABLE_TABLE_INDEX_KNOWLEDGE:
        return []
    entries: list[KnowledgeEntry] = []
    seen_resources: set[str] = set()
    for node in context.nodes:
        if node.label != "表格":
            continue
        props = node.properties
        description = _text(props.get("表格描述"))
        if not description:
            stats.skip("表格索引知识", "表格描述为空")
            continue
        table_id = _text(props.get("table_id"))
        number = _text(props.get("表号"))
        title = _text(props.get("表题"))
        location_number, _location_title, _path = context.source_location(node)
        resource_key = table_id or "\x1f".join(
            (node.filename, location_number, number, title, _normalize_text(description))
        )
        if resource_key in seen_resources:
            stats.skip("表格索引知识", "重复表格资源")
            continue
        seen_resources.add(resource_key)
        prefix = _location_prefix(context.standard_name(node), location_number)
        locator = number or "表格"
        if title and _comparison_text(title) != _comparison_text(description):
            locator += f"“{title}”"
        sentence = f"{prefix}中的{locator}的表格描述为：{description}"
        entries.append(
            _make_entry(
                context,
                node,
                "表格索引知识",
                sentence,
                dedup_text=description,
                resource_key=resource_key,
                metadata=_metadata(table_id=table_id, table_number=number, table_title=title),
            )
        )
    return entries


def _generate_flowchart_entries(
    context: StandardGraphContext,
    stats: ConversionStats,
) -> list[KnowledgeEntry]:
    if not ENABLE_FLOWCHART_DESCRIPTION_KNOWLEDGE:
        return []
    entries: list[KnowledgeEntry] = []
    seen: set[str] = set()
    for node in context.nodes:
        if node.label != "流程图":
            continue
        props = node.properties
        description = _text(props.get("流程图描述"))
        if not description:
            stats.skip("流程图描述知识", "流程图描述为空")
            continue
        number = _text(props.get("流程图编号"))
        title = _text(props.get("流程图标题"))
        location_number, _location_title, _path = context.source_location(node)
        resource_key = "\x1f".join(
            (node.filename, location_number, number, title, _normalize_text(description))
        )
        if resource_key in seen:
            stats.skip("流程图描述知识", "重复流程图资源")
            continue
        seen.add(resource_key)
        prefix = _location_prefix(context.standard_name(node), location_number)
        locator = number or "流程图"
        if title:
            locator += f"“{title}”"
        sentence = f"{prefix}中的{locator}表示：{description}"
        entries.append(
            _make_entry(
                context,
                node,
                "流程图描述知识",
                sentence,
                dedup_text=description,
                resource_key=resource_key,
                metadata=_metadata(flowchart_number=number, flowchart_title=title),
            )
        )
    return entries


def _reference_target(node: NodeInfo) -> str:
    props = node.properties
    number = _text(props.get("标准编号"))
    name = _text(props.get("标准名称"))
    if name and number and number not in name:
        return f"{number}《{name}》"
    return name or number or node.name


def _reference_explanation(node: NodeInfo, edge: ResolvedEdge) -> str:
    return (
        _text(edge.properties.get("引用说明"))
        or _text(node.properties.get("引用说明"))
        or _text(edge.properties.get("说明"))
    )


def _is_internal_reference(
    context: StandardGraphContext,
    source: NodeInfo,
    target: NodeInfo,
) -> bool:
    if context.effective_label(target) in {"标准结构节点", "术语定义"}:
        return source.filename == target.filename
    props = target.properties
    if _text(props.get("是否内部引用")) == "是" or _text(props.get("是否本标准自身引用")) == "是":
        return True
    target_number = _text(props.get("标准编号"))
    if target_number and _CHAPTER_NUMBER_RE.fullmatch(target_number):
        return True
    standard_number = context.standard_number(source)
    return bool(target_number and standard_number and target_number in standard_number)


def _generate_reference_entries(
    context: StandardGraphContext,
    stats: ConversionStats,
) -> list[KnowledgeEntry]:
    entries: list[KnowledgeEntry] = []
    for edge in context.edges:
        if edge.predicate in ORGANIZATION_RELATIONSHIPS:
            continue
        start_label = context.effective_label(edge.start)
        end_label = context.effective_label(edge.end)
        if edge.predicate == "依据" and start_label == "标准文件" and end_label == "标准依据":
            target = _reference_target(edge.end)
            if not target:
                stats.skip("引用知识", "标准依据目标为空")
                continue
            basis_type = _text(edge.end.properties.get("依据类型"))
            if basis_type == "参考":
                stats.skip("引用知识", "文件级参考文献不生成知识")
                continue
            verb = {
                "采用": "采用",
                "代替": "代替",
                "引用": "引用",
                "制定依据": "依据",
                "其他": "涉及",
            }.get(basis_type, "涉及")
            entries.append(
                _make_entry(
                    context,
                    edge.start,
                    "引用知识",
                    f"{context.standard_name(edge.start)}{verb}{target}",
                    dedup_text=f"{verb}{target}",
                    metadata=_metadata(reference_scope="文件级", reference_type=basis_type, target=target),
                )
            )
            continue
        if edge.predicate != "引用":
            stats.relationship_issue("非组织且非依据引用关系")
            continue
        if start_label not in {"标准结构节点", "标准要求", "试验检测方法"}:
            stats.skip("引用知识", "引用来源类型不支持")
            continue
        target = _reference_target(edge.end)
        explanation = _reference_explanation(edge.end, edge)
        if not target or target in {"本标准", "见下文"}:
            stats.skip("引用知识", "引用目标不明确")
            continue
        if not explanation:
            stats.skip("引用知识", "缺少引用说明")
            continue
        number, title, _path = context.source_location(edge.start)
        prefix = _location_prefix(context.standard_name(edge.start), number, title)
        internal = _is_internal_reference(context, edge.start, edge.end)
        if internal:
            content = f"{prefix}明确引用本标准{target}，引用说明为：{explanation}"
        else:
            content = f"{prefix}规定：{explanation}"
        entries.append(
            _make_entry(
                context,
                edge.start,
                "引用知识",
                content,
                dedup_text=f"{'内部' if internal else '外部'}引用{target}{explanation}",
                metadata=_metadata(
                    reference_scope="内部" if internal else "外部",
                    target=target,
                    reference_explanation=explanation,
                ),
            )
        )
    return entries


def _entry_priority(entry: KnowledgeEntry) -> int:
    return PRIMARY_TYPE_PRIORITY.get(entry.knowledge_type, 99)


def _deduplicate_entries(
    entries: Iterable[KnowledgeEntry],
    stats: ConversionStats,
) -> list[KnowledgeEntry]:
    selected: dict[tuple[str, str, str], KnowledgeEntry] = {}
    resource_selected: dict[str, KnowledgeEntry] = {}
    for entry in entries:
        normalized_body = _comparison_text(entry.dedup_text or entry.content)
        if not normalized_body:
            stats.skip(entry.knowledge_type, "正文为空")
            continue
        if entry.resource_key:
            resource_key = f"{entry.knowledge_type}\x1f{entry.source_file}\x1f{entry.resource_key}"
            if resource_key in resource_selected:
                stats.skip(entry.knowledge_type, "重复资源知识")
                continue
            resource_selected[resource_key] = entry
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
        entry.knowledge_id = f"ns_{hashlib.sha256(raw_id.encode('utf-8')).hexdigest()[:24]}"
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


def convert_national_standard_json_to_text(
    graph_type: str = "国家标准",
    *,
    input_dir: Path | None = None,
    output_dir: Path | None = None,
) -> dict[str, Any]:
    """将国家标准 Neo4j 导出 JSON 转换为自然语言知识条目。

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
    context = StandardGraphContext(node_records, edge_records, stats)

    candidates: list[KnowledgeEntry] = []
    candidates.extend(_generate_standard_file_entries(context, stats))
    if ENABLE_STANDARD_SEMANTIC_KNOWLEDGE:
        semantic_entries: list[KnowledgeEntry] = []
        semantic_entries.extend(_generate_term_entries(context, stats))
        semantic_entries.extend(_generate_requirement_entries(context, stats))
        semantic_entries.extend(_generate_indicator_entries(context, stats))
        semantic_entries.extend(_generate_method_entries(context, stats))
        candidates.extend(semantic_entries)
        candidates.extend(_generate_structure_entries(context, semantic_entries, stats))
    candidates.extend(_generate_table_entries(context, stats))
    candidates.extend(_generate_flowchart_entries(context, stats))
    candidates.extend(_generate_reference_entries(context, stats))

    entries = _deduplicate_entries(candidates, stats)
    txt_path, jsonl_path, csv_path, type_counts, file_counts = _write_outputs(
        entries,
        stats,
        graph_type,
        output_root,
    )

    logger.info("国家标准自然语言知识总条数：%s", len(entries))
    logger.info("国家标准知识来源文件数：%s", len(file_counts))
    logger.info("国家标准知识类型统计：%s", type_counts)
    logger.info("国家标准知识跳过原因统计：%s", dict(stats.skip_reasons))
    logger.info("国家标准关系问题统计：%s", dict(stats.relationship_issues))
    logger.info("国家标准来源语境问题统计：%s", dict(stats.source_context_issues))
    logger.info("国家标准知识 TXT：%s", txt_path)
    logger.info("国家标准知识 JSONL：%s", jsonl_path)
    logger.info("国家标准知识统计 CSV：%s", csv_path)

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
    "convert_national_standard_json_to_text",
]
