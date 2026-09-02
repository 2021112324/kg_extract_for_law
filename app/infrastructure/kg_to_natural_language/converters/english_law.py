"""English-law Neo4j graph data to English natural-language knowledge.

The converter reads exported ``node.json`` and ``edge.json`` files and writes
line-oriented TXT, auditable JSONL, and CSV statistics. It is intentionally
separate from the Chinese-law and national-standard converters.
"""

from __future__ import annotations

import ast
import csv
import hashlib
import importlib
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
from .source_context import add_english_source_context, clean_source_name

logger = logging.getLogger(__name__)

_TEMP_DIR = Path(__file__).resolve().parents[1] / "temp"

BUSINESS_LABELS = {
    "LegalDocument",
    "LegalBasis",
    "LegalProvision",
    "ProvisionClause",
    "ProvisionTextParagraph",
    "Citation",
}

LEGAL_TEXT_LABELS = {
    "LegalProvision",
    "ProvisionClause",
    "ProvisionTextParagraph",
}

LEGAL_TEXT_PROPERTIES = {
    "LegalProvision": "provision_content",
    "ProvisionClause": "unit_content",
    "ProvisionTextParagraph": "unit_content",
}

LEGAL_TEXT_PRIORITY = {
    "LegalProvision": 1,
    "ProvisionClause": 2,
    "ProvisionTextParagraph": 3,
}

VALID_TRIPLES = {
    ("LegalDocument", "CONTAINS", "LegalProvision"),
    ("LegalProvision", "CONTAINS", "ProvisionClause"),
    ("ProvisionClause", "CONTAINS", "ProvisionTextParagraph"),
    ("ProvisionTextParagraph", "CONTAINS", "ProvisionTextParagraph"),
    ("LegalDocument", "BASED_ON", "LegalBasis"),
    ("ProvisionTextParagraph", "CITES", "Citation"),
}

ALLOWED_RISK_TYPES = {
    "产品法律风险",
    "供应链合规风险",
    "劳动用工法律合规风险",
    "企业关联方合规风险",
    "企业国际化经营合规风险",
    "企业信用风险",
}

SCHEMA_MODULES = (
    "app.infrastructure.information_extraction.en_law_v1.prompt.schema",
    "app.infrastructure.information_extraction.en_law_v2.prompt.schema",
    "app.infrastructure.information_extraction.en_law_v3.prompt.schema",
)

INFRASTRUCTURE_PROPERTIES = {
    "id",
    "name",
    "label",
    "filename",
    "graph_tag",
    "graph_level",
}

# These fields are produced by deterministic splitters or Neo4j export
# normalization rather than declared as model-generated Schema properties.
SOURCE_PROPERTY_OVERRIDES = {
    "LegalDocument": {
        "document_format",
        "source_filename",
        "risk_types",
    },
    "LegalBasis": {"instrument", "article"},
    "LegalProvision": {
        "provision_content",
        "title",
        "chapter",
        "part",
        "section",
    },
    "ProvisionClause": {
        "unit_number",
        "unit_content",
        "clause_index",
        "source_article_number",
        "explicit_boundary",
    },
    "ProvisionTextParagraph": {
        "unit_number",
        "unit_content",
        "legal_function",
        "clause_purpose",
        "main_subject",
        "main_action",
        "main_object",
        "has_condition",
        "has_exception",
    },
    "Citation": {
        "resolution_status",
        "citation_relation_raw",
    },
}

LEGAL_TEXT_METADATA_FIELDS = (
    "provision_heading",
    "core_topic",
    "scope_of_effect",
    "unit_level",
    "unit_purpose",
    "clause_summary",
    "clause_purpose",
    "main_subject",
    "main_action",
    "main_object",
    "legal_function",
    "function_type",
    "applicable_subject",
    "responsibility_role",
    "conduct_description",
    "condition",
    "legal_consequence",
    "exception",
    "time_element",
    "other_information",
    "has_quantitative_detail",
    "has_exception",
    "has_condition",
    "applicable_industry",
    "compliance_domains",
    "economic_industries",
)

_CHINESE_RE = re.compile(r"[\u3400-\u4dbf\u4e00-\u9fff]")
_MARKDOWN_HEADING_RE = re.compile(r"(?m)^\s{0,3}#{1,6}\s*")
_INTERNAL_REFERENCE_RE = re.compile(
    r"\b(?:this|the present)\s+(?:regulation|directive|act|section|article|paragraph|subsection)\b",
    re.IGNORECASE,
)
_PROPERTY_RE = re.compile(r"^-\s+([A-Za-z_][A-Za-z0-9_]*)\s*:")


@dataclass
class NodeInfo:
    """Normalized exported Neo4j node."""

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
    """Neo4j relationship whose endpoints have been resolved by identity."""

    key: str
    predicate: str
    start: NodeInfo
    end: NodeInfo
    properties: dict[str, Any]


@dataclass
class KnowledgeEntry:
    """English knowledge entry and its auditable source data."""

    knowledge_type: str
    content: str
    source_file: str
    document_name: str
    document_format: str
    source_node_id: str = ""
    source_node_type: str = ""
    source_location: str = ""
    risk_types: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    related_node_ids: list[str] = field(default_factory=list)
    related_edge_ids: list[str] = field(default_factory=list)
    validation_issues: list[str] = field(default_factory=list)
    knowledge_id: str = ""
    source_node_key: str = field(default="", repr=False)
    dedup_text: str = field(default="", repr=False)

    def to_dict(self) -> dict[str, Any]:
        return {
            "knowledge_id": self.knowledge_id,
            "knowledge_type": self.knowledge_type,
            "content": self.content,
            "source_file": self.source_file,
            "document_name": self.document_name,
            "document_format": self.document_format,
            "source_node_id": self.source_node_id,
            "source_node_type": self.source_node_type,
            "source_location": self.source_location,
            "risk_types": self.risk_types,
            "metadata": self.metadata,
            "related_node_ids": self.related_node_ids,
            "related_edge_ids": self.related_edge_ids,
            "validation_issues": self.validation_issues,
        }


@dataclass
class ConversionStats:
    """Conversion counters used in logs and CSV review output."""

    candidate_counts: Counter[str] = field(default_factory=Counter)
    source_level_candidates: Counter[str] = field(default_factory=Counter)
    skip_reasons: Counter[str] = field(default_factory=Counter)
    relationship_issues: Counter[str] = field(default_factory=Counter)
    filtered_properties: Counter[str] = field(default_factory=Counter)
    validation_issues: Counter[str] = field(default_factory=Counter)

    def candidate(self, knowledge_type: str, source_label: str = "") -> None:
        self.candidate_counts[knowledge_type] += 1
        if source_label in LEGAL_TEXT_LABELS:
            self.source_level_candidates[source_label] += 1

    def skip(self, category: str, reason: str, count: int = 1) -> None:
        self.skip_reasons[f"{category}:{reason}"] += count

    def relationship_issue(self, reason: str, count: int = 1) -> None:
        self.relationship_issues[reason] += count

    def validation_issue(self, reason: str, count: int = 1) -> None:
        self.validation_issues[reason] += count


def _text(value: Any) -> str:
    if is_empty_value(value):
        return ""
    if isinstance(value, (list, tuple, set)):
        return ", ".join(_text(item) for item in value if _text(item))
    return value_to_text(value).strip()


def _as_list(value: Any) -> list[str]:
    if is_empty_value(value):
        return []
    if isinstance(value, (list, tuple, set)):
        return [_text(item) for item in value if _text(item)]
    text = _text(value)
    if not text:
        return []
    return [part.strip() for part in re.split(r"[,，、;；\n]+", text) if part.strip()]


def _clean_source_text(value: Any) -> str:
    text = _text(value)
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()


def _normalize_text(value: Any) -> str:
    text = _clean_source_text(value)
    text = _MARKDOWN_HEADING_RE.sub("", text)
    return text.strip()


def _sentence(value: str) -> str:
    text = value.strip()
    if not text or text.endswith((".", "?", "!", ":", ";")):
        return text
    return f"{text}."


def _contains_chinese(value: str) -> bool:
    return bool(_CHINESE_RE.search(value))


def _node_id(node: NodeInfo) -> str:
    return node.business_id or node.element_id or str(node.identity or "")


def _edge_id(edge: ResolvedEdge) -> str:
    return edge.key


def _metadata(**values: Any) -> dict[str, Any]:
    return {key: value for key, value in values.items() if not is_empty_value(value)}


def _article_for(value: str) -> str:
    return "an" if value[:1].lower() in "aeiou" else "a"


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return _text(value).lower() in {"1", "true", "yes", "y"}


def _load_schema_property_whitelist() -> dict[str, set[str]]:
    """Extract allowed entity properties from the three live Schema modules."""

    result: dict[str, set[str]] = defaultdict(set)
    for module_name in SCHEMA_MODULES:
        try:
            module = importlib.import_module(module_name)
        except Exception as exc:  # pragma: no cover - fallback protects runtime packaging
            logger.warning("Unable to load English-law Schema %s: %s", module_name, exc)
            continue
        for name, schema_text in vars(module).items():
            if not name.startswith("schema_") or not isinstance(schema_text, str):
                continue
            current_label = ""
            in_properties = False
            for raw_line in schema_text.splitlines():
                line = raw_line.strip()
                if line.startswith("## "):
                    candidate = line[3:].strip().split()[0]
                    current_label = candidate if candidate in BUSINESS_LABELS else ""
                    in_properties = False
                    continue
                if line == "### Properties":
                    in_properties = bool(current_label)
                    continue
                if line.startswith("### "):
                    in_properties = False
                    continue
                if not current_label or not in_properties:
                    continue
                match = _PROPERTY_RE.match(line)
                if match:
                    result[current_label].add(match.group(1))

    for label in BUSINESS_LABELS:
        result[label].update(INFRASTRUCTURE_PROPERTIES)
        result[label].update(SOURCE_PROPERTY_OVERRIDES.get(label, set()))
    return dict(result)


class EnglishLawGraphContext:
    """English-law node, relationship, hierarchy, and source indexes."""

    def __init__(
        self,
        node_records: list[dict[str, Any]],
        edge_records: list[dict[str, Any]],
        stats: ConversionStats,
    ) -> None:
        self.stats = stats
        self.allowed_properties = _load_schema_property_whitelist()
        self.nodes: list[NodeInfo] = []
        self.nodes_by_key: dict[str, NodeInfo] = {}
        self.identity_index: dict[Any, NodeInfo] = {}
        self.element_id_index: dict[str, NodeInfo] = {}
        self.nodes_by_file: dict[str, list[NodeInfo]] = defaultdict(list)
        self.document_by_file: dict[str, NodeInfo] = {}
        self.edges: list[ResolvedEdge] = []
        self.incoming: dict[str, list[ResolvedEdge]] = defaultdict(list)
        self.outgoing: dict[str, list[ResolvedEdge]] = defaultdict(list)
        self.parent: dict[str, NodeInfo] = {}
        self.parent_edge: dict[str, ResolvedEdge] = {}
        self.children: dict[str, list[NodeInfo]] = defaultdict(list)
        self._parse_nodes(node_records)
        self._parse_edges(edge_records)
        self._build_hierarchy()

    def _parse_nodes(self, records: list[dict[str, Any]]) -> None:
        for index, record in enumerate(records):
            node = extract_node(record)
            if node is None:
                self.stats.skip("node", "missing_n_object")
                continue
            properties = node.get("properties") or {}
            if not isinstance(properties, dict):
                self.stats.skip("node", "properties_not_object")
                continue
            label = _text(properties.get("label"))
            if label not in BUSINESS_LABELS:
                self.stats.skip("node", "unknown_business_type")
                continue
            identity = node.get("identity")
            element_id = _text(node.get("elementId"))
            business_id = _text(properties.get("id"))
            fallback = element_id or str(identity) or f"record-{index}"
            key = element_id or (f"identity:{identity}" if identity is not None else fallback)
            filename = first_filename(properties)
            info = NodeInfo(
                key=key,
                identity=identity,
                element_id=element_id,
                business_id=business_id or fallback,
                name=_text(properties.get("name")),
                label=label,
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
            if label == "LegalDocument" and filename not in self.document_by_file:
                self.document_by_file[filename] = info
            allowed = self.allowed_properties.get(label, INFRASTRUCTURE_PROPERTIES)
            for property_name in properties:
                if property_name not in allowed:
                    self.stats.filtered_properties[f"{label}:{property_name}"] += 1

    def _parse_edges(self, records: list[dict[str, Any]]) -> None:
        seen: set[tuple[str, str, str]] = set()
        for index, record in enumerate(records):
            raw_edge = extract_edge(record)
            if raw_edge is None:
                self.stats.relationship_issue("missing_p_object")
                continue
            predicate = _text(raw_edge.get("type"))
            start = self._find_node(raw_edge.get("start"), raw_edge.get("startNodeElementId"))
            end = self._find_node(raw_edge.get("end"), raw_edge.get("endNodeElementId"))
            if not predicate:
                self.stats.relationship_issue("empty_relationship_type")
                continue
            if start is None or end is None:
                self.stats.relationship_issue("unresolved_endpoint")
                continue
            if start.key == end.key:
                self.stats.relationship_issue("self_loop")
                continue
            triple = (start.label, predicate, end.label)
            if triple not in VALID_TRIPLES:
                self.stats.relationship_issue("invalid_triple")
                continue
            edge_tuple = (start.key, predicate, end.key)
            if edge_tuple in seen:
                self.stats.relationship_issue("duplicate_relationship")
                continue
            seen.add(edge_tuple)
            properties = raw_edge.get("properties") or {}
            edge_key = _text(raw_edge.get("elementId")) or str(raw_edge.get("identity") or index)
            edge = ResolvedEdge(
                key=edge_key,
                predicate=predicate,
                start=start,
                end=end,
                properties=properties if isinstance(properties, dict) else {},
            )
            self.edges.append(edge)
            self.outgoing[start.key].append(edge)
            self.incoming[end.key].append(edge)

    def _find_node(self, identity: Any, element_id: Any) -> NodeInfo | None:
        if identity in self.identity_index:
            return self.identity_index[identity]
        text_element_id = _text(element_id)
        if text_element_id:
            return self.element_id_index.get(text_element_id)
        return None

    def _build_hierarchy(self) -> None:
        for edge in self.edges:
            if edge.predicate != "CONTAINS":
                continue
            existing = self.parent.get(edge.end.key)
            if existing is not None and existing.key != edge.start.key:
                self.stats.relationship_issue("multiple_containment_parents")
                continue
            self.parent[edge.end.key] = edge.start
            self.parent_edge[edge.end.key] = edge
            self.children[edge.start.key].append(edge.end)

    def document_for(self, node: NodeInfo) -> NodeInfo | None:
        if node.label == "LegalDocument":
            return node
        current = node
        visited: set[str] = set()
        while current.key not in visited:
            visited.add(current.key)
            parent = self.parent.get(current.key)
            if parent is None:
                break
            if parent.label == "LegalDocument":
                return parent
            current = parent
        return self.document_by_file.get(node.filename)

    def legal_provision_for(self, node: NodeInfo) -> NodeInfo | None:
        current = node
        visited: set[str] = set()
        while current.key not in visited:
            visited.add(current.key)
            if current.label == "LegalProvision":
                return current
            parent = self.parent.get(current.key)
            if parent is None:
                return None
            current = parent
        return None

    def ancestors(self, node: NodeInfo) -> list[NodeInfo]:
        result: list[NodeInfo] = []
        current = node
        visited: set[str] = set()
        while current.key not in visited:
            visited.add(current.key)
            parent = self.parent.get(current.key)
            if parent is None:
                break
            result.append(parent)
            current = parent
        return result

    def is_ancestor(self, possible_ancestor: NodeInfo, node: NodeInfo) -> bool:
        return any(parent.key == possible_ancestor.key for parent in self.ancestors(node))

    def depth(self, node: NodeInfo) -> int:
        return len(self.ancestors(node))

    def source_location(self, node: NodeInfo) -> str:
        properties = node.properties
        return (
            _text(properties.get("unit_number"))
            or _text(properties.get("provision_number"))
            or node.name
        )

    def document_name(self, node: NodeInfo) -> str:
        document = self.document_for(node)
        if document is None:
            return Path(node.filename).stem
        properties = document.properties
        document_name = _text(properties.get("document_name"))
        short_title = _text(properties.get("short_title"))
        node_name = document.name
        filename = Path(document.filename).stem
        if document_name and document_name.lower() not in {"an act", "act", "a regulation"}:
            return document_name
        if short_title:
            return short_title
        if node_name and node_name.lower() not in {"an act", "act", "a regulation"}:
            return node_name
        return filename or document_name or node_name

    def document_format(self, node: NodeInfo) -> str:
        document = self.document_for(node)
        if document is not None:
            explicit = _text(document.properties.get("document_format")).lower()
            if "format_three" in explicit or "format_3" in explicit:
                return "format_three"
            if "format_two" in explicit or "format_2" in explicit:
                return "format_two"
            if "format_one" in explicit or "format_1" in explicit:
                return "format_one"
        locator = self.source_location(node).strip()
        if locator.startswith("§"):
            return "format_three"
        if re.match(r"^(?:SEC\.|SECTION\b|Sec\.)", locator, re.IGNORECASE):
            return "format_two"
        return "format_one"

    def risk_types(self, node: NodeInfo) -> list[str]:
        values: list[str] = []
        document = self.document_for(node)
        for target in (document, node):
            if target is None:
                continue
            for value in _as_list(target.properties.get("risk_types")):
                if value in ALLOWED_RISK_TYPES and value not in values:
                    values.append(value)
                elif value and value not in ALLOWED_RISK_TYPES:
                    self.stats.validation_issue("invalid_risk_type")
        return values

    def context_node_ids(self, node: NodeInfo) -> list[str]:
        return [_node_id(item) for item in self.ancestors(node) if _node_id(item)]

    def context_edge_ids(self, node: NodeInfo) -> list[str]:
        result: list[str] = []
        current = node
        visited: set[str] = set()
        while current.key not in visited:
            visited.add(current.key)
            edge = self.parent_edge.get(current.key)
            if edge is None:
                break
            result.append(_edge_id(edge))
            current = edge.start
        return result


def _make_entry(
    context: EnglishLawGraphContext,
    node: NodeInfo,
    knowledge_type: str,
    content: str,
    *,
    source_location: str | None = None,
    metadata: dict[str, Any] | None = None,
    related_node_ids: Iterable[str] = (),
    related_edge_ids: Iterable[str] = (),
    validation_issues: Iterable[str] = (),
    dedup_text: str = "",
) -> KnowledgeEntry:
    document_name = context.document_name(node)
    body = _clean_source_text(content)
    rendered_content = body
    if node.label != "LegalDocument":
        rendered_content = add_english_source_context(body, document_name)
        if rendered_content == body and not clean_source_name(document_name):
            context.stats.validation_issue("missing_source_context")
    return KnowledgeEntry(
        knowledge_type=knowledge_type,
        content=rendered_content,
        source_file=node.filename,
        document_name=document_name,
        document_format=context.document_format(node),
        source_node_id=_node_id(node),
        source_node_type=node.label,
        source_location=source_location if source_location is not None else context.source_location(node),
        risk_types=context.risk_types(node),
        metadata=metadata or {},
        related_node_ids=list(dict.fromkeys(item for item in related_node_ids if item)),
        related_edge_ids=list(dict.fromkeys(item for item in related_edge_ids if item)),
        validation_issues=list(dict.fromkeys(item for item in validation_issues if item)),
        source_node_key=node.key,
        dedup_text=dedup_text,
    )


def _generate_document_entries(
    context: EnglishLawGraphContext,
    stats: ConversionStats,
) -> list[KnowledgeEntry]:
    entries: list[KnowledgeEntry] = []
    for document in context.nodes:
        if document.label != "LegalDocument":
            continue
        properties = document.properties
        name = context.document_name(document)
        number = _text(properties.get("document_number"))
        document_type = _text(properties.get("document_type"))
        authority = _text(properties.get("issuing_authority"))

        if name and (number or document_type or authority):
            display = name
            if number and number.lower() not in name.lower():
                display = f"{display} ({number})"
            if document_type:
                content = f"{display} is {_article_for(document_type)} {document_type}"
                if authority:
                    content += f" issued by {authority}"
            else:
                content = f"{display} is issued by {authority}"
            entries.append(_make_entry(context, document, "document_identity", _sentence(content)))
            stats.candidate("document_identity")
        else:
            stats.skip("document_identity", "insufficient_explicit_fields")

        publication_date = _text(properties.get("publication_date"))
        effective_date = _text(properties.get("effective_date"))
        if publication_date or effective_date:
            parts: list[str] = []
            if publication_date:
                verb = "was enacted on" if document_type.lower() in {"act", "public law"} else "was adopted on"
                parts.append(f"{verb} {publication_date}")
            if effective_date:
                prefix = "takes effect" if re.match(r"^(?:on|from|upon|the\b)", effective_date, re.I) else "takes effect on"
                parts.append(f"{prefix} {effective_date}")
            entries.append(
                _make_entry(
                    context,
                    document,
                    "document_effect",
                    _sentence(f"{name} {' and '.join(parts)}"),
                )
            )
            stats.candidate("document_effect")

        subject = _text(properties.get("subject_matter"))
        purpose = _text(properties.get("purpose"))
        normalized_subject = _normalize_text(subject).lower()
        normalized_purpose = _normalize_text(purpose).lower()
        if normalized_subject and normalized_purpose:
            if normalized_subject == normalized_purpose or normalized_purpose in normalized_subject:
                purpose = ""
            elif normalized_subject in normalized_purpose:
                subject = ""
        if subject or purpose:
            if subject and purpose:
                purpose_phrase = purpose if purpose.lower().startswith("to ") else f"to {purpose}"
                content = f"{name} concerns {subject} and aims {purpose_phrase}"
            elif subject:
                content = f"{name} concerns {subject}"
            else:
                purpose_phrase = purpose if purpose.lower().startswith("to ") else f"to {purpose}"
                content = f"{name} aims {purpose_phrase}"
            entries.append(_make_entry(context, document, "document_subject_purpose", _sentence(content)))
            stats.candidate("document_subject_purpose")

        scope = _text(properties.get("scope_of_application"))
        if scope:
            entries.append(
                _make_entry(
                    context,
                    document,
                    "document_scope",
                    _sentence(f"{name} applies to {scope}"),
                )
            )
            stats.candidate("document_scope")
    return entries


def _basis_display(node: NodeInfo) -> str:
    properties = node.properties
    citation_text = _text(properties.get("citation_text"))
    if citation_text:
        return citation_text
    official_title = _text(properties.get("official_title"))
    if official_title:
        return official_title
    instrument = _text(properties.get("instrument"))
    article = _text(properties.get("article"))
    combined = " ".join(part for part in (instrument, article) if part)
    return combined or node.name


def _generate_legal_basis_entries(
    context: EnglishLawGraphContext,
    stats: ConversionStats,
) -> list[KnowledgeEntry]:
    entries: list[KnowledgeEntry] = []
    for edge in context.edges:
        if edge.predicate != "BASED_ON":
            continue
        basis = _basis_display(edge.end)
        document_name = context.document_name(edge.start)
        if not document_name or not basis:
            stats.skip("legal_basis", "empty_source_or_target")
            continue
        metadata = _metadata(basis_role=_text(edge.end.properties.get("basis_role")))
        entries.append(
            _make_entry(
                context,
                edge.start,
                "legal_basis",
                _sentence(f"{document_name} is based on {basis}"),
                source_location="Legal basis",
                metadata=metadata,
                related_node_ids=(_node_id(edge.end),),
                related_edge_ids=(_edge_id(edge),),
                dedup_text=basis,
            )
        )
        stats.candidate("legal_basis")
    return entries


def _parse_quantitative_indicator(
    node: NodeInfo,
    source_text: str,
    stats: ConversionStats,
) -> tuple[dict[str, Any] | None, list[str]]:
    properties = node.properties
    feature = _text(properties.get("quantitative_feature"))
    raw_indicator = properties.get("quantitative_indicator")
    issues: list[str] = []
    if is_empty_value(raw_indicator):
        if feature.lower() == "quantitative":
            issues.append("quantitative_indicator_missing")
            stats.validation_issue("quantitative_indicator_missing")
        return None, issues

    parsed: Any = raw_indicator
    if isinstance(raw_indicator, str):
        try:
            parsed = json.loads(raw_indicator)
        except json.JSONDecodeError:
            try:
                parsed = ast.literal_eval(raw_indicator)
            except (SyntaxError, ValueError):
                parsed = None
    if not isinstance(parsed, dict):
        issues.append("quantitative_indicator_unparseable")
        stats.validation_issue("quantitative_indicator_unparseable")
        return None, issues
    if feature.lower() != "quantitative":
        issues.append("quantitative_feature_mismatch")
        stats.validation_issue("quantitative_feature_mismatch")
        return None, issues

    allowed_keys = {"raw_text", "value_type", "min", "max", "unit", "relation"}
    normalized = {key: parsed.get(key) for key in allowed_keys}
    raw_text = _text(normalized.get("raw_text"))
    if not raw_text:
        issues.append("quantitative_raw_text_missing")
        stats.validation_issue("quantitative_raw_text_missing")
        return None, issues
    if _normalize_text(raw_text).lower() not in _normalize_text(source_text).lower():
        issues.append("quantitative_raw_text_not_in_source")
        stats.validation_issue("quantitative_raw_text_not_in_source")
        return None, issues
    return normalized, issues


def _legal_text_metadata(
    context: EnglishLawGraphContext,
    node: NodeInfo,
    provision: NodeInfo | None,
    source_text: str,
    stats: ConversionStats,
) -> tuple[dict[str, Any], list[str]]:
    metadata: dict[str, Any] = {}
    allowed = context.allowed_properties.get(node.label, INFRASTRUCTURE_PROPERTIES)
    for field_name in LEGAL_TEXT_METADATA_FIELDS:
        if field_name not in allowed:
            continue
        value = node.properties.get(field_name)
        if not is_empty_value(value):
            metadata[field_name] = value

    quantitative_indicator, issues = _parse_quantitative_indicator(node, source_text, stats)
    feature = _text(node.properties.get("quantitative_feature"))
    if feature:
        metadata["quantitative_feature"] = feature
    if quantitative_indicator is not None:
        metadata["quantitative_indicator"] = quantitative_indicator

    if provision is not None:
        for field_name in ("is_amendment_article", "amendment_action", "amendment_target"):
            value = provision.properties.get(field_name)
            if not is_empty_value(value):
                metadata[field_name] = value
    return metadata, issues


def _is_amendment(provision: NodeInfo | None) -> bool:
    if provision is None:
        return False
    properties = provision.properties
    return bool(
        _truthy(properties.get("is_amendment_article"))
        or _text(properties.get("amendment_action"))
        or _text(properties.get("amendment_target"))
    )


def _generate_legal_text_entries(
    context: EnglishLawGraphContext,
    stats: ConversionStats,
) -> list[KnowledgeEntry]:
    entries: list[KnowledgeEntry] = []
    for node in context.nodes:
        if node.label not in LEGAL_TEXT_LABELS:
            continue
        source_property = LEGAL_TEXT_PROPERTIES[node.label]
        source_text = _clean_source_text(node.properties.get(source_property))
        if not source_text:
            stats.skip(node.label, "empty_source_text")
            continue
        if _contains_chinese(source_text):
            stats.skip(node.label, "unexpected_chinese_source_text")
            stats.validation_issue("unexpected_chinese_source_text")
            continue
        document_name = context.document_name(node)
        location = context.source_location(node)
        if not document_name or not location:
            stats.skip(node.label, "missing_document_or_locator")
            continue
        provision = context.legal_provision_for(node)
        knowledge_type = "amendment_provision" if _is_amendment(provision) else "legal_provision"
        metadata, issues = _legal_text_metadata(context, node, provision, source_text, stats)
        if "�" in source_text:
            issues.append("source_contains_replacement_character")
            stats.validation_issue("source_contains_replacement_character")
        content = f"{location} provides: {source_text}"
        entries.append(
            _make_entry(
                context,
                node,
                knowledge_type,
                content,
                metadata=metadata,
                related_node_ids=context.context_node_ids(node),
                related_edge_ids=context.context_edge_ids(node),
                validation_issues=issues,
                dedup_text=source_text,
            )
        )
        stats.candidate(knowledge_type, node.label)
    return entries


def _citation_display(node: NodeInfo) -> str:
    properties = node.properties
    citation_text = _text(properties.get("citation_text"))
    if citation_text:
        return citation_text
    official_title = _text(properties.get("official_title"))
    provision_number = _text(properties.get("provision_number"))
    if official_title and provision_number and provision_number.lower() not in official_title.lower():
        return f"{official_title} {provision_number}"
    return official_title or node.name


def _is_internal_or_unresolved_citation(node: NodeInfo) -> bool:
    properties = node.properties
    if _truthy(properties.get("is_internal_reference")):
        return True
    status = _text(properties.get("resolution_status")).lower()
    if "unresolved" in status or "internal_reference" in status:
        return True
    display = _citation_display(node)
    return bool(_INTERNAL_REFERENCE_RE.search(display))


def _generate_external_citation_entries(
    context: EnglishLawGraphContext,
    stats: ConversionStats,
) -> list[KnowledgeEntry]:
    entries: list[KnowledgeEntry] = []
    for edge in context.edges:
        if edge.predicate != "CITES":
            continue
        citation = edge.end
        if _is_internal_or_unresolved_citation(citation):
            stats.skip("citation", "internal_or_unresolved")
            continue
        target = _citation_display(citation)
        location = context.source_location(edge.start)
        document_name = context.document_name(edge.start)
        if not target or not location or not document_name:
            stats.skip("citation", "empty_source_or_target")
            continue
        purpose = _text(citation.properties.get("citation_purpose"))
        content = f"{location} cites {target}"
        if purpose:
            normalized_purpose = purpose[:1].lower() + purpose[1:]
            if normalized_purpose.startswith("to "):
                content += f" {normalized_purpose}"
            else:
                content += f" in connection with {normalized_purpose}"
        metadata = _metadata(
            citation_type=_text(citation.properties.get("citation_type")),
            citation_relation=_text(citation.properties.get("citation_relation")),
            citation_purpose=purpose,
            official_title=_text(citation.properties.get("official_title")),
            provision_number=_text(citation.properties.get("provision_number")),
        )
        entries.append(
            _make_entry(
                context,
                edge.start,
                "citation",
                _sentence(content),
                metadata=metadata,
                related_node_ids=(_node_id(citation),),
                related_edge_ids=(_edge_id(edge),),
                dedup_text=target,
            )
        )
        stats.candidate("citation")
    return entries


def _is_legal_text_entry(entry: KnowledgeEntry) -> bool:
    return entry.knowledge_type in {"legal_provision", "amendment_provision"}


def _deduplicate_entries(
    entries: Iterable[KnowledgeEntry],
    context: EnglishLawGraphContext,
    stats: ConversionStats,
) -> list[KnowledgeEntry]:
    valid: list[KnowledgeEntry] = []
    for entry in entries:
        if not entry.content:
            stats.skip(entry.knowledge_type, "empty_content")
            continue
        if _contains_chinese(entry.content):
            stats.skip(entry.knowledge_type, "non_english_content")
            stats.validation_issue("non_english_content")
            continue
        valid.append(entry)

    legal_entries = [entry for entry in valid if _is_legal_text_entry(entry)]
    other_entries = [entry for entry in valid if not _is_legal_text_entry(entry)]
    legal_entries.sort(
        key=lambda entry: (
            -context.depth(context.nodes_by_key[entry.source_node_key]),
            -LEGAL_TEXT_PRIORITY.get(entry.source_node_type, 0),
            entry.source_file,
            entry.source_location,
        )
    )

    selected_legal: list[KnowledgeEntry] = []
    legal_by_text: dict[tuple[str, str], list[KnowledgeEntry]] = defaultdict(list)
    for entry in legal_entries:
        normalized = _normalize_text(entry.dedup_text or entry.content)
        key = (entry.source_file, normalized)
        node = context.nodes_by_key[entry.source_node_key]
        duplicate = False
        for existing in legal_by_text.get(key, []):
            existing_node = context.nodes_by_key[existing.source_node_key]
            if (
                context.is_ancestor(node, existing_node)
                or context.is_ancestor(existing_node, node)
                or (
                    entry.source_location == existing.source_location
                    and entry.document_name == existing.document_name
                )
            ):
                duplicate = True
                break
        if duplicate:
            stats.skip(entry.source_node_type, "exact_cross_level_duplicate")
            continue
        legal_by_text[key].append(entry)
        selected_legal.append(entry)

    selected_other: dict[tuple[str, str, str, str], KnowledgeEntry] = {}
    for entry in other_entries:
        key = (
            entry.source_file,
            entry.knowledge_type,
            entry.source_location,
            _normalize_text(entry.content),
        )
        if key in selected_other:
            stats.skip(entry.knowledge_type, "exact_duplicate")
            continue
        selected_other[key] = entry

    result = selected_legal + list(selected_other.values())
    result.sort(
        key=lambda entry: (
            entry.source_file,
            entry.source_location,
            entry.knowledge_type,
            entry.content,
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
        entry.knowledge_id = f"enlaw_{hashlib.sha256(raw_id.encode('utf-8')).hexdigest()[:24]}"
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
    csv_path = output_root / "csv" / f"{safe_name}_knowledge_statistics.csv"

    _atomic_text_write(txt_path, (entry.content for entry in entries))
    _atomic_text_write(
        jsonl_path,
        (json.dumps(entry.to_dict(), ensure_ascii=False) for entry in entries),
    )

    type_counts = Counter(entry.knowledge_type for entry in entries)
    file_counts = Counter(entry.source_file for entry in entries)
    source_type_counts = Counter(entry.source_node_type for entry in entries if _is_legal_text_entry(entry))
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
            writer.writerow(["dimension", "name", "count"])
            writer.writerow(["total", graph_type, len(entries)])
            for name, count in sorted(type_counts.items()):
                writer.writerow(["knowledge_type", name, count])
            for name, count in sorted(source_type_counts.items()):
                writer.writerow(["legal_text_source_type", name, count])
            for name, count in sorted(stats.source_level_candidates.items()):
                writer.writerow(["legal_text_candidate_type", name, count])
            for name, count in sorted(file_counts.items()):
                writer.writerow(["source_file", name, count])
            for name, count in sorted(stats.skip_reasons.items()):
                writer.writerow(["skip_reason", name, count])
            for name, count in sorted(stats.relationship_issues.items()):
                writer.writerow(["relationship_issue", name, count])
            for name, count in sorted(stats.filtered_properties.items()):
                writer.writerow(["filtered_property", name, count])
            for name, count in sorted(stats.validation_issues.items()):
                writer.writerow(["validation_issue", name, count])
        os.replace(temp_name, csv_path)
    except Exception:
        Path(temp_name).unlink(missing_ok=True)
        raise

    return txt_path, jsonl_path, csv_path, dict(type_counts), dict(file_counts)


def convert_english_law_json_to_text(
    graph_type: str = "English Law",
    *,
    input_dir: Path | None = None,
    output_dir: Path | None = None,
) -> dict[str, Any]:
    """Convert exported English-law graph JSON into English knowledge entries.

    Args:
        graph_type: Name used for output files and total statistics.
        input_dir: Directory containing ``node.json`` and ``edge.json``.
        output_dir: Output root containing ``txt``, ``jsonl``, and ``csv``.

    Returns:
        Counts, validation statistics, and output paths.
    """

    input_root = Path(input_dir) if input_dir is not None else _TEMP_DIR / "json"
    output_root = Path(output_dir) if output_dir is not None else _TEMP_DIR
    node_records = load_json_array(input_root / "node.json")
    edge_records = load_json_array(input_root / "edge.json")

    stats = ConversionStats()
    context = EnglishLawGraphContext(node_records, edge_records, stats)
    candidates: list[KnowledgeEntry] = []
    candidates.extend(_generate_document_entries(context, stats))
    candidates.extend(_generate_legal_basis_entries(context, stats))
    candidates.extend(_generate_legal_text_entries(context, stats))
    candidates.extend(_generate_external_citation_entries(context, stats))
    entries = _deduplicate_entries(candidates, context, stats)

    txt_path, jsonl_path, csv_path, type_counts, file_counts = _write_outputs(
        entries,
        stats,
        graph_type,
        output_root,
    )

    final_source_type_counts = dict(
        Counter(entry.source_node_type for entry in entries if _is_legal_text_entry(entry))
    )
    logger.info("English-law natural-language knowledge entries: %s", len(entries))
    logger.info("English-law source files: %s", len(file_counts))
    logger.info("English-law knowledge types: %s", type_counts)
    logger.info("English-law legal-text candidates: %s", dict(stats.source_level_candidates))
    logger.info("English-law final legal-text source types: %s", final_source_type_counts)
    logger.info("English-law skipped candidates: %s", dict(stats.skip_reasons))
    logger.info("English-law relationship issues: %s", dict(stats.relationship_issues))
    logger.info("English-law filtered properties: %s", dict(stats.filtered_properties))
    logger.info("English-law validation issues: %s", dict(stats.validation_issues))
    logger.info("English-law TXT: %s", txt_path)
    logger.info("English-law JSONL: %s", jsonl_path)
    logger.info("English-law CSV: %s", csv_path)

    return {
        "total_entries": len(entries),
        "source_files": len(file_counts),
        "knowledge_type_counts": type_counts,
        "source_file_counts": file_counts,
        "legal_text_candidate_counts": dict(stats.source_level_candidates),
        "legal_text_output_counts": final_source_type_counts,
        "skip_reason_counts": dict(stats.skip_reasons),
        "relationship_issue_counts": dict(stats.relationship_issues),
        "filtered_property_counts": dict(stats.filtered_properties),
        "validation_issue_counts": dict(stats.validation_issues),
        "txt_path": str(txt_path),
        "jsonl_path": str(jsonl_path),
        "csv_path": str(csv_path),
    }


__all__ = [
    "KnowledgeEntry",
    "convert_english_law_json_to_text",
]
