"""In-memory converter adapters; provenance is captured before rendering/deduplication.

``source_node_ids`` contains generating nodes only. Relationship endpoints are
``related_node_ids`` with primary/related roles in ``source_refs``. All physical
IDs and ``source_node_keys`` are snapshot-local, never cross-version identity.
"""

from __future__ import annotations

from collections import Counter
from copy import deepcopy
from typing import Any

from ..converters import chinese_law, compliance_guide, english_law, national_standard
from ..converters.base import build_node_indexes, is_empty_value, value_to_text
from ..converters.source_context import add_chinese_source_context


CONVERTER_TYPES = ("chinese_law", "rule", "national_standard", "guide", "english_law", "paragraph")
_LABELS = {
    "chinese_law": {"法规文件", "法条", "条款单元", "引用依据", "法规依据"},
    "rule": {"法规文件", "法条", "条款单元", "引用依据", "法规依据"},
    "national_standard": ({"标准文件", "标准结构节点", "标准依据", "引用标准"}
                          | national_standard.SEMANTIC_LABELS | national_standard.RESOURCE_LABELS),
    "guide": (compliance_guide.FILE_LABELS | compliance_guide.CONTENT_LABELS
              | compliance_guide.REFERENCE_TARGET_LABELS | compliance_guide.STRUCTURE_LABELS
              | compliance_guide.ARTICLE_LABELS | compliance_guide.QUANTITATIVE_TARGET_LABELS
              | compliance_guide.RESPONSIBILITY_LABELS),
    "english_law": english_law.BUSINESS_LABELS,
    "paragraph": {"来源文件", "段落节点", "段落知识单元", "引用依据"},
}


def _text(value: Any) -> str:
    return "" if is_empty_value(value) else value_to_text(value)


def _unique(values: list) -> list:
    result = []
    for value in values:
        if value not in result:
            result.append(value)
    return result


def _physical_id(node: dict, index: int | None = None) -> str:
    if node.get("elementId"):
        return str(node["elementId"])
    if node.get("identity") is not None:
        return str(node["identity"])
    return _text(node.get("properties", {}).get("id")) or f"record:{index}"


def _ref(node: dict, role: str = "primary", index: int | None = None) -> dict:
    props = node["properties"]
    return {"node_id": _physical_id(node, index), "role": role,
            "business_id": props.get("id"), "identity": node.get("identity"),
            "element_id": node.get("elementId"), "node_type": props.get("label"),
            "node_key": _physical_id(node, index), "key_scope": "snapshot",
            "filenames": deepcopy(props.get("filename", props.get("来源文件", props.get("source_file"))))}


def _validate(converter_type: str, nodes: list, edges: list) -> None:
    if converter_type not in CONVERTER_TYPES:
        raise ValueError(f"Unsupported converter_type: {converter_type!r}; expected {CONVERTER_TYPES}")
    if not isinstance(nodes, list) or not isinstance(edges, list):
        raise ValueError("node_records and edge_records must be lists")
    labels = set()
    for record in nodes:
        node = record.get("n") if isinstance(record, dict) else None
        if not isinstance(node, dict) or not isinstance(node.get("properties"), dict):
            raise ValueError("Expected exported node record with n.properties")
        label = node["properties"].get("label")
        if not isinstance(label, str) or not label:
            raise ValueError("Node requires an explicit business label")
        labels.add(label)
    for record in edges:
        edge = record.get("p") if isinstance(record, dict) else None
        if not isinstance(edge, dict) or not isinstance(edge.get("type"), str) or not edge["type"]:
            raise ValueError("Expected exported relationship record with p.type")
    unknown = labels - _LABELS[converter_type]
    if unknown:
        raise ValueError(f"Unsupported {converter_type} node labels: {sorted(unknown)}")


def _chinese_entries(nodes: list, edges: list) -> list[dict]:
    lines, collector = [], []
    counts, issues = Counter(), Counter()
    identities, elements = build_node_indexes(nodes)
    names = chinese_law.build_regulation_source_names(nodes)
    chinese_law.convert_regulation_documents(nodes, lines, counts, collector=collector)
    chinese_law.convert_clause_units(nodes, lines, counts, names, issues, collector=collector)
    chinese_law.convert_references(edges, identities, elements, lines, counts, names, issues,
                                   collector=collector)
    chinese_law.convert_law_articles(nodes, lines, counts, names, issues, collector=collector)
    for entry in collector:
        entry["source_refs"] = [_ref(n) for n in entry.pop("source_nodes")]
        entry["source_refs"].extend(_ref(n, "related") for n in entry.pop("related_nodes"))
        entry["source_relationship_ids"] = [_relationship_id(e, edges) for e in entry.pop("source_edges")]
    return collector


def _relationship_id(edge: dict, records: list) -> str:
    if edge.get("elementId"):
        return str(edge["elementId"])
    if edge.get("identity") is not None:
        return str(edge["identity"])
    index = next(i for i, record in enumerate(records) if record["p"] is edge)
    return f"relationship:{index}"


def _typed_entries(converter_type: str, nodes: list, edges: list) -> list[dict]:
    module = {"national_standard": national_standard, "guide": compliance_guide,
              "english_law": english_law}[converter_type]
    stats = module.ConversionStats()
    context_class = {"national_standard": national_standard.StandardGraphContext,
                     "guide": compliance_guide.GuideGraphContext,
                     "english_law": english_law.EnglishLawGraphContext}[converter_type]
    context = context_class(nodes, edges, stats)
    candidates = []
    if converter_type == "national_standard":
        candidates.extend(module._generate_standard_file_entries(context, stats))
        if module.ENABLE_STANDARD_SEMANTIC_KNOWLEDGE:
            semantic = []
            for generator in (module._generate_term_entries, module._generate_requirement_entries,
                              module._generate_indicator_entries, module._generate_method_entries):
                semantic.extend(generator(context, stats))
            candidates.extend(semantic)
            candidates.extend(module._generate_structure_entries(context, semantic, stats))
        candidates.extend(module._generate_table_entries(context, stats))
        candidates.extend(module._generate_flowchart_entries(context, stats))
        candidates.extend(module._generate_reference_entries(context, stats))
    elif converter_type == "guide":
        candidates.extend(module._generate_file_entries(context, stats))
        if module.ENABLE_GUIDE_SEMANTIC_KNOWLEDGE:
            semantic = []
            for generator in (module._generate_content_entries, module._generate_quantitative_target_entries,
                              module._generate_responsibility_entries):
                semantic.extend(generator(context, stats))
            candidates.extend(semantic)
            candidates.extend(module._generate_structure_entries(context, semantic, stats))
        candidates.extend(module._generate_article_entries(context, stats))
        candidates.extend(module._generate_reference_entries(context, stats))
    else:
        for generator in (module._generate_document_entries, module._generate_legal_basis_entries,
                          module._generate_legal_text_entries, module._generate_external_citation_entries):
            candidates.extend(generator(context, stats))
    entries = (module._deduplicate_entries(candidates, context, stats) if converter_type == "english_law"
               else module._deduplicate_entries(candidates, stats))
    return [entry.to_dict() for entry in entries]


def _paragraph_entries(nodes: list, edges: list) -> list[dict]:
    """Only extracted paragraph propositions, not arbitrary node properties."""
    raw_nodes = [row["n"] for row in nodes]
    by_identity = {n["identity"]: n for n in raw_nodes if n.get("identity") is not None}
    by_element = {str(n["elementId"]): n for n in raw_nodes if n.get("elementId")}
    parents: dict[str, list[dict]] = {}
    for record in edges:
        edge = record["p"]
        start = by_element.get(str(edge.get("startNodeElementId"))) or by_identity.get(edge.get("start"))
        end = by_element.get(str(edge.get("endNodeElementId"))) or by_identity.get(edge.get("end"))
        if start is None or end is None:
            raise ValueError("Paragraph relationship endpoint is missing")
        triple = (start["properties"]["label"], edge["type"], end["properties"]["label"])
        if triple in {("来源文件", "包含", "段落节点"), ("段落节点", "承载", "段落知识单元")}:
            parents.setdefault(_physical_id(end), []).append(start)
    entries = []
    for index, node in enumerate(raw_nodes):
        props = node["properties"]
        if props["label"] != "段落知识单元":
            continue
        content = _text(props.get("知识内容"))
        if not content:
            continue
        knowledge_type = _text(props.get("知识类型"))
        if knowledge_type not in {"规范要求", "指导建议", "概念定义", "事实陈述", "观点转述"}:
            raise ValueError(f"Unsupported paragraph knowledge type: {knowledge_type!r}")
        paragraphs = parents.get(_physical_id(node), [])
        documents = _unique([d for p in paragraphs for d in parents.get(_physical_id(p), [])])
        filenames = props.get("filename")
        if not documents and filenames:
            names = filenames if isinstance(filenames, list) else [filenames]
            documents = [n for n in raw_nodes if n["properties"]["label"] == "来源文件"
                         and set(names).intersection(_filenames(n["properties"]))]
        nature = _unique([_text(d["properties"].get("来源性质")) for d in documents])
        usage = _text(props.get("使用定位"))
        if usage not in {"规范知识", "参考知识", ""}:
            raise ValueError(f"Unsupported paragraph usage: {usage!r}")
        reference = knowledge_type in {"观点转述", "事实陈述"} or nature != ["正式文件"]
        usage = "参考知识" if reference else usage or "参考知识"
        attribution = _text(props.get("归属说明"))
        if attribution and attribution not in content:
            content = f"{attribution}：{content}"
        titles = _unique([_text(d["properties"].get("文件名称")) or _text(d["properties"].get("name"))
                          for d in documents])
        source_name = titles[0] if len(titles) == 1 else ""
        if source_name:
            content = add_chinese_source_context(content, source_name)
        location = _text(props.get("目标ID"))
        if not location:
            location = " / ".join(
                f"段落{p['properties']['顺序']}" if p["properties"].get("顺序") is not None
                else _text(p["properties"].get("name")) for p in paragraphs
            )
        paragraph_locations = [{key: deepcopy(p["properties"].get(key)) for key in
                                ("顺序", "所属标题", "原文范围")} for p in paragraphs]
        entries.append({"content": content, "knowledge_type": knowledge_type,
                        "semantic_types": [knowledge_type],
                        "source_refs": [dict(_ref(node, index=index), source_location=location)],
                        "source_relationship_ids": [], "source_location": location,
                        "metadata": {"使用定位": usage, "知识类型": knowledge_type,
                                     "归属说明": props.get("归属说明"), "source_natures": nature,
                                     "paragraph_locations": paragraph_locations,
                                     "原文依据": deepcopy(props.get("原文依据")),
                                     "上下文依据": deepcopy(props.get("上下文依据")),
                                     "source_usage_raw": props.get("使用定位")}})
    return entries


def _filenames(props: dict) -> list[str]:
    value = props.get("filename", props.get("来源文件", props.get("source_file", [])))
    return [value] if isinstance(value, str) else list(value or [])


def generate_entries(converter_type: str, node_records: list, edge_records: list,
                     graph_label: str) -> list[dict]:
    """Reuse converter rules without writing TXT/JSONL or connecting a database.

    Physical references are preferred to avoid ambiguous duplicate business IDs;
    each reference retains the original business/element/numeric ID mapping.
    ``source_node_keys_scope`` explicitly limits converter keys to the snapshot.
    """
    _validate(converter_type, node_records, edge_records)
    if converter_type in {"chinese_law", "rule"}:
        entries = _chinese_entries(node_records, edge_records)
    elif converter_type == "paragraph":
        entries = _paragraph_entries(node_records, edge_records)
    else:
        entries = _typed_entries(converter_type, node_records, edge_records)
    raw_nodes = [row["n"] for row in node_records]
    identities = {n["identity"]: (i, n) for i, n in enumerate(raw_nodes) if n.get("identity") is not None}
    elements = {str(n["elementId"]): (i, n) for i, n in enumerate(raw_nodes) if n.get("elementId")}
    businesses: dict[str, list[tuple[int, dict]]] = {}
    for i, node in enumerate(raw_nodes):
        businesses.setdefault(_text(node["properties"].get("id")), []).append((i, node))
    for entry in entries:
        refs = []
        for old_ref in entry.get("source_refs", []):
            found = elements.get(str(old_ref.get("element_id"))) or identities.get(old_ref.get("identity"))
            if found is None:
                matches = businesses.get(_text(old_ref.get("business_id") or old_ref.get("node_id")), [])
                found = matches[0] if len(matches) == 1 else None
            if found is None:
                raise ValueError(f"Missing or ambiguous generating node: {old_ref!r}")
            index, node = found
            ref = _ref(node, old_ref.get("role", "primary"), index)
            if old_ref.get("source_location"):
                ref["source_location"] = old_ref["source_location"]
            refs.append(ref)
        primary = [ref for ref in refs if ref["role"] == "primary"]
        if not primary:
            raise ValueError("Knowledge entry has no generating node provenance")
        entry["source_refs"] = _unique(refs)
        entry["source_node_ids"] = _unique([ref["node_id"] for ref in primary])
        entry["source_node_keys"] = _unique([ref["node_key"] for ref in primary])
        entry["source_node_keys_scope"] = "snapshot"
        related = [ref["node_id"] for ref in refs if ref["role"] == "related"]
        if not entry.get("metadata", {}).get("relationship"):
            for legacy_id in entry.get("related_node_ids", []):
                matches = businesses.get(str(legacy_id), [])
                if len(matches) == 1:
                    index, node = matches[0]
                    related.append(_physical_id(node, index))
                else:
                    related.append(str(legacy_id))
        entry["related_node_ids"] = _unique(related)
        entry["source_relationship_ids"] = _unique(entry.get("source_relationship_ids", []))
        entry.setdefault("semantic_types", [entry["knowledge_type"]])
        entry.setdefault("source_location", "")
        metadata = entry.setdefault("metadata", {})
        metadata.update(converter_type=converter_type, graph_label=graph_label,
                        source_node_keys_scope="snapshot")
        metadata["source_filenames"] = _unique([name for ref in primary
                                               for name in (ref["filenames"] if isinstance(ref["filenames"], list)
                                                            else [ref["filenames"]]) if name])
        if converter_type == "rule":
            metadata["rule_source_context"] = [
                {"node_id": _physical_id(n), **{key: deepcopy(n["properties"][key])
                 for key in ("条", "单元编号", "上级上下文", "来源编号列表", "上下文依据") if key in n["properties"]}}
                for n in raw_nodes if _physical_id(n) in entry["source_node_ids"]
            ]
    return entries


__all__ = ["CONVERTER_TYPES", "generate_entries"]
