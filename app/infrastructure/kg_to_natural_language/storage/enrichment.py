"""Source-bounded metadata enrichment. No model, graph writes or guessed mappings."""

import json
import re
from collections import defaultdict, deque
from datetime import datetime

from .records import (INDUSTRIES, RISK_TYPES, KnowledgeStorageError, canonical, digest,
                      normalized_text, now_cn, record_digest, validate_indicator, validate_record)

FILE_TYPES = {"法规文件", "标准文件", "合规指引文件", "LegalDocument", "来源文件"}
ARTICLE_TYPES = {"法条", "LegalProvision"}
CONTAINER_TYPES = FILE_TYPES | ARTICLE_TYPES | {"标准结构节点", "指引结构节点", "段落节点",
                                                "ProvisionClause", "ProvisionTextParagraph", "条款单元"}
OWNERSHIP = {"包含", "CONTAINS", "提出", "承载", "规定", "定义", "设定目标", "由其负责"}


def strings(value):
    if value is None:
        return []
    if isinstance(value, str):
        return [value.strip()] if value.strip() else []
    if not isinstance(value, list) or any(not isinstance(v, str) for v in value):
        raise KnowledgeStorageError("Expected string or string array")
    return sorted(set(v.strip() for v in value if v.strip()))


def node_filenames(props):
    for field in ("filename", "source_filename", "source_file", "来源文件"):
        if field in props and props[field]:
            return strings(props[field])
    return []


def first(props, keys):
    for key in keys:
        value = props.get(key)
        if value is not None and value != "":
            return value, key
    return None, None


def parse_date(value):
    if not isinstance(value, str):
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y年%m月%d日", "%d %B %Y", "%B %d, %Y"):
        try:
            return datetime.strptime(value.strip(), fmt).date().isoformat()
        except ValueError:
            pass
    return None


class GraphContext:
    def __init__(self, snapshot):
        self.snapshot = snapshot
        self.nodes, self.aliases, self.parents = {}, defaultdict(set), defaultdict(set)
        self.identity, self.elements, self.edges = {}, {}, {}
        self.edge_aliases = defaultdict(set)
        self.ownership_diagnostics = []
        self.by_filename = defaultdict(set)
        for i, row in enumerate(snapshot["nodes"]):
            n = row.get("n") if isinstance(row, dict) else None
            if not isinstance(n, dict) or not isinstance(n.get("properties"), dict):
                raise KnowledgeStorageError("Invalid exported node structure")
            p = n["properties"]
            if n.get("labels") and snapshot["source"]["graph_label"] not in n["labels"]:
                raise KnowledgeStorageError("Node labels do not match the explicitly configured source graph")
            if not isinstance(p.get("label"), str) or not p["label"]:
                raise KnowledgeStorageError("Node lacks explicit business label")
            key = str(n.get("elementId") or ("identity:" + str(n["identity"]) if n.get("identity") is not None
                                             else p.get("id") or "record:" + str(i)))
            if key in self.nodes:
                raise KnowledgeStorageError("Duplicate graph node physical identity")
            self.nodes[key] = n
            for alias in (key, p.get("id"), n.get("elementId"), str(n.get("identity")) if n.get("identity") is not None else None):
                if alias is not None:
                    self.aliases[str(alias)].add(key)
            if n.get("identity") is not None:
                if n["identity"] in self.identity:
                    raise KnowledgeStorageError("Duplicate numeric node identity")
                self.identity[n["identity"]] = key
            if n.get("elementId"):
                self.elements[n["elementId"]] = key
            if p["label"] in FILE_TYPES:
                for name in node_filenames(p):
                    self.by_filename[name].add(key)
        for i, row in enumerate(snapshot["edges"]):
            e = row.get("p") if isinstance(row, dict) else None
            if not isinstance(e, dict):
                raise KnowledgeStorageError("Invalid exported relationship structure")
            a = self.elements.get(e.get("startNodeElementId")) or self.identity.get(e.get("start"))
            b = self.elements.get(e.get("endNodeElementId")) or self.identity.get(e.get("end"))
            if a is None or b is None:
                raise KnowledgeStorageError("Relationship endpoint is missing from the source snapshot")
            key = str(e.get("elementId") or (e["identity"] if e.get("identity") is not None else "relationship:" + str(i)))
            if key in self.edges:
                raise KnowledgeStorageError("Duplicate relationship identity")
            self.edges[key] = (a, b, e)
            for alias in (key, str(e["identity"]) if e.get("identity") is not None else None,
                          e.get("properties", {}).get("id")):
                if alias is not None:
                    self.edge_aliases[str(alias)].add(key)
            if e.get("type") in OWNERSHIP and self.props(a)["label"] in CONTAINER_TYPES:
                if a == b:
                    # Keep the original edge, but a node cannot own itself.
                    self.ownership_diagnostics.append({
                        "code": "self_referential_ownership", "edge_id": key,
                        "node_id": a, "node_type": self.props(a)["label"],
                        "node_name": self.props(a).get("name"),
                        "filenames": node_filenames(self.props(a)),
                        "relationship_type": e.get("type"),
                        "action": "excluded_from_ownership_traversal_only",
                    })
                else:
                    self.parents[b].add(a)

    def props(self, key):
        return self.nodes[key]["properties"]

    def resolve_ref(self, ref):
        if isinstance(ref, dict):
            ref = ref.get("node_id") or ref.get("key") or ref.get("source_node_id")
        if ref is None:
            raise KnowledgeStorageError("Missing generating node reference")
        ref = str(ref)
        found = self.aliases.get(ref, set())
        if not found and "\x1f" in ref:
            found = self.aliases.get(ref.rsplit("\x1f", 1)[-1], set())
        if len(found) != 1:
            raise KnowledgeStorageError("Generating node reference is missing or ambiguous")
        return next(iter(found))

    def ancestors(self, key):
        result, visited, pending = [], {key}, deque((p, 1) for p in sorted(self.parents[key]))
        while pending:
            parent, depth = pending.popleft()
            if parent == key:
                raise KnowledgeStorageError("Cyclic owning relationship")
            if parent in visited:
                continue
            visited.add(parent)
            result.append((parent, depth))
            pending.extend((p, depth + 1) for p in sorted(self.parents[parent]))
        return result

    def files_for(self, key):
        if self.props(key)["label"] in FILE_TYPES:
            return [key], False
        found = sorted({p for p, _ in self.ancestors(key) if self.props(p)["label"] in FILE_TYPES})
        if found:
            return found, len(found) > 1
        candidates = set()
        ambiguous = False
        for name in node_filenames(self.props(key)):
            matches = self.by_filename.get(name, set())
            ambiguous |= len(matches) > 1
            candidates.update(matches)
        return sorted(candidates), ambiguous

    def values_for(self, keys, aliases, *, inherit=True):
        values = []
        for node in keys:
            levels = [(node, 0)]
            if inherit:
                levels += self.ancestors(node)
            selected_depth = None
            for candidate, depth in levels:
                if selected_depth is not None and depth > selected_depth:
                    continue
                value, prop = first(self.props(candidate), aliases)
                if prop is not None:
                    selected_depth = depth if selected_depth is None else selected_depth
                    values.append({"value": value, "node": candidate, "property": prop,
                                   "method": "direct" if not depth else "owning_ancestor"})
            # Exact filename ownership is a provenance fallback, never a citation traversal.
            if selected_depth is None and inherit:
                files, ambiguous = self.files_for(node)
                if not ambiguous:
                    for file in files:
                        value, prop = first(self.props(file), aliases)
                        if prop is not None:
                            values.append({"value": value, "node": file, "property": prop, "method": "owning_file"})
        return values

    def source(self, primary, file, location):
        p = self.props(file) if file else self.props(primary)
        names = node_filenames(p) or node_filenames(self.props(primary))
        title, _ = first(p, ("文件全称", "标准中文名称", "document_name", "文件名称") + (("name",) if file else ()))
        number, _ = first(p, ("标准编号", "文号", "document_number"))
        version, _ = first(p, ("文件版本", "修订版本", "document_version"))
        publication, _ = first(p, ("发布日期", "publication_date"))
        effective, _ = first(p, ("生效日期", "实施日期", "effective_date"))
        fingerprint, _ = first(p, ("source_sha256", "文件内容摘要", "source_hash"))
        scope = self.snapshot["source"]
        document_key = digest([scope["graph_source"], scope["graph_label"], names, title, number,
                               version, publication, effective, fingerprint or self.snapshot["snapshot"]])
        node = self.nodes[primary]
        validity, _ = first(p, ("时效性", "validity_status", "legal_status", "标准状态"))
        return {"document_key": document_key, "filename": names[0] if names else None,
                "filenames": names, "document_name": title, "document_number": number,
                "document_version": version, "publication_date": parse_date(publication),
                "effective_date": parse_date(effective), "date_raw": {"publication_date": publication,
                "effective_date": effective}, "validity_status": validity,
                "graph_source": scope["graph_source"], "graph_label": scope["graph_label"],
                "graph_snapshot": self.snapshot["snapshot"],
                "source_file_node_id": self.props(file).get("id") or file if file else None,
                "source_file_snapshot_key": file,
                "source_node_ids": [self.props(primary).get("id") or primary],
                "source_nodes": [{"id": self.props(primary).get("id"), "snapshot_key": primary,
                                  "type": self.props(primary)["label"]}],
                "source_relationship_ids": [], "source_node_types": [self.props(primary)["label"]],
                "source_location": location or {},
                "identity_basis": "source_fingerprint" if fingerprint else "snapshot_scoped"}


def decode_indicators(value, version=None):
    if not isinstance(value, list):
        raise KnowledgeStorageError("Formal indicators must be an array")
    result = {}
    def unique_pairs(pairs):
        result = {}
        for key, val in pairs:
            if key in result:
                raise KnowledgeStorageError("Duplicate indicator JSON key")
            result[key] = val
        return result
    for raw in value:
        raw = json.loads(raw, object_pairs_hook=unique_pairs) if isinstance(raw, str) else raw
        if not isinstance(raw, dict):
            raise KnowledgeStorageError("Indicator item must be an object")
        item = {"indicator_number": raw.get("indicator_number", raw.get("指标序号")),
                "indicator_name": raw.get("indicator_name", raw.get("指标名")),
                "indicator_description": raw.get("indicator_description", raw.get("指标描述")),
                "indicator_system_version": raw.get("indicator_system_version", version)}
        validate_indicator(item)
        identity = (item["indicator_system_version"], item["indicator_number"])
        if identity in result and result[identity] != item:
            raise KnowledgeStorageError("Conflicting indicator number metadata")
        result[identity] = item
    return sorted(result.values(), key=canonical)


def enrich_entry(entry, context, config, run_id, input_binding, log_reference):
    refs = entry.get("source_node_ids") or ([entry["source_node_id"]] if entry.get("source_node_id") else [])
    keys = sorted({context.resolve_ref(ref) for ref in refs})
    if not keys:
        raise KnowledgeStorageError("Converted knowledge has no generating node")
    provenance, states, raw_values = [], {}, {}
    location = entry.get("source_location")
    sources, files = [], {}
    ambiguous = False
    relationship_ids = []
    for reference in entry.get("source_relationship_ids") or []:
        found = context.edge_aliases.get(str(reference), set())
        if len(found) != 1:
            raise KnowledgeStorageError("Knowledge relationship reference is missing or ambiguous")
        relationship_ids.append(next(iter(found)))
    for key in keys:
        owners, conflict = context.files_for(key)
        ambiguous |= conflict
        local_location = next((ref["source_location"] for ref in entry.get("source_refs", [])
                               if ref.get("source_location") and context.resolve_ref(ref) == key), location)
        for file in owners or [None]:
            src = context.source(key, file, local_location)
            src["source_relationship_ids"] = sorted(set(relationship_ids))
            sources.append(src)
            if file:
                files[(src["document_key"], file)] = (file, src)
    sources = sorted({canonical(s): s for s in sources}.values(), key=canonical)
    states["sources"] = {"state": "conflict" if ambiguous else "present"}

    def controlled(field, aliases, allowed, mapping=None):
        inputs = []
        for node in keys:
            local_inputs = context.values_for([node], aliases)
            if not local_inputs and field == "economic_industries":
                owners, ownership_conflict = context.files_for(node)
                if not ownership_conflict:
                    names = node_filenames(context.props(node))
                    for file in owners:
                        names.extend(node_filenames(context.props(file)))
                    for name in sorted(set(names)):
                        if name in config.filename_industries:
                            local_inputs.append({"value": config.filename_industries[name], "node": node,
                                                 "property": "filename_industries", "method": "configured_mapping"})
            inputs.extend(local_inputs)
        raw_values[field] = inputs
        values, invalid = set(), False
        for found in inputs:
            try:
                candidates = strings(found["value"])
                for value in candidates:
                    mapped = (mapping or {}).get(value, value)
                    for v in strings(mapped):
                        if v not in allowed:
                            invalid = True
                        else:
                            values.add(v)
            except KnowledgeStorageError:
                invalid = True
            provenance.append({"field": field, **found})
        conflict = field == "economic_industries" and "通用" in values and len(values) > 1
        state = "conflict" if conflict else "invalid" if invalid else "present" if values else "empty" if inputs else "missing"
        states[field] = {"state": state}
        return None if state in {"invalid", "conflict", "missing"} else sorted(values)

    risks = controlled("risk_types", ("合规风险类型", "risk_types"), RISK_TYPES)
    industries = controlled("economic_industries", ("经济行业", "economic_industries", "适用行业", "applicable_industry"),
                            INDUSTRIES, config.industry_mapping)
    validity_inputs = context.values_for(keys, ("时效性", "validity_status", "legal_status", "标准状态"))
    validity_values = {v["value"].strip() for v in validity_inputs if isinstance(v["value"], str)
                       and v["value"].strip() not in {"", "无", "暂无", "未知", "不适用", "null", "None"}}
    validity = next(iter(validity_values)) if len(validity_values) == 1 else None
    invalid_validity = any(not isinstance(v["value"], str) for v in validity_inputs)
    if invalid_validity:
        validity = None
    states["validity_status"] = {"state": "invalid" if invalid_validity else "conflict" if len(validity_values) > 1 else "present" if validity else "missing"}
    provenance.extend({"field": "validity_status", **v} for v in validity_inputs)

    # Only generating semantic nodes and their unique article owners supply formal associations.
    formal, formal_states = [], []
    reference_entry = bool(entry.get("metadata", {}).get("relationship")) or "引用" in entry.get("knowledge_type", "")
    for key in keys:
        p = context.props(key)
        chosen = [key] if config.formal_indicator_property in p and p["label"] not in FILE_TYPES else []
        if not chosen and not reference_entry:
            articles = [(a, d) for a, d in context.ancestors(key) if context.props(a)["label"] in ARTICLE_TYPES]
            if articles:
                depth = min(d for _, d in articles)
                chosen = [a for a, d in articles if d == depth]
        if len(chosen) > 1:
            formal_states.append("conflict")
            continue
        if not chosen:
            formal_states.append("not_applicable" if p["label"] in FILE_TYPES or reference_entry else "missing")
        for node in chosen:
            value = context.props(node).get(config.formal_indicator_property)
            if value is None:
                formal_states.append("missing")
                continue
            try:
                items = decode_indicators(value, config.indicator_system_version)
                formal_states.append("present" if items else "empty")
                for item in items:
                    formal.append({**item, "association_scope": "法条级" if context.props(node)["label"] in ARTICLE_TYPES else "知识单元级",
                                   "association_method": "直接映射" if node == key else "沿所属法条继承",
                                   "source_reference": {"source_node_id": context.props(node).get("id") or node,
                                                        "source_snapshot_key": node,
                                                        "graph_source": context.snapshot["source"]["graph_source"],
                                                        "graph_label": context.snapshot["source"]["graph_label"],
                                                        "graph_snapshot": context.snapshot["snapshot"],
                                                        "property": config.formal_indicator_property}})
            except (ValueError, TypeError):
                formal_states.append("invalid")
                raw_values.setdefault("indicators", []).append({"node": node, "value": value})
    # Differing definitions for one number are not silently resolved by first wins.
    definitions = defaultdict(set)
    for item in formal:
        definitions[(item["indicator_system_version"], item["indicator_number"])].add(
            canonical([item["indicator_name"], item["indicator_description"]]))
    if any(len(v) > 1 for v in definitions.values()):
        formal_states.append("conflict")
    formal_state = next((s for s in ("conflict", "invalid", "present", "missing", "empty", "not_applicable") if s in formal_states), "missing")
    states["indicators"] = {"state": formal_state}
    if formal_state in {"conflict", "invalid"}:
        raw_values["partial_indicators"] = formal
        formal = None
    elif formal_state == "present":
        formal = sorted({canonical(v): v for v in formal}.values(), key=canonical)
        if "missing" in formal_states:
            states["indicators"]["coverage"] = "partial"
    elif formal_state == "empty":
        formal = []
    else:
        formal = None

    document_indicators = []
    for _, (file, src) in sorted(files.items()):
        doc_key = src["document_key"]
        p = context.props(file)
        prop = config.file_indicator_properties.get(p["label"])
        group = {key: src[key] for key in ("document_key", "filename", "graph_source", "graph_label", "graph_snapshot", "source_file_node_id", "source_file_snapshot_key")}
        group.update(source_property=prop, mapping_state="missing", indicators=None)
        if prop and p.get(prop) is not None:
            try:
                group["indicators"] = decode_indicators(p[prop], config.indicator_system_version)
                group["mapping_state"] = "present" if group["indicators"] else "empty"
            except (ValueError, TypeError):
                group["mapping_state"] = "invalid"
                raw_values.setdefault("document_indicators", []).append({"document_key": doc_key, "value": p[prop]})
        document_indicators.append(group)
    grouped_definitions = defaultdict(set)
    for group in document_indicators:
        grouped_definitions[group["document_key"]].add(canonical([group["mapping_state"], group["indicators"]]))
    for group in document_indicators:
        if len(grouped_definitions[group["document_key"]]) > 1:
            group["mapping_state"] = "conflict"
    mapping_states = {g["mapping_state"] for g in document_indicators}
    states["document_indicators"] = {"state": next(
        (state for state in ("conflict", "invalid", "present", "missing", "empty")
         if state in mapping_states), "missing")}

    content = entry["content"].strip()
    source_identity = sorted({(s["document_key"], canonical(s["source_location"])) for s in sources})
    knowledge_id = "kn_" + digest([config.rule_version, source_identity, entry["knowledge_type"], normalized_text(content)])
    now = now_cn()
    result = {"knowledge_id": knowledge_id, "dataset_version": config.dataset_version,
              "content": content, "knowledge_category": context.snapshot["source"]["category"],
              "knowledge_type": entry["knowledge_type"], "validity_status": validity,
              "semantic_types": sorted(set(entry.get("semantic_types") or [entry["knowledge_type"]])),
              "risk_types": risks, "economic_industries": industries, "tags": sorted(set(config.tags)),
              "indicators": formal, "document_indicators": document_indicators, "sources": sources,
              "attribute_provenance": sorted({canonical(v): v for v in provenance}.values(), key=canonical),
              "field_states": states, "metadata": {"converter_metadata": entry.get("metadata") or {},
              "source_refs": entry.get("source_refs") or [], "related_node_ids": entry.get("related_node_ids") or [],
              "raw_attributes": raw_values, "input_binding": input_binding},
              "content_hash": digest(content), "run_id": run_id,
              "conversion_rule_version": config.rule_version,
              "classification_config_version": config.classification_hash,
              "publication_status": "staging", "generated_at": now, "created_at": now, "updated_at": now,
              "log_reference": str(log_reference)}
    result["record_hash"] = record_digest(result)
    validate_record(result)
    return result
