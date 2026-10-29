"""Build only validated semantic results; retain separate normative/reference views."""
import json

from ..guide_p1.graph_builder import prepare_guide_p1_kg_for_neo4j as prepare_graph_for_neo4j
from .io_utils import stable_id, compact
from .validator import CONTRACT, ReviewRequired, interface_eligibility, file_properties


class ParagraphGraphBuilder:
    def build(self, parsed, file_result, results, usage=None):
        nodes, edges, by_id, edge_keys, evidence_index, interfaces = [], [], {}, set(), {}, {}

        def node(identifier, name, kind, props):
            if set(props) - CONTRACT.allowed_properties[kind]:
                raise ValueError("Graph property is outside local Schema: " + kind)
            value = {"node_id": identifier, "node_name": name, "node_type": kind,
                     "properties": props, "filename": parsed["filename"]}
            if identifier in by_id:
                if by_id[identifier]["properties"] != props:
                    raise ReviewRequired("Conflicting propositions share a source/claim key")
            else:
                by_id[identifier] = value
                nodes.append(value)
            return identifier

        def edge(source, kind, target):
            key = (source, kind, target)
            if source not in by_id or target not in by_id or source == target:
                raise ValueError("Unknown graph endpoint or self edge")
            if (by_id[source]["node_type"], kind, by_id[target]["node_type"]) not in CONTRACT.legal_triples:
                raise ValueError("Graph triple is outside Schema")
            if key not in edge_keys:
                edges.append({"edge_id": stable_id("paragraph_edge", *key), "source_id": source,
                              "target_id": target, "relation_type": kind, "directionality": "single", "properties": {},
                              "filename": parsed["filename"]})
                edge_keys.add(key)

        metadata = {"document_id": parsed["document_id"], "source_sha256": parsed["source_sha256"],
                    "semantic_success": False, "evidence_index": evidence_index, "interface_eligibility": interfaces}
        if file_result is None:
            return {"nodes": [], "edges": [], "metadata": metadata}
        local = {}
        for entity in file_result["entities"]:
            props, kind = dict(entity["properties"]), entity["entity_type"]
            identifier = parsed["document_id"] if kind == "来源文件" else stable_id("paragraph_reference", parsed["document_id"], json.dumps(props, ensure_ascii=False, sort_keys=True))
            local[entity["name"]] = node(identifier, entity["name"], kind, props)
        for relation in file_result["relations"]:
            edge(local[relation["source"]], relation["type"], local[relation["target"]])
        for paragraph in parsed["paragraphs"]:
            props = {"段落原文": paragraph["text"], "顺序": paragraph["order"], "所属标题": paragraph["heading"],
                     "来源文件": parsed["filename"], "原文范围": {"start": paragraph["start"], "end": paragraph["end"]}}
            node(paragraph["paragraph_id"], "段落"+str(paragraph["order"]), "段落节点", props)
            edge(parsed["document_id"], "包含", paragraph["paragraph_id"])
        for result in results:
            if result["status"] != "success":
                continue
            claims = [e for e in result["entities"] if e["entity_type"] == "段落知识单元"
                      and (usage is None or e["properties"]["使用定位"] == usage)]
            selected = {e["name"] for e in claims}
            relations = [r for r in result["relations"] if r["source"] in selected]
            references = {r["target"] for r in relations}
            local = {}
            for entity in result["entities"]:
                name, kind = entity["name"], entity["entity_type"]
                if name not in selected | references:
                    continue
                props = {k: v for k, v in entity["properties"].items() if k != "目标ID"}
                if kind == "段落知识单元":
                    ranges = result["evidence_ranges"][name]
                    core = {k: compact(props.get(k)) for k in ("知识类型", "适用主体", "行为描述", "适用前提", "例外情形", "归属说明")}
                    identifier = stable_id("paragraph_knowledge", parsed["document_id"], result["paragraph_id"],
                                           json.dumps(ranges["target"], sort_keys=True), json.dumps(core, ensure_ascii=False, sort_keys=True))
                    # Generated display names do not participate in identity.
                    local[name] = node(identifier, props["知识内容"], kind, props)
                    evidence_index[identifier] = ranges
                    interfaces[identifier] = interface_eligibility(props, file_properties(file_result))
                    edge(result["paragraph_id"], "承载", identifier)
                else:
                    identifier = stable_id("paragraph_reference", parsed["document_id"], json.dumps(props, ensure_ascii=False, sort_keys=True))
                    local[name] = node(identifier, props["文件名称"], kind, props)
            for relation in relations:
                edge(local[relation["source"]], relation["type"], local[relation["target"]])
        metadata["file_semantics_valid"] = True
        metadata["semantic_success"] = any(n["node_type"] == "段落知识单元" for n in nodes)
        return {"nodes": nodes, "edges": edges, "metadata": metadata}

    def structure(self, parsed):
        return {"nodes": [{"node_id": p["paragraph_id"], "node_type": "段落节点", "text": p["text"],
                           "start": p["start"], "end": p["end"], "order": p["order"]} for p in parsed["paragraphs"]],
                "edges": [], "metadata": {"semantic_success": False, "status": "structure_only"}}
