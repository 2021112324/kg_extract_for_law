"""Assemble only validated semantic results; source numbers remain stable."""
from ..guide_p1.graph_builder import prepare_guide_p1_kg_for_neo4j as prepare_graph_for_neo4j
from .io_utils import stable_id, compact


class RuleGraphBuilder:
    def build(self, parsed, plan, file_result, content_results):
        nodes, edges, edge_keys, by_id = [], [], {}, {}

        def node(identifier, name, kind, props):
            if identifier not in by_id:
                value = {"node_id": identifier, "node_name": name, "node_type": kind,
                         "properties": dict(props), "filename": parsed["filename"]}
                nodes.append(value)
                by_id[identifier] = value
            return identifier

        def edge(source, relation, target, properties=None):
            key = (source, relation, target)
            if key in edge_keys and properties and properties.get("引用定位"):
                existing = edge_keys[key]["properties"]
                old = existing.get("引用定位")
                locations = list(old) if isinstance(old, list) else [old] if old else []
                if properties["引用定位"] not in locations:
                    locations.append(properties["引用定位"])
                existing["引用定位"] = locations[0] if len(locations) == 1 else locations
            if key not in edge_keys and source != target:
                if source not in by_id or target not in by_id:
                    raise ValueError("Dangling graph edge")
                value = {"edge_id": stable_id("rule_edge", *key), "source_id": source, "target_id": target,
                              "relation_type": relation, "directionality": "single", "properties": properties or {},
                              "filename": parsed["filename"]}
                edge_keys[key] = value
                edges.append(value)

        if not file_result:
            return {"nodes": [], "edges": [], "metadata": {"semantic_success": False}}
        document_id = parsed["document_id"]
        names = {}
        for e in file_result["entities"]:
            kind = e["entity_type"]
            identifier = document_id if kind == "法规文件" else stable_id("rule_basis", document_id, e["name"])
            names[e["name"]] = node(identifier, e["name"], kind, e["properties"])
        for r in file_result["relations"]:
            edge(names[r["source"]], r["type"], names[r["target"]])
        units = {u["unit_id"]: u for u in plan["units"]}
        created, pending_refs = {}, []
        for result in content_results:
            if result["status"] != "success":
                continue
            unit = units[result["unit_id"]]
            local = {}
            for e in result["validated"]["entities"]:
                kind, props = e["entity_type"], dict(e["properties"])
                if kind == "法条":
                    identifier = unit["unit_id"]
                    props.update({"条": unit["number"], "法条全文": unit["text"], "章": unit["chapter"], "节": unit["section"],
                                  "完整路径": " / ".join(unit["path"]), "来源文件": parsed["filename"]})
                    if unit["context"]:
                        props["上级上下文"] = unit["context"]
                    if unit["mode"] == "group":
                        props["来源编号列表"] = unit["member_numbers"]
                    created[unit["node_id"]] = identifier
                    name = parsed["title"] + " " + unit["number"]
                else:
                    # Evidence and condition differences must not be merged by entity name.
                    key = (props.get("单元编号"), props.get("条款单元内容"), props.get("原文依据"),
                           props.get("上下文依据"), props.get("适用前提"), props.get("例外情形")) if kind == "条款单元" else tuple(sorted((k, repr(v)) for k, v in props.items()))
                    identifier = stable_id("rule_unit" if kind == "条款单元" else "rule_reference", unit["unit_id"], key)
                    name = e["name"]
                local[e["name"]] = node(identifier, name, kind, props)
            for r in result["validated"]["relations"]:
                edge(local[r["source"]], r["type"], local[r["target"]])
                if r["type"] == "引用":
                    pending_refs.append((local[r["source"]], local[r["target"]], unit))
            edge(document_id, "包含", unit["unit_id"])
        for unit in units.values():
            if unit["unit_id"] not in by_id:
                continue
            for ancestor in reversed(unit["parent_node_ids"]):
                if ancestor in created:
                    edge(created[ancestor], "包含", unit["unit_id"])
                    break
        for source_id, reference_id, source_unit in pending_refs:
            ref = by_id[reference_id]["properties"]
            if ref.get("是否本文件内引用") != "是":
                continue
            number = str(ref.get("条款编号", ""))
            targets = [u for u in units.values() if number in u["member_numbers"] and u["unit_id"] in by_id]
            if len(targets) > 1:
                targets = [u for u in targets if u["scope"] == source_unit["scope"]]
            if len(targets) == 1:
                edge(source_id, "引用", targets[0]["unit_id"], {"引用定位": number})
        return {"nodes": nodes, "edges": edges, "metadata": {"document_id": document_id,
                "semantic_success": True, "source_sha256": parsed["source_sha256"]}}
