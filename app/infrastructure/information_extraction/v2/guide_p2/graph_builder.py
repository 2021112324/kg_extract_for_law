"""No semantic fallback: create a graph only from a validated document result."""
import json
from ..guide_p1.graph_builder import prepare_guide_p1_kg_for_neo4j as prepare_graph_for_neo4j
from .io_utils import stable_id
from .prompt.schema import STRUCTURE_PROPERTIES


class GuideP2GraphBuilder:
    def build(self, parsed, plan, file_result, results):
        nodes, edges, ids, edge_keys = [], [], {}, set()

        def node(identifier, name, kind, props):
            if identifier not in ids:
                value = {"node_id": identifier, "node_name": name, "node_type": kind,
                         "properties": props, "filename": parsed["filename"]}
                nodes.append(value)
                ids[identifier] = value
            return identifier

        def edge(source, relation, target):
            key = (source, relation, target)
            if source not in ids or target not in ids or source == target:
                raise ValueError("Invalid graph endpoint or self-edge")
            if key not in edge_keys:
                edge_keys.add(key)
                edges.append({"edge_id": stable_id("p2_edge", *key), "source_id": source, "target_id": target,
                              "relation_type": relation, "directionality": "single", "properties": {}, "filename": parsed["filename"]})

        if not file_result:
            return {"nodes": [], "edges": [], "metadata": {"semantic_success": False}}
        document_id = parsed["document_id"]
        local = {}
        for e in file_result["entities"]:
            identifier = document_id if e["entity_type"] == "合规指引文件" else stable_id("p2_file_reference", document_id, e["properties"])
            local[e["name"]] = node(identifier, e["name"], e["entity_type"], dict(e["properties"]))
        for r in file_result["relations"]:
            edge(local[r["source"]], r["type"], local[r["target"]])
        structures = {}
        for i, n in enumerate(parsed["nodes"], 1):
            identifier = stable_id("p2_structure_graph", document_id, n["node_id"])
            props = {"结构类型": n["kind"], "显示编号": n["number"], "节点标题": n["title"], "节点内容": n["content"],
                     "完整路径": " / ".join(n["path"]), "顺序": i, "来源原文": n["raw_text"], "source_file": parsed["filename"]}
            assert set(props) <= STRUCTURE_PROPERTIES
            structures[n["node_id"]] = node(identifier, n["number"] + n["title"], "指引结构节点", props)
        for n in parsed["nodes"]:
            edge(structures.get(n["parent_id"], document_id), "包含", structures[n["node_id"]])
        units = {u["entry_id"]: u for u in plan["units"]}
        for result in results:
            if result["status"] != "success":
                continue
            local = {}
            for e in result["entities"]:
                props = {k: v for k, v in e["properties"].items() if k != "目标条目ID"}
                identifier = stable_id("p2_knowledge", document_id, result["entry_id"], e["entity_type"], json.dumps(props, ensure_ascii=False, sort_keys=True))
                local[e["name"]] = node(identifier, e["name"], e["entity_type"], props)
                if e["entity_type"] == "指引知识单元":
                    edge(structures[units[result["entry_id"]]["node_id"]], "提出", identifier)
            for r in result["relations"]:
                edge(local[r["source"]], r["type"], local[r["target"]])
        return {"nodes": nodes, "edges": edges, "metadata": {"document_id": document_id, "source_sha256": parsed["source_sha256"], "semantic_success": True}}
