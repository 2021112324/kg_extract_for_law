"""Thin bridge over the project's graph adapter; no separate driver implementation."""

import copy

from ..graph_storage.subgraph_records import digest, graph_content, validate_snapshot
from .config import LEGACY, MANAGED


def copy_plan(snapshot, source, target):
    plan = copy.deepcopy(snapshot)
    for row in plan["nodes"]:
        node = row["n"]
        if source not in node["labels"] or target in node["labels"]:
            raise ValueError("源成员作用域不一致或已带目标标签")
        if node["properties"].get("graph_tag", source) != source:
            raise ValueError("源节点graph_tag与源标签冲突")
        node["labels"] = sorted({target if label == source else label for label in node["labels"]})
        node["properties"]["graph_tag"] = target
    for row in plan["edges"]:
        if row["p"]["properties"].get("graph_tag", source) != source:
            raise ValueError("源关系graph_tag与源标签冲突")
        row["p"]["properties"]["graph_tag"] = target
    plan["boundary_count"] = 0
    validate_snapshot(plan)
    return plan


def freeze_articles(snapshot, manifest):
    node_map = dict(manifest["node_map"])
    if len(node_map) != len(snapshot["nodes"]) or len(set(node_map.values())) != len(node_map):
        raise ValueError("复制身份映射不完整")
    by_id = {row["n"]["elementId"]: row["n"] for row in snapshot["nodes"]}
    parents = {}
    for row in snapshot["edges"]:
        edge = row["p"]
        parent = by_id[edge["startNodeElementId"]]
        if parent["properties"].get("label") == "法规文件":
            parents.setdefault(edge["endNodeElementId"], []).append(parent)
    def filenames(props):
        values = props.get("filename", [])
        return [values] if isinstance(values, str) else [v for v in values if isinstance(v, str)]
    inventory = []
    for source_id, node in by_id.items():
        props = node["properties"]
        if props.get("label") != "法条":
            continue
        names = filenames(props)
        for parent in parents.get(source_id, []):
            names.extend(filenames(parent["properties"]))
            title = parent["properties"].get("文件全称")
            if isinstance(title, str) and title:
                names.append(title)
        inventory.append({"id": node_map[source_id], "source_id": source_id,
                          "number": str(props.get("条") or props.get("条文编号") or props.get("法条编号") or props.get("法条序号") or ""),
                          "text": props.get("法条全文", ""), "filenames": sorted(set(names))})
    inventory.sort(key=lambda item: item["id"])
    if len({item["id"] for item in inventory}) != len(inventory):
        raise ValueError("法条物理身份重复")
    return inventory


def verify_target(snapshot, source, manifest, patches):
    """Immutable graph baseline plus confirmed managed-property patches."""
    validate_snapshot(snapshot)
    if snapshot.get("boundary_count", 0):
        raise ValueError("目标出现跨图谱关系")
    if snapshot.get("scope_counts", {"nodes": len(snapshot["nodes"]), "relationships": len(snapshot["edges"])}) != {
            "nodes": len(snapshot["nodes"]), "relationships": len(snapshot["edges"])}:
        raise ValueError("目标graph_tag范围出现额外成员")
    expected = graph_content(copy_plan(source, manifest["source_label"], manifest["target_label"]))
    node_keys = {physical: logical for logical, physical in manifest["node_map"]}
    edge_keys = {physical: logical for logical, physical in manifest["edge_map"]}
    if (len(node_keys) != len(expected["nodes"]) or len(edge_keys) != len(expected["edges"])
            or len(snapshot["nodes"]) != len(node_keys) or len(snapshot["edges"]) != len(edge_keys)):
        raise ValueError("目标成员数量发生变化")
    expected_nodes = {node["key"]: node for node in expected["nodes"]}
    for row in patches:
        if row["id"] not in node_keys or set(row["properties"]) - set(MANAGED):
            raise ValueError("patch超出清单或可写字段")
        if any(row["properties"].get(key) is not None for key in LEGACY):
            raise ValueError("历史映射字段仅允许移除")
        props = expected_nodes[node_keys[row["id"]]]["properties"]
        for key, value in row["properties"].items():
            if value is None:
                props.pop(key, None)
            else:
                props[key] = value
    try:
        actual = graph_content(snapshot, node_keys, edge_keys)
    except KeyError as exc:
        raise ValueError("目标物理成员发生变化") from exc
    if digest(actual) != digest(expected):
        raise ValueError("目标内容与复制基线及已确认patch不一致")
    return {"status": "verified", "nodes": len(node_keys), "relationships": len(edge_keys)}
