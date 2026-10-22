"""Shared export records, lossless audit values and strict graph comparison."""

import base64
from collections import Counter
from datetime import date, datetime, time, timedelta
import hashlib
import json
import math

from neo4j.spatial import CartesianPoint, Point, WGS84Point
from neo4j.time import Date, DateTime, Duration, Time


class SubgraphError(ValueError):
    """Invalid graph input, unsupported value or failed integrity check."""


def identifier(value):
    if not isinstance(value, str) or not value.strip() or len(value) > 512:
        raise SubgraphError("invalid_identifier")
    if any(ord(c) < 32 for c in value) or "\\u0060" in value.lower():
        raise SubgraphError("invalid_identifier")
    return "`" + value.replace("`", "``") + "`"


def node_to_dict(node):
    return {"identity": node.id, "labels": list(node.labels),
            "properties": dict(node.items()), "elementId": node.element_id}


def relationship_to_dict(rel):
    return {"identity": rel.id, "start": rel.start_node.id, "end": rel.end_node.id,
            "type": rel.type, "properties": dict(rel.items()), "elementId": rel.element_id,
            "startNodeElementId": rel.start_node.element_id,
            "endNodeElementId": rel.end_node.element_id}


def node_query(tag):
    return f"MATCH (n:{identifier(tag)}) RETURN n"


def relationship_query(tag, internal_only=True):
    target = f":{identifier(tag)}" if internal_only else ""
    return f"MATCH (n:{identifier(tag)})-[p]->(q{target}) RETURN p"


def read_records(tx, tag, *, internal_only=True, node_converter=node_to_dict,
                 edge_converter=relationship_to_dict, max_nodes=None, max_relationships=None, max_bytes=None):
    nodes, edges = [], []
    byte_count = 0
    for row in tx.run(node_query(tag)):
        nodes.append({"n": node_converter(row["n"])})
        if max_bytes is not None:
            byte_count += len(json.dumps(encode(nodes[-1]), ensure_ascii=False).encode("utf-8"))
            if byte_count > max_bytes:
                raise SubgraphError("snapshot_byte_budget_exceeded")
        if max_nodes is not None and len(nodes) > max_nodes:
            raise SubgraphError("node_budget_exceeded")
    for row in tx.run(relationship_query(tag, internal_only)):
        edges.append({"p": edge_converter(row["p"])})
        if max_bytes is not None:
            byte_count += len(json.dumps(encode(edges[-1]), ensure_ascii=False).encode("utf-8"))
            if byte_count > max_bytes:
                raise SubgraphError("snapshot_byte_budget_exceeded")
        if max_relationships is not None and len(edges) > max_relationships:
            raise SubgraphError("relationship_budget_exceeded")
    return nodes, edges


def encode(value):
    """Every value is tagged, so user keys cannot impersonate a type marker."""
    if value is None:
        return ["null", None]
    if type(value) in (str, bool, int):
        return [type(value).__name__, value]
    if type(value) is float:
        return ["float", value.hex()]
    if isinstance(value, (bytes, bytearray)):
        return ["bytes", base64.b64encode(value).decode("ascii")]
    if isinstance(value, Point):
        if value.srid not in {7203, 9157, 4326, 4979}:
            raise SubgraphError("unsupported_point_srid")
        return ["Point", [value.srid, [float(v).hex() for v in value]]]
    if isinstance(value, Duration):
        return ["Duration", [value.months, value.days, value.seconds, value.nanoseconds]]
    if isinstance(value, (DateTime, Time, Date)):
        zone = getattr(getattr(value, "tzinfo", None), "zone", None)
        zone = zone or getattr(getattr(value, "tzinfo", None), "key", None)
        return [type(value).__name__, [value.iso_format(), zone]]
    if type(value) in (datetime, date, time):
        return ["native_" + type(value).__name__, value.isoformat()]
    if type(value) is timedelta:
        return ["timedelta", [value.days, value.seconds, value.microseconds]]
    if type(value) is dict:
        if any(not isinstance(k, str) for k in value):
            raise SubgraphError("non_string_dictionary_key")
        return ["dict", [[k, encode(v)] for k, v in sorted(value.items())]]
    if isinstance(value, (list, tuple)):
        return ["list", [encode(v) for v in value]]
    raise SubgraphError("unsupported_property_type:" + type(value).__name__)


def decode(value):
    kind, data = value
    if kind == "null":
        return None
    if kind in {"str", "bool", "int"}:
        return data
    if kind == "float":
        return float.fromhex(data)
    if kind == "bytes":
        return bytearray(base64.b64decode(data))
    if kind == "Point":
        cls = CartesianPoint if data[0] in {7203, 9157} else WGS84Point
        return cls(tuple(float.fromhex(v) for v in data[1]))
    if kind == "Duration":
        return Duration(months=data[0], days=data[1], seconds=data[2], nanoseconds=data[3])
    if kind in {"DateTime", "Date", "Time"}:
        result = {"DateTime": DateTime, "Date": Date, "Time": Time}[kind].from_iso_format(data[0])
        if data[1] and kind == "DateTime":
            import pytz
            result = result.as_timezone(pytz.timezone(data[1]))
        return result
    if kind.startswith("native_"):
        return {"native_datetime": datetime, "native_date": date, "native_time": time}[kind].fromisoformat(data)
    if kind == "timedelta":
        return timedelta(days=data[0], seconds=data[1], microseconds=data[2])
    if kind == "dict":
        return {k: decode(v) for k, v in data}
    if kind == "list":
        return [decode(v) for v in data]
    raise SubgraphError("unknown_snapshot_type")


def json_view(value):
    if value is None or type(value) in (str, bool, int):
        return value
    if type(value) is float and math.isfinite(value):
        return value
    if type(value) is dict:
        return {k: json_view(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)) and not isinstance(value, (Point, Duration)):
        return [json_view(v) for v in value]
    return {"neo4j_audit_value": encode(value)}


def digest(value):
    return hashlib.sha256(json.dumps(encode(value), ensure_ascii=False,
                                     separators=(",", ":")).encode("utf-8")).hexdigest()


def snapshot_hash(snapshot):
    nodes = sorted((row["n"] for row in snapshot["nodes"]), key=lambda n: n["elementId"])
    edges = sorted((row["p"] for row in snapshot["edges"]), key=lambda r: r["elementId"])
    return digest({"nodes": [{**n, "labels": sorted(n["labels"])} for n in nodes],
                   "edges": edges, "boundary_count": snapshot.get("boundary_count", 0)})


def graph_content(snapshot, node_keys=None, edge_keys=None):
    """Restore logical snapshot keys when verifying newly created physical objects."""
    nodes, edges = [], []
    for row in snapshot["nodes"]:
        node = row["n"]
        physical = node["elementId"]
        key = node_keys[physical] if node_keys is not None else physical
        nodes.append({"key": key, "labels": sorted(node["labels"]), "properties": node["properties"]})
    for row in snapshot["edges"]:
        edge = row["p"]
        key = edge_keys[edge["elementId"]] if edge_keys is not None else edge["elementId"]
        start, end = edge["startNodeElementId"], edge["endNodeElementId"]
        edges.append({"key": key, "start": node_keys[start] if node_keys is not None else start,
                      "end": node_keys[end] if node_keys is not None else end,
                      "type": edge["type"], "properties": edge["properties"]})
    return {"nodes": sorted(nodes, key=lambda n: n["key"]), "edges": sorted(edges, key=lambda r: r["key"])}


def validate_snapshot(snapshot):
    nodes = [n["n"] for n in snapshot["nodes"]]
    edges = [r["p"] for r in snapshot["edges"]]
    ids = [n["elementId"] for n in nodes]
    eids = [r["elementId"] for r in edges]
    if any(not isinstance(k, str) or not k for k in ids + eids):
        raise SubgraphError("missing_physical_identity")
    if len(set(ids)) != len(ids) or len(set(eids)) != len(eids):
        raise SubgraphError("duplicate_physical_identity")
    membership = set(ids)
    if any(r["startNodeElementId"] not in membership or r["endNodeElementId"] not in membership for r in edges):
        raise SubgraphError("relationship_endpoint_missing")
    for node in nodes:
        if not isinstance(node["properties"], dict):
            raise SubgraphError("invalid_node_properties")
    for edge in edges:
        identifier(edge["type"])
        if not isinstance(edge["properties"], dict):
            raise SubgraphError("invalid_edge_properties")
    encode(snapshot)
    return Counter(nodes=len(nodes), relationships=len(edges))
