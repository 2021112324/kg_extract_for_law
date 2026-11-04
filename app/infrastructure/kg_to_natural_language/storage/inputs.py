"""Bounded graph snapshots and read-only Neo4j input."""

import json
from pathlib import Path

from .records import KnowledgeStorageError, digest


def read_source(source, *, driver=None, database=None):
    source.validate()
    if source.source_type == "json":
        paths = [Path(source.node_path), Path(source.edge_path)]
        if sum(p.stat().st_size for p in paths) > source.max_bytes:
            raise KnowledgeStorageError("Graph input exceeds max_bytes")
        nodes, edges = [json.loads(p.read_text(encoding="utf-8-sig")) for p in paths]
    else:
        from ..graph.exporter import read_knowledge_snapshot
        nodes, edges = read_knowledge_snapshot(source.graph_label, driver=driver,
                                                database=database, max_bytes=source.max_bytes)
    if not isinstance(nodes, list) or not isinstance(edges, list):
        raise KnowledgeStorageError("Graph exports must be JSON arrays")
    # A canonical sort makes a different Neo4j return order the same frozen input.
    nodes = sorted(nodes, key=digest)
    edges = sorted(edges, key=digest)
    return {"nodes": nodes, "edges": edges, "snapshot": digest([nodes, edges]),
            "source": {"converter": source.converter, "category": source.category,
                       "graph_label": source.graph_label, "graph_source": source.graph_source}}
