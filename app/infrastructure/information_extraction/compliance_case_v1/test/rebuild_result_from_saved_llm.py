"""Rebuild compliance_case_v1 result files from saved LLM outputs.

This script does not call the LLM. It reads each single-case JSON in the
result directory, replays its saved ``llm_extraction`` through the current
post-processing graph builder, and refreshes:

- per-case JSON graph/case_knowledge/stats;
- node.json;
- edge.json;
- case_knowledge.json;
- summary.json aggregate counts.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[5]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.infrastructure.information_extraction.compliance_case_v1.compliance_case_parse import (
    ComplianceCaseDocument,
)
from app.infrastructure.information_extraction.compliance_case_v1.export_case_knowledge import (
    export_case_knowledge_from_graph,
)
from app.infrastructure.information_extraction.compliance_case_v1.graph_extract import (
    _ComplianceCaseGraphBuilder,
)


BASE_DIR = Path(__file__).resolve().parents[1]
RESULT_DIR = BASE_DIR / "result"
AGGREGATE_FILES = {"summary.json", "node.json", "edge.json", "case_knowledge.json"}


def rebuild_result_from_saved_llm(result_dir: Path = RESULT_DIR) -> dict[str, Any]:
    """Refresh result files using saved ``llm_extraction`` values."""

    all_nodes: list[dict[str, Any]] = []
    all_edges: list[dict[str, Any]] = []
    all_case_knowledge: list[dict[str, Any]] = []
    updated_items: list[dict[str, Any]] = []

    for path in sorted(result_dir.glob("*.json")):
        if path.name in AGGREGATE_FILES or path.name.endswith("_质量校验明细.json"):
            continue
        data = json.loads(path.read_text(encoding="utf-8"))
        parsed_document = data.get("parsed_document")
        if not parsed_document or "llm_extraction" not in data:
            continue

        document = ComplianceCaseDocument(**parsed_document)
        builder = _ComplianceCaseGraphBuilder(document)
        builder.add_case_context_node()
        builder.add_llm_result(data.get("llm_extraction") or {"entities": [], "relations": []})
        graph = builder.to_graph()
        case_knowledge = export_case_knowledge_from_graph(graph, document.to_dict())

        data["graph"] = graph
        data["case_knowledge"] = case_knowledge
        data.setdefault("stats", {})["node_count"] = len(graph.get("nodes", []))
        data.setdefault("stats", {})["edge_count"] = len(graph.get("edges", []))
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

        all_nodes.extend(graph.get("nodes", []) or [])
        all_edges.extend(graph.get("edges", []) or [])
        all_case_knowledge.append(case_knowledge)
        updated_items.append(
            {
                "file": path.name,
                "case_id": document.case_id,
                "node_count": len(graph.get("nodes", [])),
                "edge_count": len(graph.get("edges", [])),
            }
        )

    summary_path = result_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8")) if summary_path.exists() else {}
    summary.update(
        {
            "node_count": len(all_nodes),
            "edge_count": len(all_edges),
            "case_knowledge_count": len(all_case_knowledge),
            "postprocess_rebuilt": True,
            "postprocess_updated_files": len(updated_items),
        }
    )

    (result_dir / "node.json").write_text(json.dumps(all_nodes, ensure_ascii=False, indent=2), encoding="utf-8")
    (result_dir / "edge.json").write_text(json.dumps(all_edges, ensure_ascii=False, indent=2), encoding="utf-8")
    (result_dir / "case_knowledge.json").write_text(
        json.dumps(all_case_knowledge, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    return {
        "updated": len(updated_items),
        "nodes": len(all_nodes),
        "edges": len(all_edges),
        "case_knowledge": len(all_case_knowledge),
        "items": updated_items,
    }


def main() -> None:
    result = rebuild_result_from_saved_llm()
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
