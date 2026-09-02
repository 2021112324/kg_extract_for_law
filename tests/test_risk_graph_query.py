from __future__ import annotations

from typing import Any

from app.infrastructure.kg_to_natural_language.risk_graph_query import (
    query_mapped_file_subgraphs,
)
from app.infrastructure.kg_to_natural_language.risk_indicator_tree import MappedFile


def _node(identity: int, filename: str | list[str]) -> dict[str, Any]:
    return {
        "identity": identity,
        "labels": ["测试标签"],
        "properties": {"filename": filename, "label": "测试节点", "name": str(identity)},
        "elementId": f"node-{identity}",
    }


def _edge(identity: int, start: int, end: int) -> dict[str, Any]:
    return {
        "identity": identity,
        "start": start,
        "end": end,
        "type": "包含",
        "properties": {"label": "包含"},
        "elementId": f"edge-{identity}",
        "startNodeElementId": f"node-{start}",
        "endNodeElementId": f"node-{end}",
    }


class _Session:
    def __init__(self, records: dict[str, dict[str, list[dict[str, Any]]]]) -> None:
        self.records = records

    def __enter__(self) -> "_Session":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def run(self, query: str, **_: object) -> list[dict[str, Any]]:
        label = next((name for name in self.records if f"`{name}`" in query), "")
        if "RETURN n" in query:
            return [{"n": node} for node in self.records.get(label, {}).get("nodes", [])]
        return [
            {"relationship": edge}
            for edge in self.records.get(label, {}).get("edges", [])
        ]


class _Driver:
    def __init__(self, records: dict[str, dict[str, list[dict[str, Any]]]]) -> None:
        self.records = records
        self.closed = False

    def verify_connectivity(self) -> None:
        return None

    def session(self, **_: object) -> _Session:
        return _Session(self.records)

    def close(self) -> None:
        self.closed = True


def test_query_partitions_five_labels_and_isolates_relationships() -> None:
    labels = ["法律法规条款", "国家标准", "行政监管规则", "英文法规", "合规指引"]
    records: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for index, label in enumerate(labels, start=1):
        records[label] = {
            "nodes": [
                _node(index * 10, f"文件{index}.txt"),
                _node(index * 10 + 1, [f"文件{index}.md"]),
            ],
            "edges": [_edge(index, index * 10, index * 10 + 1)],
        }
    # 同一文件跨标签命中，并加入一条跨文件关系，后者不得进入任何子图。
    records["英文法规"]["nodes"].append(_node(99, "文件1.pdf"))
    records["法律法规条款"]["nodes"].append(_node(88, "文件2.txt"))
    records["法律法规条款"]["edges"].append(_edge(88, 10, 88))

    mapped_files = [
        MappedFile(normalized_key=f"文件{index}", found_filename=f"文件{index}.txt")
        for index in range(1, 7)
    ]
    driver = _Driver(records)
    result = query_mapped_file_subgraphs(mapped_files, driver=driver)

    assert result.queried_labels == labels
    assert "文件6" in result.unmatched_keys
    assert set(result.cross_label_matches["文件1"]) == {"法律法规条款", "英文法规"}
    assert result.relationship_issue_count == 1
    law_partition = next(
        partition
        for partition in result.partitions
        if partition.normalized_key == "文件1" and partition.graph_category == "法律法规条款"
    )
    assert len(law_partition.node_records) == 2
    assert len(law_partition.edge_records) == 1
    assert driver.closed is False


def test_query_without_files_does_not_open_database() -> None:
    result = query_mapped_file_subgraphs([])
    assert result.partitions == []
    assert result.unmatched_keys == []
    assert result.queried_labels == []
