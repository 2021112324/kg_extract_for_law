import copy
from pathlib import Path

import pytest

from .. import ParagraphConfig, ParagraphExtractor, ParagraphStandaloneRunner
from .test_paragraph import FakeModel, FORMAL, NEWS


class FakeStorage:
    password = "test-secret"

    def __init__(self, occupied=False, unknown=False, corrupt=False):
        self.nodes, self.edges, self.calls = [], [], []
        self.occupied, self.unknown, self.corrupt = occupied, unknown, corrupt

    def connect(self):
        self.calls.append("connect")
        return True

    def disconnect(self):
        self.calls.append("disconnect")

    def check_subgraph_destination(self, label):
        if self.occupied:
            raise ValueError("target already exists")

    def add_subgraph_with_merge(self, graph, label, level, filename):
        self.calls.append("write")
        for node in graph["nodes"]:
            self.nodes.append({"n": {"elementId": node["node_id"], "labels": [label], "properties": {
                "id": node["node_id"], "name": node["node_name"], "label": node["node_type"], "graph_tag": label,
                "graph_level": level, "filename": [filename], **copy.deepcopy(node["properties"])}}})
        for edge in graph["edges"]:
            self.edges.append({"p": {"elementId": edge["edge_id"], "type": edge["relation_type"],
                "startNodeElementId": edge["source_id"], "endNodeElementId": edge["target_id"], "properties": {"graph_tag": label}}})
        return not self.unknown

    def read_subgraph_records(self, label, **kwargs):
        result = {"nodes": copy.deepcopy(self.nodes), "edges": copy.deepcopy(self.edges)}
        if self.corrupt:
            result["nodes"][-1]["n"]["properties"]["知识内容"] = "corrupt"
        return result


def runner(storage, publication="normative"):
    return ParagraphStandaloneRunner(ParagraphConfig(retry_delay=0, publication=publication), storage,
        lambda config: ParagraphExtractor(config, FakeModel()))


@pytest.mark.asyncio
async def test_confirmed_write_verifies_properties_and_writes_receipt(tmp_path):
    path = tmp_path/"示例规定.txt"
    path.write_text(FORMAL, encoding="utf-8")
    storage = FakeStorage()
    summary = await runner(storage).run(path, tmp_path/"out", "write", "paragraph_test")
    assert summary["status"] == "completed" and summary["storage_status"] == "confirmed"
    assert storage.calls.count("write") == 1
    assert path.read_text(encoding="utf-8") == FORMAL
    assert (Path(summary["files"][0]["record_directory"])/"storage_receipt.json").exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("corrupt", [False, True])
async def test_unknown_or_mismatching_write_stops_later_writes_without_deleting(tmp_path, corrupt):
    source = tmp_path/"inputs"
    source.mkdir()
    for i in range(2):
        (source/f"{i}.txt").write_text(FORMAL, encoding="utf-8")
    storage = FakeStorage(unknown=not corrupt, corrupt=corrupt)
    summary = await runner(storage).run(source, tmp_path/"out", "write", "paragraph_test")
    assert summary["status"] == "failed" and storage.calls.count("write") == 1
    assert summary["files"][0]["storage_status"] == "failed_or_unknown"
    assert summary["files"][1]["storage_status"] == "skipped_after_storage_error"
    assert storage.nodes  # No automatic rollback/deletion claim.


@pytest.mark.asyncio
async def test_existing_label_is_never_overwritten(tmp_path):
    path = tmp_path/"示例规定.txt"
    path.write_text(FORMAL, encoding="utf-8")
    storage = FakeStorage(occupied=True)
    result = await runner(storage).run(path, tmp_path/"out", "write", "old_label")
    assert result["status"] == "failed" and "write" not in storage.calls


@pytest.mark.asyncio
async def test_news_only_writes_when_reference_view_is_explicit(tmp_path):
    path = tmp_path/"管理动态.txt"
    path.write_text(NEWS, encoding="utf-8")
    normative = FakeStorage()
    result = await runner(normative).run(path, tmp_path/"out", "write", "paragraph_normative_test")
    assert result["storage_status"] == "not_written" and "write" not in normative.calls
    reference = FakeStorage()
    result = await runner(reference, "reference").run(path, tmp_path/"out", "write", "paragraph_reference_test")
    assert result["storage_status"] == "confirmed"
    claims = [n["n"]["properties"] for n in reference.nodes if n["n"]["properties"]["label"] == "段落知识单元"]
    assert claims and all(p["使用定位"] == "参考知识" for p in claims)
