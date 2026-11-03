"""Expert associations are preserved; diagnostics are local and protocols isolated."""

import asyncio
import copy
from dataclasses import replace
import json
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from app.infrastructure.risk_map import execute_mapping
from app.infrastructure.risk_map.common.io import atomic_json, read_json
from app.infrastructure.risk_map.common.model_client import ModelFailure
from app.infrastructure.risk_map.config import CANDIDATES, FINAL, PROTOCOL
from app.infrastructure.risk_map.local_records import RunRecords
from app.infrastructure.risk_map.maintenance import repair_dual_use_filename as legacy
from .test_checkpoint_memory import fixture
from .test_copy_first import Client, TEXT, payload_data, tree


def files(data, number=1):
    return data["产品法律风险"]["一级"]["二级"][f"指标{number}"]["映射文件"]["中国"]


@pytest.mark.asyncio
async def test_nonexact_location_preserves_all_expert_indicators_and_never_calls_review(tmp_path):
    config, adapter, _ = fixture(tmp_path)
    atomic_json(tree(30, "企业应当开展合规管理。"), config.tree_path)

    class Locator(Client):
        async def complete(self, payload):
            assert set(payload_data(payload)) == {"article_number", "article_text", "description"}
            assert "candidates" not in payload_data(payload)
            return await super().complete(payload)

    client = Locator(adapter)
    with patch("app.infrastructure.risk_map.review.review.review_article", side_effect=AssertionError("review forbidden")):
        summary, code = await execute_mapping(config, adapter, client=client)
    assert code == 0 and client.calls > 0
    for row in adapter.target["nodes"]:
        props = row["n"]["properties"]
        assert props[CANDIDATES] == props[FINAL] and len(props[FINAL]) == 30
    folder = Path(summary["record_directory"])
    assert not (folder / "review").exists()
    assert not list((folder / "checkpoints").glob("*_review_*.json"))
    assert not summary["semantic_review_enabled"] and "review_audit" not in summary
    assert summary["location"]["saved_links"] == 60
    assert summary["location"]["mapped_articles"] == 2
    assert summary["location"]["mapped_files"] == 1
    assert set(read_json(folder / "inputs/prompts.json")) == {"matching"}


@pytest.mark.asyncio
async def test_partial_success_survives_unrelated_failed_reference(tmp_path):
    config, adapter, client = fixture(tmp_path, 1)
    data = tree(2)
    files(data, 2)["测试法"]["知识依据"] = "企业应当开展专项风险评估。"
    atomic_json(data, config.tree_path)
    client.failure = True
    summary, code = await execute_mapping(config, adapter, client=client)
    props = adapter.target["nodes"][0]["n"]["properties"]
    assert code == 2 and summary["article_statuses"] == {"部分完成": 1}
    assert props[CANDIDATES] == props[FINAL]
    assert [json.loads(item)["指标序号"] for item in props[FINAL]] == ["2.1.1.1"]
    assert summary["location"]["saved_links"] == 1 and summary["failed_stage_tasks"] == 1
    unresolved = read_json(Path(summary["record_directory"]) / "unresolved_basis.json")
    assert {r["indicator_number"] for r in unresolved["entries"]} == {"2.1.1.2"}
    assert unresolved["entries"][0]["reason"] == "location_failed_or_incomplete"


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["no_match", "missing_file", "missing_text", "ambiguity", "no_articles"])
async def test_unresolved_reasons_remain_local(tmp_path, mode):
    config, adapter, _ = fixture(tmp_path, 0 if mode == "no_articles" else 1)
    data = tree(1, "企业应当开展专项风险评估。")
    atomic_json(data, config.tree_path)
    if mode == "missing_file":
        adapter.source["nodes"][0]["n"]["properties"]["filename"] = ["完全不同的文件"]
    elif mode == "missing_text":
        adapter.source["nodes"][0]["n"]["properties"].pop("法条全文")
    elif mode == "ambiguity":
        second = copy.deepcopy(files(data)["测试法"])
        second["依据文件名"] = "另外一份法"
        files(data)["另外一份法"] = second
        atomic_json(data, config.tree_path)
        adapter.source["nodes"][0]["n"]["properties"]["filename"] = ["测试法", "另外一份法"]
    elif mode == "no_articles":
        adapter.source["nodes"][0]["n"]["properties"].update(filename=["测试法"], 文件全称="测试法")

    class Negative(Client):
        async def complete(self, payload):
            await super().complete(payload)
            return '{"match":"否","reason":"未定位到该依据"}'

    summary, code = await execute_mapping(config, adapter, client=Negative(adapter))
    report = read_json(Path(summary["record_directory"]) / "unresolved_basis.json")
    assert report["entries"]
    reasons = {r["reason"] for r in report["entries"]}
    expected = {"no_match": "reference_not_located", "missing_file": "graph_file_not_found",
                "missing_text": "location_failed_or_incomplete", "ambiguity": "file_ambiguity",
                "no_articles": "no_processed_article_for_reference"}
    assert expected[mode] in reasons
    for row in adapter.target["nodes"]:
        props = row["n"]["properties"]
        assert not {"reason", "status", "provenance", "未定位原因", "三级指标映射状态"} & props.keys()
        if mode == "no_match":
            assert props[FINAL] == []
        elif mode != "no_articles":
            assert FINAL not in props
    assert code == (2 if mode == "missing_text" else 0)
    assert summary["mapping_status"] != "completed"


@pytest.mark.asyncio
async def test_same_indicator_multiple_excerpt_hits_are_deduplicated(tmp_path):
    config, adapter, client = fixture(tmp_path, 1)
    atomic_json(tree(1, TEXT + "\n第二条 企业应当建立管理制度。"), config.tree_path)
    summary, code = await execute_mapping(config, adapter, client=client)
    assert code == 0
    assert len(adapter.target["nodes"][0]["n"]["properties"][FINAL]) == 1
    assert summary["location"]["saved_links"] == 1


@pytest.mark.asyncio
async def test_file_match_does_not_map_unmatched_articles_or_add_other_indicators(tmp_path):
    config, adapter, _ = fixture(tmp_path, 2)
    atomic_json(tree(1, "企业应当开展专项风险评估。"), config.tree_path)
    adapter.source["nodes"][1]["n"]["properties"]["法条全文"] = "第二条 无对应内容。"

    class Selective(Client):
        async def complete(self, payload):
            await super().complete(payload)
            positive = payload_data(payload)["article_text"] == TEXT
            return {"match": "是" if positive else "否", "reason": "仅定位输入依据，不判断指标"}

    summary, code = await execute_mapping(config, adapter, client=Selective(adapter))
    assert code == 0
    assert len(adapter.target["nodes"][0]["n"]["properties"][FINAL]) == 1
    assert adapter.target["nodes"][1]["n"]["properties"][FINAL] == []
    assert summary["location"]["mapped_files"] == summary["location"]["mapped_articles"] == 1


@pytest.mark.asyncio
async def test_model_cannot_return_self_invented_indicator_fields(tmp_path):
    config, adapter, _ = fixture(tmp_path, 1)
    atomic_json(tree(1, "企业应当开展专项风险评估。"), config.tree_path)

    class Invented(Client):
        async def complete(self, payload):
            await super().complete(payload)
            return {"match": "是", "reason": "对应", "indicators": ["9.9.9.9"]}

    summary, code = await execute_mapping(config, adapter, client=Invented(adapter))
    assert code == 2 and summary["failed_stage_tasks"] == 1
    assert FINAL not in adapter.target["nodes"][0]["n"]["properties"]


@pytest.mark.asyncio
async def test_model_requests_reach_but_never_exceed_twenty_concurrent(tmp_path):
    config, adapter, client = fixture(tmp_path, 41)
    atomic_json(tree(1, "企业应当开展专项风险评估。"), config.tree_path)
    for i, row in enumerate(adapter.source["nodes"]):
        row["n"]["properties"]["法条全文"] = TEXT + f"编号-{i}。"
    summary, code = await execute_mapping(config, adapter, client=client)
    assert code == 0 and client.calls == 41 and client.maximum == 20
    assert summary["location"]["saved_links"] == 41
    assert [n for kind, n in adapter.calls[1:] if kind == "update"] == [20, 20, 1, 20, 20, 1]


@pytest.mark.asyncio
async def test_resume_keeps_twenty_confirmed_location_results_without_replaying_them(tmp_path):
    config, adapter, _ = fixture(tmp_path, 21)
    atomic_json(tree(1, "企业应当开展专项风险评估。"), config.tree_path)
    for i, row in enumerate(adapter.source["nodes"]):
        row["n"]["properties"]["法条全文"] = TEXT + f"编号-{i}。"
    reached = asyncio.Event()

    class PauseLast(Client):
        async def complete(self, payload):
            if payload_data(payload)["article_text"].endswith("编号-9。"):
                reached.set()
                await asyncio.Event().wait()
            return await super().complete(payload)

    task = asyncio.create_task(execute_mapping(config, adapter, client=PauseLast(adapter)))
    await asyncio.wait_for(reached.wait(), 20)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    run_id = next((config.output_root / "runs").iterdir()).name
    records = RunRecords(config.output_root, run_id, resume=True)
    assert len(records.batches()[0]["matching"]) == 20
    before = len(adapter.calls)
    client = Client(adapter)
    summary, code = await execute_mapping(config, adapter, action="resume", run_id=run_id, client=client)
    assert code == 0 and client.calls == 1
    assert summary["location"]["saved_links"] == 21
    assert adapter.calls[before:] == [("read", 1), ("update", 1)]


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["resume", "inspect"])
async def test_old_protocol_is_rejected_without_any_mutation_or_requests(tmp_path, action):
    config, adapter, client = fixture(tmp_path)
    summary, _ = await execute_mapping(config, adapter, client=client)
    folder = Path(summary["record_directory"])
    run = read_json(folder / "run.json")
    atomic_json({**run, "protocol": "copy-first-many-to-many-v3-audit-fixes"}, folder / "run.json")
    before = {p: p.read_bytes() for p in folder.rglob("*") if p.is_file()}
    graph = copy.deepcopy(adapter.target)
    with patch.object(adapter, "read_subgraph_records", side_effect=AssertionError("no DB reads")):
        with pytest.raises(ValueError, match="旧语义审查"):
            await execute_mapping(config, adapter, action=action, run_id=summary["run_id"], client=client)
    assert graph == adapter.target and before == {p: p.read_bytes() for p in before}


@pytest.mark.asyncio
async def test_new_protocol_does_not_read_old_cache_or_inherit_old_final(tmp_path):
    config, adapter, client = fixture(tmp_path, 1)
    adapter.source["nodes"][0]["n"]["properties"].update({CANDIDATES: ["obsolete"], FINAL: []})
    atomic_json(tree(1, "企业应当开展专项风险评估。"), config.tree_path)
    old_cache = config.output_root / "cache/copy-first-many-to-many-v3-audit-fixes/old.json"
    atomic_json({"response": '{"removed_indicator_numbers":["2.1.1.1"],"reasons":{}}'}, old_cache)
    old_bytes = old_cache.read_bytes()
    summary, code = await execute_mapping(config, adapter, client=client)
    assert code == 0 and client.calls == 1
    assert len(adapter.target["nodes"][0]["n"]["properties"][FINAL]) == 1
    assert summary["protocol"] == PROTOCOL and old_cache.read_bytes() == old_bytes
    assert (config.output_root / "cache" / PROTOCOL).is_dir()


@pytest.mark.asyncio
async def test_old_unknown_commit_blocks_new_run_for_same_target(tmp_path):
    config, adapter, client = fixture(tmp_path, 1)
    from app.infrastructure.risk_map.common.io import content_hash
    records = RunRecords(config.output_root, "historical")
    connection = content_hash([adapter.uri, adapter.username, adapter.database])
    records.save("run.json", {"protocol": "old", "connection": connection, "target_label": config.target_label})
    records.save("copy_intent.json", {})
    summary, code = await execute_mapping(config, adapter, client=client)
    assert code == 2 and summary["mapping_status"] == "write_unknown"
    assert adapter.target is None and client.calls == 0


@pytest.mark.asyncio
async def test_unresolved_report_save_failure_is_not_success(tmp_path):
    config, adapter, client = fixture(tmp_path)
    original = RunRecords.save

    def fail(records, name, value):
        if name == "unresolved_basis.json":
            raise OSError("simulated unresolved report disk failure")
        return original(records, name, value)

    with patch.object(RunRecords, "save", fail):
        summary, code = await execute_mapping(config, adapter, client=client)
    assert code == 2 and summary["record_validation_error"]
    assert summary["status"] == "record_error"


@pytest.mark.parametrize("entry", ["prepare", "manifest_for", "apply_mapping", "adopt_verified_quality", "main"])
def test_retired_maintenance_cannot_access_graph_or_mutate_local_results(tmp_path, entry):
    adapter = Mock()
    with pytest.raises(ValueError, match="旧定向修复入口已停用"):
        if entry == "main":
            legacy.main()
        elif entry == "adopt_verified_quality":
            legacy.adopt_verified_quality(tmp_path, [])
        else:
            getattr(legacy, entry)(adapter, tmp_path)
    assert not adapter.mock_calls and not list(tmp_path.iterdir())


@pytest.mark.asyncio
@pytest.mark.parametrize("entry", ["map_articles", "quality_review"])
async def test_retired_model_maintenance_cannot_send_requests(tmp_path, entry):
    adapter = Mock()
    with pytest.raises(ValueError, match="旧定向修复入口已停用"):
        await getattr(legacy, entry)(adapter, tmp_path)
    assert not adapter.mock_calls and not list(tmp_path.iterdir())
