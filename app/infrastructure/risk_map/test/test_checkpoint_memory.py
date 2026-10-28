"""Checkpoint replay must not duplicate live history or mask an original OOM."""
import json
from pathlib import Path
from unittest.mock import patch

import pytest

from app.infrastructure.risk_map import MappingConfig, ModelConfig, execute_mapping
from app.infrastructure.risk_map.common.io import atomic_json, read_json
from app.infrastructure.risk_map.config import CANDIDATES, FINAL
from app.infrastructure.risk_map.local_records import RunRecords
from app.infrastructure.risk_map import pipeline
from .test_copy_first import Client, FakeAdapter, tree


def fixture(tmp_path, count=2):
    path = tmp_path / "tree.json"
    atomic_json(tree(), path)
    config = MappingConfig("source", "target", tree_path=path, output_root=tmp_path/"out",
                           write_enabled=True, exclusive_target=True, model=ModelConfig(retry_delay=0))
    adapter = FakeAdapter(count)
    return config, adapter, Client(adapter)


@pytest.mark.asyncio
async def test_full_history_is_loaded_once_per_run_not_again_at_completion(tmp_path):
    config, adapter, client = fixture(tmp_path)
    original = RunRecords.batches
    calls = []

    def once(records, **kwargs):
        calls.append(kwargs)
        if len(calls) > 1:
            raise MemoryError("unnecessary full replay")
        return original(records, **kwargs)

    with patch.object(RunRecords, "batches", once):
        summary, code = await execute_mapping(config, adapter, client=client)
    assert code == 0 and summary["inspection"]["status"] == "verified"
    assert calls == [{"include_patches": False}]
    assert summary["confirmed_counts"] == {"initialization": 2, "matching": 2, "review": 2}
    before = len(adapter.calls)
    calls.clear()
    with patch.object(RunRecords, "batches", once):
        resumed, code = await execute_mapping(config, adapter, action="resume", run_id=summary["run_id"], client=client)
    assert code == 0 and len(adapter.calls) == before
    assert resumed["confirmed_counts"] == summary["confirmed_counts"]
    assert calls == [{"include_patches": False}]


@pytest.mark.asyncio
@pytest.mark.parametrize("message", ["original checkpoint allocation failure", ""])
async def test_initial_replay_oom_is_not_retried_while_building_failure_summary(tmp_path, message):
    config, adapter, client = fixture(tmp_path)
    with patch.object(RunRecords, "batches", side_effect=MemoryError(message)) as replay:
        summary, code = await execute_mapping(config, adapter, client=client)
    assert replay.call_count == 1 and code == 2
    assert summary["copy_status"] == "committed" and summary["error_type"] == "MemoryError"
    assert summary["error"] == (message or "MemoryError")
    assert summary["record_validation_error"] and summary["unprocessed"] is None
    saved = read_json(Path(summary["record_directory"])/"summary.json")
    assert saved["error_type"] == "MemoryError"
    assert client.calls == 0


@pytest.mark.asyncio
async def test_end_verification_oom_preserves_confirmed_counts_and_original_error(tmp_path):
    config, adapter, client = fixture(tmp_path)
    original = pipeline.verify_target
    calls = []

    def verify(*args):
        calls.append(1)
        if len(calls) == 2:
            raise MemoryError("original final verification failure")
        return original(*args)

    with patch.object(pipeline, "verify_target", verify), patch.object(RunRecords, "batches", autospec=True, side_effect=RunRecords.batches) as replay:
        summary, code = await execute_mapping(config, adapter, client=client)
    assert code == 2 and replay.call_count == 1
    assert summary["error_type"] == "MemoryError" and summary["error"] == "original final verification failure"
    assert summary["confirmed_counts"]["review"] == 2 and summary["unprocessed"] == 0
    assert not summary.get("record_validation_error")


@pytest.mark.asyncio
async def test_unknown_write_still_blocks_replay_and_is_reported_pending(tmp_path):
    config, adapter, client = fixture(tmp_path)
    adapter.unknown = True
    summary, code = await execute_mapping(config, adapter, client=client)
    assert code == 2 and summary["mapping_status"] == "write_unknown"
    assert summary["pending_batches"] == 1
    count = len(adapter.calls)
    resumed, code = await execute_mapping(config, adapter, action="resume", run_id=summary["run_id"], client=client)
    assert code == 2 and resumed["mapping_status"] == "write_unknown"
    assert len(adapter.calls) == count


def ledger(tmp_path):
    records = RunRecords(tmp_path)
    for phase, properties in [("initialization", {CANDIDATES: None, FINAL: None}),
                              ("matching", {CANDIDATES: ["candidate " + "x"*10000], FINAL: None}),
                              ("review", {FINAL: []})]:
        rows = [{"id": "article", "properties": properties}]
        results = [{"id": "article", "properties": properties, "status": phase}]
        key, digest = records.intent(phase, rows, results)
        records.acknowledge(key, digest, {"status": "committed", "ids": ["article"]})
    return records


def test_lean_replay_keeps_protocol_validation_and_shares_property_payload(tmp_path):
    records = ledger(tmp_path)
    before = {p: p.read_bytes() for p in records.path.rglob("*.json")}
    confirmed, pending, patches = records.batches()
    lean, pending_lean, discarded = records.batches(include_patches=False)
    assert confirmed == lean and pending == pending_lean == [] and discarded == []
    assert patches[1]["properties"] is confirmed["matching"]["article"]["properties"]
    assert before == {p: p.read_bytes() for p in records.path.rglob("*.json")}
    intent_path = next((records.path/"checkpoints").glob("*_matching_intent.json"))
    value = read_json(intent_path)
    value["results"][0]["status"] = "tampered"
    atomic_json(value, intent_path)
    with pytest.raises(ValueError, match="回执校验"):
        records.batches(include_patches=False)


def test_pending_discovery_never_reads_payloads(tmp_path):
    records = ledger(tmp_path)
    checkpoint = records.path/"checkpoints/00000001_matching_ack.json"
    checkpoint.unlink()
    with patch("app.infrastructure.risk_map.local_records.read_json", side_effect=AssertionError("must not decode")):
        pending = records.pending_batches()
    assert len(pending) == 1 and "matching_intent" in pending[0]


def test_checkpoint_oom_includes_the_file_and_size(tmp_path):
    records = ledger(tmp_path)
    with patch("app.infrastructure.risk_map.local_records.read_json", side_effect=MemoryError()):
        with pytest.raises(MemoryError, match="file_bytes=.*confirmed_tasks=0"):
            records.batches(include_patches=False)


def test_streamed_patches_revalidate_disk_instead_of_trusting_cached_results(tmp_path):
    records = ledger(tmp_path)
    confirmed, pending, patches = records.batches()
    assert list(records.iter_patches()) == patches
    intent_path = next((records.path/"checkpoints").glob("*_review_intent.json"))
    intent = read_json(intent_path)
    intent["results"][0]["status"] = "tampered after initial recovery"
    atomic_json(intent, intent_path)
    with pytest.raises(ValueError, match="回执校验"):
        list(records.iter_patches())


@pytest.mark.asyncio
async def test_end_verification_still_rejects_record_tampering_after_ack(tmp_path):
    config, adapter, client = fixture(tmp_path)
    original = RunRecords.acknowledge

    def acknowledge(records, key, digest, receipt):
        original(records, key, digest, receipt)
        if key.endswith("review"):
            path = records.path/"checkpoints"/(key+"_intent.json")
            value = read_json(path)
            value["results"][0]["status"] = "tampered after acknowledgement"
            atomic_json(value, path)

    with patch.object(RunRecords, "acknowledge", acknowledge):
        summary, code = await execute_mapping(config, adapter, client=client)
    assert code == 2 and "回执校验失败" in summary["error"]
    assert summary["mapping_status"] != "completed"
