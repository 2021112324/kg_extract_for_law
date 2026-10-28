"""Final-replay OOM can be verified without replaying writes or model calls."""

from pathlib import Path
from unittest.mock import patch

import pytest

from app.infrastructure.risk_map import execute_mapping, pipeline
from app.infrastructure.risk_map.common.io import atomic_json, read_json
from app.infrastructure.risk_map.finish_memory_error_run import verify_finished_run
from .test_checkpoint_memory import fixture


async def interrupted_after_review(tmp_path):
    config, adapter, client = fixture(tmp_path)
    original = pipeline.verify_target
    calls = []

    def verify(*args):
        calls.append(1)
        if len(calls) == 2:
            raise MemoryError("simulated final replay allocation failure")
        return original(*args)

    with patch.object(pipeline, "verify_target", verify):
        summary, code = await execute_mapping(config, adapter, client=client)
    assert code == 2 and summary["confirmed_counts"]["review"] == 2
    return config, adapter, client, summary


@pytest.mark.asyncio
async def test_read_only_finish_preserves_original_records_and_never_replays(tmp_path):
    config, adapter, client, summary = await interrupted_after_review(tmp_path)
    folder = Path(summary["record_directory"])
    original_records = {path: path.read_bytes() for path in folder.rglob("*") if path.is_file()}
    calls = client.calls
    with patch.object(adapter, "create_subgraph_copy", side_effect=AssertionError("must not copy")), \
         patch.object(adapter, "update_subgraph_article_properties", side_effect=AssertionError("must not write")):
        report, code = verify_finished_run(config, adapter, summary["run_id"])
    assert code == 0 and report["status"] == "verified"
    assert report["inspection"]["status"] == "verified"
    assert report["confirmed_counts"] == {"initialization": 2, "matching": 2, "review": 2}
    assert report["database_writes"] == report["model_requests"] == 0
    assert client.calls == calls
    assert all(path.read_bytes() == raw for path, raw in original_records.items())
    assert Path(report["report_path"]).is_file()


@pytest.mark.asyncio
async def test_graph_content_change_cannot_be_reported_as_finished(tmp_path):
    config, adapter, _, summary = await interrupted_after_review(tmp_path)
    adapter.target["nodes"][0]["n"]["properties"]["法条全文"] = "tampered"
    with pytest.raises(ValueError, match="目标内容"):
        verify_finished_run(config, adapter, summary["run_id"])


@pytest.mark.asyncio
async def test_pending_checkpoint_never_replayed_or_reported_complete(tmp_path):
    config, adapter, _, summary = await interrupted_after_review(tmp_path)
    path = next((Path(summary["record_directory"])/"checkpoints").glob("*_review_ack.json"))
    path.unlink()
    with pytest.raises(ValueError, match="未确认批次"):
        verify_finished_run(config, adapter, summary["run_id"])


@pytest.mark.asyncio
async def test_missing_review_is_not_treated_as_success(tmp_path):
    config, adapter, _, summary = await interrupted_after_review(tmp_path)
    folder = Path(summary["record_directory"])/"checkpoints"
    for path in folder.glob("*_review_*.json"):
        path.unlink()
    with pytest.raises(ValueError, match="任务尚未全部完成"):
        verify_finished_run(config, adapter, summary["run_id"])


@pytest.mark.asyncio
async def test_tampered_input_is_rejected_before_database_read(tmp_path):
    config, adapter, _, summary = await interrupted_after_review(tmp_path)
    (Path(summary["record_directory"])/"inputs/prompts.json").write_text("{}", encoding="utf-8")
    with patch.object(adapter, "read_subgraph_records", side_effect=AssertionError("must not read")):
        with pytest.raises(ValueError, match="冻结输入文件校验失败"):
            verify_finished_run(config, adapter, summary["run_id"])


@pytest.mark.asyncio
async def test_tampered_checkpoint_is_rejected(tmp_path):
    config, adapter, _, summary = await interrupted_after_review(tmp_path)
    path = next((Path(summary["record_directory"])/"checkpoints").glob("*_review_intent.json"))
    value = read_json(path)
    value["results"][0]["status"] = "forged"
    atomic_json(value, path)
    with pytest.raises(ValueError, match="回执校验失败"):
        verify_finished_run(config, adapter, summary["run_id"])


@pytest.mark.asyncio
async def test_changed_connection_and_unrelated_failure_are_rejected(tmp_path):
    config, adapter, _, summary = await interrupted_after_review(tmp_path)
    adapter.uri = "fake://another-server"
    with pytest.raises(ValueError, match="connection"):
        verify_finished_run(config, adapter, summary["run_id"])
    adapter.uri = "fake://isolated"
    path = Path(summary["record_directory"])/"run_failure.json"
    value = read_json(path)
    value["error_type"] = "ConnectionError"
    atomic_json(value, path)
    with pytest.raises(ValueError, match="不是本任务的MemoryError"):
        verify_finished_run(config, adapter, summary["run_id"])
