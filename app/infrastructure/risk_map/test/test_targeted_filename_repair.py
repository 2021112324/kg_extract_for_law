"""Pure and mocked safety tests for the explicitly scoped maintenance script."""

import copy
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from app.infrastructure.risk_map.maintenance.repair_dual_use_filename import (
    OLD, NEW, TARGET, corrected, guarded_write, replace_filename, validate_quality,
)
from app.infrastructure.risk_map.common.model_client import ModelFailure


@pytest.mark.parametrize("value,expected", [
    (OLD, NEW), ([OLD, "another.txt", OLD], [NEW, "another.txt", NEW]),
    (["prefix_" + OLD], ["prefix_" + OLD]), (None, None),
])
def test_only_exact_filename_values_change(value, expected):
    original = copy.deepcopy(value)
    assert replace_filename(value) == expected
    assert value == original


def test_snapshot_changes_no_other_properties_or_relationships():
    snapshot = {"nodes": [{"n": {"elementId": "x", "properties": {"filename": [OLD], "name": OLD}}}],
                "edges": [{"p": {"properties": {"filename": [OLD]}}}]}
    original = copy.deepcopy(snapshot)
    changed = corrected(snapshot)
    assert snapshot == original
    assert changed["nodes"][0]["n"]["properties"] == {"filename": [NEW], "name": OLD}
    assert changed["edges"] == original["edges"]


@pytest.mark.parametrize("matched", [True, False])
def test_write_is_cas_and_failed_count_rolls_back(tmp_path, matched):
    tx = Mock()
    tx.run.return_value.single.return_value = {"count": 1 if matched else 0}
    session = Mock()
    session.__enter__ = Mock(return_value=session)
    session.__exit__ = Mock(return_value=False)
    session.begin_transaction.return_value = tx
    adapter = SimpleNamespace(database="fake", driver=Mock())
    adapter.driver.session.return_value = session
    rows = {TARGET: [{"id": "x", "before": {"filename": [OLD]}, "patch": {"filename": [NEW]}}]}
    if matched:
        guarded_write(adapter, tmp_path, "rename", rows)
        tx.commit.assert_called_once()
        tx.rollback.assert_not_called()
        assert (tmp_path / "rename_receipt.json").exists()
    else:
        with pytest.raises(ValueError, match="Compare-and-set"):
            guarded_write(adapter, tmp_path, "rename", rows)
        tx.commit.assert_not_called()
        tx.rollback.assert_called_once()
        assert not (tmp_path / "rename_receipt.json").exists()
    assert "properties(n)=row.before" in tx.run.call_args.args[0]
    with pytest.raises(ValueError, match="intent already exists"):
        guarded_write(adapter, tmp_path, "rename", rows)


@pytest.mark.parametrize("fault", [None, "quote", "item", "missing", "retained", "duplicate"])
def test_evidence_review_requires_complete_grounded_decisions(fault):
    article = {"text": "经营者应当保存出口单证五年。"}
    candidates = [{"指标序号": "6.2.1.2", "指标描述": "企业是否建立出口单证留存制度"}]
    result = {"kept_indicator_numbers": ["6.2.1.2"], "decisions": [{"number": "6.2.1.2", "keep": True,
        "supported_item": "出口单证留存制度", "evidence": "应当保存出口单证五年", "reason": "单证留存获得具体支持"}]}
    if fault == "quote":
        result["decisions"][0]["evidence"] = "原文不存在的核查要求"
    elif fault == "item":
        result["decisions"][0]["supported_item"] = "未给出的反制裁事项"
    elif fault == "missing":
        result["decisions"] = []
    elif fault == "retained":
        result["kept_indicator_numbers"] = []
    elif fault == "duplicate":
        result["kept_indicator_numbers"] *= 2
    if fault:
        with pytest.raises(ModelFailure):
            validate_quality(result, article, candidates)
    else:
        assert validate_quality(result, article, candidates) == result
