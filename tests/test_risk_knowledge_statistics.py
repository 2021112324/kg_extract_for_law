from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.infrastructure.kg_to_natural_language import (
    build_risk_indicator_knowledge_statistics,
)
from app.infrastructure.kg_to_natural_language.risk import statistics
from app.infrastructure.kg_to_natural_language.risk.statistics import (
    RiskIndicatorStatisticsError,
)


def _leaf(sequence: str, description: str = "指标描述") -> dict[str, object]:
    return {
        "序号": sequence,
        "指标描述": description,
        "映射文件": {},
    }


def _write_tree(path: Path, tree: dict[str, object]) -> None:
    path.write_text(
        json.dumps(tree, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _entry(
    knowledge_id: str | None,
    source_file: str | None,
    *,
    knowledge: str | None = None,
    graph_filename: str | None = None,
    graph_category: str = "法律法规条款",
) -> dict[str, object]:
    return {
        "knowledge_id": knowledge_id,
        "knowledge": knowledge or f"知识-{knowledge_id or 'legacy'}",
        "source_file": source_file,
        "graph_filename": graph_filename,
        "graph_category": graph_category,
    }


def _write_artifact(
    root: Path,
    sequence: str,
    entries: list[dict[str, object]],
    *,
    declared_count: int | None = None,
    actual_sequence: str | None = None,
) -> Path:
    path = root / sequence / "all.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    value = {
        "schema_version": "1.0",
        "stage": "knowledge_generation",
        "status": "success",
        "indicator": {
            "number": actual_sequence or sequence,
            "name": f"指标{sequence}",
            "description": "指标描述",
            "risk_type": "测试风险",
        },
        "knowledge_count": len(entries) if declared_count is None else declared_count,
        "knowledge_entries": entries,
    }
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return path


def test_hierarchy_parent_file_dedup_and_cross_root_independence(tmp_path: Path) -> None:
    tree_path = tmp_path / "tree.json"
    temp_root = tmp_path / "temp" / "risk"
    output_path = tmp_path / "data" / "stats" / "知识条目数统计.json"
    _write_tree(
        tree_path,
        {
            "风险一": {
                "指标A": _leaf("1.1.1.1"),
                "指标B": _leaf("1.1.1.2"),
                "指标C": _leaf("1.1.2.1"),
            },
            "风险二": {"指标D": _leaf("2.1.1.1")},
        },
    )
    shared = [_entry("shared-1", r"C:\laws\Shared.txt"), _entry("shared-2", "Shared")]
    _write_artifact(temp_root, "1.1.1.1", shared)
    _write_artifact(
        temp_root,
        "1.1.1.2",
        [
            _entry("shared-1", "SHARED.PDF"),
            _entry("shared-2", "shared.md"),
            _entry("other-1", "Other Law.docx"),
        ],
    )
    _write_artifact(temp_root, "2.1.1.1", shared)

    result = build_risk_indicator_knowledge_statistics(
        risk_tree_path=tree_path,
        temp_risk_root=temp_root,
        output_path=output_path,
    )

    risk_one = result["risk_types"]["风险一"]
    second = risk_one["children"]["1.1"]["children"]["1.1.1"]
    assert second["unique_file_count"] == 2
    assert second["knowledge_entry_count"] == 3
    assert risk_one["knowledge_entry_count"] == 3
    assert risk_one["is_complete"] is False
    assert result["risk_types"]["风险二"]["knowledge_entry_count"] == 2
    assert "knowledge_entry_count" not in result["summary"]
    assert "unique_file_count" not in result["summary"]
    assert "不生成跨风险类型总计" in result["statistics_scope"]["cross_risk_types"]
    assert result["missing_indicators"][0]["indicator_number"] == "1.1.2.1"
    assert output_path.is_file()
    assert json.loads(output_path.read_text(encoding="utf-8")) == result

    repeated = build_risk_indicator_knowledge_statistics(
        risk_tree_path=tree_path,
        temp_risk_root=temp_root,
        output_path=tmp_path / "repeated.json",
    )
    assert repeated["risk_types"] == result["risk_types"]
    assert list(repeated["risk_types"]["风险一"]["children"]) == ["1.1"]
    assert list(repeated["risk_types"]["风险一"]["children"]["1.1"]["children"]) == [
        "1.1.1",
        "1.1.2",
    ]


def test_observed_count_fallback_identity_and_missing_file_warning(tmp_path: Path) -> None:
    tree_path = tmp_path / "tree.json"
    temp_root = tmp_path / "temp"
    output_path = tmp_path / "stats.json"
    _write_tree(tree_path, {"风险一": {"指标A": _leaf("1.1.1.1")}})
    _write_artifact(
        temp_root,
        "1.1.1.1",
        [
            _entry(None, None, graph_filename="Legacy Law.txt", knowledge="旧知识"),
            _entry("stable-1", None, knowledge="缺少来源"),
        ],
        declared_count=9,
    )

    result = build_risk_indicator_knowledge_statistics(
        risk_tree_path=tree_path,
        temp_risk_root=temp_root,
        output_path=output_path,
    )
    third = result["risk_types"]["风险一"]["children"]["1.1"]["children"][
        "1.1.1"
    ]["children"]["1.1.1.1"]
    assert third["knowledge_entry_count"] == 2
    assert third["unique_file_count"] == 1
    assert third["fallback_identity_count"] == 1
    assert third["excluded_from_parent_count"] == 1
    assert result["risk_types"]["风险一"]["knowledge_entry_count"] == 1
    assert {
        warning["code"] for warning in result["warnings"]
    } >= {"legacy_knowledge_id_fallback", "missing_file_identity", "declared_knowledge_count_mismatch"}
    assert result["status"] == "partial_success"


@pytest.mark.parametrize("invalid_kind", ["identity", "json"])
def test_invalid_artifact_is_visible_and_excluded(
    tmp_path: Path,
    invalid_kind: str,
) -> None:
    tree_path = tmp_path / "tree.json"
    temp_root = tmp_path / "temp"
    output_path = tmp_path / "stats.json"
    _write_tree(tree_path, {"风险一": {"指标A": _leaf("1.1.1.1")}})
    artifact_path = _write_artifact(
        temp_root,
        "1.1.1.1",
        [_entry("id-1", "Law.txt")],
        actual_sequence="1.1.1.2" if invalid_kind == "identity" else None,
    )
    if invalid_kind == "json":
        artifact_path.write_text("{broken", encoding="utf-8")

    result = build_risk_indicator_knowledge_statistics(
        risk_tree_path=tree_path,
        temp_risk_root=temp_root,
        output_path=output_path,
    )
    assert result["summary"]["invalid_third_level_count"] == 1
    assert result["invalid_indicators"][0]["indicator_number"] == "1.1.1.1"
    assert result["risk_types"]["风险一"]["knowledge_entry_count"] == 0
    assert result["risk_types"]["风险一"]["is_complete"] is False


def test_repeated_file_mismatch_and_category_collision_use_union(tmp_path: Path) -> None:
    tree_path = tmp_path / "tree.json"
    temp_root = tmp_path / "temp"
    output_path = tmp_path / "stats.json"
    _write_tree(
        tree_path,
        {"风险一": {"指标A": _leaf("1.1.1.1"), "指标B": _leaf("1.1.1.2")}},
    )
    _write_artifact(
        temp_root,
        "1.1.1.1",
        [_entry("id-1", "Same Law.txt", graph_category="法律法规条款")],
    )
    _write_artifact(
        temp_root,
        "1.1.1.2",
        [
            _entry("id-1", "same law.pdf", graph_category="国家标准"),
            _entry("id-2", "same law.pdf", graph_category="国家标准"),
        ],
    )

    result = build_risk_indicator_knowledge_statistics(
        risk_tree_path=tree_path,
        temp_risk_root=temp_root,
        output_path=output_path,
    )
    assert result["risk_types"]["风险一"]["unique_file_count"] == 1
    assert result["risk_types"]["风险一"]["knowledge_entry_count"] == 2
    assert result["status"] == "partial_success"
    conflict = result["file_conflicts"][0]
    assert conflict["conflict_types"] == [
        "knowledge_set_mismatch",
        "graph_category_collision",
    ]
    assert conflict["union_knowledge_count"] == 2
    assert conflict["difference_knowledge_count"] == 1


def test_default_paths_and_atomic_write_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    tree_path = tmp_path / "tree.json"
    temp_root = tmp_path / "temp"
    stats_root = tmp_path / "data" / "stats"
    target = stats_root / "知识条目数统计.json"
    _write_tree(tree_path, {"风险一": {"指标A": _leaf("1.1.1.1")}})
    _write_artifact(temp_root, "1.1.1.1", [_entry("id-1", "Law.txt")])
    monkeypatch.setattr(statistics, "DEFAULT_RISK_TREE_PATH", tree_path)
    monkeypatch.setattr(statistics, "DEFAULT_RISK_TEMP_ROOT", temp_root)
    monkeypatch.setattr(statistics, "DEFAULT_RISK_STATISTICS_PATH", target)

    result = build_risk_indicator_knowledge_statistics()
    assert result["status"] == "success"
    assert target.is_file()

    failed_target = stats_root / "failed.json"
    monkeypatch.setattr(
        statistics.os,
        "replace",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(PermissionError("occupied")),
    )
    with pytest.raises(RiskIndicatorStatisticsError, match="写入失败"):
        build_risk_indicator_knowledge_statistics(
            risk_tree_path=tree_path,
            temp_risk_root=temp_root,
            output_path=failed_target,
        )
    assert not failed_target.exists()
    assert not list(stats_root.glob(".failed.json.*.tmp"))


def test_output_path_must_be_json(tmp_path: Path) -> None:
    with pytest.raises(RiskIndicatorStatisticsError, match="必须是JSON"):
        build_risk_indicator_knowledge_statistics(output_path=tmp_path / "stats.txt")
