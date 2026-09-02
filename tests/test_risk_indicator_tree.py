from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.infrastructure.kg_to_natural_language.risk_indicator_config import (
    DEFAULT_RISK_TREE_PATH,
)
from app.infrastructure.kg_to_natural_language.risk_indicator_tree import (
    RiskIndicatorTreeError,
    filename_query_variants,
    list_indicator_numbers,
    load_indicator_context,
    load_indicator_contexts,
    normalize_filename,
)


def _write_tree(path: Path, indicators: dict[str, object]) -> None:
    path.write_text(
        json.dumps({"企业关联方合规风险": indicators}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def test_repository_tree_audit_and_indicator_1111() -> None:
    tree = json.loads(DEFAULT_RISK_TREE_PATH.read_text(encoding="utf-8"))
    leaves = [leaf for indicators in tree.values() for leaf in indicators.values()]
    sequences = [leaf["序号"] for leaf in leaves]
    assert len(leaves) == len(set(sequences)) == 183
    assert all(str(leaf.get("指标描述") or "").strip() for leaf in leaves)

    context = load_indicator_context("1.1.1.1")
    assert context.indicator_name == "企业股东及投资方是否向企业披露重大关联交易情况"
    assert context.indicator_description.startswith("是指企业的股东")
    assert context.risk_type == "企业关联方合规风险"
    assert context.mapping_reference_count >= len(context.mapped_files) > 0
    assert all(mapped_file.mapping_sources for mapped_file in context.mapped_files)


def test_mapping_selection_deduplication_and_filename_normalization(tmp_path: Path) -> None:
    tree_path = tmp_path / "tree.json"
    _write_tree(
        tree_path,
        {
            "测试指标": {
                "序号": "1.1.1.1",
                "指标描述": "测试指标的完整描述",
                "映射文件": {
                    "中国": {
                        "文件A": {"原始文本": "《文件A》", "已找到": "folder/文件 A.TXT"},
                        "文件A别名": {"原始文本": "《文件A别名》", "已找到": "文件 A.md"},
                        "未找到": {"原始文本": "《未找到》", "已找到": ""},
                    }
                },
            }
        },
    )

    context = load_indicator_context("1.1.1.1", tree_path=tree_path)
    assert context.mapping_reference_count == 2
    assert context.ignored_empty_found_count == 1
    assert len(context.mapped_files) == 1
    assert len(context.mapped_files[0].mapping_sources) == 2
    assert normalize_filename(r"D:\laws\文件  A.md.txt") == "文件 a"
    variants = filename_query_variants("文件 A.txt")
    assert {"文件 a", "文件 a.txt", "文件 a.md"} <= variants


def test_invalid_unknown_duplicate_and_legacy_description(tmp_path: Path) -> None:
    with pytest.raises(RiskIndicatorTreeError, match="格式错误"):
        load_indicator_context("../1.1.1.1")

    tree_path = tmp_path / "tree.json"
    _write_tree(
        tree_path,
        {
            "指标A": {
                "序号": "1.1.1.1",
                "映射文件": {},
            }
        },
    )
    with pytest.warns(RuntimeWarning, match="缺少指标描述"):
        legacy = load_indicator_context("1.1.1.1", tree_path=tree_path)
    assert legacy.indicator_description == "指标A"
    with pytest.raises(RiskIndicatorTreeError, match="不存在"):
        load_indicator_context("1.1.1.2", tree_path=tree_path)

    duplicate = {
        "风险A": {
            "指标A": {"序号": "1.1.1.1", "指标描述": "A", "映射文件": {}}
        },
        "风险B": {
            "指标B": {"序号": "1.1.1.1", "指标描述": "B", "映射文件": {}}
        },
    }
    tree_path.write_text(json.dumps(duplicate, ensure_ascii=False), encoding="utf-8")
    with pytest.raises(RiskIndicatorTreeError, match="重复"):
        load_indicator_context("1.1.1.1", tree_path=tree_path)


def test_load_all_contexts_preserves_six_roots_and_numeric_order(tmp_path: Path) -> None:
    tree_path = tmp_path / "tree.json"
    tree: dict[str, object] = {}
    for root_number in range(6, 0, -1):
        sequence = f"{root_number}.10.2.1" if root_number == 1 else f"{root_number}.1.1.1"
        tree[f"风险{root_number}"] = {
            f"指标{root_number}": {
                "序号": sequence,
                "指标描述": f"描述{root_number}",
                "映射文件": {},
            }
        }
    tree["风险1"]["指标1-补充"] = {
        "序号": "1.2.1.1",
        "指标描述": "描述1-补充",
        "映射文件": {},
    }
    tree_path.write_text(json.dumps(tree, ensure_ascii=False), encoding="utf-8")

    contexts = load_indicator_contexts(tree_path=tree_path)
    numbers = tuple(context.indicator_number for context in contexts)
    assert numbers == (
        "1.2.1.1",
        "1.10.2.1",
        "2.1.1.1",
        "3.1.1.1",
        "4.1.1.1",
        "5.1.1.1",
        "6.1.1.1",
    )
    assert list_indicator_numbers(tree_path=tree_path) == numbers
    assert {context.risk_type for context in contexts} == {
        f"风险{index}" for index in range(1, 7)
    }


def test_load_all_contexts_rejects_missing_description_when_strict(tmp_path: Path) -> None:
    tree_path = tmp_path / "tree.json"
    _write_tree(
        tree_path,
        {"指标A": {"序号": "1.1.1.1", "映射文件": {}}},
    )
    with pytest.raises(RiskIndicatorTreeError, match="缺少正式指标描述"):
        load_indicator_contexts(
            tree_path=tree_path,
            allow_legacy_description=False,
        )
