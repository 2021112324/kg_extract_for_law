from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from app.infrastructure import kg_to_natural_language as package
from app.infrastructure.kg_to_natural_language import risk_knowledge_generation as pipeline
from app.infrastructure.kg_to_natural_language.risk_graph_query import (
    GraphFilePartition,
    GraphQueryResult,
)
from app.infrastructure.kg_to_natural_language.risk_indicator_tree import (
    RiskIndicatorTreeError,
)


CATEGORIES = ["法律法规条款", "国家标准", "行政监管规则", "英文法规", "合规指引"]


def test_all_configured_categories_have_a_converter() -> None:
    converters = {
        category: pipeline._converter_for_category(category)
        for category in CATEGORIES
    }

    assert all(callable(converter) for converter in converters.values())
    assert (
        converters["合规指引"]
        is pipeline.convert_compliance_guide_json_to_text
    )


def _write_tree(path: Path) -> None:
    mapped = {
        "中国": {
            **{
                f"文件{index}": {
                    "原始文本": f"《文件{index}》",
                    "已找到": f"文件{index}.txt",
                }
                for index in range(1, 7)
            },
            "空文件": {"原始文本": "《空文件》", "已找到": ""},
        }
    }
    tree = {
        "企业关联方合规风险": {
            "测试指标": {
                "序号": "1.1.1.1",
                "指标描述": "测试指标的完整描述",
                "映射文件": mapped,
            },
            "另一指标": {
                "序号": "1.1.1.2",
                "指标描述": "另一个测试指标",
                "映射文件": {},
            },
        }
    }
    path.write_text(json.dumps(tree, ensure_ascii=False), encoding="utf-8")


def _partition(index: int, category: str) -> GraphFilePartition:
    node = {
        "identity": index,
        "labels": [category],
        "properties": {
            "filename": [f"文件{index}.txt"],
            "label": "测试节点",
            "name": f"节点{index}",
        },
        "elementId": f"node-{index}",
    }
    return GraphFilePartition(
        normalized_key=f"文件{index}",
        graph_category=category,
        graph_label=category,
        graph_filenames=(f"文件{index}.txt",),
        node_records=[{"n": node}],
        edge_records=[],
    )


def _fake_query(_: object) -> GraphQueryResult:
    return GraphQueryResult(
        partitions=[_partition(index, category) for index, category in enumerate(CATEGORIES, 1)],
        unmatched_keys=["文件6"],
        cross_label_matches={},
        relationship_issue_count=1,
        queried_labels=list(CATEGORIES),
    )


def _fake_converter_for(category: str):
    def convert(
        graph_type: str,
        *,
        input_dir: str | Path,
        output_dir: str | Path,
    ) -> dict[str, Any]:
        assert graph_type == category
        assert (Path(input_dir) / "node.json").is_file()
        output = Path(output_dir)
        txt_path = output / "txt" / f"{category}.txt"
        txt_path.parent.mkdir(parents=True, exist_ok=True)
        first = f"{category}知识一"
        lines = [first, first, f"{category}知识二"]
        txt_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        result: dict[str, Any] = {
            "total_entries": len(lines),
            "source_files": 1,
            "txt_path": str(txt_path),
        }
        if category in {"国家标准", "英文法规"}:
            jsonl_path = output / "jsonl" / f"{category}.jsonl"
            jsonl_path.parent.mkdir(parents=True, exist_ok=True)
            jsonl_path.write_text(
                "\n".join(
                    json.dumps({"content": line, "knowledge_type": "测试知识"}, ensure_ascii=False)
                    for line in lines
                )
                + "\n",
                encoding="utf-8",
            )
            result["jsonl_path"] = str(jsonl_path)
        return result

    return convert


def test_stage_one_generates_per_file_and_all_json(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tree_path = tmp_path / "tree.json"
    output_dir = tmp_path / "risk" / "1.1.1.1"
    _write_tree(tree_path)
    monkeypatch.setattr(pipeline, "query_mapped_file_subgraphs", _fake_query)
    monkeypatch.setattr(pipeline, "_converter_for_category", _fake_converter_for)

    result = package.generate_risk_indicator_knowledge(
        "1.1.1.1",
        risk_tree_path=tree_path,
        output_dir=output_dir,
    )

    assert result["status"] == "success"
    assert result["indicator_name"] == "测试指标"
    assert result["indicator_description"] == "测试指标的完整描述"
    assert result["unique_found_file_count"] == 6
    assert result["matched_source_file_count"] == 5
    assert result["unmatched_file_count"] == 1
    assert result["knowledge_count"] == 10
    assert len(result["per_file_json_paths"]) == 5

    all_json = json.loads((output_dir / "all.json").read_text(encoding="utf-8"))
    assert all_json["indicator"] == {
        "number": "1.1.1.1",
        "name": "测试指标",
        "description": "测试指标的完整描述",
        "risk_type": "企业关联方合规风险",
    }
    assert {item["graph_category"] for item in all_json["matched_files"]} == set(CATEGORIES)
    assert all_json["unmatched_files"][0]["found_filename"] == "文件6.txt"
    assert all(entry["ranking_status"] == "pending" for entry in all_json["knowledge_entries"])
    assert all(entry["relevance_score"] is None for entry in all_json["knowledge_entries"])
    assert any(
        entry["converter_metadata"] == {"knowledge_type": "测试知识"}
        for entry in all_json["knowledge_entries"]
    )


def test_failed_generation_preserves_previous_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tree_path = tmp_path / "tree.json"
    output_dir = tmp_path / "risk" / "1.1.1.1"
    output_dir.mkdir(parents=True)
    (output_dir / "all.json").write_text('{"old": true}', encoding="utf-8")
    _write_tree(tree_path)
    monkeypatch.setattr(pipeline, "query_mapped_file_subgraphs", _fake_query)

    def fail(_: str):
        raise RuntimeError("转换器故障")

    monkeypatch.setattr(pipeline, "_converter_for_category", fail)
    with pytest.raises(pipeline.RiskKnowledgeGenerationError):
        package.generate_risk_indicator_knowledge(
            "1.1.1.1",
            risk_tree_path=tree_path,
            output_dir=output_dir,
        )
    assert (output_dir / "all.json").read_text(encoding="utf-8") == '{"old": true}'


def test_path_traversal_rejected_and_indicator_outputs_are_isolated(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tree_path = tmp_path / "tree.json"
    _write_tree(tree_path)
    with pytest.raises(RiskIndicatorTreeError):
        package.generate_risk_indicator_knowledge(
            "../1.1.1.1",
            risk_tree_path=tree_path,
            output_dir=tmp_path / "bad",
        )

    monkeypatch.setattr(
        pipeline,
        "query_mapped_file_subgraphs",
        lambda _: GraphQueryResult([], [], {}, 0, list(CATEGORIES)),
    )
    first = tmp_path / "risk" / "1.1.1.1"
    second = tmp_path / "risk" / "1.1.1.2"
    package.generate_risk_indicator_knowledge(
        "1.1.1.1", risk_tree_path=tree_path, output_dir=first
    )
    package.generate_risk_indicator_knowledge(
        "1.1.1.2", risk_tree_path=tree_path, output_dir=second
    )
    assert (first / "all.json").is_file()
    assert (second / "all.json").is_file()
    assert package.process_graph is not None
    assert package.process_graph_v2 is not None
    assert package.process_graphs_by_type is not None
