from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from app.infrastructure.kg_to_natural_language import converter_unified
from app.infrastructure.kg_to_natural_language.converter_v2 import convert_json_to_text_v2
from app.infrastructure.kg_to_natural_language.converter_english_law import (
    convert_english_law_json_to_text,
)
from app.infrastructure.kg_to_natural_language.converter_national_standard import (
    convert_national_standard_json_to_text,
)
from app.infrastructure.kg_to_natural_language.source_context import (
    add_chinese_source_context,
    add_english_source_context,
    clean_source_name,
)


def _node(
    identity: int,
    label: str,
    filename: str | None,
    **properties: Any,
) -> dict[str, Any]:
    node_properties = {
        "id": f"node_{identity}",
        "name": properties.pop("name", f"{label}_{identity}"),
        "label": label,
        **properties,
    }
    if filename is not None:
        node_properties["filename"] = [filename]
    return {
        "n": {
            "identity": identity,
            "labels": ["法律法规条款"],
            "properties": node_properties,
            "elementId": f"4:test:{identity}",
        }
    }


def _edge(identity: int, start: int, end: int, predicate: str) -> dict[str, Any]:
    return {
        "p": {
            "identity": identity,
            "start": start,
            "end": end,
            "type": predicate,
            "properties": {"label": predicate},
            "elementId": f"5:test:{identity}",
            "startNodeElementId": f"4:test:{start}",
            "endNodeElementId": f"4:test:{end}",
        }
    }


def test_source_context_helpers_clean_fallback_and_avoid_duplicates() -> None:
    assert clean_source_name(r"D:\laws\中华人民共和国土壤污染防治法.md") == "中华人民共和国土壤污染防治法"
    assert clean_source_name("Regulation (EU) 2023/956") == "Regulation (EU) 2023/956"
    assert clean_source_name("未知来源") == ""
    chinese = "在《中华人民共和国土壤污染防治法》中，土地使用权人应当修复。"
    assert add_chinese_source_context(chinese, "中华人民共和国土壤污染防治法") == chinese
    located = "在《中华人民共和国土壤污染防治法》第五十六条第一款中，土地使用权人应当修复。"
    assert add_chinese_source_context(
        "土地使用权人应当修复。",
        "中华人民共和国土壤污染防治法",
        "第五十六条第一款",
    ) == located
    assert add_chinese_source_context(
        located,
        "中华人民共和国土壤污染防治法",
        "第五十六条第一款",
    ) == located
    english = "Under Example Act, Article 1 provides: A person shall comply."
    assert add_english_source_context(english, "Example Act") == english
    assert add_english_source_context(english, "Canonical Act") == (
        "Under Canonical Act, Article 1 provides: A person shall comply."
    )
    assert add_chinese_source_context("保留正文。", "未知来源") == "保留正文。"


def test_chinese_law_prefix_file_exception_fallback_and_scope_order(tmp_path: Path) -> None:
    filename = "土壤污染防治法来源.md"
    clause_text = "土壤污染责任人无法认定的，土地使用权人应当实施土壤污染风险管控和修复。"
    nodes = [
        _node(
            1,
            "法规文件",
            filename,
            name="中华人民共和国土壤污染防治法",
            文件全称="中华人民共和国土壤污染防治法",
            文件性质="法律",
        ),
        _node(2, "法条", filename, name="第五十六条", 效力范围="全国范围"),
        _node(
            3,
            "条款单元",
            filename,
            name="第五十六条第一款",
            单元编号="第五十六条第一款",
            条款单元内容=clause_text,
        ),
        _node(
            4,
            "引用依据",
            filename,
            name="中华人民共和国环境保护法",
            文件全称="中华人民共和国环境保护法",
            引用关系="引用",
        ),
        _node(5, "法规依据", filename, name="中华人民共和国宪法"),
        _node(6, "条款单元", "folder/回退法规.txt", name="第一条", 条款单元内容="回退内容。"),
        _node(7, "条款单元", None, name="第二条", 条款单元内容="无来源内容。"),
    ]
    edges = [
        _edge(1, 3, 4, "引用"),
        _edge(2, 1, 5, "依据"),
    ]
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    (input_dir / "node.json").write_text(json.dumps(nodes, ensure_ascii=False), encoding="utf-8")
    (input_dir / "edge.json").write_text(json.dumps(edges, ensure_ascii=False), encoding="utf-8")

    result = convert_json_to_text_v2(
        "法律法规条款测试",
        input_dir=input_dir,
        output_dir=output_dir,
    )
    lines = Path(result["txt_path"]).read_text(encoding="utf-8").splitlines()

    expected_clause = f"在《中华人民共和国土壤污染防治法》第五十六条第一款中，{clause_text}"
    assert expected_clause in lines
    assert "在《回退法规》第一条中，回退内容。" in lines
    assert "第二条规定：无来源内容。" in lines
    assert result["source_context_issue_counts"]["非文件节点缺少有效法规名称"] == 1
    assert "中华人民共和国土壤污染防治法是法律" in lines
    assert not any(line.startswith("在《中华人民共和国土壤污染防治法》中，中华人民共和国土壤污染防治法是") for line in lines)
    reference = next(line for line in lines if "引用中华人民共和国环境保护法" in line)
    assert reference.startswith("在《中华人民共和国土壤污染防治法》中，")
    file_basis = next(line for line in lines if "依据中华人民共和国宪法制定" in line)
    assert not file_basis.startswith("在《")
    scope_index = next(i for i, line in enumerate(lines) if "的效力范围是全国范围" in line)
    assert scope_index > lines.index(expected_clause)
    assert scope_index > lines.index(reference)


def test_standard_and_english_fallback_without_file_nodes(tmp_path: Path) -> None:
    standard_input = tmp_path / "standard-input"
    standard_input.mkdir()
    standard_nodes = [
        _node(
            1,
            "标准要求",
            "folder/回退标准.md",
            name="第一条要求",
            要求内容="设备应设置保护装置。",
        ),
        _node(
            2,
            "标准要求",
            None,
            name="第二条要求",
            要求内容="设备应保持稳定。",
        ),
    ]
    (standard_input / "node.json").write_text(
        json.dumps(standard_nodes, ensure_ascii=False), encoding="utf-8"
    )
    (standard_input / "edge.json").write_text("[]", encoding="utf-8")
    standard_result = convert_national_standard_json_to_text(
        "国家标准测试",
        input_dir=standard_input,
        output_dir=tmp_path / "standard-output",
    )
    standard_lines = Path(standard_result["txt_path"]).read_text(encoding="utf-8").splitlines()
    assert any(line.startswith("在《回退标准》中，") for line in standard_lines)
    assert "设备应保持稳定" in next(line for line in standard_lines if "设备应保持稳定" in line)
    assert standard_result["source_context_issue_counts"]["非文件节点缺少有效标准名称"] == 1

    english_input = tmp_path / "english-input"
    english_input.mkdir()
    english_nodes = [
        _node(
            1,
            "LegalProvision",
            "folder/Fallback Act.md",
            name="Section 1",
            provision_number="Section 1",
            provision_content="A person shall comply.",
        ),
        _node(
            2,
            "LegalProvision",
            None,
            name="Section 2",
            provision_number="Section 2",
            provision_content="A person shall report.",
        ),
    ]
    (english_input / "node.json").write_text(
        json.dumps(english_nodes, ensure_ascii=False), encoding="utf-8"
    )
    (english_input / "edge.json").write_text("[]", encoding="utf-8")
    english_result = convert_english_law_json_to_text(
        "English Law Test",
        input_dir=english_input,
        output_dir=tmp_path / "english-output",
    )
    english_lines = Path(english_result["txt_path"]).read_text(encoding="utf-8").splitlines()
    assert any(line.startswith("Under Fallback Act, Section 1 provides:") for line in english_lines)
    assert any(line.startswith("Section 2 provides:") for line in english_lines)
    assert english_result["validation_issue_counts"]["missing_source_context"] == 1


@pytest.mark.parametrize(
    ("business_label", "converter_name", "expected_kind"),
    [
        ("法规文件", "convert_json_to_text_v2", "chinese_law"),
        ("标准要求", "convert_national_standard_json_to_text", "national_standard"),
        ("LegalDocument", "convert_english_law_json_to_text", "english_law"),
    ],
)
def test_unified_converter_routes_to_specific_updated_converter(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    business_label: str,
    converter_name: str,
    expected_kind: str,
) -> None:
    input_dir = tmp_path / expected_kind
    input_dir.mkdir()
    nodes = [_node(1, business_label, "Example.md", name="Example")]
    (input_dir / "node.json").write_text(
        json.dumps(nodes, ensure_ascii=False), encoding="utf-8"
    )
    (input_dir / "edge.json").write_text("[]", encoding="utf-8")
    calls: list[tuple[str, Path | None, Path | None]] = []

    def fake_converter(
        graph_type: str,
        *,
        input_dir: Path | None = None,
        output_dir: Path | None = None,
    ) -> dict[str, Any]:
        calls.append((graph_type, input_dir, output_dir))
        return {"kind": expected_kind, "total_entries": 0}

    monkeypatch.setattr(converter_unified, converter_name, fake_converter)
    output_dir = tmp_path / "output"
    result = converter_unified.convert_graph_to_text(
        "统一入口测试",
        input_dir=input_dir,
        output_dir=output_dir,
    )

    assert result["kind"] == expected_kind
    assert calls == [("统一入口测试", input_dir, output_dir)]
