from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from app.infrastructure.kg_to_natural_language import convert_english_law_json_to_text


def _node(identity: int, label: str, filename: str, **properties: Any) -> dict[str, Any]:
    node_properties = {
        "id": properties.pop("id", f"node_{identity}"),
        "name": properties.pop("name", f"{label}_{identity}"),
        "label": label,
        "filename": [filename],
        **properties,
    }
    return {
        "n": {
            "identity": identity,
            "labels": ["EnglishLawTestGraph"],
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


def _convert(
    tmp_path: Path,
    nodes: list[dict[str, Any]],
    edges: list[dict[str, Any]],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir(parents=True)
    (input_dir / "node.json").write_text(
        json.dumps(nodes, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (input_dir / "edge.json").write_text(
        json.dumps(edges, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    result = convert_english_law_json_to_text(
        "English Law Test",
        input_dir=input_dir,
        output_dir=output_dir,
    )
    entries = [
        json.loads(line)
        for line in Path(result["jsonl_path"]).read_text(encoding="utf-8").splitlines()
        if line
    ]
    return result, entries


def test_generates_multi_level_english_knowledge_and_filters_uncertain_rules(
    tmp_path: Path,
) -> None:
    filename = "Example Regulation.md"
    nodes = [
        _node(
            1,
            "LegalDocument",
            filename,
            name="Regulation (EU) 2026/1",
            document_name="Regulation (EU) 2026/1 on compliance records",
            document_number="(EU) 2026/1",
            document_type="Regulation",
            issuing_authority="the European Parliament and the Council",
            publication_date="1 January 2026",
            effective_date="2 January 2026",
            subject_matter="compliance records",
            purpose="to improve regulatory traceability",
            scope_of_application="economic operators placing products on the Union market",
            document_format="format_one_eu_regulation_directive",
            risk_types=["企业国际化经营合规风险"],
            unexpected_dynamic="must not become prose",
        ),
        _node(
            2,
            "LegalBasis",
            filename,
            name="Article 114 TFEU",
            citation_text="Article 114 of the Treaty on the Functioning of the European Union",
            basis_role="treaty_basis",
        ),
        _node(
            3,
            "LegalProvision",
            filename,
            name="Article 1",
            provision_number="Article 1",
            provision_content="A person shall register. Records shall be retained for at least 5 years. Reports shall be filed annually.",
        ),
        _node(
            4,
            "ProvisionClause",
            filename,
            name="Article 1(1)",
            unit_number="Article 1(1)",
            unit_content="A person shall register.",
            legal_function="mandatory",
        ),
        _node(
            5,
            "ProvisionTextParagraph",
            filename,
            name="Article 1(1)",
            unit_number="Article 1(1)",
            unit_content="A person shall register.",
            quantitative_feature="Qualitative",
        ),
        _node(
            6,
            "ProvisionClause",
            filename,
            name="Article 1(2)",
            unit_number="Article 1(2)",
            unit_content="Records shall be retained for at least 5 years. Reports shall be filed annually.",
            has_quantitative_detail=True,
        ),
        _node(
            7,
            "ProvisionTextParagraph",
            filename,
            name="Article 1(2)(a)",
            unit_number="Article 1(2)(a)",
            unit_content="Records shall be retained for at least 5 years.",
            quantitative_feature="Quantitative",
            quantitative_indicator=json.dumps(
                {
                    "raw_text": "at least 5 years",
                    "value_type": "time_limit",
                    "min": 5,
                    "max": None,
                    "unit": "years",
                    "relation": "lower_bound",
                }
            ),
        ),
        _node(
            8,
            "ProvisionTextParagraph",
            filename,
            name="Article 1(2)(b)",
            unit_number="Article 1(2)(b)",
            unit_content="Reports shall be filed annually.",
            quantitative_feature="Qualitative",
        ),
        _node(
            9,
            "Citation",
            filename,
            name="Directive 2000/1/EC",
            citation_text="Directive 2000/1/EC",
            is_internal_reference="No",
            citation_purpose="record retention requirements",
        ),
        _node(
            10,
            "Citation",
            filename,
            name="this Regulation",
            citation_text="this Regulation",
            is_internal_reference="Yes",
            resolution_status="unresolved_internal_reference",
        ),
    ]
    edges = [
        _edge(1, 1, 2, "BASED_ON"),
        _edge(2, 1, 3, "CONTAINS"),
        _edge(3, 3, 4, "CONTAINS"),
        _edge(4, 4, 5, "CONTAINS"),
        _edge(5, 3, 6, "CONTAINS"),
        _edge(6, 6, 7, "CONTAINS"),
        _edge(7, 6, 8, "CONTAINS"),
        _edge(8, 7, 9, "CITES"),
        _edge(9, 7, 10, "CITES"),
    ]

    result, entries = _convert(tmp_path, nodes, edges)

    assert result["knowledge_type_counts"]["legal_provision"] == 5
    assert result["knowledge_type_counts"]["citation"] == 1
    assert "quantitative_provision" not in result["knowledge_type_counts"]
    assert result["skip_reason_counts"]["ProvisionClause:exact_cross_level_duplicate"] == 1
    assert result["skip_reason_counts"]["citation:internal_or_unresolved"] == 1
    assert result["filtered_property_counts"]["LegalDocument:unexpected_dynamic"] == 1

    legal_entries = [entry for entry in entries if entry["knowledge_type"] == "legal_provision"]
    expected_prefix = "Under Regulation (EU) 2026/1 on compliance records, "
    for entry in entries:
        if entry["source_node_type"] == "LegalDocument":
            assert not entry["content"].startswith(expected_prefix)
            continue
        assert entry["content"].startswith(expected_prefix)
        assert entry["content"].count(expected_prefix) == 1
    assert {entry["source_node_type"] for entry in legal_entries} == {
        "LegalProvision",
        "ProvisionClause",
        "ProvisionTextParagraph",
    }
    assert any(
        entry["source_node_type"] == "ProvisionClause"
        and entry["source_location"] == "Article 1(2)"
        for entry in legal_entries
    )
    assert not any(
        entry["source_node_type"] == "ProvisionClause"
        and entry["source_location"] == "Article 1(1)"
        for entry in legal_entries
    )

    quantitative_entry = next(entry for entry in entries if entry["source_location"] == "Article 1(2)(a)" and entry["knowledge_type"] == "legal_provision")
    assert quantitative_entry["metadata"]["quantitative_indicator"]["min"] == 5
    assert quantitative_entry["metadata"]["quantitative_feature"] == "Quantitative"
    assert all(not any("\u4e00" <= char <= "\u9fff" for char in entry["content"]) for entry in entries)

    citation_entries = [entry for entry in entries if entry["knowledge_type"] == "citation"]
    assert citation_entries[0]["content"].endswith("in connection with record retention requirements.")
    assert "this Regulation" not in citation_entries[0]["content"]

    assert Path(result["txt_path"]).exists()
    assert Path(result["jsonl_path"]).exists()
    assert Path(result["csv_path"]).exists()


def test_format_three_uses_common_legal_text_rule_without_extra_entry(tmp_path: Path) -> None:
    filename = "31 USC 3729.md"
    nodes = [
        _node(
            1,
            "LegalDocument",
            filename,
            name="31 U.S.C. Chapter 37",
            document_name="31 U.S.C. Chapter 37",
            document_type="USC_Compilation",
            document_number="31 U.S.C.",
            document_format="format_three_us_code_cfr",
        ),
        _node(
            2,
            "LegalProvision",
            filename,
            name="§ 3729",
            provision_number="§ 3729",
            provision_content="A person who knowingly presents a false claim is liable to the United States Government.",
        ),
    ]
    result, entries = _convert(tmp_path, nodes, [_edge(1, 1, 2, "CONTAINS")])

    legal_entries = [entry for entry in entries if entry["knowledge_type"] == "legal_provision"]
    assert len(legal_entries) == 1
    assert legal_entries[0]["document_format"] == "format_three"
    assert legal_entries[0]["source_location"] == "§ 3729"
    assert "§ 3729 provides:" in legal_entries[0]["content"]
    assert result["legal_text_output_counts"] == {"LegalProvision": 1}


def test_invalid_quantitative_data_and_chinese_text_do_not_create_knowledge(
    tmp_path: Path,
) -> None:
    filename = "Example Act.md"
    nodes = [
        _node(
            1,
            "LegalDocument",
            filename,
            name="Example Act",
            document_name="Example Act",
            document_type="Act",
            document_format="format_two_us_act",
        ),
        _node(
            2,
            "LegalProvision",
            filename,
            name="SEC. 1",
            provision_number="SEC. 1",
            provision_content="The operator shall submit a report within 30 days.",
        ),
        _node(
            3,
            "ProvisionClause",
            filename,
            name="SEC. 1(a)",
            unit_number="SEC. 1(a)",
            unit_content="The operator shall submit a report within 30 days.",
        ),
        _node(
            4,
            "ProvisionTextParagraph",
            filename,
            name="SEC. 1(a)(1)",
            unit_number="SEC. 1(a)(1)",
            unit_content="The operator shall submit a report within 30 days.",
            quantitative_feature="Quantitative",
            quantitative_indicator="not-json",
        ),
        _node(
            5,
            "ProvisionTextParagraph",
            filename,
            name="SEC. 1(a)(2)",
            unit_number="SEC. 1(a)(2)",
            unit_content="本条规定不得输出为英文知识。",
        ),
    ]
    edges = [
        _edge(1, 1, 2, "CONTAINS"),
        _edge(2, 2, 3, "CONTAINS"),
        _edge(3, 3, 4, "CONTAINS"),
        _edge(4, 3, 5, "CONTAINS"),
    ]
    result, entries = _convert(tmp_path, nodes, edges)

    legal_entries = [entry for entry in entries if entry["knowledge_type"] == "legal_provision"]
    assert len(legal_entries) == 1
    assert legal_entries[0]["source_node_type"] == "ProvisionTextParagraph"
    assert "quantitative_indicator" not in legal_entries[0]["metadata"]
    assert "quantitative_indicator_unparseable" in legal_entries[0]["validation_issues"]
    assert result["validation_issue_counts"]["quantitative_indicator_unparseable"] == 1
    assert result["validation_issue_counts"]["unexpected_chinese_source_text"] == 1
    assert all("本条" not in entry["content"] for entry in entries)
