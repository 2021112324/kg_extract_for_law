"""en_law_v2 图谱装配测试。"""

from app.infrastructure.information_extraction.en_law_v2.graph_builder import FormatOneGraphBuilder


def test_graph_builder_separates_article_clause_and_text_paragraph():
    split_result = {
        "document_format": "format_one_eu_regulation_directive_v2",
        "fallback_metadata": {"document_name": "Sample Regulation"},
        "recitals": [],
        "annexes_metadata": [],
        "warnings": [],
        "clauses": [
            {
                "article_number": "Article 1",
                "article_heading": "Subject matter",
                "classification_context": {},
                "content": "1. Operators shall keep records for 30 days.",
                "is_amendment_article": False,
                "provision_clauses": [
                    {
                        "unit_number": "Article 1(1)",
                        "unit_level": "paragraph",
                        "unit_content": "Operators shall keep records for 30 days.",
                        "source_article_number": "Article 1",
                        "clause_index": 1,
                        "explicit_boundary": True,
                        "line_start": 4,
                        "line_end": 4,
                    }
                ],
            }
        ],
    }
    raw = {
        "file_info_extraction": {
            "entities": [{"name": "Sample Regulation", "entity_type": "LegalDocument", "properties": {}}],
            "relations": [],
        },
        "article_extractions": [
            {
                "article_number": "Article 1",
                "extraction": {
                    "entities": [
                        {
                            "name": "Article 1",
                            "entity_type": "LegalProvision",
                            "properties": {"core_topic": "records"},
                        },
                        {
                            "name": "Should be ignored",
                            "entity_type": "ProvisionTextParagraph",
                            "properties": {"unit_number": "Article 1(1)"},
                        },
                    ],
                    "relations": [],
                },
            }
        ],
        "provision_clause_extractions": [
            {
                "article_number": "Article 1",
                "unit_number": "Article 1(1)",
                "extraction": {
                    "entities": [
                        {
                            "name": "Article 1(1)",
                            "entity_type": "ProvisionClause",
                            "properties": {"clause_summary": "record keeping"},
                        },
                        {
                            "name": "Article 1(1)",
                            "entity_type": "ProvisionTextParagraph",
                            "properties": {
                                "unit_number": "Article 1(1)",
                                "unit_content": "Operators shall keep records for 30 days.",
                                "quantitative_feature": "Quantitative",
                            },
                        },
                    ],
                    "relations": [],
                },
            }
        ],
        "failed_article_extractions": [],
        "failed_provision_clause_extractions": [],
    }

    kg = FormatOneGraphBuilder().build("sample.md", split_result, raw)
    node_types = [node["node_type"] for node in kg["nodes"]]

    assert "LegalProvision" in node_types
    assert "ProvisionClause" in node_types
    assert "ProvisionTextParagraph" in node_types
    assert "ProvisionUnit" not in node_types

    article = next(node for node in kg["nodes"] if node["node_type"] == "LegalProvision")
    clause = next(node for node in kg["nodes"] if node["node_type"] == "ProvisionClause")
    paragraph = next(node for node in kg["nodes"] if node["node_type"] == "ProvisionTextParagraph")

    assert clause["properties"]["unit_number"] == "Article 1(1)"
    assert clause["properties"]["unit_content"] == "Operators shall keep records for 30 days."
    assert any(
        edge["source_id"] == article["node_id"]
        and edge["target_id"] == clause["node_id"]
        and edge["relation_type"] == "CONTAINS"
        for edge in kg["edges"]
    )
    assert any(
        edge["source_id"] == clause["node_id"]
        and edge["target_id"] == paragraph["node_id"]
        and edge["relation_type"] == "CONTAINS"
        for edge in kg["edges"]
    )


def test_graph_builder_dedupes_text_paragraph_unit_number_and_node_name():
    split_result = {
        "document_format": "format_one_eu_regulation_directive_v2",
        "fallback_metadata": {"document_name": "Sample Regulation"},
        "recitals": [],
        "annexes_metadata": [],
        "warnings": [],
        "clauses": [
            {
                "article_number": "Article 14",
                "article_heading": "",
                "classification_context": {},
                "content": "5. Where the authority sets conditions, it shall ensure that:\nThe condition is equivalent.",
                "is_amendment_article": False,
                "provision_clauses": [
                    {
                        "unit_number": "Article 14(5)",
                        "unit_level": "paragraph",
                        "unit_content": "Where the authority sets conditions, it shall ensure that:\nThe condition is equivalent.",
                        "source_article_number": "Article 14",
                        "clause_index": 1,
                        "explicit_boundary": True,
                    }
                ],
            }
        ],
    }
    raw = {
        "file_info_extraction": {"entities": [], "relations": []},
        "article_extractions": [],
        "failed_article_extractions": [],
        "failed_provision_clause_extractions": [],
        "provision_clause_extractions": [
            {
                "unit_number": "Article 14(5)",
                "extraction": {
                    "entities": [
                        {"name": "Article 14(5)", "entity_type": "ProvisionClause", "properties": {}},
                        {
                            "name": "Article 14(5)",
                            "entity_type": "ProvisionTextParagraph",
                            "properties": {
                                "unit_number": "Article 14(5)",
                                "unit_content": "Where the authority sets conditions, it shall ensure that:",
                            },
                        },
                        {
                            "name": "Article 14(5)",
                            "entity_type": "ProvisionTextParagraph",
                            "properties": {
                                "unit_number": "Article 14(5)",
                                "unit_content": "The condition is equivalent.",
                            },
                        },
                    ],
                    "relations": [],
                },
            }
        ],
    }

    kg = FormatOneGraphBuilder().build("sample.md", split_result, raw)
    paragraphs = [node for node in kg["nodes"] if node["node_type"] == "ProvisionTextParagraph"]
    names = [node["node_name"] for node in paragraphs]

    assert names == ["Article 14(5) paragraph 1", "Article 14(5) paragraph 2"]
    assert [node["properties"]["unit_number"] for node in paragraphs] == names


def test_graph_builder_normalizes_legal_function_and_quantitative_indicator():
    split_result = {
        "document_format": "format_one_eu_regulation_directive_v2",
        "fallback_metadata": {"document_name": "Sample Regulation"},
        "recitals": [],
        "annexes_metadata": [],
        "warnings": [],
        "clauses": [
            {
                "article_number": "Article 1",
                "article_heading": "",
                "classification_context": {},
                "content": "1. Operators shall keep records for 30 days.",
                "is_amendment_article": False,
                "provision_clauses": [
                    {
                        "unit_number": "Article 1(1)",
                        "unit_level": "paragraph",
                        "unit_content": "Operators shall keep records for 30 days.",
                        "source_article_number": "Article 1",
                        "clause_index": 1,
                        "explicit_boundary": True,
                    }
                ],
            }
        ],
    }
    raw = {
        "file_info_extraction": {"entities": [], "relations": []},
        "article_extractions": [],
        "failed_article_extractions": [],
        "failed_provision_clause_extractions": [],
        "provision_clause_extractions": [
            {
                "unit_number": "Article 1(1)",
                "extraction": {
                    "entities": [
                        {
                            "name": "Article 1(1)",
                            "entity_type": "ProvisionClause",
                            "properties": {"legal_function": "definition"},
                        },
                        {
                            "name": "Article 1(1)",
                            "entity_type": "ProvisionTextParagraph",
                            "properties": {
                                "unit_number": "Article 1(1)",
                                "unit_content": "Operators shall keep records for 30 days.",
                                "legal_function": "Mandatory",
                                "quantitative_feature": "Quantitative",
                                "quantitative_indicator": {
                                    "raw_text": "30 days",
                                    "value_type": "Time Limit",
                                    "max": 30,
                                    "unit": "days",
                                    "relation": "Upper Bound",
                                },
                            },
                        },
                    ],
                    "relations": [],
                },
            }
        ],
    }

    kg = FormatOneGraphBuilder().build("sample.md", split_result, raw)
    clause = next(node for node in kg["nodes"] if node["node_type"] == "ProvisionClause")
    paragraph = next(node for node in kg["nodes"] if node["node_type"] == "ProvisionTextParagraph")

    assert clause["properties"]["legal_function"] == ""
    assert paragraph["properties"]["legal_function"] == "mandatory"
    assert paragraph["properties"]["quantitative_feature"] == "Quantitative"
    assert paragraph["properties"]["quantitative_indicator"]["value_type"] == "time_limit"
    assert paragraph["properties"]["quantitative_indicator"]["relation"] == "upper_bound"
