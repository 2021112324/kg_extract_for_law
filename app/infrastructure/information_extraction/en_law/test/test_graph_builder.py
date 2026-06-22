"""格式一英文法规图谱装配器单元测试。

这些测试不调用真实 LLM，而是构造 mock raw LLM 结果，验证：
1. 英文节点类型和中文业务属性可以同时保留。
2. LegalDocument、LegalProvision、ProvisionUnit、Citation 能正确装配。
3. graph_builder 会在 KG 构建阶段修正 LLM 输出的少量不稳定值。
"""

import pytest

from app.infrastructure.information_extraction.en_law.graph_builder import FormatOneGraphBuilder


def test_graph_builder_uses_english_schema_and_chinese_risk_property():
    """验证最终图谱使用英文节点类型，并保留中文 `合规风险类型` 属性。"""
    split_result = {
        "document_format": "format_one_eu_regulation_directive",
        "compliance_risk_type": "产品法律风险",
        "fallback_metadata": {
            "document_name": "REGULATION (EU) 2023/000",
            "document_type": "Regulation",
        },
        "recitals": [],
        "annexes_metadata": [],
        "warnings": [],
        "clauses": [
            {
                "article_number": "Article 1",
                "article_heading": "Subject matter",
                "classification_context": {"title": "TITLE I", "chapter": "CHAPTER 1", "section": "Section 1"},
                "title": "TITLE I",
                "chapter": "CHAPTER 1",
                "section": "Section 1",
                "part": "",
                "content": "1. Operators shall comply.",
                "line_start": 1,
                "line_end": 3,
                "is_amendment_article": False,
            }
        ],
    }
    raw = {
        "file_info_extraction": {
            "entities": [
                {
                    "name": "REGULATION (EU) 2023/000",
                    "entity_type": "LegalDocument",
                    "properties": {"document_number": "(EU) 2023/000"},
                }
            ],
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
                            "properties": {
                                "core_topic": "Subject matter",
                                "classification_context": "LLM should not override splitter context",
                                "title": "Wrong title from LLM",
                            },
                        },
                        {
                            "name": "Article 1(1)",
                            "entity_type": "ProvisionUnit",
                            "properties": {"unit_content": "Operators shall comply."},
                        },
                    ],
                    "relations": [],
                },
            }
        ],
        "failed_article_extractions": [],
    }

    kg = FormatOneGraphBuilder().build("sample.md", split_result, raw)
    doc = next(node for node in kg["nodes"] if node["node_type"] == "LegalDocument")
    article = next(node for node in kg["nodes"] if node["node_type"] == "LegalProvision")

    assert doc["properties"]["合规风险类型"] == "产品法律风险"
    assert "document_number" in doc["properties"]
    assert article["properties"]["classification_context"] == {
        "title": "TITLE I",
        "chapter": "CHAPTER 1",
        "section": "Section 1",
    }
    assert article["properties"]["title"] == "TITLE I"
    assert article["properties"]["classification_title"] == "TITLE I"
    assert article["properties"]["classification_context_text"] == "TITLE I / CHAPTER 1 / Section 1"
    assert any(edge["relation_type"] == "CONTAINS" for edge in kg["edges"])


def test_internal_citation_resolves_to_later_article_node():
    """验证 Article 1 内部引用 Article 2 时，可以解析到后续 Article 节点。"""
    split_result = {
        "document_format": "format_one_eu_regulation_directive",
        "compliance_risk_type": "产品法律风险",
        "fallback_metadata": {"document_name": "Sample Regulation"},
        "recitals": [],
        "annexes_metadata": [],
        "warnings": [],
        "clauses": [
            {
                "article_number": "Article 1",
                "article_heading": "Scope",
                "classification_context": {},
                "content": "1. This Article refers to Article 2.",
                "line_start": 1,
                "line_end": 2,
                "is_amendment_article": False,
            },
            {
                "article_number": "Article 2",
                "article_heading": "Definitions",
                "classification_context": {},
                "content": "1. Definitions apply.",
                "line_start": 3,
                "line_end": 4,
                "is_amendment_article": False,
            },
        ],
    }
    raw = {
        "article_extractions": [
            {
                "article_number": "Article 1",
                "extraction": {
                    "entities": [
                        {"name": "Article 1", "entity_type": "LegalProvision", "properties": {}},
                        {
                            "name": "Article 1(1)",
                            "entity_type": "ProvisionUnit",
                            "properties": {"unit_number": "Article 1(1)"},
                        },
                        {
                            "name": "Article 2",
                            "entity_type": "Citation",
                            "properties": {
                                "citation_text": "Article 2",
                                "provision_number": "Article 2",
                                "is_internal_reference": "Yes",
                            },
                        },
                    ],
                    "relations": [
                        {"source": "Article 1(1)", "target": "Article 2", "type": "CITES", "properties": {}}
                    ],
                },
            },
            {
                "article_number": "Article 2",
                "extraction": {
                    "entities": [
                        {"name": "Article 2", "entity_type": "LegalProvision", "properties": {}},
                    ],
                    "relations": [],
                },
            },
        ],
        "failed_article_extractions": [],
    }

    kg = FormatOneGraphBuilder().build("sample.md", split_result, raw)
    article_2 = next(
        node
        for node in kg["nodes"]
        if node["node_type"] == "LegalProvision" and node["node_name"] == "Article 2"
    )

    assert any(
        edge["relation_type"] == "CITES" and edge["target_id"] == article_2["node_id"]
        for edge in kg["edges"]
    )
    assert not any(
        node["node_type"] == "Citation" and node["node_name"] == "Article 2"
        for node in kg["nodes"]
    )


def test_graph_builder_normalizes_legacy_entity_type_names():
    """验证历史带空格实体类型会被规范化为 Neo4j 友好的 schema 标签。"""
    split_result = {
        "document_format": "format_one_eu_regulation_directive",
        "compliance_risk_type": "产品法律风险",
        "fallback_metadata": {"document_name": "Sample Regulation"},
        "recitals": [],
        "annexes_metadata": [],
        "warnings": [],
        "clauses": [
            {
                "article_number": "Article 1",
                "article_heading": "Scope",
                "classification_context": {},
                "content": "1. Operators shall comply.",
                "line_start": 1,
                "line_end": 2,
                "is_amendment_article": False,
            }
        ],
    }
    raw = {
        "file_info_extraction": {
            "entities": [{"name": "Sample Regulation", "entity_type": "Legal Document", "properties": {}}],
            "relations": [],
        },
        "article_extractions": [
            {
                "article_number": "Article 1",
                "extraction": {
                    "entities": [
                        {"name": "Article 1", "entity_type": "Legal Provision", "properties": {}},
                        {
                            "name": "Article 1(1)",
                            "entity_type": "Provision Unit",
                            "properties": {"unit_content": "Operators shall comply."},
                        },
                    ],
                    "relations": [],
                },
            }
        ],
        "failed_article_extractions": [],
    }

    kg = FormatOneGraphBuilder().build("sample.md", split_result, raw)
    node_types = {node["node_type"] for node in kg["nodes"]}

    assert "LegalDocument" in node_types
    assert "LegalProvision" in node_types
    assert "ProvisionUnit" in node_types
    assert "Legal Document" not in node_types
    assert "Legal Provision" not in node_types
    assert "Provision Unit" not in node_types


def test_external_citation_is_connected_to_article_and_boolean_normalized():
    """验证外部 Citation 节点会被 CITES 边接入主图，并统一内部引用布尔值。"""
    split_result = {
        "document_format": "format_one_eu_regulation_directive",
        "compliance_risk_type": "产品法律风险",
        "fallback_metadata": {"document_name": "Sample Regulation"},
        "recitals": [],
        "annexes_metadata": [],
        "warnings": [],
        "clauses": [
            {
                "article_number": "Article 1",
                "article_heading": "Scope",
                "classification_context": {},
                "content": "1. Operators shall comply with Regulation (EU) 2020/000.",
                "line_start": 1,
                "line_end": 2,
                "is_amendment_article": False,
            }
        ],
    }
    raw = {
        "article_extractions": [
            {
                "article_number": "Article 1",
                "extraction": {
                    "entities": [
                        {"name": "Article 1", "entity_type": "LegalProvision", "properties": {}},
                        {
                            "name": "Regulation (EU) 2020/000",
                            "entity_type": "Citation",
                            "properties": {
                                "citation_text": "Regulation (EU) 2020/000",
                                "citation_type": "Document",
                                "is_internal_reference": "No",
                            },
                        },
                    ],
                    "relations": [],
                },
            }
        ],
        "failed_article_extractions": [],
    }

    kg = FormatOneGraphBuilder().build("sample.md", split_result, raw)
    article = next(node for node in kg["nodes"] if node["node_type"] == "LegalProvision")
    citation = next(node for node in kg["nodes"] if node["node_type"] == "Citation")

    assert citation["properties"]["is_internal_reference"] is False
    assert any(
        edge["relation_type"] == "CITES"
        and edge["source_id"] == article["node_id"]
        and edge["target_id"] == citation["node_id"]
        for edge in kg["edges"]
    )


def test_provision_unit_properties_are_normalized_for_kg_and_neo4j():
    """验证 ProvisionUnit 的枚举越界值和嵌套定量条件会被规范化。"""
    split_result = {
        "document_format": "format_one_eu_regulation_directive",
        "compliance_risk_type": "产品法律风险",
        "fallback_metadata": {"document_name": "Sample Regulation"},
        "recitals": [],
        "annexes_metadata": [],
        "warnings": [],
        "clauses": [
            {
                "article_number": "Article 1",
                "article_heading": "Scope",
                "classification_context": {},
                "content": "1. Operators shall comply within 30 days.",
                "line_start": 1,
                "line_end": 2,
                "is_amendment_article": False,
            }
        ],
    }
    raw = {
        "article_extractions": [
            {
                "article_number": "Article 1",
                "extraction": {
                    "entities": [
                        {"name": "Article 1", "entity_type": "LegalProvision", "properties": {}},
                        {
                            "name": "Article 1(1)",
                            "entity_type": "ProvisionUnit",
                            "properties": {
                                "unit_number": "Article 1(1)",
                                "function_type": "qualitative condition",
                                "quantitative_feature": "quantitative",
                                "quantitative_condition": {
                                    "raw_text": "within 30 days",
                                    "maximum_value": 30,
                                    "unit": "days",
                                },
                            },
                        },
                    ],
                    "relations": [],
                },
            }
        ],
        "failed_article_extractions": [],
    }

    kg = FormatOneGraphBuilder().build("sample.md", split_result, raw)
    unit = next(node for node in kg["nodes"] if node["node_type"] == "ProvisionUnit")

    assert unit["properties"]["function_type"] == "other"
    assert unit["properties"]["quantitative_feature"] == "Quantitative"
    assert unit["properties"]["quantitative_condition"]["maximum_value"] == 30
    assert '"maximum_value":30' in unit["properties"]["quantitative_condition_json"]


def test_strict_graph_builder_does_not_create_fallback_for_failed_article():
    """验证严格模式下失败 Article 不会被 fallback 成 LegalProvision 节点。"""
    split_result = {
        "document_format": "format_one_eu_regulation_directive",
        "compliance_risk_type": "产品法律风险",
        "fallback_metadata": {"document_name": "Sample Regulation"},
        "recitals": [],
        "annexes_metadata": [],
        "warnings": [],
        "clauses": [
            {
                "article_number": "Article 1",
                "article_heading": "Scope",
                "classification_context": {},
                "content": "1. Operators shall comply.",
                "line_start": 1,
                "line_end": 2,
                "is_amendment_article": False,
            }
        ],
    }
    raw = {
        "file_info_extraction": {
            "entities": [{"name": "Sample Regulation", "entity_type": "LegalDocument", "properties": {}}],
            "relations": [],
        },
        "article_extractions": [],
        "failed_article_extractions": [
            {"article_number": "Article 1", "error": "mock article failure"},
        ],
    }

    with pytest.raises(ValueError, match="strict mode"):
        FormatOneGraphBuilder(lenient_mode=False).build("sample.md", split_result, raw)
