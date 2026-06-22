"""英文法规 Neo4j 入库清洗测试。"""

from app.infrastructure.information_extraction.en_law.neo4j_export import prepare_en_law_kg_for_neo4j


def test_prepare_en_law_kg_for_neo4j_removes_review_only_properties():
    """验证入库前删除行号、原文兜底和嵌套审查字段。"""
    kg = {
        "nodes": [
            {
                "node_id": "n1",
                "node_name": "Article 1",
                "node_type": "LegalProvision",
                "filename": "sample.md",
                "properties": {
                    "provision_number": "Article 1",
                    "line_start": 10,
                    "line_end": 20,
                    "article_text": "Full article text",
                    "classification_context": {"title": "TITLE I"},
                    "classification_title": "TITLE I",
                    "quantitative_condition": {"raw_text": "within 30 days"},
                    "quantitative_condition_json": '{"raw_text":"within 30 days"}',
                },
            }
        ],
        "edges": [
            {
                "source_id": "n1",
                "target_id": "n2",
                "relation_type": "CITES",
                "directionality": "single",
                "filename": "sample.md",
                "properties": {"source": "citation_entity_fallback", "citation_relation": "under"},
            }
        ],
        "metadata": {
            "filename": "sample.md",
            "document_format": "format_one_eu_regulation_directive",
            "compliance_risk_type": "产品法律风险",
        },
        "split_result": {"debug": True},
        "raw_llm_result": {"debug": True},
    }

    cleaned = prepare_en_law_kg_for_neo4j(kg)
    props = cleaned["nodes"][0]["properties"]
    edge_props = cleaned["edges"][0]["properties"]

    assert "line_start" not in props
    assert "line_end" not in props
    assert "article_text" not in props
    assert "classification_context" not in props
    assert "quantitative_condition" not in props
    assert props["classification_title"] == "TITLE I"
    assert props["quantitative_condition_json"] == '{"raw_text":"within 30 days"}'
    assert "source" not in edge_props
    assert edge_props["citation_relation"] == "under"
    assert "split_result" not in cleaned
    assert "raw_llm_result" not in cleaned
