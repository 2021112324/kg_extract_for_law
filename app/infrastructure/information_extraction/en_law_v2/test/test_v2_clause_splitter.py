"""en_law_v2 ProvisionClause 切分测试。"""

from app.infrastructure.information_extraction.en_law_v2.splitter import split_format_one_document


def test_explicit_provision_clause_uses_only_line_start_number_dot():
    text = """REGULATION (EU) 2024/1

Article 1
Subject matter
1. This Regulation establishes rules:
(a) first item;
(b) second item.
2. It also establishes procedures.
"""

    result = split_format_one_document(text, filename="sample.md")
    article = result["clauses"][0]
    provision_clauses = article["provision_clauses"]

    assert [item["unit_number"] for item in provision_clauses] == ["Article 1(1)", "Article 1(2)"]
    assert "(a) first item;" in provision_clauses[0]["unit_content"]
    assert provision_clauses[0]["explicit_boundary"] is True


def test_implicit_provision_clause_when_no_line_start_number_dot():
    text = """DIRECTIVE 2024/1/EU

Article 2
Definitions
(1) "product" means any item.
(a) "item" includes a component.
"""

    result = split_format_one_document(text, filename="implicit.md")
    provision_clauses = result["clauses"][0]["provision_clauses"]

    assert len(provision_clauses) == 1
    assert provision_clauses[0]["unit_number"] == "Article 2"
    assert provision_clauses[0]["explicit_boundary"] is False
    assert "(a) \"item\" includes a component." in provision_clauses[0]["unit_content"]


def test_single_sentence_after_article_heading_is_body_not_heading():
    text = """DIRECTIVE 2006/112/EC

Article 357

This Chapter shall apply until 31 December 2006.
"""

    result = split_format_one_document(text, filename="vat.md")
    article = result["clauses"][0]

    assert article["article_heading"] == ""
    assert article["content"] == "This Chapter shall apply until 31 December 2006."
    assert article["provision_clauses"][0]["unit_number"] == "Article 357"


def test_numbered_heading_list_after_colon_stays_inside_parent_clause():
    text = """REGULATION (EC) No 1907/2006

Article 31
Requirements for safety data sheets
6. The safety data sheet shall contain the following headings:
1. identification of the substance/mixture and of the company/undertaking;
2. hazards identification;
7. Any actor in the supply chain shall provide information.
"""

    result = split_format_one_document(text, filename="reach.md")
    provision_clauses = result["clauses"][0]["provision_clauses"]

    assert [item["unit_number"] for item in provision_clauses] == ["Article 31(6)", "Article 31(7)"]
    assert "1. identification of the substance" in provision_clauses[0]["unit_content"]
    assert "2. hazards identification;" in provision_clauses[0]["unit_content"]
