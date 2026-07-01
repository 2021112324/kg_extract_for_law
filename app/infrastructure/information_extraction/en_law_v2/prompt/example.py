"""格式一英文法规 LLM few-shot 示例文件。

本文件只存放会实际传入 Langextract 的英文示例。
中文说明仅存在于 Python 注释和 docstring 中，不会混入英文法规提示词正文。
"""

# 文件级抽取示例列表；用于避免底层适配器回退到通用示例。
example_for_file_info = [
    {
        "text": (
            "Filename: Example Regulation.md\n"
            "File header:\n"
            "REGULATION (EU) 2024/999 OF THE EUROPEAN PARLIAMENT AND OF THE COUNCIL "
            "of 1 January 2024 on example compliance requirements\n"
            "Having regard to the Treaty on the Functioning of the European Union, "
            "and in particular Article 114 thereof"
        ),
        "extractions": [
            {
                "name": "Regulation (EU) 2024/999",
                "type": "LegalDocument",
                "attributes": {
                    "document_name": "Regulation (EU) 2024/999 on example compliance requirements",
                    "document_type": "Regulation",
                    "document_number": "2024/999",
                    "publication_date": "1 January 2024",
                    "issuing_authority": "European Parliament and Council",
                    "legal_basis_text": "Treaty on the Functioning of the European Union, Article 114",
                },
            },
            {
                "name": "Treaty on the Functioning of the European Union, Article 114",
                "type": "LegalBasis",
                "attributes": {
                    "instrument": "Treaty on the Functioning of the European Union",
                    "article": "Article 114",
                },
            },
        ],
    }
]

# Article 级抽取示例列表；用于约束 Article、ProvisionUnit、Citation 的英文输出形态。
example_for_article = [
    {
        "text": (
            '{"article_number": "Article 99", "article_heading": "Synthetic example", '
            '"classification_context": {"title": "TITLE EXAMPLE", "chapter": "CHAPTER EXAMPLE"}, '
            '"text": "1. The ExampleOnly operator shall keep the ExampleOnly record for '
            '30 days. 2. For the purposes of paragraph 1, the ExampleOnly authority may '
            'request the ExampleOnly record."}'
        ),
        "extractions": [
            {
                "name": "Article 99",
                "type": "LegalProvision",
                "attributes": {
                    "provision_number": "Article 99",
                    "provision_heading": "Synthetic example",
                    "core_topic": "synthetic example record keeping",
                    "scope_of_effect": "synthetic example only",
                    "applicable_industry": "General",
                    "compliance_domains": ["example compliance"],
                    "economic_industries": ["General"],
                    "is_amendment_article": False,
                    "amendment_target": "",
                    "amendment_action": "",
                },
            },
            {
                "name": "Article 99(1)",
                "type": "ProvisionUnit",
                "attributes": {
                    "unit_number": "Article 99(1)",
                    "unit_level": "paragraph",
                    "unit_content": "The ExampleOnly operator shall keep the ExampleOnly record for 30 days.",
                    "unit_purpose": "synthetic record keeping obligation",
                    "applicable_industry": "General",
                    "compliance_domains": ["example compliance"],
                    "economic_industries": ["General"],
                    "function_type": "mandatory",
                    "quantitative_feature": "Quantitative",
                    "quantitative_indicator": {
                        "raw_text": "30 days",
                        "value_type": "deadline",
                        "min": 30,
                        "max": 30,
                        "unit": "days",
                        "relation": "duration",
                    },
                    "applicable_subject": "ExampleOnly operator",
                    "conduct_description": "keep the ExampleOnly record",
                    "condition": "",
                    "legal_consequence": "",
                    "exception": "",
                },
            },
            {
                "name": "Article 99(2)",
                "type": "ProvisionUnit",
                "attributes": {
                    "unit_number": "Article 99(2)",
                    "unit_level": "paragraph",
                    "unit_content": "For the purposes of paragraph 1, the ExampleOnly authority may request the ExampleOnly record.",
                    "unit_purpose": "synthetic authority permission",
                    "applicable_industry": "General",
                    "compliance_domains": ["example compliance"],
                    "economic_industries": ["General"],
                    "function_type": "optional",
                    "quantitative_feature": "Qualitative",
                    "quantitative_indicator": None,
                    "applicable_subject": "ExampleOnly authority",
                    "conduct_description": "request the ExampleOnly record",
                    "condition": "For the purposes of paragraph 1",
                    "legal_consequence": "",
                    "exception": "",
                },
            },
        ],
    }
]

# v2 覆盖 Article 示例：Article 抽取只产出 LegalProvision，避免示例污染到 Clause 阶段。
example_for_article = [
    {
        "text": (
            '{"article_number": "Article 99", "article_heading": "Synthetic example", '
            '"classification_context": {"title": "TITLE EXAMPLE"}, '
            '"text": "1. The ExampleOnly operator shall keep the ExampleOnly record."}'
        ),
        "extractions": [
            {
                "name": "Article 99",
                "type": "LegalProvision",
                "attributes": {
                    "provision_number": "Article 99",
                    "provision_heading": "Synthetic example",
                    "core_topic": "synthetic example record keeping",
                    "scope_of_effect": "synthetic example only",
                    "applicable_industry": "General",
                    "compliance_domains": ["example compliance"],
                    "economic_industries": ["General"],
                    "is_amendment_article": False,
                    "amendment_target": "",
                    "amendment_action": "",
                },
            }
        ],
    }
]

example_for_provision_clause = [
    {
        "text": (
            '{"article_number": "Article 99", "provision_clause": {'
            '"unit_number": "Article 99(1)", '
            '"unit_content": "The ExampleOnly operator shall keep the ExampleOnly record for 30 days."}}'
        ),
        "extractions": [
            {
                "name": "Article 99(1)",
                "type": "ProvisionClause",
                "attributes": {
                    "unit_level": "paragraph",
                    "clause_summary": "synthetic record keeping clause",
                    "legal_function": "mandatory",
                    "has_quantitative_detail": True,
                    "has_exception": False,
                    "has_condition": False,
                },
            },
            {
                "name": "Article 99(1)",
                "type": "ProvisionTextParagraph",
                "attributes": {
                    "unit_number": "Article 99(1)",
                    "unit_level": "paragraph_fragment",
                    "unit_content": "The ExampleOnly operator shall keep the ExampleOnly record for 30 days.",
                    "quantitative_feature": "Quantitative",
                    "quantitative_indicator": {
                        "raw_text": "30 days",
                        "value_type": "time_limit",
                        "min": None,
                        "max": 30,
                        "unit": "days",
                        "relation": "upper_bound",
                    },
                    "applicable_subject": "ExampleOnly operator",
                    "conduct_description": "keep the ExampleOnly record",
                },
            },
        ],
    }
]

