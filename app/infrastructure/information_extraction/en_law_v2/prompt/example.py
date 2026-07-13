"""格式二美国法案 LLM few-shot 示例。"""

example_for_file_info = [
    {
        "text": (
            "Filename: Example Act.md\n"
            "File header:\n"
            "EXAMPLEONLY ACT\n"
            "[Public Law 999-999]\n"
            "An Act To establish example compliance requirements."
        ),
        "extractions": [
            {
                "name": "ExampleOnly Act",
                "type": "LegalDocument",
                "attributes": {
                    "document_name": "ExampleOnly Act",
                    "document_type": "Act",
                    "document_number": "Public Law 999-999",
                    "issuing_authority": "United States Congress",
                    "subject_matter": "example compliance requirements",
                    "applicable_industry": "General",
                    "compliance_domains": ["example compliance"],
                    "economic_industries": ["General"],
                },
            }
        ],
    }
]


example_for_article = [
    {
        "text": (
            '{"article_number": "SEC. 99", "article_heading": "EXAMPLEONLY DUTIES", '
            '"classification_context": {"title": "TITLE EXAMPLE"}, '
            '"text": "(a) The ExampleOnly operator shall keep the ExampleOnly record."}'
        ),
        "extractions": [
            {
                "name": "SEC. 99",
                "type": "LegalProvision",
                "attributes": {
                    "provision_number": "SEC. 99",
                    "provision_heading": "EXAMPLEONLY DUTIES",
                    "core_topic": "synthetic example duties",
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
            '{"article_number": "SEC. 99", "provision_clause": {'
            '"unit_number": "SEC. 99(a)", '
            '"unit_content": "(a) The ExampleOnly operator shall keep the ExampleOnly record for 30 days."}}'
        ),
        "extractions": [
            {
                "name": "SEC. 99(a)",
                "type": "ProvisionClause",
                "attributes": {
                    "unit_level": "subsection",
                    "clause_summary": "synthetic record keeping clause",
                    "legal_function": "mandatory",
                    "has_quantitative_detail": True,
                    "has_exception": False,
                    "has_condition": False,
                },
            },
            {
                "name": "SEC. 99(a)",
                "type": "ProvisionTextParagraph",
                "attributes": {
                    "unit_number": "SEC. 99(a)",
                    "unit_level": "paragraph_fragment",
                    "unit_content": "(a) The ExampleOnly operator shall keep the ExampleOnly record for 30 days.",
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
