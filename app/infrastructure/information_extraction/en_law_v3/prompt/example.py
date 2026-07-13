"""格式三美国法案 LLM few-shot 示例。"""

example_for_file_info = [
    {
        "text": (
            "Filename: Example CFR Part.md\n"
            "File header:\n"
            "EXAMPLEONLY CFR PART\n"
            "Title 15 — Commerce and Foreign Trade\n"
            "Part 999 Example Compliance"
        ),
        "extractions": [
            {
                "name": "ExampleOnly CFR Part",
                "type": "LegalDocument",
                "attributes": {
                    "document_name": "ExampleOnly CFR Part",
                    "document_type": "CFR_Part",
                    "document_number": "Part 999",
                    "issuing_authority": "Bureau of Industry and Security",
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
            '{"article_number": "§ 999.1", "article_heading": "ExampleOnly duties", '
            '"classification_context": {"title": "TITLE EXAMPLE"}, '
            '"text": "(a) The ExampleOnly operator shall keep the ExampleOnly record."}'
        ),
        "extractions": [
            {
                "name": "§ 999.1",
                "type": "LegalProvision",
                "attributes": {
                    "provision_number": "§ 999.1",
                    "provision_heading": "ExampleOnly duties",
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
            '{"article_number": "§ 999.1", "provision_clause": {'
            '"unit_number": "999.1(a)", '
            '"unit_content": "(a) The ExampleOnly operator shall keep the ExampleOnly record for 30 days."}}'
        ),
        "extractions": [
            {
                "name": "999.1(a)",
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
                "name": "999.1(a)",
                "type": "ProvisionTextParagraph",
                "attributes": {
                    "unit_number": "999.1(a)",
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


