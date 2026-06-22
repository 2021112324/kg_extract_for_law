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
            '{"article_number": "Article 1", "article_heading": "Subject matter", '
            '"text": "This Regulation lays down rules for economic operators placing '
            'products on the Union market. It applies without prejudice to Regulation '
            '(EU) 2019/1020."}'
        ),
        "extractions": [
            {
                "name": "Article 1",
                "type": "LegalProvision",
                "attributes": {
                    "article_number": "Article 1",
                    "article_heading": "Subject matter",
                    "topic": "rules for economic operators placing products on the Union market",
                    "is_amendment_article": False,
                },
            },
            {
                "name": "Article 1 sentence 1",
                "type": "ProvisionUnit",
                "attributes": {
                    "unit_number": "sentence 1",
                    "subject": "This Regulation",
                    "conduct": "lays down rules",
                    "object": "economic operators placing products on the Union market",
                },
            },
            {
                "name": "Regulation (EU) 2019/1020",
                "type": "Citation",
                "attributes": {
                    "citation_text": "Regulation (EU) 2019/1020",
                    "cited_instrument": "Regulation (EU) 2019/1020",
                },
            },
        ],
    }
]
