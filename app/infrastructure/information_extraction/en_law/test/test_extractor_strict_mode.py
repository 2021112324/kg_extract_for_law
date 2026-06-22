"""英文法规抽取器严格模式测试。"""

import asyncio

import pytest

from app.infrastructure.information_extraction.en_law.extractor import FormatOneEnLawExtractor


class MockStrictExtractor(FormatOneEnLawExtractor):
    """用 mock LLM 方法测试严格模式，不调用真实模型。"""

    async def llm_extract_file_info(self, filename: str, split_result: dict) -> dict:
        return {
            "input_text": "",
            "extraction": {
                "entities": [
                    {"name": "Sample Regulation", "entity_type": "LegalDocument", "properties": {}},
                ],
                "relations": [],
            },
            "raw": {},
        }

    async def llm_extract_article(self, filename: str, article: dict) -> dict:
        raise RuntimeError("mock article failure")


def test_strict_extractor_raises_when_article_extraction_failed():
    """验证严格模式下 Article 抽取失败会中断，不返回可构图 raw。"""
    split_result = {
        "clauses": [
            {
                "article_number": "Article 1",
                "article_heading": "Scope",
                "classification_context": {},
                "content": "1. Operators shall comply.",
            }
        ],
        "annexes_metadata": [],
    }
    extractor = MockStrictExtractor(lenient_mode=False)

    with pytest.raises(ValueError, match="Article extractions failed in strict mode"):
        asyncio.run(extractor.llm_extract_from_split_result("sample.md", split_result))
