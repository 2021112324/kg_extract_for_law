
"""格式一英文法规 v2 抽取模块入口。"""

from app.infrastructure.information_extraction.en_law_v2.extractor import FormatOneEnLawExtractor
from app.infrastructure.information_extraction.en_law_v2.splitter import split_format_one_document

__all__ = ["FormatOneEnLawExtractor", "split_format_one_document"]
