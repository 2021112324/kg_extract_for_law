"""格式二美国法案知识图谱抽取模块入口。"""

from app.infrastructure.information_extraction.en_law_v2.extractor import FormatTwoEnLawExtractor
from app.infrastructure.information_extraction.en_law_v2.splitter import split_v2_document

__all__ = ["FormatTwoEnLawExtractor", "split_v2_document"]
