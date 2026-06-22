"""格式一英文法规知识图谱抽取包入口。

本包暴露两个最常用对象：
1. `FormatOneEnLawExtractor`：完整抽取器，可执行切分、LLM 抽取和图谱装配。
2. `split_format_one_document`：规则切分函数，可在不调用 LLM 的情况下测试一阶段切分。
"""

# 对外导出完整抽取器入口。
from app.infrastructure.information_extraction.en_law.extractor import FormatOneEnLawExtractor
# 对外导出规则切分函数，方便测试和独立调用。
from app.infrastructure.information_extraction.en_law.splitter import split_format_one_document

# 控制 `from ...en_law import *` 时暴露的公共对象。
__all__ = ["FormatOneEnLawExtractor", "split_format_one_document"]
