"""格式一英文法规 prompt 包入口。

本文件把 example、prompt、schema 三类 prompt 资产统一导出，
方便 `extractor.py` 通过一个包路径完成导入。
"""

# 导出 few-shot 示例对象。
from app.infrastructure.information_extraction.en_law_v2.prompt.example import (
    example_for_article,
    example_for_file_info,
    example_for_provision_clause,
)
# 导出文件级和 Article 级提示词文本。
from app.infrastructure.information_extraction.en_law_v2.prompt.prompt import (
    prompt_for_article,
    prompt_for_file_info,
    prompt_for_provision_clause,
)
# 导出文件级和 Article 级 ontology schema 文本。
from app.infrastructure.information_extraction.en_law_v2.prompt.schema import (
    schema_for_article,
    schema_for_file_info,
    schema_for_provision_clause,
)

# 控制 `from ...prompt import *` 时暴露的公共对象，避免导出内部变量。
__all__ = [
    "example_for_article",
    "example_for_file_info",
    "example_for_provision_clause",
    "prompt_for_article",
    "prompt_for_file_info",
    "prompt_for_provision_clause",
    "schema_for_article",
    "schema_for_file_info",
    "schema_for_provision_clause",
]

