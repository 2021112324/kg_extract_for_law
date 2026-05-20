# 英文法条抽取 prompt 包入口。
# prompt_for_clause / schema_for_clause / example_for_clause：
#   用于单条英文法条的实体关系抽取。
# prompt_for_file_info / schema_for_file_info / example_for_file_info：
#   用于英文法规文件头信息的法规文件和制定依据抽取。
from app.infrastructure.information_extraction.law_en_extract_cp.prompt.example import (
    example_for_clause,
    example_for_file_info,
)
from app.infrastructure.information_extraction.law_en_extract_cp.prompt.prompt import (
    prompt_for_clause,
    prompt_for_file_info,
)
from app.infrastructure.information_extraction.law_en_extract_cp.prompt.schema import (
    schema_for_clause,
    schema_for_file_info,
)

__all__ = [
    "example_for_clause",
    "example_for_file_info",
    "prompt_for_clause",
    "prompt_for_file_info",
    "schema_for_clause",
    "schema_for_file_info",
]
