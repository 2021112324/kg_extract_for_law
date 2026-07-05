"""格式二英文法规 prompt 包入口。"""

from app.infrastructure.information_extraction.en_law_v2.prompt.example import (
    example_for_article,
    example_for_file_info,
    example_for_provision_clause,
)
from app.infrastructure.information_extraction.en_law_v2.prompt.prompt import (
    prompt_for_article,
    prompt_for_file_info,
    prompt_for_provision_clause,
)
from app.infrastructure.information_extraction.en_law_v2.prompt.schema import (
    schema_for_article,
    schema_for_file_info,
    schema_for_provision_clause,
)

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
