# 英文法律法条抽取包入口。
# ClauseEnExtractor = English clause extractor，中文含义为“英文法条抽取器”。
# split_clause / split_clause_file 保持函数级入口，便于只做英文法条切分。
from app.infrastructure.information_extraction.law_en_extract_cp.clause_extract import (
    ClauseEnExtractor,
    split_clause,
    split_clause_file,
)

__all__ = [
    "ClauseEnExtractor",
    "split_clause",
    "split_clause_file",
]
