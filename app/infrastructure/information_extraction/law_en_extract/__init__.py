"""
英文法律信息抽取模块的公开接口。

本包（law_en_extract）负责：
- 任务1：将英文法律文本按 Section/Article 层级切分为文件头信息 + 法条列表
  （纯规则，不依赖大模型）
- 任务2：调用 LLM 对切分后的文本进行实体关系抽取
  （输出 Legal Document / Legal Provision / Provision Unit / Citation 等）

对外暴露的核心：
- ClauseEnExtractor：完整的英文法条抽取器（任务1 + 任务2）
- split_clause / split_clause_file：纯规则法条切分（任务1）
- extract_clause_json_file：对任务1输出的 JSON 执行任务2 LLM 抽取
- save_llm_extraction_result：保存 LLM 抽取结果
"""

# 从 clause_extract 模块导入对外公开的类和函数
from app.infrastructure.information_extraction.law_en_extract.clause_extract import (
    ClauseEnExtractor,         # 英文法条抽取器主类（任务1切分 + 任务2 LLM抽取）
    extract_clause_json_file,  # 快捷函数：对任务1输出JSON执行LLM抽取
    save_llm_extraction_result, # 保存 LLM 抽取结果到 JSON 文件
    split_clause,              # 纯文本 → 切分结果 dict
    split_clause_file,         # 文件路径 → 切分结果 dict + 保存 JSON
)

__all__ = [
    "ClauseEnExtractor",
    "extract_clause_json_file",
    "save_llm_extraction_result",
    "split_clause",
    "split_clause_file",
]
