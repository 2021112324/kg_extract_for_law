"""
英文法律 LLM 抽取的提示词子包。

本子包（law_en_extract.prompt）包含发送给大模型的三个核心组件：
- schema  ：实体和关系的本体定义（Schema/Ontology），告诉模型要抽取什么
           -- 文件信息抽取（Legal Document / Legal Basis）
           -- 法条抽取（Legal Provision / Provision Unit / Citation）
- prompt  ：任务指令文本，告诉模型怎么抽取（Role + Task + Mandatory Rules）
- example ：多示例（few-shot examples），帮助模型学习正确的抽取格式

重要约定：
- schema/prompt/example 正文均使用英文，确保模型输出英文图谱标签
- 关系格式兼容现有 Langextract 解析器：extraction class="关系"，键名为中文
  "主体"/"谓词"/"客体"，但谓词值必须是英文（如 CONTAINS、CITES、BASED_ON）
"""

# 从各子模块导入对外使用的常量和变量
from app.infrastructure.information_extraction.law_en_extract.prompt.example import (
    example_for_clause,       # 法条抽取的 few-shot 示例
    example_for_file_info,    # 文件信息抽取的 few-shot 示例
)
from app.infrastructure.information_extraction.law_en_extract.prompt.prompt import (
    prompt_for_clause,        # 法条抽取的系统提示词
    prompt_for_file_info,     # 文件信息抽取的系统提示词
)
from app.infrastructure.information_extraction.law_en_extract.prompt.schema import (
    schema_for_clause,        # 法条抽取的实体/关系定义
    schema_for_file_info,     # 文件信息抽取的实体/关系定义
)

__all__ = [
    "example_for_clause",
    "example_for_file_info",
    "prompt_for_clause",
    "prompt_for_file_info",
    "schema_for_clause",
    "schema_for_file_info",
]
