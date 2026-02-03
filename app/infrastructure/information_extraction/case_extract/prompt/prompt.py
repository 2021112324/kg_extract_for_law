from typing import List

from app.infrastructure.information_extraction.case_extract.prompt.structure_for_chunk import \
    default_predefined_sections, default_legal_structure


def default_chunking_prompt() -> str:
    """构建分块提示词"""
    sections_str = ", ".join(default_predefined_sections)

    sections_json = {section: "分块类别对应的文本内容" for section in default_predefined_sections}

    prompt = f"""
# 角色
你是一位专业的法律文书分析师，擅长识别和解析判决书的结构。

# 任务
请将给定的法律文书中按照其内在结构划分为不同的部分。严格按照结构说明，将文本划分为分块类别下的分块文本，并以JSON格式输出。
⚠️ 注意：

# 结构说明：
{default_legal_structure}

# 允许的分块类别：
{sections_str}

# 要求：
1. 根据文书的实际内容，选择最合适的分块类别
2. 如果某些内容不属于预定义类别，可以创建新的类别名称
3. 保持原始文本的完整性，不要修改内容
4. 每个分块应具有明确的法律意义和逻辑连贯性

# 输出格式要求
输出格式必须为JSON格式，格式如下：
""" + str(sections_json) + """

"""
    return prompt

