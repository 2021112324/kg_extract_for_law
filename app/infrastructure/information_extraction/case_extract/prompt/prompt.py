from typing import List



# def default_chunking_prompt() -> str:
#     """构建默认分块提示词"""
#     legal_structure, predefined_sections = default_legal_structure()
#     sections_str = ", ".join(predefined_sections)
#
#     sections_json = {section: "分块类别对应的文本内容" for section in predefined_sections}
#
#     prompt = f"""
# # 角色
# 你是一位专业的法律文书分析师，按诉讼文书结构对文书内容进行分块。
#
# # 任务
# 请将给定的法律文书中按照其内在结构划分为不同的部分。严格按照结构说明，将文本划分为分块类别下的分块文本，并以JSON格式输出。
# ⚠️ 注意：
# # 分块结构说明：
# {legal_structure}
#
# # 要求：
# 1. 根据文书的实际内容，选择最合适的分块类别
# 2. 禁止创建新的类别名称，禁止将一个分块内容进行拆分或合并
# 3. **⚠️重要**：保持原始文本的完整性，保证分块内容对应完整段落，不要修改内容
# 4. 每个分块应具有明确的法律意义和逻辑连贯性
#
# # 输出格式要求
# 输出格式必须为JSON格式，格式如下：
# """ + str(sections_json) + """
#
# """
#     return prompt


