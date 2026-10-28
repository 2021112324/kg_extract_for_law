import re
from ...guide_p1.prompt.schema import (
    schema_for_file_info as P1_FILE, schema_for_content_block as P1_CONTENT,
    KNOWLEDGE_TYPE_VALUES as P1_KNOWLEDGE, CONSTRAINT_STRENGTH_VALUES as P1_STRENGTH,
    RISK_TYPE_VALUES, RESPONSIBILITY_ROLE_VALUES, QUANTITATIVE_VALUE_TYPE_VALUES, QUANTITATIVE_RELATION_VALUES,
)

KNOWLEDGE_TYPE_VALUES = P1_KNOWLEDGE + ("目录归属", "范围界定")
CONSTRAINT_STRENGTH_VALUES = P1_STRENGTH + ("不适用",)
schema_for_file_info = P1_FILE.replace("规划、方案、指南、指引或其他指导性文件", "目录、清单、短分点文书或其他指导性文件").replace(
    "规划、方案、指南、指引、意见或其他指导性文件", "目录、清单、规划、方案、指南、指引、意见或原文明示类型")
schema_for_content = P1_CONTENT.replace(
    "能够独立表达一项政策目标、行动任务、指导性要求、禁止性要求或保障措施的最小完整命题。背景、现状、原则和脱离上文无法理解的碎片不得创建该实体。",
    "有目标及授权上下文依据的完整命题；目录归属、范围界定也是知识。名词条目不等于无知识，但不能凭条目名称推断禁止、义务或处罚。")
schema_for_content = re.sub(r"(?m)^- 知识类型：.*$", "- 知识类型：" + "/".join(KNOWLEDGE_TYPE_VALUES), schema_for_content)
schema_for_content = re.sub(r"(?m)^- 约束强度：.*$", "- 约束强度：" + "/".join(CONSTRAINT_STRENGTH_VALUES), schema_for_content)
schema_for_content = schema_for_content.replace("- 行为描述：", "- 上下文依据：字符串数组，逐字引用当前条目context中确实用于补全命题的片段；无则为空数组。\n- 行为描述：非行为型目录/范围知识留空；")
# Attribution belongs to the transport contract and is removed before graph creation.
schema_for_content = re.sub(r"(?m)^### 属性$", "### 属性\n- 目标条目ID：必须逐字复制对应输入entry_id，不得自行生成。", schema_for_content)
schema_for_content = schema_for_content.replace("# 关系", """## 条目处理
### 介绍
每个输入条目必须恰好返回一条处理回执，仅用于执行记录，不写入图谱。
### 属性
- 目标条目ID：对应输入entry_id。
- 处理状态：知识/非知识/不确定。
- 理由：说明该条目有知识、确无知识或无法确认的原因，不得因名词无动词判为非知识。

# 关系""", 1)
schema_for_content_model = schema_for_content
schema_for_content += """
## 提出
### 关系三元组
- 指引结构节点-提出-指引知识单元
"""
STRUCTURE_PROPERTIES = frozenset(("结构类型", "显示编号", "节点标题", "节点内容", "完整路径", "顺序", "来源原文", "source_file"))
