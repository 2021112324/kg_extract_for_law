"""Keep the law ontology; adapt numeric locations locally, not in zh_law."""
import re
from ...zh_law.prompt.schema import schema_for_clause as LAW_CLAUSE, schema_for_file_info


def replace_attribute(schema, name, description):
    return re.sub(r"(?m)^- " + re.escape(name) + r"[：:].*$", "- " + name + "：" + description, schema)


schema_for_clause = replace_attribute(LAW_CLAUSE, "单元编号", "默认使用本次承载number。真实局部列项可保留为number加原标记，例如number=1.1且原文为（1），填1.1（1）。不得把（1）改造成不存在的1.1.1；不得转换为不存在的第X条。")
schema_for_clause = replace_attribute(schema_for_clause, "条款编号", "被引真实编号，可为1.1或1.1.1及原文款项，也可为原文中的第X条；保留点号。")
schema_for_clause = replace_attribute(schema_for_clause, "功能类型", "禁止/必要/可选，非约束内容允许空字符串并在其他信息说明，不强迫定义成为义务。")
# Remove incompatible prose restrictions as well as adapting the attribute.
schema_for_clause = re.sub(r"(?m)^.*(?:只能是包含条、款、项|严禁.*包含章、节|法条变化为实体标识).*$", "", schema_for_clause)
schema_for_clause = schema_for_clause.replace("### 属性\n", "### 属性\n- 条：本次输入的真实承载编号。\n- 法条全文：留空字符串，由程序保存原始目标全文；不要生成或改写全文。\n- 上级上下文：留空字符串，由程序保存输入中的真实上下文。\n- 来源编号列表：可留空数组，由程序保存已确认的来源编号。\n- 原文依据：支持该法条的目标原文片段。\n", 1)
schema_for_clause = schema_for_clause.replace("- 条款单元内容：", "- 原文依据：当前目标原文中直接支持本单元的逐字片段。\n- 上下文依据：确实继承的上级原文逐字片段，允许来自context或当前target内局部列项之前的标题、总领句，不要求特定结尾标点；不得借用其他列项或后续内容；不需要则为空字符串。\n- 条款单元内容：")
schema_for_clause = schema_for_clause.replace("- 引用类型：", "- 引用原文：包含引用对象和编号的当前目标原文逐字片段。\n- 引用类型：")
schema_for_clause += "\n# 数字规则补充\n数字编号是原文真实定位，不限制在同一深度。上下文仅解释目标，不输出上下文中的独立法条。\n"
schema_for_clause += "\n# 关系\n## 包含\n### 关系三元组\n- 法规文件-包含-法条\n- 法条-包含-法条\n- 条款单元-引用-法条\n"

schema_for_group = re.sub(r"判断标准：[\s\S]*?(?=### 属性)",
    "判断标准：本次是已确认不可独立拆分的完整条款组。只能提取保留全部条件的完整规范，不再将每个子项另建条款单元。单元编号使用承载number而非成员编号。\n", schema_for_clause)

schema_for_boundary = """
# 实体
## 承载判定
### 属性
- 处理方式：整组/继承/独立。整组表示条件必须共同使用，继承表示子项分别带父级前提，独立表示父子各有独立正文。
- 成员编号：整组必须列出需整体适用的真实子编号字符串数组，不含父编号；其他方式为空数组。
- 证据原文：判断条件逻辑的父级目标原文逐字片段。
# 关系
"""
