"""
英文法律法条抽取的 LLM 提示词。

中文说明：
- prompt 字符串正文均为英文，直接发给大模型，不包含任何中文内容。
- 中文翻译仅存在于 Python # 注释中，不会发送给模型。
- prompt 正文使用英文是为了让模型稳定输出英文实体类型、属性和关系。
"""

from app.infrastructure.information_extraction.law_en_extract.prompt.schema import (
    OBJECT_KEY,       # 客体 — 关系三元组中的 target
    PREDICATE_KEY,    # 谓词 — 关系三元组中的 relation type
    RELATION_CLASS,   # 关系 — 关系实体的 extraction class 名称
    SUBJECT_KEY,      # 主体 — 关系三元组中的 source
)


# =============================================================================
# prompt_for_file_info：文件信息抽取指令
# =============================================================================
# [中文翻译]
# 角色：你是英文法律和监管文件分析专家。
# 任务：从给定英文法律文件的 file_info 文本中抽取结构化实体和关系，
#       抽取结果首先供人工审阅，不直接入库知识图谱。
# 强制性规则：
#   1. 使用英文实体类型、节点名称、属性名、属性值和关系谓词值。
#   2. 对当前 file_info 提取且仅提取一个 Legal Document（法规文件）实体。
#   3. 仅当文本明确出现授权或法律依据时才提取 Legal Basis 实体，
#      如 Authority:、pursuant to、under、in accordance with 等措辞。
#   4. 不要推断文本中未明确说明的日期、发布单位、状态、目的或依据。
# 解析器兼容规则：关系记录必须使用 "{RELATION_CLASS}" 作为 extraction class，
#   属性键为 "{SUBJECT_KEY}"、"{PREDICATE_KEY}"、"{OBJECT_KEY}"，但键值必须为英文。

prompt_for_file_info = f"""
# Role
You are an expert in English legal and regulatory document analysis.

# Task
Extract structured entities and relations from the given file_info text of an
English legal document. The result is for review first, not for direct graph
storage.

# Mandatory Rules
1. Use English entity types, node names, property names, property values and
   relation predicate values.
2. Extract exactly one Legal Document entity for the current file_info.
3. Extract Legal Basis entities only when the text explicitly states an
   authority or legal basis, such as "Authority:", "pursuant to", "under",
   "in accordance with", or similar wording.
4. Do not infer unstated dates, issuing authorities, status, purpose or basis.

# Parser Compatibility Rule
For relation records only, use extraction class exactly "{RELATION_CLASS}" and
attribute keys exactly "{SUBJECT_KEY}", "{PREDICATE_KEY}" and "{OBJECT_KEY}".
Their values must still be English.
"""


# =============================================================================
# prompt_for_clause：法条抽取指令
# =============================================================================
# [中文翻译]
# 角色：你是英文成文法、行政法规、CFR 条款和公法修正案分析专家。
# 任务：从一条英文 Legal Provision（法条）中抽取结构化实体和关系，
#       抽取结果首先供人工审阅，不直接入库知识图谱。
# 强制性规则：
#   1. 输入文本整体描述且仅描述一条 Legal Provision。
#   2. 提取 Provision Unit 实体，覆盖法条内的完整语义单元：
#      subsection（款）、paragraph（项）、subparagraph（目）、clause（段）、
#      item（小项）、note（注释）、reserved（保留）单元。
#   3. 如果拆分低层级条目后丢失上级上下文，不要拆为独立单元；
#      保留上级 paragraph/subsection 作为单元，将条目文本放在 unit_content 中。
#   4. 在 hierarchy_path 和 unit_number 中保留上级上下文信息。
#   5. 仅对文本中明确出现的引用提取 Citation 实体。
#   6. "this section"/"this subchapter"/"this title"/"this Act"
#      指向同一文件的引用视为内部引用（is_internal_reference=Yes）。
#   7. 不要凭空编造法律后果、日期、主体、引用或法律依据。
# 解析器兼容规则：同上。

prompt_for_clause = f"""
# Role
You are an expert in English statutes, regulations, CFR provisions and public
law amendments.

# Task
Extract structured entities and relations from one English Legal Provision.
The result is for review first, not for direct graph storage.

# Mandatory Rules
1. The input text as a whole describes exactly one Legal Provision.
2. Extract Provision Unit entities for complete legal semantic units inside the
   provision, such as subsections, paragraphs, subparagraphs, clauses, items,
   notes or reserved units.
3. Do not split a lower-level item into a standalone unit if it loses parent
   context. In that case, keep the parent paragraph or subsection as the unit
   and include the item text inside unit_content.
4. Preserve parent context in hierarchy_path and unit_number.
5. Extract Citation entities only for citations explicitly present in the text.
6. Treat references such as "this section", "this subchapter", "this title" or
   "this Act" as internal references when they point to the same document.
7. Do not invent legal consequences, dates, subjects, citations or legal bases.

# Parser Compatibility Rule
For relation records only, use extraction class exactly "{RELATION_CLASS}" and
attribute keys exactly "{SUBJECT_KEY}", "{PREDICATE_KEY}" and "{OBJECT_KEY}".
Their values must still be English.
"""
