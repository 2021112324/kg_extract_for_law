# =============================================================================
# 英文 prompt 提示词文件（Chinese Annotated Version / 中文翻译注解版）
# =============================================================================
# 说明：
# - 下方字符串会直接发送给大模型，所以正文保持英文，确保模型输出英文图谱。
# - 中文注释（翻译 + 说明）只写在 Python 注释（# 开头）里，用于维护者理解。
# - Role                          = 角色（定义模型在对话中扮演的角色）
# - Task                          = 任务（描述模型需要完成的具体工作）
# - Mandatory English Graph Rule  = 强制英文图谱规则（要求所有图谱元素使用英文）
# - Parser Compatibility Rule     = 适配现有 Langextract 解析器的关系格式规则
# - Legal Provision Rules         = 法条抽取规则
# - Legal Document Rules          = 法规文件抽取规则
# =============================================================================

# 关系抽取的 class 名称和属性名（中文，用 unicode 转义表示）
# 这是为了兼容现有 Langextract 解析器，解析器要求关系 entity 使用特定的中文 class 名
_RELATION_CLASS = "\u5173\u7cfb"      # 关系
_SUBJECT_KEY = "\u4e3b\u4f53"         # 主体（source）
_PREDICATE_KEY = "\u8c13\u8bcd"       # 谓词（relation type）
_OBJECT_KEY = "\u5ba2\u4f53"          # 客体（target）

# =============================================================================
# prompt_for_clause：单条英文法条抽取提示词
#
# 中文翻译摘要：
#   你是英文法律法规分析专家，你需要从英文法律/法规/规章/命令/指令/法典和
#   条约式法律条文中抽取合规知识图谱数据。
#   任务：从给定的英文法律条文中抽取合规法律知识图谱的实体和关系。
#   输出必须使用英文实体类型、英文节点名、英文关系和英文属性。
#   不要输出中文图谱标签。
#   法条抽取规则：
#     1. 输入的文本整体描述的是一个 Legal Provision（法条）
#     2. 对完整的语义单元（section/subsection/paragraph/subparagraph/clause/item）
#        抽取 Provision Unit 节点
#     3. 如果枚举项本身不是完整法律语句，保留包含它们的 subsection/paragraph 作为 Provision Unit
#     4. 仅当条文明确引用其他法律文件或条款时才抽取 Citation 节点，不要虚构引用
#     5. 内部引用（如 this section/this title/this chapter/this Act）设置
#        is_internal_reference 为 "Yes"，否则为 "No"
#     6. provision_number 使用英文规范化编号格式
# =============================================================================
prompt_for_clause = f"""
# Role
You are an expert in English legal and regulatory analysis. You extract legal
knowledge graph data from English statutes, regulations, rules, orders,
directives, codes and treaty-style legal articles.

# Task
Extract the graph entities and relations needed for a compliance legal
knowledge graph from the given English legal provision.

# Mandatory English Graph Rule
All graph entity types, node names, relation values, property names and
property values must be English. Do not output graph labels from the Chinese
legal extractor.

# Parser Compatibility Rule
For relation records only, use extraction class exactly "{_RELATION_CLASS}" and
use relation attribute keys exactly "{_SUBJECT_KEY}", "{_PREDICATE_KEY}" and
"{_OBJECT_KEY}". Their values must still be English. For example, the predicate
value should be "CONTAINS", "CITES" or "BASED_ON".

# Legal Provision Rules
1. The input text as a whole describes exactly one Legal Provision.
2. Extract Provision Unit nodes for complete semantic units such as a section,
   subsection, paragraph, subparagraph, clause or item.
3. If enumerated items are not complete legal sentences by themselves, keep the
   containing subsection or paragraph as the Provision Unit.
4. Extract Citation nodes only when the provision explicitly refers to another
   legal document or another provision. Do not invent citations.
5. For internal citations such as "this section", "this title", "this chapter"
   or "this Act", set "is_internal_reference" to "Yes"; otherwise set it to
   "No".
6. For "provision_number", use English normalized numbering such as
   "Section 1702", "Section 206(a)", "Article 5", or "50 U.S.C. Section 1705".
"""

# =============================================================================
# prompt_for_file_info：英文法规文件头信息抽取提示词
#
# 中文翻译摘要：
#   你是英文法律文件分析专家，你需要从英文法律文件的文件头信息中抽取元数据和
#   授权/依据关系。
#   任务：从文件信息中抽取 Legal Document（法规文件）实体，以及文本中明确引用
#   作为发布或制定依据的 Legal Basis（法规依据）实体。
#   输出必须使用英文图谱标签、属性名和属性值。不要输出中文图谱标签。
#   法规文件抽取规则：
#     1. 文件信息描述的是一个 Legal Document
#     2. Legal Basis 必须是文本中明确指出的依据（通常由 pursuant to / under /
#        in accordance with / based on 等短语引入）
#     3. 不要推断文本中未说明的发布机关、日期或法律依据
# =============================================================================
prompt_for_file_info = f"""
# Role
You are an expert in English legal and regulatory document analysis. You
extract metadata and authority relationships for English legal documents.

# Task
Extract the Legal Document entity described by the file information and any
Legal Basis entities explicitly cited as the authority for issuing or enacting
the document.

# Mandatory English Graph Rule
All graph entity types, node names, relation values, property names and
property values must be English. Do not output Chinese graph labels.

# Parser Compatibility Rule
For relation records only, use extraction class exactly "{_RELATION_CLASS}" and
use relation attribute keys exactly "{_SUBJECT_KEY}", "{_PREDICATE_KEY}" and
"{_OBJECT_KEY}". Their values must still be English. For example, the predicate
value should be "BASED_ON".

# Legal Document Rules
1. The file information describes exactly one Legal Document.
2. Legal Basis entities must be explicit authorities in the text, usually
   introduced by phrases such as "pursuant to", "under", "in accordance with",
   "based on" or similar wording.
3. Do not infer issuing authorities, dates or legal bases that are not stated.
"""
