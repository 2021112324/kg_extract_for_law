# =============================================================================
# 英文 schema 定义文件（Chinese Annotated Version / 中文翻译注解版）
# =============================================================================
# 说明：
# - schema 字符串会直接拼接到模型提示词中，因此 schema 本体保持英文。
# - 中文翻译写在注释中，用于维护者理解和查阅。
# =============================================================================
#
# 图谱实体类型对照表（Entity Type Translation）：
#   Legal Document  = 法规文件（整部法规如 Act、Order、Code 等）
#   Legal Basis     = 法规依据（当前法规的上位法或制定依据）
#   Legal Provision = 法条（Section/Article 层级的法律条文）
#   Provision Unit  = 条款单元（法条内部的细分段落 subsection/paragraph/clause）
#   Citation        = 引用依据（法条中引用的其他法律文件或具体条款）
#
# 图谱关系类型对照表（Relation Type Translation）：
#   CONTAINS = 包含关系（上级实体包含下级实体，如法规包含法条、法条包含条款单元）
#   BASED_ON = 依据/制定依据关系（法规文件基于其上位法制定）
#   CITES    = 引用关系（条款单元引用其他法条或文件）
#
# 注意：
# - 关系抽取仍要兼容现有 Langextract 解析器，所以关系 entity 使用中文 class/key
#   （通过 unicode 转义表示），但谓词值（predicate value）必须用英文。
# =============================================================================

# 关系抽取的 class 名称和属性名（中文，用 unicode 转义表示）
# 这是为了兼容现有 Langextract 解析器
_RELATION_CLASS = "\u5173\u7cfb"      # 关系
_SUBJECT_KEY = "\u4e3b\u4f53"         # 主体（source）
_PREDICATE_KEY = "\u8c13\u8bcd"       # 谓词（relation type）
_OBJECT_KEY = "\u5ba2\u4f53"          # 客体（target）

# =============================================================================
# schema_for_clause：单条英文法条的实体和关系定义
#
# 中文翻译摘要：
#   实体（Entities）：
#     - Legal Provision：当前处理的 Section / Article 法条。
#       每个 clause 抽取任务中必须有且仅有一个 Legal Provision。
#       属性：core_topic（核心主题）、scope_of_effect（效力范围）、
#             applicable_industry（适用行业）
#     - Provision Unit：法条内部的完整语义单元，如 section/subsection/
#       paragraph/subparagraph/clause/item。
#       属性：unit_content（单元全文）、unit_heading（单元标题）、
#             unit_level（单元层级）、unit_number（单元编号）、
#             applicable_industry（适用行业）、function_type（功能类型：
#             obligation=义务/prohibition=禁止/authorization=授权/
#             procedure=程序/liability=责任/definition=定义/exception=例外/
#             rule=规则/other=其他）、applicable_subject（适用主体）、
#             responsible_role（责任角色）、conduct_description（行为描述）、
#             condition（适用条件）、legal_consequence（法律后果）、
#             exception（例外情形）、time_element（时间要素）、
#             quantitative_standard（量化标准）、other_information（其他信息）
#     - Citation：Provision Unit 明确引用的法律文件或具体条款。
#       属性：citation_type（引用类型：Document=文件级/Provision=条款级）、
#             official_title（被引用文件正式标题）、alias（别名/简称）、
#             document_type（文件类型：Act/Code/Regulation/Rule/Order/
#             Directive/Treaty/other）、provision_number（被引用条款编号）、
#             is_internal_reference（是否内部引用：Yes/No）、
#             citation_relation（引用关系描述）、citation_purpose（引用目的）
#   关系（Relations）：
#     - CONTAINS：Legal Provision -CONTAINS-> Provision Unit（法条包含条款单元）
#     - CITES：Provision Unit -CITES-> Citation（条款单元引用引用依据）
#   解析器兼容说明：
#     - 关系必须使用中文 extraction class "关系" 和中文键名
#       "主体"/"谓词"/"客体"，但谓词值为英文如 "CONTAINS"/"CITES"
# =============================================================================
schema_for_clause = f"""
# Entities

## Legal Provision
### Description
The section-level or article-level provision being processed. There must be
exactly one Legal Provision in each clause extraction task.
### Properties
- core_topic: the central legal topic or regulatory issue of the provision
- scope_of_effect: the scope of application by territory, time, subject or object
- applicable_industry: manufacturing, electronic information, general, other, or another explicit industry if stated

## Provision Unit
### Description
A complete semantic unit inside the Legal Provision, such as a section,
subsection, paragraph, subparagraph, clause or item.
### Properties
- unit_content: the complete text content of the unit
- unit_heading: the heading immediately attached to the unit, if any
- unit_level: section, subsection, paragraph, subparagraph, clause or item
- unit_number: the normalized English unit number, for example "Section 1702 subsection (a)"
- applicable_industry: the industry to which the unit applies, if stated
- function_type: obligation, prohibition, authorization, procedure, liability, definition, exception, rule or other
- applicable_subject: the subject directly governed by the unit
- responsible_role: the role of the subject in the unit
- conduct_description: the required, permitted or prohibited conduct
- condition: the condition under which the unit applies
- legal_consequence: the consequence of compliance or violation
- exception: exceptions or carve-outs
- time_element: deadlines, effective dates, durations or timing requirements
- quantitative_standard: numbers, thresholds, ratios, monetary amounts or other quantified requirements
- other_information: other explicit information that does not fit the above fields

## Citation
### Description
A legal document or specific provision explicitly cited by a Provision Unit.
### Properties
- citation_type: "Document" or "Provision"
- official_title: full official title of the cited legal document, if stated
- alias: common short title or abbreviation, if stated
- document_type: Act, Code, Regulation, Rule, Order, Directive, Treaty or other
- provision_number: specific cited provision number, if any, such as "Section 1702"
- is_internal_reference: "Yes" if the citation points to the same document; otherwise "No"
- citation_relation: the textual relationship such as pursuant to, under, subject to, except as provided in, defined in, reference
- citation_purpose: the purpose of the citation, such as authority, exception, definition, penalty, procedure or supplement

# Relations

## CONTAINS
### Triple
Legal Provision - CONTAINS - Provision Unit

## CITES
### Triple
Provision Unit - CITES - Citation

# Parser Note
Relations must be emitted as extraction class "{_RELATION_CLASS}" with
attributes "{_SUBJECT_KEY}", "{_PREDICATE_KEY}" and "{_OBJECT_KEY}". The
predicate values must be English relation names such as "CONTAINS" or "CITES".
"""

# =============================================================================
# schema_for_file_info：英文法规文件头信息的实体和关系定义
#
# 中文翻译摘要：
#   实体（Entities）：
#     - Legal Document：当前文件信息描述的法规文件。
#       每个 file-info 抽取任务中必须有且仅有一个 Legal Document。
#       属性：official_title（正式标题）、document_number（文件编号）、
#             alias（别名/简称）、document_type（文件类型：Act/Code/
#             Regulation/Rule/Order/Directive/Treaty/other）、
#             publication_effective_info（发布生效信息）、purpose（制定目的）、
#             domain（法律/政策领域）、applicable_industry（适用行业）、
#             scope_of_application（适用范围）、issuing_authority（发布单位）、
#             publication_date（发布日期）、effective_date（生效日期）、
#             status（当前状态）
#     - Legal Basis：文本中明确引用作为 Legal Document 授权或依据的其他法律文件。
#       属性：official_title（依据文件正式标题）、alias（别名/简称）
#   关系（Relations）：
#     - BASED_ON：Legal Document -BASED_ON-> Legal Basis（法规依据其上位法制定）
#   解析器兼容说明：
#     - 关系必须使用中文 extraction class "关系" 和中文键名
#       "主体"/"谓词"/"客体"，但谓词值必须为英文 "BASED_ON"
# =============================================================================
schema_for_file_info = f"""
# Entities

## Legal Document
### Description
The legal document described by the file information. There must be exactly one
Legal Document in each file-info extraction task.
### Properties
- official_title: full official title
- document_number: document number or legal citation, if stated
- alias: short title or abbreviation, if stated
- document_type: Act, Code, Regulation, Rule, Order, Directive, Treaty or other
- publication_effective_info: publication and commencement information as stated
- purpose: stated enactment or regulatory purpose
- domain: legal or policy domain
- applicable_industry: industries to which the document applies, if stated
- scope_of_application: territory, time, subject or object scope
- issuing_authority: issuing or enacting authority
- publication_date: publication or enactment date
- effective_date: effective or commencement date
- status: current status, if stated

## Legal Basis
### Description
Another legal document explicitly cited as an authority or basis for this Legal
Document.
### Properties
- official_title: full official title of the basis document
- alias: short title or abbreviation, if stated

# Relations

## BASED_ON
### Triple
Legal Document - BASED_ON - Legal Basis

# Parser Note
Relations must be emitted as extraction class "{_RELATION_CLASS}" with
attributes "{_SUBJECT_KEY}", "{_PREDICATE_KEY}" and "{_OBJECT_KEY}". The
predicate value must be the English relation name "BASED_ON".
"""
