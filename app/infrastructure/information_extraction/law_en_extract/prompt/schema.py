"""
英文法律法条 LLM 抽取的实体/关系本体定义（Ontology Schema）。

中文说明：
- schema 字符串正文均为英文，直接发给大模型，不包含任何中文内容。
- 中文翻译仅存在于 Python # 注释中，不会影响模型输出。
- 关系抽取兼容现有 Langextract 解析器：extraction class="关系"，
  键名"主体/谓词/客体"，但谓词值必须为英文。

实体类型对照：
  Legal Document        = 法规文件
  Legal Basis           = 法规依据（当前法规明确引用的上位法或授权法）
  Legal Provision       = 法条（Section/Article 层级）
  Provision Unit        = 条款单元（subsection/paragraph/clause/item 等）
  Citation              = 引用（法条中明确引用的其他法律文件或具体条款）

关系类型对照：
  BASED_ON    = 依据…制定（法规文件基于上位法制定的关系）
  CONTAINS    = 包含（上级实体包含下级实体）
  CITES       = 引用（条款单元引用其他文件或条款）
"""

# 关系抽取中使用的 extraction class 和属性键（中文，用 unicode 转义）
# 兼容现有 Langextract 解析器的格式要求
RELATION_CLASS = "\u5173\u7cfb"  # 关系
SUBJECT_KEY = "\u4e3b\u4f53"     # 主体（source）
PREDICATE_KEY = "\u8c13\u8bcd"   # 谓词（relation type）
OBJECT_KEY = "\u5ba2\u4f53"      # 客体（target）


# =============================================================================
# schema_for_file_info：文件信息抽取本体
# =============================================================================
# 实体：Legal Document（法规文件）
#   - official_title       正式标题        - document_type      文件类型
#   - short_title          短标题          - codification_text  法典位置文本
#   - abbreviation         缩写            - authority_text     授权依据文本
#   - legal_citation       法律引用编号
#   - compliance_risk_type 合规风险类型
#   - publication_effective_info  发布生效信息
#   - purpose              制定目的        - domain            领域
#   - applicable_industry  适用行业        - scope_of_application  适用范围
#   - issuing_authority    发布单位        - publication_date  发布日期
#   - effective_date       生效日期        - status            当前状态
#
# 实体：Legal Basis（法规依据，即授权本法规的上位法）
#   - official_title       依据文件正式标题
#   - abbreviation         缩写
#   - legal_citation       法律引用编号
#   - basis_role           依据角色（authority/authorization/delegation等）
#
# 关系：
#   BASED_ON    : Legal Document → Legal Basis（法规依据上位法制定）
# =============================================================================

schema_for_file_info = f"""
# Entities

## Legal Document
### Description
The legal document described by the file_info text. There must be exactly one
Legal Document entity in each file-info extraction task.
### Properties
- official_title: full official title of the legal document
- short_title: short title if explicitly stated
- abbreviation: abbreviation if explicitly stated, such as IEEPA or FIRRMA
- legal_citation: official citation or codified citation, such as 50 U.S.C. 1701 et seq.
- document_type: Act, Code, Regulation, CFR Part, Rule, Order, Directive, Treaty or other
- compliance_risk_type: use only the Chinese enum values [产品法律风险、供应链合规风险、劳动用工法律合规风险、企业关联方合规风险、企业国际化经营合规风险、企业信用风险], or an empty value. If the file text contains a very explicit compliance risk type statement, choose the corresponding Chinese enum value. If the file text does not contain clear relevant content, set this property to an empty value, such as "". Example: if the text contains "合规风险类型：产品法律风险", output "合规风险类型": "产品法律风险"; if there is no explicit relevant content, output "合规风险类型": "". This field must be based entirely on explicit text and its specific description; do not add, infer or invent any value. Multiple results are allowed.
- codification_text: codification or location text, such as title/chapter/part path
- authority_text: authority paragraph or authority line as stated in the text
- publication_effective_info: publication, enactment, commencement or effective information
- purpose: stated legislative or regulatory purpose
- domain: legal or policy domain
- applicable_industry: industries or sectors to which the document applies, if stated
- scope_of_application: territory, time, subject matter or regulated object scope
- issuing_authority: issuing, enacting or administering authority if stated
- publication_date: publication or enactment date if stated
- effective_date: effective or commencement date if stated
- status: current status if stated

## Legal Basis
### Description
A legal document explicitly cited as an authority or legal basis for the Legal
Document. Do not treat a mere codification path as a Legal Basis.
### Properties
- official_title: full official title of the basis document
- abbreviation: abbreviation if stated
- legal_citation: citation of the basis document or authority provision
- basis_role: authority, authorization, delegation, amendment basis or other stated role

# Relations

## BASED_ON
### Triple
Legal Document - BASED_ON - Legal Basis

# Parser Compatibility Note
Relations must be emitted as extraction class "{RELATION_CLASS}" with
attributes "{SUBJECT_KEY}", "{PREDICATE_KEY}" and "{OBJECT_KEY}". Predicate
values must be English relation names, such as "BASED_ON".
"""


# =============================================================================
# schema_for_clause：法条抽取本体
# =============================================================================
# 实体：Legal Provision（法条，即当前处理的 Section/Article）
#   - provision_number     法条编号        - provision_heading  法条标题
#   - core_topic           核心主题        - scope_of_effect    效力范围
#   - classification_context  层级上下文   - applicable_industry  适用行业
#
# 实体：Provision Unit（条款单元，法条内的语义子单元）
#   - unit_content         单元内容
#   - unit_heading         单元标题
#   - unit_level           单元层级（subsection/paragraph/subparagraph/clause/item/note/reserved）
#   - unit_number          单元编号
#   - applicable_industry  适用行业
#   - function_type        功能类型：
#       obligation=义务  prohibition=禁止  authorization=授权
#       procedure=程序   liability=责任   definition=定义
#       exception=例外   limitation=限制   rule=规则  other=其他
#   - quantitative_feature 量化特征
#   - quantitative_condition 量化条件
#   - applicable_subject   适用主体        - responsible_role    责任角色
#   - conduct_description  行为描述        - condition           适用条件
#   - legal_consequence    法律后果        - exception           例外情形
#   - time_element         时间要素
#   - other_information    其他信息
#
# 实体：Citation（引用，Provision Unit 中明确引用的法律文件或条款）
#   - citation_type        引用类型（Document文件级/Provision条款级）
#   - official_title       被引用文件标题  - abbreviation        缩写
#   - document_type        被引用文件类型  - provision_number    被引用条款编号
#   - is_internal_reference  是否内部引用（Yes/No）
#   - citation_relation    引用关系（pursuant to/under/subject to等）
#   - citation_purpose     引用目的（authority/exception/definition/procedure等）
#
# 关系：
#   CONTAINS: Legal Provision → Provision Unit（法条包含条款单元）
#   CITES   : Provision Unit → Citation（条款单元引用某文件或条款）
# 
# TODO:applicable_industry字段可能需要审查一下
#   =============================================================================

schema_for_clause = f"""
# Entities

## Legal Provision
### Description
The section-level or article-level provision being processed. There must be
exactly one Legal Provision entity in each clause extraction task.
### Properties
- provision_number: section or article number, such as Section 1701 or § 123.1
- provision_heading: heading of the provision
- core_topic: central legal topic or regulatory issue
- scope_of_effect: scope of application by territory, time, subject or object
- classification_context: title/chapter/subchapter/part/subpart context if provided
- applicable_industry: choose one from [manufacturing, electronic information, general, other]. Choose manufacturing if the provision applies to manufacturing but not electronic information; choose electronic information if it applies to electronic information but not manufacturing; choose general if it applies to both; choose other if it is unrelated to both.

## Provision Unit
### Description
A minimal normative text unit inside the Legal Provision that can form a stable
citation identifier, be understood relatively independently and be applied
independently. Provision Units usually correspond to subsections, paragraphs,
subparagraphs, clauses or items. Do not arbitrarily split individual sentences.
Extraction criteria:
1. Structurally, the unit should correspond to an established numbered level,
   such as subsection, paragraph, subparagraph, clause or item.
2. Semantically, the unit should express a relatively complete subject, conduct,
   condition, obligation, right, prohibition, procedure or legal consequence.
3. If an item is incomplete by itself but can inherit the lead-in condition from
   its parent paragraph, use "parent lead-in + item text" as the Provision Unit.
4. If each item can be independently applied after adding the parent lead-in,
   extract only the items and do not separately extract the whole parent unit.
5. If the items cannot be separately understood or independently applied,
   extract the whole parent unit as one Provision Unit.
6. The unit should have a stable citation position, such as "Section 1701(a)" or
   "Section 1701(a)(1)".
### Properties
- unit_content: complete text content of the unit
- unit_heading: heading immediately attached to the unit, if any
- unit_level: section, subsection, paragraph, subparagraph, clause, item, note or reserved
- unit_number: specific number of the unit corresponding to unit_level; do not include chapter, part or descriptive title, such as Section 1701(a) or Section 1701(a)(1)
- applicable_industry: choose one from [manufacturing, electronic information, general, other] using the same rule as Legal Provision
- function_type: obligation, prohibition, authorization, procedure, liability, definition, exception, limitation, rule or other
- quantitative_feature: choose one from [qualitative, quantitative, mixed]. Qualitative means the unit states conduct, rights, obligations or principles without specific quantities; quantitative means it contains concrete numbers, amounts, deadlines, ratios, ranges or thresholds; mixed means it contains both qualitative and quantitative elements.
- quantitative_condition: exact or lightly normalized phrase or sentence from the provision text describing quantified constraints, such as amount, numerical range, multiple, ratio, deadline or threshold
- applicable_subject: subject directly governed by the unit
- responsible_role: role of the subject in the unit
- conduct_description: required, permitted or prohibited conduct
- condition: condition under which the unit applies
- legal_consequence: consequence of compliance, violation or applicability
- exception: exceptions or carve-outs
- time_element: deadlines, effective dates, durations or timing requirements
- other_information: other explicit information that does not fit the above fields

## Citation
### Description
A legal document or specific provision explicitly cited by a Provision Unit.
Do not invent citations. Do not turn the current provision itself into a
Citation unless the text explicitly self-references it.
Each Citation must be a single entity. If the text cites multiple documents or
multiple provisions, create separate Citation entities.
### Properties
- citation_type: Document or Provision. Only use Document when the unit cites a whole document; use Provision when the citation contains a specific provision number.
- official_title: full title of the cited legal document if stated or clearly named
- abbreviation: abbreviation if stated
- document_type: Act, Code, Regulation, CFR Part, Rule, Order, Directive, Treaty or other
- provision_number: cited provision number if any. It must contain only section/article/subsection/paragraph/item numbering and must not include chapter, part, subpart or descriptive titles.
- is_internal_reference: Yes if the citation points to the same document; otherwise No
- citation_relation: textual relation such as pursuant to, under, subject to, except as provided in, defined in, reference
- citation_purpose: authority, exception, definition, procedure, penalty, supplement or other explicit purpose

# Relations

## CONTAINS
### Triple
Legal Provision - CONTAINS - Provision Unit

## CITES
### Triple
Provision Unit - CITES - Citation

# Parser Compatibility Note
Relations must be emitted as extraction class "{RELATION_CLASS}" with
attributes "{SUBJECT_KEY}", "{PREDICATE_KEY}" and "{OBJECT_KEY}". Predicate
values must be English relation names, such as "CONTAINS" or "CITES".
"""
