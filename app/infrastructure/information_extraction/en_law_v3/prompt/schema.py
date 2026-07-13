"""格式三美国 CFR/USC/ITAR `§` 文本 LLM 抽取 Schema。

重要约定：
1. `schema_for_file_info`、`schema_for_article` 和 `schema_for_provision_clause` 是实际传给 LLM 的 ontology 字符串。
2. 中文翻译注解必须写成 Python `#` 注释，不能写进 schema 字符串。
3. `合规风险类型` 由源文件元数据提供，不出现在 LLM schema 中强制抽取。
4. 格式三处理 CFR/USC/ITAR 联邦法规 `§` 格式，splitter 按 `§` 切分，内部按 `(a)` 切分 Subsection。
"""

# 关系记录在 Langextract 中以实体形式表达时使用的类名。
RELATION_CLASS = "关系"
# 关系主体字段名，保持与现有抽取框架兼容。
SUBJECT_KEY = "主体"
# 关系谓词字段名，保持与现有抽取框架兼容。
PREDICATE_KEY = "谓词"
# 关系客体字段名，保持与现有抽取框架兼容。
OBJECT_KEY = "客体"


# =============================================================================
# schema_for_file_info 中文翻译注解，仅供开发者阅读，不会进入 LLM 输入
# =============================================================================
# 总体说明：
# - 本 schema 只约束"文件级"知识图谱抽取，不处理 §/Subsection 内部条款单元。
# - 文件类型包括 CFR Part、ITAR Part、USC 编纂版、联邦成文法、州公司法等。
# - 实际传给 LLM 的字段名全部使用英文；下面中文仅用于开发者理解字段含义。
#
# ## LegalDocument / 法规文件
# LegalDocument：当前输入所描述的美国联邦法规文件，每个文件级输入必须且只能抽取一个。
# document_name：文件完整正式标题，优先使用原文标题（如 "Part 730 — General Information"）。
# document_type：法规类型，如 CFR_Part、ITAR_Part、USC_Compilation、State_Law、Federal_Statute。
# document_number：CFR/USC/Part 编号、Public Law 编号或其他官方文件编号。
# short_title：原文明确给出的简称、通称或缩写。
# issuing_authority：发布机关或制定机关，通常从文件头部的层级归属中提取。
# publication_date：发布日期、批准日期或通过日期，只取文本明确写出的日期。
# effective_date：生效日期或适用日期，只取文本明确写出的日期。
# subject_matter：文件规定的主题事项，用于概括"这部法规管什么"。
# purpose：制定目的或政策目标，只能来自明示的目的性表述。
# scope_of_application：适用范围，包括地域、主体、对象或活动范围。
# legal_basis_text：Authority 行中的法律授权依据原文，或编纂说明中的法律基础。
# applicable_industry：适用行业枚举，只能是 Manufacturing、Electronic Information Industry、General、Other。
# compliance_domains：合规域数组，记录法规涉及的合规管理主题，如 export control、data security、tax compliance。
# economic_industries：经济行业数组，一般适用时输出 ["General"]。
#
# ## LegalBasis / 法律依据
# LegalBasis：被当前法规文件明确作为法律基础、编纂依据或授权依据引用的法律。
# official_title：法律依据的完整标题或最佳可识别标题。
# citation_text：法律依据在原文中的引用文本。
# basis_role：依据角色，如 authority、codification_basis、amendment_basis。
#
# ## BASED_ON / 基于
# BASED_ON：表示 LegalDocument 基于某个 LegalBasis 制定、编纂或授权。
schema_for_file_info = f"""
# Entities

## LegalDocument
### Description
The United States CFR, USC, ITAR, state law or statutory compilation described by the file-info input.
Extract exactly one LegalDocument entity.
### Properties
- document_name: full official or best explicit act title
- document_type: CFR_Part, ITAR_Part, USC_Compilation, State_Law, Federal_Statute or other explicit type
- document_number: CFR/USC/Part number, Public Law number, chapter number or other explicit official number
- short_title: short title or common abbreviation if explicitly stated
- issuing_authority: issuing or adopting authority, usually United States Congress if explicit
- publication_date: enactment, approval or publication date if explicitly stated
- effective_date: effective date if explicitly stated
- subject_matter: stated subject matter
- purpose: stated purpose or policy objective based on explicit text only
- scope_of_application: stated territorial, subject, object or activity scope
- legal_basis_text: explicit authority or codification basis text if stated
- applicable_industry: choose one from [Manufacturing, Electronic Information Industry, General, Other]
- compliance_domains: string array of explicit compliance or regulatory domains
- economic_industries: string array. Use ["General"] when generally applicable

## LegalBasis
### Description
A law, United States Code title, public law or statutory authority explicitly
cited as a basis for the current document.
### Properties
- official_title: full or best explicit title
- citation_text: citation text as written
- basis_role: authority, codification_basis, amendment_basis or other explicit role

# Relations

## BASED_ON
LegalDocument - BASED_ON - LegalBasis

# Parser Compatibility Note
Relations must be emitted as extraction class "{RELATION_CLASS}" with
attributes "{SUBJECT_KEY}", "{PREDICATE_KEY}" and "{OBJECT_KEY}".
"""


# =============================================================================
# schema_for_article 中文翻译注解，仅供开发者阅读，不会进入 LLM 输入
# =============================================================================
# 总体说明：
# - 本 schema 只约束"单个 §"级别的条文头部抽取，不处理 Subsection/Paragraph 内部条款单元。
# - 格式三中 "Article" 实际对应 CFR/USC/ITAR 的 `§` Section 级别。
# - 字段名保持英文，中文说明仅作为源码注释，不进入 LLM 输入。
#
# ## LegalProvision / 法律条款（§ Section 级别）
# LegalProvision：当前输入的 § 级法律条款，每个 § 输入必须且只能抽取一个。
# provision_number：§ 编号，如 § 730.1、§ 120.1、§ 1030；含 § 符号。
# provision_number 注解：CFR 格式为 § [Part].[序号]（如 § 730.1），USC/州法格式为 § [整数]（如 § 101）。
# provision_heading：§ 标题或条名，只有原文明确提供时填写。
# core_topic：该 § 的核心监管主题，用简短英文概括。
# scope_of_effect：该 § 明示的效力范围或规制范围。
# applicable_industry：适用行业枚举，规则同文件级字段。
# compliance_domains：合规域数组，记录该 § 明确涉及的合规主题。
# economic_industries：经济行业数组，不能为空，通用规则同文件级字段。
# is_amendment_article：是否为修订型条款；只有明确 amend/repeal/insert/strike 等才为 true。
# amendment_target：被修订的文件或条款，必须来自原文明示目标。
# amendment_action：修订动作，如 amend、replace、insert、strike、repeal；未明示则为空。
#
# ## 禁止输出
# 在 § 级步骤中禁止输出 ProvisionClause、ProvisionTextParagraph 和 Citation 实体及关系。
schema_for_article = """
# Entities

## LegalProvision
### Description
The current code-split `§` legal provision. Extract exactly one
LegalProvision entity. Do not extract ProvisionClause, ProvisionTextParagraph,
ProvisionUnit or Citation in this step.
### Properties
- provision_number: section number with section sign, such as § 730.1, § 120.1 or § 1030
- provision_heading: section heading if explicitly stated
- core_topic: central topic of the section
- scope_of_effect: stated application scope of the section
- applicable_industry: choose one from [Manufacturing, Electronic Information Industry, General, Other]
- compliance_domains: string array of explicit compliance domains
- economic_industries: string array. Use ["General"] when generally applicable
- is_amendment_article: true if the section explicitly amends, repeals, inserts or strikes another law
- amendment_target: amended document/provision if explicitly stated
- amendment_action: amend, replace, insert, strike, repeal or empty if not stated

# Forbidden Output
Do not output ProvisionClause, ProvisionTextParagraph, ProvisionUnit or Citation
entities. Do not output CONTAINS or CITES relations in this section-level step.
"""


# =============================================================================
# schema_for_provision_clause 中文翻译注解，仅供开发者阅读，不会进入 LLM 输入
# =============================================================================
# 总体说明：
# - 本 schema 约束"单个 § 内部 Subsection"的 clause 和 paragraph 级抽取。
# - 输入是由 splitter 从 § 中切分出的 `(a)` subsection 文本片段。
# - ProvisionClause 对应 (a)/(b)/(c) 款，ProvisionTextParagraph 对应更深层片段。
# - 字段名保持英文，中文说明仅作为源码注释，不进入 LLM 输入。
#
# ## ProvisionClause / 条款子节（Subsection (a) 级别）
# ProvisionClause：当前 splitter 切分的 subsection，每个输入必须且只能抽取一个。
# unit_level：结构层级，通常为 subsection。
# unit_number 注解：由 splitter 代码锁定，LLM 不得修改。格式如 § 730.1(a)。
# unit_content 注解：由 splitter 代码锁定，LLM 不得修改。
# clause_summary：对该 subsection 内容的简洁摘要。
# clause_purpose：该 subsection 的明确目的，不明显时可留空。
# main_subject：被规制的主要主体，如 person、Secretary、manufacturer。
# main_action：被要求、禁止或许可的主要行为。
# main_object：主要行为的对象。
# legal_function：法律功能类型，只能选 prohibition、mandatory、optional；无法抽取时留空。
# legal_function 注解：shall not/must not/is prohibited 属于 prohibition。
# legal_function 注解：shall/must/is required 属于 mandatory。
# legal_function 注解：may/can/is entitled 属于 optional。
# has_quantitative_detail：是否存在具体数字、阈值、金额、比例、期限或公式。
# has_exception：是否存在例外或豁免条款。
# has_condition：是否存在适用条件。
# applicable_industry：适用行业枚举，规则同文件级字段。
# compliance_domains：合规域数组，记录该 subsection 明确涉及的合规主题。
# economic_industries：经济行业数组，不能为空。
#
# ## ProvisionTextParagraph / 条文文本段落
# ProvisionTextParagraph：ProvisionClause 内部的细粒度连续文本片段。
# unit_number：结构定位符，必须以父 ProvisionClause 的 unit_number 开头。
# unit_number 注解：如 § 730.1(a)(1)、§ 101(a)(1)(A)。
# unit_content：当前 ProvisionClause 的精确原文文本，不得改写、总结或翻译。
# unit_level：结构层级，如 paragraph_fragment、point、subpoint、clause_item。
# unit_purpose：该文本片段的明确目的或主题，不明显时可留空。
# quantitative_feature：量化特征，只能是 Qualitative 或 Quantitative。
# quantitative_feature 注解：有金额、比例、期限、次数、倍数、阈值等可核验数字条件时为 Quantitative。
# quantitative_indicator：量化指标对象或 null。
# quantitative_indicator.raw_text：量化条件的原文短语或句子。
# quantitative_indicator.value_type：量化值类型，如 amount、ratio、time_limit、count、multiple、other。
# quantitative_indicator.min：下限数值，没有下限时为 null。
# quantitative_indicator.max：上限数值，没有上限时为 null。
# quantitative_indicator.unit：原文明示单位，如 dollar、day、month、year、%、times。
# quantitative_indicator.relation：约束关系，如 range、lower_bound、upper_bound、equal、other。
# quantitative_indicator 联动规则：若该对象非空，quantitative_feature 必须为 Quantitative。
# quantitative_indicator 排除规则：引用型表述（如 "in accordance with § ..."）和模糊表述（如 "reasonable duration"）不是量化指标，应输出 null。
# applicable_subject：被该文本片段直接规制的主体。
# responsibility_role：主体在规则中的责任角色。
# conduct_description：被要求、允许或禁止的行为描述。
# condition：规则适用条件，例如 when、where、if、unless 引导的条件。
# legal_consequence：法律后果、处罚或效力后果。
# exception：例外、豁免或排除适用。
# time_element：日期、期限、期间或时间语义短语。
# other_information：无法放入上述字段但原文明示的重要信息。
#
# ## Citation / 引用
# Citation：ProvisionTextParagraph 明确引用的法律文件、CFR 条款、USC 条款、公法、行政命令或其他法律工具。
# citation_type：Document 或 Provision；引用整部法律工具时为 Document，引用具体条款时为 Provision。
# official_title：被引用文件或条款的完整标题。
# alias：被引用文件的简称或缩写。
# document_type：被引用对象类型，如 Act、Public Law、United States Code、CFR、Section、Regulation、Executive Order。
# citation_text：原文中的准确引用文本，如 "15 CFR 730.1"、"50 U.S.C. 4801"。
# provision_number：被引用的 section 或 CFR/USC 编号。
# is_internal_reference：Yes 表示指向当前文件内部引用，No 表示外部引用。
# citation_relation：引用关系，收敛为 reference/definition/exception/amendment/authorization/compliance/other。
# citation_purpose：引用目的，如 supplement、interpretation、exclusion、reference。
#
# ## Relations / 关系
# CONTAINS：ProvisionClause 包含 ProvisionTextParagraph；可嵌套包含子 ProvisionTextParagraph。
# CITES：ProvisionTextParagraph 引用 Citation。
schema_for_provision_clause = f"""
# Entities

## ProvisionClause
### Description
The current code-split subsection under one `§` provision. Extract
exactly one ProvisionClause entity. The fields unit_number and unit_content are
locked by code and must not be changed.
### Properties
- unit_level: normally subsection
- clause_summary: concise summary of this clause
- clause_purpose: explicit purpose if stated
- main_subject: main regulated subject if stated
- main_action: main required, prohibited or permitted action if stated
- main_object: object of the main action if stated
- legal_function: choose one from [prohibition, mandatory, optional], or leave empty if unknown
- has_quantitative_detail: true if concrete numbers, thresholds, amounts, ratios, periods or formulae are present
- has_exception: true if an exception or carve-out is present
- has_condition: true if a condition is present
- applicable_industry: choose one from [Manufacturing, Electronic Information Industry, General, Other]
- compliance_domains: string array of explicit compliance domains
- economic_industries: string array. Use ["General"] when generally applicable

## ProvisionTextParagraph
### Description
A fine-grained continuous source-text fragment inside the current
ProvisionClause. It must come only from the current ProvisionClause text and
must keep required lead-in context.
### Properties
- unit_number: structural locator. It must start with the parent ProvisionClause unit_number
- unit_content: exact source text from the current ProvisionClause only. Do not paraphrase or invent
- unit_level: paragraph_fragment, point, subpoint, clause_item or other
- unit_purpose: explicit purpose or topic if stated
- quantitative_feature: choose one from [Qualitative, Quantitative]
- quantitative_indicator: structured object or null for concrete and verifiable numbers, amounts, ratios, time limits, counts, multiples or thresholds. Use exactly {{"raw_text":"original phrase or sentence","value_type":"amount/ratio/time_limit/count/multiple/other","min":number or null,"max":number or null,"unit":"explicit unit or empty","relation":"range/lower_bound/upper_bound/equal/other"}}
- applicable_subject: subject directly governed by this text fragment
- responsibility_role: role of the subject if stated
- conduct_description: required, permitted or prohibited conduct
- condition: condition under which this fragment applies
- legal_consequence: consequence if stated
- exception: exception or carve-out if stated
- time_element: dates, deadlines, periods or timing requirements if stated
- other_information: other explicit information that cannot be placed above

## Citation
### Description
A legal document, U.S.C. provision, public law, section or instrument explicitly
cited by a ProvisionTextParagraph. Do not invent citations.
### Properties
- citation_type: Document or Provision
- official_title: full or best explicit title of the cited instrument if stated
- alias: common short title or abbreviation if stated
- document_type: Act, Public Law, United States Code, Section, Regulation or other
- citation_text: exact citation text
- provision_number: cited section or U.S.C. provision number if any
- is_internal_reference: Yes if the citation points to the current document, otherwise No
- citation_relation: choose one from [reference, definition, exception, amendment, authorization, compliance, other]
- citation_purpose: main purpose of the citation if explicit

# Relations

## CONTAINS
ProvisionClause - CONTAINS - ProvisionTextParagraph
ProvisionTextParagraph - CONTAINS - ProvisionTextParagraph

## CITES
ProvisionTextParagraph - CITES - Citation

# Parser Compatibility Note
Relations must be emitted as extraction class "{RELATION_CLASS}" with
attributes "{SUBJECT_KEY}", "{PREDICATE_KEY}" and "{OBJECT_KEY}".
"""
