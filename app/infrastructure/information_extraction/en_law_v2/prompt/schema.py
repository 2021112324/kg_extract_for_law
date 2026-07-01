"""格式一英文法规 LLM 抽取 Schema。

重要约定：
1. `schema_for_file_info` 和 `schema_for_article` 是实际传给 LLM 的 ontology 字符串。
2. 中文翻译注解必须写成 Python `#` 注释，不能写进 schema 字符串。
3. `合规风险类型` 由一阶段规则切分获得，不出现在 LLM schema 中。
"""

# 关系记录在 Langextract 中以实体形式表达时使用的类名。
RELATION_CLASS = "\u5173\u7cfb"
# 关系主体字段名，保持与现有抽取框架兼容。
SUBJECT_KEY = "\u4e3b\u4f53"
# 关系谓词字段名，保持与现有抽取框架兼容。
PREDICATE_KEY = "\u8c13\u8bcd"
# 关系客体字段名，保持与现有抽取框架兼容。
OBJECT_KEY = "\u5ba2\u4f53"


# =============================================================================
# schema_for_file_info 中文翻译注解，仅供开发者阅读，不会进入 LLM 输入
# =============================================================================
# 总体说明：
# - 本 schema 只约束“文件级”知识图谱抽取，不处理 Article 内部条款单元。
# - 实际传给 LLM 的字段名全部使用英文；下面中文仅用于开发者理解字段含义。
# - “合规风险类型”不在这里出现，它由一阶段基于目录/路径的规则解析补充到法规文件节点。
#
# ## LegalDocument / 法规文件
# LegalDocument：当前输入所描述的欧盟法规文件，每个文件级输入必须且只能抽取一个。
# document_name：文件完整正式标题，优先使用原文标题，不使用文件名臆造。
# document_type：法规类型，例如 Regulation、Directive、Implementing Regulation、Delegated Regulation、Decision。
# document_number：官方文件编号，例如 (EU) 2023/956；没有编号时不从文件名强行拼接。
# short_title：原文明确给出的简称、通称或缩写，例如某些法规的常用简称。
# issuing_authority：发布机关、通过机关或制定机关，例如 European Parliament and Council、Commission。
# publication_date：发布日期或通过日期，只取文本明确写出的日期。
# effective_date：生效日期、适用日期或 entry into force 日期，只取文本明确写出的日期。
# subject_matter：文件规定的主题事项，用于概括“这部法规管什么”。
# purpose：制定目的、政策目标或监管目标，只能来自明示的目的性表述或精选 recitals。
# scope_of_application：适用范围，包括地域范围、主体范围、对象范围、时间范围或产品/活动范围。
# legal_basis_text：来自 Having regard to 等法律依据部分的条约依据、授权依据或上位法依据原文。
# applicable_industry：适用行业枚举，只能是 Manufacturing、Electronic Information Industry、General、Other。
# applicable_industry 注解：若同时适用于制造业和电子信息业或属于普遍适用规则，选 General。
# applicable_industry 注解：若与制造业和电子信息业均无明显关联，选 Other。
# compliance_domains：合规域数组，记录法规涉及的合规管理主题，如 product quality、data security、IP、environment。
# compliance_domains 注解：只抽显式或可由明确监管对象直接归类的主题，不因文件目录名自动生成。
# economic_industries：经济行业数组，尽量接近 GB/T 4754 这类国民经济行业分类粒度。
# economic_industries 注解：一般适用且非特定行业法规时输出 ["General"]，且 General 不与具体行业并列。
# economic_industries 注解：该字段不能为空或缺失，避免 Neo4j 节点缺少统一行业检索字段。
#
# ## LegalBasis / 法律依据
# LegalBasis：被当前法规文件明确作为制定依据、授权依据或修订依据引用的法律文件、条约或条款。
# official_title：法律依据的完整标题或原文中能识别出的最佳标题。
# citation_text：法律依据在原文中的引用文本，应尽量保留原始引用形式。
# basis_role：依据角色，例如 authority、treaty_basis、delegated_power、amendment_basis。
#
# ## BASED_ON / 基于
# BASED_ON：表示 LegalDocument 基于某个 LegalBasis 制定、授权或修订。
# 关系注解：关系记录为了兼容现有解析器，仍使用固定中文键名“关系/主体/谓词/客体”承载。
# 关系注解：谓词值必须是英文关系名，例如 BASED_ON。
schema_for_file_info = f"""
# Entities

## LegalDocument
### Description
The EU legal document described by the file-info input. Extract exactly one
LegalDocument entity.
### Properties
- document_name: full official title of the document
- document_type: Regulation, Directive, Implementing Regulation, Delegated Regulation, Decision or other explicit type
- document_number: official document number, such as (EU) 2023/956
- short_title: short title or common abbreviation if explicitly stated
- issuing_authority: issuing or adopting authority
- publication_date: publication or adoption date if explicitly stated
- effective_date: entry-into-force or application date if explicitly stated
- subject_matter: stated subject matter
- purpose: stated purpose or objective, based on explicit text only
- scope_of_application: territory, regulated object, time or subject scope if stated
- legal_basis_text: Treaty or authorization basis text from Having regard to lines
- applicable_industry: choose one from [Manufacturing, Electronic Information Industry, General, Other]. Use Manufacturing if the document clearly applies to manufacturing but not electronic information; use Electronic Information Industry if it clearly applies to electronic information but not manufacturing; use General if it applies to both or is generally applicable; use Other if it is unrelated to both manufacturing and electronic information.
- compliance_domains: string array. Compliance management domains, risk domains or regulatory topics explicitly reflected by the document, such as product quality, data security, intellectual property, environmental protection, labor employment or supply chain management. Use an empty array if no compliance domain is explicit.
- economic_industries: string array. Applicable national economic industry categories, preferably at a consistent level comparable to GB/T 4754 categories, such as Manufacturing, Construction, or Information Transmission, Software and Information Technology Services. If the document targets one or more specific industries, output the specific categories. If it is generally applicable and not targeted at any specific industry, output ["General"]. Do not combine "General" with specific industries. Do not leave this property blank or missing.

## LegalBasis
### Description
A treaty article, regulation, directive, decision, agreement or legal instrument
explicitly cited as a legal basis or authority for the current LegalDocument.
### Properties
- official_title: full or best explicit title
- citation_text: citation text as written
- basis_role: authority, treaty_basis, delegated_power, amendment_basis or other explicit role

# Relations

## BASED_ON
### Triple
LegalDocument - BASED_ON - LegalBasis

# Parser Compatibility Note
Relations must be emitted as extraction class "{RELATION_CLASS}" with
attributes "{SUBJECT_KEY}", "{PREDICATE_KEY}" and "{OBJECT_KEY}". Predicate
values must be English relation names, such as "BASED_ON".
"""


# =============================================================================
# schema_for_article 中文翻译注解，仅供开发者阅读，不会进入 LLM 输入
# =============================================================================
# 总体说明：
# - 本 schema 只约束“单个 Article”内部的条文级抽取。
# - Annex、大段 Whereas、文件级元数据不在这里抽取，避免正文法条节点过大或主题漂移。
# - 字段名保持英文，中文说明仅作为源码注释，不进入 LLM 输入。
#
# ## LegalProvision / 法律条文
# LegalProvision：当前输入的 Article 级法律条文，每个 Article 输入必须且只能抽取一个。
# provision_number：Article 编号，例如 Article 7；不包含 Chapter/Section 标题。
# provision_heading：Article 标题或条名，只有原文明确提供时填写。
# core_topic：该 Article 的核心监管主题，用简短英文概括。
# classification_context：由 splitter 正则确定，不由 LLM 抽取。
# scope_of_effect：该 Article 明示的效力范围、适用范围或规制范围。
# applicable_industry：适用行业枚举，规则同文件级字段。
# compliance_domains：合规域数组，记录该 Article 明确涉及的合规主题。
# economic_industries：经济行业数组，不能为空，通用规则同文件级字段。
# is_amendment_article：是否为修订型条文；只有明确 amend/replace/insert/delete/repeal 等才为 true。
# amendment_target：被修订的文件或条款，必须来自原文明示目标。
# amendment_action：修订动作，例如 amend、replace、insert、delete、repeal；未明示则为空。
#
# ## ProvisionUnit / 条文单元
# ProvisionUnit：Article 内部可被引用、可独立落库或可表达完整规则的 paragraph、point、subpoint、dash item。
# unit_number：稳定结构编号，必须锚定当前输入 Article 的编号；正文中引用的其他 Article 不能作为本单元编号。
# unit_level：结构层级，例如 paragraph、point、subpoint、dash_item。
# unit_content：条文单元完整文本；若下级项依赖上级引导语，应补入必要上下文。
# unit_purpose：该单元的明确目的或主题，不明显时可留空。
# applicable_industry：该单元适用行业枚举，优先依据本单元及必要上文判断。
# compliance_domains：该单元涉及的合规域数组，要求来自明确监管对象、义务或禁止事项。
# economic_industries：该单元适用经济行业数组，不能为空，一般适用时为 ["General"]。
# function_type：条文功能类型，只能选 prohibition、mandatory、optional；无法抽取时留空，不输出 other。
# function_type 注解：shall not/must not/banned/excluded 属于 prohibition。
# function_type 注解：shall/must/required/obligation/procedure 属于 mandatory。
# function_type 注解：may/permission/right/authorization 属于 optional。
# function_type 注解：other 只允许后处理兜底生成，不允许 LLM 主动输出。
# quantitative_feature：量化特征，只能是 Qualitative 或 Quantitative，不输出 Mixed。
# quantitative_feature 注解：有金额、比例、期限、次数、倍数、范围、阈值等可核验数字条件时为 Quantitative。
# quantitative_feature 注解：只有行为、原则、义务、权利、禁止事项而无具体数字条件时为 Qualitative。
# quantitative_indicator：量化指标对象或 null，不是风险分值，也不是模型自评得分。
# quantitative_indicator.raw_text：量化条件的原文短语或句子，应保留原始表达。
# quantitative_indicator.value_type：量化值类型，如 Amount、Ratio、Time Limit、Frequency、Multiplier、Other。
# quantitative_indicator.min：下限数值，没有下限时为 null。
# quantitative_indicator.max：上限数值，没有上限时为 null。
# quantitative_indicator.unit：原文明示单位，例如 EUR、day、month、year、%、times、multiple。
# quantitative_indicator.relation：约束关系，如 Range、Lower Bound、Upper Bound、Equal、Other。
# quantitative_indicator 联动规则：若该对象非空，quantitative_feature 必须为 Quantitative。
# quantitative_indicator 联动规则：若没有可核验量化条件，该对象必须为 null，quantitative_feature 必须为 Qualitative。
# applicable_subject：被该单元直接规制的主体，如 operator、manufacturer、provider、competent authority。
# responsibility_role：主体在规则中的责任角色，如 responsible operator、supervisory body。
# conduct_description：被要求、允许或禁止的行为描述，是条文义务抽取的核心字段。
# condition：规则适用条件，例如 when、where、if、unless 引导的条件。
# legal_consequence：法律后果、处罚、效力或程序后果，只有原文明示时填写。
# exception：例外、豁免、排除适用或 carve-out。
# time_element：日期、期限、期间、过渡期、频率等时间语义短语。
# other_information：无法放入上述字段但原文明示的重要信息，例如原则性要求、政策背景或兜底条款。
#
# ## Citation / 引用
# Citation：ProvisionUnit 明确引用的法规文件、条约、Article、Directive、Regulation 或其他法律工具。
# citation_type：Document 或 Provision；引用整部法律工具时为 Document，引用具体条款时为 Provision。
# official_title：被引用文件或条款的完整标题或可识别标题。
# alias：被引用文件的简称、缩写或通称，只有原文明示时填写。
# document_type：被引用对象类型，如 Regulation、Directive、Treaty、Article、Decision。
# citation_text：原文中的准确引用文本。
# provision_number：被引用的 Article 或 provision 编号，只放编号，不放章节标题。
# is_internal_reference：是否指向当前文件内部引用，Yes 表示内部引用，No 表示外部引用。
# citation_relation：引用关系，如 pursuant to、under、subject to、defined in、reference、supplement。
# citation_purpose：引用目的，如 supplement、interpretation、exclusion、reference，只有明确时填写。
#
# ## Relations / 关系
# CONTAINS：LegalProvision 包含 ProvisionUnit。
# CITES：ProvisionUnit 引用 Citation。
# 关系注解：关系记录为兼容现有解析器，仍使用固定中文键名“关系/主体/谓词/客体”承载。
# 关系注解：谓词值必须是英文关系名，例如 CONTAINS 或 CITES。
schema_for_article = f"""
# Entities

## LegalProvision
### Description
The Article-level legal provision being processed. Extract exactly one
LegalProvision entity for the input Article.
### Properties
- provision_number: Article number, such as Article 7
- provision_heading: Article heading
- core_topic: central topic of the Article
- scope_of_effect: stated application scope of the Article
- applicable_industry: choose one from [Manufacturing, Electronic Information Industry, General, Other]. Use Manufacturing if the Article clearly applies to manufacturing but not electronic information; use Electronic Information Industry if it clearly applies to electronic information but not manufacturing; use General if it applies to both or is generally applicable; use Other if it is unrelated to both manufacturing and electronic information.
- compliance_domains: string array. Compliance management domains, risk domains or regulatory topics explicitly reflected by the Article, such as product quality, data security, intellectual property, environmental protection, labor employment or supply chain management. Use an empty array if no compliance domain is explicit.
- economic_industries: string array. Applicable national economic industry categories, preferably at a consistent level comparable to GB/T 4754 categories, such as Manufacturing, Construction, or Information Transmission, Software and Information Technology Services. If the Article targets one or more specific industries, output the specific categories. If it is generally applicable and not targeted at any specific industry, output ["General"]. Do not combine "General" with specific industries. Do not leave this property blank or missing.
- is_amendment_article: true or false
- amendment_target: amended document/provision if explicitly stated
- amendment_action: amend, replace, insert, delete, repeal or empty if not stated

## ProvisionUnit
### Description
A stable paragraph, point, subpoint or item inside the Article that can be cited
or understood as a legal rule unit. Preserve parent context when needed.
### Properties
- unit_number: stable structural number of this unit. It must be the current input Article number plus the local unit marker. If the input Article is Article 20, units inside it must be numbered as Article 20(1), Article 20(1)(a), Article 20(2), etc. The ProvisionUnit entity name must be exactly the same value as unit_number. Do not use Article numbers that only appear in citations, cross-references, amended text, examples, or referenced procedures. If the local structure is clear but the full number is not written in the source, combine the current Article number with the local paragraph/point marker.
- unit_level: paragraph, point, subpoint, dash_item or other
- unit_content: exact source text of the legal unit copied from the current input Article only. Do not paraphrase, summarize, translate, normalize, complete from memory, or copy text from examples, citations, other Articles, Annexes or Whereas. If inherited lead-in is needed, copy the required lead-in text exactly as it appears in the current input Article.
- unit_purpose: concise topic or purpose of this unit if explicit
- applicable_industry: choose one from [Manufacturing, Electronic Information Industry, General, Other]. Use Manufacturing if the unit clearly applies to manufacturing but not electronic information; use Electronic Information Industry if it clearly applies to electronic information but not manufacturing; use General if it applies to both or is generally applicable; use Other if it is unrelated to both manufacturing and electronic information.
- compliance_domains: string array. Compliance management domains, risk domains or regulatory topics explicitly reflected by the unit, such as product quality, data security, intellectual property, environmental protection, labor employment or supply chain management. Use an empty array if no compliance domain is explicit.
- economic_industries: string array. Applicable national economic industry categories, preferably at a consistent level comparable to GB/T 4754 categories. If the unit targets one or more specific industries, output the specific categories. If it is generally applicable and not targeted at any specific industry, output ["General"]. Do not combine "General" with specific industries. Do not leave this property blank or missing.
- function_type: choose one from [prohibition, mandatory, optional]. Prohibition covers "shall not", "must not", banned or excluded conduct. Mandatory covers "shall", "must", required obligations, definitions that set binding legal meaning, compulsory procedures or required compliance conditions. Optional covers "may", rights, authorizations or permissions. Do not output other. If the function type cannot be extracted, leave this property empty.
- quantitative_feature: choose one from [Qualitative, Quantitative]. Qualitative means the unit regulates conduct patterns, rights, obligations, principles or requirements without concrete numbers. Quantitative means it contains concrete numbers, amounts, deadlines, ratios, ranges, frequencies, multipliers or other measurable elements. Do not output Mixed.
- quantitative_indicator: structured object or null. Use this only for computable or verifiable quantitative constraints in the original text, such as amounts, ratios, deadlines, frequencies, multipliers or numerical ranges. It is not a risk score. If no quantifiable condition exists, this must be null and quantitative_feature must be Qualitative. If a quantifiable condition exists, preserve the original phrase and structure it exactly as {{"raw_text":"original phrase or sentence","value_type":"Amount/Ratio/Time Limit/Frequency/Multiplier/Other","min":number or null,"max":number or null,"unit":"EUR/day/month/year/%/times/multiple or other explicit unit","relation":"Range/Lower Bound/Upper Bound/Equal/Other"}}. Do not output quantitative_condition, quantitative_value_type, minimum_value, maximum_value or constraint_relation. If quantitative_indicator is a non-empty object, quantitative_feature must be Quantitative.
- applicable_subject: subject directly governed by the unit
- responsibility_role: role of the subject in the unit, such as responsible operator, competent authority, supervisory body or other explicit role
- conduct_description: required, permitted or prohibited conduct
- condition: condition under which the unit applies
- legal_consequence: consequence if stated
- exception: exception or carve-out if stated
- time_element: complete semantic phrase describing dates, deadlines, periods or timing requirements if stated
- other_information: other explicit information that cannot be placed in the fields above, such as policy background, principle-based guidance or residual clauses

## Citation
### Description
A legal document, treaty, Article, Regulation, Directive or instrument explicitly
cited by a ProvisionUnit. Do not invent citations.
### Properties
- citation_type: choose one from [Document, Provision]. Use Document when only a whole instrument is cited. Use Provision when a concrete Article or provision number is cited.
- official_title: full or best explicit title of the cited instrument if stated
- alias: common short title or abbreviation of the cited instrument if stated
- document_type: Regulation, Directive, Treaty, Article, Decision or other
- citation_text: exact citation text
- provision_number: cited Article or provision number if any. It must contain only the Article/provision number, not title/chapter/section names.
- is_internal_reference: Yes if the citation points to the current document, otherwise No
- citation_relation: pursuant to, under, subject to, defined in, reference, supplement, interpretation, exclusion or other explicit relation
- citation_purpose: main purpose of the citation if explicit, such as supplement, interpretation, exclusion or reference

# Relations

## CONTAINS
### Triple
LegalProvision - CONTAINS - ProvisionUnit

## CITES
### Triple
ProvisionUnit - CITES - Citation

# Parser Compatibility Note
Relations must be emitted as extraction class "{RELATION_CLASS}" with
attributes "{SUBJECT_KEY}", "{PREDICATE_KEY}" and "{OBJECT_KEY}". Predicate
values must be English relation names, such as "CONTAINS" or "CITES".
"""


# =============================================================================
# v2 schema overrides
# =============================================================================
# v2 中 Article/LegalProvision 抽取与 ProvisionClause 抽取必须分离：
# - Article schema 只允许产出 LegalProvision。
# - ProvisionClause schema 才允许产出 ProvisionClause、ProvisionTextParagraph、Citation 和关系。
schema_for_article = f"""
# Entities

## LegalProvision
### Description
The Article-level legal provision being processed. Extract exactly one
LegalProvision entity for the input Article. Do not extract ProvisionClause,
ProvisionTextParagraph, ProvisionUnit or Citation in this Article step.
### Properties
- provision_number: Article number, such as Article 7
- provision_heading: Article heading
- core_topic: central topic of the Article
- scope_of_effect: stated application scope of the Article
- applicable_industry: choose one from [Manufacturing, Electronic Information Industry, General, Other]
- compliance_domains: string array of explicit compliance domains
- economic_industries: string array. Use ["General"] when generally applicable
- is_amendment_article: true or false
- amendment_target: amended document/provision if explicitly stated
- amendment_action: amend, replace, insert, delete, repeal or empty if not stated

# Forbidden Output
Do not output ProvisionClause, ProvisionTextParagraph, ProvisionUnit or Citation
entities. Do not output CONTAINS or CITES relations in this Article step.
"""

schema_for_provision_clause = f"""
# Entities

## ProvisionClause
### Description
The current code-split ProvisionClause under one Article. Extract exactly one
ProvisionClause entity for the provided clause. The fields unit_number and
unit_content are locked by code and must not be changed.
### Properties
- unit_level: normally paragraph
- clause_summary: concise summary of this clause
- clause_purpose: explicit purpose if stated
- main_subject: main regulated subject if stated
- main_action: main required, prohibited or permitted action if stated
- main_object: object of the main action if stated
- legal_function: choose one from [prohibition, mandatory, optional], or leave empty if unknown. mandatory means must/shall/is required/is obliged; prohibition means shall not/must not/is prohibited/no ... shall; optional means may/can/is entitled/is allowed. Do not output other, definition, scope, procedure, exception or exclusion.
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
- unit_level: point, subpoint, dash_item, paragraph_fragment or other
- unit_purpose: explicit purpose or topic if stated
- quantitative_feature: choose one from [Qualitative, Quantitative]
- quantitative_indicator: structured object or null for concrete and verifiable numbers, amounts, ratios, time limits, counts, multiples or thresholds. Use exactly {{"raw_text":"original phrase or sentence","value_type":"amount/ratio/time_limit/count/multiple/other","min":number or null,"max":number or null,"unit":"explicit unit or empty","relation":"range/lower_bound/upper_bound/equal/other"}}. Do not output Reference, Formula, Time Limit, Deadline, Amount, Ratio, Upper Bound or other open enum values. References such as "in accordance with Article ...", "Part 5 of Annex VI", "Articles 85 to 89", and vague phrases such as "reasonable duration" or "the period" are not quantitative indicators; output null and set quantitative_feature to Qualitative.
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
A legal document, treaty, Article, Regulation, Directive or instrument
explicitly cited by a ProvisionTextParagraph. Do not invent citations.
### Properties
- citation_type: Document or Provision
- official_title: full or best explicit title of the cited instrument if stated
- alias: common short title or abbreviation if stated
- document_type: Regulation, Directive, Treaty, Article, Decision or other
- citation_text: exact citation text
- provision_number: cited Article or provision number if any
- is_internal_reference: Yes if the citation points to the current document, otherwise No
- citation_relation: pursuant to, under, subject to, defined in, reference, supplement, interpretation, exclusion or other explicit relation
- citation_purpose: main purpose of the citation if explicit

# Relations

## CONTAINS
### Triple
ProvisionClause - CONTAINS - ProvisionTextParagraph
ProvisionTextParagraph - CONTAINS - ProvisionTextParagraph

## CITES
### Triple
ProvisionTextParagraph - CITES - Citation

# Parser Compatibility Note
Relations must be emitted as extraction class "{RELATION_CLASS}" with
attributes "{SUBJECT_KEY}", "{PREDICATE_KEY}" and "{OBJECT_KEY}". Predicate
values must be English relation names, such as "CONTAINS" or "CITES".
"""

