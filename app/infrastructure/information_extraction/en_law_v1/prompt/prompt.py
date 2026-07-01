"""格式一英文法规 LLM 抽取提示词。

重要约定：
1. `prompt_for_file_info` 和 `prompt_for_article` 是实际传给 LLM 的字符串。
2. 中文翻译注解必须写成 Python `#` 注释，不能写进三引号字符串。
3. 合规风险类型由一阶段规则切分获得，不出现在 LLM prompt/schema 中。
"""

# 从 schema 中引入关系兼容字段，确保 prompt 与 schema 使用同一套键名。
from app.infrastructure.information_extraction.en_law_v1.prompt.schema import (
    OBJECT_KEY,
    PREDICATE_KEY,
    RELATION_CLASS,
    SUBJECT_KEY,
)


# =============================================================================
# prompt_for_file_info 中文翻译注解，仅供开发者阅读，不会进入 LLM 输入
# =============================================================================
# # Role
# # 角色
# You are an expert in EU Regulation and Directive analysis.
# 你是欧盟 Regulation 和 Directive 分析专家。
#
# # Task
# # 任务
# Extract file-level knowledge graph information from the provided bounded file-info context.
# 从受控的文件级信息上下文中抽取文件级知识图谱信息。
# The input contains deterministic metadata from a rule-based splitter and selected recitals only.
# 输入只包含规则切分器产生的确定性元数据和精选 recitals。
# do not ask for the full document.
# 不要要求完整法规文件。
#
# # Mandatory Rules
# # 强制规则
# 1. Use English entity types and English property names.
# 1. 使用英文实体类型和英文属性名。
# 2. Extract exactly one LegalDocument entity.
# 2. 只能抽取一个 LegalDocument 实体。
# 3. Extract LegalBasis entities only from explicit legal basis text.
# 3. 只从明确写出的法律依据文本中抽取 LegalBasis 实体。
# 4. Extract applicable_industry, compliance_domains and economic_industries only from explicit or clearly scoped text.
# 4. 只基于明确文本或清晰适用范围抽取 applicable_industry、compliance_domains 和 economic_industries。
# 5. Ignore footnote/source markers such as $^{1}$ or ( $^{8}$ ); do not create Footnote entities.
# 5. 忽略脚注或来源标记，例如 $^{1}$ 或 ( $^{8}$ )，不要创建 Footnote 实体。
# 6. Do not infer unstated dates, authorities, purposes, legal bases, scope, industries or compliance domains.
# 6. 不要推断文本未明示的日期、机构、目的、法律依据、范围、行业或合规域。
#
# 抽取边界注解：
# - 这里不要求 LLM 抽取“合规风险类型”，因为该字段来自一阶段目录/文件路径规则切分。
# - file-info 输入只给文件标题、结构化元数据、Having regard to、少量精选 Whereas/recitals 等信息。
# - 精选 recitals 只作为文件目的、适用范围、主题事项的补充证据，不要求处理完整 Whereas。
# - applicable_industry 是高层行业适配枚举，用来服务制造业/电子信息业/通用/其他的分类。
# - compliance_domains 是合规主题数组，例如 product quality、data security、IP、environment 等。
# - economic_industries 是经济行业数组，偏向 GB/T 4754 类似粒度；一般适用时使用 General。
# - 脚注来源标记不建节点，避免把 EUR-Lex 来源、OJ 来源或纯上标误识别成法律实体。
# - 未明示字段宁可缺省或置空，也不要用法规标题、文件名或常识强行补全。
#
# # Parser Compatibility Rule
# # 解析器兼容规则
# For relation records only, use extraction class exactly "{RELATION_CLASS}" and attribute keys exactly
# "{SUBJECT_KEY}", "{PREDICATE_KEY}" and "{OBJECT_KEY}".
# 只有关系记录需要使用固定抽取类名 "{RELATION_CLASS}"，并使用固定属性键
# "{SUBJECT_KEY}"、"{PREDICATE_KEY}"、"{OBJECT_KEY}"。
# Their values must still be English.
# 这些字段的值仍必须是英文。
prompt_for_file_info = f"""
# Role
You are an expert in EU Regulation and Directive analysis.

# Task
Extract file-level knowledge graph information from the provided bounded
file-info context. The input contains deterministic metadata from a rule-based
splitter and selected recitals only; do not ask for the full document.

# Mandatory Rules
1. Use English entity types and English property names.
2. Extract exactly one LegalDocument entity.
3. Extract LegalBasis entities only from explicit legal basis text.
4. Extract applicable_industry, compliance_domains and economic_industries only
   from explicit or clearly scoped text.
5. Ignore footnote/source markers such as $^{{1}}$ or ( $^{{8}}$ ); do not
   create Footnote entities.
6. Do not infer unstated dates, authorities, purposes, legal bases, scope,
   industries or compliance domains.

# Parser Compatibility Rule
For relation records only, use extraction class exactly "{RELATION_CLASS}" and
attribute keys exactly "{SUBJECT_KEY}", "{PREDICATE_KEY}" and "{OBJECT_KEY}".
Their values must still be English.
"""


# =============================================================================
# prompt_for_article 中文翻译注解，仅供开发者阅读，不会进入 LLM 输入
# =============================================================================
# # Role
# # 角色
# You are an expert in EU Regulation and Directive Article analysis.
# 你是欧盟 Regulation 和 Directive 的 Article 条文分析专家。
#
# # Task
# # 任务
# Extract Article-level legal provision knowledge graph information from one Article.
# 从一个 Article 中抽取条文级法律规定知识图谱信息。
# The input is already split by deterministic rules and does not include Annex content or the full Whereas section.
# 输入已经由确定性规则切分，不包含 Annex 内容，也不包含完整 Whereas 区块。
#
# # Mandatory Rules
# # 强制规则
# 1. The input describes exactly one Article-level LegalProvision.
# 1. 输入只描述一个 Article 级 LegalProvision。
# 2. Preserve paragraph, point and subpoint structure such as 1., (a), and (i).
# 2. 保留 1.、(a)、(i) 等 paragraph、point、subpoint 结构。
# 3. Extract ProvisionUnit entities for complete legal units; keep parent lead-in context when a point or subpoint is incomplete by itself.
# 3. 对完整法律单元抽取 ProvisionUnit；当 point 或 subpoint 自身不完整时，保留上级引导语上下文。
# 4. Extract Citation entities only for citations explicitly present in the text.
# 4. 只抽取文本中明确出现的 Citation 实体。
# 5. Extract applicable_industry, compliance_domains and economic_industries from explicit scope or regulated subject text.
# 5. 从明确的适用范围或规制对象文本中抽取 applicable_industry、compliance_domains 和 economic_industries。
# 6. Extract quantitative feature and quantitative condition according to the linked rules in the schema.
# 6. 按 schema 中的联动规则抽取量化特征和量化条件。
# 7. Ignore footnote/source markers such as $^{1}$ or ( $^{8}$ ).
# 7. 忽略脚注或来源标记，例如 $^{1}$ 或 ( $^{8}$ )。
# 8. Preserve formulas, units, thresholds and chemical expressions as text; do not try to mathematically solve formulas.
# 8. 将公式、单位、阈值和化学表达式作为文本保留，不要尝试进行数学求解。
# 9. Do not invent legal consequences, dates, subjects, citations, industries, quantitative conditions or amendment targets.
# 9. 不要编造法律后果、日期、主体、引用、行业、量化条件或修订对象。
#
# 抽取边界注解：
# - Article 输入已经由切分器定位到单个 Article，不应跨 Article 合并抽取。
# - Annex 和完整 Whereas 不在此阶段处理，避免附件表格、长清单和说明性前言污染正文法条图谱。
# - ProvisionUnit 应尽量对应可引用、可落库、可追溯的 paragraph/point/subpoint。
# - 当 point 或 subpoint 依赖上级 lead-in 才能表达完整义务时，要在 unit_content 中保留必要上文。
# - Citation 只抽文本中明示的法律引用，不把普通名词、机构名或政策背景误当引用。
# - quantitative_feature 与 quantitative_indicator 必须联动：无可核验数字条件时为 Qualitative + null。
# - 数字、期限、比例、金额、倍数、公式和单位属于重要合规约束，应保留原文短语。
# - 修订型 Article 只在原文明确 amend/replace/insert/delete/repeal 时填写 amendment_target/action。
#
# # Parser Compatibility Rule
# # 解析器兼容规则
# For relation records only, use extraction class exactly "{RELATION_CLASS}" and attribute keys exactly
# "{SUBJECT_KEY}", "{PREDICATE_KEY}" and "{OBJECT_KEY}".
# 只有关系记录需要使用固定抽取类名 "{RELATION_CLASS}"，并使用固定属性键
# "{SUBJECT_KEY}"、"{PREDICATE_KEY}"、"{OBJECT_KEY}"。
# Their values must still be English.
# 这些字段的值仍必须是英文。
prompt_for_article = f"""
# Role
You are an expert in EU Regulation and Directive Article analysis.

# Task
Extract Article-level legal provision knowledge graph information from one
Article. The input is already split by deterministic rules and does not include
Annex content or the full Whereas section.

# Mandatory Rules
1. The input describes exactly one Article-level LegalProvision.
2. Preserve paragraph, point and subpoint structure such as 1., (a), and (i).
3. Extract ProvisionUnit entities for complete legal units; keep parent lead-in
   context when a point or subpoint is incomplete by itself.
4. Extract Citation entities only for citations explicitly present in the text.
5. Extract applicable_industry, compliance_domains and economic_industries from
   explicit scope or regulated subject text.
6. Extract quantitative_feature and quantitative_indicator according to the
   linked rules in the schema.
7. Ignore footnote/source markers such as $^{{1}}$ or ( $^{{8}}$ ).
8. Preserve formulas, units, thresholds and chemical expressions as text; do not
   try to mathematically solve formulas.
9. Do not invent legal consequences, dates, subjects, citations, industries,
   quantitative indicators or amendment targets.
10. For ProvisionUnit.unit_number, use the current input Article number as the
    only Article prefix and append the local legal unit marker. The
    ProvisionUnit entity name must be exactly the same as unit_number. If the
    input Article is Article 20, valid unit names are Article 20(1),
    Article 20(1)(a), Article 20(2), etc. Do not use Article numbers that
    appear only in citations, cross-references, amended provisions, referenced
    procedures or examples.
11. ProvisionUnit.unit_content must be copied exactly from the current input
    Article text. Do not paraphrase, summarize, translate, complete, or copy it
    from examples, citations, other Articles, Annexes or Whereas. If parent
    lead-in context is needed, copy that lead-in exactly from the current input
    Article.
12. Never copy names, text, entities, numbers or attributes from the Examples
    section into the answer. Extract only facts that are explicitly present in
    the current input Article.

# Parser Compatibility Rule
For relation records only, use extraction class exactly "{RELATION_CLASS}" and
attribute keys exactly "{SUBJECT_KEY}", "{PREDICATE_KEY}" and "{OBJECT_KEY}".
Their values must still be English.
"""


# =============================================================================
# v1 prompt overrides
# =============================================================================
prompt_for_article = f"""
# Role
You are an expert in EU Regulation and Directive Article analysis.

# Task
Extract attributes for exactly one Article-level LegalProvision. This step is
only for the Article itself.

# Mandatory Rules
1. Extract exactly one LegalProvision entity.
2. Do not split the Article into ProvisionClause.
3. Do not extract ProvisionTextParagraph, ProvisionUnit or Citation.
4. Do not output CONTAINS or CITES relations.
5. Use only facts explicitly present in the current Article input.
6. Do not copy text, names or attributes from examples.
7. Do not infer unstated dates, industries, compliance domains or amendment
   targets.
"""

prompt_for_provision_clause = f"""
# Role
You are an expert in EU Regulation and Directive clause analysis.

# Task
Extract knowledge graph information from one code-split ProvisionClause only.
The parent Article and the current ProvisionClause are already provided by
deterministic code.

# Mandatory Rules
1. Extract exactly one ProvisionClause entity for the current clause.
2. Do not change ProvisionClause.unit_number or ProvisionClause.unit_content.
3. Extract ProvisionTextParagraph entities only from the current
   ProvisionClause.unit_content.
4. ProvisionTextParagraph.unit_content must be copied from the current clause.
   Do not paraphrase, summarize, translate or invent text.
5. ProvisionTextParagraph.unit_number must start with the parent
   ProvisionClause unit_number.
6. Preserve required lead-in context. Do not output isolated (1), (a), (i) or
   dash items when they cannot be understood without the lead-in.
7. Do not cross into another Article or another ProvisionClause.
8. Extract Citation entities only when explicitly cited by a
   ProvisionTextParagraph.
9. Never copy text, names or attributes from examples.
10. The ProvisionTextParagraph entity name must be exactly the same as
    ProvisionTextParagraph.unit_number.
11. When several continuous source-text fragments under the same parent
    ProvisionClause share the same structural locator, use deterministic
    fallback locators such as "Article 14(5) paragraph 1" and
    "Article 14(5) paragraph 2". Do not use "first subparagraph" or
    "second subparagraph" naming.
12. legal_function may only be mandatory, prohibition or optional. Use
    mandatory for must/shall/is required/is obliged; prohibition for shall not,
    must not, is prohibited, or no ... shall; optional for may/can/is entitled
    or is allowed. If unknown, leave it empty and do not output other.
13. quantitative_indicator must use lower-case enum values only:
    value_type = amount/ratio/time_limit/count/multiple/other and relation =
    range/lower_bound/upper_bound/equal/other. Do not treat legal references
    or vague phrases such as "in accordance with Article ...", "Part 5 of
    Annex VI", "Articles 85 to 89", "reasonable duration" or "the period" as
    quantitative indicators.

# Parser Compatibility Rule
For relation records only, use extraction class exactly "{RELATION_CLASS}" and
attribute keys exactly "{SUBJECT_KEY}", "{PREDICATE_KEY}" and "{OBJECT_KEY}".
Their values must still be English.
"""

