"""格式三美国法案 LLM 抽取提示词。"""

from app.infrastructure.information_extraction.en_law_v3.prompt.schema import (
    OBJECT_KEY,
    PREDICATE_KEY,
    RELATION_CLASS,
    SUBJECT_KEY,
)


prompt_for_file_info = f"""
# Role
You are an expert in United States act and statutory compilation analysis.

# Task
Extract file-level knowledge graph information from the bounded file-info
context. The input is not the full document.

# Mandatory Rules
1. Use English entity types and English property names.
2. Extract exactly one LegalDocument entity.
3. Extract LegalBasis entities only from explicit authority, public law,
   United States Code or statutory basis text.
4. Do not infer unstated dates, authorities, purposes, industries or domains.
5. Ignore page headers, page numbers, footnote markers and source markers.

# Parser Compatibility Rule
For relation records only, use extraction class exactly "{RELATION_CLASS}" and
attribute keys exactly "{SUBJECT_KEY}", "{PREDICATE_KEY}" and "{OBJECT_KEY}".
"""


prompt_for_article = """
# Role
You are an expert in United States CFR/USC section analysis.

# Task
Extract attributes for exactly one `§` LegalProvision. This step is
only for the section itself.

# Mandatory Rules
1. Extract exactly one LegalProvision entity.
2. Do not split the section into ProvisionClause in this step.
3. Do not extract ProvisionTextParagraph, ProvisionUnit or Citation.
4. Do not output CONTAINS or CITES relations.
5. Use only facts explicitly present in the current section input.
6. Do not copy text, names or attributes from examples.
7. Do not infer unstated dates, industries, compliance domains or amendment
   targets.
"""


prompt_for_provision_clause = f"""
# Role
You are an expert in United States CFR/USC subsection analysis.

# Task
Extract knowledge graph information from one code-split ProvisionClause only.
The parent `§` provision and current ProvisionClause are already
provided by deterministic code.

# Mandatory Rules
1. Extract exactly one ProvisionClause entity for the current clause.
2. Do not change ProvisionClause.unit_number or ProvisionClause.unit_content.
3. Extract ProvisionTextParagraph entities only from the current
   ProvisionClause.unit_content.
4. ProvisionTextParagraph.unit_content must be copied from the current clause.
   Do not paraphrase, summarize, translate or invent text.
5. ProvisionTextParagraph.unit_number must start with the parent
   ProvisionClause unit_number.
6. Preserve required lead-in context. Do not output isolated (1), (A), (i) or
   dash items when they cannot be understood without the lead-in.
7. Do not cross into another `§` provision, Supplement, Editorial Note or another
   ProvisionClause.
8. Extract Citation entities only when explicitly cited by a
   ProvisionTextParagraph.
9. Never copy text, names or attributes from examples.
10. The ProvisionTextParagraph entity name must be exactly the same as
    ProvisionTextParagraph.unit_number.
11. legal_function may only be mandatory, prohibition or optional. If unknown,
    leave it empty and do not output other.
12. quantitative_indicator must use lower-case enum values only:
    value_type = amount/ratio/time_limit/count/multiple/other and relation =
    range/lower_bound/upper_bound/equal/other. Do not treat legal references
    or vague phrases as quantitative indicators.
13. Citation.citation_relation must use lower-case enum values only:
    reference/definition/exception/amendment/authorization/compliance/other.
    Do not output raw phrases such as pursuant to, under, defined in or
    except as provided in as citation_relation.

# Parser Compatibility Rule
For relation records only, use extraction class exactly "{RELATION_CLASS}" and
attribute keys exactly "{SUBJECT_KEY}", "{PREDICATE_KEY}" and "{OBJECT_KEY}".
"""


