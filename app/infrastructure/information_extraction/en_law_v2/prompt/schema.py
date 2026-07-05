"""格式二美国法案 LLM 抽取 Schema。"""

RELATION_CLASS = "关系"
SUBJECT_KEY = "主体"
PREDICATE_KEY = "谓词"
OBJECT_KEY = "客体"


schema_for_file_info = f"""
# Entities

## LegalDocument
### Description
The United States act or statutory compilation described by the file-info input.
Extract exactly one LegalDocument entity.
### Properties
- document_name: full official or best explicit act title
- document_type: Act, Public Law, Code compilation or other explicit type
- document_number: Public Law number, chapter number or other explicit official number
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


schema_for_article = """
# Entities

## LegalProvision
### Description
The current code-split SECTION/SEC/Sec legal provision. Extract exactly one
LegalProvision entity. Do not extract ProvisionClause, ProvisionTextParagraph,
ProvisionUnit or Citation in this step.
### Properties
- provision_number: SECTION/SEC/Sec number, such as SEC. 2 or SECTION 10001
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


schema_for_provision_clause = f"""
# Entities

## ProvisionClause
### Description
The current code-split subsection under one SECTION/SEC/Sec provision. Extract
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
