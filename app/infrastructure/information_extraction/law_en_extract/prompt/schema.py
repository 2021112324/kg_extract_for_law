_RELATION_CLASS = "\u5173\u7cfb"
_SUBJECT_KEY = "\u4e3b\u4f53"
_PREDICATE_KEY = "\u8c13\u8bcd"
_OBJECT_KEY = "\u5ba2\u4f53"

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
