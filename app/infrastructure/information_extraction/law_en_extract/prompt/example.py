"""
英文法律 LLM 抽取的少样本示例（Few-shot Examples）。

中文说明：
- 示例 text、实体类型、属性名和属性值均保持英文，不含中文内容。
- 中文翻译仅存在于 Python # 注释中，不会发送给模型。
- 关系格式兼容 Langextract 解析器：extraction class="关系"，
  键名"主体/谓词/客体"，但谓词值必须为英文（BASED_ON/CONTAINS/CITES）。

示例概览：
  example_for_file_info:
    1. IEEPA 法案（U.S.C. 格式）— 仅 Legal Document（codification_text 属性记录位置）
    2. 22 CFR Part 123（CFR 格式）— Legal Document + Legal Basis ×2 + BASED_ON ×2
  example_for_clause:
    1. § 1701（IEEPA 授权+限制条款）— 内部引用 CONTAINS/CITES
    2. § 123.1（义务+程序条款）— 外部引用 CITES
"""

from app.infrastructure.information_extraction.law_en_extract.prompt.schema import (
    OBJECT_KEY,       # 客体 — target
    PREDICATE_KEY,    # 谓词 — relation type
    RELATION_CLASS,   # 关系 — extraction class
    SUBJECT_KEY,      # 主体 — source
)


# =============================================================================
# example_for_file_info 示例1: IEEPA 法案（U.S.C. 格式）
# =============================================================================
# 输入：Title 50 Chapter 35 § 1701 的文件头
# 抽取意图：
#   - Legal Document: IEEPA 作为一部 Act
#     codification_text 属性直接记录法典位置
# 注意：此处没有 Legal Basis，因为 input 中只有 codification 路径，没有 Authority 行。
#       codification_text 是 Legal Document 的属性，不是独立实体。

_FI_EXAMPLE_1 = {
    "text": """
Filename: International Emergency Economic Powers Act.txt
Document type: usc_or_statute
File information:
United States Code Annotated
Title 50. War and National Defense
Chapter 35. International Emergency Economic Powers
§ 1701. Unusual and extraordinary threat; declaration of national emergency; exercise of Presidential authorities
""",
    "extractions": [
        # Legal Document: 国际紧急经济权力法（IEEPA）
        #   codification_text 记录该法案在 U.S. Code 中的 Title/Chapter 路径
        #   不要将 codification_text 单独提取为 Codification Location 实体
        {
            "name": "International Emergency Economic Powers Act",
            "type": "Legal Document",
            "attributes": {
                "official_title": "International Emergency Economic Powers Act",
                "abbreviation": "IEEPA",
                "document_type": "Act",
                "codification_text": "United States Code Annotated Title 50. War and National Defense Chapter 35. International Emergency Economic Powers",
                "domain": "national emergency economic powers and sanctions",
            },
        },
    ],
}

# =============================================================================
# example_for_file_info 示例2: 22 CFR Part 123（CFR 格式）
# =============================================================================
# 输入：22 CFR Part 123 的 CFR 文件头，包含 Authority 行和 Source 行
# 抽取意图：
#   - Legal Document: 22 CFR Part 123（CFR Part 类型）
#     codification_text/codification_path 作为属性记录
#   - Legal Basis ×2: Arms Export Control Act + E.O. 13637
#     （Authority 行中明确列出的授权法/行政令）
#   - 关系 BASED_ON ×2: 本法规依据这两部法律
# 注意：codification 信息是 Legal Document 的属性，不要提取为独立实体。

_FI_EXAMPLE_2 = {
    "text": """
Filename: 22 CFR Part 123 (up to date as of 4-21-2026).txt
Document type: cfr_part
File information:
Title 22 —Foreign Relations
Chapter I —Department of State
Subchapter M —International Traffic in Arms Regulations
Part 123 Licenses for the Export and Temporary Import of Defense Articles
Authority: Secs. 2, 38, and 71, Pub. L. 90-629, 90 Stat. 744 (22 U.S.C. 2752, 2778, 2797); E.O. 13637, 78 FR 16129.
Source: 58 FR 39280, July 22, 1993, unless otherwise noted.
""",
    "extractions": [
        # Legal Document: 22 CFR Part 123
        #   document_type=CFR Part, legal_citation=22 CFR Part 123
        #   codification_text 作为属性记录法典位置
        #   authority_text 记录 Authority 行的完整文本
        {
            "name": "22 CFR Part 123",
            "type": "Legal Document",
            "attributes": {
                "official_title": "22 CFR Part 123 Licenses for the Export and Temporary Import of Defense Articles",
                "legal_citation": "22 CFR Part 123",
                "document_type": "CFR Part",
                "codification_text": "Title 22 —Foreign Relations; Chapter I —Department of State; Subchapter M —International Traffic in Arms Regulations; Part 123",
                "authority_text": "Secs. 2, 38, and 71, Pub. L. 90-629, 90 Stat. 744 (22 U.S.C. 2752, 2778, 2797); E.O. 13637, 78 FR 16129.",
                "domain": "defense trade controls",
            },
        },
        # Legal Basis 1: 武器出口管制法（Arms Export Control Act）
        #   basis_role=authority（授权角色）
        #   通过 Authority 行识别：Secs. 2, 38, and 71, Pub. L. 90-629
        {
            "name": "Arms Export Control Act Sections 2, 38, and 71",
            "type": "Legal Basis",
            "attributes": {
                "official_title": "Arms Export Control Act",
                "legal_citation": "22 U.S.C. 2752, 2778, 2797",
                "basis_role": "authority",
            },
        },
        # Legal Basis 2: 第13637号行政令（Executive Order 13637）
        #   basis_role=authority
        #   通过 Authority 行识别：E.O. 13637, 78 FR 16129
        {
            "name": "Executive Order 13637",
            "type": "Legal Basis",
            "attributes": {
                "official_title": "Executive Order 13637",
                "legal_citation": "78 FR 16129",
                "basis_role": "authority",
            },
        },
        # 关系: Legal Document → BASED_ON → Arms Export Control Act
        {
            "name": "",
            "type": RELATION_CLASS,
            "attributes": {
                SUBJECT_KEY: "Legal Document_22 CFR Part 123",
                PREDICATE_KEY: "BASED_ON",
                OBJECT_KEY: "Legal Basis_Arms Export Control Act Sections 2, 38, and 71",
            },
        },
        # 关系: Legal Document → BASED_ON → E.O. 13637
        {
            "name": "",
            "type": RELATION_CLASS,
            "attributes": {
                SUBJECT_KEY: "Legal Document_22 CFR Part 123",
                PREDICATE_KEY: "BASED_ON",
                OBJECT_KEY: "Legal Basis_Executive Order 13637",
            },
        },
    ],
}

example_for_file_info = [_FI_EXAMPLE_1, _FI_EXAMPLE_2]


# =============================================================================
# example_for_clause 示例1: § 1701（IEEPA 法案）
# =============================================================================
# 输入：§ 1701 - 异常和特殊威胁；国家紧急状态声明；总统权力行使
#   层级：Title 50 War and National Defense / Chapter 35 IEEPA
#   内容：(a) 授权条款：总统可行使 § 1702 的权力应对境外异常威胁
#         (b) 限制条款：权力仅可为已宣布的国家紧急状态行使，不得他用
# 抽取意图：
#   - Legal Provision: § 1701
#   - Provision Unit (a): 授权条款（function_type=authorization, 引用 § 1702）
#   - Provision Unit (b): 限制条款（function_type=limitation）
#   - Citation: § 1702（内部引用，同一法案内）
#   - 关系: CONTAINS ×2, CITES ×1

_CC_EXAMPLE_1 = {
    "text": """
Filename: International Emergency Economic Powers Act.txt
Classification: {"title": "United States Code Annotated Title 50. War and National Defense", "chapter": "Chapter 35. International Emergency Economic Powers"}
Section number: § 1701
Section heading: Unusual and extraordinary threat; declaration of national emergency; exercise of Presidential authorities
Clause content:
Unusual and extraordinary threat; declaration of national emergency; exercise of Presidential authorities
(a) Any authority granted to the President by section 1702 of this title may be exercised to deal with any unusual and extraordinary threat, which has its source in whole or substantial part outside the United States, to the national security, foreign policy, or economy of the United States, if the President declares a national emergency with respect to such threat.
(b) The authorities granted to the President by section 1702 of this title may only be exercised to deal with an unusual and extraordinary threat with respect to which a national emergency has been declared for purposes of this chapter and may not be exercised for any other purpose.
""",
    "extractions": [
        # Legal Provision: IEEPA § 1701
        #   core_topic=异常威胁与国家紧急状态声明
        {
            "name": "International Emergency Economic Powers Act Section 1701",
            "type": "Legal Provision",
            "attributes": {
                "provision_number": "Section 1701",
                "provision_heading": "Unusual and extraordinary threat; declaration of national emergency; exercise of Presidential authorities",
                "core_topic": "unusual and extraordinary threat and declaration of national emergency",
                "scope_of_effect": "Presidential emergency economic authorities for threats sourced outside the United States",
                "classification_context": "Title 50, Chapter 35",
                "applicable_industry": "general",
            },
        },
        # Provision Unit (a): 授权条款
        #   function_type=authorization, applicable_subject=President
        #   conduct_description=可行使 § 1702 授予的权力应对异常威胁
        #   condition=威胁来源在境外 + 总统宣布国家紧急状态
        {
            "name": "Section 1701 subsection (a)",
            "type": "Provision Unit",
            "attributes": {
                "unit_content": "Any authority granted to the President by section 1702 of this title may be exercised to deal with any unusual and extraordinary threat, which has its source in whole or substantial part outside the United States, to the national security, foreign policy, or economy of the United States, if the President declares a national emergency with respect to such threat.",
                "unit_heading": "",
                "unit_level": "subsection",
                "unit_number": "Section 1701 subsection (a)",
                "hierarchy_path": "Section 1701 > subsection (a)",
                "applicable_industry": "general",
                "function_type": "authorization",
                "applicable_subject": "President",
                "responsible_role": "authorized decision-maker",
                "conduct_description": "may exercise authority granted by section 1702 to deal with an unusual and extraordinary threat",
                "condition": "the threat has its source outside the United States and the President declares a national emergency",
                "legal_consequence": "",
                "exception": "",
                "time_element": "",
                "quantitative_standard": "",
                "other_information": "",
            },
        },
        # Provision Unit (b): 限制条款
        #   function_type=limitation
        #   exception=不得为任何其他目的行使权力
        {
            "name": "Section 1701 subsection (b)",
            "type": "Provision Unit",
            "attributes": {
                "unit_content": "The authorities granted to the President by section 1702 of this title may only be exercised to deal with an unusual and extraordinary threat with respect to which a national emergency has been declared for purposes of this chapter and may not be exercised for any other purpose.",
                "unit_heading": "",
                "unit_level": "subsection",
                "unit_number": "Section 1701 subsection (b)",
                "hierarchy_path": "Section 1701 > subsection (b)",
                "applicable_industry": "general",
                "function_type": "limitation",
                "applicable_subject": "President",
                "responsible_role": "authorized decision-maker",
                "conduct_description": "may exercise the authorities only for the declared emergency threat",
                "condition": "a national emergency has been declared for purposes of this chapter",
                "legal_consequence": "",
                "exception": "may not be exercised for any other purpose",
                "time_element": "",
                "quantitative_standard": "",
                "other_information": "",
            },
        },
        # Citation: IEEPA § 1702（内部引用）
        #   citation_type=Provision（条款级引用）
        #   is_internal_reference=Yes（同一法案内的条款）
        {
            "name": "International Emergency Economic Powers Act Section 1702",
            "type": "Citation",
            "attributes": {
                "citation_type": "Provision",
                "official_title": "International Emergency Economic Powers Act",
                "abbreviation": "IEEPA",
                "document_type": "Act",
                "provision_number": "Section 1702",
                "is_internal_reference": "Yes",
                "citation_relation": "authority granted by",
                "citation_purpose": "identify Presidential authorities",
            },
        },
        # 关系: Legal Provision → CONTAINS → Provision Unit (a)
        {
            "name": "",
            "type": RELATION_CLASS,
            "attributes": {
                SUBJECT_KEY: "Legal Provision_International Emergency Economic Powers Act Section 1701",
                PREDICATE_KEY: "CONTAINS",
                OBJECT_KEY: "Provision Unit_Section 1701 subsection (a)",
            },
        },
        # 关系: Legal Provision → CONTAINS → Provision Unit (b)
        {
            "name": "",
            "type": RELATION_CLASS,
            "attributes": {
                SUBJECT_KEY: "Legal Provision_International Emergency Economic Powers Act Section 1701",
                PREDICATE_KEY: "CONTAINS",
                OBJECT_KEY: "Provision Unit_Section 1701 subsection (b)",
            },
        },
        # 关系: Provision Unit (a) → CITES → Citation (§ 1702)
        {
            "name": "",
            "type": RELATION_CLASS,
            "attributes": {
                SUBJECT_KEY: "Provision Unit_Section 1701 subsection (a)",
                PREDICATE_KEY: "CITES",
                OBJECT_KEY: "Citation_International Emergency Economic Powers Act Section 1702",
            },
        },
    ],
}

# =============================================================================
# example_for_clause 示例2: § 123.1（22 CFR Part 123）
# =============================================================================
# 输入：§ 123.1 - 出口或临时进口许可证要求
#   层级：Title 22 / Chapter I / Subchapter M (ITAR) / Part 123
#   内容：(a) 义务条款：出口/进口前须获得 DDTC 批准
#         (b) 程序条款：申请须按本部和 Part 120 规定提交
# 抽取意图：
#   - Legal Provision: § 123.1
#   - Provision Unit (a): 义务条款（function_type=obligation）
#   - Provision Unit (b): 程序条款（function_type=procedure）
#   - Citation: 22 CFR Part 120（外部引用，不同的 CFR Part）
#   - 关系: CONTAINS ×2, CITES ×1

_CC_EXAMPLE_2 = {
    "text": """
Filename: 22 CFR Part 123 (up to date as of 4-21-2026).txt
Classification: {"title": "Title 22 —Foreign Relations", "chapter": "Chapter I —Department of State", "subchapter": "Subchapter M —International Traffic in Arms Regulations", "part": "Part 123 Licenses for the Export and Temporary Import of Defense Articles"}
Section number: § 123.1
Section heading: Requirement for export or temporary import licenses.
Clause content:
Requirement for export or temporary import licenses.
(a) Any person who intends to export or temporarily import a defense article must obtain the approval of the Directorate of Defense Trade Controls prior to the export or temporary import, unless the export or temporary import qualifies for an exemption under this subchapter.
(b) Applications for licenses must be submitted in accordance with this part and part 120 of this subchapter.
""",
    "extractions": [
        # Legal Provision: 22 CFR § 123.1
        #   core_topic=国防物品出口/进口许可证要求
        #   applicable_industry=defense trade（国防贸易）
        {
            "name": "22 CFR Section 123.1",
            "type": "Legal Provision",
            "attributes": {
                "provision_number": "§ 123.1",
                "provision_heading": "Requirement for export or temporary import licenses.",
                "core_topic": "license requirement for export or temporary import of defense articles",
                "scope_of_effect": "exports and temporary imports of defense articles controlled under the ITAR",
                "classification_context": "Title 22, Chapter I, Subchapter M, Part 123",
                "applicable_industry": "defense trade",
            },
        },
        # Provision Unit (a): 义务条款（obligation）
        #   适用主体=任何意图出口/进口国防物品的人
        #   行为=必须在出口/进口前获得 DDTC 批准
        #   例外=符合本章豁免条件时可免于申请
        {
            "name": "Section 123.1 paragraph (a)",
            "type": "Provision Unit",
            "attributes": {
                "unit_content": "Any person who intends to export or temporarily import a defense article must obtain the approval of the Directorate of Defense Trade Controls prior to the export or temporary import, unless the export or temporary import qualifies for an exemption under this subchapter.",
                "unit_level": "paragraph",
                "unit_number": "Section 123.1 paragraph (a)",
                "hierarchy_path": "§ 123.1 > paragraph (a)",
                "applicable_industry": "defense trade",
                "function_type": "obligation",
                "applicable_subject": "any person who intends to export or temporarily import a defense article",
                "responsible_role": "applicant or exporter/importer",
                "conduct_description": "must obtain approval before export or temporary import",
                "condition": "the export or temporary import does not qualify for an exemption under this subchapter",
                "legal_consequence": "",
                "exception": "export or temporary import qualifies for an exemption under this subchapter",
                "time_element": "prior to the export or temporary import",
                "quantitative_standard": "",
                "other_information": "",
            },
        },
        # Provision Unit (b): 程序条款（procedure）
        #   conduct_description=必须按本部和 Part 120 提交申请
        {
            "name": "Section 123.1 paragraph (b)",
            "type": "Provision Unit",
            "attributes": {
                "unit_content": "Applications for licenses must be submitted in accordance with this part and part 120 of this subchapter.",
                "unit_level": "paragraph",
                "unit_number": "Section 123.1 paragraph (b)",
                "hierarchy_path": "§ 123.1 > paragraph (b)",
                "applicable_industry": "defense trade",
                "function_type": "procedure",
                "applicable_subject": "license applicants",
                "responsible_role": "applicant",
                "conduct_description": "must submit applications in accordance with this part and part 120",
                "condition": "when submitting license applications",
                "legal_consequence": "",
                "exception": "",
                "time_element": "",
                "quantitative_standard": "",
                "other_information": "",
            },
        },
        # Citation: 22 CFR Part 120（外部引用）
        #   citation_type=Document（文件级引用）
        #   is_internal_reference=No（不同的 CFR Part）
        {
            "name": "22 CFR Part 120",
            "type": "Citation",
            "attributes": {
                "citation_type": "Document",
                "official_title": "22 CFR Part 120",
                "document_type": "CFR Part",
                "provision_number": "",
                "is_internal_reference": "No",
                "citation_relation": "in accordance with",
                "citation_purpose": "application procedure",
            },
        },
        # 关系: Legal Provision → CONTAINS → Provision Unit (a)
        {
            "name": "",
            "type": RELATION_CLASS,
            "attributes": {
                SUBJECT_KEY: "Legal Provision_22 CFR Section 123.1",
                PREDICATE_KEY: "CONTAINS",
                OBJECT_KEY: "Provision Unit_Section 123.1 paragraph (a)",
            },
        },
        # 关系: Legal Provision → CONTAINS → Provision Unit (b)
        {
            "name": "",
            "type": RELATION_CLASS,
            "attributes": {
                SUBJECT_KEY: "Legal Provision_22 CFR Section 123.1",
                PREDICATE_KEY: "CONTAINS",
                OBJECT_KEY: "Provision Unit_Section 123.1 paragraph (b)",
            },
        },
        # 关系: Provision Unit (b) → CITES → Citation (Part 120)
        {
            "name": "",
            "type": RELATION_CLASS,
            "attributes": {
                SUBJECT_KEY: "Provision Unit_Section 123.1 paragraph (b)",
                PREDICATE_KEY: "CITES",
                OBJECT_KEY: "Citation_22 CFR Part 120",
            },
        },
    ],
}

example_for_clause = [_CC_EXAMPLE_1, _CC_EXAMPLE_2]
