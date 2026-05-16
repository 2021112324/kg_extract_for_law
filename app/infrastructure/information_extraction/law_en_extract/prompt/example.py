_RELATION_CLASS = "\u5173\u7cfb"
_SUBJECT_KEY = "\u4e3b\u4f53"
_PREDICATE_KEY = "\u8c13\u8bcd"
_OBJECT_KEY = "\u5ba2\u4f53"

example_for_clause = [
    {
        "text": """
International Emergency Economic Powers Act Title 50. War and National Defense Chapter 35. International Emergency Economic Powers Section 1701 Unusual and extraordinary threat:
Unusual and extraordinary threat; declaration of national emergency; exercise of Presidential authorities
(a) Any authority granted to the President by section 1702 of this title may be exercised to deal with any unusual and extraordinary threat, which has its source in whole or substantial part outside the United States, to the national security, foreign policy, or economy of the United States, if the President declares a national emergency with respect to such threat.
(b) The authorities granted to the President by section 1702 of this title may only be exercised to deal with an unusual and extraordinary threat with respect to which a national emergency has been declared for purposes of this chapter and may not be exercised for any other purpose.
""",
        "extractions": [
            {
                "name": "International Emergency Economic Powers Act Section 1701",
                "type": "Legal Provision",
                "attributes": {
                    "core_topic": "Unusual and extraordinary threat and declaration of national emergency",
                    "scope_of_effect": "Presidential emergency economic authorities addressing threats outside the United States",
                    "applicable_industry": "general",
                },
            },
            {
                "name": "Section 1701 subsection (a)",
                "type": "Provision Unit",
                "attributes": {
                    "unit_content": "Any authority granted to the President by section 1702 of this title may be exercised to deal with any unusual and extraordinary threat, which has its source in whole or substantial part outside the United States, to the national security, foreign policy, or economy of the United States, if the President declares a national emergency with respect to such threat.",
                    "unit_heading": "",
                    "unit_level": "subsection",
                    "unit_number": "Section 1701 subsection (a)",
                    "applicable_industry": "general",
                    "function_type": "authorization",
                    "applicable_subject": "President",
                    "responsible_role": "authorized decision-maker",
                    "conduct_description": "may exercise authority to deal with an unusual and extraordinary threat",
                    "condition": "the threat has its source outside the United States and the President declares a national emergency",
                    "legal_consequence": "",
                    "exception": "",
                    "time_element": "",
                    "quantitative_standard": "",
                    "other_information": "",
                },
            },
            {
                "name": "Section 1701 subsection (b)",
                "type": "Provision Unit",
                "attributes": {
                    "unit_content": "The authorities granted to the President by section 1702 of this title may only be exercised to deal with an unusual and extraordinary threat with respect to which a national emergency has been declared for purposes of this chapter and may not be exercised for any other purpose.",
                    "unit_heading": "",
                    "unit_level": "subsection",
                    "unit_number": "Section 1701 subsection (b)",
                    "applicable_industry": "general",
                    "function_type": "limitation",
                    "applicable_subject": "President",
                    "responsible_role": "authorized decision-maker",
                    "conduct_description": "may exercise authorities only for the declared emergency threat",
                    "condition": "a national emergency has been declared for purposes of this chapter",
                    "legal_consequence": "",
                    "exception": "may not be exercised for any other purpose",
                    "time_element": "",
                    "quantitative_standard": "",
                    "other_information": "",
                },
            },
            {
                "name": "International Emergency Economic Powers Act Section 1702",
                "type": "Citation",
                "attributes": {
                    "citation_type": "Provision",
                    "official_title": "International Emergency Economic Powers Act",
                    "alias": "IEEPA",
                    "document_type": "Act",
                    "provision_number": "Section 1702",
                    "is_internal_reference": "Yes",
                    "citation_relation": "authority granted by",
                    "citation_purpose": "identify Presidential authorities",
                },
            },
            {
                "name": "",
                "type": _RELATION_CLASS,
                "attributes": {
                    _SUBJECT_KEY: "Legal Provision_International Emergency Economic Powers Act Section 1701",
                    _PREDICATE_KEY: "CONTAINS",
                    _OBJECT_KEY: "Provision Unit_Section 1701 subsection (a)",
                },
            },
            {
                "name": "",
                "type": _RELATION_CLASS,
                "attributes": {
                    _SUBJECT_KEY: "Legal Provision_International Emergency Economic Powers Act Section 1701",
                    _PREDICATE_KEY: "CONTAINS",
                    _OBJECT_KEY: "Provision Unit_Section 1701 subsection (b)",
                },
            },
            {
                "name": "",
                "type": _RELATION_CLASS,
                "attributes": {
                    _SUBJECT_KEY: "Provision Unit_Section 1701 subsection (a)",
                    _PREDICATE_KEY: "CITES",
                    _OBJECT_KEY: "Citation_International Emergency Economic Powers Act Section 1702",
                },
            },
            {
                "name": "",
                "type": _RELATION_CLASS,
                "attributes": {
                    _SUBJECT_KEY: "Provision Unit_Section 1701 subsection (b)",
                    _PREDICATE_KEY: "CITES",
                    _OBJECT_KEY: "Citation_International Emergency Economic Powers Act Section 1702",
                },
            },
        ],
    },
    {
        "text": """
Clean Water Act Section 311 Oil and hazardous substance liability:
(b) It shall be unlawful for any person to discharge oil or hazardous substances into or upon the navigable waters of the United States.
(c) The President is authorized to act to remove or arrange for the removal of any oil or hazardous substance discharged in violation of this section.
""",
        "extractions": [
            {
                "name": "Clean Water Act Section 311",
                "type": "Legal Provision",
                "attributes": {
                    "core_topic": "Oil and hazardous substance discharge liability",
                    "scope_of_effect": "Discharges into or upon navigable waters of the United States",
                    "applicable_industry": "general",
                },
            },
            {
                "name": "Section 311 subsection (b)",
                "type": "Provision Unit",
                "attributes": {
                    "unit_content": "It shall be unlawful for any person to discharge oil or hazardous substances into or upon the navigable waters of the United States.",
                    "unit_heading": "",
                    "unit_level": "subsection",
                    "unit_number": "Section 311 subsection (b)",
                    "applicable_industry": "general",
                    "function_type": "prohibition",
                    "applicable_subject": "any person",
                    "responsible_role": "regulated person",
                    "conduct_description": "discharge oil or hazardous substances into or upon navigable waters",
                    "condition": "",
                    "legal_consequence": "the conduct is unlawful",
                    "exception": "",
                    "time_element": "",
                    "quantitative_standard": "",
                    "other_information": "",
                },
            },
            {
                "name": "Section 311 subsection (c)",
                "type": "Provision Unit",
                "attributes": {
                    "unit_content": "The President is authorized to act to remove or arrange for the removal of any oil or hazardous substance discharged in violation of this section.",
                    "unit_heading": "",
                    "unit_level": "subsection",
                    "unit_number": "Section 311 subsection (c)",
                    "applicable_industry": "general",
                    "function_type": "authorization",
                    "applicable_subject": "President",
                    "responsible_role": "authorized decision-maker",
                    "conduct_description": "act to remove or arrange for removal of discharged oil or hazardous substances",
                    "condition": "oil or hazardous substance was discharged in violation of this section",
                    "legal_consequence": "",
                    "exception": "",
                    "time_element": "",
                    "quantitative_standard": "",
                    "other_information": "",
                },
            },
            {
                "name": "Clean Water Act Section 311",
                "type": "Citation",
                "attributes": {
                    "citation_type": "Provision",
                    "official_title": "Clean Water Act",
                    "alias": "",
                    "document_type": "Act",
                    "provision_number": "Section 311",
                    "is_internal_reference": "Yes",
                    "citation_relation": "this section",
                    "citation_purpose": "identify the violated provision",
                },
            },
            {
                "name": "",
                "type": _RELATION_CLASS,
                "attributes": {
                    _SUBJECT_KEY: "Legal Provision_Clean Water Act Section 311",
                    _PREDICATE_KEY: "CONTAINS",
                    _OBJECT_KEY: "Provision Unit_Section 311 subsection (b)",
                },
            },
            {
                "name": "",
                "type": _RELATION_CLASS,
                "attributes": {
                    _SUBJECT_KEY: "Legal Provision_Clean Water Act Section 311",
                    _PREDICATE_KEY: "CONTAINS",
                    _OBJECT_KEY: "Provision Unit_Section 311 subsection (c)",
                },
            },
            {
                "name": "",
                "type": _RELATION_CLASS,
                "attributes": {
                    _SUBJECT_KEY: "Provision Unit_Section 311 subsection (c)",
                    _PREDICATE_KEY: "CITES",
                    _OBJECT_KEY: "Citation_Clean Water Act Section 311",
                },
            },
        ],
    },
]

example_for_file_info = [
    {
        "text": """
Executive Order 14024 document description:
Executive Order 14024 of April 15, 2021. Blocking Property With Respect To Specified Harmful Foreign Activities of the Government of the Russian Federation. By the authority vested in me as President by the Constitution and the laws of the United States of America, including the International Emergency Economic Powers Act and the National Emergencies Act, I hereby order:
""",
        "extractions": [
            {
                "name": "Executive Order 14024",
                "type": "Legal Document",
                "attributes": {
                    "official_title": "Executive Order 14024",
                    "document_number": "Executive Order 14024",
                    "alias": "",
                    "document_type": "Order",
                    "publication_effective_info": "April 15, 2021",
                    "purpose": "Blocking Property With Respect To Specified Harmful Foreign Activities of the Government of the Russian Federation",
                    "domain": "sanctions and foreign affairs",
                    "applicable_industry": "general",
                    "scope_of_application": "specified harmful foreign activities of the Government of the Russian Federation",
                    "issuing_authority": "President of the United States",
                    "publication_date": "April 15, 2021",
                    "effective_date": "",
                    "status": "",
                },
            },
            {
                "name": "International Emergency Economic Powers Act",
                "type": "Legal Basis",
                "attributes": {
                    "official_title": "International Emergency Economic Powers Act",
                    "alias": "IEEPA",
                },
            },
            {
                "name": "National Emergencies Act",
                "type": "Legal Basis",
                "attributes": {
                    "official_title": "National Emergencies Act",
                    "alias": "",
                },
            },
            {
                "name": "",
                "type": _RELATION_CLASS,
                "attributes": {
                    _SUBJECT_KEY: "Legal Document_Executive Order 14024",
                    _PREDICATE_KEY: "BASED_ON",
                    _OBJECT_KEY: "Legal Basis_International Emergency Economic Powers Act",
                },
            },
            {
                "name": "",
                "type": _RELATION_CLASS,
                "attributes": {
                    _SUBJECT_KEY: "Legal Document_Executive Order 14024",
                    _PREDICATE_KEY: "BASED_ON",
                    _OBJECT_KEY: "Legal Basis_National Emergencies Act",
                },
            },
        ],
    }
]
