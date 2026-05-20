# =============================================================================
# 英文抽取 few-shot 示例文件（Chinese Annotated Version / 中文翻译注解版）
# =============================================================================
# 说明：
# - 示例中的 text / entity type / attributes 保持英文，因为它们会参与模型学习输出格式。
# - 中文翻译写在注释中，避免污染模型输出。
# - extraction    = 抽取项
# - attributes    = 属性
# - Legal Provision = 法条（Section/Article 层级的法律条文）
# - Provision Unit   = 条款单元（法条内部的细分段落 subsection/paragraph/clause）
# - Citation         = 引用依据（法条中引用的其他法律文件或具体条款）
# - Legal Document   = 法规文件（整部法规如 Act、Order、Code 等）
# - Legal Basis      = 法规依据（当前法规的上位法或制定依据）
# =============================================================================

# 关系抽取的 class 名称和属性名（中文，用 unicode 转义表示）
# 这是为了兼容现有 Langextract 解析器，解析器要求关系 entity 使用特定的中文 class 名
_RELATION_CLASS = "\u5173\u7cfb"      # 关系
_SUBJECT_KEY = "\u4e3b\u4f53"         # 主体（source）
_PREDICATE_KEY = "\u8c13\u8bcd"       # 谓词（relation type）
_OBJECT_KEY = "\u5ba2\u4f53"          # 客体（target）

# =============================================================================
# example_for_clause：单条英文法条抽取示例
# 用途：教模型如何从英文法条中抽取 Legal Provision、Provision Unit、Citation
#       以及它们之间的 CONTAINS / CITES 关系
#
# 第一个示例展示：
#   - International Emergency Economic Powers Act (IEEPA) 的 Section 1701
#   - 同文件内部引用 "section 1702 of this title" → Citation 标记 internal
#   - 授权性条款 (a)：总统行使紧急经济权力的条件
#   - 限制性条款 (b)：总统权力仅可用于已宣布紧急状态的威胁
#
# 第二个示例展示：
#   - Clean Water Act 的 Section 311 油污责任条款
#   - 禁止性条款 (b)：禁止向美国水域排放油污
#   - 授权性条款 (c)：总统有权清除违法排放的油污
#   - 内部引用 "this section" → Citation 指向 Clean Water Act Section 311
# =============================================================================
example_for_clause = [
    # =========================================================================
    # 示例 1：IEEPA Section 1701 - 异常与非常威胁
    # 展示：授权条款、限制条款、同文件内部引用（section 1702 of this title）
    # =========================================================================
    {
        # text: 输入的英文章节全文
        # 说明：包含文件名、层级标题、法条编号和标题、以及条款正文
        "text": """
International Emergency Economic Powers Act Title 50. War and National Defense Chapter 35. International Emergency Economic Powers Section 1701 Unusual and extraordinary threat:
Unusual and extraordinary threat; declaration of national emergency; exercise of Presidential authorities
(a) Any authority granted to the President by section 1702 of this title may be exercised to deal with any unusual and extraordinary threat, which has its source in whole or substantial part outside the United States, to the national security, foreign policy, or economy of the United States, if the President declares a national emergency with respect to such threat.
(b) The authorities granted to the President by section 1702 of this title may only be exercised to deal with an unusual and extraordinary threat with respect to which a national emergency has been declared for purposes of this chapter and may not be exercised for any other purpose.
""",
        # extractions: 模型应输出的抽取结果列表
        "extractions": [
            # --- Legal Provision: Section 1701 法条节点 ---
            # name: 法条名称 = 法规名 + Section 编号
            # type: 实体类型 = Legal Provision（法条）
            # attributes:
            #   core_topic              = 核心主题：异常与非常威胁及国家紧急状态宣告
            #   scope_of_effect         = 效力范围：总统紧急经济授权，针对美国境外威胁
            #   applicable_industry     = 适用行业：general（一般适用）
            {
                "name": "International Emergency Economic Powers Act Section 1701",
                "type": "Legal Provision",
                "attributes": {
                    "core_topic": "Unusual and extraordinary threat and declaration of national emergency",
                    "scope_of_effect": "Presidential emergency economic authorities addressing threats outside the United States",
                    "applicable_industry": "general",
                },
            },

            # --- Provision Unit: subsection (a) 条款单元 ---
            # name: 单元名称 = Section 1701 subsection (a)
            # type: 实体类型 = Provision Unit（条款单元）
            # attributes:
            #   unit_content            = 单元全文：总统可行使 section 1702 授予之权力应对境外威胁
            #   unit_heading            = 单元标题：无（inline subsection 通常无独立标题）
            #   unit_level              = 单元层级：subsection
            #   unit_number             = 单元编号：Section 1701 subsection (a)
            #   applicable_industry     = 适用行业：general
            #   function_type           = 功能类型：authorization（授权性条款）
            #   applicable_subject      = 适用主体：President（总统）
            #   responsible_role        = 责任角色：authorized decision-maker（被授权决策者）
            #   conduct_description     = 行为描述：可行使权力应对异常与非常威胁
            #   condition               = 适用条件：威胁源自美国境外且总统宣布国家紧急状态
            #   legal_consequence       = 法律后果：无（纯授权条款）
            #   exception               = 例外情形：无
            #   time_element            = 时间要素：无
            #   quantitative_standard   = 量化标准：无
            #   other_information       = 其他信息：无
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

            # --- Provision Unit: subsection (b) 条款单元 ---
            # function_type: limitation（限制性条款）
            # conduct_description: 仅可为已宣告紧急状态的威胁行使权力
            # exception: 不得为其他任何目的行使权力（相当于对授权范围的限制）
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

            # --- Citation: Section 1702 引用依据 ---
            # citation_type: Provision（针对具体条款的引用，而非整个文件）
            # is_internal_reference: Yes（同文件内部引用："of this title"）
            # citation_relation: authority granted by（授权关系：section 1702 授予的权力）
            # citation_purpose: identify Presidential authorities（识别总统权力来源）
            # alias: IEEPA（简称/缩写）
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

            # --- 关系：Legal Provision CONTAINS Provision Unit (a) ---
            # subject:    "Legal Provision_International Emergency Economic Powers Act Section 1701"
            #             源 = Section 1701 法条
            # predicate:  "CONTAINS"
            #             关系类型 = 包含
            # object:     "Provision Unit_Section 1701 subsection (a)"
            #             目标 = subsection (a) 条款单元
            {
                "name": "",
                "type": _RELATION_CLASS,
                "attributes": {
                    _SUBJECT_KEY: "Legal Provision_International Emergency Economic Powers Act Section 1701",
                    _PREDICATE_KEY: "CONTAINS",
                    _OBJECT_KEY: "Provision Unit_Section 1701 subsection (a)",
                },
            },

            # --- 关系：Legal Provision CONTAINS Provision Unit (b) ---
            {
                "name": "",
                "type": _RELATION_CLASS,
                "attributes": {
                    _SUBJECT_KEY: "Legal Provision_International Emergency Economic Powers Act Section 1701",
                    _PREDICATE_KEY: "CONTAINS",
                    _OBJECT_KEY: "Provision Unit_Section 1701 subsection (b)",
                },
            },

            # --- 关系：Provision Unit (a) CITES Citation Section 1702 ---
            # subsection (a) 引用了 Section 1702（"by section 1702 of this title"）
            {
                "name": "",
                "type": _RELATION_CLASS,
                "attributes": {
                    _SUBJECT_KEY: "Provision Unit_Section 1701 subsection (a)",
                    _PREDICATE_KEY: "CITES",
                    _OBJECT_KEY: "Citation_International Emergency Economic Powers Act Section 1702",
                },
            },

            # --- 关系：Provision Unit (b) CITES Citation Section 1702 ---
            # subsection (b) 也引用了 Section 1702
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

    # =========================================================================
    # 示例 2：Clean Water Act Section 311 - 油污与有害物质责任
    # 展示：禁止条款（unlawful）、授权条款、内部引用（this section）
    # =========================================================================
    {
        # text: Clean Water Act（清洁水法）Section 311 条款
        # (b) 禁止性：任何人向美国可航行水域排放油污或有害物质均属违法
        # (c) 授权性：总统有权清除违法排放的油污或有害物质
        "text": """
Clean Water Act Section 311 Oil and hazardous substance liability:
(b) It shall be unlawful for any person to discharge oil or hazardous substances into or upon the navigable waters of the United States.
(c) The President is authorized to act to remove or arrange for the removal of any oil or hazardous substance discharged in violation of this section.
""",
        "extractions": [
            # --- Legal Provision: Section 311 法条 ---
            # core_topic: 油污与有害物质排放责任
            # scope_of_effect: 针对美国可航行水域的排放行为
            {
                "name": "Clean Water Act Section 311",
                "type": "Legal Provision",
                "attributes": {
                    "core_topic": "Oil and hazardous substance discharge liability",
                    "scope_of_effect": "Discharges into or upon navigable waters of the United States",
                    "applicable_industry": "general",
                },
            },

            # --- Provision Unit: subsection (b) 禁止条款 ---
            # function_type: prohibition（禁止性条款）
            # applicable_subject: any person（任何人，泛指所有受规制主体）
            # responsible_role: regulated person（被规制者）
            # conduct_description: 向美国可航行水域排放油污或有害物质
            # legal_consequence: 该行为属于违法（unlawful）
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

            # --- Provision Unit: subsection (c) 授权清除条款 ---
            # function_type: authorization（授权性条款）
            # condition: 油污或有害物质已违反本节规定被排放（触发了清除条件）
            # conduct_description: 采取行动清除或安排清除已排放的油污
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

            # --- Citation: Clean Water Act Section 311 自引用 ---
            # subsection (c) 中 "in violation of this section" 引用自身
            # is_internal_reference: Yes（内部自引用）
            # citation_relation: "this section"（本节）
            # citation_purpose: identify the violated provision（标识被违反的条款）
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

            # --- 关系：Legal Provision CONTAINS Provision Unit (b) ---
            {
                "name": "",
                "type": _RELATION_CLASS,
                "attributes": {
                    _SUBJECT_KEY: "Legal Provision_Clean Water Act Section 311",
                    _PREDICATE_KEY: "CONTAINS",
                    _OBJECT_KEY: "Provision Unit_Section 311 subsection (b)",
                },
            },

            # --- 关系：Legal Provision CONTAINS Provision Unit (c) ---
            {
                "name": "",
                "type": _RELATION_CLASS,
                "attributes": {
                    _SUBJECT_KEY: "Legal Provision_Clean Water Act Section 311",
                    _PREDICATE_KEY: "CONTAINS",
                    _OBJECT_KEY: "Provision Unit_Section 311 subsection (c)",
                },
            },

            # --- 关系：Provision Unit (c) CITES Citation Section 311 ---
            # subsection (c) 的清除条件引用了 Section 311 自身的违法排放规定
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

# =============================================================================
# example_for_file_info：法规文件头信息抽取示例
# 用途：教模型如何从法规文件的头部信息中抽取 Legal Document（法规文件）
#       和 Legal Basis（法规依据），以及它们之间的 BASED_ON 关系
#
# 示例说明：
#   - Executive Order 14024（第14024号行政令）
#   - 由美国总统于2021年4月15日签署发布
#   - 目的：封锁俄罗斯联邦政府特定有害涉外活动的财产
#   - 依据：International Emergency Economic Powers Act (IEEPA)
#          和 National Emergencies Act（国家紧急状态法）
#   - BASED_ON 关系表示行政令的制定依据是这两部上位法
# =============================================================================
example_for_file_info = [
    {
        # text: 文件头描述文本
        # 模拟从文件头部提取的描述信息，包含：
        #   - 文件编号与发布日期：Executive Order 14024 of April 15, 2021
        #   - 主题/目的：Blocking Property With Respect To Specified Harmful Foreign Activities...
        #   - 制定依据：Constitution + IEEPA + National Emergencies Act
        "text": """
Executive Order 14024 document description:
Executive Order 14024 of April 15, 2021. Blocking Property With Respect To Specified Harmful Foreign Activities of the Government of the Russian Federation. By the authority vested in me as President by the Constitution and the laws of the United States of America, including the International Emergency Economic Powers Act and the National Emergencies Act, I hereby order:
""",
        "extractions": [
            # --- Legal Document: Executive Order 14024 法规文件节点 ---
            # name: Executive Order 14024（文件名称）
            # type: Legal Document（法规文件）
            # attributes:
            #   official_title              = 正式标题：Executive Order 14024
            #   document_number             = 文件编号：Executive Order 14024
            #   alias                       = 别名/简称：无
            #   document_type               = 文件类型：Order（行政令）
            #   publication_effective_info  = 发布生效信息：April 15, 2021
            #   purpose                     = 制定目的：封锁俄罗斯政府特定有害涉外活动的财产
            #   domain                      = 领域：制裁与外交事务
            #   applicable_industry         = 适用行业：general
            #   scope_of_application        = 适用范围：俄罗斯政府特定有害涉外活动
            #   issuing_authority           = 发布单位：President of the United States
            #   publication_date            = 发布日期：April 15, 2021
            #   effective_date              = 生效日期：未指定（可能立即生效）
            #   status                      = 状态：未指定
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

            # --- Legal Basis: International Emergency Economic Powers Act 法规依据 ---
            # 说明：国际紧急经济权力法（IEEPA）是行政令的上位法依据之一
            # alias: IEEPA（常用缩写）
            {
                "name": "International Emergency Economic Powers Act",
                "type": "Legal Basis",
                "attributes": {
                    "official_title": "International Emergency Economic Powers Act",
                    "alias": "IEEPA",
                },
            },

            # --- Legal Basis: National Emergencies Act 法规依据 ---
            # 说明：国家紧急状态法是行政令的第二部上位法依据
            {
                "name": "National Emergencies Act",
                "type": "Legal Basis",
                "attributes": {
                    "official_title": "National Emergencies Act",
                    "alias": "",
                },
            },

            # --- 关系：Legal Document BASED_ON Legal Basis (IEEPA) ---
            # subject:    "Legal Document_Executive Order 14024"
            #             源 = 第14024号行政令
            # predicate:  "BASED_ON"
            #             关系类型 = 依据（上位法）
            # object:     "Legal Basis_International Emergency Economic Powers Act"
            #             目标 = 国际紧急经济权力法
            {
                "name": "",
                "type": _RELATION_CLASS,
                "attributes": {
                    _SUBJECT_KEY: "Legal Document_Executive Order 14024",
                    _PREDICATE_KEY: "BASED_ON",
                    _OBJECT_KEY: "Legal Basis_International Emergency Economic Powers Act",
                },
            },

            # --- 关系：Legal Document BASED_ON Legal Basis (National Emergencies Act) ---
            # 源 = 第14024号行政令，目标 = 国家紧急状态法
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
