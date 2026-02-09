# 每个法律结构构建一套结构名称和结构对应结构内schema的dict
# 结构：{
#     "name": "结构名称",
#     "description": "结构描述"
# }
# schema：{
#     "nodes": "节点",
#     "edges": "边",
#     "structure_prompt": "结构内提示词"
# }
# 每个类别的诉讼文书对应一组结构（list[dict]），多个结构组成一份诉讼文书架构
# 根据类别，分块提示词获取列表中的结构；块内提示词使用对应结构的schema和提示词
# 除诉讼基本信息由文书首部、诉讼参与者信息、审判组织及日期组成诉讼基础信息外，其余部分基本一种结构对应一种schema

# 默认诉讼结构
default_head = {
    "name": "文书首部",
    "description": "包含文书类型、法院名称、案号等最基础的文书标识信息。"
}

default_participant = {
    "name": "诉讼参与者信息",
    "description": "详细列明本案所有参与诉讼的机关和个人身份信息，包括公诉机关、原告、被告、第三人等的基本信息。"
}

default_trial_process = {
    "name": "案件审理过程",
    "description": "记载本案从起诉到开庭的程序性事项，是案件进入实体审理的过渡段。"
}

default_defense_claim = {
    "name": "诉辩主张",
    "description": "包含“公诉机关、上诉人等指控”的全部内容（事实、证据、法律意见）以及“被告人及辩护人等意见”的全部内容。这是双方诉辩观点的完整呈现。"
}

default_fact = {
    "name": "事实认定",
    "description": "一般以“经审理查明”开头，是法院对案件事实的最终确认，包括事实本身、证据列举和证据认证结论。"
}

default_court_reasoned = {
    "name": "法院评议",
    "description": "一般以“本院认为”开头，是法院对案件性质、法律适用、争议焦点进行分析论证的部分，不包含事实描述。"
}

default_judgment = {
    "name": "裁判主文",
    "description": "一般以“依照XX(法规)...，判决如下：”或“裁定如下：”开头，是法院作出的最终、具体的处理决定。"
}

default_right_to_appeal = {
    "name": "上诉权利告知",
    "description": "一般以“如不服本判决，可在判决书送达之日起十五日内，向本院递交上诉状，并按对方当事人的人数提出副本，上诉于[上诉法院名称]。”开头，固定格式文本，告知当事人不服判决时的上诉权利、期限和法院。"
}

default_organization_and_date_of_trial = {
    "name": "审判组织及日期",
    "description": "列明参与案件审判的审判人员、辅助人员姓名及判决作出的日期，如“审判员：[审判员姓名]，书记员：[书记员姓名]，[日期]”等。"
}

default_appendix_french = {
    "name": "附录法文",
    "description": "附在判决书正文之后，补充列明判决所依据的主要法律条文的具体内容，是可选结构。"
}
# 默认结构schema
default_litigation_basic_info = {
    "nodes": """
    
    """,
    "edges": """
    
    """,
    "structure_prompt": "请提取并描述本案的诉讼基本信息，包括文书首部、诉讼参与者信息、审判组织及日期。"
}


def default_legal_structure():
    default_structure_list = [
        default_head,
        default_participant,
        default_trial_process,
        default_defense_claim,
        default_fact,
        default_court_reasoned,
        default_judgment,
        default_right_to_appeal,
        default_organization_and_date_of_trial,
        default_appendix_french
    ]
    structure_description = "一份完整的民事判决书通常包含以下结构部分："
    structure_list = []
    for structure in default_structure_list:
        if not structure:
            continue
        structure_description += "\n - " + structure["name"] + ": " + structure["description"]
        structure_list.append(structure["name"])
    return structure_description, structure_list

