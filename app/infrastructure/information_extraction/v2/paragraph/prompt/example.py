"""Small positive/negative examples; no production filenames or special-case rules."""


def receipt(target, decision, reason):
    return {"name": "处理_" + target, "type": "段落处理",
            "attributes": {"目标ID": target, "处理结论": decision, "理由": reason}}


def claim(target, text, kind, quote, **extra):
    return {"name": "知识_" + target, "type": "段落知识单元", "attributes": {
        "目标ID": target, "知识内容": text, "知识类型": kind, "原文依据": quote,
        "上下文依据": [], "合规风险类型": [], "合规域": [], "经济行业": [],
        "量化特征": "定性", "量化条件": None, "功能类型": None, **extra}}


FILE_EXAMPLES = [{"text": 'title=管理动态; source=新华社北京2月24日电，某机关印发了《示例意见》。', "extractions": [
    {"name": "管理动态", "type": "来源文件", "attributes": {"文件名称": "管理动态", "来源性质": "新闻报道",
      "来源机构": "新华社", "发布日期": "2月24日", "证据原文": "新华社北京2月24日电"}}]}]

CONTENT_EXAMPLES = [
    {"text": 'target_id=A; target=企业应当保存业务记录。; source_type=正式文件', "extractions": [
        receipt("A", "知识", "完整保存义务"), claim("A", "企业应当保存业务记录。", "规范要求", "企业应当保存业务记录。",
            适用主体="企业", 行为描述="保存业务记录", 功能类型="必要")]},
    {"text": 'target_id=B; target=近年来相关工作持续推进。', "extractions": [receipt("B", "非知识", "概括背景，无独立可验证命题")]},
    {"text": 'target_id=C; target=《示例意见》要求各部门建立协调机制。; source_type=新闻报道', "extractions": [
        receipt("C", "知识", "保留文件归属的参考命题"), claim("C", "《示例意见》要求各部门建立协调机制。", "规范要求",
            "《示例意见》要求各部门建立协调机制。", 归属说明="《示例意见》", 适用主体="各部门", 行为描述="建立协调机制", 功能类型="必要")]},
    {"text": 'target_id=D; target=甲专家认为，企业可优先完善内部审计。', "extractions": [
        receipt("D", "知识", "是专家观点，不是法律义务"), claim("D", "甲专家认为，企业可优先完善内部审计。", "观点转述",
            "甲专家认为，企业可优先完善内部审计。", 归属说明="甲专家")]},
    {"text": 'target_id=E; target=其应当保存记录。; context=本文件适用于甲类经营者。', "extractions": [
        receipt("E", "知识", "前段仅补齐指代对象"), claim("E", "甲类经营者应当保存记录。", "规范要求", "其应当保存记录。",
            上下文依据=["本文件适用于甲类经营者。"], 适用主体="甲类经营者", 行为描述="保存记录", 功能类型="必要")]},
    {"text": 'target_id=F; target=企业应当报告。; context=但依法豁免的情形除外。', "extractions": [
        receipt("F", "知识", "保留相邻段落的明确例外"), claim("F", "企业应当报告，但依法豁免的情形除外。", "规范要求",
            "企业应当报告。", 上下文依据=["但依法豁免的情形除外。"], 例外情形="但依法豁免的情形除外。", 行为描述="报告", 功能类型="必要")]},
    {"text": 'target_id=G; target=业务记录是指办理业务形成的原始资料。', "extractions": [
        receipt("G", "知识", "完整概念定义，没有行为约束方向"), claim("G", "业务记录是指办理业务形成的原始资料。", "概念定义",
            "业务记录是指办理业务形成的原始资料。", 适用主体="业务记录")]},
    {"text": 'target_id=H; target=该系统于2020年投入使用。', "extractions": [
        receipt("H", "知识", "有明确时点的事实，年份不是合规阈值"), claim("H", "该系统于2020年投入使用。", "事实陈述", "该系统于2020年投入使用。")]},
    {"text": 'target_id=I; target=对于上述情形，按照规定分别办理。; context=缺失', "extractions": [
        receipt("I", "不确定", "指代和适用规定缺失，不能编造完整要求")]},
    {"text": 'target_id=J; target=经营者保存交易记录并定期整理。; source_type=正式文件', "extractions": [
        receipt("J", "知识", "保留原文陈述；不能据普通陈述补写应当，方向暂不确认"),
        claim("J", "经营者保存交易记录并定期整理。", "规范要求", "经营者保存交易记录并定期整理。",
              适用主体="经营者", 行为描述="保存交易记录并定期整理", 功能类型=None, 归属说明=None)]},
    {"text": 'target_id=K; target=某机关印发了《示例通知》，要求各单位结合实际认真贯彻落实。; source_type=新闻报道', "extractions": [
        receipt("K", "非知识", "文件发布和笼统落实背景，作为来源元信息，不构成具体知识要求")]},
    {"text": 'target_id=L; target=但法律另有规定的除外。; context=经营者应当提交材料。', "extractions": [
        receipt("L", "非知识", "本目标是相邻主命题的例外说明，供主行为目标绑定；本段没有独立主行为，不重复抽取上下文要求")]},
    {"text": 'target_id=M; target=经营者应当在七日内报告事故。; source_type=正式文件', "extractions": [
        receipt("M", "知识", "有明确报告义务及期限"), claim("M", "经营者应当在七日内报告事故。", "规范要求",
            "经营者应当在七日内报告事故。", 适用主体="经营者", 行为描述="报告事故", 功能类型="必要", 量化特征="定量",
            量化条件={"原文件": "七日内", "量化值类型": "期限", "最小值": None, "最大值": 7, "单位": "日", "约束关系": "上限"})]},
    {"text": 'target_id=N; target=企业应当缴纳三千元以上八千元以下的罚款。; source_type=正式文件', "extractions": [
        receipt("N", "知识", "数量单独结构化，保留行为和原文"), claim("N", "企业应当缴纳三千元以上八千元以下的罚款。", "规范要求",
            "企业应当缴纳三千元以上八千元以下的罚款。", 适用主体="企业", 行为描述="缴纳罚款", 功能类型="必要", 量化特征="定量",
            量化条件={"原文件": "三千元以上八千元以下", "量化值类型": "金额", "最小值": 3000, "最大值": 8000, "单位": "元", "约束关系": "区间"})]},
]
