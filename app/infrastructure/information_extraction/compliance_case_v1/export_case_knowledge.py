"""从合规案例知识图谱导出 case_knowledge 视图。

本文件的定位：
1. 输入是已经生成好的图谱 graph，而不是原始文本。
2. 输出是知识库字段 case_knowledge，供后续数据处理或知识库入库使用。
3. 这里不新增图谱知识，不修改节点和边，只做查询、聚合、去重和字段映射。

也就是说：知识图谱是主产物，case_knowledge 是从图谱派生出的下游视图。
"""

from __future__ import annotations

import re
from typing import Any


CONTROLLED_RISK_TYPES = [
    "产品法律风险",
    "供应链合规风险",
    "劳动用工法律合规风险",
    "企业关联方合规风险",
    "企业国际化经营合规风险",
    "企业信用风险",
]

RISK_TYPE_INDICATOR_SYSTEM_RULES = [
    ("产品法律风险", r"(?:\d+[.．、])?产品法律风险(?:体系)?指标"),
    ("供应链合规风险", r"(?:\d+[.．、])?供应链合规风险(?:体系)?指标"),
    ("劳动用工法律合规风险", r"(?:\d+[.．、])?劳动用工法律合规风险(?:体系)?指标|(?:\d+[.．、])?劳动法律风险(?:体系)?指标"),
    ("企业关联方合规风险", r"(?:\d+[.．、])?企业关联方合规风险(?:体系)?指标|(?:\d+[.．、])?关联方合规风险(?:体系)?指标"),
    ("企业国际化经营合规风险", r"(?:\d+[.．、])?企业国际化经营合规风险(?:体系)?指标|(?:\d+[.．、])?国际化经营风险(?:体系)?指标"),
    ("企业信用风险", r"(?:\d+[.．、])?企业信用风险(?:体系)?指标|(?:\d+[.．、])?信用风险(?:体系)?指标"),
]

RISK_POINT_ROOT_TYPE_MAPPING = {
    "产品法律风险体系指标": "产品法律风险",
    "产品法律风险指标": "产品法律风险",
    "信用风险指标": "企业信用风险",
    "企业信用风险指标体系": "企业信用风险",
    "劳动用工法律合规风险指标": "劳动用工法律合规风险",
    "劳动用工法律风险指标": "劳动用工法律合规风险",
    "人力资源合规风险指标": "劳动用工法律合规风险",
    "供应链合规风险指标": "供应链合规风险",
    "企业关联方合规风险指标": "企业关联方合规风险",
    "关联方合规风险指标": "企业关联方合规风险",
    "企业国际化经营合规风险指标": "企业国际化经营合规风险",
    "治理结构与组织合规指标": "劳动用工法律合规风险",
    "知识产权信用": "产品法律风险",
    "知识产权合规": "产品法律风险",
    "知识产权合规风险": "产品法律风险",
    "侵权应对机制": "产品法律风险",
    "公司治理与信用管理合规": "企业信用风险",
}

RISK_TYPE_KEYWORD_RULES = [
    ("劳动用工法律合规风险", r"劳动|用工|员工|工伤|社保|劳务|劳动合同|职业健康|特殊劳动"),
    ("供应链合规风险", r"供应链|供应商|采购|分包|承包|招投标|招标|投标|物流|运输|原材料"),
    ("企业关联方合规风险", r"关联方|关联交易|控股|子公司|母公司|股东|产权处置|投资退出|国资监管|重大事项报告"),
    ("企业国际化经营合规风险", r"国际|境外|海外|出口|进口|跨境|制裁|经济制裁|海关|外汇|反洗钱|贸易管制|海外投资"),
    ("企业信用风险", r"信用|失信|征信|反腐败|商业贿赂|贪污|受贿|挪用|市场竞争|反垄断|反不正当竞争"),
    ("产品法律风险", r"产品|质量|安全生产|环保|环境|污染|知识产权|专利|商标|著作权|商业秘密|数据安全|个人信息|网络安全"),
]


def export_case_knowledge_from_graph(
    graph: dict[str, Any],
    parsed_document: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """将图谱聚合为一条 case_knowledge 记录。

    参数：
    - graph：graph_extract.py 生成的图谱，包含 nodes 和 edges。
    - parsed_document：解析器输出的中间结构，用作兜底信息来源。

    兜底逻辑示例：
    - 如果 LLM 没有补出“案例名称”，就使用 parser 解析出的 case_title。

    注意：
    本函数不会从原文重新抽取新实体，也不会推理额外关系。
    """

    parsed_document = parsed_document or {}
    nodes = graph.get("nodes", []) or []
    edges = graph.get("edges", []) or []

    # 主案例节点是 case_knowledge 的核心来源。
    case_node = _first_node(nodes, "案例")
    case_props = case_node.get("properties", {}) if case_node else {}

    # 按节点类型分组，后续字段从这些节点聚合。
    risk_nodes = _nodes_by_type(nodes, "风险点")
    analysis_nodes = _nodes_by_type(nodes, "案例分析")
    regulation_nodes = _nodes_by_type(nodes, "法规条款依据")
    disposal_nodes = _nodes_by_type(nodes, "处置方案")
    insight_nodes = _nodes_by_type(nodes, "案例启示")

    return {
        "object_type": "case_knowledge",

        # case_id 优先使用图谱主节点 ID；没有主节点时退回 parser 结果。
        "case_id": case_node.get("node_id") if case_node else parsed_document.get("case_id", ""),

        # 标题优先级：LLM 补充的案例属性 > parser 标题 > 节点名。
        "case_title": _first_non_empty(
            case_props.get("案例名称"),
            parsed_document.get("case_title"),
            case_node.get("node_name") if case_node else "",
        ),

        # source_compliance_domain 是原始合规领域，不等同于六类 risk_types。
        "source_compliance_domain": _join_inline(
            _unique_values(case_props.get("原始合规领域"))
        ),

        # full_text 保留完整原文，便于后续审计和回溯。
        "full_text": _first_non_empty(case_props.get("原文全文"), parsed_document.get("full_text")),

        # risk_types 是受控字段，只允许输出 CONTROLLED_RISK_TYPES 中的枚举值。
        "risk_types": _normalize_risk_types(
            [node.get("properties", {}).get("风险类型") for node in risk_nodes]
            + _unique_values(case_props.get("风险类型"))
            + _unique_values(case_props.get("合规领域")),
            case_props=case_props,
            parsed_document=parsed_document,
        ),

        # 关键词只来自图谱案例节点属性，不由 parser 或导出层重新抽取。
        "keywords": _normalize_keywords(case_props.get("关键词") or []),

        # 法规依据 ID 来自法规节点；同时补充依据/违反关系指向的法规节点 ID。
        "related_regulation_ids": _unique_values(
            [node.get("node_id") for node in regulation_nodes] + _edge_related_regulation_ids(edges)
        ),

        # 指标 ID 只能来自风险点节点属性。原文没有 ID 时这里保持空列表。
        "related_indicator_ids": _unique_values([node.get("properties", {}).get("指标ID") for node in risk_nodes]),

        # 专家结论、分析、处置方案按扩展参考知识保留，默认不直接参与后续模型输入。
        "conclusion": _text_values(
            [node.get("properties", {}).get("裁决决定") for node in _nodes_by_type(nodes, "案例结果")]
        ),
        "conclusion_usage": "参考知识，不直接参与后续模型输入",
        "analysis": _text_values([node.get("properties", {}).get("分析过程") for node in analysis_nodes]),
        "analysis_usage": "参考知识，不直接参与后续模型输入",

        # 处置方案可来自“处置方案”节点，也可来自“案例启示”节点。
        "disposal_plan": _text_values(
            [node.get("properties", {}).get("处置方案内容") for node in disposal_nodes]
        ),
        "disposal_plan_usage": "参考知识，不直接参与后续模型输入",
    }


def _first_node(nodes: list[dict[str, Any]], node_type: str) -> dict[str, Any]:
    """返回指定类型的第一个节点。

    当前一个文件只对应一个案例，因此“案例”节点理论上只有一个。
    对其他类型如果需要全部节点，应使用 _nodes_by_type。
    """

    for node in nodes:
        if node.get("node_type") == node_type:
            return node
    return {}


def _nodes_by_type(nodes: list[dict[str, Any]], node_type: str) -> list[dict[str, Any]]:
    """按 node_type 过滤节点列表。"""

    return [node for node in nodes if node.get("node_type") == node_type]


def _first_non_empty(*values: Any) -> Any:
    """按顺序返回第一个非空值。

    空字符串、None、空列表、空字典都视为无效值。
    """

    for value in values:
        if value not in (None, "", [], {}):
            return value
    return ""


def _unique_values(values: Any) -> list[Any]:
    """把单值/列表统一转换为去重列表。

    支持嵌套一层列表，主要用于处理：
    - 案例关键词可能本来就是 list；
    - 风险类型可能来自多个风险点节点。
    """

    if values is None:
        return []
    if not isinstance(values, list):
        values = [values]
    result: list[Any] = []
    for value in values:
        if value in (None, "", [], {}):
            continue
        if isinstance(value, list):
            for item in value:
                if item not in (None, "", [], {}) and item not in result:
                    result.append(item)
        elif value not in result:
            result.append(value)
    return result


def _normalize_risk_types(
    values: Any,
    case_props: dict[str, Any] | None = None,
    parsed_document: dict[str, Any] | None = None,
) -> list[str]:
    """将风险类型收敛到受控词表。

    输出只能是：
    [产品法律风险、供应链合规风险、劳动用工法律合规风险、企业关联方合规风险、
    企业国际化经营合规风险、企业信用风险]。

    若原值无法映射到受控词表，则不输出该值，避免 case_knowledge 混入
    “反腐败合规”“商业秘密合规”等非枚举标签。
    """

    case_props = case_props or {}
    parsed_document = parsed_document or {}
    candidates: list[Any] = []
    candidates.extend(_unique_values(values))
    candidates.extend(_unique_values(case_props.get("风险类型")))
    candidates.extend(_unique_values(case_props.get("合规领域")))
    result: list[str] = []
    for candidate in candidates:
        text = str(candidate or "").strip()
        if not text:
            continue
        system_risk_types = _map_indicator_system_risk_types(text)
        for mapped in system_risk_types:
            if mapped not in result:
                result.append(mapped)
        if system_risk_types:
            continue
        if text in CONTROLLED_RISK_TYPES and text not in result:
            result.append(text)
            continue
        mapped = _map_to_controlled_risk_type(text)
        if mapped and mapped not in result:
            result.append(mapped)
    return result


_RISK_POINT_BOOK_TITLE_RE = re.compile(r"《([^》]+)》")
_RISK_POINT_ROOT_SEPARATORS = {"-", "－", "—", "–", ":", "："}


def extract_risk_point_root_types(value: str) -> list[str]:
    """提取风险点文本中的指标体系根节点标题。

    风险点中可能同时出现指标体系书名号和法规书名号，例如：
    “《2.产品法律风险体系指标》 - ... 是否按照《安全生产法》要求...”
    这里仅接受“《...》”后紧接路径分隔符或编号路径起点的标题，避免把法规引用
    误识别为风险点根节点。
    """

    text = str(value or "")
    if not text:
        return []

    result: list[str] = []
    for match in _RISK_POINT_BOOK_TITLE_RE.finditer(text):
        if not _is_risk_point_root_title(text, match.end()):
            continue
        title = re.sub(r"\s+", "", match.group(1).strip())
        if title and title not in result:
            result.append(title)
    return result


def strip_risk_point_root_order(value: str) -> str:
    """去除风险点根节点标题前的编号，保留可用于映射六类风险的稳定标题。"""

    text = re.sub(r"\s+", "", str(value or "").strip())
    return re.sub(r"^\d+(?:\.\d+)*[.．、]?", "", text)


def _is_risk_point_root_title(text: str, end_index: int) -> bool:
    tail = text[end_index:].lstrip()
    if not tail:
        return False
    if tail[0] in _RISK_POINT_ROOT_SEPARATORS:
        return True
    return bool(re.match(r"\d+(?:\.\d+)*", tail))


def _map_indicator_system_risk_types(value: str) -> list[str]:
    """从风险点原文中的指标体系名称映射到六类受控风险类型。

    该规则优先级高于语义关键词映射。例如原文出现
    “《供应链合规风险指标》”，即使指标名称中包含“合同与交易信用”等词，
    也应优先识别为“供应链合规风险”。
    """

    raw_text = str(value or "")
    root_types = extract_risk_point_root_types(raw_text)
    if root_types:
        result: list[str] = []
        for root_type in root_types:
            mapped = RISK_POINT_ROOT_TYPE_MAPPING.get(strip_risk_point_root_order(root_type), "")
            if mapped and mapped not in result:
                result.append(mapped)
        return result

    text = re.sub(r"\s+", "", raw_text)
    if not text:
        return []
    matches: list[tuple[int, str]] = []
    for risk_type, pattern in RISK_TYPE_INDICATOR_SYSTEM_RULES:
        match = re.search(pattern, text)
        if match:
            matches.append((match.start(), risk_type))
    result: list[str] = []
    for _, risk_type in sorted(matches, key=lambda item: item[0]):
        if risk_type not in result:
            result.append(risk_type)
    return result


def _map_to_controlled_risk_type(value: str) -> str:
    """根据短语关键词映射到受控风险类型。"""

    text = re.sub(r"\s+", "", str(value or ""))
    if not text:
        return ""
    for risk_type, pattern in RISK_TYPE_KEYWORD_RULES:
        if re.search(pattern, text):
            return risk_type
    return ""


def _join_values(values: list[Any]) -> str:
    """对多个文本字段去重后用空行拼接。"""

    items = [str(value).strip() for value in _unique_values(values) if str(value).strip()]
    return "\n\n".join(items)


def _join_inline(values: Any) -> str:
    """对多个短标签去重后用中文分号拼接。"""

    return "；".join(str(value).strip() for value in _unique_values(values) if str(value).strip())


def _text_values(values: list[Any]) -> list[str]:
    """输出知识库数组字段，保留顺序并去重。"""

    return [str(value).strip() for value in _unique_values(values) if str(value).strip()]


def _normalize_keywords(values: Any) -> list[str]:
    """清理关键词，避免完整指标路径进入知识库关键词。"""

    result: list[str] = []
    for value in _unique_values(values):
        text = str(value or "").strip()
        if not text:
            continue
        if len(text) > 40:
            continue
        if " - " in text or re.search(r"\d+\.\d+", text):
            continue
        if text not in result:
            result.append(text)
    return result


def _edge_related_regulation_ids(edges: list[dict[str, Any]]) -> list[str]:
    """从边中补充法规依据 ID。

    如果某条边是“依据”或“违反”，且 target_id 是 regulation_ 开头，
    说明该边指向法规条款依据节点，可以纳入 related_regulation_ids。
    """

    result: list[str] = []
    for edge in edges:
        if edge.get("relation_type") in {"依据", "违反"}:
            target_id = edge.get("target_id")
            if isinstance(target_id, str) and target_id.startswith("regulation_"):
                result.append(target_id)
    return result
