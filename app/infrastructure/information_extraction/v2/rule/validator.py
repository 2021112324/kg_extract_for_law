"""Law-schema validation with explicit evidence and numeric location checks."""
from ..guide_p1.validator import build_schema_contract, normalize_extraction_result
from ...quantitative import normalize_quant_condition
from .io_utils import compact
from .prompt.schema import schema_for_clause, schema_for_file_info
import re


CONTRACT = build_schema_contract(schema_for_file_info, schema_for_clause)
RISK_TYPES = ("产品法律风险", "供应链合规风险", "劳动用工法律合规风险", "企业关联方合规风险", "企业国际化经营合规风险", "企业信用风险")


def supported(quote, source):
    return bool(compact(quote)) and compact(quote) in compact(source)


def validate_result(raw, *, source, unit=None, title=None):
    result = normalize_extraction_result(raw)
    entities, relations = result["entities"], result["relations"]
    allowed_types = {"法规文件", "法规依据"} if unit is None else {"法条", "条款单元", "引用依据"}
    if not entities:
        raise ValueError("Missing required model entities")
    names = {}
    for e in entities:
        kind, name, props = e["entity_type"], e["name"], e["properties"]
        if kind not in allowed_types or not name or name in names:
            raise ValueError("Unknown, empty or ambiguous entity: " + str((kind, name)))
        names[name] = kind
        extra = set(props) - CONTRACT.allowed_properties[kind]
        if extra:
            raise ValueError("Schema outside properties: " + str(sorted(extra)))
        for field in ("合规域", "经济行业", "合规风险类型", "来源编号列表"):
            if field in props:
                value = props[field]
                if not isinstance(value, list) or any(not isinstance(v, str) for v in value):
                    raise ValueError(field + " must be a string array")
        if "合规风险类型" in props:
            if any(v not in RISK_TYPES or v not in source for v in props["合规风险类型"]):
                raise ValueError("Risk type not explicitly supported by source")
        if "经济行业" in props and (not props["经济行业"] or "通用" in props["经济行业"] and len(props["经济行业"]) > 1):
            raise ValueError("Invalid economic industry array")
        if kind == "法规文件" and (not title or compact(props.get("文件全称", name)) != compact(title)):
            raise ValueError("Document entity does not match the actual source title")
        if kind == "法规依据" and not supported(props.get("文件全称", name), source):
            raise ValueError("Unsupported document basis")
        if unit is None:
            continue
        if kind == "法条":
            if props.get("条") != unit["number"]:
                raise ValueError("Article number changed")
            if props.get("法条全文") and compact(props["法条全文"]) != compact(unit["target"]):
                raise ValueError("Article source text rewritten or context mixed into original")
            if props.get("上级上下文") and compact(props["上级上下文"]) != compact(unit["context"]):
                raise ValueError("Article context changed")
            if props.get("来源编号列表") and props["来源编号列表"] != unit["member_numbers"]:
                raise ValueError("Group members changed")
        if kind in {"法条", "条款单元"} and not supported(props.get("原文依据"), unit["target"]):
            raise ValueError(kind + " missing source-grounded target evidence")
        if kind == "条款单元":
            if not isinstance(props.get("条款单元内容"), str) or not props["条款单元内容"].strip():
                raise ValueError("Empty semantic proposition")
            location = str(props.get("单元编号", ""))
            def owns_location(number):
                if location == number or location.startswith(number + "第"):
                    return True
                suffix = location[len(number):] if location.startswith(number) else ""
                return bool(re.fullmatch(r"[（(][一二三四五六七八九十百\d]+[）)]", suffix) and
                            re.search(r"(?m)^\s*" + re.escape(suffix), unit["target"]))
            if not any(owns_location(n) for n in unit["member_numbers"]):
                raise ValueError("单元编号 " + location + " 不属于本次承载；请使用 " + unit["number"] +
                                 "，或拼接正文中真实的局部标记，例如 " + unit["number"] + "（1）；不得把局部（1）转换成新的数字子编号")
            if props.get("上下文依据") and not supported(props["上下文依据"], unit["context"]):
                raise ValueError("Unsupported inherited evidence")
            if unit["mode"] == "context" and not props.get("上下文依据"):
                raise ValueError("Context-dependent unit must identify inherited evidence")
            if unit["mode"] == "group" and re.search(r"同时满足|以下全部|下列全部", unit["target"]):
                if location != unit["number"] and not location.startswith(unit["number"] + "第"):
                    raise ValueError("Grouped conjunctive members cannot create independent semantic units")
                if compact(props["原文依据"]) != compact(unit["target"]) or not re.search(
                    r"同时|全部|均|且", props["条款单元内容"] + str(props.get("适用前提", ""))):
                    raise ValueError("Grouped conjunctive evidence or all-conditions logic lost")
            if props.get("功能类型", "") not in {"禁止", "必要", "可选", ""}:
                raise ValueError("Invalid functional direction")
            if not props.get("功能类型") and not props.get("其他信息"):
                raise ValueError("Nonconstraint unit must explain its meaning")
            condition = normalize_quant_condition(props.get("量化条件"), context=unit["target"] + "\n" + unit["context"])
            if condition is not None and not isinstance(condition, dict):
                raise ValueError("Quantitative condition must be one structured object or null")
            if condition and not supported(condition.get("原文件"), unit["target"] + "\n" + unit["context"]):
                raise ValueError("Unsupported quantitative evidence")
            if condition and condition.get("量化值类型") == "期限":
                day_types = set(re.findall(r"(?:工作|交易|自然)日", condition.get("原文件", "")))
                if len(day_types) == 1 and condition.get("单位") not in day_types:
                    raise ValueError("期限单位必须保留原文明示的" + next(iter(day_types)) + "，不能简化为日或换成其他日类型")
            expected = "定量" if condition else "定性"
            if props.get("量化特征", expected) != expected:
                raise ValueError("Quantitative feature inconsistent with condition")
            props["量化特征"], props["量化条件"] = expected, condition
        if kind == "引用依据":
            quote = props.get("引用原文", "")
            if not supported(quote, unit["target"]):
                raise ValueError("Reference without explicit target evidence")
            if props.get("条款编号") and compact(props["条款编号"]) not in compact(quote):
                raise ValueError("Reference number is not present in quote")
            if props.get("文件全称") and compact(props["文件全称"]) not in compact(quote):
                raise ValueError("Reference document name expanded without source")
            if props.get("引用类型") not in {"文件", "条款"}:
                raise ValueError("Invalid reference type")
            if props.get("是否本文件内引用") not in {"是", "否"}:
                raise ValueError("Reference scope not specified")
    required = "法规文件" if unit is None else "法条"
    if sum(e["entity_type"] == required for e in entities) != 1:
        raise ValueError("Expected exactly one " + required)
    for relation in relations:
        a, b, kind = relation["source"], relation["target"], relation["type"]
        # Adapter outputs may use typed names rather than the bare entity name.
        for field in ("source", "target"):
            value = relation[field]
            if value not in names:
                matches = [n for n, t in names.items() if value in {t + "_" + n, t + " " + n}]
                if len(matches) == 1:
                    relation[field] = matches[0]
        a, b = relation["source"], relation["target"]
        if (names.get(a), kind, names.get(b)) not in CONTRACT.legal_triples:
            raise ValueError("Illegal or ambiguous relation: " + str((a, kind, b)))
    if unit:
        article = next(e["name"] for e in entities if e["entity_type"] == "法条")
        for e in entities:
            if e["entity_type"] == "条款单元" and not any(r["source"] == article and r["target"] == e["name"] and r["type"] == "包含" for r in relations):
                raise ValueError("Unowned semantic unit")
            if e["entity_type"] == "引用依据" and not any(names.get(r["source"]) == "条款单元" and r["target"] == e["name"] and r["type"] == "引用" for r in relations):
                raise ValueError("Unowned citation")
    else:
        document = next(e["name"] for e in entities if e["entity_type"] == "法规文件")
        for e in entities:
            if e["entity_type"] == "法规依据" and not any(r["source"] == document and r["target"] == e["name"] and r["type"] == "依据" for r in relations):
                raise ValueError("Unowned document basis")
    return result
