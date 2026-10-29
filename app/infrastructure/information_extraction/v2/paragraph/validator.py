"""Schema-derived structural checks plus paragraph-local provenance semantics."""
import math
import re

from ..guide_p1.validator import build_schema_contract, normalize_extraction_result, to_plain
from ...quantitative import normalize_quant_condition, build_quant_condition_from_text, QUANTITY, LOWER, UPPER
from .io_utils import compact, supported, quote_range
from .prompt.schema import FILE_SCHEMA, CONTENT_SCHEMA, STRUCTURE_SCHEMA, SOURCE_TYPES, KNOWLEDGE_TYPES, DIRECTIONS, INDUSTRIES, RISK_TYPE_VALUES

CONTRACT = build_schema_contract(FILE_SCHEMA, CONTENT_SCHEMA, STRUCTURE_SCHEMA)
OBLIGATION = re.compile(r"应当|必须|须|应及时|应在|应保存|应建立|应提交|应落实|应履行|要求.{0,40}(?:要|建立|落实|保存|履行|承担|纳入)|(?:^|[，,。；;：:\s])要[\u4e00-\u9fff]")
PROHIBITION = re.compile(r"不得|禁止|严禁|不准")
PERMISSION = re.compile(r"可以|有权|可依法|允许")
EXCEPTION = re.compile(r"(?:除[^。；\n]{1,80}?外|不包括[^。；\n]{1,80}|不适用[^。；\n]{1,80}|但[^。；\n]{1,80}?除外)")
REPORTING = re.compile(r"《[^》]+》.{0,12}(?:指出|要求|强调|规定|提出)|(?:认为|表示|指出|建议|报道|强调|要求)")
FORMAL_TYPES = ("法律", "行政法规", "地方性法规", "规章", "规范性文件", "规定", "办法", "条例", "通知", "意见", "指引")
QUANTITY_KEYS = {"原文件", "量化值类型", "最小值", "最大值", "单位", "约束关系"}


class ReviewRequired(ValueError):
    """Semantic uncertainty is not a transport failure or a successful empty."""


def normalize(raw):
    plain = to_plain(raw)
    envelope = plain.get("extraction", plain) if isinstance(plain, dict) else None
    if not isinstance(envelope, dict) or not isinstance(envelope.get("entities"), list):
        raise ValueError("Expected an entities list, not an empty/malformed response")
    if any(not isinstance(e, dict) for e in envelope["entities"]):
        raise ValueError("Malformed entity")
    if "relations" in envelope and (not isinstance(envelope["relations"], list)
            or any(not isinstance(r, dict) for r in envelope["relations"])):
        raise ValueError("Malformed relations")
    if any(not isinstance(e.get("properties", e.get("attributes", {})), dict) for e in envelope["entities"]):
        raise ValueError("Entity properties must be objects")
    result = normalize_extraction_result(plain)
    names = {e["name"]: e["entity_type"] for e in result["entities"]}
    if not all(names) or len(names) != len(result["entities"]):
        raise ValueError("Entity names must be nonempty and unambiguous")
    for entity in result["entities"]:
        kind = entity["entity_type"]
        if kind not in CONTRACT.allowed_properties:
            raise ValueError("Unknown entity type: " + kind)
        extra = set(entity["properties"]) - CONTRACT.allowed_properties[kind]
        if extra:
            raise ValueError("Unknown Schema properties: " + ",".join(sorted(extra)))
    for relation in result["relations"]:
        for key in ("source", "target"):
            if relation[key] not in names:
                matches = [name for name, kind in names.items() if relation[key] in {kind+"_"+name, kind+" "+name}]
                if len(matches) == 1:
                    relation[key] = matches[0]
        if (names.get(relation["source"]), relation["type"], names.get(relation["target"])) not in CONTRACT.legal_triples:
            raise ValueError("Illegal relationship endpoints or type")
        if set(relation.get("properties", {})) - {"主体", "谓词", "客体", "source", "type", "target"}:
            raise ValueError("Uncontrolled relation properties")
    return result


def _string(props, key, required=False):
    value = props.get(key)
    if value is not None and not isinstance(value, str):
        raise ValueError(key + " must be a string or null")
    if required and not compact(value):
        raise ValueError(key + " is required")
    return value


def _reference(entity, source):
    props = entity["properties"]
    if not supported(props.get("引用原文"), source) or not name_supported(props.get("文件名称"), props.get("引用原文", "")):
        raise ValueError("Reference requires an explicit source name and quote")
    for key in ("文件别名", "原始定位"):
        value = _string(props, key)
        if value and not supported(value, source):
            raise ValueError("Invented reference property: " + key)


def name_supported(name, source):
    if not isinstance(name, str) or not name.strip():
        return False
    return compact(name).replace("《", "").replace("》", "") in compact(source).replace("《", "").replace("》", "")


def behavior_supported(behavior, source, quantity=None):
    if supported(behavior, source):
        return True
    if isinstance(quantity, dict):
        raw = quantity.get("原文件")
        if isinstance(raw, str) and raw and raw in source and isinstance(build_quant_condition_from_text(raw, context=source), dict):
            # A verified quantity can be separate from its action, without inventing words.
            matches = list(QUANTITY.finditer(raw))
            if matches:
                start, end = matches[0].start(), matches[-1].end()
                prefix = re.search(rf"(?:{LOWER}|{UPPER})\s*$", raw[:start])
                suffix = re.match(rf"\s*(?:(?:{LOWER}|{UPPER})\s*)?(?:的)?", raw[end:])
                start = prefix.start() if prefix else start
                end += suffix.end() if suffix else 0
                matched = raw+"的" if end == len(raw) and raw+"的" in source else raw
                if behavior_supported(behavior, source.replace(matched, raw[:start]+raw[end:])):
                    return True
    # List punctuation may change, but every action must still occur in order.
    parts = [p.strip() for p in re.split(r"[、；;，,\n]+", behavior or "") if p.strip()]
    if len(parts) < 2:
        return False
    # Soft list punctuation does not change action words or their ordering.
    cursor, text = 0, re.sub(r"[、；;，,]", "", compact(source))
    for part in parts:
        token = re.sub(r"[、；;，,]", "", compact(part))
        found = text.find(token, cursor)
        if found < 0:
            return False
        cursor = found + len(token)
    return True


def validate_file(raw, parsed, payload):
    result = normalize(raw)
    docs = [e for e in result["entities"] if e["entity_type"] == "来源文件"]
    if len(docs) != 1:
        raise ValueError("Expected one actual source material, not the reported policy")
    document = docs[0]
    props, source = document["properties"], payload["source"]
    if compact(props.get("文件名称")) != compact(parsed["title"]):
        raise ValueError("Source material title does not match input title")
    if props.get("来源性质") not in SOURCE_TYPES:
        raise ValueError("Unknown source nature")
    if not supported(props.get("证据原文"), source):
        raise ValueError("Source nature requires source evidence")
    news = parsed["source_signals"]
    if news and props["来源性质"] != "新闻报道":
        raise ReviewRequired("Explicit news attribution conflicts with source classification")
    for key in ("文件类型", "来源机构", "发布日期", "文号", "适用范围"):
        _string(props, key)
    if props.get("来源机构") and not supported(props["来源机构"], source):
        raise ValueError("Source institution is not grounded")
    known_news = {n["institution"] for n in news if n["institution"]}
    if known_news and props.get("来源机构") and props["来源机构"] not in known_news:
        raise ReviewRequired("News institution cannot be replaced by the reported policy issuer")
    if props.get("文号") and (props["来源性质"] == "新闻报道" or not supported(props["文号"], source)):
        raise ValueError("A reported policy number is not the current news document number")
    date = props.get("发布日期")
    if date and not supported(date, source):
        dates = re.findall(r"(\d{4})[年./-](\d{1,2})[月./-](\d{1,2})", source)
        candidate = re.fullmatch(r"(\d{4})[年./-](\d{1,2})[月./-](\d{1,2})日?", date)
        if not candidate or tuple(map(int, candidate.groups())) not in {tuple(map(int, d)) for d in dates}:
            raise ValueError("Publication date/year is not in source")
    if props["来源性质"] == "正式文件" and not re.search(r"印发|发布|通知|本.{0,15}(?:适用|规定|指引)|应当|不得|必须", source):
        raise ReviewRequired("Formal source lacks positive document evidence")
    if props["来源性质"] == "未知":
        raise ReviewRequired("Unknown source nature needs review")
    for entity in result["entities"]:
        if entity is not document:
            if entity["entity_type"] != "引用依据":
                raise ValueError("Unexpected file-level entity")
            if "目标ID" in entity["properties"]:
                raise ValueError("File-level reference must not contain a target execution ID")
            _reference(entity, source)
            if not any(r["source"] == document["name"] and r["target"] == entity["name"] for r in result["relations"]):
                raise ValueError("Unowned source reference")
    return result


def file_properties(file_result):
    return next(e["properties"] for e in file_result["entities"] if e["entity_type"] == "来源文件")


def _arrays(props, source, explicit):
    for key in ("合规风险类型", "合规域", "经济行业"):
        value = props.setdefault(key, [])
        if not isinstance(value, list) or any(not isinstance(v, str) or not v.strip() for v in value) or len(set(value)) != len(value):
            raise ValueError(key + " must be a unique string array")
    if any(r not in RISK_TYPE_VALUES or r not in explicit for r in props["合规风险类型"]):
        raise ValueError("Risk type is not one of the explicitly supplied six categories")
    sectors = props["经济行业"]
    if any(v not in INDUSTRIES for v in sectors) or "通用" in sectors and len(sectors) > 1:
        raise ValueError("Economic industry must use the controlled category level")
    unsupported = [v for v in props["合规域"] if not supported(v, source)]
    if unsupported:
        raise ValueError("合规域必须由本条原文依据或已声明的上下文依据支持；不能概括生成原文没有的分类词，不明确填[]。当前不支持值=" + ",".join(unsupported))
    if sectors == ["通用"] and not re.search(r"各类企业|所有企业|各行业|通用|所有行业|普遍适用", source):
        raise ReviewRequired("General industry applicability is not established; use [] when unknown")


def _quantity_shape(value):
    if value is None:
        return
    if isinstance(value, list):
        raise ReviewRequired("Multiple quantitative objects require semantic review, not interval compression")
    if not isinstance(value, dict) or set(value) != QUANTITY_KEYS:
        raise ValueError("量化条件必须为null或仅含六个键的对象：原文件、量化值类型、最小值、最大值、单位、约束关系；禁止对象等额外键。原文件填写量化原文，不是文件名；约束关系填写区间/上限/下限/等于。")


def _quantitative(props, source):
    value = props.setdefault("量化条件", None)
    _quantity_shape(value)
    if value is None:
        if props["知识类型"] == "规范要求" and props.get("功能类型") and QUANTITY.search(source):
            if isinstance(build_quant_condition_from_text(source, context=source), dict):
                raise ValueError("Explicit normative numerical condition was omitted")
            raise ReviewRequired("Complex normative numerical content requires review, not a qualitative fallback")
        if props.get("量化特征", "定性") != "定性":
            raise ValueError("Quantitative classification requires a structured condition")
        props["量化特征"] = "定性"
        return
    if props["知识类型"] != "规范要求" or props.get("功能类型") is None:
        raise ReviewRequired("Non-normative dates/statistics cannot become compliance thresholds")
    quote = value["原文件"]
    if not supported(quote, source):
        raise ValueError("Numerical raw text is outside authorized source")
    if re.search(r"分别|或者|否则|(?:；|;).*(?:元|%|日|次|倍)|但", quote):
        raise ReviewRequired("Complex quantitative branches must not collapse into one interval")
    expected = build_quant_condition_from_text(quote, context=source)
    normalized = normalize_quant_condition(value, context=source)
    if not isinstance(expected, dict) or not isinstance(normalized, dict):
        raise ReviewRequired("Quantity cannot be reliably normalized; preserve raw text for review")
    for key in ("最小值", "最大值", "单位", "约束关系", "量化值类型"):
        if normalized.get(key) != expected.get(key):
            raise ValueError("Quantitative value/unit/relation disagrees with source: " + key)
    for key in ("最小值", "最大值"):
        value = normalized.get(key)
        if value is not None and (isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value)):
            raise ValueError("Invalid quantitative bound")
    if props.get("量化特征", "定量") != "定量":
        raise ValueError("Nonempty quantitative condition requires 定量")
    props.update(量化条件=normalized, 量化特征="定量")


def validate_content(raw, unit, source_props, explicit=()):
    result = normalize(raw)
    receipts = [e for e in result["entities"] if e["entity_type"] == "段落处理"]
    if len(receipts) != 1:
        raise ValueError("Each target requires exactly one processing receipt")
    receipt = receipts[0]["properties"]
    for entity in result["entities"]:
        if entity["properties"].get("目标ID") != unit["target_id"]:
            raise ValueError("Unknown or missing target ID")
        if entity["entity_type"] not in {"段落处理", "段落知识单元", "引用依据"}:
            raise ValueError("Unexpected target-level entity")
    decision = receipt.get("处理结论")
    if decision not in {"知识", "非知识", "不确定"} or not isinstance(receipt.get("理由"), str) or not compact(receipt.get("理由")):
        raise ValueError("Invalid receipt decision or reason")
    entities = [e for e in result["entities"] if e not in receipts]
    claims = [e for e in entities if e["entity_type"] == "段落知识单元"]
    if decision != "知识":
        if entities or result["relations"]:
            raise ValueError("A nonknowledge/uncertain receipt must not carry semantic entities")
        return {"target_id": unit["target_id"], "decision": decision, "reason": receipt["理由"], "entities": [], "relations": []}
    if not claims:
        raise ValueError("Knowledge decision requires at least one grounded proposition")
    source = unit["target"] + "\n" + "\n".join(c["text"] for c in unit["context"])
    ranges, normalizations = {}, []
    for entity in entities:
        props = entity["properties"]
        if entity["entity_type"] == "引用依据":
            _reference(entity, source)
            if not any(r["target"] == entity["name"] and r["source"] in {c["name"] for c in claims} for r in result["relations"]):
                raise ValueError("Unowned target reference")
            continue
        kind = props.get("知识类型")
        if kind not in KNOWLEDGE_TYPES:
            raise ValueError("Uncontrolled knowledge type")
        claim = _string(props, "知识内容", True)
        _quantity_shape(props.get("量化条件"))
        publication_only = kind == "事实陈述" and re.fullmatch(
            r".{0,80}?(?:印发|发布|公布|颁布|转发)(?:了)?《[^》]+》(?:[（(][^）)]{0,80}[）)])?[。.!！]?", compact(claim))
        generic_notice = (kind == "规范要求" and source_props["来源性质"] == "新闻报道"
            and re.search(r"印发|发布|转发", unit["target"])
            and re.fullmatch(r"(?:结合实际)?(?:认真|及时|严格|全面)?贯彻(?:执行|落实)", compact(props.get("行为描述"))))
        scope_only = (re.fullmatch(r"(?:本|该)[^。！？；;，,]{0,24}?适用于[^。！？；;，,]{1,160}[。.!?]?", compact(claim))
                      and not (OBLIGATION.search(claim) or PROHIBITION.search(claim) or PERMISSION.search(claim)))
        if publication_only or generic_notice or scope_only:
            raise ValueError("发布文件、单纯适用范围或笼统贯彻落实属于来源元信息，不单列知识。删除此类知识实体；若目标仅含背景或范围说明，回执非知识，若另有具体知识则保留具体内容。范围仍供其他主命题作上下文。")
        quote = _string(props, "原文依据", True)
        position = quote_range(quote, unit["target"], unit["start"])
        contexts = props.setdefault("上下文依据", [])
        if not isinstance(contexts, list) or any(not isinstance(q, str) for q in contexts):
            raise ValueError("Context evidence must be a string array")
        extra_ranges = []
        for q in contexts:
            candidates = [{"text": unit["target"], "start": unit["start"]}, *unit["context"]]
            matches = [c for c in candidates if supported(q, c["text"])]
            if not matches:
                raise ValueError("上下文依据必须逐字来自当前target或授权context，不得自行补写。当前值=" + q[:120])
            c = matches[0]
            extra_ranges.append(quote_range(q, c["text"], c["start"]))
        authorized = quote + "\n" + "\n".join(contexts)
        _arrays(props, authorized, explicit)
        for key in ("适用主体", "行为描述", "适用前提", "例外情形", "归属说明"):
            _string(props, key)
        subject = props.get("适用主体")
        if subject and not supported(subject, authorized):
            raise ValueError("适用主体必须在原文依据或上下文依据中可定位；若来自target其他句，须将该句补入上下文依据。目标及context均未明确时填null，不猜测或加注释。当前值=" + subject[:100])
        behavior = props.get("行为描述")
        if kind == "规范要求" and not behavior:
            raise ValueError("Normative knowledge requires an explicit source-grounded behavior")
        if behavior and not behavior_supported(behavior, quote, props.get("量化条件")):
            if behavior_supported(behavior, source) and not behavior_supported(behavior, unit["target"]):
                raise ReviewRequired("Main behavior occurs only in context; primary ownership is uncertain")
            raise ValueError("行为描述必须由当前目标原文依据覆盖，可按原顺序用顿号列举原文短语；多个事项请拆成多条知识或扩大原文依据。当前行为=" + behavior[:120])
        direction = props.setdefault("功能类型", None)
        if direction is not None and (direction not in DIRECTIONS or kind != "规范要求"):
            raise ValueError("Only a normative requirement can carry a controlled direction")
        for value, pattern in (("禁止", PROHIBITION), ("必要", OBLIGATION), ("可选", PERMISSION)):
            if direction == value and not pattern.search(authorized):
                props["功能类型"] = direction = None
                normalizations.append({"entity": entity["name"], "field": "功能类型", "original": value,
                    "normalized": None, "reason": "原文未明确支持约束方向；保留原文知识，降为参考，不输出规范规则"})
        if PROHIBITION.search(claim) and not PROHIBITION.search(authorized):
            raise ValueError("Invented prohibition")
        if OBLIGATION.search(claim) and not OBLIGATION.search(authorized):
            raise ValueError("知识内容新增了原文没有的义务用语；不得把普通陈述改写为应当或必须，保留原文语气，无法确认功能类型填null。")
        mandatory_source = unit["target"] + "\n" + "\n".join(c["text"] for c in unit["context"] if c["role"] in {"scope", "exception"}) + "\n" + "\n".join(contexts)
        for exception in EXCEPTION.findall(mandatory_source):
            if compact(exception) not in compact(claim + str(props.get("适用前提", "")) + str(props.get("例外情形", ""))):
                raise ReviewRequired("An explicit neighboring exception was lost: " + exception)
        attribute = props.get("归属说明")
        if (attribute and source_props["来源性质"] == "正式文件" and
                compact(attribute).strip("《》") == compact(source_props["文件名称"]).strip("《》") and
                not name_supported(attribute, quote) and not REPORTING.search(quote)):
            normalizations.append({"entity": entity["name"], "field": "归属说明", "original": attribute,
                "normalized": None, "reason": "当前正式来源不是被转述对象；来源身份由来源文件节点保留"})
            props["归属说明"] = attribute = None
        if attribute and (not name_supported(attribute, authorized) or not name_supported(attribute, claim)):
            raise ValueError("归属说明必须同时出现在知识内容和已声明的原文依据/上下文依据中；若归属位于当前target段首，须将段首原话补入上下文依据。当前归属=" + attribute[:120])
        needs_attribution = kind == "观点转述" or source_props["来源性质"] in {"新闻报道", "解读材料"} and kind in {"规范要求", "指导建议"}
        if needs_attribution and (not attribute or not name_supported(attribute, authorized) or not name_supported(attribute, claim)):
            raise ValueError("Reported knowledge requires explicit source attribution in the claim")
        if REPORTING.search(unit["target"]) and re.search(r"《[^》]+》", unit["target"]) and kind == "规范要求" and not attribute:
            raise ValueError("Quoted policy requirement must retain its document attribution")
        usage = "规范知识" if source_props["来源性质"] == "正式文件" and kind == "规范要求" and direction and not attribute else "参考知识"
        if props.get("使用定位") not in (None, usage):
            raise ValueError("Reference knowledge cannot be upgraded to a formal rule")
        props["使用定位"] = usage
        original_quantity = to_plain(props.get("量化条件"))
        _quantitative(props, authorized)
        if original_quantity != props.get("量化条件"):
            normalizations.append({"entity": entity["name"], "field": "量化条件", "original": original_quantity,
                "normalized": props.get("量化条件"), "reason": "按明确原文及公共量化规则规范化；原始模型响应另存审计"})
        if compact(claim) == compact(props.get("适用主体")):
            raise ValueError("Keyword or incomplete fragment is not a reusable proposition")
        ranges[entity["name"]] = {"target": position, "context": extra_ranges}
    claim_names = {c["name"] for c in claims}
    if any(r["source"] not in claim_names for r in result["relations"]):
        raise ValueError("Only knowledge claims may own target references")
    return {"target_id": unit["target_id"], "decision": decision, "reason": receipt["理由"],
            "entities": entities, "relations": result["relations"], "evidence_ranges": ranges, "normalizations": normalizations}


def interface_eligibility(props, source_props):
    reasons = []
    if props.get("使用定位") != "规范知识":
        reasons.append("reference_not_formal_rule")
    if source_props.get("文件类型") not in FORMAL_TYPES:
        reasons.append("unconfirmed_legal_document_type")
    if not props.get("经济行业"):
        reasons.append("unknown_economic_industry")
    if props.get("功能类型") not in DIRECTIONS:
        reasons.append("unknown_direction")
    return {"eligible": not reasons, "reasons": reasons}
