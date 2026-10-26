"""Conservative legal quantity normalization, not a general calculation engine."""

import copy
from decimal import Decimal, InvalidOperation, localcontext
import json
import re


# family, canonical unit, multiplier; shared by code and prompt guidance.
UNITS = {
    "年": ("calendar", "月", Decimal(12)), "个月": ("calendar", "月", Decimal(1)),
    "月": ("calendar", "月", Decimal(1)), "万元": ("money", "元", Decimal(10000)),
    "元": ("money", "元", Decimal(1)), "%": ("ratio", "倍", Decimal("0.01")),
    "％": ("ratio", "倍", Decimal("0.01")), "倍": ("ratio", "倍", Decimal(1)),
    "日": ("day", "日", Decimal(1)), "天": ("day", "日", Decimal(1)),
    "次": ("count", "次", Decimal(1)),
}
NUMBER = r"(?:\d+(?:,\d{3})*(?:\.\d+)?|[零〇一二两三四五六七八九十百千万亿]+(?:点[零〇一二两三四五六七八九]+)?)"
QUANTITY = re.compile(rf"百分之(?P<percent>{NUMBER})|(?P<number>{NUMBER})\s*(?P<unit>{'|'.join(sorted(UNITS, key=len, reverse=True))})")
LOWER = r"以上|不少于|不低于|不得低于|超过|大于"
UPPER = r"以下|以内|之内|内|不超过|不得超过|不高于|低于|小于"
BASE = r"(?:所获收入|违法所得|销售收入|销售额|营业额|营业收入|货值金额|收入|所得|利润|金额|基数)"
FIELDS = ("原文件", "量化值类型", "最小值", "最大值", "单位", "约束关系")


class QuantitativeValidationError(ValueError):
    pass


def unit_guidance():
    year = UNITS["年"][2]
    money = UNITS["万元"][2]
    percent = UNITS["%"][2]
    return ("上下限必须采用同一单位并保留原文、计算基数和适用条件，不得摘取不同单位数字拼区间，"
            f"不得交换上下限。年/月统一为月（1年={year}个月）；万元/元统一为元（1万元={money}元）；"
            f"仅明确同一基数时百分比/倍数统一为倍（1%={percent}倍）。月/日、年/日不固定换算。"
            "忠于原文：未明确的端点可为null、单位可留空；不得猜补或将明示数值清空规避错误。"
            "不能可靠结构化时完整保留原文，不强行生成区间。")


def number(value):
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise QuantitativeValidationError("非空数值不是合法有限数字：布尔值")
    text = str(value).replace(",", "").strip()
    try:
        result = Decimal(text)
    except InvalidOperation:
        digits = {c: i for i, c in enumerate("零一二三四五六七八九")}
        digits.update({"〇": 0, "两": 2})
        units = {"十": 10, "百": 100, "千": 1000, "万": 10000, "亿": 100000000}
        integer, dot, fraction = text.partition("点")
        total = section = current = 0
        if not integer or any(c not in digits and c not in units for c in integer):
            raise QuantitativeValidationError("非空数值不能转换为合法有限数字") from None
        for c in integer:
            if c in digits:
                current = digits[c]
            elif units[c] < 10000:
                section += (current or 1) * units[c]
                current = 0
            else:
                section = (section + current) * units[c]
                total += section
                section = current = 0
        if all(c in digits for c in integer):
            total, section, current = int("".join(str(digits[c]) for c in integer)), 0, 0
        result = Decimal(total + section + current)
        if dot:
            if not fraction or any(c not in digits for c in fraction):
                raise QuantitativeValidationError("中文小数不是合法有限数字")
            result += Decimal("0." + "".join(str(digits[c]) for c in fraction))
    if not result.is_finite():
        raise QuantitativeValidationError("非空数值不是合法有限数字")
    return result


def public_number(value):
    if value is None:
        return None
    result = int(value) if value == value.to_integral_value() else float(value)
    # Reject overflow introduced by JSON-compatible numeric conversion, too.
    if not Decimal(str(result)).is_finite():
        raise QuantitativeValidationError("数值超出可序列化有限数字范围")
    return result


def _same_base(raw, first, second, context):
    first, second = sorted((first, second), key=lambda item: item.start())
    gap = raw[first.end():second.start()]
    if not re.fullmatch(r"\s*(?:以上|以下|至|到|及|且|和|的|、|，|,|\s)*", gap):
        return False
    prefix = raw[:first.start()]
    if not prefix.strip() and raw and raw in context:
        start = context.index(raw)
        prefix = context[max(0, start - 40):start]
    return bool(re.search(BASE + r"(?:的)?\s*$", prefix))


def _endpoints(raw):
    matches = list(QUANTITY.finditer(raw))
    if len(matches) != 2:
        return matches, None
    a, b = matches
    if not re.fullmatch(r"\s*(?:以上|以下|至|到|及|且|和|的|、|，|,|不超过|不少于|不低于|不得超过|\s)*", raw[a.end():b.start()]):
        return matches, None
    lower = bool(re.search(LOWER, raw[a.end():b.start()]) or re.search(rf"(?:{LOWER})\s*$", raw[:a.start()]))
    upper = bool(re.match(rf"\s*(?:{UPPER})", raw[b.end():]) or re.search(rf"(?:{UPPER})\s*$", raw[a.end():b.start()]))
    # Prefix '不超过' must not be mistaken for a lower-bound '超过'.
    if re.search(rf"(?:{UPPER})\s*$", raw[:a.start()]):
        lower = False
    return matches, (a, b) if lower and upper else None


def _token_unit(token):
    return token["unit"] or "%"


def _token_number(token):
    return number(token["number"] or token["percent"])


def _normalized(value, context):
    result = copy.deepcopy(value)
    raw = str(result.get("原文件") or result.get("原文") or result.get("raw_text") or "")
    low, high = number(result.get("最小值")), number(result.get("最大值"))
    matches, pair = _endpoints(raw)
    unit = str(result.get("单位") or "").strip()
    comparable = unit in UNITS
    if pair:
        a, b = pair
        au, bu = _token_unit(a), _token_unit(b)
        fa, ua, ma = UNITS[au]
        fb, _, mb = UNITS[bu]
        if {fa, fb} == {"calendar", "day"}:
            return result
        if fa != fb:
            raise QuantitativeValidationError("明确不同量纲单位未统一，不能作为同一区间")
        if fa == "ratio":
            if au.replace("％", "%") == bu.replace("％", "%"):
                ua, ma, mb = au.replace("％", "%"), Decimal(1), Decimal(1)
            elif not _same_base(raw, a, b, context):
                return result
        with localcontext() as ctx:
            ctx.prec = 64
            low, high = _token_number(a) * ma, _token_number(b) * mb
        result.update(最小值=public_number(low), 最大值=public_number(high), 单位=ua, 约束关系="区间")
        kind = {"calendar": "期限", "day": "期限", "money": "金额", "ratio": "比例" if ua == "%" else "倍数", "count": "次数"}[fa]
        result["量化值类型"] = kind
        comparable = True
    elif len(matches) > 1:
        # Complex/multi-branch source is not permission to merge or erase endpoints.
        return result
    elif comparable:
        family, target, multiplier = UNITS[unit]
        if family == "ratio":
            # Do not change a lone percentage into a multiple without a range need.
            target, multiplier = unit, Decimal(1)
        with localcontext() as ctx:
            ctx.prec = 64
            low = low * multiplier if low is not None else None
            high = high * multiplier if high is not None else None
        if unit != target:
            result.update(单位=target)
        for key, endpoint in (("最小值", low), ("最大值", high)):
            if result.get(key) is not None and result.get(key) != "":
                result[key] = public_number(endpoint)
    relation = result.get("约束关系")
    if low is not None and high is not None:
        if comparable and relation == "区间" and low > high:
            raise QuantitativeValidationError("单位统一后最小值大于最大值，不得交换上下限")
        if relation == "等于" and low != high:
            raise QuantitativeValidationError("等于关系的两个数值不相等")
    return result


def build_quant_condition_from_text(text, *, context=""):
    raw = str(text or "")
    if not raw.strip():
        return None
    matches, pair = _endpoints(raw)
    if pair:
        a, b = pair
        candidate = dict(zip(FIELDS, (raw, "其他", public_number(_token_number(a)),
                                      public_number(_token_number(b)), _token_unit(a), "区间")))
        converted = _normalized(candidate, context)
        # Unknown common basis or calendar/day must preserve the entire input text.
        if converted == candidate and _token_unit(a) != _token_unit(b):
            return raw
        return converted
    if len(matches) != 1:
        return raw
    match = matches[0]
    value = public_number(_token_number(match))
    around = raw[:match.start()] + " " + raw[match.end():]
    upper = bool(re.search(UPPER, around))
    lower = not upper and bool(re.search(LOWER, around))
    candidate = dict(zip(FIELDS, (raw, "其他", None if upper else value, None if lower else value,
                                  _token_unit(match), "上限" if upper else "下限" if lower else "等于")))
    family = UNITS[_token_unit(match)][0]
    candidate["量化值类型"] = {"calendar": "期限", "day": "期限", "money": "金额", "ratio": "倍数" if _token_unit(match) == "倍" else "比例", "count": "次数"}[family]
    return _normalized(candidate, context)


def normalize_quant_condition(value, *, context=""):
    if value is None:
        return None
    if isinstance(value, str):
        if not value.strip():
            return None
        if value.lstrip().startswith(("{", "[")) or value.strip() == "null":
            try:
                decoded = json.loads(value)
            except (ValueError, RecursionError):
                return value
            return normalize_quant_condition(decoded, context=context)
        return build_quant_condition_from_text(value, context=context)
    if isinstance(value, list):
        return [normalize_quant_condition(item, context=context) for item in value]
    if isinstance(value, dict):
        return _normalized(value, context)
    return copy.deepcopy(value)


def law_document_validator(filename, article_number, parent_text):
    """Return a local per-call hook; no shared state or extra model requests."""
    def validate(documents):
        errors = []
        for document in documents:
            for entity in document.get("extractions", []):
                if entity.get("extraction_class") != "条款单元":
                    continue
                props = entity.get("attributes")
                if not isinstance(props, dict) or "量化条件" not in props:
                    continue
                original = props["量化条件"]
                decoded = original
                if isinstance(original, str) and original.lstrip().startswith(("{", "[")):
                    try:
                        decoded = json.loads(original)
                    except (ValueError, RecursionError):
                        pass
                values = decoded if isinstance(decoded, list) else [decoded]
                normalized = []
                for index, value in enumerate(values, 1):
                    try:
                        normalized.append(normalize_quant_condition(value, context=parent_text))
                    except QuantitativeValidationError as exc:
                        fields = value if isinstance(value, dict) else {}
                        current = json.dumps({key: fields.get(key) for key in ("最小值", "最大值", "单位", "约束关系")},
                                             ensure_ascii=False, default=str)
                        raw = str(fields.get("原文件") or fields.get("原文") or fields.get("raw_text") or value)
                        errors.append(f"文件={filename} 法条={article_number} 实体={entity.get('extraction_text', '')} "
                                      f"量化对象{index} 原文={raw[:120]} 当前结果={current} 错误={exc}")
                if len(normalized) == len(values):
                    props["量化条件"] = normalized if isinstance(decoded, list) else normalized[0]
        if errors:
            raise QuantitativeValidationError("量化条件校验失败；修正要求：忠于原文统一单位，不得交换上下限或清空明示数值；"
                "重新输出当前抽取单元。" + "；".join(errors[:4]))
    return validate


def unit_examples():
    result = []
    for raw in ("三个月以上二年以下", "所获收入30%以上3倍以下"):
        text = "第一条 适用范围为" + raw + "。"
        result.append({"text": "量化示例法规 " + text, "nodes": [
            {"name": "第一条", "type": "法条", "attributes": {"核心主题": "量化示例"}},
            {"name": "第一条第一款", "type": "条款单元", "attributes": {
                "条款单元内容": text, "单元层级": "款", "单元编号": "第一条第一款",
                "量化条件": build_quant_condition_from_text(raw), "量化特征": "定量"}},
        ], "edges": [{"type": "包含", "attributes": {"主体": "法条_第一条", "客体": "条款单元_第一条第一款"}}]})
    return result
