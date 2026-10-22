"""Authoritative indicator string items supported by Neo4j list properties."""

import json


def number_key(number):
    return tuple(int(part) for part in number.split("."))


def encode_candidates(index, numbers):
    return [json.dumps({"指标名": index.indicators[n].name, "指标序号": n,
                        "指标描述": index.indicators[n].description}, ensure_ascii=False, separators=(",", ":"))
            for n in sorted(set(numbers), key=number_key)]


def decode_candidates(items):
    if not isinstance(items, list) or any(not isinstance(item, str) for item in items):
        raise ValueError("候选必须是JSON字符串数组")
    result, seen = [], set()
    for item in items:
        value = json.loads(item)
        if (not isinstance(value, dict) or set(value) != {"指标名", "指标序号", "指标描述"}
                or any(not isinstance(v, str) or not v.strip() for v in value.values())):
            raise ValueError("候选字段不合法")
        number = value["指标序号"]
        try:
            number_key(number)
        except ValueError as exc:
            raise ValueError("指标序号不合法") from exc
        if number in seen:
            raise ValueError("候选指标序号重复")
        seen.add(number)
        result.append(value)
    return result
