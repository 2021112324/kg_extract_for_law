"""Guide-local schema and evidence checks, without guide_p1 enum coercion."""
import re
from ..guide_p1.validator import build_schema_contract, normalize_extraction_result
from ...quantitative import normalize_quant_condition
from .io_utils import compact
from .prompt.schema import (schema_for_file_info, schema_for_content, KNOWLEDGE_TYPE_VALUES,
                            CONSTRAINT_STRENGTH_VALUES, RISK_TYPE_VALUES, RESPONSIBILITY_ROLE_VALUES,
                            QUANTITATIVE_VALUE_TYPE_VALUES, QUANTITATIVE_RELATION_VALUES)

CONTRACT = build_schema_contract(schema_for_file_info, schema_for_content)
NONBEHAVIOR = {"目录归属", "范围界定"}
FORBIDDEN = re.compile(r"禁止|不得|严禁|不准")
OBLIGATION = re.compile(r"应当|必须|须|应及时|应在|应以")
EXCLUSION = re.compile(r"不包括[^。；\n（）()]*|不含[^。；\n（）()]*|除[^。；\n]*?外")
NEGATIVE_CONNECTOR = re.compile(r"未[^，,。；;\n]{1,70}?(和|与|且|并且|或(?:者)?)(?=未)")


def supported(quote, source):
    return isinstance(quote, str) and bool(compact(quote)) and compact(quote) in compact(source)


def normalize(raw):
    result = normalize_extraction_result(raw)
    names = {e["name"]: e["entity_type"] for e in result["entities"]}
    if len(names) != len(result["entities"]) or not all(names):
        raise ValueError("Entity names must be nonempty and unique")
    for entity in result["entities"]:
        kind, props = entity["entity_type"], entity["properties"]
        if kind not in CONTRACT.allowed_properties or set(props) - CONTRACT.allowed_properties[kind]:
            raise ValueError("Unknown entity or Schema property: " + kind)
    for relation in result["relations"]:
        for key in ("source", "target"):
            if relation[key] not in names:
                matches = [n for n, t in names.items() if relation[key] in (t + "_" + n, t + " " + n)]
                if len(matches) == 1:
                    relation[key] = matches[0]
        if (names.get(relation["source"]), relation["type"], names.get(relation["target"])) not in CONTRACT.legal_triples:
            raise ValueError("Illegal or unknown relationship")
    return result


def risk_check(props, explicit):
    if "合规风险类型" in props:
        risks = props["合规风险类型"]
        if not isinstance(risks, list) or any(v not in RISK_TYPE_VALUES or v not in explicit for v in risks):
            raise ValueError("Risk types must be explicit controlled string arrays")


def validate_file(raw, title, source, explicit=()):
    result = normalize(raw)
    docs = [e for e in result["entities"] if e["entity_type"] == "合规指引文件"]
    if len(docs) != 1 or compact(docs[0]["properties"].get("文件全称")) != compact(title):
        raise ValueError("Expected the actual single document title")
    for e in result["entities"]:
        if e["entity_type"] not in {"合规指引文件", "引用依据"}:
            raise ValueError("Unexpected file-level entity")
        props = e["properties"]
        risk_check(props, explicit)
        quote = props.get("证据原文") if e["entity_type"] == "合规指引文件" else props.get("引用原文")
        if not supported(quote, source):
            raise ValueError("File entity requires source evidence")
        if e["entity_type"] == "合规指引文件":
            for field in ("文号", "发布单位"):
                value = props.get(field)
                if value and (not isinstance(value, str) or any(not supported(part, source) for part in re.split(r"[;；]", value) if part.strip())):
                    raise ValueError("File metadata is not present in source: " + field)
            date = props.get("发布日期")
            if date and not supported(date, source):
                tokens = re.findall(r"(\d{4})[年./-](\d{1,2})[月./-](\d{1,2})", source)
                candidate = re.fullmatch(r"(\d{4})[年./-](\d{1,2})[月./-](\d{1,2})日?", str(date))
                if not candidate or tuple(map(int, candidate.groups())) not in {tuple(map(int, t)) for t in tokens}:
                    raise ValueError("Publication date not supported by source")
        if e["entity_type"] == "引用依据":
            if not supported(props.get("文件全称"), props["引用原文"]):
                raise ValueError("Citation name not present in evidence")
            if not any(r["source"] == docs[0]["name"] and r["target"] == e["name"] and r["type"] == "引用" for r in result["relations"]):
                raise ValueError("Unowned file citation")
    return result


def validate_batch(raw, entries, explicit=()):
    result = normalize(raw)
    targets = {u["entry_id"]: u for u in entries}
    receipts, entities, names, relations = {}, {}, {}, result["relations"]
    for e in result["entities"]:
        props, kind = e["properties"], e["entity_type"]
        key = props.get("目标条目ID")
        if key not in targets:
            raise ValueError("Unknown or missing target entry ID")
        if kind == "条目处理":
            if key in receipts or props.get("处理状态") not in {"知识", "非知识", "不确定"} or not isinstance(props.get("理由"), str) or not props["理由"].strip():
                raise ValueError("Missing, duplicate or invalid entry receipt")
            receipts[key] = props
            continue
        if kind not in {"指引知识单元", "引用依据", "责任主体", "量化目标"}:
            raise ValueError("Unexpected content entity")
        unit = targets[key]
        target = unit["target"]
        context = [c["text"] for c in unit["context"]]
        source = target + "\n" + "\n".join(context)
        risk_check(props, explicit)
        if kind == "指引知识单元":
            if props.get("知识类型") not in KNOWLEDGE_TYPE_VALUES or props.get("约束强度") not in CONSTRAINT_STRENGTH_VALUES:
                raise ValueError("Unknown knowledge type or strength")
            if not isinstance(props.get("知识内容"), str) or not props["知识内容"].strip() or not supported(props.get("原文依据"), target):
                raise ValueError("Knowledge requires target-grounded proposition and evidence")
            evidence = props.get("上下文依据", [])
            if not isinstance(evidence, list) or any(not any(supported(q, c) for c in context) for q in evidence):
                raise ValueError("Unsupported context evidence")
            if props.get("知识类型") in NONBEHAVIOR:
                if props.get("约束强度") != "不适用" or props.get("行为描述"):
                    raise ValueError("Nonbehavioral membership cannot invent a task")
                if not evidence:
                    raise ValueError("每一条目录归属/范围界定单元均须单独填写上下文依据字符串数组，至少逐字引用context中的真实目录标题，不能为空")
                if re.search(r"(?:企业|公司|生产者|经营者).{0,15}(?:应当|必须|不得|禁止)", props["知识内容"]):
                    raise ValueError("Independent enterprise duty cannot be disguised as catalogue membership")
            elif props.get("约束强度") == "不适用":
                raise ValueError("Behavioral knowledge cannot hide behind nonapplicable strength")
            if "是否知识条目候选" in props and type(props["是否知识条目候选"]) is not bool:
                raise ValueError("Knowledge candidate flag must be boolean")
            claim = props["知识内容"] + str(props.get("行为描述", ""))
            authorized = target + "\n" + "\n".join(evidence)
            if (FORBIDDEN.search(claim) or props["知识类型"] == "禁止性要求") and not FORBIDDEN.search(authorized):
                raise ValueError("Prohibition not supported by authorized source")
            if OBLIGATION.search(claim) and not OBLIGATION.search(authorized):
                raise ValueError("Obligation not supported by authorized source")
            condition = str(props.get("适用条件", ""))
            source_logic = {"or" if c.startswith("或") else "and" for c in NEGATIVE_CONNECTOR.findall(compact(authorized))}
            output_logic = {"or" if c.startswith("或") else "and" for c in NEGATIVE_CONNECTOR.findall(compact(claim + "\n" + condition))}
            if len(source_logic) == 1 and output_logic - source_logic:
                raise ValueError("知识内容/适用条件中的未A和未B、未A或未B逻辑与授权原文冲突，逐字保留原条件连接词，不得互换和/或")
            for fragment in EXCLUSION.findall(props["原文依据"] + "\n" + "\n".join(context)):
                if compact(fragment.strip("，,）) ")) not in compact(claim + condition):
                    raise ValueError("Explicit source exclusion was lost: " + fragment)
        elif kind == "引用依据":
            if not supported(props.get("引用原文"), source) or not supported(props.get("文件全称"), props.get("引用原文", "")):
                raise ValueError("Unsupported citation")
        elif kind == "责任主体":
            if props.get("责任角色") not in RESPONSIBILITY_ROLE_VALUES or not supported(props.get("责任原文"), source) or not supported(props.get("主体名称"), props.get("责任原文", "")):
                raise ValueError("Unsupported responsibility")
        else:
            quote = props.get("原始文本", "")
            if props.get("量化值类型") not in QUANTITATIVE_VALUE_TYPE_VALUES or props.get("约束关系") not in QUANTITATIVE_RELATION_VALUES:
                raise ValueError("Uncontrolled numerical type or relation")
            if not supported(quote, target) or not re.search(r"应当|必须|不得|达到|至少|不超过|以上|以下|内|超过", quote) or re.search(r"共\s*[\d一二三四五六七八九十]+\s*[类种项]", quote):
                raise ValueError("Inventory, model or ordinal is not an actual numerical target")
            condition = normalize_quant_condition({"原文件": quote, **{k: v for k, v in props.items() if k in {"量化值类型", "最小值", "最大值", "单位", "约束关系"}}}, context=target)
            if not isinstance(condition, dict):
                raise ValueError("Invalid numerical target")
            props.update({k: v for k, v in condition.items() if k != "原文件"})
        names[e["name"]] = key
        entities.setdefault(key, []).append(e)
    if set(receipts) != set(targets):
        raise ValueError("Missing entry receipts: " + str(sorted(set(targets) - set(receipts))))
    for r in relations:
        if r["source"] not in names or names.get(r["source"]) != names.get(r["target"]):
            raise ValueError("Cross-entry or control receipt relationship")
    output = []
    for key, receipt in receipts.items():
        own = entities.get(key, [])
        knowledge = [e for e in own if e["entity_type"] == "指引知识单元"]
        if bool(knowledge) != (receipt["处理状态"] == "知识") or not knowledge and own:
            raise ValueError("Receipt and semantic entities disagree")
        # If the model explicitly links the same object to a range, membership
        # must remain usable on its own rather than silently broaden that object.
        for member in knowledge:
            mp = member["properties"]
            if mp["知识类型"] != "目录归属" or not mp.get("适用对象"):
                continue
            for definition in knowledge:
                dp = definition["properties"]
                condition = dp.get("适用条件", "")
                if dp["知识类型"] == "范围界定" and compact(dp.get("适用对象")) == compact(mp["适用对象"]) and condition:
                    if compact(condition) not in compact(mp["知识内容"] + str(mp.get("适用条件", ""))):
                        raise ValueError("同一对象的目录归属遗漏了已提取的范围条件，请将原文范围同时保留在归属单元的适用条件中：" + str(condition))
        for e in own:
            if e["entity_type"] != "指引知识单元":
                required = {"引用依据": "引用", "责任主体": "由其负责", "量化目标": "设定目标"}[e["entity_type"]]
                parents = [k for k in knowledge if any(r["source"] == k["name"] and r["target"] == e["name"] and r["type"] == required for r in relations)]
                if not parents or e["entity_type"] in {"责任主体", "量化目标"} and any(k["properties"]["知识类型"] in NONBEHAVIOR for k in parents):
                    raise ValueError("Unsupported or unowned auxiliary entity")
        output.append({"entry_id": key, "decision": receipt["处理状态"], "reason": receipt["理由"],
                       "entities": own, "relations": [r for r in relations if names[r["source"]] == key]})
    return output
