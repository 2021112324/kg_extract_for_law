"""Verify optional reference explanations without changing authoritative tree text."""

from collections import Counter, defaultdict
import copy
import json

from ..common.models import RiskMapInputError
from .document_identity import normalize_document_name

EMPTY_REASONS = {"成功但当前风险无知识": "risk_column_empty", "未找到文件": "source_not_found",
                 "匹配歧义": "source_ambiguity"}
STATES = {"成功且有知识", *EMPTY_REASONS}


def text(value):
    return value.replace("\r\n", "\n").replace("\r", "\n").strip()


def strings(value):
    return isinstance(value, list) and all(isinstance(v, str) and v.strip() for v in value)


def names(value):
    if not isinstance(value, list) or any(not isinstance(v, str) for v in value):
        return None
    return {normalize_document_name(v) for v in value if normalize_document_name(v)}


def source_key(value):
    # Upstream identity serialization maps the list punctuation to an ASCII comma.
    # This is only for checking provenance keys, not a new graph filename alias rule.
    return normalize_document_name(value).replace("、", ",")


def name_evidence(row, attachment):
    details = row.get("名称匹配明细")
    if not isinstance(details, list) or not details:
        return None
    allowed = {normalize_document_name(attachment.mapped_name), *names(attachment.aliases)}
    identities, states, queried = set(), [], set()
    for item in details:
        if not isinstance(item, dict) or not isinstance(item.get("查询名称"), str):
            return None
        name = normalize_document_name(item["查询名称"])
        if name not in allowed:
            return None
        roles = set()
        if name == normalize_document_name(attachment.mapped_name):
            roles.add("主名称")
        if name in names(attachment.aliases):
            roles.add("别名")
        if not isinstance(item.get("角色"), str) or item["角色"] not in roles:
            return None
        candidates = item.get("候选来源")
        state = item.get("状态")
        if not isinstance(state, str) or state not in {"matched", "unmatched", "ambiguous"} or not isinstance(candidates, list):
            return None
        for candidate in candidates:
            if (not isinstance(candidate, list) or len(candidate) != 2
                    or any(not isinstance(v, str) or not v for v in candidate)
                    or candidate[0] != attachment.region):
                return None
            identities.add((candidate[0], source_key(candidate[1])))
        if state == "unmatched" and candidates or state == "matched" and not candidates:
            return None
        queried.add(name)
        states.append(state)
    if queried != allowed:
        return None
    return identities, states


def verify_row(a, row, sources):
    if (row.get("指标序号") != a.indicator_number or row.get("风险类型") != a.risk_type
            or row.get("地区") != a.region or row.get("文件名称") != a.mapped_name
            or not isinstance(row.get("主名称", a.mapped_name), str)
            or normalize_document_name(row.get("主名称", a.mapped_name)) != normalize_document_name(a.mapped_name)
            or row.get("原始文本") != a.original_text or names(row.get("别名", [])) != names(a.aliases)):
        return "reference_fields_mismatch"
    state = row.get("状态")
    if not isinstance(state, str) or state not in STATES:
        return "unknown_source_status"
    adopted, source_ids, file_ids = (row.get(k) for k in ("采用来源ID", "来源ID", "文件来源ID"))
    if (not strings(adopted) or not strings(source_ids) or not strings(file_ids)
            or len(set(adopted)) != len(adopted) or not set(adopted) <= set(source_ids)):
        return "invalid_source_ids"
    evidence = name_evidence(row, a)
    identity = row.get("匹配文件身份")
    if state in {"成功且有知识", "成功但当前风险无知识"}:
        if not isinstance(identity, list) or len(identity) != 2 or any(not isinstance(v, str) for v in identity):
            return "missing_file_identity"
        expected_identity = (a.region, source_key(a.identity[1]))
        if (identity[0], source_key(identity[1])) != expected_identity:
            return "file_identity_mismatch"
        if evidence is not None and (evidence[0] != {expected_identity} or "ambiguous" in evidence[1]):
            return "name_evidence_conflict"
        # Legacy rows may omit name details, but malformed supplied details are not ignored.
        if "名称匹配明细" in row and evidence is None:
            return "invalid_name_evidence"
    for sid in source_ids:
        record = sources.get(sid)
        if (not isinstance(record, dict) or record.get("来源ID") != sid
                or record.get("地区") != a.region or record.get("风险类型") != a.risk_type
                or not isinstance(record.get("文件名称"), str)
                or normalize_document_name(record["文件名称"]) != a.identity[1]
                or record.get("文件来源ID") not in file_ids
                or not isinstance(record.get("采用知识文本"), str)):
            return "source_cell_mismatch"
    values = []
    for sid in adopted:
        value = text(sources[sid]["采用知识文本"])
        if value and value not in values:
            values.append(value)
    if state == "成功且有知识":
        if not a.knowledge.strip() or not adopted or "\n\n".join(values) != text(a.knowledge):
            return "adopted_text_mismatch"
    else:
        if a.knowledge.strip() or adopted:
            return "empty_status_conflicts_with_text"
        if state == "成功但当前风险无知识":
            if not source_ids or any(text(sources[sid]["采用知识文本"]) for sid in source_ids):
                return "risk_column_empty_not_proven"
        else:
            if source_ids or file_ids or identity is not None or evidence is None:
                return "unresolved_source_not_proven"
            identities, states = evidence
            if state == "未找到文件" and (identities or any(s != "unmatched" for s in states)):
                return "source_not_found_not_proven"
            if state == "匹配歧义" and not (len(identities) > 1 or "ambiguous" in states
                                                 or identities and "unmatched" in states):
                return "source_ambiguity_not_proven"
    return None


def verify_provenance(index, provenance, diagnostics):
    if (not isinstance(provenance, dict) or not isinstance(provenance.get("来源单元格"), dict)
            or not isinstance(provenance.get("指标文件挂接"), list)):
        raise RiskMapInputError("来源明细必须包含来源单元格对象和指标文件挂接数组")
    lookup = defaultdict(list)
    for row in provenance["指标文件挂接"]:
        if not isinstance(row, dict) or any(not isinstance(row.get(k), str)
                for k in ("指标序号", "地区", "文件名称")):
            diagnostics.add("provenance_row_invalid", "来源挂接缺少合法引用键", "strong_warning")
            continue
        lookup[(row["指标序号"], row["地区"], row["文件名称"])].append(row)
    counts = Counter()
    for a in index.attachments:
        rows = lookup.get((a.indicator_number, a.region, a.mapped_name), [])
        distinct = {json.dumps(row, ensure_ascii=False, sort_keys=True) for row in rows}
        a.provenance.clear()
        if not rows:
            error = "source_entry_missing"
        elif len(distinct) > 1:
            error = "conflicting_source_entries"
        else:
            error = verify_row(a, rows[0], provenance["来源单元格"])
        a.source_verification = {"verified": error is None,
            "detail": error or "verified_current_source",
            "status": rows[0]["状态"] if error is None else "unverified"}
        if error:
            counts["unverified"] += 1
            diagnostics.add("provenance_mismatch", "来源解释未核验，主树内容保持不变", "strong_warning",
                            path=a.tree_path, detail=error)
        else:
            a.provenance.append(copy.deepcopy(rows[0]))
            counts["verified"] += 1
            if not a.knowledge.strip():
                a.source_verification["empty_reason"] = EMPTY_REASONS[rows[0]["状态"]]
    index.input_stats["provenance"] = {"enabled": True, "verified": counts["verified"],
                                       "unverified": counts["unverified"]}
    return index.input_stats["provenance"]
