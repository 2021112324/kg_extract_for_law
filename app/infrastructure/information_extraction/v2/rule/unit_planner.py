"""Article carriers are not tied to a fixed identifier depth."""
import re
from .io_utils import compact, stable_id
from .structure_parser import substantive


def descendants(node, by_id):
    result = []
    for child_id in node["children"]:
        child = by_id[child_id]
        result.append(child)
        result.extend(descendants(child, by_id))
    return result


def boundary_candidates(parsed):
    return [n for n in parsed["nodes"] if n["kind"] == "numeric" and n["children"] and
            (re.search(r"[：:]\s*$", n["body_text"]) or
             re.search(r"同时满足|以下全部|下列全部|下列条件", n["body_text"]))]


def split_text(text, limit):
    chunks = []
    offset = 0
    while len(text) - offset > limit:
        part = text[offset:offset + limit]
        boundaries = [m.end() for m in re.finditer(r"\n|[。！？；]", part)]
        if not boundaries:
            raise ValueError("oversized_sentence: no safe source boundary within block_chars")
        end = offset + boundaries[-1]
        chunks.append({"text": text[offset:end], "start": offset, "end": end})
        offset = end
    if offset < len(text):
        chunks.append({"text": text[offset:], "start": offset, "end": len(text)})
    assert "".join(c["text"] for c in chunks) == text
    return chunks


class RuleUnitPlanner:
    def __init__(self, block_chars=6000, context_chars=12000):
        self.block_chars = block_chars
        self.context_chars = context_chars

    def plan(self, parsed, decisions=None):
        decisions = decisions or {}
        by_id = {n["node_id"]: n for n in parsed["nodes"]}
        candidates = {n["node_id"] for n in boundary_candidates(parsed)}
        file_scope_nodes = [n for n in parsed["nodes"] if n["kind"] == "numeric" and re.search(
            r"本(?:规则|办法|规定|预案|指引)适用于", n["body_text"])]
        consumed, context_only, owners, units, errors = set(), set(), {}, [], []
        pending = []
        for node in parsed["nodes"]:
            key = node["node_id"]
            if key in consumed or node["kind"] != "numeric":
                continue
            decision = decisions.get(key)
            if key in candidates and not decision:
                pending.append(key)
                # The offline candidate remains conservative and cannot be written.
                decision = {"mode": "group", "member_ids": [c["node_id"] for c in descendants(node, by_id)]}
            if decision and decision["mode"] == "inherit":
                context_only.add(key)
                continue
            has_own_content = substantive(node["body_text"]) or len(node["own_lines"]) > 1
            if not has_own_content:
                continue
            members = [node]
            if decision and decision["mode"] == "group":
                allowed = {c["node_id"] for c in descendants(node, by_id) if c["kind"] == "numeric"}
                chosen = decision["member_ids"]
                if not chosen or not set(chosen) <= allowed or len(chosen) != len(set(chosen)):
                    raise ValueError("Invalid group members")
                if consumed.intersection(chosen):
                    raise ValueError("Group target ownership overlaps")
                members += [by_id[c] for c in chosen]
                members.sort(key=lambda n: n["line_start"])
            consumed.update(n["node_id"] for n in members)
            source_text = "\n".join(n["own_text"] for n in members)
            context_nodes = []
            parent = by_id.get(node["parent_id"])
            while parent:
                context_nodes.append(parent)
                parent = by_id.get(parent["parent_id"])
            context_nodes.reverse()
            parent_node_ids = [n["node_id"] for n in context_nodes]
            for scope_node in file_scope_nodes:
                if scope_node["node_id"] not in {n["node_id"] for n in members + context_nodes}:
                    context_nodes.insert(0, scope_node)
            context = "\n".join(n["own_text"] for n in context_nodes)
            if len(context) > self.context_chars:
                errors.append({"code": "context_too_long", "number": node["number"]})
            unit_id = stable_id("rule_article", parsed["document_id"], node["scope"], node["number"])
            for member in members:
                if member["node_id"] in owners:
                    raise ValueError("Duplicate target assignment")
                owners[member["node_id"]] = unit_id
            unit = {"unit_id": unit_id, "node_id": key, "number": node["number"], "scope": node["scope"],
                    "mode": "group" if len(members) > 1 else "context" if any(n["node_id"] in context_only for n in context_nodes) else "independent",
                    "member_ids": [n["node_id"] for n in members], "member_numbers": [n["number"] for n in members],
                    "text": source_text, "context": context, "path": node["path"],
                    "parent_node_ids": parent_node_ids,
                    "chapter": node["chapter"], "section": node["section"],
                    "source_ranges": [{"number": n["number"], "line_start": n["line_start"], "line_end": n["line_end"],
                                       "spans": [{"start": r["start"], "end": r["end"]} for r in n["own_lines"]]} for n in members],
                    "context_ranges": [{"node_id": n["node_id"], "line_start": n["line_start"], "line_end": n["line_end"]} for n in context_nodes]}
            try:
                unit["chunks"] = split_text(source_text, self.block_chars)
                if unit["mode"] == "group" and len(unit["chunks"]) > 1:
                    errors.append({"code": "inseparable_group_too_long", "number": node["number"]})
                    unit["chunks"] = []
            except ValueError as exc:
                errors.append({"code": "oversized_sentence", "number": node["number"], "message": str(exc)})
                unit["chunks"] = []
            units.append(unit)
        coverage = []
        for n in parsed["nodes"]:
            if n["node_id"] in owners:
                kind = "target"
            elif n["node_id"] in context_only or n["kind"] != "numeric":
                kind = "context_or_heading"
            elif not substantive(n["body_text"]) and len(n["own_lines"]) == 1:
                kind = "heading"
            else:
                kind = "unresolved"
                errors.append({"code": "unassigned_content", "number": n["number"]})
            coverage.append({"node_id": n["node_id"], "number": n["number"], "kind": kind,
                             "owner": owners.get(n["node_id"]), "line_start": n["line_start"], "line_end": n["line_end"]})
        return {"units": units, "coverage": coverage, "pending_boundaries": pending, "decisions": decisions,
                "errors": errors, "formal_plan_eligible": not pending and not errors,
                "counts": {"identifier_nodes": sum(n["kind"] == "numeric" for n in parsed["nodes"]),
                           "candidate_carriers": len(units), "confirmed_carriers": len(units) if not pending and not errors else 0,
                           "groups": sum(u["mode"] == "group" for u in units),
                           "group_members": sum(len(u["member_ids"])-1 for u in units if u["mode"] == "group"),
                           "execution_chunks": sum(len(u["chunks"]) for u in units)}}
