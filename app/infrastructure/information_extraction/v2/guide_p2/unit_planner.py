"""A target owns its source once; only verified scopes are shared as context."""
import json
import re
from .config import GuideP2Config
from .io_utils import stable_id


class GuideP2UnitPlanner:
    def __init__(self, config=None):
        self.config = config or GuideP2Config()

    def plan(self, parsed):
        by_id = {n["node_id"]: n for n in parsed["nodes"]}
        global_nodes = [n for n in parsed["nodes"] if n["kind"] == "publication" and re.search(r"(?:本|该)(?:目录|清单)|列入(?:本|该)?(?:目录|清单)|(?:目录|清单)内", n["raw_text"])]
        units, errors, coverage, batches = [], [], [], []
        background = "\n".join(s for s in (parsed["title"], parsed["version"]) if s)
        for node in parsed["nodes"]:
            contexts = [{"source_id": "document_background", "text": background}]
            contexts.extend({"source_id": key, "text": by_id[key]["raw_text"]} for key in node["ancestors"])
            contexts.extend({"source_id": n["node_id"], "text": n["raw_text"]} for n in global_nodes if n["node_id"] != node["node_id"] and n["node_id"] not in node["ancestors"])
            target = node["raw_text"]
            identifier = stable_id("p2_entry", parsed["document_id"], node["node_id"])
            high = parsed["is_catalogue"] and node["kind"] != "publication" and not (node["children"] and target.rstrip().endswith(("：", ":")))
            high = high or bool(re.search(r"应当|必须|不得|禁止|应及时|应以|应在", target))
            unit = {"entry_id": identifier, "node_id": node["node_id"], "number": node["number"], "target": target,
                    "context": contexts, "path": node["path"], "high_signal": high,
                    "scope": node["parent_id"], "source_ranges": node["own_lines"],
                    "empty_content": not bool(node["content"]), "has_children": bool(node["children"])}
            size = len(json.dumps({k: unit[k] for k in ("entry_id", "number", "target", "context", "path")}, ensure_ascii=False))
            if len(target) > self.config.block_chars or sum(len(c["text"]) for c in contexts) > self.config.context_chars or size > self.config.block_chars + self.config.context_chars:
                errors.append({"code": "oversized_complete_entry_or_context", "entry_id": identifier})
            units.append(unit)
            coverage.append({"node_id": node["node_id"], "owner": identifier, "kind": "target", "line_start": node["line_start"], "line_end": node["line_end"]})
        for unit in units:
            key = (unit["scope"], tuple(c["source_id"] for c in unit["context"]))
            packed = {k: unit[k] for k in ("entry_id", "number", "target", "context", "path")}
            if not batches or batches[-1]["key"] != key or len(batches[-1]["entries"]) >= self.config.batch_size or len(json.dumps(batches[-1]["entries"] + [packed], ensure_ascii=False)) > self.config.block_chars + self.config.context_chars:
                batches.append({"key": key, "entries": []})
            batches[-1]["entries"].append(packed)
        return {"units": units, "batches": batches, "coverage": coverage, "errors": errors,
                "formal_plan_eligible": parsed["validation"]["passed"] and not errors,
                "counts": {"structure_nodes": len(parsed["nodes"]), "candidate_entries": len(units),
                           "empty_content_candidates": sum(u["empty_content"] for u in units),
                           "context_reuses": sum(len(u["context"])-1 for u in units), "planned_batches": len(batches),
                           "excluded_ranges": len(parsed["excluded"])}}
