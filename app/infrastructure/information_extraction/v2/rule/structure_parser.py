"""Deterministic mixed hierarchy parser; never renumbers source provisions."""
from hashlib import sha256
from pathlib import Path
import re

from .io_utils import compact, load_text, stable_id


CHAPTER = re.compile(r"^第([零〇一二三四五六七八九十百千万\d]+)(章|节)\s*(.+)$")
NUMBER = re.compile(r"^(\d+(?:[.．]\d+)*)(?:[、．.]?\s+)(\S.*)$")
MARKER = re.compile(r"^(附件|附录|附表)(?:\s*[A-Za-z0-9一二三四五六七八九十]*)?(?:[：:].*)?$")
SIGNAL = re.compile(r"应当|必须|不得|禁止|可以|有权|应(?!急)|须|负责|是指|适用|包括|按照|规定|需要|不得|：|[。；！？]")


def clean_line(line):
    return re.sub(r"^\s*#{1,6}\s*", "", line.strip().lstrip("\ufeff")).replace("\u200b", "")


def substantive(text):
    return bool(SIGNAL.search(text)) or len(compact(text)) > 48


def detect(line):
    m = CHAPTER.match(line)
    if m:
        return ("chapter" if m[2] == "章" else "section", "第" + m[1] + m[2], m[3])
    m = NUMBER.match(line)
    if not m:
        return None
    number, rest = m.groups()
    if len(number.split(".")[0]) >= 4 or re.match(r"^[%‰年月日元万亿倍]|^号(?:\b|发布|公布|印发)", rest):
        return None
    if re.fullmatch(r"\d+", number) and re.match(r"^\d|^[%‰年月日元]", rest):
        return None
    return "numeric", number.replace("．", "."), rest


class RuleStructureParser:
    def parse_file(self, path):
        return self.parse_text(load_text(path), Path(path).name, str(Path(path).resolve()))

    def parse_text(self, text, filename="memory.txt", source_path=""):
        raw_lines = text.splitlines(keepends=True)
        records, offset = [], 0
        for i, raw in enumerate(raw_lines, 1):
            records.append({"line": i, "start": offset, "end": offset + len(raw),
                            "raw": raw.rstrip("\r\n"), "text": clean_line(raw)})
            offset += len(raw)
        nodes, excluded, header, diagnostics = [], [], [], []
        chapter = section = current = None
        numeric = {}
        dispositions = {}
        started = False
        stopped = False
        contents_candidate = False
        title_candidates = []

        def exclude(row, reason):
            excluded.append({"line_start": row["line"], "line_end": row["line"], "reason": reason,
                             "start": row["start"], "end": row["end"]})
            dispositions[row["line"]] = {"kind": "excluded", "reason": reason}

        for row in records:
            line = row["text"]
            if not line:
                dispositions[row["line"]] = {"kind": "blank"}
                continue
            if stopped:
                exclude(row, "trailing_attachment")
                continue
            if MARKER.fullmatch(line):
                if not started:
                    exclude(row, "leading_attachment_wrapper")
                else:
                    stopped = True
                    exclude(row, "trailing_attachment")
                continue
            if compact(line) in {"目录", "目次"}:
                exclude(row, "contents_label_only")
                contents_candidate = True
                continue
            if re.search(r"[.．…·]{3,}\s*\d+\s*$", line):
                exclude(row, "confirmed_contents_entry")
                continue
            if re.fullmatch(r"[—-]\s*\d+\s*[—-]", line):
                exclude(row, "page_footer")
                continue
            found = detect(line)
            if found and found[0] == "numeric" and current and current["kind"] == "numeric":
                # A local 1. item under 2.1 is not a new root provision.
                if re.match(r"^\d+[.、]\s*", line) and "." not in found[1] and "." in current["number"]:
                    found = None
            if found:
                kind, number, tail = found
                # A repeated short contents heading before a later body entry is excluded.
                later = records[row["line"]:]
                if contents_candidate and not substantive(tail) and any(
                    (d := detect(r["text"])) and d[:2] == (kind, number) and
                    (d[2] == tail or substantive(d[2])) for r in later
                ):
                    exclude(row, "repeated_contents_heading")
                    continue
                if substantive(tail):
                    contents_candidate = False
                started = True
                parent = None
                if kind == "chapter":
                    chapter = section = None
                elif kind == "section":
                    parent = chapter
                    section = None
                else:
                    pieces = number.split(".")
                    chapter_key = chapter["node_id"] if chapter else "body"
                    for n in range(len(pieces)-1, 0, -1):
                        ancestor = numeric.get((chapter_key, ".".join(pieces[:n])))
                        if ancestor:
                            parent = ancestor
                            break
                    parent = parent or section or chapter
                    if parent and parent["kind"] == "numeric" and section and parent.get("section_node_id") != section["node_id"]:
                        diagnostics.append({"code": "scope_conflict", "line": row["line"], "number": number, "severity": "error"})
                    if any(str(int(p)) != p for p in pieces):
                        diagnostics.append({"code": "leading_zero", "line": row["line"], "number": number, "severity": "warning"})
                    key = (chapter_key, number)
                    if key in numeric:
                        diagnostics.append({"code": "duplicate_number", "line": row["line"], "number": number, "severity": "error"})
                    elif len(pieces) > 1 and not parent:
                        diagnostics.append({"code": "missing_parent", "line": row["line"], "number": number, "severity": "warning"})
                node_id = stable_id("rule_structure", chapter["number"] if chapter else "body", kind, number, row["line"])
                current = {"node_id": node_id, "kind": kind, "number": number, "raw_number": line[:len(number)],
                           "title": tail if not substantive(tail) else "", "inline_text": tail,
                           "parent_id": parent["node_id"] if parent else "", "children": [], "own_lines": [row],
                           "chapter": chapter["number"] + " " + chapter["inline_text"] if chapter else "",
                           "section": section["number"] + " " + section["inline_text"] if section else "",
                           "section_node_id": section["node_id"] if section else "",
                           "scope": chapter["number"] if chapter else "body"}
                if parent:
                    parent["children"].append(node_id)
                nodes.append(current)
                dispositions[row["line"]] = {"kind": "node", "node_id": node_id}
                if kind == "chapter":
                    chapter = current
                    current["scope"] = number
                elif kind == "section":
                    section = current
                else:
                    numeric[(chapter["node_id"] if chapter else "body", number)] = current
            elif current:
                current["own_lines"].append(row)
                dispositions[row["line"]] = {"kind": "node", "node_id": current["node_id"]}
            else:
                header.append(row["raw"])
                dispositions[row["line"]] = {"kind": "header"}
                if not re.search(r"[：:]|^第\s*\d+\s*号\s*$|^（?\d{4}年", line):
                    title_candidates.append(line)
        by_id = {n["node_id"]: n for n in nodes}
        for node in nodes:
            node["own_text"] = "\n".join(r["raw"] for r in node["own_lines"])
            node["body_text"] = node["inline_text"] + "\n" + "\n".join(r["raw"] for r in node["own_lines"][1:])
            node["line_start"] = node["own_lines"][0]["line"]
            node["line_end"] = node["own_lines"][-1]["line"]
            chain, ancestor = [], node
            while ancestor:
                chain.append(ancestor["number"] + " " + (ancestor["title"] or ancestor["inline_text"][:80]))
                ancestor = by_id.get(ancestor["parent_id"])
            node["path"] = list(reversed(chain))
        body_fingerprint = sha256(compact("\n".join(n["own_text"] for n in nodes)).encode("utf-8")).hexdigest()
        title = "".join(title_candidates) if title_candidates else Path(filename).stem
        if not any(n["kind"] == "numeric" for n in nodes):
            diagnostics.append({"code": "no_numeric_body", "severity": "error"})
        return {"filename": filename, "source_path": source_path, "title": title, "file_header": "\n".join(header),
                "document_id": stable_id("rule_document", body_fingerprint), "source_sha256": sha256(text.encode("utf-8")).hexdigest(),
                "body_fingerprint": body_fingerprint, "nodes": nodes, "excluded": excluded,
                "line_dispositions": dispositions, "diagnostics": diagnostics,
                "validation": {"passed": not any(d["severity"] == "error" for d in diagnostics)},
                "source_line_count": len(records)}
