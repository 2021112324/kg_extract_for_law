"""Keep real list titles, scopes and source lines, including catalogue attachments."""
from hashlib import sha256
from pathlib import Path
import re

from .io_utils import compact, load_text, stable_id

CN = "一二三四五六七八九十百零〇"
TOP = re.compile(r"^([" + CN + r"]+、)\s*(.+)$")
PAREN = re.compile(r"^([（(][" + CN + r"\d]+[）)])\s*(.+)$")
NUMBER = re.compile(r"^(\d{1,3}[.、])\s*([^\d\s].*)$")
SPACED_NUMBER = re.compile(r"^(\d{1,3}[.、])\s+(\S.*)$")
BULLET = re.compile(r"^([-*•])\s+(.+)$")
ATTACHMENT = re.compile(r"^(附件|附录|附表)\s*[A-Za-z0-9一二三四五六七八九十]*\s*[：:]?\s*(.*)$")
METADATA = re.compile(r"^(?:数据ID|来源清单名称|类别[一二]|法律类型|发布单位|文号|发布日期|生效日期|时效性|合规风险类型)[：:]")
NORM = re.compile(r"应当|必须|不得|禁止|应及时|应在|应以|需要|鼓励|支持|负责")


def clean(text):
    return re.sub(r"^\s*#{1,6}\s*", "", text.strip().lstrip("\ufeff")).replace("\u200b", "")


def title_key(text):
    return re.sub(r"[\s《》]", "", text)


def detect(text):
    for pattern, style in ((TOP, "top"), (PAREN, "paren"), (SPACED_NUMBER, "number"), (NUMBER, "number"), (BULLET, "bullet")):
        match = pattern.match(text)
        if match:
            return style, match[1], match[2]
    return None


class GuideP2StructureParser:
    def parse_file(self, path):
        return self.parse_text(load_text(path), Path(path).name, str(Path(path).resolve()))

    def parse_text(self, text, filename="memory.txt", source_path=""):
        rows, offset = [], 0
        for i, line in enumerate(text.splitlines(keepends=True), 1):
            rows.append({"line": i, "start": offset, "end": offset + len(line), "raw": line.rstrip("\r\n"), "text": clean(line)})
            offset += len(line)
        first_heading = next((i for i, row in enumerate(rows) if detect(row["text"])), len(rows))
        title = next((r["text"].strip("《》") for r in rows[:first_heading]
                      if r["text"] and not METADATA.match(r["text"]) and not ATTACHMENT.match(r["text"])
                      and not re.match(r"^[（(]?\d{4}|^目录$", r["text"])), Path(filename).stem)
        version = "\n".join(r["raw"] for r in rows[:first_heading] if re.search(r"\d{4}\s*年\s*版", r["text"]))
        formal_identity = "\n".join(r["text"] for r in rows[:first_heading] if re.match(r"^(?:文号|发布日期|生效日期)[：:]", r["text"]))
        catalogue = bool(re.search(r"目录|清单|名录", title))
        nodes, header, excluded, diagnostics, disposition = [], [], [], [], {}
        stack, current, seen = [], None, set()
        in_body, stopped = False, False

        def exclusion(row, reason):
            excluded.append({"line": row["line"], "start": row["start"], "end": row["end"], "reason": reason})
            disposition[row["line"]] = {"kind": "excluded", "reason": reason}

        def add_node(row, kind, number, node_title, rank, parent=None):
            node = {"node_id": stable_id("p2_structure", row["line"], kind, number), "kind": kind,
                    "number": number, "title": node_title, "parent_id": parent["node_id"] if parent else "",
                    "rank": rank, "children": [], "own_lines": [row], "line_start": row["line"]}
            if parent:
                parent["children"].append(node["node_id"])
            nodes.append(node)
            disposition[row["line"]] = {"kind": "node", "node_id": node["node_id"]}
            return node

        for row in rows:
            line = row["text"]
            if not line:
                disposition[row["line"]] = {"kind": "blank"}
                continue
            if stopped:
                exclusion(row, "non_target_form")
                continue
            marker = ATTACHMENT.match(line)
            if marker:
                named = marker[2].strip("《》 ")
                next_text = next((r["text"] for r in rows[row["line"]:] if r["text"]), "")
                if title_key(named) == title_key(title) or not named and title_key(next_text) == title_key(title):
                    exclusion(row, "target_attachment_pointer")
                elif in_body and re.search(r"申请表|登记表|空白|模板", named):
                    stopped = True
                    exclusion(row, "non_target_form")
                elif not in_body and not named:
                    exclusion(row, "leading_wrapper")
                else:
                    diagnostics.append({"code": "ambiguous_attachment_or_document", "line": row["line"], "severity": "error"})
                    header.append(row)
                    disposition[row["line"]] = {"kind": "unresolved"}
                continue
            if compact(line) in {"目录", "目次"} or re.search(r"[.．…·]{3,}\s*\d+$", line):
                exclusion(row, "contents_marker")
                continue
            if re.match(r"^\d+\.\d+(?:\.\d+)*\s+", line):
                diagnostics.append({"code": "numeric_rule_requires_rule_pipeline", "line": row["line"], "severity": "error"})
            found = detect(line)
            if found:
                style, number, tail = found
                rank = 1 if style == "top" or not stack else 2 if style == "paren" else 3
                if style in {"number", "bullet"}:
                    rank = 3 if any(n["kind"] == "paren" for n in stack) else 2 if any(n["kind"] == "top" for n in stack) else 1
                while stack and stack[-1]["rank"] >= rank:
                    stack.pop()
                parent = stack[-1] if stack else None
                scoped_key = (parent["node_id"] if parent else "", style, number)
                if style != "bullet" and scoped_key in seen:
                    diagnostics.append({"code": "duplicate_scoped_number", "line": row["line"], "severity": "error"})
                seen.add(scoped_key)
                current = add_node(row, style, number, tail, rank, parent)
                stack.append(current)
                in_body = True
            elif current:
                if title_key(line) == title_key(title):
                    diagnostics.append({"code": "repeated_document_title_in_body", "line": row["line"], "severity": "error"})
                current["own_lines"].append(row)
                disposition[row["line"]] = {"kind": "node", "node_id": current["node_id"]}
            else:
                header.append(row)
                disposition[row["line"]] = {"kind": "header"}
        # Clearly line-separated nominal catalogues need no invented numbering.
        if not nodes and catalogue:
            nominal = [r for r in header if r["text"] and title_key(r["text"]) != title_key(title)
                       and not METADATA.match(r["text"]) and not re.search(r"\d{4}\s*年\s*版", r["text"])]
            if nominal and all(len(r["text"]) <= 120 and not NORM.search(r["text"])
                               and not re.search(r"[。；：:]", r["text"]) for r in nominal):
                for row in nominal:
                    add_node(row, "unnumbered", "", row["text"], 1)
        # Publication paragraphs are targets as well as scoped catalogue context.
        paragraphs, paragraph = [], []
        for row in header:
            if METADATA.match(row["text"]) or title_key(row["text"]) == title_key(title) or len(row["text"]) < 40:
                if paragraph:
                    paragraphs.append(paragraph)
                    paragraph = []
                continue
            paragraph.append(row)
        if paragraph:
            paragraphs.append(paragraph)
        for paragraph in paragraphs:
            body = "\n".join(r["raw"] for r in paragraph)
            if NORM.search(body) or re.search(r"适用|根据|依据|决定|制定", body):
                node = add_node(paragraph[0], "publication", "", "发布说明", 0)
                node["own_lines"] = paragraph
                for r in paragraph:
                    disposition[r["line"]] = {"kind": "node", "node_id": node["node_id"]}
        nodes.sort(key=lambda n: n["line_start"])
        by_id = {n["node_id"]: n for n in nodes}
        for node in nodes:
            node["content"] = "\n".join(r["raw"] for r in node["own_lines"][1:])
            node["raw_text"] = "\n".join(r["raw"] for r in node["own_lines"])
            node["line_end"] = node["own_lines"][-1]["line"]
            chain, cursor = [], node
            while cursor:
                chain.append(cursor["node_id"])
                cursor = by_id.get(cursor["parent_id"])
            node["ancestors"] = list(reversed(chain[1:]))
            node["path"] = [by_id[k]["number"] + by_id[k]["title"] for k in reversed(chain)]
        if not any(n["kind"] != "publication" for n in nodes):
            diagnostics.append({"code": "no_list_structure", "severity": "error"})
        fingerprint = sha256((title + version + formal_identity + "\n".join(n["raw_text"] for n in nodes)).encode("utf-8")).hexdigest()
        return {"filename": filename, "source_path": source_path, "title": title, "version": version,
                "is_catalogue": catalogue, "document_id": stable_id("p2_document", fingerprint),
                "source_sha256": sha256(text.encode("utf-8")).hexdigest(), "body_fingerprint": fingerprint,
                "file_header": "\n".join(r["raw"] for r in header), "nodes": nodes, "excluded": excluded,
                "line_dispositions": disposition, "source_line_count": len(rows), "diagnostics": diagnostics,
                "validation": {"passed": not any(d["severity"] == "error" for d in diagnostics)}}
