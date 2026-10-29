"""Preserve real paragraphs and source ranges, never manufacture legal articles."""
from hashlib import sha256
from pathlib import Path
import re

from .config import ParagraphConfig
from .io_utils import compact, stable_id

NEWS = re.compile(r"(?:新华社|中新社|中国新闻社)[^\r\n。]{0,55}(?:电|讯)|(?:记者|通讯员)[^\r\n。]{0,25}(?:报道|获悉)")
METADATA = re.compile(r"^\s*(?:来源|发布单位|发布日期|发布时间|文号|合规风险类型)\s*[:：]")
NUMBERED = re.compile(r"^\s*(?:第[零〇一二三四五六七八九十百千万\d]+[条章节]|\d+(?:\.\d+)+\s*\S|[一二三四五六七八九十]+[、.]|[（(][一二三四五六七八九十\d]+[）)])")
IMAGE = re.compile(r"^\s*(?:!\[[^\]]*\]\([^)]*\)|<img\b[^>]*>)\s*$", re.I)


class ParagraphStructureParser:
    def __init__(self, config=None):
        self.config = config or ParagraphConfig()

    def parse_file(self, path):
        path = Path(path).resolve()
        if path.suffix.lower() not in {".txt", ".md"}:
            raise ValueError("paragraph supports UTF-8 TXT and Markdown only")
        raw = path.read_bytes()
        parsed = self.parse_text(raw.decode("utf-8"), path.name, str(path))
        assert parsed["source_sha256"] == sha256(raw).hexdigest()
        return parsed

    def parse_text(self, text, filename="material.txt", source_path=""):
        if not isinstance(text, str) or not text.strip("\ufeff \r\n\t") or "\x00" in text:
            raise ValueError("Input must be nonempty UTF-8 prose without NUL bytes")
        fingerprint = sha256(text.encode("utf-8")).hexdigest()
        source_identity = source_path or filename
        document_id = stable_id("paragraph_source", source_identity, fingerprint)
        paragraphs, headings, metadata, excluded, coverage, diagnostics = [], [], [], [], [], []
        title, title_origin = Path(filename).stem, "filename"
        heading_stack, group = [], []
        lines = [match for match in re.finditer(r"[^\r\n]*(?:\r\n|\r|\n|$)", text) if match.end() > match.start()]

        def disposition(role, raw, start, end, first, last, **extra):
            value = {"role": role, "text": raw, "start": start, "end": end,
                     "line_start": first, "line_end": last, **extra}
            coverage.append(value.copy())
            return value

        def flush():
            if not group:
                return
            start, end = group[0][1].start(), group[-1][1].end()
            raw = text[start:end]
            node_id = stable_id("paragraph", document_id, start, end, raw)
            item = disposition("target", raw, start, end, group[0][0], group[-1][0], paragraph_id=node_id)
            item.update(order=len(paragraphs) + 1, heading=[h["text"] for h in heading_stack],
                        section_id=heading_stack[-1]["id"] if heading_stack else document_id)
            paragraphs.append(item)
            group.clear()

        first_nonempty = True
        fenced = False
        for line_no, line in enumerate(lines, 1):
            raw = line.group()
            clean = raw.strip().lstrip("\ufeff")
            if not clean:
                flush()
                continue
            is_title = first_nonempty and compact(clean) == compact(Path(filename).stem)
            first_nonempty = False
            heading = re.match(r"^(#{1,6})\s+(.+?)\s*#*\s*$", clean)
            if heading or is_title:
                flush()
                level = len(heading[1]) if heading else 1
                display = heading[2] if heading else clean
                value = disposition("heading", raw, line.start(), line.end(), line_no, line_no,
                                    id=stable_id("paragraph_heading", document_id, line.start()), level=level)
                value["display"] = display
                headings.append(value)
                if title_origin == "filename" and (is_title or len(headings) == 1 and not paragraphs):
                    title, title_origin = display, "source"
                while heading_stack and heading_stack[-1]["level"] >= level:
                    heading_stack.pop()
                heading_stack.append(value)
                continue
            if METADATA.match(clean):
                flush()
                metadata.append(disposition("metadata", raw, line.start(), line.end(), line_no, line_no))
                continue
            if IMAGE.fullmatch(clean):
                flush()
                excluded.append(disposition("excluded", raw, line.start(), line.end(), line_no, line_no, reason="image_markup"))
                continue
            if clean.startswith("```") or clean.startswith("~~~"):
                fenced = not fenced
            if fenced or clean.startswith(("```", "~~~")) or clean.startswith("|") and clean.count("|") >= 2:
                flush()
                excluded.append(disposition("needs_review", raw, line.start(), line.end(), line_no, line_no, reason="unsupported_table_or_code"))
                continue
            group.append((line_no, line))
            if self.config.paragraph_mode == "one_line":
                flush()
        flush()
        news = [{"kind": "news", "text": match.group(), "start": match.start(), "end": match.end(),
                 "institution": next((s for s in ("新华社", "中新社", "中国新闻社") if s in match.group()), None)}
                for match in NEWS.finditer(text)]
        if len(news) > 1:
            diagnostics.append({"code": "possible_multiple_documents", "severity": "review"})
        if sum(bool(NUMBERED.match(line.group().lstrip("\ufeff"))) for line in lines) >= 2:
            diagnostics.append({"code": "numbered_document_requires_other_workflow", "severity": "review"})
        if excluded and any(row["role"] == "needs_review" for row in excluded):
            diagnostics.append({"code": "unsupported_embedded_structure", "severity": "review"})
        if not paragraphs:
            diagnostics.append({"code": "no_body_paragraphs", "severity": "review"})
        if title_origin == "filename":
            diagnostics.append({"code": "title_from_filename", "severity": "warning"})
        # Coverage is checked against original non-whitespace positions, not reconstructed text.
        covered = bytearray(len(text))
        for row in coverage:
            covered[row["start"]:row["end"]] = b"\x01" * (row["end"] - row["start"])
        passed = all(covered[i] for i, char in enumerate(text) if not char.isspace() and char != "\ufeff")
        return {"filename": filename, "source_path": source_path, "source_sha256": fingerprint,
                "document_id": document_id, "source_identity": source_identity, "raw_text": text,
                "title": title, "title_origin": title_origin, "paragraphs": paragraphs,
                "headings": headings, "metadata": metadata, "excluded": excluded,
                "coverage": coverage, "source_signals": news, "diagnostics": diagnostics,
                "validation": {"passed": passed, "uncovered_nonempty": not passed}}
