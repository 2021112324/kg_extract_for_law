"""Sentence-safe execution chunks with bounded, separately authorized context."""
import json
import re
from .config import ParagraphConfig
from .io_utils import stable_id

PAIRS = {"“": "”", "‘": "’", "「": "」", "『": "』", "（": "）", "(": ")", "[": "]", "【": "】"}
SCOPE = re.compile(r"(?:本|该).{0,15}(?:适用|针对)|仅适用|在.{1,50}时|对于.{1,40}[，,]|除.{1,40}外")
DEPENDENCY = re.compile(r"^(?:其|该|上述|前述|这些|同时|但是|但|除|其中|相应|对此|此外)")


def sentence_ranges(text):
    protected = [(m.start(), m.end()) for m in re.finditer(r"https?://[^\s<>，。；！？）)]+", text)]
    protected = [(a, b - len(text[a:b]) + len(text[a:b].rstrip(".!?"))) for a, b in protected]
    stack, ends = [], []
    for i, char in enumerate(text):
        if char in PAIRS:
            stack.append(PAIRS[char])
        elif stack and char == stack[-1]:
            stack.pop()
            if not stack and i and text[i-1] in "。！？.!?":
                ends.append(i + 1)
        elif char == '"':
            if stack and stack[-1] == '"':
                stack.pop()
            else:
                stack.append('"')
        elif char in "。！？.!?" and not stack:
            if any(a <= i < b for a, b in protected):
                continue
            if char == "." and (i and i+1 < len(text) and text[i-1].isdigit() and text[i+1].isdigit()
                                or re.search(r"(?:\b(?:Mr|Mrs|Ms|Dr|Prof|etc)|\b[A-Za-z]|\b(?:e\.g|i\.e))\.$", text[:i+1])):
                continue
            ends.append(i + 1)
    if stack:
        raise ValueError("unclosed_quote_or_bracket")
    starts, result = 0, []
    for end in sorted(set(ends + [len(text)])):
        if end > starts:
            result.append((starts, end))
            starts = end
    return result


def split_paragraph(text, limit):
    ranges = sentence_ranges(text)
    if len(text) <= limit:
        return [(0, len(text))]
    chunks, start, end = [], 0, 0
    for a, b in ranges:
        if b-a > limit:
            raise ValueError("indivisible_sentence_exceeds_budget")
        if b-start > limit:
            chunks.append((start, end))
            start = a
        end = b
    if end > start:
        chunks.append((start, end))
    return chunks


class ParagraphUnitPlanner:
    def __init__(self, config=None):
        self.config = config or ParagraphConfig()

    def plan(self, parsed):
        units, review = [], []
        paragraphs = parsed["paragraphs"]
        for i, paragraph in enumerate(paragraphs):
            try:
                ranges = split_paragraph(paragraph["text"], self.config.block_chars)
            except ValueError as exc:
                review.append({"paragraph_id": paragraph["paragraph_id"], "reason": str(exc)})
                continue
            for a, b in ranges:
                contexts = []
                for h in parsed["headings"]:
                    if h["start"] < paragraph["start"] and h["text"] in paragraph["heading"]:
                        contexts.append({"role": "heading", "text": h["text"], "start": h["start"], "end": h["end"]})
                # Adjacent chunks only explain this target; no paragraph-wide recursive duplication.
                for ca, cb in ranges:
                    if cb == a or ca == b:
                        contexts.append({"role": "same_paragraph_neighbor", "text": paragraph["text"][ca:cb],
                                         "start": paragraph["start"]+ca, "end": paragraph["start"]+cb})
                for j in (i-1, i+1):
                    if 0 <= j < len(paragraphs):
                        other = paragraphs[j]
                        if other["section_id"] == paragraph["section_id"]:
                            role = "exception" if j > i and re.match(r"^(?:但|除|不包括|不适用)", other["text"].lstrip()) else "scope" if SCOPE.search(other["text"]) else "neighbor_for_resolution_only"
                            if j < i or DEPENDENCY.match(other["text"].lstrip()):
                                contexts.append({"role": role, "text": other["text"], "start": other["start"], "end": other["end"]})
                for signal in parsed["source_signals"]:
                    if not (paragraph["start"]+a <= signal["start"] and signal["end"] <= paragraph["start"]+b):
                        contexts.append({"role": "source_attribution_only", "text": signal["text"],
                                         "start": signal["start"], "end": signal["end"]})
                contexts = list({(c["start"], c["end"]): c for c in contexts}.values())
                target = paragraph["text"][a:b]
                unit = {"target_id": stable_id("paragraph_target", paragraph["paragraph_id"], a, b),
                        "paragraph_id": paragraph["paragraph_id"], "target": target,
                        "start": paragraph["start"]+a, "end": paragraph["start"]+b,
                        "context": contexts, "heading": paragraph["heading"],
                        "high_signal": bool(re.search(r"应当|必须|不得|禁止", target))}
                if sum(len(c["text"]) for c in contexts) > self.config.context_chars:
                    review.append({"target_id": unit["target_id"], "paragraph_id": paragraph["paragraph_id"], "reason": "necessary_context_exceeds_budget"})
                else:
                    units.append(unit)
        file_parts = [{"role": "heading", **h} for h in parsed["headings"][:1]] + parsed["metadata"]
        for p in paragraphs[:2]:
            if sum(len(r["text"]) for r in file_parts) + len(p["text"]) <= self.config.context_chars:
                file_parts.append(p)
        for signal in parsed["source_signals"]:
            if not any(r["start"] <= signal["start"] and signal["end"] <= r["end"] for r in file_parts):
                file_parts.append(signal)
        source = "\n".join(r["text"] for r in file_parts)
        if not source.strip():
            review.append({"reason": "file_metadata_source_exceeds_budget"})
        elif len(source) > self.config.context_chars:
            review.append({"reason": "file_source_context_exceeds_budget"})
        return {"units": units, "review": review,
                "file_payload": {"title": parsed["title"], "title_origin": parsed["title_origin"],
                                 "source": source, "source_signals": parsed["source_signals"]},
                "formal_plan_eligible": parsed["validation"]["passed"] and not review
                    and not any(d["severity"] == "review" for d in parsed["diagnostics"]),
                "coverage": parsed["coverage"],
                "counts": {"paragraphs": len(paragraphs), "planned_targets": len(units),
                           "planning_review": len(review), "source_ranges": len(parsed["coverage"]),
                           "excluded_ranges": len(parsed["excluded"])}}

    def check_request(self, payload, prompt, schema, examples):
        size = len(prompt) + len(schema) + len(json.dumps(payload, ensure_ascii=False)) + len(json.dumps(examples, ensure_ascii=False))
        if size + self.config.output_reserve_chars > self.config.request_chars:
            raise ValueError("complete_request_and_output_reserve_exceed_budget")
        return size
