"""Deterministic first-stage parser for point-form compliance guides."""

from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import Any

from .config import DEFAULT_CONFIG, GuideP1Config
from .io_utils import discover_input_files, load_text, save_json, stage_output_path


RISK_TYPES = (
    "产品法律风险",
    "供应链合规风险",
    "劳动用工法律合规风险",
    "企业关联方合规风险",
    "企业国际化经营合规风险",
    "企业信用风险",
)

TOP_HEADING_RE = re.compile(r"^\s*([一二三四五六七八九十百]+)、\s*(.+?)\s*$")
POINT_HEADING_RE = re.compile(r"^\s*[（(]([一二三四五六七八九十百]+)[）)]\s*(.+?)\s*$")
CHAPTER_HEADING_RE = re.compile(r"^\s*(第[一二三四五六七八九十百\d]+章)\s*(.*?)\s*$")
NUMBERED_ITEM_RE = re.compile(r"^\s*(\d+(?:\.\d+)*[.、])\s*(.+?)\s*$")
COLUMN_RE = re.compile(r"^\s*(专栏\s*[A-Za-z0-9一二三四五六七八九十]*)\s*(.*?)\s*$")
ATTACHMENT_RE = re.compile(r"^\s*(附件|附录|附表)\s*([A-Za-z0-9一二三四五六七八九十]*)\s*(.*?)\s*$")
DOCUMENT_NUMBER_RE = re.compile(r"[\u4e00-\u9fffA-Za-z]{0,12}[〔\[]\d{4}[〕\]]\d+号")
DATE_RE = re.compile(r"\d{4}年\d{1,2}月\d{1,2}日")
ACTION_SIGNAL_RE = re.compile(
    r"应当|必须|不得|严禁|禁止|推动|推进|加强|建立|健全|完善|实施|开展|落实|确保|鼓励|支持|到\d{4}年|达到|完成|提升|降低|控制"
)


def _stable_id(*parts: Any) -> str:
    raw = "\x1f".join(str(part or "") for part in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def _clean_line(value: str) -> str:
    text = str(value or "").replace("\u3000", " ").strip()
    text = re.sub(r"^\s{0,3}#{1,6}\s*", "", text)
    return re.sub(r"[ \t]+", " ", text).strip()


def _compact_title(value: str) -> str:
    return re.sub(r"[\s《》“”\"'‘’]", "", str(value or ""))


def _split_inline_title(value: str) -> tuple[str, str]:
    text = _clean_line(value)
    if "。" not in text:
        return text, ""
    head, tail = text.split("。", 1)
    if 1 <= len(head) <= 60:
        return head.strip(), tail.strip()
    return text, ""


class GuideP1StructureParser:
    """Recover auditable document structure without semantic inference."""

    def __init__(self, config: GuideP1Config | None = None) -> None:
        self.config = config or DEFAULT_CONFIG

    def parse_file(self, input_path: str | Path) -> dict[str, Any]:
        path = Path(input_path)
        return self.parse_text(load_text(path), filename=path.name, source_path=str(path))

    def parse_text(self, text: str, filename: str = "", source_path: str = "") -> dict[str, Any]:
        original_lines = str(text or "").splitlines()
        lines = [_clean_line(line) for line in original_lines]
        metadata = self._extract_metadata(lines, filename)
        attachment_markers = [
            (index, ATTACHMENT_RE.match(line))
            for index, line in enumerate(lines)
            if line and ATTACHMENT_RE.match(line)
        ]
        body_limit = attachment_markers[0][0] if attachment_markers else len(lines)
        attachments = self._build_skipped_attachments(lines, attachment_markers, filename)
        nodes = self._build_structure_nodes(lines, body_limit, filename)
        self._assign_paths(nodes)
        semantic_blocks = self._build_semantic_blocks(nodes, filename)
        validation = self._validate(nodes, attachments, metadata, body_limit, len(lines))
        first_structure_line = min((node["line_start"] for node in nodes), default=body_limit + 1)
        header_end = max(0, first_structure_line - 1)
        header_text = "\n".join(line for line in lines[:header_end] if line).strip()

        return {
            "filename": filename,
            "source_path": source_path,
            "metadata_candidates": metadata,
            "file_header": header_text,
            "structure_nodes": nodes,
            "semantic_blocks": semantic_blocks,
            "skipped_attachments": attachments,
            "validation": validation,
            "stats": {
                "source_line_count": len(lines),
                "structure_node_count": len(nodes),
                "semantic_block_count": len(semantic_blocks),
                "skipped_attachment_count": len(attachments),
                "body_end_line": body_limit,
            },
        }

    def _extract_metadata(self, lines: list[str], filename: str) -> dict[str, Any]:
        first_structure_index = next(
            (
                index
                for index, line in enumerate(lines)
                if TOP_HEADING_RE.match(line) or CHAPTER_HEADING_RE.match(line) or POINT_HEADING_RE.match(line)
            ),
            min(len(lines), 80),
        )
        scan_lines = [line for line in lines[:first_structure_index] if line]
        risk_types = [risk for risk in RISK_TYPES if any(risk in line for line in scan_lines[:5])]
        document_numbers = list(dict.fromkeys(match.group(0) for line in scan_lines for match in DOCUMENT_NUMBER_RE.finditer(line)))
        dates = list(dict.fromkeys(match.group(0) for line in scan_lines for match in DATE_RE.finditer(line)))
        filename_key = _compact_title(Path(filename).stem)
        title_candidates = []
        for line in scan_lines:
            key = _compact_title(line)
            if not key or "合规风险类型" in line:
                continue
            if filename_key and (key == filename_key or filename_key in key) and not line.endswith("通知"):
                title_candidates.append(line.strip("《》"))
        if not title_candidates:
            title_candidates = [
                line.strip("《》")
                for line in scan_lines
                if re.search(r"(规划|方案|指南|指引|意见|通知|纲要)$", line)
                and not re.search(r"(关于印发|印发.+通知)$", line)
            ]
        title = title_candidates[-1] if title_candidates else Path(filename).stem
        issuers = []
        for line in scan_lines:
            if 2 <= len(line) <= 40 and re.search(r"(国务院|委员会|人民政府|总局|部|厅|局)$", line):
                if line not in issuers:
                    issuers.append(line)
        document_type = next((kind for kind in ("规划", "方案", "指南", "指引", "意见", "通知") if title.endswith(kind)), "合规指引")
        title_index = next((index for index, line in enumerate(lines[:80]) if _compact_title(line) == _compact_title(title)), -1)
        return {
            "risk_types": risk_types,
            "document_title": title,
            "document_numbers": document_numbers,
            "dates": dates,
            "issuer_candidates": issuers,
            "document_type": document_type,
            "document_title_line": title_index + 1 if title_index >= 0 else None,
        }

    def _build_structure_nodes(self, lines: list[str], body_limit: int, filename: str) -> list[dict[str, Any]]:
        nodes: list[dict[str, Any]] = []
        node_by_id: dict[str, dict[str, Any]] = {}
        current_id = ""
        last_top_id = ""
        last_point_id = ""

        for index in range(body_limit):
            line = lines[index]
            if not line or line.startswith("合规风险类型："):
                continue
            detected = self._detect_structure(line)
            if not detected:
                if current_id:
                    node = node_by_id[current_id]
                    node["content_lines"].append(line)
                    node["line_end"] = index + 1
                continue

            node_type, number, title, inline_content = detected
            if node_type in {"一级标题", "章"}:
                parent_id = ""
                level = 1
            elif node_type == "二级分点":
                parent_id = last_top_id
                level = 2
            elif node_type == "专栏":
                parent_id = last_point_id or last_top_id
                level = 3 if last_point_id else 2
            else:
                parent_id = last_point_id or last_top_id
                level = 3 if last_point_id else 2

            node_id = f"guide_structure_{_stable_id(filename, index + 1, node_type, number, title)}"
            node = {
                "node_id": node_id,
                "node_type": node_type,
                "number": number,
                "title": title,
                "level": level,
                "parent_id": parent_id,
                "order": len(nodes) + 1,
                "line_start": index + 1,
                "line_end": index + 1,
                "title_line": line,
                "content_lines": [inline_content] if inline_content else [],
                "content": inline_content,
                "raw_text": line,
                "path_titles": [],
                "complete_path": "",
            }
            nodes.append(node)
            node_by_id[node_id] = node
            current_id = node_id
            if node_type in {"一级标题", "章"}:
                last_top_id = node_id
                last_point_id = ""
            elif node_type == "二级分点":
                last_point_id = node_id

        for node in nodes:
            node["content"] = "\n".join(item for item in node.pop("content_lines") if item).strip()
            node["raw_text"] = "\n".join(item for item in (node["title_line"], node["content"]) if item).strip()
        return nodes

    @staticmethod
    def _detect_structure(line: str) -> tuple[str, str, str, str] | None:
        match = TOP_HEADING_RE.match(line)
        if match:
            return "一级标题", f"{match.group(1)}、", match.group(2).strip(), ""
        match = CHAPTER_HEADING_RE.match(line)
        if match and match.group(2):
            return "章", match.group(1), match.group(2).strip(), ""
        match = POINT_HEADING_RE.match(line)
        if match:
            title, content = _split_inline_title(match.group(2))
            return "二级分点", f"（{match.group(1)}）", title, content
        match = COLUMN_RE.match(line)
        if match:
            title, content = _split_inline_title(match.group(2))
            return "专栏", re.sub(r"\s+", "", match.group(1)), title or match.group(1), content
        match = NUMBERED_ITEM_RE.match(line)
        if match:
            title, content = _split_inline_title(match.group(2))
            return "编号分点", match.group(1), title, content
        return None

    @staticmethod
    def _assign_paths(nodes: list[dict[str, Any]]) -> None:
        by_id = {node["node_id"]: node for node in nodes}
        for node in nodes:
            titles = []
            current = node
            seen = set()
            while current and current["node_id"] not in seen:
                seen.add(current["node_id"])
                display = f"{current.get('number', '')}{current.get('title', '')}".strip()
                if display:
                    titles.append(display)
                current = by_id.get(current.get("parent_id", ""))
            node["path_titles"] = list(reversed(titles))
            node["complete_path"] = " / ".join(node["path_titles"])

    def _build_semantic_blocks(self, nodes: list[dict[str, Any]], filename: str) -> list[dict[str, Any]]:
        blocks = []
        for node in nodes:
            if not node.get("content"):
                continue
            source_text = node["raw_text"]
            chunks = self._chunk_text(source_text, self.config.semantic_block_chars)
            for chunk_index, chunk in enumerate(chunks, start=1):
                block_id = f"guide_block_{_stable_id(filename, node['node_id'], chunk_index, chunk)}"
                blocks.append(
                    {
                        "block_id": block_id,
                        "source_node_id": node["node_id"],
                        "source_node_type": node["node_type"],
                        "source_path": node["complete_path"],
                        "line_start": node["line_start"],
                        "line_end": node["line_end"],
                        "chunk_index": chunk_index,
                        "chunk_count": len(chunks),
                        "ancestor_context": " / ".join(node["path_titles"][:-1]),
                        "text": chunk,
                        "high_knowledge_signal": bool(ACTION_SIGNAL_RE.search(chunk)),
                    }
                )
        return blocks

    @staticmethod
    def _chunk_text(text: str, max_chars: int) -> list[str]:
        value = str(text or "").strip()
        if not value:
            return []
        if len(value) <= max_chars:
            return [value]
        sentences = [item.strip() for item in re.split(r"(?<=[。！？；])", value) if item.strip()]
        chunks: list[str] = []
        current = ""
        for sentence in sentences:
            if len(sentence) > max_chars:
                if current:
                    chunks.append(current)
                    current = ""
                chunks.extend(sentence[i : i + max_chars] for i in range(0, len(sentence), max_chars))
                continue
            candidate = current + sentence
            if current and len(candidate) > max_chars:
                chunks.append(current)
                current = sentence
            else:
                current = candidate
        if current:
            chunks.append(current)
        return chunks

    @staticmethod
    def _build_skipped_attachments(
        lines: list[str],
        markers: list[tuple[int, re.Match[str] | None]],
        filename: str,
    ) -> list[dict[str, Any]]:
        result = []
        for position, (index, match) in enumerate(markers):
            if match is None:
                continue
            next_index = markers[position + 1][0] if position + 1 < len(markers) else len(lines)
            next_title = next((line for line in lines[index + 1 : next_index] if line), "")
            marker_title = match.group(3).strip()
            title = marker_title or next_title
            number = f"{match.group(1)}{match.group(2)}".strip()
            result.append(
                {
                    "attachment_id": f"guide_attachment_{_stable_id(filename, index + 1, number, title)}",
                    "type": match.group(1),
                    "number": number,
                    "title": title,
                    "line_start": index + 1,
                    "line_end": next_index,
                    "status": "skipped",
                    "reason": "首版默认跳过附件、附录和附表正文",
                }
            )
        return result

    @staticmethod
    def _validate(
        nodes: list[dict[str, Any]],
        attachments: list[dict[str, Any]],
        metadata: dict[str, Any],
        body_limit: int,
        line_count: int,
    ) -> dict[str, Any]:
        errors: list[str] = []
        warnings: list[str] = []
        if not nodes:
            errors.append("未识别到正文结构节点")
        ids = [node["node_id"] for node in nodes]
        if len(ids) != len(set(ids)):
            errors.append("结构节点 ID 重复")
        known_ids = set(ids)
        for node in nodes:
            if node.get("parent_id") and node["parent_id"] not in known_ids:
                errors.append(f"结构节点父级不存在: {node['node_id']}")
            if node["line_start"] > node["line_end"]:
                errors.append(f"结构节点行号范围无效: {node['node_id']}")
        if not metadata.get("risk_types"):
            warnings.append("文首未识别到六类受控合规风险类型")
        if not metadata.get("document_title"):
            warnings.append("未识别到文件标题")
        if attachments and body_limit >= line_count:
            errors.append("附件边界记录不一致")
        return {
            "passed": not errors,
            "errors": errors,
            "warnings": warnings,
        }


def parse_guide_p1_file(
    input_path: str | Path,
    output_dir: str | Path | None = None,
    config: GuideP1Config | None = None,
) -> dict[str, Any]:
    parser = GuideP1StructureParser(config=config)
    result = parser.parse_file(input_path)
    if output_dir:
        result["output_path"] = save_json(result, stage_output_path(input_path, output_dir, "split"))
    return result


def parse_guide_p1_batch(
    input_path: str | Path,
    output_dir: str | Path,
    config: GuideP1Config | None = None,
) -> dict[str, Any]:
    effective_config = config or DEFAULT_CONFIG
    files = discover_input_files(input_path, effective_config.supported_suffixes)
    summary: dict[str, Any] = {
        "input_path": str(input_path),
        "output_dir": str(output_dir),
        "total_files": len(files),
        "success_files": 0,
        "failed_files": 0,
        "structure_node_count": 0,
        "semantic_block_count": 0,
        "skipped_attachment_count": 0,
        "items": [],
    }
    parser = GuideP1StructureParser(config=effective_config)
    for path in files:
        try:
            result = parser.parse_file(path)
            save_json(result, stage_output_path(path, output_dir, "split"))
            passed = bool(result.get("validation", {}).get("passed"))
            summary["success_files" if passed else "failed_files"] += 1
            for key in ("structure_node_count", "semantic_block_count", "skipped_attachment_count"):
                summary[key] += int(result.get("stats", {}).get(key, 0))
            summary["items"].append(
                {
                    "filename": path.name,
                    "status": "success" if passed else "validation_failed",
                    "stats": result.get("stats", {}),
                    "validation": result.get("validation", {}),
                }
            )
        except Exception as exc:
            summary["failed_files"] += 1
            summary["items"].append({"filename": path.name, "status": "failed", "error": str(exc)})
    summary["summary_path"] = save_json(summary, Path(output_dir) / "summary.json")
    return summary
