"""国家标准第二阶段 LLM 知识图谱抽取。"""

from __future__ import annotations

import asyncio
from difflib import SequenceMatcher
import json
import logging
import os
import re
import string
from pathlib import Path
from typing import Any, Iterable

from app.infrastructure.information_extraction.base import Entity, Relationship
from app.infrastructure.information_extraction.factory import InformationExtractionFactory
from app.infrastructure.information_extraction.method.base import LangextractConfig
from app.infrastructure.information_extraction.national_standard.national_standard_extract import (
    NationalStandardExtractor,
    _discover_markdown_standard_dirs,
    _sanitize_minio_path_part,
)
from app.infrastructure.information_extraction.national_standard.prompt.example import (
    example_for_standard_file_info,
    example_for_standard_tree_node,
)
from app.infrastructure.information_extraction.national_standard.prompt.prompt_compact import (
    prompt_for_standard_file_info,
    prompt_for_standard_tree_node,
)
from app.infrastructure.information_extraction.national_standard.prompt.schema import (
    schema_for_standard_file_info,
    schema_for_standard_tree_node,
)
from app.infrastructure.string_utils.id_tool import generate_hex_uuid


NATIONAL_STANDARD_GRAPH_MODEL = os.getenv("NATIONAL_STANDARD_GRAPH_MODEL", "qwen3-30b-a3b-instruct-2507")
NATIONAL_STANDARD_GRAPH_API_KEY = os.getenv(
    "NATIONAL_STANDARD_GRAPH_API_KEY",
    "gpustack_342609ce423be29a_4371426b285a91dc44fb4e8d72454847",
)
NATIONAL_STANDARD_GRAPH_API_URL = os.getenv(
    "NATIONAL_STANDARD_GRAPH_API_URL",
    "http://222.171.219.26:20001/v1/chat/completions",
)
NATIONAL_STANDARD_GRAPH_TIMEOUT = int(os.getenv("NATIONAL_STANDARD_GRAPH_TIMEOUT", "3000"))
NATIONAL_STANDARD_GRAPH_MAX_CHAR_BUFFER = int(os.getenv("NATIONAL_STANDARD_GRAPH_MAX_CHAR_BUFFER", "7500"))
NATIONAL_STANDARD_GRAPH_BATCH_LENGTH = int(os.getenv("NATIONAL_STANDARD_GRAPH_BATCH_LENGTH", "5"))
NATIONAL_STANDARD_GRAPH_MAX_WORKERS = int(os.getenv("NATIONAL_STANDARD_GRAPH_MAX_WORKERS", "5"))

CONTROLLED_CONSTRAINT_STRENGTH = {"强制", "建议", "无约束", "不明确"}
STANDARD_PREFIX_RE = re.compile(
    r"\b(?:GB|GB/T|GB/Z|GJB|HJ|JB|JB/T|SJ|SJ/T|SJ/Z|ISO|IEC|EN|ASTM|DIN|YY|YY/T|AQ|SN|QB|HG|CJ|CJJ|NB|DL)"
    r"\s*[\w./-]*\d*",
    re.I,
)
INTERNAL_REFERENCE_RE = re.compile(
    r"^(?:\d+(?:\.\d+)*(?:\s*[a-z]\))?|[a-z]\)|\d+\)|第\s*\d+\s*章|附录\s*[A-Za-zＡ-Ｚａ-ｚ]|表\s*[A-Za-z]?\s*\d+(?:\.\d+)*)$",
    re.I,
)
NORMATIVE_MODAL_RE = re.compile(
    r"应当|不应|不得|不宜|不准|不可|不能|禁止|严禁|必须|需要|应符合|应按|应|需|须|宜|建议|推荐|可以|允许|准许|可|执行|符合|满足|不小于|不大于|不少于|不超过|至少"
)
FORCE_MODAL_RE = re.compile(r"应当|不应|不得|不准|不可|不能|禁止|严禁|必须|需要|应符合|应按|应|需|须|执行|符合|满足|不小于|不大于|不少于|不超过|至少")
ADVISORY_MODAL_RE = re.compile(r"不宜|宜|建议|推荐")
OPTIONAL_MODAL_RE = re.compile(r"可以|允许|准许|可")
LIST_ITEM_ONLY_RE = re.compile(r"^\s*(?:[a-zA-Z]\)|[a-zA-Z]）|\d+\)|\d+）|[-—–•])\s*[^。；;！？!?]{1,40}[；;。]?\s*$")
STANDARD_REQUIREMENT_ALLOWED_PROPS = {
    "要求内容",
    "要求类型",
    "约束强度",
    "约束强度原文",
    "适用对象",
    "适用条件",
    "行为描述",
    "name",
    "量化特征",
    "量化条件",
}
PROPERTY_ALLOW_OVERRIDES = {
    "标准结构节点": {"节点层级", "完整路径标题", "是否附录节点", "附录编号", "附录类型", "相关图片地址"},
    "表格": {"表格HTML", "解析行"},
    "图片": {"图片标题", "图片链接", "图片类型", "image_id"},
}


def _extract_all_allowed_properties_from_schema(schema_text: str) -> dict[str, set[str]]:
    allowed: dict[str, set[str]] = {}
    entity_blocks = re.finditer(r"^##\s+(.+?)\s*\n(.*?)(?=^##\s+|\Z)", schema_text, flags=re.M | re.S)
    for entity_match in entity_blocks:
        entity_type = entity_match.group(1).strip()
        block = entity_match.group(2)
        attr_match = re.search(r"^###\s+属性\s*\n(.*?)(?=^###\s+|^##\s+|\Z)", block, flags=re.M | re.S)
        if not attr_match:
            continue
        props: set[str] = set()
        for line in attr_match.group(1).splitlines():
            prop_match = re.match(r"\s*-\s*([^：:\n]+?)\s*[：:]", line)
            if prop_match:
                props.add(prop_match.group(1).strip())
        if props:
            allowed[entity_type] = props
    return allowed


NODE_ALLOWED_PROPERTIES = _extract_all_allowed_properties_from_schema(
    f"{schema_for_standard_file_info}\n{schema_for_standard_tree_node}"
)


class ResultStats:
    def __init__(self):
        self.error = 0
        self.error_msg = ""
        self.week_warning = 0
        self.week_warning_msg = ""
        self.strong_warning = 0
        self.strong_warning_msg = ""


def _normalize_match_text(value: Any) -> str:
    text = str(value or "").lower()
    text = re.sub(r"\s+", "", text)
    text = text.translate(
        str.maketrans(
            {
                "：": ":",
                "；": ";",
                "，": ",",
                "。": ".",
                "（": "(",
                "）": ")",
                "【": "[",
                "】": "]",
                "—": "-",
                "–": "-",
                "－": "-",
            }
        )
    )
    return re.sub(r"[\s:：/\\\-—–－（）(),，。.;；]+", "", text)


def _looks_like_path_or_url(value: Any) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    return bool(
        re.match(r"^(?:https?://|s3://|minio://|file://)", text, flags=re.I)
        or re.search(r"[\\/](?:images?|tables?)[\\/]", text, flags=re.I)
        or re.search(r"\.(?:png|jpe?g|webp|gif|bmp|svg|pdf|md|json)$", text, flags=re.I)
    )


def _normalize_constraint_strength(raw_strength: Any, requirement_content: Any = "") -> tuple[str, str]:
    strength = str(raw_strength or "").strip()
    text = str(requirement_content or "")
    if strength in CONTROLLED_CONSTRAINT_STRENGTH:
        return strength, text if NORMATIVE_MODAL_RE.search(text) else ""

    combined = f"{strength} {text}"
    if ADVISORY_MODAL_RE.search(combined):
        return "建议", text if NORMATIVE_MODAL_RE.search(text) else strength
    if OPTIONAL_MODAL_RE.search(combined):
        return "无约束", text if NORMATIVE_MODAL_RE.search(text) else strength
    if FORCE_MODAL_RE.search(combined):
        return "强制", text if NORMATIVE_MODAL_RE.search(text) else strength
    return "不明确", ""


def _normalize_quant_condition(value: Any) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if not text:
        return ""
    # 孤立数字、比例、章节号、表号或单位片段不是可追溯的量化条件。
    if re.fullmatch(r"(?:\d+(?:\.\d+)*|[A-Za-z]?\d+|[<>≤≥=]*\s*\d+(?:\.\d+)?(?:[:：]\d+(?:\.\d+)?)+)", text):
        return ""
    if not re.search(r"\d", text):
        return ""
    return text


class NationalStandardGraphExtractor:
    """国家标准二阶段 LLM 抽取器。

    二阶段默认且只能调用 LLM。第一阶段结构只用于提供上下文节点和 LLM 输入，
    不再通过规则生成“标准要求”等语义节点。
    """

    def __init__(
        self,
        max_concurrent: int = 3,
        include_validation_failed: bool = False,
        max_tree_nodes: int | None = None,
    ) -> None:
        self.include_validation_failed = include_validation_failed
        self.max_tree_nodes = max_tree_nodes
        self.result_stats = ResultStats()
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.first_stage_extractor = NationalStandardExtractor(upload_images=True)
        config = LangextractConfig(
            model_name=NATIONAL_STANDARD_GRAPH_MODEL,
            api_key=NATIONAL_STANDARD_GRAPH_API_KEY,
            api_url=NATIONAL_STANDARD_GRAPH_API_URL,
            config={"timeout": NATIONAL_STANDARD_GRAPH_TIMEOUT},
            max_char_buffer=NATIONAL_STANDARD_GRAPH_MAX_CHAR_BUFFER,
            batch_length=NATIONAL_STANDARD_GRAPH_BATCH_LENGTH,
            max_workers=NATIONAL_STANDARD_GRAPH_MAX_WORKERS,
        )
        self.extractor = InformationExtractionFactory.create("langextract", max_retries=3, config=config)

    async def extract(self, input_path: str | Path) -> dict[str, Any]:
        parse_result = self._load_or_parse_first_stage(input_path)
        filename = self._filename_from_parse_result(parse_result, input_path)
        return await self.extract_from_parse_result(parse_result, filename)

    async def extract_from_parse_result(self, parse_result: dict[str, Any], filename: str) -> dict[str, Any]:
        validation = parse_result.get("validation", {}) or {}
        validation_passed = bool(validation.get("passed"))
        if not validation_passed:
            self.result_stats.error += 1
            self.result_stats.error_msg += f"第一阶段解析未通过: {filename}\n"
        if not validation_passed and not self.include_validation_failed:
            return {
                "filename": filename,
                "status": "skipped_validation_failed",
                "parse_validation": validation,
                "source": parse_result.get("source", {}),
                "review_status": parse_result.get("review_status", ""),
                "graph": {"nodes": [], "edges": []},
                "llm_extractions": [],
                "stats": {
                    "node_count": 0,
                    "edge_count": 0,
                    "llm_call_count": 0,
                    "validation_passed": False,
                },
            }

        llm_extractions = await self._extract_with_llm(parse_result, filename)
        for item in llm_extractions:
            if item.get("status") != "success":
                self.result_stats.error += 1
                self.result_stats.error_msg += f"LLM调用失败: {filename} error={item.get('error', '')}\n"
        graph_builder = _GraphBuilder(filename, self.result_stats)
        graph_builder.add_context_graph(parse_result)
        graph_builder.add_llm_extractions(llm_extractions)
        graph = graph_builder.to_graph()
        graph_builder.validate_and_report()
        if len(graph.get("nodes", [])) == 0:
            self.result_stats.error += 1
            self.result_stats.error_msg += f"图谱节点数为0: {filename}\n"
        llm_failed_count = sum(1 for item in llm_extractions if item.get("status") != "success")
        if llm_failed_count > 0:
            self.result_stats.week_warning += 1
            self.result_stats.week_warning_msg += f"LLM部分调用失败: {filename} 失败数={llm_failed_count}\n"

        return {
            "filename": filename,
            "status": "success" if validation_passed and llm_failed_count == 0 else "llm_partial_failed",
            "extraction_mode": "llm",
            "parse_validation": validation,
            "source": parse_result.get("source", {}),
            "review_status": parse_result.get("review_status", ""),
            "graph": graph,
            "llm_extractions": llm_extractions,
            "stats": {
                "node_count": len(graph.get("nodes", [])),
                "edge_count": len(graph.get("edges", [])),
                "llm_call_count": len(llm_extractions),
                "llm_failed_count": llm_failed_count,
                "validation_passed": validation_passed,
            },
        }

    def _load_or_parse_first_stage(self, input_path: str | Path) -> dict[str, Any]:
        path = Path(input_path)
        if path.is_file() and path.suffix.lower() == ".json":
            return json.loads(path.read_text(encoding="utf-8"))
        return self.first_stage_extractor.extract(path)

    @staticmethod
    def _filename_from_parse_result(parse_result: dict[str, Any], input_path: str | Path) -> str:
        source = parse_result.get("source", {}) or {}
        for candidate in (source.get("root"), source.get("input_file"), input_path):
            name = NationalStandardGraphExtractor._source_path_name(candidate)
            if name:
                return name
        return Path(input_path).stem

    @staticmethod
    def _source_path_name(value: Any) -> str:
        if value in (None, ""):
            return ""
        path = Path(str(value))
        if path.suffix.lower() == ".json":
            return path.stem
        if path.suffix.lower() in {".md", ".txt"}:
            return path.parent.name or path.stem
        return path.name

    async def _extract_with_llm(self, parse_result: dict[str, Any], filename: str) -> list[dict[str, Any]]:
        tasks = [self._llm_extract_file_info(parse_result, filename)]
        nodes = list(self._walk_nodes(parse_result.get("body_tree", []) + parse_result.get("appendix_tree", [])))
        if self.max_tree_nodes is not None:
            nodes = nodes[: self.max_tree_nodes]
        for node in nodes:
            content = self._build_node_llm_input(node, parse_result, filename)
            if content.strip():
                tasks.append(self._llm_extract_tree_node(content, node))
        results = await asyncio.gather(*tasks, return_exceptions=True)

        normalized: list[dict[str, Any]] = []
        for result in results:
            if isinstance(result, Exception):
                normalized.append({"status": "failed", "error": str(result), "entities": [], "relations": []})
            else:
                normalized.append(result)
        return normalized

    async def _llm_extract_file_info(self, parse_result: dict[str, Any], filename: str) -> dict[str, Any]:
        async with self.semaphore:
            result = await self.extractor.entity_and_relationship_extract(
                user_prompt=prompt_for_standard_file_info,
                schema=schema_for_standard_file_info,
                input_text=self._build_file_info_llm_input(parse_result, filename),
                examples=example_for_standard_file_info,
            )
            return {"scope": "file_info", "status": "success", **self._serialize_extract_result(result)}

    async def _llm_extract_tree_node(self, content: str, node: dict[str, Any]) -> dict[str, Any]:
        async with self.semaphore:
            result = await self.extractor.entity_and_relationship_extract(
                user_prompt=prompt_for_standard_tree_node,
                schema=schema_for_standard_tree_node,
                input_text=content,
                examples=example_for_standard_tree_node,
            )
            return {
                "scope": "tree_node",
                "node_number": node.get("number", ""),
                "node_title": node.get("title", ""),
                "status": "success",
                **self._serialize_extract_result(result),
            }

    @staticmethod
    def _serialize_extract_result(result: dict[str, Any]) -> dict[str, Any]:
        entities = []
        for entity in result.get("entities", []) or []:
            if isinstance(entity, Entity):
                entities.append(
                    {
                        "name": entity.name,
                        "type": entity.entity_type,
                        "properties": entity.properties,
                    }
                )
        relations = []
        for relation in result.get("relations", []) or []:
            if isinstance(relation, Relationship):
                relations.append(
                    {
                        "source": relation.source,
                        "target": relation.target,
                        "type": relation.type,
                        "properties": relation.properties,
                    }
                )
        return {"entities": entities, "relations": relations}

    @staticmethod
    def _build_file_info_llm_input(parse_result: dict[str, Any], filename: str) -> str:
        file_info = parse_result.get("file_info", {}) or {}
        references = parse_result.get("reference_candidates", []) or []
        preface = parse_result.get("preface", "") or ""
        introduction = parse_result.get("introduction", "") or ""
        toc = parse_result.get("toc", "") or ""
        reference_text = parse_result.get("reference_text", "") or ""
        scope_text = NationalStandardGraphExtractor._find_scope_text(parse_result)
        return (
            f"国家标准文件名：{filename}\n"
            f"规则候选文件信息：\n{json.dumps(file_info, ensure_ascii=False, indent=2)}\n\n"
            f"范围章节原文：\n{NationalStandardGraphExtractor._escape_latex_for_json(scope_text)}\n\n"
            f"前言原文：\n{NationalStandardGraphExtractor._escape_latex_for_json(preface)}\n\n"
            f"引言原文：\n{NationalStandardGraphExtractor._escape_latex_for_json(introduction)}\n\n"
            f"目次原文：\n{NationalStandardGraphExtractor._escape_latex_for_json(toc)}\n\n"
            f"参考文献原文：\n{NationalStandardGraphExtractor._escape_latex_for_json(reference_text)}\n\n"
            f"规范性引用文件候选：\n{json.dumps(references[:80], ensure_ascii=False, indent=2)}"
        )

    @staticmethod
    def _escape_latex_for_json(text: str) -> str:
        """对文本中的 LaTeX 反斜杠做 JSON 兼容转义。

        MinerU 输出的 full.md 中数学公式使用 LaTeX 语法，如 $17^{\\circ}C$、
        $\\mathrm{mg/m^{3}}$ 等，其中的 \\ 不是合法的 JSON 转义字符。
        如果把这些原文直接送给 LLM，LLM 输出的 JSON 字符串中会包含 \\circ 等
        非法转义，导致 absl JSON 解析失败。

        解决方式：在送 LLM 前把单反斜杠变为双反斜杠。LLM 将其原样放入 JSON
        字符串时，双反斜杠是合法的 JSON 转义（代表一个字面反斜杠），JSON 解析
        后自动还原为单反斜杠，LaTeX 语义完整保留。
        """
        if not text:
            return text
        return re.sub(r"\\(?![\"/bfnrtu\\])", r"\\\\", text)

    @staticmethod
    def _find_scope_text(parse_result: dict[str, Any]) -> str:
        for node in NationalStandardGraphExtractor._walk_nodes(parse_result.get("body_tree", []) or []):
            if str(node.get("number", "")) == "1" or "范围" in str(node.get("title", "")):
                return str(node.get("content", "") or "")
        return ""

    @staticmethod
    def _build_node_llm_input(node: dict[str, Any], parse_result: dict[str, Any], filename: str) -> str:
        table_map = {table.get("table_id"): table for table in parse_result.get("tables", []) or []}
        image_map = {image.get("image_id"): image for image in parse_result.get("images", []) or []}
        tables = [table_map.get(table_id) for table_id in node.get("tables", []) if table_map.get(table_id)]
        images = [image_map.get(image_id) for image_id in node.get("images", []) if image_map.get(image_id)]
        table_context = NationalStandardGraphExtractor._build_table_llm_context(tables)
        return (
            f"文件：{filename}\n"
            f"节点编号：{node.get('number', '')}\n"
            f"节点标题：{node.get('title', '')}\n"
            f"节点路径：{node.get('full_path_title', '')}\n"
            f"节点内容：\n{NationalStandardGraphExtractor._escape_latex_for_json(str(node.get('content', '') or ''))}\n"
            f"相关表格：\n{json.dumps(table_context, ensure_ascii=False, indent=2)}\n"
            f"相关图片：\n{json.dumps(images[:5], ensure_ascii=False, indent=2)}"
        )

    @staticmethod
    def _build_table_llm_context(tables: list[dict[str, Any]], max_tables: int = 5) -> list[dict[str, Any]]:
        """Build lightweight table context for LLM without copying full table HTML."""
        result: list[dict[str, Any]] = []
        for table in tables[:max_tables]:
            parsed_rows = table.get("parsed_rows")
            result.append(
                {
                    "table_id": table.get("table_id", ""),
                    "table_number": table.get("table_number", ""),
                    "table_caption": table.get("table_caption", ""),
                    "related_section_number": table.get("related_section_number", ""),
                    "related_section_title": table.get("related_section_title", ""),
                    "field_summary": NationalStandardGraphExtractor._summarize_table_fields(table),
                    "row_count": len(parsed_rows) if isinstance(parsed_rows, list) else "",
                    "output_hint": "完整表格内容已由一阶段表格节点保存；LLM输出时请使用table_id，不要复述完整HTML或Markdown。",
                }
            )
        return result

    @staticmethod
    def _summarize_table_fields(table: dict[str, Any], max_fields: int = 20) -> list[str]:
        parsed_rows = table.get("parsed_rows")
        fields: list[str] = []
        if isinstance(parsed_rows, list) and parsed_rows:
            first_row = parsed_rows[0]
            if isinstance(first_row, dict):
                fields = [str(key) for key in first_row.keys()]
            elif isinstance(first_row, list):
                fields = [str(value) for value in first_row]
        if not fields:
            table_markdown = str(table.get("table_markdown", "") or "")
            for line in table_markdown.splitlines():
                if "|" not in line:
                    continue
                cells = [cell.strip() for cell in line.strip().strip("|").split("|") if cell.strip()]
                if cells and not all(re.fullmatch(r":?-+:?", cell) for cell in cells):
                    fields = cells
                    break
        return fields[:max_fields]

    @staticmethod
    def _walk_nodes(nodes: list[dict[str, Any]]) -> Iterable[dict[str, Any]]:
        for node in nodes:
            yield node
            yield from NationalStandardGraphExtractor._walk_nodes(node.get("children", []) or [])

    async def logging_result_stats(self):
        # logging.info("📄❌：错误与警告统计结果如下")
        # logging.info("======================================================================")
        # logging.info(f"📄❌：错误信息: {self.result_stats.error_msg}")
        # logging.info("======================================================================")
        # logging.info(f"📄🔧：弱警告信息: {self.result_stats.week_warning_msg}")
        # logging.info("======================================================================")
        # logging.info(f"📄强警告：强警告信息: {self.result_stats.strong_warning_msg}")
        # logging.info("======================================================================")
        logging.info("📄✅：数据统计")
        logging.info(f"📄✅：错误数: {self.result_stats.error}")
        logging.info(f"📄✅：弱警告数: {self.result_stats.week_warning}")
        logging.info(f"📄✅：强警告数: {self.result_stats.strong_warning}")


class _GraphBuilder:
    """整合一阶段上下文节点与 LLM 抽取结果。"""

    def __init__(self, filename: str, result_stats: ResultStats | None = None) -> None:
        self.filename = filename
        self.nodes: dict[tuple[str, str], dict[str, Any]] = {}
        self.edges: list[dict[str, Any]] = []
        self.name_to_id: dict[str, str] = {}
        self.table_content_to_id: dict[str, str] = {}
        self.result_stats = result_stats

    def add_context_graph(self, parse_result: dict[str, Any]) -> None:
        file_info = parse_result.get("file_info", {}) or {}
        file_name = file_info.get("标准中文名称") or self.filename
        context_file_properties = {
            key: value
            for key, value in file_info.items()
            if key not in {"封面文本", "标准编号候选"} and value not in (None, "", [], {})
        }
        context_file_properties.setdefault("标准中文名称", file_name)
        file_id = self.add_node(file_name, "标准文件", context_file_properties, source="first_stage_context")
        for root in parse_result.get("body_tree", []) or []:
            self._add_context_tree_node(root, file_id, parse_result, parent_id=None)
        for root in parse_result.get("appendix_tree", []) or []:
            self._add_context_tree_node(root, file_id, parse_result, parent_id=None)

    def add_llm_extractions(self, llm_extractions: list[dict[str, Any]]) -> None:
        for item in llm_extractions:
            for entity in item.get("entities", []) or []:
                name = entity.get("name") or ""
                entity_type = entity.get("type") or ""
                if not name or not entity_type:
                    continue
                entity["properties"] = self._clean_entity_properties(entity.get("properties", {}) or {})
                if entity_type == "图片":
                    # 图片节点只由第一阶段真实图片资源生成，二阶段 LLM 不新增图片节点。
                    continue
                if entity_type == "流程图" and not self._is_valid_flowchart(entity.get("properties", {})):
                    continue
                if entity_type == "标准文件":
                    if not self._is_current_standard_file_entity(name, entity.get("properties", {})):
                        self._alias_to_standard_file(name, entity.get("properties", {}))
                        if self.result_stats:
                            self.result_stats.strong_warning += 1
                            self.result_stats.strong_warning_msg += (
                                f"跳过疑似引用文件覆盖标准文件根节点: {self.filename} llm_name={name}\n"
                            )
                        continue
                    self._normalize_standard_file_properties(entity)
                if entity_type == "标准要求":
                    if not self._normalize_requirement_entity(entity, item):
                        if self.result_stats:
                            self.result_stats.strong_warning += 1
                            self.result_stats.strong_warning_msg += (
                                f"跳过不完整或非规范性标准要求: {self.filename} name={name}\n"
                            )
                        continue
                    name = entity.get("name") or name
                if entity_type == "引用标准":
                    self._mark_internal_reference(entity)
                    internal_id = self._resolve_internal_reference_entity_id(entity)
                    if internal_id:
                        self._register_entity_aliases(internal_id, name, entity_type, entity.get("properties", {}))
                        continue
                existing_id = self._resolve_resource_node_id(name, entity_type, entity.get("properties", {}))
                filtered_properties = self._filter_entity_properties_by_schema(
                    entity_type,
                    entity.get("properties", {}) or {},
                    source="llm",
                )
                entity["properties"] = filtered_properties
                if existing_id:
                    self._update_node_by_id(existing_id, filtered_properties, source="llm")
                    self._register_entity_aliases(existing_id, name, entity_type, filtered_properties)
                else:
                    self.add_node(name, entity_type, filtered_properties, source="llm")
                if entity_type == "标准要求" and self.result_stats:
                    properties = entity.get("properties", {}) or {}
                    qiangdu = (properties.get("约束强度") or "").strip()
                    # 强警告: 约束强度不明确 (描述性文本误分类)
                    if qiangdu == "不明确":
                        self.result_stats.strong_warning += 1
                        self.result_stats.strong_warning_msg += (
                            f"标准要求约束强度为'不明确'(疑似描述性文本误分类): {self.filename} name={name}\n"
                        )
                    # 强警告: 约束强度原文缺失
                    normative_qiangdu = {"强制", "建议", "无约束"}
                    if qiangdu in normative_qiangdu:
                        qiangdu_yuanwen = (properties.get("约束强度原文") or "").strip()
                        if not qiangdu_yuanwen:
                            self.result_stats.strong_warning += 1
                            self.result_stats.strong_warning_msg += (
                                f"标准要求约束强度原文为空(约束强度={qiangdu}): {self.filename} name={name}\n"
                            )
                    # 弱警告: 行为描述过短 (2-4字)
                    behavior_desc = (properties.get("行为描述") or "").strip()
                    if 2 <= len(behavior_desc) <= 4:
                        self.result_stats.week_warning += 1
                        self.result_stats.week_warning_msg += (
                            f"标准要求行为描述过短(len={len(behavior_desc)}): {self.filename} name={name} value='{behavior_desc}'\n"
                        )
                    # 弱警告: name与要求内容相同或仅去标点
                    requirement_content = (properties.get("要求内容") or "").strip()
                    entity_name_stripped = name.strip()
                    if requirement_content and entity_name_stripped:
                        punct = string.punctuation + "，。！？；：""''（）【】《》…—・"
                        content_no_punct = requirement_content.translate(str.maketrans("", "", punct)).strip()
                        name_no_punct = entity_name_stripped.translate(str.maketrans("", "", punct)).strip()
                        if entity_name_stripped == requirement_content or name_no_punct == content_no_punct:
                            self.result_stats.week_warning += 1
                            self.result_stats.week_warning_msg += (
                                f"标准要求name与要求内容相同: {self.filename} name={name}\n"
                            )

        for item in llm_extractions:
            for relation in item.get("relations", []) or []:
                props = relation.get("properties", {}) or {}
                source_key = props.get("主体") or relation.get("source") or ""
                target_key = props.get("客体") or relation.get("target") or ""
                relation_type = props.get("谓词") or relation.get("type") or "关联"
                source_id = self._resolve_node_id(source_key)
                target_id = self._resolve_node_id(target_key)
                if source_id and target_id:
                    self.add_edge(source_id, target_id, relation_type, props, source="llm")

    def _add_context_tree_node(
        self,
        node: dict[str, Any],
        file_id: str,
        parse_result: dict[str, Any],
        parent_id: str | None,
    ) -> None:
        node_name = self._tree_node_name(node)
        node_id = self.add_node(
            node_name,
            "标准结构节点",
            {
                "节点编号": node.get("number", ""),
                "节点标题": node.get("title", ""),
                "节点层级": node.get("level", ""),
                "节点路径": node.get("path", ""),
                "完整路径标题": node.get("full_path_title", ""),
                "节点内容": node.get("content", ""),
                "是否附录节点": node.get("is_appendix_node", False),
                "附录编号": node.get("appendix_code", ""),
                "附录类型": node.get("appendix_type", ""),
            },
            source="first_stage_context",
        )
        if node.get("number"):
            self.name_to_id[str(node.get("number"))] = node_id
        if node.get("full_path_title"):
            self.name_to_id[str(node.get("full_path_title"))] = node_id
        self.add_edge(parent_id or file_id, node_id, "包含", {"来源": "first_stage_context"}, source="first_stage_context")
        self._add_resource_context(node, node_id, parse_result)
        for child in node.get("children", []) or []:
            self._add_context_tree_node(child, file_id, parse_result, node_id)

    def _add_resource_context(self, node: dict[str, Any], node_id: str, parse_result: dict[str, Any]) -> None:
        table_map = {table.get("table_id"): table for table in parse_result.get("tables", []) or []}
        for table_id in node.get("tables", []) or []:
            table = table_map.get(table_id)
            if not table:
                continue
            name = table.get("table_caption") or table.get("table_number") or f"表格_{table_id}"
            table_node_id = self.add_node(
                name,
                "表格",
                {
                    "表号": table.get("table_number", ""),
                    "表题": table.get("table_caption", ""),
                    "表格内容": table.get("table_markdown", ""),
                    "表格描述": "",
                    "表格HTML": table.get("table_html", ""),
                    "解析行": table.get("parsed_rows", []),
                    "table_id": table_id,
                },
                source="first_stage_context",
            )
            self.add_edge(node_id, table_node_id, "包含", {"来源": "first_stage_context"}, source="first_stage_context")
            self.name_to_id[str(table_id)] = table_node_id
            self.name_to_id[f"table_id_{table_id}"] = table_node_id
            table_content_key = self._normalize_resource_content(table.get("table_markdown", "") or table.get("table_html", ""))
            if table_content_key:
                self.table_content_to_id[table_content_key] = table_node_id

        image_map = {image.get("image_id"): image for image in parse_result.get("images", []) or []}
        for image_id in node.get("images", []) or []:
            image = image_map.get(image_id)
            if not image:
                continue
            image_url = image.get("minio_url") or image.get("online_path") or image.get("local_path", "")
            if not image_url:
                continue
            self._append_node_list_property(node_id, "相关图片地址", str(image_url))
            name = image.get("caption") or image.get("local_path") or f"图片_{image_id}"
            image_node_id = self.add_node(
                name,
                "图片",
                {
                    "图片标题": image.get("caption", ""),
                    "图片链接": image_url,
                    "图片类型": image.get("image_type", ""),
                    "image_id": image_id,
                },
                source="first_stage_context",
            )
            self.name_to_id[str(image_id)] = image_node_id
            self.name_to_id[f"image_id_{image_id}"] = image_node_id
            self.add_edge(node_id, image_node_id, "包含", {"来源": "first_stage_context"}, source="first_stage_context")

    def add_node(
        self,
        node_name: str,
        node_type: str,
        properties: dict[str, Any] | None = None,
        source: str = "llm",
    ) -> str:
        filtered_properties = self._filter_entity_properties_by_schema(node_type, properties or {}, source=source)
        if node_type == "标准文件":
            for key, node in self.nodes.items():
                if key[0] == "标准文件":
                    self._merge_node_properties(node, filtered_properties)
                    if source == "llm":
                        node["node_name"] = node_name or node["node_name"]
                        node["source"] = "llm"
                    self.name_to_id[node_name] = node["node_id"]
                    self.name_to_id[f"{node_type}_{node_name}"] = node["node_id"]
                    return node["node_id"]
        key = (node_type, node_name)
        if key in self.nodes:
            self._merge_node_properties(self.nodes[key], filtered_properties)
            if source == "llm":
                self.nodes[key]["source"] = "llm"
            return self.nodes[key]["node_id"]
        node_id = f"{node_type}_{generate_hex_uuid()}"
        node = {
            "node_id": node_id,
            "node_name": node_name,
            "node_type": node_type,
            "properties": filtered_properties,
            "filename": self.filename,
            "source": source,
        }
        self.nodes[key] = node
        self.name_to_id[node_name] = node_id
        self.name_to_id[f"{node_type}_{node_name}"] = node_id
        return node_id

    def _update_node_by_id(self, node_id: str, properties: dict[str, Any] | None = None, source: str = "llm") -> None:
        for node in self.nodes.values():
            if node.get("node_id") == node_id:
                self._merge_node_properties(
                    node,
                    self._filter_entity_properties_by_schema(
                        str(node.get("node_type") or ""),
                        properties or {},
                        source=source,
                    ),
                )
                if source == "llm":
                    node["source"] = "first_stage_context+llm" if node.get("source") == "first_stage_context" else "llm"
                return

    def _filter_entity_properties_by_schema(
        self,
        entity_type: str,
        properties: dict[str, Any],
        source: str = "llm",
    ) -> dict[str, Any]:
        allowed = set(NODE_ALLOWED_PROPERTIES.get(entity_type) or set())
        allowed.update(PROPERTY_ALLOW_OVERRIDES.get(entity_type, set()))
        if not allowed:
            if properties and self.result_stats:
                self.result_stats.strong_warning += 1
                self.result_stats.strong_warning_msg += (
                    f"节点类型未配置属性白名单: {self.filename} entity_type={entity_type}\n"
                )
            return {}

        filtered = {
            key: value
            for key, value in (properties or {}).items()
            if key in allowed and value not in (None, "", [], {})
        }
        dropped = [
            str(key)
            for key, value in (properties or {}).items()
            if key not in allowed and value not in (None, "", [], {})
        ]
        if dropped and source == "llm" and self.result_stats:
            self.result_stats.week_warning += 1
            preview = ", ".join(key[:80] for key in sorted(dropped)[:10])
            self.result_stats.week_warning_msg += (
                f"丢弃schema外属性: {self.filename} entity_type={entity_type} keys={preview}\n"
            )
        return filtered

    @staticmethod
    def _merge_node_properties(node: dict[str, Any], properties: dict[str, Any]) -> None:
        target = node.setdefault("properties", {})
        for key, value in (properties or {}).items():
            if value in (None, "", [], {}):
                continue
            target[key] = value

    def _append_node_list_property(self, node_id: str, property_name: str, value: Any) -> None:
        if value in (None, ""):
            return
        for node in self.nodes.values():
            if node.get("node_id") != node_id:
                continue
            items = node["properties"].setdefault(property_name, [])
            if not isinstance(items, list):
                items = [items]
                node["properties"][property_name] = items
            if value not in items:
                items.append(value)
            return

    def _register_entity_aliases(
        self,
        node_id: str,
        name: str,
        entity_type: str,
        properties: dict[str, Any] | None = None,
    ) -> None:
        props = properties or {}
        aliases = [
            name,
            f"{entity_type}_{name}",
            f"{entity_type} {name}",
            props.get("标准编号"),
            props.get("标准名称"),
            props.get("节点编号"),
            props.get("节点路径"),
            props.get("完整路径标题"),
        ]
        for alias in aliases:
            if alias:
                self.name_to_id[str(alias)] = node_id

    def _resolve_resource_node_id(
        self,
        node_name: str,
        node_type: str,
        properties: dict[str, Any] | None = None,
    ) -> str:
        if node_type not in {"标准结构节点", "流程图", "表格"}:
            return ""
        props = properties or {}
        for key in (
            props.get("节点编号"),
            props.get("节点路径"),
            props.get("完整路径标题"),
            props.get("table_id"),
            props.get("表格ID"),
            node_name,
            f"{props.get('节点编号', '')} {props.get('节点标题', '')}".strip(),
        ):
            if not key:
                continue
            value = str(key)
            resolved = self._resolve_node_id(value)
            if resolved:
                return resolved
            if value.startswith("image_id_"):
                resolved = self._resolve_node_id(value.removeprefix("image_id_"))
                if resolved:
                    return resolved
        if node_type == "表格":
            table_content_key = self._normalize_resource_content(props.get("表格内容", ""))
            if table_content_key and table_content_key in self.table_content_to_id:
                return self.table_content_to_id[table_content_key]
        return ""

    @staticmethod
    def _normalize_resource_content(value: Any) -> str:
        return "".join(str(value or "").split())

    @staticmethod
    def _clean_entity_properties(properties: dict[str, Any]) -> dict[str, Any]:
        cleaned: dict[str, Any] = {}
        for key, value in (properties or {}).items():
            key_text = str(key or "").strip()
            if not key_text:
                continue
            if key_text in {"filename", "文件名", "国家标准文件名"}:
                continue
            cleaned[key_text] = value
        return cleaned

    def _normalize_standard_file_properties(self, entity: dict[str, Any]) -> None:
        props = entity.setdefault("properties", {})
        standard_name = str(props.get("标准中文名称") or entity.get("name") or self.filename).strip()
        if not standard_name:
            standard_name = self.filename
        if not props.get("标准中文名称"):
            props["标准中文名称"] = standard_name
        entity["name"] = standard_name

    def _is_current_standard_file_entity(self, name: str, properties: dict[str, Any] | None = None) -> bool:
        filename_text = str(self.filename or "").strip()
        filename_key = _normalize_match_text(filename_text)
        if not filename_key:
            return True
        candidates = [
            name,
            (properties or {}).get("标准中文名称"),
        ]
        for candidate in candidates:
            candidate_text = str(candidate or "").strip()
            candidate_key = _normalize_match_text(candidate_text)
            if not candidate_key:
                continue
            if self._has_conflicting_part_number(filename_text, candidate_text):
                continue
            if filename_key in candidate_key or candidate_key in filename_key:
                return True
            if SequenceMatcher(None, filename_key, candidate_key).ratio() >= 0.72:
                return True
        return False

    @staticmethod
    def _has_conflicting_part_number(left: str, right: str) -> bool:
        part_re = re.compile(r"第\s*(\d+(?:[-.]\d+)?)\s*部分")
        left_match = part_re.search(str(left or ""))
        right_match = part_re.search(str(right or ""))
        return bool(left_match and right_match and left_match.group(1) != right_match.group(1))

    def _alias_to_standard_file(self, name: str, properties: dict[str, Any] | None = None) -> None:
        standard_file_id = self._standard_file_id()
        if standard_file_id:
            self._register_entity_aliases(standard_file_id, name, "标准文件", properties or {})

    def _standard_file_id(self) -> str:
        for key, node in self.nodes.items():
            if key[0] == "标准文件":
                return str(node.get("node_id") or "")
        return ""

    def _normalize_requirement_entity(self, entity: dict[str, Any], extraction: dict[str, Any]) -> bool:
        props = {
            key: value
            for key, value in (entity.get("properties", {}) or {}).items()
            if key in STANDARD_REQUIREMENT_ALLOWED_PROPS
        }
        content = re.sub(r"\s+", " ", str(props.get("要求内容") or "").strip())
        if not content:
            return False
        if LIST_ITEM_ONLY_RE.fullmatch(content) and not NORMATIVE_MODAL_RE.search(content):
            return False
        if not NORMATIVE_MODAL_RE.search(content):
            return False

        normalized_strength, source_text = _normalize_constraint_strength(props.get("约束强度"), content)
        props["约束强度"] = normalized_strength
        if normalized_strength == "不明确":
            return False

        original = str(props.get("约束强度原文") or "").strip()
        if not original or len(_normalize_match_text(original)) <= 3:
            props["约束强度原文"] = source_text or content

        behavior = re.sub(r"\s+", " ", str(props.get("行为描述") or "").strip())
        if len(_normalize_match_text(behavior)) <= 4:
            props["行为描述"] = self._derive_behavior_description(content, behavior)
        if len(_normalize_match_text(props.get("行为描述", ""))) <= 4:
            return False

        quant = _normalize_quant_condition(props.get("量化条件"))
        props["量化条件"] = quant
        props["量化特征"] = "定量" if quant else "定性"
        props["要求内容"] = content

        node_number = str(extraction.get("node_number") or "").strip()
        prop_name = str(props.get("name") or "").strip()
        if prop_name and node_number and not prop_name.startswith(node_number):
            props.pop("name", None)
        entity["properties"] = props
        return True

    @staticmethod
    def _derive_behavior_description(requirement_content: str, fallback: str = "") -> str:
        content = re.sub(r"^\s*(?:[a-zA-Z]\)|[a-zA-Z]）|\d+\)|\d+）|[-—–•])\s*", "", requirement_content).strip()
        content = content.strip("。；; ")
        if len(_normalize_match_text(content)) > 4:
            return content
        return fallback

    def _resolve_internal_reference_entity_id(self, entity: dict[str, Any]) -> str:
        props = entity.get("properties", {}) or {}
        if props.get("是否内部引用") != "是":
            return ""
        for value in (props.get("标准编号"), entity.get("name"), props.get("引用位置")):
            resolved = self._resolve_internal_reference_node_id(str(value or ""))
            if resolved:
                return resolved
        return ""

    def _resolve_internal_reference_node_id(self, value: str) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        number_match = re.search(r"(?<![\w/])(\d+(?:\.\d+)*)(?![\w/])", text)
        if number_match:
            resolved = self._resolve_node_id(number_match.group(1))
            if resolved:
                return resolved
        appendix_match = re.search(r"附录\s*([A-Za-zＡ-Ｚａ-ｚ])", text)
        if appendix_match:
            code = appendix_match.group(1)
            for key in (f"附录{code}", f"附录 {code}", f"附录{code.upper()}", f"附录 {code.upper()}"):
                resolved = self._resolve_node_id(key)
                if resolved:
                    return resolved
        return ""

    @staticmethod
    def _is_valid_flowchart(properties: dict[str, Any] | None) -> bool:
        props = properties or {}
        text = "\n".join(
            str(props.get(key, "") or "")
            for key in ("流程图语法类型", "流程图内容")
        ).lower()
        return any(
            marker in text
            for marker in (
                "```mermaid",
                "flowchart ",
                "graph td",
                "graph lr",
                "graph bt",
                "graph rl",
                "sequencediagram",
                "statediagram",
                "classdiagram",
                "erdiagram",
            )
        )

    @staticmethod
    def _mark_internal_reference(entity: dict[str, Any]) -> None:
        props = entity.setdefault("properties", {})
        number = str(props.get("标准编号") or entity.get("name") or "")
        is_internal = bool(
            re.fullmatch(r"\d+(?:\.\d+)*", number.strip())
            or re.fullmatch(r"附录\s*[A-Za-zＡ-Ｚａ-ｚ]", number.strip())
        )
        props["是否内部引用"] = "是" if is_internal else "否"
        if is_internal and str(entity.get("name", "")).upper().startswith(("GB/T ", "GB ", "SJ/T ", "SJ/Z ")):
            entity["name"] = number.strip()

    def add_edge(
        self,
        source_id: str,
        target_id: str,
        relation_type: str,
        properties: dict[str, Any] | None = None,
        source: str = "llm",
    ) -> None:
        if not source_id or not target_id:
            return
        for existing in self.edges:
            if (
                existing.get("source_id") == source_id
                and existing.get("target_id") == target_id
                and existing.get("relation_type") == relation_type
            ):
                existing["properties"].update(properties or {})
                if source and source not in str(existing.get("source", "")):
                    existing["source"] = f"{existing.get('source')}+{source}"
                return
        edge = {
            "source_id": source_id,
            "target_id": target_id,
            "relation_type": relation_type,
            "directionality": "单向",
            "properties": properties or {},
            "filename": self.filename,
            "source": source,
        }
        if edge not in self.edges:
            self.edges.append(edge)

    def _resolve_node_id(self, key: str) -> str:
        value = str(key or "")
        return self.name_to_id.get(value) or self.name_to_id.get(value.split("_", 1)[-1], "")

    def to_graph(self) -> dict[str, Any]:
        return {"nodes": list(self.nodes.values()), "edges": self.edges}

    def validate_and_report(self) -> None:
        """Post-build validation: check standard file info for missing key fields."""
        if not self.result_stats:
            return
        for key, node in self.nodes.items():
            if key[0] != "标准文件":
                continue
            props = node.get("properties", {}) or {}
            missing = []
            if not (props.get("标准编号") or "").strip():
                missing.append("标准编号")
            if not (props.get("标准性质") or "").strip():
                missing.append("标准性质")
            if missing:
                self.result_stats.strong_warning += 1
                self.result_stats.strong_warning_msg += (
                    f"标准文件信息缺少关键字段{missing}: {self.filename}\n"
                )
            break

    @staticmethod
    def _tree_node_name(node: dict[str, Any]) -> str:
        return f"{node.get('number', '')} {node.get('title', '')}".strip() or node.get("node_id", "未命名节点")


async def extract_national_standard_graph(
    input_path: str | Path,
    output_path: str | Path | None = None,
    max_tree_nodes: int | None = None,
) -> dict[str, Any]:
    extractor = NationalStandardGraphExtractor(max_tree_nodes=max_tree_nodes)
    result = await extractor.extract(input_path)
    if output_path:
        output = Path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return result


async def extract_national_standard_graph_batch(
    input_root_dir: str | Path,
    output_dir: str | Path,
    max_tree_nodes: int | None = None,
) -> dict[str, Any]:
    input_root = Path(input_root_dir)
    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    standard_dirs = _discover_markdown_standard_dirs(input_root)
    if not standard_dirs:
        standard_dirs = [input_root]

    extractor = NationalStandardGraphExtractor(max_tree_nodes=max_tree_nodes)
    summary: dict[str, Any] = {
        "input_root_dir": str(input_root),
        "output_dir": str(output_root),
        "total": len(standard_dirs),
        "success": 0,
        "failed": 0,
        "skipped_validation_failed": 0,
        "items": [],
    }
    for standard_dir in standard_dirs:
        output_path = output_root / f"{_sanitize_minio_path_part(standard_dir.name)}.json"
        try:
            result = await extractor.extract(standard_dir)
            output_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
            status = result.get("status")
            if status == "success":
                summary["success"] += 1
            else:
                summary["failed"] += 1
                if status == "skipped_validation_failed":
                    summary["skipped_validation_failed"] += 1
            summary["items"].append(
                {
                    "name": standard_dir.name,
                    "status": status,
                    "output_path": str(output_path),
                    "node_count": result.get("stats", {}).get("node_count", 0),
                    "edge_count": result.get("stats", {}).get("edge_count", 0),
                    "validation_passed": result.get("stats", {}).get("validation_passed", False),
                    "llm_call_count": result.get("stats", {}).get("llm_call_count", 0),
                    "llm_failed_count": result.get("stats", {}).get("llm_failed_count", 0),
                    "extraction_mode": result.get("extraction_mode", ""),
                }
            )
        except Exception as exc:
            logging.exception("国家标准二阶段抽取失败: %s", standard_dir)
            summary["failed"] += 1
            summary["items"].append(
                {
                    "name": standard_dir.name,
                    "status": "failed",
                    "output_path": str(output_path),
                    "error": str(exc),
                }
            )
    summary_path = output_root / "summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    summary["summary_path"] = str(summary_path)
    return summary


def extract_national_standard_graph_batch_sync(
    input_root_dir: str | Path,
    output_dir: str | Path,
    max_tree_nodes: int | None = None,
) -> dict[str, Any]:
    return asyncio.run(extract_national_standard_graph_batch(input_root_dir, output_dir, max_tree_nodes=max_tree_nodes))
