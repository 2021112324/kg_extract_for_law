"""Two-stage LangExtract pipeline for point-form compliance guides."""

from __future__ import annotations

import asyncio
import json
import logging
import sys
from pathlib import Path
from typing import Any

from .config import DEFAULT_CONFIG, GuideP1Config
from .graph_builder import GuideP1GraphBuilder, prepare_guide_p1_kg_for_neo4j
from .io_utils import discover_input_files, load_text, save_json, stage_output_path
from .prompt import (
    example_for_content_block,
    example_for_file_info,
    prompt_for_content_block,
    prompt_for_file_info,
    schema_for_content_block,
    schema_for_file_info,
)
from .stats import ResultStats
from .structure_parser import GuideP1StructureParser
from .validator import validate_extraction_result


logger = logging.getLogger(__name__)


class GuideP1Extractor:
    def __init__(
        self,
        config: GuideP1Config | None = None,
        llm_extractor: Any | None = None,
    ) -> None:
        self.config = config or DEFAULT_CONFIG
        self.structure_parser = GuideP1StructureParser(self.config)
        self.semaphore = asyncio.Semaphore(self.config.max_concurrent)
        self._llm_extractor = llm_extractor
        self.result_stats = ResultStats()

    def _get_llm_extractor(self) -> Any:
        if self._llm_extractor is not None:
            return self._llm_extractor
        self._configure_console_errors()
        from app.infrastructure.information_extraction.factory import InformationExtractionFactory
        from app.infrastructure.information_extraction.method.base import LangextractConfig

        llm_config = LangextractConfig(
            model_name=self.config.model_name,
            api_key=self.config.api_key,
            api_url=self.config.api_url,
            config={"timeout": self.config.timeout, "extraction_timeout": self.config.timeout},
            max_char_buffer=self.config.max_char_buffer,
            batch_length=self.config.batch_length,
            max_workers=self.config.max_workers,
        )
        self._llm_extractor = InformationExtractionFactory.create(
            "langextract",
            max_retries=self.config.max_retries,
            config=llm_config,
        )
        return self._llm_extractor

    @staticmethod
    def _configure_console_errors() -> None:
        """Prevent third-party progress output from aborting on legacy Windows encodings."""
        for stream in (sys.stdout, sys.stderr):
            reconfigure = getattr(stream, "reconfigure", None)
            if callable(reconfigure):
                try:
                    reconfigure(errors="replace")
                except (OSError, ValueError):
                    pass

    @staticmethod
    def build_file_info_input(parse_result: dict[str, Any]) -> str:
        payload = {
            "filename": parse_result.get("filename", ""),
            "explicit_risk_types": (parse_result.get("metadata_candidates") or {}).get("risk_types", []),
            "metadata_candidates": parse_result.get("metadata_candidates", {}),
            "file_header": parse_result.get("file_header", ""),
        }
        return json.dumps(payload, ensure_ascii=False, indent=2)

    @staticmethod
    def build_content_block_input(parse_result: dict[str, Any], block: dict[str, Any]) -> str:
        payload = {
            "filename": parse_result.get("filename", ""),
            "risk_types": (parse_result.get("metadata_candidates") or {}).get("risk_types", []),
            "source_block_id": block.get("block_id", ""),
            "source_path": block.get("source_path", ""),
            "ancestor_context": block.get("ancestor_context", ""),
            "line_start": block.get("line_start"),
            "line_end": block.get("line_end"),
            "text": block.get("text", ""),
        }
        return json.dumps(payload, ensure_ascii=False, indent=2)

    async def _extract_file_info(self, parse_result: dict[str, Any]) -> dict[str, Any]:
        input_text = self.build_file_info_input(parse_result)
        async with self.semaphore:
            raw = await self._get_llm_extractor().entity_and_relationship_extract(
                user_prompt=prompt_for_file_info,
                schema=schema_for_file_info,
                input_text=input_text,
                examples=example_for_file_info,
            )
        validated = validate_extraction_result(raw, source_text=parse_result.get("file_header", ""))
        has_document = any(item.get("entity_type") == "合规指引文件" for item in validated["entities"])
        status = "success" if has_document and validated.get("valid") else "failed"
        return {
            "scope": "file_info",
            "status": status,
            "input_text": input_text,
            "validated": validated,
            "raw": self._to_plain(raw),
            "error": "文件级抽取未返回合规指引文件实体" if not has_document else "",
        }

    async def _extract_content_block(self, parse_result: dict[str, Any], block: dict[str, Any]) -> dict[str, Any]:
        input_text = self.build_content_block_input(parse_result, block)
        async with self.semaphore:
            raw = await self._get_llm_extractor().entity_and_relationship_extract(
                user_prompt=prompt_for_content_block,
                schema=schema_for_content_block,
                input_text=input_text,
                examples=example_for_content_block,
            )
        context_types = {
            f"{block.get('source_path', '')}": "指引结构节点",
        }
        validated = validate_extraction_result(
            raw,
            context_entity_types=context_types,
            source_text=block.get("text", ""),
        )
        has_content = bool(validated["entities"] or validated["relations"])
        status = "success" if has_content else "empty"
        return {
            "scope": "content_block",
            "status": status,
            "block": block,
            "input_text": input_text,
            "validated": validated,
            "raw": self._to_plain(raw),
        }

    async def extract_from_parse_result(
        self,
        parse_result: dict[str, Any],
        run_llm: bool = True,
        max_blocks: int | None = None,
    ) -> dict[str, Any]:
        filename = parse_result.get("filename", "")
        stats = ResultStats(total_files=1)
        stats.structure_node_count = len(parse_result.get("structure_nodes", []) or [])
        stats.skipped_attachment_count = len(parse_result.get("skipped_attachments", []) or [])
        parse_passed = bool((parse_result.get("validation") or {}).get("passed"))
        blocks = list(parse_result.get("semantic_blocks", []) or [])
        is_partial_test = max_blocks is not None and max_blocks < len(blocks)
        if max_blocks is not None:
            blocks = blocks[: max(0, max_blocks)]
        stats.planned_block_count = len(blocks)

        extraction_result: dict[str, Any] = {
            "filename": filename,
            "mode": "llm" if run_llm else "structure_only",
            "is_partial_test": is_partial_test,
            "file_info": {},
            "content_blocks": [],
            "failed_blocks": [],
        }
        if not parse_passed:
            stats.add_file_processing_error(f"{filename}: 第一阶段结构校验未通过")

        if run_llm and parse_passed:
            try:
                file_info = await self._extract_file_info(parse_result)
                extraction_result["file_info"] = file_info
                if file_info["status"] == "success":
                    stats.llm_success_count += 1
                else:
                    stats.add_extraction_error(f"{filename}: {file_info.get('error') or '文件级抽取失败'}")
            except Exception as exc:
                message = f"{filename}: 文件级 LLM 抽取失败: {exc}"
                stats.add_extraction_error(message)
                extraction_result["file_info"] = {
                    "scope": "file_info",
                    "status": "failed",
                    "error": str(exc),
                    "input_text": self.build_file_info_input(parse_result),
                    "validated": {"entities": [], "relations": [], "warnings": [], "errors": [str(exc)]},
                }

            tasks = [self._extract_content_block(parse_result, block) for block in blocks]
            results = await asyncio.gather(*tasks, return_exceptions=True) if tasks else []
            for block, result in zip(blocks, results):
                if isinstance(result, Exception):
                    message = f"{filename} {block.get('block_id')}: 内容块 LLM 抽取失败: {result}"
                    stats.add_extraction_error(message)
                    failed = {"scope": "content_block", "status": "failed", "block": block, "error": str(result)}
                    extraction_result["content_blocks"].append(failed)
                    extraction_result["failed_blocks"].append(failed)
                    continue
                extraction_result["content_blocks"].append(result)
                if result["status"] == "success":
                    stats.llm_success_count += 1
                else:
                    stats.llm_empty_count += 1
                    if block.get("high_knowledge_signal"):
                        stats.add_strong_warning(f"{filename} {block.get('block_id')}: 高知识信号内容块返回空结果")
                for warning in (result.get("validated") or {}).get("warnings", []):
                    stats.add_weak_warning(f"{filename} {block.get('block_id')}: {warning}")

        graph = GuideP1GraphBuilder(filename, parse_result).build(extraction_result if run_llm else None)
        for warning in (graph.get("metadata") or {}).get("warnings", []):
            if warning and not any(message.endswith(warning) for message in stats.weak_warning_messages):
                stats.add_weak_warning(f"{filename}: {warning}")
        stats.knowledge_unit_count = sum(1 for node in graph["nodes"] if node["node_type"] == "指引知识单元")
        stats.node_count = len(graph["nodes"])
        stats.edge_count = len(graph["edges"])
        failed = bool(extraction_result.get("failed_blocks")) or (
            run_llm and (extraction_result.get("file_info") or {}).get("status") != "success"
        )
        if not run_llm:
            status = "structure_only"
        elif not parse_passed:
            status = "failed"
        elif failed and self.config.strict_mode:
            status = "incomplete"
        elif is_partial_test:
            status = "partial_test"
        else:
            status = "success"
        formal_graph_eligible = status == "success"
        if formal_graph_eligible:
            stats.success_files = 1
        elif status in {"incomplete", "partial_test", "structure_only"}:
            stats.incomplete_files = 1

        extraction_result["stats"] = {
            "planned_block_count": len(blocks),
            "successful_block_count": sum(1 for item in extraction_result["content_blocks"] if item.get("status") == "success"),
            "empty_block_count": sum(1 for item in extraction_result["content_blocks"] if item.get("status") == "empty"),
            "failed_block_count": len(extraction_result["failed_blocks"]),
        }
        self.result_stats = stats
        return {
            "filename": filename,
            "status": status,
            "formal_graph_eligible": formal_graph_eligible,
            "strict_mode": self.config.strict_mode,
            "parse_result": parse_result,
            "extraction_result": extraction_result,
            "graph": graph,
            "neo4j_graph": prepare_guide_p1_kg_for_neo4j(graph),
            "run_stats": stats.to_dict(),
        }

    async def extract_text(
        self,
        text: str,
        filename: str = "memory.txt",
        run_llm: bool = True,
        max_blocks: int | None = None,
    ) -> dict[str, Any]:
        parse_result = self.structure_parser.parse_text(text, filename=filename)
        return await self.extract_from_parse_result(parse_result, run_llm=run_llm, max_blocks=max_blocks)

    async def extract_file(
        self,
        input_path: str | Path,
        output_dir: str | Path | None = None,
        run_llm: bool = True,
        max_blocks: int | None = None,
    ) -> dict[str, Any]:
        path = Path(input_path)
        parse_result = self.structure_parser.parse_text(load_text(path), filename=path.name, source_path=str(path))
        result = await self.extract_from_parse_result(parse_result, run_llm=run_llm, max_blocks=max_blocks)
        if output_dir:
            save_json(parse_result, stage_output_path(path, output_dir, "split"))
            save_json(result["extraction_result"], stage_output_path(path, output_dir, "extraction"))
            save_json(
                {
                    "filename": result["filename"],
                    "status": result["status"],
                    "formal_graph_eligible": result["formal_graph_eligible"],
                    **result["graph"],
                    "neo4j_graph": result["neo4j_graph"],
                },
                stage_output_path(path, output_dir, "kg"),
            )
            save_json(result["run_stats"], stage_output_path(path, output_dir, "summary"))
        self.log_result_stats()
        return result

    def log_result_stats(self) -> None:
        stats = self.result_stats
        logger.info("guide_p1 extraction statistics")
        logger.info("files=%s success=%s incomplete=%s", stats.total_files, stats.success_files, stats.incomplete_files)
        logger.info(
            "errors=%s extraction_errors=%s file_errors=%s storage_errors=%s",
            stats.error,
            stats.extraction_error,
            stats.file_processing_error,
            stats.storage_error,
        )
        logger.info("weak_warnings=%s strong_warnings=%s", stats.weak_warning, stats.strong_warning)

    @staticmethod
    def _to_plain(value: Any) -> Any:
        if hasattr(value, "model_dump"):
            return value.model_dump()
        if hasattr(value, "dict"):
            return value.dict()
        if isinstance(value, dict):
            return {key: GuideP1Extractor._to_plain(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [GuideP1Extractor._to_plain(item) for item in value]
        return value


async def extract_guide_p1_batch(
    input_path: str | Path,
    output_dir: str | Path,
    config: GuideP1Config | None = None,
    run_llm: bool = True,
    max_files: int | None = None,
    max_blocks_per_file: int | None = None,
    llm_extractor: Any | None = None,
) -> dict[str, Any]:
    effective_config = config or DEFAULT_CONFIG
    files = discover_input_files(input_path, effective_config.supported_suffixes)
    if max_files is not None:
        files = files[: max(0, max_files)]
    batch_stats = ResultStats()
    items = []
    extractor = GuideP1Extractor(config=effective_config, llm_extractor=llm_extractor)
    for path in files:
        try:
            result = await extractor.extract_file(
                path,
                output_dir=output_dir,
                run_llm=run_llm,
                max_blocks=max_blocks_per_file,
            )
            batch_stats.merge(extractor.result_stats)
            items.append(
                {
                    "filename": path.name,
                    "status": result["status"],
                    "formal_graph_eligible": result["formal_graph_eligible"],
                    "run_stats": result["run_stats"],
                }
            )
        except Exception as exc:
            file_stats = ResultStats(total_files=1)
            file_stats.add_file_processing_error(f"{path.name}: {exc}")
            batch_stats.merge(file_stats)
            items.append({"filename": path.name, "status": "failed", "error": str(exc)})
            logger.exception("guide_p1 file extraction failed: %s", path)
    summary = {
        "input_path": str(input_path),
        "output_dir": str(output_dir),
        "run_llm": run_llm,
        "items": items,
        "stats": batch_stats.to_dict(),
    }
    summary["summary_path"] = save_json(summary, Path(output_dir) / "summary.json")
    return summary
