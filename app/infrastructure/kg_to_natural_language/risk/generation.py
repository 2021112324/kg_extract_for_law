"""按三级风险指标生成映射文件知识条目的阶段一流程。"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import shutil
import threading
import time
import uuid
from collections import defaultdict, deque
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from ..converters.base import convert_json_to_text
from ..converters.compliance_guide import convert_compliance_guide_json_to_text
from ..converters.english_law import convert_english_law_json_to_text
from ..converters.national_standard import convert_national_standard_json_to_text
from ..converters.chinese_law import convert_json_to_text_v2
from .graph_query import GraphFilePartition, query_mapped_file_subgraphs
from .config import DEFAULT_RISK_TEMP_ROOT, DEFAULT_RISK_TREE_PATH
from .indicator_tree import IndicatorContext, MappedFile, load_indicator_context
from ..batch.categories import TYPED_KNOWLEDGE_CATEGORIES


logger = logging.getLogger(__name__)
_GENERATION_LOCK = threading.Lock()


class RiskKnowledgeGenerationError(RuntimeError):
    """阶段一知识生成失败。"""


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as file:
        json.dump(value, file, ensure_ascii=False, indent=2)
        file.write("\n")


def _safe_filename(value: str, *, fallback: str = "knowledge") -> str:
    name = str(value or "").replace("\\", "/").rsplit("/", 1)[-1]
    name = re.sub(r"\.[^.]{1,10}$", "", name).strip()
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", name)
    name = re.sub(r"\s+", " ", name).strip(" .") or fallback
    if name.upper() in {
        "CON",
        "PRN",
        "AUX",
        "NUL",
        *(f"COM{index}" for index in range(1, 10)),
        *(f"LPT{index}" for index in range(1, 10)),
    }:
        name = f"_{name}"
    return name[:120].rstrip(" .") or fallback


def _normalized_knowledge_text(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _knowledge_id(category: str, source_key: str, knowledge: str) -> str:
    raw = "\0".join((category, source_key, _normalized_knowledge_text(knowledge)))
    return f"risk_{hashlib.sha256(raw.encode('utf-8')).hexdigest()[:24]}"


def _converter_for_category(category_name: str) -> Callable[..., dict[str, Any]]:
    converter_names = {
        category.name: category.converter for category in TYPED_KNOWLEDGE_CATEGORIES
    }
    converter_map: dict[str, Callable[..., dict[str, Any]]] = {
        "chinese_law": convert_json_to_text_v2,
        "national_standard": convert_national_standard_json_to_text,
        "english_law": convert_english_law_json_to_text,
        "guide": convert_compliance_guide_json_to_text,
        "generic": convert_json_to_text,
    }
    try:
        return converter_map[converter_names[category_name]]
    except KeyError as exc:
        raise RiskKnowledgeGenerationError(
            f"图谱类别{category_name}没有配置知识转换器"
        ) from exc


def _read_txt_knowledge(path: Path) -> list[str]:
    if not path.is_file():
        raise RiskKnowledgeGenerationError(f"转换器未生成TXT：{path}")
    return [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _read_jsonl_metadata(path_value: Any) -> dict[str, deque[dict[str, Any]]]:
    metadata: dict[str, deque[dict[str, Any]]] = defaultdict(deque)
    if not path_value:
        return metadata
    path = Path(str(path_value))
    if not path.is_file():
        return metadata
    with path.open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            if not line.strip():
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError as exc:
                raise RiskKnowledgeGenerationError(
                    f"转换器JSONL第{line_number}行无法解析：{path}"
                ) from exc
            if not isinstance(item, dict):
                continue
            content = item.get("content") or item.get("knowledge")
            if not content:
                continue
            metadata[_normalized_knowledge_text(str(content))].append(item)
    return metadata


def _convert_partition(
    partition: GraphFilePartition,
    mapped_file: MappedFile,
    indicator: IndicatorContext,
    work_root: Path,
) -> dict[str, Any]:
    partition_hash = hashlib.sha256(
        f"{partition.graph_category}\0{partition.normalized_key}".encode("utf-8")
    ).hexdigest()[:10]
    partition_root = work_root / partition_hash
    graph_dir = partition_root / "json"
    conversion_dir = partition_root / "conversion"
    _write_json(graph_dir / "node.json", partition.node_records)
    _write_json(graph_dir / "edge.json", partition.edge_records)

    converter = _converter_for_category(partition.graph_category)
    try:
        convert_stats = converter(
            partition.graph_category,
            input_dir=graph_dir,
            output_dir=conversion_dir,
        )
    except Exception as exc:
        raise RiskKnowledgeGenerationError(
            f"文件{mapped_file.found_filename}（{partition.graph_category}）转换失败：{exc}"
        ) from exc

    knowledge_lines = _read_txt_knowledge(Path(str(convert_stats.get("txt_path") or "")))
    reported_count = int(convert_stats.get("total_entries", len(knowledge_lines)))
    if reported_count != len(knowledge_lines):
        raise RiskKnowledgeGenerationError(
            f"文件{mapped_file.found_filename}转换统计为{reported_count}条，"
            f"但TXT包含{len(knowledge_lines)}条非空知识"
        )

    jsonl_metadata = _read_jsonl_metadata(convert_stats.get("jsonl_path"))
    mapping_sources = [source.to_dict() for source in mapped_file.mapping_sources]
    graph_filename = partition.graph_filename
    entries: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    duplicate_count = 0
    for knowledge in knowledge_lines:
        identifier = _knowledge_id(
            partition.graph_category,
            partition.normalized_key,
            knowledge,
        )
        if identifier in seen_ids:
            duplicate_count += 1
            continue
        seen_ids.add(identifier)
        metadata_items = jsonl_metadata.get(_normalized_knowledge_text(knowledge))
        converter_metadata = metadata_items.popleft() if metadata_items else None
        if converter_metadata:
            converter_metadata = {
                key: value
                for key, value in converter_metadata.items()
                if key not in {"content", "knowledge"}
            }
        entries.append(
            {
                "knowledge_id": identifier,
                "knowledge": knowledge,
                "source_file": mapped_file.found_filename,
                "graph_filename": graph_filename,
                "graph_category": partition.graph_category,
                "mapping_sources": mapping_sources,
                "converter_metadata": converter_metadata,
                "relevance_score": None,
                "relevance_reason": None,
                "ranking_status": "pending",
                "ranking_error": None,
            }
        )

    return {
        "indicator_number": indicator.indicator_number,
        "indicator_name": indicator.indicator_name,
        "indicator_description": indicator.indicator_description,
        "risk_type": indicator.risk_type,
        "source_file": mapped_file.found_filename,
        "normalized_file_key": mapped_file.normalized_key,
        "graph_category": partition.graph_category,
        "graph_label": partition.graph_label,
        "graph_filename": graph_filename,
        "graph_filenames": list(partition.graph_filenames),
        "node_count": len(partition.node_records),
        "relationship_count": len(partition.edge_records),
        "knowledge_count": len(entries),
        "duplicate_knowledge_count": duplicate_count,
        "mapping_sources": mapping_sources,
        "converter_statistics": {
            key: value
            for key, value in convert_stats.items()
            if not key.endswith("_path")
        },
        "knowledge_entries": entries,
    }


def _publish_directory(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    backup = target.parent / f".{target.name}.backup-{uuid.uuid4().hex}"
    had_previous = target.exists()
    if had_previous:
        os.replace(target, backup)
    try:
        os.replace(source, target)
    except Exception:
        if had_previous and backup.exists():
            os.replace(backup, target)
        raise
    else:
        if backup.exists():
            shutil.rmtree(backup, ignore_errors=True)


def _build_unmatched_files(
    mapped_files: tuple[MappedFile, ...],
    unmatched_keys: set[str],
) -> list[dict[str, Any]]:
    return [
        mapped_file.to_dict()
        for mapped_file in mapped_files
        if mapped_file.normalized_key in unmatched_keys
    ]


def generate_risk_indicator_knowledge(
    indicator_number: str,
    *,
    risk_tree_path: str | Path | None = None,
    output_dir: str | Path | None = None,
) -> dict[str, Any]:
    """生成一个三级风险指标的映射文件知识 JSON。

    该阶段只访问风险树、Neo4j 和本地文件系统，不调用模型，也不初始化
    MySQL、MinIO 或任务数据库。
    """
    started = time.perf_counter()
    tree_path = Path(risk_tree_path) if risk_tree_path is not None else DEFAULT_RISK_TREE_PATH
    indicator = load_indicator_context(
        indicator_number,
        tree_path=tree_path if risk_tree_path is not None else None,
    )
    target_dir = (
        Path(output_dir)
        if output_dir is not None
        else DEFAULT_RISK_TEMP_ROOT / indicator.indicator_number
    )

    with _GENERATION_LOCK:
        query_result = query_mapped_file_subgraphs(indicator.mapped_files)
        mapped_by_key = {
            mapped_file.normalized_key: mapped_file
            for mapped_file in indicator.mapped_files
        }

        staging_parent = target_dir.parent / ".staging"
        run_root = staging_parent / uuid.uuid4().hex
        publish_dir = run_root / "publish"
        work_root = run_root / "work"
        publish_dir.mkdir(parents=True, exist_ok=True)
        work_root.mkdir(parents=True, exist_ok=True)

        try:
            file_results: list[dict[str, Any]] = []
            per_file_paths: list[str] = []
            for partition in query_result.partitions:
                mapped_file = mapped_by_key[partition.normalized_key]
                file_result = _convert_partition(
                    partition,
                    mapped_file,
                    indicator,
                    work_root,
                )
                short_hash = hashlib.sha256(
                    f"{partition.graph_category}\0{partition.normalized_key}".encode("utf-8")
                ).hexdigest()[:8]
                filename = (
                    f"{_safe_filename(mapped_file.found_filename)}_{short_hash}.json"
                )
                _write_json(publish_dir / filename, file_result)
                per_file_paths.append(str(target_dir / filename))
                file_results.append(file_result)

            all_entries: list[dict[str, Any]] = []
            knowledge_by_id: dict[str, dict[str, Any]] = {}
            duplicate_count = 0
            for file_result in file_results:
                for entry in file_result["knowledge_entries"]:
                    identifier = entry["knowledge_id"]
                    existing = knowledge_by_id.get(identifier)
                    if existing is None:
                        copied = dict(entry)
                        copied["original_order"] = len(all_entries)
                        knowledge_by_id[identifier] = copied
                        all_entries.append(copied)
                        continue
                    duplicate_count += 1
                    existing_sources = existing["mapping_sources"]
                    for source in entry["mapping_sources"]:
                        if source not in existing_sources:
                            existing_sources.append(source)

            category_statistics: dict[str, dict[str, int]] = {}
            for file_result in file_results:
                stats = category_statistics.setdefault(
                    file_result["graph_category"],
                    {
                        "matched_file_count": 0,
                        "node_count": 0,
                        "relationship_count": 0,
                        "knowledge_count": 0,
                    },
                )
                stats["matched_file_count"] += 1
                stats["node_count"] += file_result["node_count"]
                stats["relationship_count"] += file_result["relationship_count"]
                stats["knowledge_count"] += file_result["knowledge_count"]

            unmatched_keys = set(query_result.unmatched_keys)
            unmatched_files = _build_unmatched_files(
                indicator.mapped_files,
                unmatched_keys,
            )
            matched_files = [
                {
                    "source_file": result["source_file"],
                    "normalized_file_key": result["normalized_file_key"],
                    "graph_category": result["graph_category"],
                    "graph_filename": result["graph_filename"],
                    "node_count": result["node_count"],
                    "relationship_count": result["relationship_count"],
                    "knowledge_count": result["knowledge_count"],
                    "mapping_sources": result["mapping_sources"],
                }
                for result in file_results
            ]
            all_json = {
                "schema_version": "1.0",
                "stage": "knowledge_generation",
                "status": "success",
                "indicator": indicator.to_dict(),
                "file_summary": {
                    "mapping_reference_count": indicator.mapping_reference_count,
                    "ignored_empty_found_count": indicator.ignored_empty_found_count,
                    "unique_found_file_count": len(indicator.mapped_files),
                    "matched_source_file_count": len(
                        {result["normalized_file_key"] for result in file_results}
                    ),
                    "matched_graph_partition_count": len(file_results),
                    "unmatched_file_count": len(unmatched_files),
                    "converted_file_count": len(file_results),
                    "zero_knowledge_file_count": sum(
                        result["knowledge_count"] == 0 for result in file_results
                    ),
                },
                "matched_files": matched_files,
                "unmatched_files": unmatched_files,
                "cross_label_matches": query_result.cross_label_matches,
                "relationship_issue_count": query_result.relationship_issue_count,
                "queried_labels": query_result.queried_labels,
                "category_statistics": category_statistics,
                "knowledge_count": len(all_entries),
                "duplicate_knowledge_count": duplicate_count,
                "knowledge_entries": all_entries,
                "warnings": list(indicator.warnings),
                "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
            }
            _write_json(publish_dir / "all.json", all_json)
            shutil.rmtree(work_root, ignore_errors=True)
            _publish_directory(publish_dir, target_dir)

            elapsed = round(time.perf_counter() - started, 3)
            response = {
                "status": "success",
                "indicator_number": indicator.indicator_number,
                "indicator_name": indicator.indicator_name,
                "indicator_description": indicator.indicator_description,
                "risk_type": indicator.risk_type,
                **all_json["file_summary"],
                "knowledge_count": len(all_entries),
                "duplicate_knowledge_count": duplicate_count,
                "ranking_success_count": 0,
                "ranking_failure_count": 0,
                "output_directory": str(target_dir.resolve()),
                "all_json_path": str((target_dir / "all.json").resolve()),
                "per_file_json_paths": per_file_paths,
                "elapsed_seconds": elapsed,
            }
            logger.info(
                "风险指标阶段一完成：%s，映射文件%s，命中%s，未命中%s，知识%s",
                indicator.indicator_number,
                len(indicator.mapped_files),
                response["matched_source_file_count"],
                response["unmatched_file_count"],
                response["knowledge_count"],
            )
            return response
        except RiskKnowledgeGenerationError:
            raise
        except Exception as exc:
            raise RiskKnowledgeGenerationError(
                f"三级指标{indicator.indicator_number}阶段一生成失败：{exc}"
            ) from exc
        finally:
            shutil.rmtree(run_root, ignore_errors=True)
            try:
                staging_parent.rmdir()
            except OSError:
                pass


__all__ = ["RiskKnowledgeGenerationError", "generate_risk_indicator_knowledge"]
