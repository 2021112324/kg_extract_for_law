"""五类 Neo4j 图谱的知识条目批量导出与发布。"""

from __future__ import annotations

import json
import logging
import os
import shutil
import threading
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from ..converters.base import convert_json_to_text
from ..converters.compliance_guide import convert_compliance_guide_json_to_text
from ..converters.english_law import convert_english_law_json_to_text
from ..converters.national_standard import convert_national_standard_json_to_text
from ..converters.chinese_law import convert_json_to_text_v2
from ..graph.exporter import export_graph_to_directory
from .categories import TYPED_KNOWLEDGE_CATEGORIES, TypedKnowledgeCategory

logger = logging.getLogger(__name__)

_DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parents[1] / "data" / "type"
_STATISTICS_FILENAME = "知识条目统计.json"
_RUN_LOCK = threading.Lock()


class TypedKnowledgeExportError(RuntimeError):
    """包含失败阶段和可选类别的批处理异常。"""

    def __init__(self, stage: str, reason: str, category: str | None = None) -> None:
        self.stage = stage
        self.category = category
        self.reason = reason
        location = f"，类别: {category}" if category else ""
        super().__init__(f"五类知识条目处理失败（阶段: {stage}{location}）: {reason}")


def _converter_for(category: TypedKnowledgeCategory) -> Callable[..., dict[str, Any]]:
    converters: dict[str, Callable[..., dict[str, Any]]] = {
        "chinese_law": convert_json_to_text_v2,
        "national_standard": convert_national_standard_json_to_text,
        "english_law": convert_english_law_json_to_text,
        "guide": convert_compliance_guide_json_to_text,
        "generic": convert_json_to_text,
    }
    try:
        return converters[category.converter]
    except KeyError as exc:
        raise TypedKnowledgeExportError(
            "转换器路由",
            f"未配置转换器标识 {category.converter}",
            category.name,
        ) from exc


def _count_nonempty_lines(path: Path) -> int:
    if not path.is_file():
        raise FileNotFoundError(f"转换器未生成 TXT 文件: {path}")
    with path.open("r", encoding="utf-8") as file:
        return sum(1 for line in file if line.strip())


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as file:
        json.dump(value, file, ensure_ascii=False, indent=2)
        file.write("\n")


def _prepare_category(
    category: TypedKnowledgeCategory,
    run_root: Path,
) -> tuple[dict[str, Any], Path]:
    category_root = run_root / category.name
    graph_dir = category_root / "json"
    conversion_dir = category_root / "conversion"

    try:
        export_stats = export_graph_to_directory(category.label, graph_dir)
    except Exception as exc:
        raise TypedKnowledgeExportError("Neo4j 导出", str(exc), category.name) from exc

    node_count = int(export_stats.get("node_count", 0))
    relationship_count = int(export_stats.get("edge_count", 0))

    if node_count == 0:
        txt_path = conversion_dir / "txt" / category.output_filename
        txt_path.parent.mkdir(parents=True, exist_ok=True)
        txt_path.write_text("", encoding="utf-8")
        convert_stats: dict[str, Any] = {
            "total_entries": 0,
            "source_files": 0,
            "txt_path": str(txt_path),
        }
    else:
        try:
            converter = _converter_for(category)
            convert_stats = converter(
                category.name,
                input_dir=graph_dir,
                output_dir=conversion_dir,
            )
        except TypedKnowledgeExportError:
            raise
        except Exception as exc:
            raise TypedKnowledgeExportError("知识条目转换", str(exc), category.name) from exc

    try:
        txt_path = Path(str(convert_stats["txt_path"]))
        knowledge_entry_count = int(convert_stats["total_entries"])
        actual_line_count = _count_nonempty_lines(txt_path)
    except Exception as exc:
        raise TypedKnowledgeExportError("结果校验", str(exc), category.name) from exc

    if actual_line_count != knowledge_entry_count:
        raise TypedKnowledgeExportError(
            "结果校验",
            f"转换器统计 {knowledge_entry_count} 条，但 TXT 含 {actual_line_count} 条非空记录",
            category.name,
        )
    if node_count > 0 and knowledge_entry_count == 0:
        raise TypedKnowledgeExportError(
            "结果校验",
            "图谱包含节点，但配置的转换器未生成任何知识条目",
            category.name,
        )

    result = {
        "label": category.label,
        "node_count": node_count,
        "relationship_count": relationship_count,
        "knowledge_entry_count": knowledge_entry_count,
        "source_file_count": int(convert_stats.get("source_files", 0)),
        "output_file": category.output_filename,
        "status": "success",
    }
    return result, txt_path


def _replace_file(source: Path, target: Path) -> None:
    """将同文件系统暂存文件原子替换为正式文件。"""
    os.replace(source, target)


def _publish_artifacts(publish_dir: Path, output_dir: Path) -> None:
    """发布完整成果；失败时恢复本次发布前的全部文件。"""
    artifact_names = [
        *(category.output_filename for category in TYPED_KNOWLEDGE_CATEGORIES),
        _STATISTICS_FILENAME,
    ]
    backup_dir = publish_dir.parent / "backup"
    backup_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    existed: set[str] = set()
    replaced: list[str] = []

    for name in artifact_names:
        target = output_dir / name
        if target.exists():
            shutil.copy2(target, backup_dir / name)
            existed.add(name)

    try:
        for name in artifact_names:
            source = publish_dir / name
            if not source.is_file():
                raise FileNotFoundError(f"待发布成果不存在: {source}")
            _replace_file(source, output_dir / name)
            replaced.append(name)
    except Exception as exc:
        for name in replaced:
            target = output_dir / name
            backup = backup_dir / name
            try:
                if name in existed:
                    os.replace(backup, target)
                elif target.exists():
                    target.unlink()
            except OSError:
                logger.exception("恢复发布前成果失败: %s", target)
        raise TypedKnowledgeExportError("成果发布", str(exc)) from exc


def process_typed_knowledge_entries(
    *,
    output_dir: str | Path | None = None,
) -> dict[str, Any]:
    """顺序处理五类固定图谱，并一次性发布知识条目和统计。"""
    output_root = Path(output_dir) if output_dir is not None else _DEFAULT_OUTPUT_DIR

    with _RUN_LOCK:
        run_id = uuid.uuid4().hex
        staging_root = output_root / ".staging"
        run_root = staging_root / run_id
        publish_dir = run_root / "publish"
        publish_dir.mkdir(parents=True, exist_ok=True)

        try:
            category_results: dict[str, dict[str, Any]] = {}
            for category in TYPED_KNOWLEDGE_CATEGORIES:
                logger.info("开始处理知识类别: %s", category.name)
                result, txt_path = _prepare_category(category, run_root)
                shutil.copy2(txt_path, publish_dir / category.output_filename)
                category_results[category.name] = result
                logger.info(
                    "知识类别处理完成: %s，节点 %s，关系 %s，知识条目 %s",
                    category.name,
                    result["node_count"],
                    result["relationship_count"],
                    result["knowledge_entry_count"],
                )

            total = sum(
                result["knowledge_entry_count"]
                for result in category_results.values()
            )
            statistics = {
                "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
                "status": "success",
                "categories": category_results,
                "total_knowledge_entry_count": total,
            }
            _write_json(publish_dir / _STATISTICS_FILENAME, statistics)
            _publish_artifacts(publish_dir, output_root)

            response = {
                **statistics,
                "output_directory": str(output_root.resolve()),
                "statistics_json_path": str((output_root / _STATISTICS_FILENAME).resolve()),
            }
            logger.info("五类知识条目处理完成，总计 %s 条", total)
            return response
        except TypedKnowledgeExportError:
            raise
        except Exception as exc:
            raise TypedKnowledgeExportError("批处理编排", str(exc)) from exc
        finally:
            shutil.rmtree(run_root, ignore_errors=True)
            try:
                staging_root.rmdir()
            except OSError:
                pass


__all__ = [
    "TypedKnowledgeExportError",
    "process_typed_knowledge_entries",
]
