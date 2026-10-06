"""按来源文件地区发布英文法规自然语言知识。"""

from __future__ import annotations

import json
import logging
import os
import shutil
import tempfile
import threading
import uuid
from collections import Counter
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any, Iterable

from ..converters.english_law import convert_english_law_json_to_text
from ..graph.exporter import export_graph_to_directory

logger = logging.getLogger(__name__)

ENGLISH_LAW_LABEL = "英文法规"
CONTROLLED_REGIONS = ("美国", "欧盟及其成员国")
REGION_OUTPUT_FILENAMES = {
    "美国": "美国.txt",
    "欧盟及其成员国": "欧盟及其成员国.txt",
}

_MODULE_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REGION_MAPPING_PATH = _MODULE_ROOT / "data" / "json" / "filename到region映射.json"
DEFAULT_REGION_OUTPUT_DIR = _MODULE_ROOT / "data" / "type" / "地区"
REGION_STATISTICS_FILENAME = "知识条目统计.json"
_RUN_LOCK = threading.Lock()


class EnglishLawRegionExportError(RuntimeError):
    """包含失败阶段及诊断信息的地区导出异常。"""

    def __init__(
        self,
        stage: str,
        reason: str,
        *,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.stage = stage
        self.reason = reason
        self.details = details or {}
        super().__init__(f"英文法规地区知识导出失败（阶段: {stage}）: {reason}")


def _now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _normalize_filename(value: Any) -> str:
    text = str(value or "").strip().strip('"\'').replace("\\", "/")
    return PurePosixPath(text).name.casefold() if text else ""


def _load_region_mapping(path: Path) -> tuple[dict[str, str], dict[str, str]]:
    try:
        with path.open("r", encoding="utf-8-sig") as file:
            raw_mapping = json.load(file)
    except FileNotFoundError as exc:
        raise EnglishLawRegionExportError("地区映射加载", f"映射文件不存在: {path}") from exc
    except json.JSONDecodeError as exc:
        raise EnglishLawRegionExportError(
            "地区映射加载", f"映射文件不是合法JSON: {path}: {exc}"
        ) from exc

    if not isinstance(raw_mapping, dict):
        raise EnglishLawRegionExportError("地区映射校验", "映射文件顶层必须是JSON对象")

    mapping: dict[str, str] = {}
    original_names: dict[str, str] = {}
    for raw_filename, raw_region in raw_mapping.items():
        filename = str(raw_filename or "").strip()
        region = str(raw_region or "").strip()
        normalized = _normalize_filename(filename)
        if not normalized:
            raise EnglishLawRegionExportError("地区映射校验", "映射中存在空filename")
        if region not in CONTROLLED_REGIONS:
            raise EnglishLawRegionExportError(
                "地区映射校验",
                f"{filename}使用了不受支持的地区值: {region or '<空>'}",
                details={"filename": filename, "region": region},
            )
        previous = mapping.get(normalized)
        if previous is not None and previous != region:
            raise EnglishLawRegionExportError(
                "地区映射校验",
                f"规范化filename存在地区冲突: {original_names[normalized]} / {filename}",
                details={
                    "normalized_filename": normalized,
                    "filenames": [original_names[normalized], filename],
                    "regions": [previous, region],
                },
            )
        mapping[normalized] = region
        original_names[normalized] = filename
    return mapping, original_names


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8-sig") as file:
            for line_number, line in enumerate(file, start=1):
                if not line.strip():
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise EnglishLawRegionExportError(
                        "地区分流",
                        f"英文法规JSONL第{line_number}行格式错误: {exc}",
                    ) from exc
                if not isinstance(record, dict):
                    raise EnglishLawRegionExportError(
                        "地区分流", f"英文法规JSONL第{line_number}行不是JSON对象"
                    )
                records.append(record)
    except FileNotFoundError as exc:
        raise EnglishLawRegionExportError("地区分流", f"英文法规JSONL不存在: {path}") from exc
    return records


def _split_entries_by_region(
    records: Iterable[dict[str, Any]],
    mapping: dict[str, str],
) -> tuple[dict[str, list[str]], dict[str, set[str]], set[str]]:
    contents = {region: [] for region in CONTROLLED_REGIONS}
    source_files = {region: set() for region in CONTROLLED_REGIONS}
    used_mapping_keys: set[str] = set()
    unmapped_sources: Counter[str] = Counter()
    invalid_entries: list[dict[str, Any]] = []

    for index, record in enumerate(records, start=1):
        content = record.get("content")
        source_file = record.get("source_file")
        if not isinstance(content, str) or not content.strip():
            invalid_entries.append({"entry": index, "reason": "content为空或不是字符串"})
            continue
        normalized_source = _normalize_filename(source_file)
        if not normalized_source:
            invalid_entries.append({"entry": index, "reason": "source_file为空"})
            continue
        region = mapping.get(normalized_source)
        if region is None:
            unmapped_sources[str(source_file)] += 1
            continue

        contents[region].append(content)
        source_files[region].add(PurePosixPath(str(source_file).replace("\\", "/")).name)
        used_mapping_keys.add(normalized_source)

    if invalid_entries or unmapped_sources:
        raise EnglishLawRegionExportError(
            "地区分流校验",
            f"存在{len(invalid_entries)}条无效知识和{sum(unmapped_sources.values())}条未映射知识",
            details={
                "invalid_entries": invalid_entries,
                "unmapped_sources": dict(sorted(unmapped_sources.items())),
            },
        )
    return contents, source_files, used_mapping_keys


def _atomic_write_lines(path: Path, lines: Iterable[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        newline="\n",
        delete=False,
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
    )
    temporary = Path(handle.name)
    try:
        with handle:
            for line in lines:
                handle.write(line)
                handle.write("\n")
        os.replace(temporary, path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def _atomic_write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        newline="\n",
        delete=False,
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
    )
    temporary = Path(handle.name)
    try:
        with handle:
            json.dump(value, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
        os.replace(temporary, path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def _count_nonempty_lines(path: Path) -> int:
    with path.open("r", encoding="utf-8") as file:
        return sum(1 for line in file if line.strip())


def _replace_file(source: Path, target: Path) -> None:
    os.replace(source, target)


def _publish_artifacts(publish_dir: Path, output_dir: Path, backup_dir: Path) -> None:
    artifact_names = [*REGION_OUTPUT_FILENAMES.values(), REGION_STATISTICS_FILENAME]
    output_dir.mkdir(parents=True, exist_ok=True)
    backup_dir.mkdir(parents=True, exist_ok=True)
    existed: set[str] = set()
    replaced: list[str] = []

    for name in artifact_names:
        target = output_dir / name
        if target.is_file():
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
                logger.exception("恢复地区导出旧成果失败: %s", target)
        raise EnglishLawRegionExportError("成果发布", str(exc)) from exc


def process_english_law_knowledge_by_region(
    *,
    mapping_path: str | Path | None = None,
    output_dir: str | Path | None = None,
) -> dict[str, Any]:
    """将英文法规图谱转换结果按来源文件分为两个地区知识文件。"""

    mapping_file = Path(mapping_path) if mapping_path is not None else DEFAULT_REGION_MAPPING_PATH
    output_root = Path(output_dir) if output_dir is not None else DEFAULT_REGION_OUTPUT_DIR

    with _RUN_LOCK:
        run_id = uuid.uuid4().hex
        staging_root = output_root / ".staging"
        run_root = staging_root / run_id
        graph_dir = run_root / "graph"
        conversion_dir = run_root / "conversion"
        publish_dir = run_root / "publish"
        backup_dir = run_root / "backup"
        publish_dir.mkdir(parents=True, exist_ok=True)

        logger.info(
            "开始英文法规地区知识导出: label=%s, mapping=%s, output=%s",
            ENGLISH_LAW_LABEL,
            mapping_file,
            output_root,
        )
        try:
            mapping, original_mapping_names = _load_region_mapping(mapping_file)

            try:
                export_stats = export_graph_to_directory(ENGLISH_LAW_LABEL, graph_dir)
            except Exception as exc:
                raise EnglishLawRegionExportError("Neo4j导出", str(exc)) from exc

            try:
                convert_stats = convert_english_law_json_to_text(
                    ENGLISH_LAW_LABEL,
                    input_dir=graph_dir,
                    output_dir=conversion_dir,
                )
            except Exception as exc:
                raise EnglishLawRegionExportError("知识条目转换", str(exc)) from exc

            jsonl_value = convert_stats.get("jsonl_path")
            if not jsonl_value:
                raise EnglishLawRegionExportError("转换结果校验", "转换器未返回jsonl_path")
            records = _load_jsonl(Path(str(jsonl_value)))
            declared_total = int(convert_stats.get("total_entries", -1))
            if declared_total != len(records):
                raise EnglishLawRegionExportError(
                    "转换结果校验",
                    f"转换器声明{declared_total}条，但JSONL包含{len(records)}条",
                )

            contents, source_files, used_mapping_keys = _split_entries_by_region(records, mapping)
            classified_total = sum(len(items) for items in contents.values())
            if classified_total != declared_total:
                raise EnglishLawRegionExportError(
                    "数量守恒校验",
                    f"英文知识{declared_total}条，地区分类后{classified_total}条",
                )

            region_stats: dict[str, dict[str, Any]] = {}
            for region in CONTROLLED_REGIONS:
                output_filename = REGION_OUTPUT_FILENAMES[region]
                output_path = publish_dir / output_filename
                _atomic_write_lines(output_path, contents[region])
                actual_count = _count_nonempty_lines(output_path)
                if actual_count != len(contents[region]):
                    raise EnglishLawRegionExportError(
                        "输出校验",
                        f"{region}预期{len(contents[region])}条，文件实际{actual_count}条",
                    )
                region_stats[region] = {
                    "knowledge_entry_count": actual_count,
                    "source_file_count": len(source_files[region]),
                    "output_file": output_filename,
                    "output_path": str((output_root / output_filename).resolve()),
                    "status": "success",
                }

            unused_mapping_filenames = [
                original_mapping_names[key]
                for key in mapping
                if key not in used_mapping_keys
            ]
            statistics: dict[str, Any] = {
                "schema_version": "1.0",
                "generated_at": _now_iso(),
                "status": "success",
                "source": {
                    "neo4j_label": ENGLISH_LAW_LABEL,
                    "mapping_path": str(mapping_file.resolve()),
                    "node_count": int(export_stats.get("node_count", 0)),
                    "relationship_count": int(export_stats.get("edge_count", 0)),
                    "source_file_count": int(convert_stats.get("source_files", 0)),
                },
                "mapping": {
                    "entry_count": len(mapping),
                    "used_entry_count": len(used_mapping_keys),
                    "unused_entry_count": len(unused_mapping_filenames),
                    "unused_filenames": unused_mapping_filenames,
                    "unmapped_sources": {},
                },
                "knowledge_entry_count": declared_total,
                "classified_knowledge_entry_count": classified_total,
                "count_conservation_valid": classified_total == declared_total,
                "regions": region_stats,
                "output_directory": str(output_root.resolve()),
                "statistics_path": str((output_root / REGION_STATISTICS_FILENAME).resolve()),
            }
            _atomic_write_json(publish_dir / REGION_STATISTICS_FILENAME, statistics)
            _publish_artifacts(publish_dir, output_root, backup_dir)

            logger.info(
                "英文法规地区知识导出完成: total=%s, 美国=%s, 欧盟及其成员国=%s",
                declared_total,
                region_stats["美国"]["knowledge_entry_count"],
                region_stats["欧盟及其成员国"]["knowledge_entry_count"],
            )
            logger.info("英文法规地区知识输出目录: %s", output_root.resolve())
            return statistics
        except EnglishLawRegionExportError as exc:
            logger.error(
                "英文法规地区知识导出失败: stage=%s, reason=%s, details=%s",
                exc.stage,
                exc.reason,
                exc.details,
                exc_info=True,
            )
            raise
        except Exception as exc:
            logger.exception("英文法规地区知识导出出现未预期错误")
            raise EnglishLawRegionExportError("批处理编排", str(exc)) from exc
        finally:
            shutil.rmtree(run_root, ignore_errors=True)
            try:
                staging_root.rmdir()
            except OSError:
                pass


__all__ = [
    "CONTROLLED_REGIONS",
    "DEFAULT_REGION_MAPPING_PATH",
    "DEFAULT_REGION_OUTPUT_DIR",
    "EnglishLawRegionExportError",
    "process_english_law_knowledge_by_region",
]
