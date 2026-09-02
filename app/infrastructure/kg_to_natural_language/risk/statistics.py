"""风险指标知识条目数的本地统计与层级聚合。"""

from __future__ import annotations

import hashlib
import json
import os
import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from .config import (
    DEFAULT_RISK_STATS_ROOT,
    DEFAULT_RISK_TEMP_ROOT,
    DEFAULT_RISK_TREE_PATH,
)
from .indicator_tree import (
    IndicatorContext,
    RiskIndicatorTreeError,
    load_indicator_contexts,
    normalize_filename,
)


DEFAULT_RISK_STATISTICS_PATH = DEFAULT_RISK_STATS_ROOT / "知识条目数统计.json"


class RiskIndicatorStatisticsError(RuntimeError):
    """风险指标知识统计无法完成。"""


class _ArtifactValidationError(ValueError):
    pass


@dataclass
class _FileKnowledge:
    knowledge_ids: set[str] = field(default_factory=set)
    source_names: set[str] = field(default_factory=set)
    graph_categories: set[str] = field(default_factory=set)


@dataclass
class _IndicatorArtifact:
    status: str
    path: Path
    knowledge_ids: set[str] = field(default_factory=set)
    files: dict[str, _FileKnowledge] = field(default_factory=dict)
    declared_knowledge_count: int | None = None
    fallback_identity_count: int = 0
    excluded_from_parent_count: int = 0
    warnings: list[dict[str, Any]] = field(default_factory=list)
    error: str | None = None


def _now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _sequence_key(value: str) -> tuple[int, ...]:
    return tuple(int(part) for part in value.split("."))


def _normalized_knowledge_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _legacy_knowledge_id(file_key: str, knowledge: str) -> str:
    payload = f"{file_key}\0{_normalized_knowledge_text(knowledge)}"
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]
    return f"legacy_{digest}"


def _warning(
    code: str,
    message: str,
    *,
    indicator_number: str | None = None,
    file_key: str | None = None,
) -> dict[str, Any]:
    value: dict[str, Any] = {"code": code, "message": message}
    if indicator_number:
        value["indicator_number"] = indicator_number
    if file_key:
        value["normalized_file_key"] = file_key
    return value


def _read_json_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise _ArtifactValidationError(f"JSON无法解析：{exc}") from exc
    except OSError as exc:
        raise _ArtifactValidationError(f"文件无法读取：{exc}") from exc
    if not isinstance(value, dict):
        raise _ArtifactValidationError("JSON根节点必须是对象")
    return value


def _file_identity(entry: dict[str, Any]) -> tuple[str, set[str]]:
    source_file = str(entry.get("source_file") or "").strip()
    graph_filename = str(entry.get("graph_filename") or "").strip()
    selected = source_file or graph_filename
    names = {name for name in (source_file, graph_filename) if name}
    return normalize_filename(selected), names


def _load_indicator_artifact(
    context: IndicatorContext,
    temp_root: Path,
) -> _IndicatorArtifact:
    path = temp_root / context.indicator_number / "all.json"
    if not path.is_file():
        return _IndicatorArtifact(status="missing", path=path)

    try:
        value = _read_json_object(path)
        if value.get("stage") != "knowledge_generation" or value.get("status") != "success":
            raise _ArtifactValidationError(
                "成果不是成功的knowledge_generation阶段产物"
            )
        indicator = value.get("indicator")
        if not isinstance(indicator, dict):
            raise _ArtifactValidationError("缺少indicator对象")
        actual_number = str(indicator.get("number") or "").strip()
        if actual_number != context.indicator_number:
            raise _ArtifactValidationError(
                f"指标序号不一致：期望{context.indicator_number}，实际{actual_number or '空'}"
            )
        entries = value.get("knowledge_entries")
        if not isinstance(entries, list):
            raise _ArtifactValidationError("knowledge_entries必须是数组")

        artifact = _IndicatorArtifact(status="available", path=path)
        declared = value.get("knowledge_count")
        if isinstance(declared, int) and not isinstance(declared, bool) and declared >= 0:
            artifact.declared_knowledge_count = declared

        for index, entry in enumerate(entries, start=1):
            if not isinstance(entry, dict):
                raise _ArtifactValidationError(f"第{index}条知识必须是JSON对象")
            knowledge = _normalized_knowledge_text(entry.get("knowledge"))
            if not knowledge:
                raise _ArtifactValidationError(f"第{index}条知识正文为空")

            file_key, source_names = _file_identity(entry)
            knowledge_id = str(entry.get("knowledge_id") or "").strip()
            if not knowledge_id:
                if not file_key:
                    raise _ArtifactValidationError(
                        f"第{index}条知识同时缺少knowledge_id和来源文件，无法生成兼容标识"
                    )
                knowledge_id = _legacy_knowledge_id(file_key, knowledge)
                artifact.fallback_identity_count += 1
                artifact.warnings.append(
                    _warning(
                        "legacy_knowledge_id_fallback",
                        f"第{index}条知识缺少knowledge_id，已生成兼容标识",
                        indicator_number=context.indicator_number,
                        file_key=file_key,
                    )
                )

            if knowledge_id in artifact.knowledge_ids:
                artifact.warnings.append(
                    _warning(
                        "duplicate_knowledge_id",
                        f"知识ID重复，统计时仅保留一次：{knowledge_id}",
                        indicator_number=context.indicator_number,
                        file_key=file_key or None,
                    )
                )
            artifact.knowledge_ids.add(knowledge_id)

            if not file_key:
                artifact.excluded_from_parent_count += 1
                artifact.warnings.append(
                    _warning(
                        "missing_file_identity",
                        f"知识{knowledge_id}缺少source_file和graph_filename，未纳入父级文件聚合",
                        indicator_number=context.indicator_number,
                    )
                )
                continue

            file_knowledge = artifact.files.setdefault(file_key, _FileKnowledge())
            file_knowledge.knowledge_ids.add(knowledge_id)
            file_knowledge.source_names.update(source_names)
            category = str(entry.get("graph_category") or "").strip()
            if category:
                file_knowledge.graph_categories.add(category)

        if (
            artifact.declared_knowledge_count is not None
            and artifact.declared_knowledge_count != len(artifact.knowledge_ids)
        ):
            artifact.warnings.append(
                _warning(
                    "declared_knowledge_count_mismatch",
                    "声明知识数与实际唯一知识数不一致："
                    f"声明{artifact.declared_knowledge_count}，实际{len(artifact.knowledge_ids)}",
                    indicator_number=context.indicator_number,
                )
            )
        return artifact
    except _ArtifactValidationError as exc:
        return _IndicatorArtifact(status="invalid", path=path, error=str(exc))


def _merge_file_maps(
    artifacts: Iterable[_IndicatorArtifact],
) -> dict[str, _FileKnowledge]:
    merged: dict[str, _FileKnowledge] = {}
    for artifact in artifacts:
        if artifact.status != "available":
            continue
        for file_key, observed in artifact.files.items():
            target = merged.setdefault(file_key, _FileKnowledge())
            target.knowledge_ids.update(observed.knowledge_ids)
            target.source_names.update(observed.source_names)
            target.graph_categories.update(observed.graph_categories)
    return merged


def _aggregate_node(
    contexts: list[IndicatorContext],
    artifacts: dict[str, _IndicatorArtifact],
) -> dict[str, Any]:
    selected = [artifacts[context.indicator_number] for context in contexts]
    files = _merge_file_maps(selected)
    available = sum(artifact.status == "available" for artifact in selected)
    missing = sum(artifact.status == "missing" for artifact in selected)
    invalid = sum(artifact.status == "invalid" for artifact in selected)
    return {
        "knowledge_entry_count": sum(
            len(file_knowledge.knowledge_ids) for file_knowledge in files.values()
        ),
        "unique_file_count": len(files),
        "total_third_level_count": len(contexts),
        "available_third_level_count": available,
        "missing_third_level_count": missing,
        "invalid_third_level_count": invalid,
        "is_complete": available == len(contexts),
    }


def _third_level_node(
    context: IndicatorContext,
    artifact: _IndicatorArtifact,
) -> dict[str, Any]:
    return {
        "indicator_number": context.indicator_number,
        "indicator_level": "third_level",
        "indicator_name": context.indicator_name,
        "indicator_description": context.indicator_description,
        "risk_type": context.risk_type,
        "artifact_status": artifact.status,
        "knowledge_entry_count": len(artifact.knowledge_ids),
        "unique_file_count": len(artifact.files),
        "declared_knowledge_count": artifact.declared_knowledge_count,
        "count_matches_declared": (
            None
            if artifact.declared_knowledge_count is None
            else artifact.declared_knowledge_count == len(artifact.knowledge_ids)
        ),
        "fallback_identity_count": artifact.fallback_identity_count,
        "excluded_from_parent_count": artifact.excluded_from_parent_count,
        "all_json_path": str(artifact.path.resolve()),
        "error": artifact.error,
        "warnings": artifact.warnings,
    }


def _build_file_conflicts(
    contexts: tuple[IndicatorContext, ...],
    artifacts: dict[str, _IndicatorArtifact],
) -> list[dict[str, Any]]:
    occurrences: dict[str, list[dict[str, Any]]] = {}
    for context in contexts:
        artifact = artifacts[context.indicator_number]
        if artifact.status != "available":
            continue
        for file_key, file_knowledge in artifact.files.items():
            occurrences.setdefault(file_key, []).append(
                {
                    "indicator_number": context.indicator_number,
                    "knowledge_ids": set(file_knowledge.knowledge_ids),
                    "source_names": set(file_knowledge.source_names),
                    "graph_categories": set(file_knowledge.graph_categories),
                }
            )

    conflicts: list[dict[str, Any]] = []
    for file_key in sorted(occurrences):
        items = occurrences[file_key]
        categories = sorted(
            {category for item in items for category in item["graph_categories"]}
        )
        source_names = sorted({name for item in items for name in item["source_names"]})
        knowledge_sets = {frozenset(item["knowledge_ids"]) for item in items}
        conflict_types: list[str] = []
        if len(knowledge_sets) > 1:
            conflict_types.append("knowledge_set_mismatch")
        if len(categories) > 1:
            conflict_types.append("graph_category_collision")
        if not conflict_types:
            continue
        union_ids = set().union(*(item["knowledge_ids"] for item in items))
        intersection_ids = set(items[0]["knowledge_ids"])
        for item in items[1:]:
            intersection_ids.intersection_update(item["knowledge_ids"])
        conflicts.append(
            {
                "normalized_file_key": file_key,
                "conflict_types": conflict_types,
                "indicator_numbers": sorted(
                    {item["indicator_number"] for item in items},
                    key=_sequence_key,
                ),
                "source_names": source_names,
                "graph_categories": categories,
                "occurrence_counts": [
                    {
                        "indicator_number": item["indicator_number"],
                        "knowledge_entry_count": len(item["knowledge_ids"]),
                    }
                    for item in sorted(
                        items,
                        key=lambda value: _sequence_key(value["indicator_number"]),
                    )
                ],
                "union_knowledge_count": len(union_ids),
                "intersection_knowledge_count": len(intersection_ids),
                "difference_knowledge_count": len(union_ids - intersection_ids),
            }
        )
    return conflicts


def _build_hierarchy(
    contexts: tuple[IndicatorContext, ...],
    artifacts: dict[str, _IndicatorArtifact],
) -> dict[str, Any]:
    root_groups: dict[tuple[str, str], list[IndicatorContext]] = {}
    for context in contexts:
        root_number = context.indicator_number.split(".", 1)[0]
        root_groups.setdefault((root_number, context.risk_type), []).append(context)

    risk_types: dict[str, Any] = {}
    for (root_number, risk_type), root_contexts in sorted(
        root_groups.items(), key=lambda item: _sequence_key(item[0][0])
    ):
        root_contexts.sort(key=lambda context: _sequence_key(context.indicator_number))
        first_groups: dict[str, list[IndicatorContext]] = {}
        for context in root_contexts:
            first_number = ".".join(context.indicator_number.split(".")[:2])
            first_groups.setdefault(first_number, []).append(context)

        first_children: dict[str, Any] = {}
        for first_number in sorted(first_groups, key=_sequence_key):
            first_contexts = first_groups[first_number]
            second_groups: dict[str, list[IndicatorContext]] = {}
            for context in first_contexts:
                second_number = ".".join(context.indicator_number.split(".")[:3])
                second_groups.setdefault(second_number, []).append(context)

            second_children: dict[str, Any] = {}
            for second_number in sorted(second_groups, key=_sequence_key):
                second_contexts = sorted(
                    second_groups[second_number],
                    key=lambda context: _sequence_key(context.indicator_number),
                )
                third_children = {
                    context.indicator_number: _third_level_node(
                        context,
                        artifacts[context.indicator_number],
                    )
                    for context in second_contexts
                }
                second_children[second_number] = {
                    "indicator_number": second_number,
                    "indicator_level": "second_level",
                    **_aggregate_node(second_contexts, artifacts),
                    "children": third_children,
                }

            first_children[first_number] = {
                "indicator_number": first_number,
                "indicator_level": "first_level",
                **_aggregate_node(first_contexts, artifacts),
                "children": second_children,
            }

        risk_types[risk_type] = {
            "indicator_number": root_number,
            "indicator_level": "risk_type",
            "risk_type": risk_type,
            **_aggregate_node(root_contexts, artifacts),
            "children": first_children,
        }
    return risk_types


def _atomic_write_json(path: Path, value: dict[str, Any]) -> None:
    try:
        content = json.dumps(value, ensure_ascii=False, indent=2) + "\n"
    except (TypeError, ValueError) as exc:
        raise RiskIndicatorStatisticsError(f"统计结果无法序列化为JSON：{exc}") from exc

    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary.write_text(content, encoding="utf-8", newline="\n")
        os.replace(temporary, path)
    except OSError as exc:
        raise RiskIndicatorStatisticsError(f"统计JSON写入失败：{path}：{exc}") from exc
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def build_risk_indicator_knowledge_statistics(
    *,
    risk_tree_path: str | Path | None = None,
    temp_risk_root: str | Path | None = None,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    """生成三级及上级风险指标的知识条目数 JSON 统计。"""
    tree_path = Path(risk_tree_path) if risk_tree_path is not None else DEFAULT_RISK_TREE_PATH
    temp_root = Path(temp_risk_root) if temp_risk_root is not None else DEFAULT_RISK_TEMP_ROOT
    target_path = Path(output_path) if output_path is not None else DEFAULT_RISK_STATISTICS_PATH
    if target_path.suffix.casefold() != ".json":
        raise RiskIndicatorStatisticsError(f"统计输出必须是JSON文件：{target_path}")

    try:
        contexts = load_indicator_contexts(
            tree_path=tree_path,
            allow_legacy_description=(risk_tree_path is not None),
        )
    except RiskIndicatorTreeError as exc:
        raise RiskIndicatorStatisticsError(f"风险指标树无法用于统计：{exc}") from exc

    artifacts = {
        context.indicator_number: _load_indicator_artifact(context, temp_root)
        for context in contexts
    }
    hierarchy = _build_hierarchy(contexts, artifacts)
    file_conflicts = _build_file_conflicts(contexts, artifacts)
    warnings = [
        warning
        for context in contexts
        for warning in artifacts[context.indicator_number].warnings
    ]
    missing_indicators = [
        {
            "indicator_number": context.indicator_number,
            "indicator_name": context.indicator_name,
            "all_json_path": str(artifacts[context.indicator_number].path.resolve()),
        }
        for context in contexts
        if artifacts[context.indicator_number].status == "missing"
    ]
    invalid_indicators = [
        {
            "indicator_number": context.indicator_number,
            "indicator_name": context.indicator_name,
            "all_json_path": str(artifacts[context.indicator_number].path.resolve()),
            "error": artifacts[context.indicator_number].error,
        }
        for context in contexts
        if artifacts[context.indicator_number].status == "invalid"
    ]

    first_level_numbers = {
        ".".join(context.indicator_number.split(".")[:2]) for context in contexts
    }
    second_level_numbers = {
        ".".join(context.indicator_number.split(".")[:3]) for context in contexts
    }
    available_count = sum(
        artifact.status == "available" for artifact in artifacts.values()
    )
    status = (
        "success"
        if not missing_indicators
        and not invalid_indicators
        and not file_conflicts
        and not warnings
        else "partial_success"
    )
    result: dict[str, Any] = {
        "schema_version": "1.0",
        "generated_at": _now_iso(),
        "status": status,
        "sources": {
            "risk_tree_path": str(tree_path.resolve()),
            "temp_risk_root": str(temp_root.resolve()),
        },
        "statistics_scope": {
            "third_level": "按各指标all.json中的实际唯一知识条目统计",
            "parent_levels": "在每个父级范围内按规范化来源文件去重后统计知识条目",
            "cross_risk_types": "六类风险分别统计，不生成跨风险类型总计；同一来源文件及其知识可能被多个风险类型复用",
        },
        "summary": {
            "risk_type_count": len(hierarchy),
            "first_level_count": len(first_level_numbers),
            "second_level_count": len(second_level_numbers),
            "third_level_count": len(contexts),
            "available_third_level_count": available_count,
            "missing_third_level_count": len(missing_indicators),
            "invalid_third_level_count": len(invalid_indicators),
            "file_conflict_count": len(file_conflicts),
            "warning_count": len(warnings),
        },
        "risk_types": hierarchy,
        "missing_indicators": missing_indicators,
        "invalid_indicators": invalid_indicators,
        "file_conflicts": file_conflicts,
        "warnings": warnings,
    }
    _atomic_write_json(target_path, result)
    return result


__all__ = [
    "DEFAULT_RISK_STATISTICS_PATH",
    "RiskIndicatorStatisticsError",
    "build_risk_indicator_knowledge_statistics",
]
