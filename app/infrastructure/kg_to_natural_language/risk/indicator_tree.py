"""三级风险指标解析及映射文件提取。"""

from __future__ import annotations

import json
import re
import warnings
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .config import DEFAULT_RISK_TREE_PATH


INDICATOR_SEQUENCE_PATTERN = re.compile(r"^[1-6](?:\.\d+){3}$")
KNOWN_FILE_EXTENSIONS = (
    ".txt",
    ".md",
    ".markdown",
    ".pdf",
    ".doc",
    ".docx",
    ".docs",
    ".json",
    ".html",
    ".htm",
)


class RiskIndicatorTreeError(ValueError):
    """风险指标树结构或指标输入错误。"""


@dataclass(frozen=True)
class MappingSource:
    region: str
    mapped_name: str
    raw_text: str
    found_filename: str

    def to_dict(self) -> dict[str, str]:
        return {
            "region": self.region,
            "mapped_name": self.mapped_name,
            "raw_text": self.raw_text,
            "found_filename": self.found_filename,
        }


@dataclass
class MappedFile:
    normalized_key: str
    found_filename: str
    mapping_sources: list[MappingSource] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "normalized_key": self.normalized_key,
            "found_filename": self.found_filename,
            "mapping_sources": [source.to_dict() for source in self.mapping_sources],
        }


@dataclass(frozen=True)
class IndicatorContext:
    indicator_number: str
    indicator_name: str
    indicator_description: str
    risk_type: str
    mapped_files: tuple[MappedFile, ...]
    mapping_reference_count: int
    ignored_empty_found_count: int
    warnings: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "number": self.indicator_number,
            "name": self.indicator_name,
            "description": self.indicator_description,
            "risk_type": self.risk_type,
        }


def validate_indicator_number(indicator_number: str) -> str:
    normalized = str(indicator_number or "").strip()
    if not INDICATOR_SEQUENCE_PATTERN.fullmatch(normalized):
        raise RiskIndicatorTreeError(
            f"三级指标序号格式错误：{indicator_number!r}；应为四段点分数字"
        )
    return normalized


def _basename(value: str) -> str:
    return str(value or "").strip().replace("\\", "/").rsplit("/", 1)[-1]


def normalize_filename(value: str) -> str:
    """生成只处理路径、扩展名、空白和大小写的确定性文件键。"""
    name = _basename(value)
    lowered = name.casefold()
    removed = True
    while removed:
        removed = False
        for extension in KNOWN_FILE_EXTENSIONS:
            if lowered.endswith(extension):
                name = name[: -len(extension)].rstrip()
                lowered = name.casefold()
                removed = True
                break
    return re.sub(r"\s+", " ", name).strip().casefold()


def filename_query_variants(value: str) -> set[str]:
    """返回用于 Neo4j 精确候选查询的文件名变体。"""
    basename = re.sub(r"\s+", " ", _basename(value)).strip().casefold()
    stem = normalize_filename(value)
    variants = {basename, stem}
    variants.update(f"{stem}{extension}" for extension in KNOWN_FILE_EXTENSIONS)
    return {variant for variant in variants if variant}


def _leaf_mapping_sources(
    mapping_tree: Any,
    *,
    region: str = "",
    path: tuple[str, ...] = (),
) -> list[MappingSource]:
    if not isinstance(mapping_tree, dict):
        return []
    if "已找到" in mapping_tree:
        found_filename = str(mapping_tree.get("已找到") or "").strip()
        mapped_name = path[-1] if path else ""
        return [
            MappingSource(
                region=region,
                mapped_name=mapped_name,
                raw_text=str(mapping_tree.get("原始文本") or "").strip(),
                found_filename=found_filename,
            )
        ]

    sources: list[MappingSource] = []
    for key, value in mapping_tree.items():
        child_region = region or str(key)
        sources.extend(
            _leaf_mapping_sources(
                value,
                region=child_region,
                path=(*path, str(key)),
            )
        )
    return sources


def _build_indicator_index(
    tree: dict[str, Any],
) -> dict[str, tuple[str, str, dict[str, Any]]]:
    index: dict[str, tuple[str, str, dict[str, Any]]] = {}
    for risk_type, indicators in tree.items():
        if not isinstance(indicators, dict):
            raise RiskIndicatorTreeError(f"风险类别{risk_type}的指标集合必须是JSON对象")
        for indicator_name, leaf in indicators.items():
            if not isinstance(leaf, dict) or "序号" not in leaf:
                raise RiskIndicatorTreeError(f"指标{indicator_name}缺少合法序号叶节点")
            sequence = validate_indicator_number(str(leaf["序号"]))
            if sequence in index:
                previous = index[sequence][1]
                raise RiskIndicatorTreeError(
                    f"三级指标序号{sequence}重复：{previous}、{indicator_name}"
                )
            index[sequence] = (str(risk_type), str(indicator_name), leaf)
    return index


def _load_tree(path: Path) -> dict[str, Any]:
    try:
        tree = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise RiskIndicatorTreeError(f"风险指标树不存在：{path}") from exc
    except json.JSONDecodeError as exc:
        raise RiskIndicatorTreeError(f"风险指标树JSON无法解析：{exc}") from exc
    except OSError as exc:
        raise RiskIndicatorTreeError(f"风险指标树无法读取：{path}：{exc}") from exc
    if not isinstance(tree, dict):
        raise RiskIndicatorTreeError("风险指标树根节点必须是JSON对象")
    return tree


def _sequence_sort_key(value: str) -> tuple[int, ...]:
    return tuple(int(part) for part in value.split("."))


def _build_indicator_context(
    sequence: str,
    indexed: tuple[str, str, dict[str, Any]],
    *,
    allow_legacy_description: bool,
) -> IndicatorContext:
    risk_type, indicator_name, leaf = indexed
    description = str(leaf.get("指标描述") or "").strip()
    warning_messages: list[str] = []
    if not description:
        if not allow_legacy_description:
            raise RiskIndicatorTreeError(f"三级指标{sequence}缺少正式指标描述")
        description = indicator_name
        message = f"外部旧版风险树的指标{sequence}缺少指标描述，已临时使用指标名称"
        warnings.warn(message, RuntimeWarning, stacklevel=3)
        warning_messages.append(message)

    mapping_tree = leaf.get("映射文件")
    if not isinstance(mapping_tree, dict):
        raise RiskIndicatorTreeError(f"三级指标{sequence}的映射文件必须是JSON对象")
    sources = _leaf_mapping_sources(mapping_tree)
    selected_sources = [source for source in sources if source.found_filename]
    ignored_empty = len(sources) - len(selected_sources)

    files_by_key: dict[str, MappedFile] = {}
    for source in selected_sources:
        key = normalize_filename(source.found_filename)
        if not key:
            ignored_empty += 1
            continue
        mapped_file = files_by_key.setdefault(
            key,
            MappedFile(normalized_key=key, found_filename=source.found_filename),
        )
        if source not in mapped_file.mapping_sources:
            mapped_file.mapping_sources.append(source)

    return IndicatorContext(
        indicator_number=sequence,
        indicator_name=indicator_name,
        indicator_description=description,
        risk_type=risk_type,
        mapped_files=tuple(files_by_key.values()),
        mapping_reference_count=len(selected_sources),
        ignored_empty_found_count=ignored_empty,
        warnings=tuple(warning_messages),
    )


def load_indicator_contexts(
    *,
    tree_path: str | Path | None = None,
    allow_legacy_description: bool | None = None,
) -> tuple[IndicatorContext, ...]:
    """一次读取风险树并返回按指标序号排序的全部三级指标上下文。"""
    path = Path(tree_path) if tree_path is not None else DEFAULT_RISK_TREE_PATH
    tree = _load_tree(path)
    index = _build_indicator_index(tree)
    legacy_allowed = (
        allow_legacy_description
        if allow_legacy_description is not None
        else tree_path is not None and path.resolve() != DEFAULT_RISK_TREE_PATH.resolve()
    )
    return tuple(
        _build_indicator_context(
            sequence,
            index[sequence],
            allow_legacy_description=legacy_allowed,
        )
        for sequence in sorted(index, key=_sequence_sort_key)
    )


def list_indicator_numbers(
    *,
    tree_path: str | Path | None = None,
) -> tuple[str, ...]:
    """读取风险指标树并返回按数字层级排序的全部三级指标序号。"""
    path = Path(tree_path) if tree_path is not None else DEFAULT_RISK_TREE_PATH
    index = _build_indicator_index(_load_tree(path))
    return tuple(sorted(index, key=_sequence_sort_key))


def load_indicator_context(
    indicator_number: str,
    *,
    tree_path: str | Path | None = None,
    allow_legacy_description: bool | None = None,
) -> IndicatorContext:
    sequence = validate_indicator_number(indicator_number)
    path = Path(tree_path) if tree_path is not None else DEFAULT_RISK_TREE_PATH
    tree = _load_tree(path)
    index = _build_indicator_index(tree)
    if sequence not in index:
        raise RiskIndicatorTreeError(f"风险指标树中不存在三级指标：{sequence}")
    legacy_allowed = (
        allow_legacy_description
        if allow_legacy_description is not None
        else tree_path is not None and path.resolve() != DEFAULT_RISK_TREE_PATH.resolve()
    )
    return _build_indicator_context(
        sequence,
        index[sequence],
        allow_legacy_description=legacy_allowed,
    )


__all__ = [
    "IndicatorContext",
    "MappedFile",
    "MappingSource",
    "RiskIndicatorTreeError",
    "filename_query_variants",
    "list_indicator_numbers",
    "load_indicator_context",
    "load_indicator_contexts",
    "normalize_filename",
    "validate_indicator_number",
]
