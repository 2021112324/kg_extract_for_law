"""
JSON 图谱数据转自然语言知识条目模块

从 temp/json/ 读取 node.json 和 edge.json，按法规类图谱
（"法律法规条款"/"行政监管规则"）规则转换为自然语言知识条目，
输出到 temp/txt/ 和 temp/csv/。
"""

from __future__ import annotations

import csv
import json
import logging
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 常量
# ---------------------------------------------------------------------------

# 任务说明中明确要求忽略的节点属性。
NODE_EXCLUDED_PROPERTIES = {"graph_level", "graph_tag", "id"}

# 数据里常用"无"表达缺省信息。将这些占位词视为空值。
EMPTY_TEXT_MARKERS = {"无", "暂无", "不适用", "无。", "暂无。", "不涉及"}

# Windows 文件名不能包含这些字符。
INVALID_FILENAME_CHARS = r'[<>:"/\\|?*\x00-\x1f]'

# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------


def is_empty_value(value: Any) -> bool:
    """判断属性值是否应被视为空值。"""
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == "" or value.strip() in EMPTY_TEXT_MARKERS
    if isinstance(value, (list, tuple, set, dict)):
        return len(value) == 0
    return False


def value_to_text(value: Any) -> str:
    """将属性值转换为适合写入自然语言句子的文本。"""
    if isinstance(value, (list, tuple, set)):
        return "、".join(
            value_to_text(item) for item in value if not is_empty_value(item)
        )
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False)
    return str(value).strip()


def sanitize_filename(filename: str) -> str:
    """将来源文件名清洗为可用于 Windows 文件名的字符串。"""
    safe_name = re.sub(INVALID_FILENAME_CHARS, "_", filename).strip()
    safe_name = safe_name.rstrip(". ")
    return safe_name or "未知来源"


def load_json_array(path: Path, encoding: str = "utf-8") -> list[dict[str, Any]]:
    """读取 JSON 数组文件。"""
    if not path.exists():
        raise FileNotFoundError(f"未找到输入文件：{path}")
    with path.open("r", encoding=encoding) as file:
        data = json.load(file)
    if not isinstance(data, list):
        raise ValueError(f"{path} 顶层结构应为 JSON 数组。")
    return data


def first_filename(properties: dict[str, Any], default: str = "未知来源") -> str:
    """从 properties.filename 中取第一个来源文件名。"""
    filename = properties.get("filename")
    if isinstance(filename, list):
        for item in filename:
            if not is_empty_value(item):
                return value_to_text(item)
        return default
    if not is_empty_value(filename):
        return value_to_text(filename)
    return default


def clean_node_properties(properties: dict[str, Any]) -> dict[str, Any]:
    """清洗节点属性，只保留需要转换为知识条目的属性。"""
    control_properties = {"name", "label", "filename"}
    excluded = NODE_EXCLUDED_PROPERTIES | control_properties
    cleaned: dict[str, Any] = {}
    for key, value in properties.items():
        if key in excluded:
            continue
        if is_empty_value(value):
            continue
        cleaned[key] = value
    return cleaned


def extract_node(record: dict[str, Any]) -> dict[str, Any] | None:
    """从一条 Neo4j 节点导出记录中取出 n 对象。"""
    node = record.get("n")
    if isinstance(node, dict):
        return node
    return None


def extract_edge(record: dict[str, Any]) -> dict[str, Any] | None:
    """从一条 Neo4j 关系导出记录中取出 p 对象。"""
    edge = record.get("p")
    if isinstance(edge, dict):
        return edge
    return None


def build_node_indexes(
    node_records: list[dict[str, Any]],
) -> tuple[dict[Any, dict[str, Any]], dict[str, dict[str, Any]]]:
    """构建节点索引，供边关系通过 identity 或 elementId 查找节点信息。"""
    identity_index: dict[Any, dict[str, Any]] = {}
    element_id_index: dict[str, dict[str, Any]] = {}

    for record in node_records:
        node = extract_node(record)
        if node is None:
            continue
        properties = node.get("properties") or {}
        if not isinstance(properties, dict):
            continue
        name = (
            value_to_text(properties.get("name"))
            if not is_empty_value(properties.get("name"))
            else ""
        )
        filename = first_filename(properties)
        node_info = {
            "name": name,
            "filename": filename,
            "properties": properties,
            "label": value_to_text(properties.get("label"))
            if not is_empty_value(properties.get("label"))
            else "",
        }
        identity = node.get("identity")
        element_id = node.get("elementId")
        if identity is not None:
            identity_index[identity] = node_info
        if element_id:
            element_id_index[str(element_id)] = node_info

    return identity_index, element_id_index


def find_node_info(
    *,
    identity: Any,
    element_id: Any,
    identity_index: dict[Any, dict[str, Any]],
    element_id_index: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    """通过数字 ID 或 elementId 查找节点信息。"""
    if identity in identity_index:
        return identity_index[identity]
    if element_id and str(element_id) in element_id_index:
        return element_id_index[str(element_id)]
    return None


def edge_filename(
    edge: dict[str, Any],
    start_node: dict[str, Any] | None,
    end_node: dict[str, Any] | None,
) -> str:
    """确定边关系条目归属的来源文件。"""
    properties = edge.get("properties") or {}
    if isinstance(properties, dict):
        own_filename = first_filename(properties, default="")
        if own_filename:
            return own_filename
    if start_node and start_node.get("filename"):
        return str(start_node["filename"])
    if end_node and end_node.get("filename"):
        return str(end_node["filename"])
    return "未知来源"


# ---------------------------------------------------------------------------
# 自然语言知识条目构建
# ---------------------------------------------------------------------------


def _filename_from_prefix(prefix: str) -> str:
    """从 prefix（如 '企业会计准则中'）提取来源文件名。"""
    if prefix.endswith("中"):
        return prefix[:-1]
    return prefix


def add_type_line(
    type_lines: list[str], line: str,
    file_counter: Counter[str] | None = None,
    filename: str = "未知来源",
) -> None:
    """向按类型汇总的知识条目列表中添加一行，并统计文件条目数。"""
    type_lines.append(line)
    if file_counter is not None:
        file_counter[filename] += 1


def add_single_line_pair(
    type_lines: list[str],
    line: str,
    prefix: str,
    is_regulation_doc: bool = False,
    file_counter: Counter[str] | None = None,
) -> None:
    """根据规则输出一条知识条目到 type_lines。

    法规文件节点（is_regulation_doc=True）不添加 prefix 前缀，
    其他节点正常添加 prefix。
    """
    filename = _filename_from_prefix(prefix)
    if is_regulation_doc:
        add_type_line(type_lines, line, file_counter, filename)
    else:
        add_type_line(type_lines, f"{prefix}，{line}", file_counter, filename)


def add_property_line(
    type_lines: list[str],
    prefix: str,
    subject: str,
    property_name: str,
    property_value: Any,
    is_regulation_doc: bool = False,
    file_counter: Counter[str] | None = None,
) -> None:
    """按 a1 方式输出节点属性知识："{subject}的{property}是{value}"。"""
    if is_empty_value(property_value):
        return
    value_text = value_to_text(property_value)
    if is_empty_value(value_text):
        return
    line = f"{subject}的{property_name}是{value_text}"
    add_single_line_pair(type_lines, line, prefix, is_regulation_doc, file_counter)


def add_combined_property_line(
    type_lines: list[str],
    prefix: str,
    subject: str,
    properties: dict[str, Any],
    property_names: list[str],
    phrase_overrides: dict[str, str] | None = None,
    is_regulation_doc: bool = False,
    file_counter: Counter[str] | None = None,
) -> set[str]:
    """将多个属性键值对合并为一条自然语言知识，返回已处理的属性名集合。"""
    phrase_overrides = phrase_overrides or {}
    phrases: list[str] = []
    used: set[str] = set()
    for prop_name in property_names:
        value = properties.get(prop_name)
        if is_empty_value(value):
            continue
        value_text = value_to_text(value)
        if is_empty_value(value_text):
            continue
        template = phrase_overrides.get(prop_name)
        if template:
            phrases.append(template.format(value=value_text))
        else:
            phrases.append(f"{prop_name}是{value_text}")
        used.add(prop_name)

    if phrases:
        line = f"{subject}的{'，'.join(phrases)}"
        add_single_line_pair(type_lines, line, prefix, is_regulation_doc, file_counter)
    return used


def add_subject_sentence_line(
    type_lines: list[str],
    prefix: str,
    subject: str,
    sentence_body: str,
    is_regulation_doc: bool = False,
    file_counter: Counter[str] | None = None,
) -> None:
    """输出一条以 subject 开头的完整知识条目。"""
    if sentence_body:
        line = f"{subject}{sentence_body}"
        add_single_line_pair(type_lines, line, prefix, is_regulation_doc, file_counter)


# ---------------------------------------------------------------------------
# 法规类节点转换
# ---------------------------------------------------------------------------


def convert_regulation_nodes(
    node_records: list[dict[str, Any]],
    type_lines: list[str],
    file_counter: Counter[str],
) -> None:
    """按 task2 的法规类规则处理节点属性。

    处理 label 为 法规文件、法条、条款单元 的节点。
    "法规依据"和"引用依据"节点跳过（由边关系处理）。
    """
    for record in node_records:
        node = extract_node(record)
        if node is None:
            continue

        properties = node.get("properties") or {}
        if not isinstance(properties, dict):
            continue

        subject = (
            value_to_text(properties.get("name"))
            if not is_empty_value(properties.get("name"))
            else ""
        )
        label = (
            value_to_text(properties.get("label"))
            if not is_empty_value(properties.get("label"))
            else ""
        )
        if not subject or not label:
            continue

        filename = first_filename(properties)
        prefix = f"{filename}中"
        cleaned = clean_node_properties(properties)

        # "法规依据"和"引用依据"需要结合边关系理解，节点本身不单独输出
        if label in {"法规依据", "引用依据"}:
            continue

        if label == "法规文件":
            # 法规文件节点不添加 prefix
            combined_properties = {"发布日期", "生效日期", "时效性"}
            handled = add_combined_property_line(
                type_lines,
                prefix,
                subject,
                properties,
                ["发布日期", "生效日期", "时效性"],
                is_regulation_doc=True,
                file_counter=file_counter,
            )
            for prop_name, prop_value in cleaned.items():
                if prop_name in {"文件全称", "文件别名"} and value_to_text(prop_value) == subject:
                    continue
                if prop_name in combined_properties or prop_name in handled:
                    continue
                add_property_line(
                    type_lines, prefix, subject, prop_name, prop_value,
                    is_regulation_doc=True,
                    file_counter=file_counter,
                )
            continue

        if label == "法条":
            hierarchy_parts: list[str] = []
            for prop_name in ("编", "章", "节"):
                value = properties.get(prop_name)
                if not is_empty_value(value):
                    hierarchy_parts.append(value_to_text(value))
            if hierarchy_parts:
                add_single_line_pair(
                    type_lines,
                    f"{subject}属于{''.join(hierarchy_parts)}",
                    prefix,
                    file_counter=file_counter,
                )

            for prop_name, prop_value in cleaned.items():
                if prop_name == "条" and value_to_text(prop_value) in subject:
                    continue
                if prop_name in {"编", "章", "节"}:
                    continue
                add_property_line(type_lines, prefix, subject, prop_name, prop_value, file_counter=file_counter)
            continue

        if label == "条款单元":
            # 6月8日修订：先处理"量化特征"和"量化条件"的合并
            quant_handled: set[str] = set()
            quant_feature = properties.get("量化特征")
            quant_condition = properties.get("量化条件")
            has_feature = not is_empty_value(quant_feature)
            has_condition = not is_empty_value(quant_condition)

            if has_feature and has_condition:
                line = f"{subject}的量化特征是{value_to_text(quant_feature)}，量化条件是{value_to_text(quant_condition)}"
                add_single_line_pair(type_lines, line, prefix, file_counter=file_counter)
                quant_handled = {"量化特征", "量化条件"}
            elif has_feature:
                line = f"{subject}的量化特征是{value_to_text(quant_feature)}"
                add_single_line_pair(type_lines, line, prefix, file_counter=file_counter)
                quant_handled = {"量化特征"}
            elif has_condition:
                line = f"{subject}的量化条件是{value_to_text(quant_condition)}"
                add_single_line_pair(type_lines, line, prefix, file_counter=file_counter)
                quant_handled = {"量化条件"}

            special_properties = {
                "功能类型", "责任角色", "行为描述", "例外情形",
                "时间要素", "其他信息", "量化特征", "量化条件",
            }
            for prop_name, prop_value in cleaned.items():
                if prop_name in {"单元层级", "单元编号"} and value_to_text(prop_value) in subject:
                    continue
                if prop_name in quant_handled:
                    continue
                if prop_name == "功能类型":
                    add_single_line_pair(
                        type_lines,
                        f"{subject}是{value_to_text(prop_value)}类型的条款",
                        prefix,
                        file_counter=file_counter,
                    )
                elif prop_name == "责任角色":
                    add_single_line_pair(
                        type_lines,
                        f"在{subject}条款中，主体的责任角色是{value_to_text(prop_value)}",
                        prefix,
                        file_counter=file_counter,
                    )
                elif prop_name == "行为描述":
                    add_single_line_pair(
                        type_lines,
                        f"{subject}涉及的具体行为是{value_to_text(prop_value)}",
                        prefix,
                        file_counter=file_counter,
                    )
                elif prop_name == "例外情形":
                    add_single_line_pair(
                        type_lines,
                        f"{subject}不适用于以下情形：{value_to_text(prop_value)}",
                        prefix,
                        file_counter=file_counter,
                    )
                elif prop_name == "时间要素":
                    add_single_line_pair(
                        type_lines,
                        f"{subject}涉及到的时间要素为：{value_to_text(prop_value)}",
                        prefix,
                        file_counter=file_counter,
                    )
                elif prop_name == "其他信息":
                    add_single_line_pair(
                        type_lines,
                        f"{subject}里包含的其他信息有：{value_to_text(prop_value)}",
                        prefix,
                        file_counter=file_counter,
                    )
                elif prop_name not in special_properties:
                    add_property_line(type_lines, prefix, subject, prop_name, prop_value, file_counter=file_counter)
            continue

        # 其他 label 的节点按通用 a1 方式处理
        for prop_name, prop_value in cleaned.items():
            add_property_line(type_lines, prefix, subject, prop_name, prop_value, file_counter=file_counter)


# ---------------------------------------------------------------------------
# 法规类边关系转换
# ---------------------------------------------------------------------------


def convert_regulation_edges(
    edge_records: list[dict[str, Any]],
    identity_index: dict[Any, dict[str, Any]],
    element_id_index: dict[str, dict[str, Any]],
    type_lines: list[str],
    file_counter: Counter[str],
) -> None:
    """按 task2 的法规类规则处理边关系。

    "法规文件-包含-法条" 和 "法条-包含-条款单元" 聚合计数输出。
    "依据"和"涉及"关系按规则生成自然语言句。
    """
    contains_law_article_count: Counter[tuple[str, str]] = Counter()
    contains_clause_unit_count: Counter[tuple[str, str]] = Counter()
    relation_lines: list[str] = []

    for record in edge_records:
        edge = extract_edge(record)
        if edge is None:
            continue

        predicate = (
            value_to_text(edge.get("type"))
            if not is_empty_value(edge.get("type"))
            else ""
        )
        if not predicate:
            continue

        start_node = find_node_info(
            identity=edge.get("start"),
            element_id=edge.get("startNodeElementId"),
            identity_index=identity_index,
            element_id_index=element_id_index,
        )
        end_node = find_node_info(
            identity=edge.get("end"),
            element_id=edge.get("endNodeElementId"),
            identity_index=identity_index,
            element_id_index=element_id_index,
        )
        if not start_node or not end_node:
            continue

        subject = str(start_node.get("name", ""))
        obj = str(end_node.get("name", ""))
        start_label = str(start_node.get("label", ""))
        end_label = str(end_node.get("label", ""))
        if not subject or not obj:
            continue

        filename = edge_filename(edge, start_node, end_node)
        prefix = f"{filename}中"
        triple = (start_label, predicate, end_label)

        if triple == ("法规文件", "包含", "法条"):
            contains_law_article_count[(prefix, subject)] += 1
            continue
        if triple == ("法条", "包含", "条款单元"):
            contains_clause_unit_count[(prefix, subject)] += 1
            continue

        if predicate == "依据":
            relation_lines.append(f"{prefix}，{subject}{predicate}{obj}")
            continue

        if predicate == "涉及":
            if triple == ("条款单元", "涉及", "引用依据"):
                end_properties = end_node.get("properties") or {}
                quote_relation = (
                    value_to_text(end_properties.get("引用关系"))
                    if not is_empty_value(end_properties.get("引用关系"))
                    else predicate
                )
                quote_purpose = (
                    value_to_text(end_properties.get("引用目的"))
                    if not is_empty_value(end_properties.get("引用目的"))
                    else ""
                )
                line = (
                    f"{prefix}，{subject}{quote_relation}{obj}，目的是{quote_purpose}"
                    if quote_purpose
                    else f"{prefix}，{subject}{quote_relation}{obj}"
                )
                relation_lines.append(line)
            else:
                relation_lines.append(f"{prefix}，{subject}{predicate}{obj}")
            continue

        # 其他边类型保留为普通关系句
        relation_lines.append(f"{prefix}，{subject}{predicate}{obj}")

    # 输出包含关系聚合统计
    for (prefix, subject), count in sorted(contains_law_article_count.items()):
        add_single_line_pair(type_lines, f"{subject}包含{count}条法条", prefix, file_counter=file_counter)

    for (prefix, subject), count in sorted(contains_clause_unit_count.items()):
        add_single_line_pair(type_lines, f"{subject}包含{count}条条款单元", prefix, file_counter=file_counter)

    # 输出其他关系条目（line 已包含 "{prefix}，" 前缀）
    for line in relation_lines:
        # 从行首提取 filename 进行计数
        if "中，" in line:
            fn = line.split("中，")[0]
        else:
            fn = "未知来源"
        add_type_line(type_lines, line, file_counter, fn)


# ---------------------------------------------------------------------------
# 输出写入
# ---------------------------------------------------------------------------

_TEMP_DIR = Path(__file__).resolve().parents[1] / "temp"


def write_txt(
    type_lines: list[str],
    graph_type: str,
    output_dir: str | Path | None = None,
) -> Path:
    """将知识条目写入 <output_dir>/txt/<graph_type>.txt。"""
    output_root = Path(output_dir) if output_dir is not None else _TEMP_DIR
    txt_dir = output_root / "txt"
    txt_dir.mkdir(parents=True, exist_ok=True)
    safe_name = sanitize_filename(graph_type)
    txt_path = txt_dir / f"{safe_name}.txt"
    with txt_path.open("w", encoding="utf-8", newline="\n") as f:
        for line in type_lines:
            f.write(f"{line}\n")
    return txt_path


def write_csv(
    type_lines: list[str],
    graph_type: str,
    file_stats: dict[str, int],
    output_dir: str | Path | None = None,
) -> Path:
    """生成统计 CSV，写入 <output_dir>/csv/知识条目统计.csv。"""
    output_root = Path(output_dir) if output_dir is not None else _TEMP_DIR
    csv_dir = output_root / "csv"
    csv_dir.mkdir(parents=True, exist_ok=True)
    csv_path = csv_dir / "知识条目统计.csv"
    with csv_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["统计维度", "名称", "知识条目数"])
        writer.writerow(["知识图谱类型", graph_type, len(type_lines)])
        for source_file, count in sorted(file_stats.items()):
            writer.writerow(["来源文件", source_file, count])
    return csv_path


# ---------------------------------------------------------------------------
# 主转换入口
# ---------------------------------------------------------------------------


def convert_json_to_text(
    graph_type: str,
    *,
    input_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
) -> dict[str, Any]:
    """
    从 temp/json/ 读取 node.json 和 edge.json，
    按法规类规则转换为自然语言知识并输出到 temp/txt/ 和 temp/csv/。

    Args:
        graph_type: 知识图谱类型，如 "行政监管规则"

    Returns:
        dict: 包含 counts、txt_path、csv_path 的统计信息
    """
    json_dir = Path(input_dir) if input_dir is not None else _TEMP_DIR / "json"
    node_path = json_dir / "node.json"
    edge_path = json_dir / "edge.json"

    node_records = load_json_array(node_path)
    edge_records = load_json_array(edge_path)

    type_lines: list[str] = []
    file_counter: Counter[str] = Counter()

    identity_index, element_id_index = build_node_indexes(node_records)

    convert_regulation_nodes(node_records, type_lines, file_counter)
    convert_regulation_edges(edge_records, identity_index, element_id_index, type_lines, file_counter)

    # 写入结果
    txt_path = write_txt(type_lines, graph_type, output_dir)
    file_stats = dict(file_counter)
    csv_path = write_csv(type_lines, graph_type, file_stats, output_dir)

    logger.info(f"自然语言知识总条数：{len(type_lines)}")
    logger.info(f"输出 txt：{txt_path}")
    logger.info(f"输出 csv：{csv_path}")

    return {
        "total_entries": len(type_lines),
        "source_files": len(file_stats),
        "txt_path": str(txt_path),
        "csv_path": str(csv_path),
    }
