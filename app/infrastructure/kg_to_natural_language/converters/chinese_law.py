"""
JSON 图谱数据转自然语言知识条目模块 v2

按"过滤无用属性 + 同类属性组装"规则，将 node.json/edge.json 转换为
自然语言知识条目，产出五类知识：

1. 规则知识（条款单元结构化组装，受 ENABLE_STRUCTURED_RULE_ASSEMBLY 控制）
2. 条款单元内容知识（原文，始终产出）
3. 法规文件档案知识（身份/效力/目的/范围）
4. 法条效力知识（效力范围）
5. 引用知识（引用/涉及/依据关系）

与 converter.py（v1，属性转写）并存，不修改 v1。
"""

from __future__ import annotations

import ast
import csv
import json
import logging
import re
from collections import Counter
from pathlib import Path
from typing import Any

# 复用 v1 中的工具函数
from .base import (
    EMPTY_TEXT_MARKERS,
    is_empty_value,
    value_to_text,
    sanitize_filename,
    load_json_array,
    first_filename,
    extract_node,
    extract_edge,
    build_node_indexes,
    find_node_info,
    edge_filename,
)
from .source_context import add_chinese_source_context, clean_source_name

logger = logging.getLogger(__name__)

# 为 True 时：产出"规则知识"（结构化组装）+ "条款单元内容知识"（原文）
# 为 False 时：只产出"条款单元内容知识"（原文），取消结构化组装
ENABLE_STRUCTURED_RULE_ASSEMBLY = False

_TEMP_DIR = Path(__file__).resolve().parents[1] / "temp"


# ---------------------------------------------------------------------------
# 量化条件解析
# ---------------------------------------------------------------------------


def parse_quantification(quant_condition: Any) -> str:
    """解析量化条件，提取"原文件"字段作为可读量化文本。

    量化条件是以字符串存储的 Python 字典，如：
    "{'原文件': '100万元以下罚款', '量化值类型': '金额', ...}"
    提取其中的"原文件"字段（如"100万元以下罚款"），丢弃结构化字段。
    """
    if is_empty_value(quant_condition):
        return ""
    if isinstance(quant_condition, str):
        try:
            parsed = ast.literal_eval(quant_condition)
        except (ValueError, SyntaxError):
            return quant_condition.strip()
    elif isinstance(quant_condition, dict):
        parsed = quant_condition
    else:
        return value_to_text(quant_condition)

    if isinstance(parsed, dict):
        original = parsed.get("原文件")
        if not is_empty_value(original):
            return value_to_text(original)
        return ""
    return value_to_text(parsed)


# ---------------------------------------------------------------------------
# 通用输出辅助
# ---------------------------------------------------------------------------


def _emit(
    type_lines: list[str],
    file_counter: Counter[str],
    filename: str,
    line: str,
    *,
    source_name: str = "",
    source_location: str = "",
    add_source_context: bool = False,
    source_context_issues: Counter[str] | None = None,
) -> None:
    """输出一条知识并统计来源文件条目数。"""
    rendered = (
        add_chinese_source_context(line, source_name, source_location)
        if add_source_context
        else line
    )
    if (
        add_source_context
        and not clean_source_name(source_name)
        and source_context_issues is not None
    ):
        source_context_issues["非文件节点缺少有效法规名称"] += 1
    type_lines.append(rendered)
    file_counter[filename] += 1


def build_regulation_source_names(
    node_records: list[dict[str, Any]],
) -> dict[str, str]:
    """建立 filename 到法规文件规范名称的映射。"""
    source_names: dict[str, str] = {}
    for record in node_records:
        node = extract_node(record)
        if node is None:
            continue
        properties = node.get("properties") or {}
        if not isinstance(properties, dict):
            continue
        label = value_to_text(properties.get("label")) if not is_empty_value(properties.get("label")) else ""
        if label != "法规文件":
            continue
        filename = first_filename(properties)
        candidates = (
            properties.get("文件全称"),
            properties.get("name"),
            filename,
        )
        source_name = ""
        for value in candidates:
            if is_empty_value(value):
                continue
            source_name = clean_source_name(value_to_text(value))
            if source_name:
                break
        if source_name:
            source_names[filename] = source_name
    return source_names


def regulation_source_name(filename: str, source_names: dict[str, str]) -> str:
    """优先返回法规文件节点规范名称，否则回退到清理后的 filename。"""
    return source_names.get(filename) or clean_source_name(filename)


# ---------------------------------------------------------------------------
# 法规文件 → 档案知识
# ---------------------------------------------------------------------------


def convert_regulation_documents(
    node_records: list[dict[str, Any]],
    type_lines: list[str],
    file_counter: Counter[str],
) -> None:
    """法规文件节点组装成档案知识（身份/效力/目的/范围，最多 4 条）。"""
    for record in node_records:
        node = extract_node(record)
        if node is None:
            continue
        properties = node.get("properties") or {}
        if not isinstance(properties, dict):
            continue
        name = value_to_text(properties.get("name")) if not is_empty_value(properties.get("name")) else ""
        label = value_to_text(properties.get("label")) if not is_empty_value(properties.get("label")) else ""
        if not name or label != "法规文件":
            continue

        filename = first_filename(properties)

        def emit(line: str) -> None:
            _emit(type_lines, file_counter, filename, line)

        # 1. 文件身份：{name}[（{文号}）]是[{发布单位}发布的]{文件性质}
        identity_parts: list[str] = [name]
        wenhao = properties.get("文号")
        if not is_empty_value(wenhao):
            identity_parts.append(f"（{value_to_text(wenhao)}）")
        identity_parts.append("是")
        publisher = properties.get("发布单位")
        if not is_empty_value(publisher):
            identity_parts.append(f"{value_to_text(publisher)}发布的")
        nature = properties.get("文件性质")
        if not is_empty_value(nature):
            identity_parts.append(value_to_text(nature))
        emit("".join(identity_parts))

        # 2. 效力状态：{name}发布于{发布日期}[，{生效日期}生效]，{时效性}
        validity_parts: list[str] = []
        publish_date = properties.get("发布日期")
        if not is_empty_value(publish_date):
            validity_parts.append(f"发布于{value_to_text(publish_date)}")
        effective_date = properties.get("生效日期")
        if not is_empty_value(effective_date):
            validity_parts.append(f"{value_to_text(effective_date)}生效")
        timeliness = properties.get("时效性")
        if not is_empty_value(timeliness):
            validity_parts.append(value_to_text(timeliness))
        if validity_parts:
            emit(f"{name}{'，'.join(validity_parts)}")

        # 3. 立法目的：{name}的制定目的是{制定目的}
        purpose = properties.get("制定目的")
        if not is_empty_value(purpose):
            emit(f"{name}的制定目的是{value_to_text(purpose)}")

        # 4. 适用范围：{name}的适用范围是{应用范围}
        scope = properties.get("应用范围")
        if not is_empty_value(scope):
            emit(f"{name}的适用范围是{value_to_text(scope)}")


# ---------------------------------------------------------------------------
# 法条 → 效力知识
# ---------------------------------------------------------------------------


def convert_law_articles(
    node_records: list[dict[str, Any]],
    type_lines: list[str],
    file_counter: Counter[str],
    source_names: dict[str, str] | None = None,
    source_context_issues: Counter[str] | None = None,
) -> None:
    """法条节点产出效力知识（1 条：效力范围）。"""
    for record in node_records:
        node = extract_node(record)
        if node is None:
            continue
        properties = node.get("properties") or {}
        if not isinstance(properties, dict):
            continue
        name = value_to_text(properties.get("name")) if not is_empty_value(properties.get("name")) else ""
        label = value_to_text(properties.get("label")) if not is_empty_value(properties.get("label")) else ""
        if not name or label != "法条":
            continue

        filename = first_filename(properties)
        scope = properties.get("效力范围")
        if not is_empty_value(scope):
            _emit(
                type_lines,
                file_counter,
                filename,
                f"{name}的效力范围是{value_to_text(scope)}",
                source_name=regulation_source_name(filename, source_names or {}),
                add_source_context=True,
                source_context_issues=source_context_issues,
            )


# ---------------------------------------------------------------------------
# 条款单元 → 规则知识 + 内容知识
# ---------------------------------------------------------------------------


def convert_clause_units(
    node_records: list[dict[str, Any]],
    type_lines: list[str],
    file_counter: Counter[str],
    source_names: dict[str, str] | None = None,
    source_context_issues: Counter[str] | None = None,
) -> None:
    """条款单元节点产出规则知识（结构化组装）+ 内容知识（原文）。"""
    for record in node_records:
        node = extract_node(record)
        if node is None:
            continue
        properties = node.get("properties") or {}
        if not isinstance(properties, dict):
            continue
        name = value_to_text(properties.get("name")) if not is_empty_value(properties.get("name")) else ""
        label = value_to_text(properties.get("label")) if not is_empty_value(properties.get("label")) else ""
        if not name or label != "条款单元":
            continue

        filename = first_filename(properties)
        source_name = regulation_source_name(filename, source_names or {})
        unit_number = (
            value_to_text(properties.get("单元编号"))
            if not is_empty_value(properties.get("单元编号"))
            else name
        )

        def emit(line: str, *, include_location: bool = True) -> None:
            _emit(
                type_lines,
                file_counter,
                filename,
                line,
                source_name=source_name,
                source_location=unit_number if include_location else "",
                add_source_context=True,
                source_context_issues=source_context_issues,
            )

        # 2. 条款单元内容知识（原文，始终产出）
        content = properties.get("条款单元内容")
        if not is_empty_value(content):
            emit(value_to_text(content))

        # 1. 规则知识（结构化组装，受常量控制）
        if not ENABLE_STRUCTURED_RULE_ASSEMBLY:
            continue

        subject = value_to_text(properties.get("适用主体")) if not is_empty_value(properties.get("适用主体")) else ""
        behavior = value_to_text(properties.get("行为描述")) if not is_empty_value(properties.get("行为描述")) else ""
        if not subject and not behavior:
            continue

        # 主句：{适用主体}[在{适用前提}的情况下]{行为描述}[，其量化标准为{量化标准}][，{例外情形}的除外]
        main_parts: list[str] = []
        if subject:
            main_parts.append(subject)
        precondition = properties.get("适用前提")
        if not is_empty_value(precondition):
            main_parts.append(f"在{value_to_text(precondition)}的情况下")
        if behavior:
            main_parts.append(behavior)

        if not main_parts:
            continue

        sentence = "".join(main_parts)

        quant_text = parse_quantification(properties.get("量化条件"))
        if quant_text:
            sentence += f"，其量化标准为{quant_text}"

        exception = properties.get("例外情形")
        if not is_empty_value(exception):
            sentence += f"，{value_to_text(exception)}的除外"

        sentence += "。"
        emit(sentence)

        # 后果句：[违反{条款单元}规定的，{法律后果}。]
        consequence = properties.get("法律后果")
        if not is_empty_value(consequence):
            emit(
                f"违反{name}规定的，{value_to_text(consequence)}。",
                include_location=False,
            )


# ---------------------------------------------------------------------------
# 边关系 → 引用知识（删除"包含"关系）
# ---------------------------------------------------------------------------


def convert_references(
    edge_records: list[dict[str, Any]],
    identity_index: dict[Any, dict[str, Any]],
    element_id_index: dict[str, dict[str, Any]],
    type_lines: list[str],
    file_counter: Counter[str],
    source_names: dict[str, str] | None = None,
    source_context_issues: Counter[str] | None = None,
) -> None:
    """边关系产出引用知识，删除"包含"（层级）关系。"""
    for record in edge_records:
        edge = extract_edge(record)
        if edge is None:
            continue

        predicate = value_to_text(edge.get("type")) if not is_empty_value(edge.get("type")) else ""
        if not predicate:
            continue

        # 删除"包含"层级关系
        if predicate == "包含":
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
        source_name = regulation_source_name(filename, source_names or {})

        def emit(line: str) -> None:
            _emit(
                type_lines,
                file_counter,
                filename,
                line,
                source_name=source_name,
                add_source_context=start_label != "法规文件",
                source_context_issues=source_context_issues,
            )

        # 条款单元 -引用/涉及→ 引用依据
        if end_label == "引用依据":
            end_properties = end_node.get("properties") or {}
            quote_relation = (
                value_to_text(end_properties.get("引用关系"))
                if not is_empty_value(end_properties.get("引用关系"))
                else predicate
            )
            quote_type = (
                value_to_text(end_properties.get("引用类型"))
                if not is_empty_value(end_properties.get("引用类型"))
                else ""
            )
            quote_purpose = (
                value_to_text(end_properties.get("引用目的"))
                if not is_empty_value(end_properties.get("引用目的"))
                else ""
            )
            file_full_name = (
                value_to_text(end_properties.get("文件全称"))
                if not is_empty_value(end_properties.get("文件全称"))
                else obj
            )
            clause_num = (
                value_to_text(end_properties.get("条款编号"))
                if not is_empty_value(end_properties.get("条款编号"))
                else ""
            )

            # 引用类型 = 条款 时带条款编号
            reference_obj = file_full_name + clause_num if quote_type == "条款" else file_full_name

            line = f"{subject}{quote_relation}{reference_obj}"
            if quote_purpose:
                line += f"，目的是{quote_purpose}"
            emit(line)
            continue

        # 法规文件 -依据→ 法规依据
        if predicate == "依据" and end_label == "法规依据":
            emit(f"{subject}依据{obj}制定")
            continue

        # 条款单元 -依据→ 条款单元/法条
        if predicate == "依据":
            emit(f"{subject}依据{obj}")
            continue

        # 其余引用类关系（如"引用"）按通用方式
        emit(f"{subject}{predicate}{obj}")


# ---------------------------------------------------------------------------
# 输出写入
# ---------------------------------------------------------------------------


def write_txt(type_lines: list[str], graph_type: str, output_dir: str | Path | None = None) -> Path:
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


def convert_json_to_text_v2(
    graph_type: str,
    *,
    input_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
) -> dict[str, Any]:
    """
    从 node.json/edge.json 读取数据，按"过滤+组装"规则转换为自然语言知识。

    Args:
        graph_type: 知识图谱类型，如 "行政监管规则"
        input_dir: 包含 node.json 和 edge.json 的目录；默认使用模块 temp/json
        output_dir: 输出根目录；默认使用模块 temp

    Returns:
        dict: 包含 counts、txt_path、csv_path 的统计信息
    """
    input_root = Path(input_dir) if input_dir is not None else _TEMP_DIR / "json"
    node_path = input_root / "node.json"
    edge_path = input_root / "edge.json"

    node_records = load_json_array(node_path)
    edge_records = load_json_array(edge_path)

    type_lines: list[str] = []
    file_counter: Counter[str] = Counter()
    source_context_issues: Counter[str] = Counter()

    identity_index, element_id_index = build_node_indexes(node_records)
    source_names = build_regulation_source_names(node_records)

    # 核心知识优先，法条效力范围作为补充知识最后产出。
    convert_regulation_documents(node_records, type_lines, file_counter)
    convert_clause_units(
        node_records,
        type_lines,
        file_counter,
        source_names,
        source_context_issues,
    )
    convert_references(
        edge_records,
        identity_index,
        element_id_index,
        type_lines,
        file_counter,
        source_names,
        source_context_issues,
    )
    convert_law_articles(
        node_records,
        type_lines,
        file_counter,
        source_names,
        source_context_issues,
    )

    # 写入结果
    txt_path = write_txt(type_lines, graph_type, output_dir)
    file_stats = dict(file_counter)
    csv_path = write_csv(type_lines, graph_type, file_stats, output_dir)

    logger.info(f"自然语言知识总条数：{len(type_lines)}")
    logger.info(f"输出 txt：{txt_path}")
    logger.info(f"输出 csv：{csv_path}")
    logger.info("来源语境问题统计：%s", dict(source_context_issues))

    return {
        "total_entries": len(type_lines),
        "source_files": len(file_stats),
        "txt_path": str(txt_path),
        "csv_path": str(csv_path),
        "source_context_issue_counts": dict(source_context_issues),
    }
