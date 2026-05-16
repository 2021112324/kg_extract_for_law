"""
图谱节点匹配工具。

复用项目已有的 GraphNode 模型，将"法规依据"和"引用依据"
等中间节点匹配到实际的"法规文件"和"法条"节点。
"""
import re
import logging
from typing import Optional, Dict, List

from app.infrastructure.graph_storage.base import GraphNode

logger = logging.getLogger(__name__)

# 条款编号正则：匹配 "第X条" 头部
CLAUSE_HEAD_RE = re.compile(r'第([零一二三四五六七八九十百千万\d]+)\s*条')

# 中文数字映射
CN_NUM = {
    '零': 0, '一': 1, '二': 2, '三': 3, '四': 4,
    '五': 5, '六': 6, '七': 7, '八': 8, '九': 9,
    '十': 10, '百': 100, '千': 1000, '万': 10000,
}


# ---- 工具函数 ----

def node_label(node: GraphNode) -> str:
    """获取节点的有效 label。

    兼容两种来源：get_visualization_data（label 来自 Neo4j 标签）和
    add_subgraph_with_merge（label 作为属性存储）。"""
    return node.label or node.properties.get("label", "")


def _cn_to_int(cn: str) -> int:
    """中文数字 → 整数，如 '一百二十三' → 123。"""
    cn = cn.strip()
    if cn.isdigit():
        return int(cn)
    total = section = 0
    for ch in cn:
        v = CN_NUM.get(ch)
        if v is None:
            continue
        if v >= 10:
            if section == 0:
                section = 1
            total += section * v
            section = 0
        else:
            section = v
    return total + section


def _normalize_clause_head(head: str) -> str:
    """将中文条款编号转为阿拉伯数字形式，如 '第五条' → '第5条'。"""
    m = CLAUSE_HEAD_RE.match(head)
    if not m:
        return head
    try:
        return f'第{_cn_to_int(m.group(1))}条'
    except (ValueError, KeyError):
        return head


def parse_clause_number(clause_str: str) -> Optional[str]:
    """从条款编号中提取 '第X条'。"""
    if not clause_str:
        return None
    m = CLAUSE_HEAD_RE.search(str(clause_str))
    return m.group(0) if m else None


def _to_str_list(val) -> List[str]:
    """将属性值统一为字符串列表。"""
    if val is None:
        return []
    if isinstance(val, list):
        return [str(v) for v in val if v]
    return [str(val)]


# ---- 文件名匹配 ----

def match_node_to_file(
    node: GraphNode,
    target_files: List[GraphNode],
) -> Optional[GraphNode]:
    """将中间节点（法规依据/引用依据）匹配到目标法规文件节点列表。

    匹配策略（优先级递减）:
        1. 文件全称 ↔ 文件全称 精确匹配
        2. 交叉匹配：全称 ↔ 别名、别名 ↔ 别名
        3. 加/去 "中华人民共和国" 前缀双向匹配

    Args:
        node: 待匹配节点（属性含 "文件全称"、可选 "文件别名"）
        target_files: 目标法规文件节点列表

    Returns:
        匹配到的 GraphNode，未匹配返回 None。
    """
    search_names: List[str] = []
    fullname = node.properties.get("文件全称", "")
    if fullname:
        search_names.append(str(fullname))
    alias = node.properties.get("文件别名", "")
    if alias:
        search_names.extend(_to_str_list(alias))

    if not search_names or not target_files:
        return None

    # 建立目标节点索引
    by_fullname: Dict[str, GraphNode] = {}
    by_alias: Dict[str, GraphNode] = {}
    for f in target_files:
        fn = str(f.properties.get("文件全称", ""))
        if fn:
            by_fullname[fn] = f
        for a in _to_str_list(f.properties.get("文件别名", "")):
            if a:
                by_alias[a] = f

    # 1-2. 精确 + 交叉
    for name in search_names:
        if name in by_fullname:
            return by_fullname[name]
        if name in by_alias:
            return by_alias[name]

    # 3. 加/去"中华人民共和国"前缀双向匹配
    for name in search_names:
        if not name.startswith("中华人民共和国"):
            prefixed = "中华人民共和国" + name
            if prefixed in by_fullname:
                return by_fullname[prefixed]
            if prefixed in by_alias:
                return by_alias[prefixed]
        else:
            stripped = name[7:]
            if stripped and stripped in by_fullname:
                return by_fullname[stripped]
            if stripped and stripped in by_alias:
                return by_alias[stripped]

    return None


# ---- 法条匹配（Cypher 直查） ----

def match_clause_node_via_cypher(
    clause_number: str,
    parent_file_id: str,
    adapter,
    tag: str,
) -> Optional[str]:
    """在父法规文件下，通过 Cypher 查找匹配的法条节点。

    不预建全表映射，而是直接查询 Neo4j：
        MATCH (file:`{tag}` {id: $fid})-[:包含]->(clause:`{tag}`)
        WHERE clause.label = '法条'
        RETURN clause

    然后在返回的候选法条中做字符串匹配。

    Args:
        clause_number: 条款编号（如 "第十条第一款"）
        parent_file_id: 已匹配到的父法规文件 node.id
        adapter: Neo4jAdapter 实例
        tag: 目标图谱标签

    Returns:
        匹配到的法条 node.id，未匹配返回 None。
    """
    parsed_head = parse_clause_number(clause_number)
    if not parsed_head:
        return None

    # 用 Cypher 查找该法规文件下的所有法条
    query = f"""
        MATCH (file:`{tag}` {{id: $fid}})-[:包含]->(clause:`{tag}`)
        WHERE clause.label = '法条'
        RETURN clause.id AS clause_id, clause.name AS clause_name, clause.条 AS tiao
    """
    records = adapter.run_query(query, {"fid": parent_file_id})

    if not records:
        return None

    # 第一轮：直接字符串匹配
    for rec in records:
        name = rec.get("clause_name", "") or ""
        if name and parsed_head in name:
            return rec["clause_id"]
        tiao = str(rec.get("tiao", ""))
        if tiao and parsed_head in tiao:
            return rec["clause_id"]

    # 第二轮：归一化后匹配
    normalized_head = _normalize_clause_head(parsed_head)
    for rec in records:
        name = rec.get("clause_name", "") or ""
        if name and _normalize_clause_head(name) == normalized_head:
            return rec["clause_id"]
        tiao = str(rec.get("tiao", ""))
        if tiao and _normalize_clause_head(tiao) == normalized_head:
            return rec["clause_id"]

    return None
