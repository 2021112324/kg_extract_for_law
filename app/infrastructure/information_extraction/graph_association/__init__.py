"""
图谱关联模块。

将 Neo4j 图谱中的中间节点（法规依据、引用依据）解析为对
实际节点（法规文件、法条）的直接引用关系。

通过 Neo4jAdapter 直接操作 Neo4j，仅取元数据节点做匹配，
用 Cypher 原地替换关系，避免全量拉取内存。

主入口: associate_graphs(adapter, tag_a, tag_b, log_path=None)
"""
import json
import logging
import os
from datetime import datetime
from typing import Dict, Optional

from app.infrastructure.graph_storage.neo4j_adapter import Neo4jAdapter

from .association import GraphAssociator

logger = logging.getLogger(__name__)


def associate_graphs(
    adapter: Neo4jAdapter,
    tag_a: str,
    tag_b: str,
    log_path: Optional[str] = None,
) -> Dict:
    """将 tagA 和 tagB 图谱间的中间节点解析为直接引用关系。

    处理逻辑:
        - tagA == tagB: 执行一次单向关联（图谱内关联）
        - tagA != tagB: 执行两次关联（tagA→tagB 和 tagB→tagA）

    Args:
        adapter: 已连接的 Neo4jAdapter 实例
        tag_a: 图谱标签 A
        tag_b: 图谱标签 B
        log_path: 处理记录保存路径（JSON）。为 None 则不保存。
                  默认保存到当前模块下的 logs/ 目录。

    Returns:
        {
            "stats": {"tag_a->tag_b": {"matched": N, "unmatched": N, "removed": N}, ...},
            "log_path": "path/to/log.json" 或 None
        }
    """
    associator = GraphAssociator(adapter)

    if tag_a == tag_b:
        logger.info(f"单向关联: {tag_a} → {tag_a}")
        associator.associate(tag_a, tag_a)
    else:
        logger.info(f"双向关联:")
        logger.info(f"  方向1: {tag_a} → {tag_b}")
        associator.associate(tag_a, tag_b)
        logger.info(f"  方向2: {tag_b} → {tag_a}")
        associator.associate(tag_b, tag_a)

    # 构建完整日志
    log_data = _build_log(tag_a, tag_b, associator)

    # 保存日志文件
    saved_path = None
    if log_path is None:
        # 默认路径: graph_association/logs/ 目录
        log_dir = os.path.join(os.path.dirname(__file__), "logs")
    else:
        log_dir = os.path.dirname(log_path) or "."
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        saved_path = log_path or os.path.join(log_dir, f"association_{tag_a}_{tag_b}_{ts}.json")
        with open(saved_path, "w", encoding="utf-8") as f:
            json.dump(log_data, f, ensure_ascii=False, indent=2)
        logger.info(f"处理记录已保存: {saved_path}")

    return {"stats": associator.stats, "log_path": saved_path}


def _build_log(tag_a: str, tag_b: str, associator: GraphAssociator) -> Dict:
    """构建完整的处理记录 JSON。"""
    # 汇总统计
    total_matched = sum(s["matched"] for s in associator.stats.values())
    total_unmatched = sum(s["unmatched"] for s in associator.stats.values())
    total = total_matched + total_unmatched

    return {
        "标题": "图谱关联处理记录",
        "处理时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "图谱A": tag_a,
        "图谱B": tag_b,
        "关联方向": list(associator.stats.keys()),
        "汇总": {
            "总处理数": total,
            "匹配成功": total_matched,
            "匹配失败": total_unmatched,
            "各方向统计": associator.stats,
        },
        "处理明细": associator.records,
    }
