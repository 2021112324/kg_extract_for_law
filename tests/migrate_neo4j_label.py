"""将一个图数据库某标签下的数据，迁移到另一个图数据库的某标签下。

参考 t_neo4j.py 使用 Neo4jAdapter；用两个适配器分别连接源库和目标库，
读取源标签下的全部节点与关系，按业务 id 批量 MERGE 写入目标标签
（目标标签名可以与源标签名不同）。

用法：
    1. 修改下方 __main__ 中的源库/目标库连接参数与标签名；
    2. 运行：python tests/migrate_neo4j_label.py
"""

import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable

# 将项目根目录加入 sys.path，解决 ModuleNotFoundError
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.infrastructure.graph_storage.neo4j_adapter import Neo4jAdapter


def _chunks(items: list, size: int) -> Iterable[list]:
    """按固定大小切分列表。"""
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _safe_rel_type(rel_type: str) -> str:
    """将关系类型清洗为 Cypher 允许的标识符。"""
    return "".join(c if c.isalnum() else "_" for c in rel_type) or "rel"


def _log(message: str) -> None:
    """打印一条进度日志并立即刷新到控制台。"""
    print(message, flush=True)


def migrate_label(
    source: Neo4jAdapter,
    source_tag: str,
    target: Neo4jAdapter,
    target_tag: str,
    batch_size: int = 1000,
) -> dict[str, Any]:
    """将 source 库 source_tag 标签数据迁移到 target 库 target_tag 标签。

    Args:
        source: 源库适配器（已连接）
        source_tag: 源标签名
        target: 目标库适配器（已连接）
        target_tag: 目标标签名（可与 source_tag 不同）
        batch_size: 批量导入大小

    Returns:
        dict: 迁移统计信息
    """
    _log("=" * 60)
    _log(f"开始迁移：源库[{source_tag}] → 目标库[{target_tag}]")
    _log("=" * 60)

    # 1. 读取源节点
    _log("[1/4] 读取源节点...")
    t0 = time.time()
    nodes = source.run_query(
        f"MATCH (n:`{source_tag}`) RETURN properties(n) AS props"
    )
    _log(f"      节点读取完成：{len(nodes)} 个（耗时 {time.time() - t0:.1f}s）")

    # 2. 读取源关系
    _log("[2/4] 读取源关系...")
    t0 = time.time()
    relationships = source.run_query(
        f"MATCH (a:`{source_tag}`)-[r]->(b:`{source_tag}`) "
        "RETURN a.id AS start_id, b.id AS end_id, type(r) AS type, properties(r) AS props"
    )
    _log(f"      关系读取完成：{len(relationships)} 条（耗时 {time.time() - t0:.1f}s）")

    # 3. 目标库创建唯一约束（加速 MERGE 与关系 MATCH）
    _log(f"[3/4] 目标库创建唯一约束 :{target_tag}(id)...")
    t0 = time.time()
    with target.driver.session(database=target.database) as session:
        session.run(
            f"CREATE CONSTRAINT IF NOT EXISTS "
            f"FOR (n:`{target_tag}`) REQUIRE n.id IS UNIQUE"
        )
    _log(f"      约束创建完成（耗时 {time.time() - t0:.1f}s）")

    # 4. 批量写入节点
    _log(f"[4/4] 写入目标标签 [{target_tag}]...")
    t_write = time.time()
    total_node_batches = (len(nodes) + batch_size - 1) // batch_size
    with target.driver.session(database=target.database) as session:
        for i, batch in enumerate(_chunks(nodes, batch_size), 1):
            session.run(
                "UNWIND $batch AS row "
                "MERGE (n:`" + target_tag + "` {id: row.props.id}) "
                "SET n += row.props, n.graph_tag = $graph_tag",
                batch=batch,
                graph_tag=target_tag,
            )
            done = min(i * batch_size, len(nodes))
            _log(f"      节点进度：{done}/{len(nodes)}（批 {i}/{total_node_batches}）")
    _log(f"      节点写入完成（耗时 {time.time() - t_write:.1f}s）")

    # 5. 按关系类型分组，批量写入关系
    by_type: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for rel in relationships:
        if rel["start_id"] is None or rel["end_id"] is None:
            continue
        by_type[rel["type"]].append(rel)

    _log(f"      开始写入关系（共 {len(by_type)} 种类型）...")
    t_rel = time.time()
    with target.driver.session(database=target.database) as session:
        for type_idx, (rel_type, rels) in enumerate(by_type.items(), 1):
            safe_type = _safe_rel_type(rel_type)
            total_rel_batches = (len(rels) + batch_size - 1) // batch_size
            for i, batch in enumerate(_chunks(rels, batch_size), 1):
                session.run(
                    f"UNWIND $batch AS row "
                    f"MATCH (a:`{target_tag}` {{id: row.start_id}}) "
                    f"MATCH (b:`{target_tag}` {{id: row.end_id}}) "
                    f"MERGE (a)-[r:`{safe_type}`]->(b) "
                    f"SET r += row.props, r.graph_tag = $graph_tag",
                    batch=batch,
                    graph_tag=target_tag,
                )
                done = min(i * batch_size, len(rels))
                _log(
                    f"      关系类型[{type_idx}/{len(by_type)}] {rel_type}："
                    f"{done}/{len(rels)}（批 {i}/{total_rel_batches}）"
                )
    _log(f"      关系写入完成（耗时 {time.time() - t_rel:.1f}s）")

    stats = {
        "source_tag": source_tag,
        "target_tag": target_tag,
        "node_count": len(nodes),
        "relationship_count": len(relationships),
    }
    _log("=" * 60)
    _log(
        f"迁移完成：节点 {len(nodes)} 个，关系 {len(relationships)} 条，"
        f"总耗时 {time.time() - t_write:.1f}s"
    )
    _log("=" * 60)
    return stats


if __name__ == "__main__":
    # 源库
    source = Neo4jAdapter(
        uri="bolt://39.96.198.93:7687",
        username="neo4j",
        password="ug4GMLnArVaKz8z",
        database="neo4j",
    )
    # 目标库
    # target = Neo4jAdapter(
    #     uri="neo4j+s://neo4j.i.hitices.cn:7687",
    #     username="neo4j",
    #     password="da9d00ab3db0238a",
    #     database="neo4j",
    # )

    target = Neo4jAdapter(
        uri="bolt://39.96.198.93:7687",
        username="neo4j",
        password="ug4GMLnArVaKz8z",
        database="neo4j",
    )

    _log("正在连接源库...")
    if not source.connect():
        _log("源库连接失败")
        sys.exit(1)
    _log("源库连接成功")

    _log("正在连接目标库...")
    if not target.connect():
        _log("目标库连接失败")
        sys.exit(1)
    _log("目标库连接成功")

    try:
        # 将源库 kg_366920576398295040 标签数据迁移到目标库同名标签
        migrate_label(
            source=source,
            source_tag="ad1_行政复议法规_中_kg_636100095850315776",
            target=target,
            target_tag="行政复议法律法规",
        )
        # 如需改名，把 target_tag 改成别的名字即可，例如：
        # migrate_label(source, "kg_366920576398295040", target, "法律法规条款")
    finally:
        source.disconnect()
        target.disconnect()
        _log("连接已关闭")
