"""
图谱关联模块 — Neo4j 集成测试。

将测试数据存入 Neo4j，调用 associate_graphs(adapter, tag_a, tag_b) 测试匹配功能。
"""
import sys
sys.path.insert(0, r"f:\企业大脑知识库系统\8.1项目\抽取代码\kg_extract_for_law")

from app.infrastructure.graph_storage.neo4j_adapter import Neo4jAdapter
from app.infrastructure.information_extraction.graph_association import associate_graphs


# ============================================================
# 测试数据（与旧版相同，通过 add_subgraph_with_merge 导入）
# ============================================================

def build_tag_test_a():
    return {
        "nodes": [
            {"node_id": "file_刑法", "node_name": "中华人民共和国刑法", "node_type": "法规文件",
             "properties": {"label": "法规文件", "文件全称": "中华人民共和国刑法", "文件别名": "刑法"}},
            {"node_id": "file_刑诉法", "node_name": "中华人民共和国刑事诉讼法", "node_type": "法规文件",
             "properties": {"label": "法规文件", "文件全称": "中华人民共和国刑事诉讼法", "文件别名": "刑事诉讼法"}},
            {"node_id": "basis_网络安全法", "node_name": "中华人民共和国网络安全法", "node_type": "法规依据",
             "properties": {"label": "法规依据", "文件全称": "中华人民共和国网络安全法"}},
            {"node_id": "basis_数据安全法", "node_name": "数据安全法", "node_type": "法规依据",
             "properties": {"label": "法规依据", "文件全称": "数据安全法"}},
            {"node_id": "clause_刑法第五条", "node_name": "中华人民共和国刑法第五条", "node_type": "法条",
             "properties": {"label": "法条", "条": "第五条"}},
            {"node_id": "clause_刑诉第十条", "node_name": "中华人民共和国刑事诉讼法第十条", "node_type": "法条",
             "properties": {"label": "法条", "条": "第十条"}},
            {"node_id": "unit_A", "node_name": "条款单元A", "node_type": "条款单元",
             "properties": {"label": "条款单元"}},
            {"node_id": "cite_网络安全法_文件", "node_name": "引用依据-网络安全法", "node_type": "引用依据",
             "properties": {"label": "引用依据", "引用类型": "文件", "文件全称": "中华人民共和国网络安全法"}},
            {"node_id": "cite_刑法第五条_条款", "node_name": "引用依据-刑法第五条", "node_type": "引用依据",
             "properties": {"label": "引用依据", "引用类型": "条款", "文件全称": "中华人民共和国刑法", "条款编号": "第五条"}},
            {"node_id": "cite_刑诉第十条_条款", "node_name": "引用依据-刑诉第十条", "node_type": "引用依据",
             "properties": {"label": "引用依据", "引用类型": "条款", "文件全称": "中华人民共和国刑事诉讼法", "条款编号": "第十条"}},
            {"node_id": "cite_不存在_文件", "node_name": "引用依据-不存在的文件", "node_type": "引用依据",
             "properties": {"label": "引用依据", "引用类型": "文件", "文件全称": "不存在法"}},
        ],
        "edges": [
            {"source_id": "file_刑法", "relation_type": "依据", "target_id": "basis_网络安全法", "properties": {"label": "依据"}},
            {"source_id": "file_刑诉法", "relation_type": "依据", "target_id": "basis_数据安全法", "properties": {"label": "依据"}},
            {"source_id": "clause_刑法第五条", "relation_type": "包含", "target_id": "unit_A", "properties": {"label": "包含"}},
            {"source_id": "unit_A", "relation_type": "引用", "target_id": "cite_网络安全法_文件", "properties": {"label": "引用"}},
            {"source_id": "unit_A", "relation_type": "引用", "target_id": "cite_刑法第五条_条款", "properties": {"label": "引用"}},
            {"source_id": "unit_A", "relation_type": "引用", "target_id": "cite_刑诉第十条_条款", "properties": {"label": "引用"}},
            {"source_id": "unit_A", "relation_type": "引用", "target_id": "cite_不存在_文件", "properties": {"label": "引用"}},
        ],
    }


def build_tag_test_b():
    return {
        "nodes": [
            {"node_id": "t_file_网络安全法", "node_name": "中华人民共和国网络安全法", "node_type": "法规文件",
             "properties": {"label": "法规文件", "文件全称": "中华人民共和国网络安全法"}},
            {"node_id": "t_file_数据安全法", "node_name": "中华人民共和国数据安全法", "node_type": "法规文件",
             "properties": {"label": "法规文件", "文件全称": "中华人民共和国数据安全法", "文件别名": "数据安全法"}},
            {"node_id": "t_file_刑法", "node_name": "中华人民共和国刑法", "node_type": "法规文件",
             "properties": {"label": "法规文件", "文件全称": "中华人民共和国刑法", "文件别名": "刑法"}},
            {"node_id": "t_file_刑诉法", "node_name": "中华人民共和国刑事诉讼法", "node_type": "法规文件",
             "properties": {"label": "法规文件", "文件全称": "中华人民共和国刑事诉讼法", "文件别名": "刑事诉讼法"}},
            {"node_id": "t_clause_网安第五条", "node_name": "中华人民共和国网络安全法第五条", "node_type": "法条",
             "properties": {"label": "法条", "条": "第五条"}},
            {"node_id": "t_clause_刑法第五条", "node_name": "中华人民共和国刑法第五条", "node_type": "法条",
             "properties": {"label": "法条", "条": "第五条"}},
            {"node_id": "t_clause_刑诉第十条", "node_name": "中华人民共和国刑事诉讼法第十条", "node_type": "法条",
             "properties": {"label": "法条", "条": "第十条"}},
        ],
        "edges": [
            {"source_id": "t_file_网络安全法", "relation_type": "包含", "target_id": "t_clause_网安第五条", "properties": {"label": "包含"}},
            {"source_id": "t_file_刑法",       "relation_type": "包含", "target_id": "t_clause_刑法第五条", "properties": {"label": "包含"}},
            {"source_id": "t_file_刑诉法",     "relation_type": "包含", "target_id": "t_clause_刑诉第十条", "properties": {"label": "包含"}},
        ],
    }


# ============================================================
# 测试
# ============================================================

def test_neo4j_graph_association():
    """Neo4j 图谱关联测试"""
    TAG_A = "test_assoc_tag_a"
    TAG_B = "test_assoc_tag_b"
    GLEVEL = "DomainLevel"

    # 加载配置并创建适配器
    cfg = _load_neo4j_config()
    adapter = Neo4jAdapter(uri=cfg["uri"], username=cfg["username"],
                           password=cfg["password"], database=cfg["database"])
    adapter.connect()
    
    try:
        # 0. 清理
        print("[0/4] 清理旧数据...")
        for tag in [TAG_A, TAG_B]:
            try:
                adapter.delete_subgraph(tag)
            except Exception:
                pass

        # 1. 导入
        print("[1/4] 导入测试数据到 Neo4j...")
        data_a = build_tag_test_a()
        data_b = build_tag_test_b()
        adapter.add_subgraph_with_merge(data_a, TAG_A, graph_level=GLEVEL)
        adapter.add_subgraph_with_merge(data_b, TAG_B, graph_level=GLEVEL)
        print(f"  {TAG_A}: {len(data_a['nodes'])} 节点, {len(data_a['edges'])} 边")
        print(f"  {TAG_B}: {len(data_b['nodes'])} 节点, {len(data_b['edges'])} 边")

        # 2. 执行关联（直接操作 Neo4j，不拉全量）
        print("[2/4] 执行图谱关联...")
        stats = associate_graphs(adapter, TAG_A, TAG_B)
        print(f"  统计: {stats}")

        # 3. 验证（直接用 Cypher 查询 Neo4j）
        print("[3/4] 验证结果...")

        # 3a: 法规依据应被删除
        basis = adapter.get_nodes_by_type(TAG_A, "法规依据")
        assert len(basis) == 0, f"法规依据应全部删除，实际剩余 {len(basis)}"
        print("  ✓ 法规依据已删除")

        # 3b: 引用依据应只剩 1 个（无法匹配的）
        cites = adapter.get_nodes_by_type(TAG_A, "引用依据")
        assert len(cites) == 1, f"应剩1个无法匹配的引用依据，实际 {len(cites)}"
        assert cites[0].name == "引用依据-不存在的文件"
        print("  ✓ 3个引用依据已匹配删除, 1个无法匹配的保留")

        # 3c: 依据关系: file_刑法 → t_file_网络安全法 (跨标签!)
        yiju_rels = adapter.run_query(
            f"MATCH (a:`{TAG_A}`)-[r:依据]->(b:`{TAG_B}`) RETURN a.id AS src, b.id AS tgt"
        )
        yiju_targets = {r["tgt"] for r in yiju_rels}
        assert "t_file_网络安全法" in yiju_targets, f"依据应指向 t_file_网络安全法, 实际: {yiju_targets}"
        assert "t_file_数据安全法" in yiju_targets, f"依据应指向 t_file_数据安全法, 实际: {yiju_targets}"
        print("  ✓ 法规依据→法规文件 替换成功")

        # 3d: 引用(文件) → t_file_网络安全法
        cite_file = adapter.run_query(
            f"MATCH (a:`{TAG_A}`)-[r:引用]->(b:`{TAG_B}` {{id: 't_file_网络安全法'}}) RETURN count(r) AS cnt"
        )
        assert cite_file[0]["cnt"] == 1, "引用(文件)应指向 t_file_网络安全法"
        print("  ✓ 引用依据(文件)→法规文件 替换成功")

        # 3e: 引用(条款, 刑法第五条) → t_clause_刑法第五条
        cite_c1 = adapter.run_query(
            f"MATCH (a:`{TAG_A}`)-[r:引用]->(b:`{TAG_B}` {{id: 't_clause_刑法第五条'}}) RETURN count(r) AS cnt"
        )
        assert cite_c1[0]["cnt"] == 1, "引用(条款)应指向 t_clause_刑法第五条"
        print("  ✓ 引用依据(条款,刑法第五条)→法条 替换成功")

        # 3f: 引用(条款, 刑诉第十条) → t_clause_刑诉第十条
        cite_c2 = adapter.run_query(
            f"MATCH (a:`{TAG_A}`)-[r:引用]->(b:`{TAG_B}` {{id: 't_clause_刑诉第十条'}}) RETURN count(r) AS cnt"
        )
        assert cite_c2[0]["cnt"] == 1, "引用(条款)应指向 t_clause_刑诉第十条"
        print("  ✓ 引用依据(条款,刑诉第十条)→法条 替换成功")

        # 3g: tag_b 无中间节点，保持不变
        nodes_b = adapter.run_query(f"MATCH (n:`{TAG_B}`) RETURN count(n) AS cnt")
        assert nodes_b[0]["cnt"] == len(data_b["nodes"]), "tag_b 节点数应不变"
        print("  ✓ tag_b 无中间节点，保持不变")

        print("\n" + "=" * 50)
        print("全部测试通过！")
        print("=" * 50)
    finally:
        adapter.disconnect()


def _load_neo4j_config():
    import os
    try:
        from dotenv import load_dotenv
        env_path = os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", ".env")
        load_dotenv(env_path)
    except ImportError:
        pass
    return {
        "uri": os.getenv("NEO4J_URI", "bolt://127.0.0.1:7687"),
        "username": os.getenv("NEO4J_USERNAME", "neo4j"),
        "password": os.getenv("NEO4J_PASSWORD", "testForV2"),
        "database": os.getenv("NEO4J_DATABASE", "neo4j"),
    }


if __name__ == "__main__":
    print("=" * 50)
    print("图谱关联模块 — Neo4j 集成测试")
    print("=" * 50)
    cfg = _load_neo4j_config()
    print(f"连接 Neo4j: {cfg['uri']} / {cfg['database']}")
    adapter = Neo4jAdapter(uri=cfg["uri"], username=cfg["username"],
                           password=cfg["password"], database=cfg["database"])
    adapter.connect()
    try:
        test_neo4j_graph_association()
    finally:
        adapter.disconnect()
