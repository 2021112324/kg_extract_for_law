

from app.infrastructure.graph_storage.neo4j_adapter import Neo4jAdapter

if __name__ == "__main__":
    neo4j_adapter = Neo4jAdapter()
    # kg_name = "enterprise_regulations"
    kg_names = [
        "商标_kg_533107621712887808",
        "商标案例库_kg_534103179588009984",
        "专利_kg_533208308668956672",
        "专利案例库_kg_533965669998264320"
    ]
    sum_stats = {
        "nodes_count": 0,
        "edges_count": 0,
        "properties_count": 0,
        "properties_count_without_inherent": 0
    }
    neo4j_adapter.connect()
    for kg_name in kg_names:
        # 创建知识图谱
        result = neo4j_adapter.get_graph_full_stats(kg_name)
        print(f"{kg_name}标签下的数据统计：")
        print(f"节点数：{result['nodes_count']}")
        print(f"关系数：{result['edges_count']}")
        print(f"属性数：{result['properties_count']}")
        print(f"排除固有属性后的属性数：{result['properties_count_without_inherent']}")
        sum_stats["nodes_count"] += result["nodes_count"]
        sum_stats["edges_count"] += result["edges_count"]
        sum_stats["properties_count"] += result["properties_count"]
        sum_stats["properties_count_without_inherent"] += result["properties_count_without_inherent"]
    print("--------------------------------------------------")
    print("所有知识图谱数据统计：")
    print(f"总节点数：{sum_stats['nodes_count']}")
    print(f"总关系数：{sum_stats['edges_count']}")
    print(f"总属性数：{sum_stats['properties_count']}")
    print(f"总排除固有属性后的属性数：{sum_stats['properties_count_without_inherent']}")
    neo4j_adapter.disconnect()
