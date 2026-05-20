

from app.infrastructure.graph_storage.neo4j_adapter import Neo4jAdapter

if __name__ == "__main__":
    neo4j_adapter = Neo4jAdapter(
        uri="bolt://39.96.198.93:7687",
        username="neo4j",
        password="ug4GMLnArVaKz8z",
        database="neo4j"
    )
    # kg_name = "enterprise_regulations"
    kg_names = [
        "合规案例库v8_kg_536094435075686400",
        "c1_法律法规条款",
        "c1_行政监管规则",
        "a4_合规指引v1v2_kg_564731610319028224",
        "c1_一审行政判决书_kg_579287097605619712"
        # "a4_合规指引v1_kg_564731610319028224",
        # "a4_合规指引v2_kg_565135353883656192",
        # "合规案例库v8_kg_536094435075686400",
        # "国家规章库v5_054cb5b450f34d1a97bd7da805eb7964",
        # "法规v1_697ef070b1674c6ba1844c9f440a004f",
        # "国家法律法规数据库_5c0a56861f8e4efba71413c30e04377a",
        # "一审_v4_kg_538581906984271872",
        # "二审判决_kg_539068999565049856",
        # "a1_国家规章库V5_kg_544354721847050240",
        # "a1_国家法律数据库newV2_kg_544067195521466368",
        # "a1_法规V5_kg_544885123271622656",
        # "a3_一审民事判决书_v1_kg_552088210411356160",
        # "a2_二审民事判决书v1_kg_548341426803441664"
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
