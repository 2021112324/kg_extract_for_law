import sys
from pathlib import Path

# 将项目根目录加入 sys.path，解决 ModuleNotFoundError
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.infrastructure.graph_storage.neo4j_adapter import Neo4jAdapter

if __name__ == "__main__":
    neo4j_adapter = Neo4jAdapter(
        uri="bolt://39.96.198.93:7687",
        username="neo4j",
        password="ug4GMLnArVaKz8z",
        database="neo4j"
        # uri = "blot://39.96.198.93:7687",
        # username = "neo4j",
        # password = "ug4GMLnArVaKz8z",
        # database = "neo4j"
    )
    # 创建知识图谱
    neo4j_adapter.connect()


    # 定义多个源图谱标签和一个目标图谱标签
    source_graph_tags = ['e2_行政监管规则1_kg_588399937453031424']
    target_graph_tag = "e1_行政监管规则_kg_586736132520148992"

    print(f"开始导入数据，共 {len(source_graph_tags)} 个源图谱...")
    for source_tag in source_graph_tags:
        print(f"  正在合并: {source_tag} -> {target_graph_tag}")
        neo4j_adapter.merge_graphs(source_tag, target_graph_tag)
        neo4j_adapter.delete_subgraph(source_tag)
    print("数据导入完成！")
    neo4j_adapter.disconnect()
    # result = neo4j_adapter.get_visualization_data("law_top_graph")
    # # 格式化输出result
    # print("=== 图谱可视化数据 ===")
    # print(f"节点数量: {len(result.nodes)}")
    # # print(f"关系数量: {len(result.relationships)}")
    # if result.error:
    #     print(f"错误信息: {result.error}")
    #
    # # print("\n=== 节点信息 ===")
    # for i, node in enumerate(result.nodes, 1):
    #     # print(f"节点 {i}:")
    #     # print(f"  ID: {node.id}")
    #     print(f"'{node.name}',")
    #     # print(f"  标签: {node.label}")
    #     # print(f"  属性: {node.properties}")
    #     # print()
    #
    # # print("=== 关系信息 ===")
    # # for i, rel in enumerate(result.relationships, 1):
    # #     print(f"关系 {i}:")
    # #     print(f"  ID: {rel.id}")
    # #     print(f"  源节点ID: {rel.source_id}")
    # #     print(f"  目标节点ID: {rel.target_id}")
    # #     print(f"  类型: {rel.type}")
    # #     print(f"  属性: {rel.properties}")
    # #     print()

    # result = neo4j_adapter.get_nodes_by_type("law_top_graph", "规章文件")
    # # 格式化输出result
    # print("=== 节点信息 ===")
    # for node in result:
    #     # print(f"节点:")
    #     # print(f"  ID: {node.id}")
    #     print(f"  名称: {node.name}")
    #     print(f"  标签: {node.label}")
    #     # print(f"  属性: {node.properties}")
# if __name__ == "__main__":
#     neo4j_adapter = Neo4jAdapter()
#     node_map = {
#         "label": "法规条款依据",
#         "法规名称": "《中华人民共和国刑法》"
#     }
#     neo4j_adapter.connect()
#     nodes = neo4j_adapter.get_nodes_by_properties(graph_tag="合规案例库v7_kg_536069077999812608", properties=node_map)
#     neo4j_adapter.disconnect()
#     for node in nodes:
#         print(node)
