from app.infrastructure.graph_storage.neo4j_adapter import Neo4jAdapter

if __name__ == "__main__":
    neo4j_adapter = Neo4jAdapter()
    # 创建知识图谱
    neo4j_adapter.connect()
    print("开始导入数据...")
    neo4j_adapter.merge_graphs("标识_再审_其二_task_536879725323223040", "标识_再审_其二_kg_536879714426421248")
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
