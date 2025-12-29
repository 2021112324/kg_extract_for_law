import json

from app.infrastructure.graph_storage.neo4j_adapter import Neo4jAdapter

if __name__ == "__main__":
    neo4j_adapter = Neo4jAdapter()
    # 创建知识图谱
    file_path = r"F:\企业大脑知识库系统\8.1项目\数据处理\清洗的数据\top_graph2.json"
    with open(file_path, 'r', encoding='utf-8') as file:
        graph_dict = json.load(file)
    neo4j_adapter.connect()
    neo4j_adapter.add_subgraph_with_merge(graph_dict, "law_top", "DomainLevel")
    neo4j_adapter.disconnect()
