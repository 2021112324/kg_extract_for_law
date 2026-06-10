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
    )
    neo4j_adapter.connect()

    # 查询所有以 "e1_compliance_case_v1_task_" 为前缀的图谱标签
    query = "CALL db.labels() YIELD label WHERE label STARTS WITH 'e1_compliance_case_v1_task_' RETURN label"
    records = neo4j_adapter.run_query(query)

    tags = [record["label"] for record in records]
    print(tags)

    neo4j_adapter.disconnect()
