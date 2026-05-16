from app.infrastructure.graph_storage.neo4j_adapter import Neo4jAdapter
from app.infrastructure.information_extraction.graph_association import associate_graphs

tag_a = "c1_法律法规条款"
tag_b = "c1_行政监管规则"

def _load_neo4j_config():
    import os
    try:
        from dotenv import load_dotenv
        env_path = os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "..", ".env")
        load_dotenv(env_path)
    except ImportError:
        pass
    return {
        "uri": os.getenv("NEO4J_URI", "bolt://127.0.0.1:7687"),
        "username": os.getenv("NEO4J_USERNAME", "neo4j"),
        "password": os.getenv("NEO4J_PASSWORD", "testForV2"),
        "database": os.getenv("NEO4J_DATABASE", "neo4j"),
    }


def neo4j_graph_association(TAG_A: str, TAG_B: str):
    """Neo4j 图谱关联测试"""
    # 加载配置并创建适配器
    cfg = _load_neo4j_config()
    adapter = Neo4jAdapter(uri=cfg["uri"], username=cfg["username"],
                           password=cfg["password"], database=cfg["database"])
    adapter.connect()

    try:
        # 执行关联（直接操作 Neo4j，不拉全量）
        print(" 执行图谱关联...")
        stats = associate_graphs(adapter, TAG_A, TAG_B)
        print(f"  统计: {stats}")
    except Exception as e:
        print(e)

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
        neo4j_graph_association(
            TAG_A=tag_a,
            TAG_B=tag_b
        )
    finally:
        adapter.disconnect()
