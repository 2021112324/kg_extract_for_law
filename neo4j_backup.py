from neo4j import GraphDatabase
import csv
import os

# ========== 1. 连接配置 ==========
URI = "bolt://60.205.171.106:7687"   # 改成你的 Neo4j 地址
AUTH = ("neo4j", "hit-wE8sR9wQ3pG1")   # 改成你的用户名密码

# ========== 2. 导出配置 ==========
BATCH_SIZE = 10000
OUTPUT_DIR = r"F:\企业大脑知识库系统\8.1项目\抽取代码\kg_extract_for_law\neo4j_data"

NODES_FILE = os.path.join(OUTPUT_DIR, "nodes.csv")
RELS_FILE = os.path.join(OUTPUT_DIR, "rels.csv")

os.makedirs(OUTPUT_DIR, exist_ok=True)


# ========== 3. Neo4j Driver ==========
driver = GraphDatabase.driver(URI, auth=AUTH)


# ========== 4. 导出所有节点 ==========
def export_all_nodes():
    print("Start exporting nodes...")
    last_node_id = -1
    total = 0

    with driver.session() as session, \
         open(NODES_FILE, "w", newline="", encoding="utf-8") as f:

        writer = csv.writer(f)
        writer.writerow(["node_id", "labels", "properties"])

        while True:
            result = session.run(
                """
                MATCH (n)
                WHERE id(n) > $last_id
                RETURN
                    id(n) AS node_id,
                    labels(n) AS labels,
                    properties(n) AS properties
                ORDER BY node_id
                LIMIT $limit
                """,
                last_id=last_node_id,
                limit=BATCH_SIZE
            )

            rows = list(result)
            if not rows:
                break

            for r in rows:
                writer.writerow([
                    r["node_id"],
                    r["labels"],
                    r["properties"]
                ])

            last_node_id = rows[-1]["node_id"]
            total += len(rows)
            print(f"Nodes exported: {total}, last node id = {last_node_id}")

    print(f"Finished exporting nodes. Total: {total}")


# ========== 5. 导出所有关系（关键：按 id(r) 分页） ==========
def export_all_relationships():
    print("Start exporting relationships...")
    last_rel_id = -1
    total = 0

    with driver.session() as session, \
         open(RELS_FILE, "w", newline="", encoding="utf-8") as f:

        writer = csv.writer(f)
        writer.writerow([
            "rel_id",
            "start_node_id",
            "end_node_id",
            "type",
            "properties"
        ])

        while True:
            result = session.run(
                """
                MATCH (a)-[r]->(b)
                WHERE id(r) > $last_id
                RETURN
                    id(r) AS rel_id,
                    id(a) AS start_node_id,
                    id(b) AS end_node_id,
                    type(r) AS type,
                    properties(r) AS properties
                ORDER BY rel_id
                LIMIT $limit
                """,
                last_id=last_rel_id,
                limit=BATCH_SIZE
            )

            rows = list(result)
            if not rows:
                break

            for r in rows:
                writer.writerow([
                    r["rel_id"],
                    r["start_node_id"],
                    r["end_node_id"],
                    r["type"],
                    r["properties"]
                ])

            last_rel_id = rows[-1]["rel_id"]
            total += len(rows)
            print(f"Rels exported: {total}, last rel id = {last_rel_id}")

    print(f"Finished exporting relationships. Total: {total}")


# ========== 6. 主入口 ==========
def main():
    export_all_nodes()
    export_all_relationships()
    driver.close()
    print("Export completed successfully.")


if __name__ == "__main__":
    main()

