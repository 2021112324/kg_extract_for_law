"""Neo4j 全量数据迁移工具（节点 + 关系，批量导入）。

将源数据库的全部（或指定标签的）节点与关系迁移到目标数据库，
保留原始标签、属性和关系类型。适用于 Neo4j 4.x/5.x（Cypher 5）。

用法：
    python migrator_neo4j.py
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from neo4j import GraphDatabase


class Neo4jMigrator:
    """源库 → 目标库 的节点/关系迁移器。"""

    def __init__(
        self,
        source_uri: str,
        source_user: str,
        source_password: str,
        target_uri: str,
        target_user: str,
        target_password: str,
        source_database: str = "neo4j",
        target_database: str = "neo4j",
        batch_size: int = 1000,
    ) -> None:
        self.source_driver = GraphDatabase.driver(
            source_uri, auth=(source_user, source_password)
        )
        self.target_driver = GraphDatabase.driver(
            target_uri, auth=(target_user, target_password)
        )
        self.source_database = source_database
        self.target_database = target_database
        self.batch_size = batch_size

    def close_connections(self) -> None:
        self.source_driver.close()
        self.target_driver.close()

    # ------------------------------------------------------------------
    # 对外入口
    # ------------------------------------------------------------------
    def migrate(self, labels: list[str] | None = None) -> None:
        """迁移全部节点与关系（labels 为 None 时迁移全部，否则只迁移指定标签）。

        注意：指定标签时，只迁移“两端节点都带有这些标签”的关系，避免半截关系。
        """
        nodes = self._fetch_nodes(labels)
        print(f"读取源节点：{len(nodes)} 个")
        relationships = self._fetch_relationships(labels)
        print(f"读取源关系：{len(relationships)} 条")

        id_mapping = self._import_nodes(nodes)
        print(f"导入节点：{len(id_mapping)} 个")
        self._import_relationships(relationships, id_mapping)
        print("迁移完成")

    # ------------------------------------------------------------------
    # 读取源数据
    # ------------------------------------------------------------------
    def _fetch_nodes(self, labels: list[str] | None) -> list[dict[str, Any]]:
        if labels:
            where = " OR ".join(f"n:`{label}`" for label in labels)
            query = (
                f"MATCH (n) WHERE {where} "
                "RETURN elementId(n) AS eid, labels(n) AS labels, properties(n) AS props"
            )
        else:
            query = "MATCH (n) RETURN elementId(n) AS eid, labels(n) AS labels, properties(n) AS props"

        nodes: list[dict[str, Any]] = []
        with self.source_driver.session(database=self.source_database) as session:
            for record in session.run(query):
                nodes.append(
                    {
                        "element_id": record["eid"],
                        "labels": list(record["labels"]),
                        "properties": dict(record["props"]),
                    }
                )
        return nodes

    def _fetch_relationships(self, labels: list[str] | None) -> list[dict[str, Any]]:
        if labels:
            a_where = " OR ".join(f"a:`{label}`" for label in labels)
            b_where = " OR ".join(f"b:`{label}`" for label in labels)
            query = (
                f"MATCH (a)-[r]->(b) "
                f"WHERE ({a_where}) AND ({b_where}) "
                "RETURN elementId(a) AS start_eid, elementId(b) AS end_eid, "
                "type(r) AS type, properties(r) AS props"
            )
        else:
            query = (
                "MATCH (a)-[r]->(b) "
                "RETURN elementId(a) AS start_eid, elementId(b) AS end_eid, "
                "type(r) AS type, properties(r) AS props"
            )

        relationships: list[dict[str, Any]] = []
        with self.source_driver.session(database=self.source_database) as session:
            for record in session.run(query):
                relationships.append(
                    {
                        "start_eid": record["start_eid"],
                        "end_eid": record["end_eid"],
                        "type": record["type"],
                        "properties": dict(record["props"]),
                    }
                )
        return relationships

    # ------------------------------------------------------------------
    # 导入目标数据库
    # ------------------------------------------------------------------
    def _import_nodes(self, nodes: list[dict[str, Any]]) -> dict[str, str]:
        """批量导入节点，返回 {源 elementId: 目标 elementId} 映射。

        按“标签组合”分组，每组用固定的多标签 CREATE，避免依赖
        db.create.setNodeLabels（community 版未注册该过程）。
        """
        # 清理临时属性并按标签组合分组
        by_labels: dict[tuple[str, ...], list[dict[str, Any]]] = defaultdict(list)
        for node in nodes:
            node["properties"].pop("_original_id", None)
            label_key = tuple(sorted(node["labels"]))
            by_labels[label_key].append(node)

        id_mapping: dict[str, str] = {}
        with self.target_driver.session(database=self.target_database) as session:
            for label_tuple, group in by_labels.items():
                label_str = "".join(f":`{label}`" for label in label_tuple)
                for start in range(0, len(group), self.batch_size):
                    batch = group[start : start + self.batch_size]
                    result = session.run(
                        f"UNWIND $batch AS row "
                        f"CREATE (n{label_str}) SET n += row.properties "
                        f"RETURN row.element_id AS original_id, elementId(n) AS new_id",
                        batch=batch,
                    )
                    for record in result:
                        id_mapping[record["original_id"]] = record["new_id"]
        return id_mapping

    def _import_relationships(
        self,
        relationships: list[dict[str, Any]],
        id_mapping: dict[str, str],
    ) -> None:
        """按关系类型分组批量导入关系。"""
        by_type: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for rel in relationships:
            start = id_mapping.get(rel["start_eid"])
            end = id_mapping.get(rel["end_eid"])
            if start is None or end is None:
                continue
            by_type[rel["type"]].append(
                {"start_eid": start, "end_eid": end, "properties": rel["properties"]}
            )

        with self.target_driver.session(database=self.target_database) as session:
            for rel_type, rels in by_type.items():
                for start in range(0, len(rels), self.batch_size):
                    batch = rels[start : start + self.batch_size]
                    session.run(
                        f"UNWIND $batch AS row "
                        f"MATCH (a) WHERE elementId(a) = row.start_eid "
                        f"MATCH (b) WHERE elementId(b) = row.end_eid "
                        f"CREATE (a)-[r:`{rel_type}`]->(b) SET r += row.properties",
                        batch=batch,
                    )


# 使用示例
if __name__ == "__main__":
    migrator = Neo4jMigrator(
        source_uri="bolt://39.96.198.93:7687",
        source_user="neo4j",
        source_password="ug4GMLnArVaKz8z",
        target_uri="neo4j+s://neo4j.i.hitices.cn:7687",
        target_user="neo4j",
        target_password="da9d00ab3db0238a",
        source_database="neo4j",
        target_database="neo4j",
        batch_size=1000,
    )

    try:
        # 迁移全部节点和关系（不传 labels 参数 = 全量迁移）
        # migrator.migrate()

        # 如果只想迁移某几个标签（例如 5 个正式标签），改用：
        migrator.migrate(labels=["kg_366920576398295040"])
    except Exception as exc:
        print(f"数据迁移失败: {exc}")
    finally:
        migrator.close_connections()
