from neo4j import GraphDatabase

class Neo4jMigrator:
    def __init__(self, source_uri, source_user, source_password, target_uri, target_user, target_password):
        """
        初始化源数据库和目标数据库连接配置

        Args:
            source_uri: 源数据库连接地址
            source_user: 源数据库用户名
            source_password: 源数据库密码
            target_uri: 目标数据库连接地址
            target_user: 目标数据库用户名
            target_password: 目标数据库密码
        """
        self.source_driver = GraphDatabase.driver(source_uri, auth=(source_user, source_password))
        self.target_driver = GraphDatabase.driver(target_uri, auth=(target_user, target_password))

    def close_connections(self):
        """关闭数据库连接"""
        self.source_driver.close()
        self.target_driver.close()

    def migrate_nodes_and_relationships_by_label(self, source_tag, target_tag):
        """
        将源数据库中指定标签的节点和关系迁移到目标数据库

        Args:
            source_tag: 源数据库中的标签名称
            target_tag: 目标数据库中的标签名称
        """
        # 从源数据库获取数据
        source_data = self._fetch_nodes_from_source(source_tag)
        relationship_data = self._fetch_relationships_from_source(source_tag)

        # 将数据导入到目标数据库
        id_mapping = self._import_nodes_to_target(source_data, target_tag)
        self._import_relationships_to_target(relationship_data, target_tag, id_mapping)

    def _fetch_nodes_from_source(self, tag):
        """
        从源数据库获取指定标签的所有节点

        Args:
            tag: 要查询的标签

        Returns:
            list: 包含节点数据的列表
        """
        with self.source_driver.session() as session:
            result = session.run(
                f"MATCH (n:{tag}) "
                "RETURN n"
            )

            nodes = []
            for record in result:
                node = record["n"]
                nodes.append({
                    "id": node.id,
                    "labels": list(node.labels),
                    "properties": dict(node)
                })

            return nodes

    def _fetch_relationships_from_source(self, tag):
        """
        从源数据库获取指定标签节点间的关系

        Args:
            tag: 要查询的标签

        Returns:
            list: 包含关系数据的列表
        """
        with self.source_driver.session() as session:
            result = session.run(
                f"MATCH (a:{tag})-[r]->(b:{tag}) "
                "RETURN a, r, b"
            )

            relationships = []
            for record in result:
                start_node = record["a"]
                relationship = record["r"]
                end_node = record["b"]

                relationships.append({
                    "start_node_id": start_node.id,
                    "end_node_id": end_node.id,
                    "type": relationship.type,
                    "properties": dict(relationship)
                })

            return relationships

    def _import_nodes_to_target(self, nodes, target_tag):
        """
        将节点数据导入到目标数据库

        Args:
            nodes: 节点数据列表
            target_tag: 目标标签名称

        Returns:
            dict: 原始ID到新ID的映射字典
        """
        id_mapping = {}
        with self.target_driver.session() as session:
            for node in nodes:
                original_id = node["id"]
                properties = node["properties"]

                # 添加一个临时属性用于标识原始ID
                properties["_original_id"] = original_id

                # 构造属性参数字符串
                props_str = ", ".join([f"{k}: ${k}" for k in properties.keys()])

                query = f"CREATE (n:{target_tag} {{ {props_str} }}) RETURN n"
                result = session.run(query, **properties)
                new_node = result.single()["n"]
                id_mapping[original_id] = new_node.id

            # 清除临时属性
            session.run(f"MATCH (n:{target_tag}) REMOVE n._original_id")

        return id_mapping

    def _import_relationships_to_target(self, relationships, target_tag, id_mapping):
        """
        将关系数据导入到目标数据库

        Args:
            relationships: 关系数据列表
            target_tag: 目标标签名称
            id_mapping: 原始ID到新ID的映射字典
        """
        with self.target_driver.session() as session:
            for rel in relationships:
                start_original_id = rel["start_node_id"]
                end_original_id = rel["end_node_id"]
                rel_type = rel["type"]
                properties = rel["properties"]

                # 获取新的节点ID
                start_new_id = id_mapping.get(start_original_id)
                end_new_id = id_mapping.get(end_original_id)

                if start_new_id is not None and end_new_id is not None:
                    # 构造属性参数字符串
                    props_str = ", ".join([f"{k}: ${k}" for k in properties.keys()])
                    if props_str:
                        props_str = "{" + props_str + "}"

                    query = (
                        f"MATCH (a:{target_tag}), (b:{target_tag}) "
                        f"WHERE id(a) = $start_id AND id(b) = $end_id "
                        f"CREATE (a)-[r:{rel_type} {props_str}]->(b) "
                        f"RETURN r"
                    )

                    params = {"start_id": start_new_id, "end_id": end_new_id}
                    params.update(properties)
                    session.run(query, **params)

# 使用示例
if __name__ == "__main__":
    # 配置数据库连接参数
    migrator = Neo4jMigrator(
        source_uri="bolt://localhost:7687",
        source_user="neo4j",
        source_password="password1",
        target_uri="bolt://localhost:7688",
        target_user="neo4j",
        target_password="password2"
    )

    try:
        # 执行数据迁移（包括节点和关系）
        migrator.migrate_nodes_and_relationships_by_label("tag1", "tag2")
        print("数据迁移完成")
    except Exception as e:
        print(f"数据迁移失败: {e}")
    finally:
        # 关闭数据库连接
        migrator.close_connections()
