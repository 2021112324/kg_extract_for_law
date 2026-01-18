"""
Neo4j图数据库适配器
"""
import logging
from typing import Dict, Any, Optional

from neo4j import GraphDatabase, Driver
from neo4j.exceptions import Neo4jError

from .base import (
    IGraphStorage,
    GraphNode,
    GraphEdge,
    GraphStats,
    GraphVisualizationData
)

logger = logging.getLogger(__name__)


class Neo4jAdapter(IGraphStorage):
    """Neo4j图数据库适配器"""

    # 定义常量
    DRIVER_NOT_INITIALIZED_ERROR = "Neo4j driver未初始化"

    def __init__(
            self,
            uri: str = None,
            username: str = None,
            password: str = None,
            database: str = "neo4j"
    ):
        """
        初始化Neo4j适配器

        Args:
            uri: Neo4j数据库URI
            username: 用户名
            password: 密码
            database: 数据库名称
        """
        self.uri = uri if uri else "bolt://60.205.171.106:7687"
        self.username = username if username else "neo4j"
        self.password = password if password else "hit-wE8sR9wQ3pG1"
        self.database = database if database else "neo4j"
        self.driver: Optional[Driver] = None

    def _sanitize_property_name(self, prop_name: str) -> str:
        """
        对属性名进行清洗，将特殊字符替换为下划线，确保在Cypher中有效

        Args:
            prop_name: 原始属性名

        Returns:
            str: 清洗后的属性名
        """
        if not prop_name:
            return prop_name

        import re
        # 更严格的清洗：只保留字母、数字、下划线和中文
        # 替换所有非字母数字下划线和中文字符为下划线
        sanitized = re.sub(r'[^a-zA-Z0-9_\u4e00-\u9fff]', '_', prop_name)

        # 移除连续的下划线
        sanitized = re.sub(r'_+', '_', sanitized)

        # 移除开头和结尾的下划线
        sanitized = sanitized.strip('_')

        # 如果清洗后为空，使用默认名称
        if not sanitized:
            sanitized = 'property'

        # 确保不以数字开头（Cypher要求）
        if sanitized[0].isdigit():
            sanitized = 'prop_' + sanitized

        return sanitized

    def _sanitize_properties(self, properties: Dict[str, Any]) -> Dict[str, Any]:
        """
        对属性字典进行清洗，处理所有属性名中的特殊字符
        同时确保所有属性值都是Neo4j支持的类型（原始类型或数组）

        Args:
            properties: 原始属性字典

        Returns:
            Dict[str, Any]: 清洗后的属性字典
        """
        if not properties:
            return properties

        sanitized_properties = {}
        for prop_key, prop_value in properties.items():
            sanitized_key = self._sanitize_property_name(prop_key)
            # 处理属性值，确保为Neo4j支持的类型
            if prop_value is None:
                continue
            elif isinstance(prop_value, (str, int, float, bool)):
                sanitized_properties[sanitized_key] = prop_value
            elif isinstance(prop_value, (list, tuple)):
                # 检查列表中的元素是否都是原始类型
                if all(isinstance(item, (str, int, float, bool)) for item in prop_value):
                    sanitized_properties[sanitized_key] = list(prop_value)
                else:
                    # 如果列表包含复杂类型，则将整个列表转为字符串
                    sanitized_properties[sanitized_key] = str(list(prop_value))
            else:
                # 对于其他复杂类型（如dict），转换为字符串
                sanitized_properties[sanitized_key] = str(prop_value)
        return sanitized_properties

    def connect(
        self
    ) -> bool:
        """
        建立与Neo4j数据库的连接

        该方法使用配置的URI、用户名和密码创建数据库驱动程序连接。
        成功建立连接后，会调用is_connected()方法验证连接是否有效。

        Returns:
            bool: 连接成功返回True，失败返回False

        Raises:
            Exception: 当连接过程中出现任何异常时记录错误日志
        """
        try:
            self.driver = GraphDatabase.driver(
                self.uri,
                auth=(self.username, self.password)
            )
            return self.is_connected()
        except Exception as e:
            logger.error(f"Neo4j连接失败: {str(e)}")
            return False

    def disconnect(
        self
    ) -> None:
        """
        断开与Neo4j数据库的连接

        该方法安全地关闭数据库驱动程序连接并清理相关资源。
        如果当前没有活动连接（driver为None），则不执行任何操作。
        """
        if self.driver:
            self.driver.close()
            self.driver = None

    def is_connected(
            self
    ) -> bool:
        """
        检查与Neo4j数据库的连接状态

        通过执行简单的Cypher查询来验证数据库连接是否仍然有效。
        如果没有初始化驱动程序或查询失败，则返回False。

        Returns:
            bool: 连接正常返回True，连接异常或未初始化返回False

        Raises:
            Exception: 当连接测试过程中出现任何异常时记录错误日志
        """
        if not self.driver:
            return False

        try:
            with self.driver.session(database=self.database) as session:
                result = session.run("RETURN 1 as test")
                return result.single()["test"] == 1
        except Exception as e:
            logger.error(f"Neo4j连接测试失败: {str(e)}")
            return False

    def add_subgraph_with_merge(
            self,
            kg_data: dict,
            graph_tag: str,
            graph_level: str = "DomainLevel",
            filename: str = None,
            merge_strategy: int = 1
    ) -> bool:
        """
        # TODO：待测试
        向neo4j数据库添加子图，使用合并策略
        """

        if graph_level not in ['DocumentLevel', 'DomainLevel', 'GlobalLevel']:
            raise ValueError("Invalid graph_level")
        if graph_level == 'DocumentLevel' and filename is None:
            raise ValueError("filename is required when graph_level is DocumentLevel")

        try:
            with self.driver.session(database=self.database) as session:
                with session.begin_transaction() as tx:
                    # 处理节点
                    for node in kg_data.get('nodes', []):
                        # 处理其他属性 - 简化策略，直接设置
                        properties = node.get('properties', {}) or {}
                        sanitized_properties = self._sanitize_properties(properties)
                        # 准备参数
                        params = {
                            'id': node.get('node_id'),
                            'name': node.get('node_name'),
                            'label': node.get('node_type'),
                            'graph_tag': graph_tag,
                            'graph_level': graph_level,
                            **sanitized_properties
                        }
                        if merge_strategy == 1:
                            # 修复CASE表达式语法
                            query = (
                                f"MERGE (n:{graph_tag} {{id: $id}}) "
                                "ON CREATE SET n.name = $name, n.label = $label, n.graph_tag = $graph_tag, "
                                "n.graph_level = $graph_level "
                            )
                            # 应该分别处理不同的filename来源
                            node_filename = node.get('filename')
                            if node_filename is not None:
                                # 处理节点中的filename（如果是列表则直接使用，否则转换为字符串）
                                if isinstance(node_filename, list):
                                    # 如果是列表，直接使用列表值
                                    node_filename_processed = node_filename
                                else:
                                    # 如果不是列表，转换为字符串
                                    node_filename_str = str(node_filename)
                                    if node_filename_str and node_filename_str != "None":
                                        node_filename_processed = [node_filename_str]
                                    else:
                                        node_filename_processed = None
                                if node_filename_processed:
                                    query += (
                                        ", n.filename = CASE "
                                        "WHEN n.filename IS NULL THEN $node_filename_param "
                                        "WHEN $node_filename_param IS NOT NULL THEN n.filename + $node_filename_param "
                                        "ELSE n.filename END "
                                    )
                                    params['node_filename_param'] = node_filename_processed

                            # 处理整体filename（转换为字符串）
                            if filename:
                                filename_str = str(filename) if filename else ""
                                if filename_str and filename_str != "None":
                                    query += (
                                        ", n.filename = CASE "
                                        "WHEN n.filename IS NULL THEN [$param_filename] "
                                        "WHEN NOT $param_filename IN n.filename THEN n.filename + [$param_filename] "
                                        "ELSE n.filename END "
                                    )
                                    params['param_filename'] = filename_str

                            # 处理其他属性 - 简化策略，直接设置
                            for prop_key, prop_value in sanitized_properties.items():
                                query += f", n.`{prop_key}` = ${prop_key} "

                        else:
                            # 默认处理方式
                            query = (
                                f"MERGE (n:{graph_tag} {{id: $id}}) "
                                "SET n.name = $name, n.label = $label, n.graph_tag = $graph_tag, "
                                "n.graph_level = $graph_level "
                            )

                            # 处理节点自身的filename（在默认策略下）
                            node_filename = node.get('filename')
                            if node_filename is not None:
                                node_filename_str = str(node_filename)
                                if node_filename_str and node_filename_str != "None":
                                    query += (
                                        ", n.filename = CASE "
                                        "WHEN n.filename IS NULL THEN [$node_filename_param] "
                                        "WHEN NOT $node_filename_param IN n.filename THEN n.filename + [$node_filename_param] "
                                        "ELSE n.filename END "
                                    )
                                    params['node_filename_param'] = node_filename_str

                            # 处理整体filename（转换为字符串）
                            if filename:
                                filename_str = str(filename) if filename else ""
                                if filename_str and filename_str != "None":
                                    query += (
                                        ", n.filename = CASE "
                                        "WHEN n.filename IS NULL THEN [$filename] "
                                        "WHEN NOT $filename IN n.filename THEN n.filename + [$filename] "
                                        "ELSE n.filename END "
                                    )
                                    params['filename'] = filename_str

                            for prop_key, prop_value in sanitized_properties.items():
                                query += f", n.`{prop_key}` = ${prop_key} "

                        tx.run(query, params)

                    # 处理边
                    for edge in kg_data.get('edges', []):
                        subject_id = edge.get('source_id')
                        predicate = edge.get('relation_type')
                        object_id = edge.get('target_id')

                        relation_label = edge.get('properties', {}).get('label', '') if edge.get('properties') else ''

                        # 处理关系名
                        safe_predicate = ''.join(c if c.isalnum() else '_' for c in predicate)

                        # 处理边的属性
                        edge_properties = edge.get('properties', {}) or {}
                        sanitized_edge_properties = self._sanitize_properties(edge_properties)

                        # 准备边的参数
                        params = {
                            'subject_id': subject_id,
                            'object_id': object_id,
                            'graph_tag': graph_tag,
                            'relation_label': relation_label,
                            'graph_level': graph_level,
                            **sanitized_edge_properties
                        }

                        if merge_strategy == 1:
                            query = (
                                f"MATCH (a:{graph_tag} {{id: $subject_id}}), (b:{graph_tag} {{id: $object_id}}) "
                                f"MERGE (a)-[r:{safe_predicate}]->(b) "
                                "ON CREATE SET r.graph_tag = $graph_tag, r.label = $relation_label, "
                                "r.graph_level = $graph_level "
                            )

                            # 处理边的filename（直接转换为字符串）
                            edge_filename = edge.get('filename')
                            if edge_filename is not None:
                                edge_filename_str = str(edge_filename)  # 直接使用str()转换
                                if edge_filename_str and edge_filename_str != "None":
                                    query += (
                                        ", r.filename = CASE "
                                        "WHEN r.filename IS NULL THEN [$edge_filename_param] "
                                        "WHEN NOT $edge_filename_param IN r.filename THEN r.filename + [$edge_filename_param] "
                                        "ELSE r.filename END "
                                    )
                                    params['edge_filename_param'] = edge_filename_str

                            # 设置边的其他属性
                            for prop_key, prop_value in sanitized_edge_properties.items():
                                query += f", r.`{prop_key}` = ${prop_key} "

                        else:
                            query = (
                                f"MATCH (a:{graph_tag} {{id: $subject_id}}), (b:{graph_tag} {{id: $object_id}}) "
                                f"MERGE (a)-[r:{safe_predicate}]->(b) "
                                "SET r.graph_tag = $graph_tag, r.label = $relation_label, "
                                "r.graph_level = $graph_level "
                            )

                            # 处理边的filename（直接转换为字符串）
                            edge_filename = edge.get('filename')
                            if edge_filename is not None:
                                edge_filename_str = str(edge_filename)  # 直接使用str()转换
                                if edge_filename_str and edge_filename_str != "None":
                                    query += (
                                        ", r.filename = CASE "
                                        "WHEN r.filename IS NULL THEN [$edge_filename_param] "
                                        "WHEN NOT $edge_filename_param IN r.filename THEN r.filename + [$edge_filename_param] "
                                        "ELSE r.filename END "
                                    )
                                    params['edge_filename_param'] = edge_filename_str

                            # 设置边的其他属性
                            for prop_key, prop_value in sanitized_edge_properties.items():
                                query += f", r.`{prop_key}` = ${prop_key} "

                        # 处理整体filename（统一转换为字符串）
                        if filename:
                            filename_str = str(filename) if filename else ""
                            if filename_str and filename_str != "None":
                                query += (
                                    ", r.filename = CASE "
                                    "WHEN r.filename IS NULL THEN [$filename] "
                                    "WHEN NOT $filename IN r.filename THEN r.filename + [$filename] "
                                    "ELSE r.filename END "
                                )
                                params['filename'] = filename_str

                        query += " RETURN r"

                        tx.run(query, **params)

                    tx.commit()
                    print(f"知识图谱数据已成功保存到数据库 {self.database}，使用标签 {graph_tag}")
                    return True

        except Exception as e:
            logger.error(f"保存知识图谱数据到数据库失败: {str(e)}")
            return False

    def delete_subgraph(
            self,
            graph_tag: str
    ) -> bool:
        """
        删除指定标签的子图数据

        该方法通过graph_tag参数匹配并删除Neo4j数据库中对应标签下的所有节点和关系。
        使用DETACH DELETE确保删除节点时同时删除与其关联的所有关系，避免约束冲突。

        Args:
            graph_tag (str): 图谱标签，用于标识需要删除的子图数据。
                           该标签在创建子图时被用作节点标签，用于隔离不同来源或类型的数据。

        Returns:
            bool: 删除操作结果
                - True: 成功删除指定标签下的所有数据
                - False: 删除失败，可能原因包括：
                    1. 数据库驱动未初始化
                    2. 数据库连接异常
                    3. Cypher查询执行错误

        Example:
            >>> adapter = Neo4jAdapter()
            >>> adapter.connect()
            True
            >>> adapter.delete_subgraph("project_A")
            True

        Note:
            - 该操作不可逆，删除的数据无法恢复
            - 仅删除指定标签下的数据，不影响其他标签的数据
            - 使用DETACH DELETE确保节点和关系都被删除
        """
        if not self.driver:
            logger.error(self.DRIVER_NOT_INITIALIZED_ERROR)
            return False
        try:
            with self.driver.session(database=self.database) as session:
                # 删除当前标签下的所有数据
                session.run(f"MATCH (n:{graph_tag}) "
                            f"DETACH DELETE n")
                print(f"标签 {graph_tag} 下的所有数据已删除")
                return True

        except Exception as e:
            print(f"删除数据时出错: {e}")
            return False

    def get_visualization_data(self, graph_tag: str, limit: Optional[int] = None) -> GraphVisualizationData:
        """
        获取可视化数据
        """
        if not self.driver:
            logger.error(self.DRIVER_NOT_INITIALIZED_ERROR)
            return GraphVisualizationData(
                nodes=[],
                relationships=[],
                error=self.DRIVER_NOT_INITIALIZED_ERROR
            )

        try:
            with self.driver.session(database=self.database) as session:
                # 根据limit参数构建Cypher查询语句
                if limit is not None and limit > 0:
                    cypher = """
                    MATCH (n)-[r]->(m)
                    WHERE $graph_tag IN labels(n) AND $graph_tag IN labels(m)
                    WITH n, r, m LIMIT $limit
                    RETURN 
                        collect(DISTINCT n) + collect(DISTINCT m) as allNodes,
                        collect(r) as allRels
                    """
                    result = session.run(cypher, graph_tag=graph_tag, limit=limit)
                else:
                    cypher = """
                   MATCH (n)-[r]->(m)
                    WHERE $graph_tag IN labels(n) AND $graph_tag IN labels(m)
                    RETURN 
                        collect(DISTINCT n) + collect(DISTINCT m) as allNodes,
                        collect(r) as allRels
                    """
                    result = session.run(cypher, graph_tag=graph_tag)

                record = result.single()

                if not record:
                    return GraphVisualizationData(
                        nodes=[],
                        relationships=[]
                    )

                # 使用集合去除重复节点
                nodes_data = []
                seen_node_ids = set()
                for node in record["allNodes"]:
                    if node.get('id') not in seen_node_ids:
                        nodes_data.append(node)
                        seen_node_ids.add(node.get('id'))

                relationships_data = record["allRels"]

                # nodes_data = record["allNodes"]
                # relationships_data = record["allRels"]

                # 转换节点数据
                nodes = []
                for node in nodes_data:
                    # 获取除graph_tag外的第一个标签作为label
                    labels = [label for label in node.labels if label != graph_tag]
                    label = labels[0] if labels else ""

                    # 获取节点属性并移除内部使用的字段
                    properties = dict(node)
                    properties.pop('id', None)
                    properties.pop('name', None)
                    properties.pop('graph_tag', None)

                    nodes.append(GraphNode(
                        id=node.get('id', ''),
                        name=node.get('name', ''),
                        label=label,
                        properties=properties
                    ))

                # 转换关系数据
                relationships = []
                for rel in relationships_data:
                    # 获取关系属性并移除内部使用的字段
                    properties = dict(rel)
                    properties.pop('graph_tag', None)

                    relationships.append(GraphEdge(
                        id=str(rel.id),
                        source_id=rel.start_node.get('id', ''),
                        target_id=rel.end_node.get('id', ''),
                        type=rel.type,
                        properties=properties
                    ))

                # 防止出现无节点但有关系的情况
                if len(nodes) == 0:
                    relationships = []

                return GraphVisualizationData(
                    nodes=nodes,
                    relationships=relationships
                )

        except Neo4jError as e:
            logger.error(f"获取图谱可视化数据失败: {str(e)}")
            return GraphVisualizationData(
                nodes=[],
                relationships=[],
                error=str(e)
            )

    def get_subgraph_stats(self, graph_tag: str) -> GraphStats:
        """获取子图统计信息"""
        if not self.driver:
            logger.error(self.DRIVER_NOT_INITIALIZED_ERROR)
            return GraphStats(
                node_count=0,
                edge_count=0,
                error=self.DRIVER_NOT_INITIALIZED_ERROR
            )

        try:
            with self.driver.session(database=self.database) as session:
                # 获取节点数量
                result = session.run("""
                    MATCH (n)
                    WHERE $graph_tag IN labels(n)
                    RETURN count(n) as nodeCount
                """, graph_tag=graph_tag)
                node_count = result.single()["nodeCount"]

                # 获取关系数量
                result = session.run("""
                    MATCH (n)-[r]->(m)
                    WHERE $graph_tag IN labels(n) AND $graph_tag IN labels(m)
                    RETURN count(r) as edgeCount
                """, graph_tag=graph_tag)
                edge_count = result.single()["edgeCount"]

                return GraphStats(
                    node_count=node_count,
                    edge_count=edge_count
                )
        except Neo4jError as e:
            logger.error(f"获取Neo4j子图统计信息失败: {str(e)}")
            return GraphStats(
                node_count=0,
                edge_count=0,
                error=str(e)
            )

    def get_graph_full_stats(self, graph_tag: str) -> dict:
        """获取子图统计信息"""
        if not self.driver:
            logger.error(self.DRIVER_NOT_INITIALIZED_ERROR)
            return {
                "nodes_count": 0,
                "edges_count": 0,
                "properties_count": 0,
                "properties_count_without_inherent": 0
            }

        try:
            with self.driver.session(database=self.database) as session:
                # 获取节点数量
                result = session.run("""
                    MATCH (n)
                    WHERE $graph_tag IN labels(n)
                    RETURN count(n) as nodeCount
                """, graph_tag=graph_tag)
                node_count = result.single()["nodeCount"]

                # 获取关系数量
                result = session.run("""
                    MATCH (n)-[r]->(m)
                    WHERE $graph_tag IN labels(n) AND $graph_tag IN labels(m)
                    RETURN count(r) as edgeCount
                """, graph_tag=graph_tag)
                edge_count = result.single()["edgeCount"]

                # 获取所有属性数量（包括固有属性）
                result = session.run("""
                    MATCH (n)
                    WHERE $graph_tag IN labels(n)
                    WITH collect(keys(n)) as all_keys
                    UNWIND all_keys as keys_list
                    RETURN sum(size(keys_list)) as totalProperties
                """, graph_tag=graph_tag)
                record = result.single()
                properties_count = record["totalProperties"] if record and record["totalProperties"] else 0

                # 获取关系属性数量
                result = session.run("""
                    MATCH ()-[r]->()
                    WHERE $graph_tag IN labels(startNode(r)) AND $graph_tag IN labels(endNode(r))
                    WITH collect(keys(r)) as all_rel_keys
                    UNWIND all_rel_keys as keys_list
                    RETURN sum(size(keys_list)) as totalRelProperties
                """, graph_tag=graph_tag)
                rel_record = result.single()
                rel_properties_count = rel_record["totalRelProperties"] if rel_record and rel_record["totalRelProperties"] else 0

                # 总属性数（节点属性+关系属性）
                total_properties = properties_count + rel_properties_count

                total_non_inherent_props = total_properties - node_count * 6

                return {
                    "nodes_count": node_count,
                    "edges_count": edge_count,
                    "properties_count": total_properties,
                    "properties_count_without_inherent": total_non_inherent_props
                }
        except Neo4jError as e:
            logger.error(f"获取Neo4j子图统计信息失败: {str(e)}")
            return {
                "nodes_count": 0,
                "edges_count": 0,
                "properties_count": 0,
                "properties_count_without_inherent": 0
            }


    def create_vector_index(self, name: str, dimension: int = 1536) -> bool:
        pass
        # """创建向量索引"""
        # if not self.driver:
        #     logger.error(self.DRIVER_NOT_INITIALIZED_ERROR)
        #     return False
        #
        # try:
        #     with self.driver.session(database=self.database) as session:
        #         session.run("""
        #         CREATE VECTOR INDEX IF NOT EXISTS
        #         FOR (n:Document) ON (n.embedding)
        #         OPTIONS {indexConfig: {
        #           `vector.dimensions`: $dimension,
        #           `vector.similarity_function`: 'cosine'
        #         }}
        #         """, dimension=dimension)
        #         return True
        # except Neo4jError as e:
        #     logger.error(f"创建Neo4j向量索引失败: {str(e)}")
        #     return False

    def merge_graphs(self, source_graph_tag: str, target_graph_tag: str):
        """
        将source_graph_tag图谱合并到target_graph_tag图谱中

        Args:
            source_graph_tag (str): 源图谱标识符(A)
            target_graph_tag (str): 目标图谱标识符(B)
        """
        if not self.driver:
            logger.error(self.DRIVER_NOT_INITIALIZED_ERROR)
            return GraphStats(
                node_count=0,
                edge_count=0,
                error=self.DRIVER_NOT_INITIALIZED_ERROR
            )

        try:
            # 获取源图谱数据
            source_data = self.get_visualization_data(source_graph_tag)

            with self.driver.session(database=self.database) as session:
                with session.begin_transaction() as tx:
                    # 合并节点
                    for node in source_data.nodes:
                        # 使用MERGE确保不会重复创建相同ID的节点
                        query = (
                            f"MERGE (n:{target_graph_tag} {{id: $id}}) "
                            "SET n.name = $name, n.label = $label, n.graph_tag = $graph_tag"
                        )

                        # 处理其他属性
                        properties = node.properties or {}
                        sanitized_properties = self._sanitize_properties(properties)
                        for prop_key, prop_value in sanitized_properties.items():
                            query += f", n.`{prop_key}` = ${prop_key}"

                        # 准备参数
                        params = {
                            'id': node.id,
                            'name': node.name,
                            'label': node.label,
                            'graph_tag': target_graph_tag,
                            **sanitized_properties
                        }

                        tx.run(query, params)

                    # 合并边
                    for edge in source_data.relationships:
                        # 获取关系的label属性（如果存在）
                        relation_label = edge.properties.get('label', '') if edge.properties else ''

                        # 处理关系名，确保符合Cypher命名规范
                        safe_predicate = ''.join(c if c.isalnum() else '_' for c in edge.type)

                        # 使用MERGE确保相同节点间的关系不会重复创建
                        # 采用分步MATCH方式避免笛卡尔积警告
                        query = (
                            f"MATCH (a:{target_graph_tag} {{id: $subject_id}}) "
                            f"MATCH (b:{target_graph_tag} {{id: $object_id}}) "
                            f"MERGE (a)-[r:{safe_predicate}]->(b) "
                        )

                        # 添加属性设置
                        query += "SET r.graph_tag = $graph_tag, r.label = $relation_label"

                        params = {
                            'subject_id': edge.source_id,
                            'object_id': edge.target_id,
                            'graph_tag': target_graph_tag,
                            'relation_label': relation_label
                        }

                        tx.run(query, **params)

                    tx.commit()

            return self.get_subgraph_stats(target_graph_tag)
        except Neo4jError as e:
            logger.error(e)
            return GraphStats(
                node_count=0,
                edge_count=0,
                error=str(e)
            )

    def merge_graphs_with_match_node(
            self,
            source_graph_tag: str,
            target_graph_tag: str,
            matched_node_id: str,
    ):
        """
        将source_graph_tag图谱合并到target_graph_tag图谱中，并建立源自于关系

        Args:
            source_graph_tag (str): 源图谱标识符(A)
            target_graph_tag (str): 目标图谱标识符(B)
            matched_node_id (str): 关联的节点id
        """
        if not self.driver:
            logger.error(self.DRIVER_NOT_INITIALIZED_ERROR)
            return GraphStats(
                node_count=0,
                edge_count=0,
                error=self.DRIVER_NOT_INITIALIZED_ERROR
            )

        try:
            # 获取源图谱数据
            source_data = self.get_visualization_data(source_graph_tag)

            with self.driver.session(database=self.database) as session:
                with session.begin_transaction() as tx:
                    # 首先检验是否存在target_graph_tag标签下的id为matched_node_id的节点A
                    check_node_query = f"""
                        MATCH (n:{target_graph_tag} {{id: $node_id}})
                        RETURN n
                    """
                    check_result = tx.run(check_node_query, node_id=matched_node_id)
                    target_node = check_result.single()
                    # 若没有则返回None
                    if not target_node:
                        return None

                    # 合并节点
                    created_node_ids = []  # 记录已创建的节点ID
                    for node in source_data.nodes:
                        # 使用MERGE确保不会重复创建相同ID的节点
                        query = (
                            f"MERGE (n:{target_graph_tag} {{id: $id}}) "
                            "SET n.name = $name, n.label = $label, n.graph_tag = $graph_tag"
                        )
                        # 处理其他属性
                        properties = node.properties or {}
                        sanitized_properties = self._sanitize_properties(properties)
                        for prop_key, prop_value in sanitized_properties.items():
                            query += f", n.`{prop_key}` = ${prop_key}"
                        # 准备参数
                        params = {
                            'id': node.id,
                            'name': node.name,
                            'label': node.label,
                            'graph_tag': target_graph_tag,
                            **sanitized_properties
                        }
                        tx.run(query, params)
                        created_node_ids.append(node.id)  # 记录创建的节点ID

                    # 合并边
                    for edge in source_data.relationships:
                        # 获取关系的label属性（如果存在）
                        relation_label = edge.properties.get('label', '') if edge.properties else ''
                        # 处理关系名，确保符合Cypher命名规范
                        safe_predicate = ''.join(c if c.isalnum() else '_' for c in edge.type)
                        # 使用MERGE确保相同节点间的关系不会重复创建
                        # 采用分步MATCH方式避免笛卡尔积警告
                        query = (
                            f"MATCH (a:{target_graph_tag} {{id: $subject_id}}) "
                            f"MATCH (b:{target_graph_tag} {{id: $object_id}}) "
                            f"MERGE (a)-[r:{safe_predicate}]->(b) "
                        )
                        # 添加属性设置
                        query += "SET r.graph_tag = $graph_tag, r.label = $relation_label"
                        params = {
                            'subject_id': edge.source_id,
                            'object_id': edge.target_id,
                            'graph_tag': target_graph_tag,
                            'relation_label': relation_label
                        }
                        tx.run(query, **params)

                    # 新增功能：将source_data中创建的每个节点都与target_node建立"源自于"关系
                    for node_id in created_node_ids:
                        create_relation_query = f"""
                            MATCH (source_node:{target_graph_tag} {{id: $source_id}})
                            MATCH (target_node:{target_graph_tag} {{id: $target_id}})
                            MERGE (source_node)-[r:源自于]->(target_node)
                            SET r.graph_tag = $graph_tag, r.label = '源自于'
                        """
                        tx.run(create_relation_query,
                               source_id=node_id,
                               target_id=matched_node_id,
                               graph_tag=target_graph_tag)

                    tx.commit()

            self.delete_subgraph(source_graph_tag)
            return self.get_subgraph_stats(target_graph_tag)
        except Neo4jError as e:
            logger.error(e)
            return GraphStats(
                node_count=0,
                edge_count=0,
                error=str(e)
            )

    def get_nodes_by_type(
            self,
            graph_tag: str,
            node_type: str,
    ):
        """
        获取neo4j中标签为graph_tag，且节点类型n.label为node_type的节点列表

        Args:
            graph_tag (str): 图谱标签
            node_type (str): 节点类型(label)

        Returns:
            list: 符合条件的节点列表
        """
        if not self.driver:
            logger.error(self.DRIVER_NOT_INITIALIZED_ERROR)
            return []

        try:
            with self.driver.session(database=self.database) as session:
                # 查询指定标签和节点类型的节点
                result = session.run("""
                    MATCH (n:%s)
                    WHERE n.label = $node_type
                    RETURN n
                """ % graph_tag, node_type=node_type)

                nodes = []
                for record in result:
                    node = record["n"]
                    # 转换节点数据
                    properties = dict(node)
                    nodes.append(GraphNode(
                        id=properties.get('id', ''),
                        name=properties.get('name', ''),
                        label=properties.get('label', ''),
                        properties=properties
                    ))

                return nodes

        except Exception as e:
            logger.error(f"获取节点列表失败: {str(e)}")
            return []

