"""
Neo4j图数据库适配器
"""
import logging
import copy
import json
import time
from typing import Dict, Any, Optional

from neo4j import GraphDatabase, Driver
from neo4j.exceptions import Neo4jError
from neo4j import READ_ACCESS, unit_of_work

from .subgraph_records import (
    SubgraphError, digest, encode, graph_content, identifier, read_records,
    snapshot_hash, validate_snapshot,
)
from .diagnostics import exception_details, redact_diagnostics

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
            database: str = "neo4j",
            driver_options: Optional[dict] = None
    ):
        """
        初始化Neo4j适配器

        Args:
            uri: Neo4j数据库URI
            username: 用户名
            password: 密码
            database: 数据库名称
        """
        self.uri = uri if uri else "bolt://127.0.0.1:7687"
        self.username = username if username else "neo4j"
        self.password = password if password else "testForV2"
        self.database = database if database else "neo4j"
        self.driver: Optional[Driver] = None
        self.driver_options = dict(driver_options or {})

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
            # 注意：不能用 `not prop_value` 过滤，因为 `not False` 和 `not 0` 都是 True，
            # 会误丢弃合法的布尔和数值零值。
            if prop_value in (None, "", [], {}):
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
                auth=(self.username, self.password),
                **self.driver_options
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

    def verify_subgraph_support(self):
        if not self.driver:
            raise SubgraphError("driver_not_connected")
        with self.driver.session(database=self.database, default_access_mode=READ_ACCESS) as session:
            version = session.execute_read(lambda tx: tx.run(
                "// risk_map:version\nCALL dbms.components() YIELD name, versions "
                "WHERE name='Neo4j Kernel' RETURN versions[0] AS version"
            ).single()["version"])
        if int(version.split(".")[0]) < 5:
            raise SubgraphError("neo4j_5_or_newer_required")
        return version

    def _read_subgraph_tx(self, tx, tag, max_nodes, max_relationships, max_bytes):
        nodes, edges = read_records(tx, tag, max_nodes=max_nodes, max_relationships=max_relationships, max_bytes=max_bytes)
        boundary = tx.run(
            "// risk_map:boundary\nMATCH (a)-[r]->(b) "
            "WHERE ($tag IN labels(a)) <> ($tag IN labels(b)) RETURN count(r) AS count", tag=tag,
        ).single()["count"]
        result = {"database": self.database, "source_label": tag,
                  "nodes": nodes, "edges": edges, "boundary_count": boundary,
                  "scope_counts": self._scope_counts_tx(tx, tag)}
        validate_snapshot(result)
        size = len(json.dumps(encode(result), ensure_ascii=False).encode("utf-8"))
        if size > max_bytes:
            raise SubgraphError("snapshot_byte_budget_exceeded")
        result["fingerprint"] = snapshot_hash(result)
        result["encoded_bytes"] = size
        return result

    def read_subgraph_records(self, graph_tag, *, fetch_size=1000, max_nodes=50000,
                              max_relationships=200000, max_bytes=268435456, timeout=120):
        """Strict full export. Unlike run_query, failures are never empty results."""
        identifier(graph_tag)
        if not self.driver:
            raise SubgraphError("driver_not_connected")
        if any(type(v) is not int or v <= 0 for v in (fetch_size, max_nodes, max_relationships, max_bytes)):
            raise SubgraphError("invalid_read_budget")
        with self.driver.session(database=self.database, default_access_mode=READ_ACCESS,
                                 fetch_size=fetch_size) as session:
            return session.execute_read(unit_of_work(timeout=timeout)(lambda tx: self._read_subgraph_tx(
                tx, graph_tag, max_nodes, max_relationships, max_bytes)))

    def _scope_counts_tx(self, tx, target):
        nodes = tx.run("// risk_map:occupied_nodes\nMATCH (n) WHERE $tag IN labels(n) OR n.graph_tag=$tag "
                       "RETURN count(n) AS count", tag=target).single()["count"]
        edges = tx.run("// risk_map:occupied_edges\nMATCH (a)-[r]->(b) "
                       "WHERE r.graph_tag=$tag OR ($tag IN labels(a) AND $tag IN labels(b)) "
                       "RETURN count(r) AS count", tag=target).single()["count"]
        return {"nodes": nodes, "relationships": edges}

    def _assert_target_empty_tx(self, tx, target):
        if any(self._scope_counts_tx(tx, target).values()):
            raise SubgraphError("target_graph_already_exists")

    def check_subgraph_destination(self, target):
        identifier(target)
        if target == "_RiskMapPublication":
            raise SubgraphError("reserved_target_label")
        with self.driver.session(database=self.database, default_access_mode=READ_ACCESS) as session:
            session.execute_read(lambda tx: self._assert_target_empty_tx(tx, target))

    def _verify_copy_tx(self, tx, record, *, max_nodes, max_relationships, max_bytes):
        snapshot = self._read_subgraph_tx(tx, record["target_label"], max_nodes, max_relationships, max_bytes)
        def inverse(pairs, expected):
            if (not isinstance(pairs, list) or len(pairs) != expected
                    or any(not isinstance(p, (list, tuple)) or len(p) != 2
                           or any(not isinstance(v, str) or not v for v in p) for p in pairs)
                    or len({p[0] for p in pairs}) != len(pairs)
                    or len({p[1] for p in pairs}) != len(pairs)):
                raise SubgraphError("publication_identity_map_invalid")
            return {physical: logical for logical, physical in pairs}
        nodes = inverse(record["node_map"], record["node_count"])
        edges = inverse(record["edge_map"], record["relationship_count"])
        scope = self._scope_counts_tx(tx, record["target_label"])
        if (len(snapshot["nodes"]) != len(nodes) or len(snapshot["edges"]) != len(edges)
                or snapshot["boundary_count"] or scope != {"nodes": len(nodes), "relationships": len(edges)}):
            raise SubgraphError("publication_membership_changed")
        try:
            actual = graph_content(snapshot, nodes, edges)
        except KeyError as exc:
            raise SubgraphError("publication_physical_members_changed") from exc
        if digest(actual) != record["target_hash"]:
            raise SubgraphError("publication_content_changed")

    def inspect_subgraph_publication(self, target, *, manifest=None, timeout=120,
                                    max_nodes=50000, max_relationships=200000, max_bytes=268435456):
        identifier(target)
        with self.driver.session(database=self.database, default_access_mode=READ_ACCESS) as session:
            def inspect(tx):
                if manifest is None:
                    return {"status": "unverified", "inspection_status": "unverified",
                            "target_label": target, "diagnostic_counts": self._scope_counts_tx(tx, target)}
                if (manifest.get("commit_confirmed") is not True or manifest.get("target_label") != target
                        or manifest.get("database") != self.database
                        or manifest.get("status") not in {"committed_complete", "committed_partial"}):
                    raise SubgraphError("invalid_local_publication_manifest")
                self._verify_copy_tx(tx, manifest, max_nodes=max_nodes,
                                     max_relationships=max_relationships, max_bytes=max_bytes)
                return {k: manifest[k] for k in (
                    "status", "publish_id", "target_label", "source_label", "source_hash", "target_hash", "database",
                    "nodes_created", "relationships_created")} | {"inspection_status": "verified"}
            return session.execute_read(unit_of_work(timeout=timeout)(inspect))

    def create_subgraph_copy(self, records, target_label, *, source_label, source_hash, publish_id,
                             completeness="partial", batch_size=500, timeout=120,
                             max_nodes=50000, max_relationships=200000, max_bytes=268435456,
                             preserve_extra_labels=False):
        """One explicit transaction. No registry, schema operations or automatic write replay."""
        tag = identifier(target_label)
        if target_label in {source_label, "_RiskMapPublication"}:
            raise SubgraphError("source_target_label_conflict")
        if (not isinstance(publish_id, str) or not publish_id or completeness not in {"complete", "partial"}
                or type(batch_size) is not int or batch_size <= 0):
            raise SubgraphError("invalid_publication_options")
        plan = copy.deepcopy(records)
        validate_snapshot(plan)
        if len(plan["nodes"]) > max_nodes or len(plan["edges"]) > max_relationships:
            raise SubgraphError("atomic_budget_exceeded")
        if len(json.dumps(encode(plan), ensure_ascii=False).encode("utf-8")) > max_bytes:
            raise SubgraphError("atomic_byte_budget_exceeded")
        for row in plan["nodes"]:
            labels = row["n"]["labels"]
            scope_valid = (target_label in labels and source_label not in labels
                           if preserve_extra_labels else labels == [target_label])
            if not scope_valid or row["n"]["properties"].get("graph_tag") != target_label:
                raise SubgraphError("invalid_target_node_scope")
        if any(r["p"]["properties"].get("graph_tag") != target_label for r in plan["edges"]):
            raise SubgraphError("invalid_target_relationship_scope")
        expected = graph_content(plan)
        base = {"publish_id": publish_id, "target_label": target_label, "source_label": source_label,
                "source_hash": source_hash, "database": self.database}
        tx = None
        commit_started = False
        commit_acknowledged = False
        rollback_confirmed = False
        try:
            with self.driver.session(database=self.database) as session:
                try:
                    tx = session.begin_transaction(timeout=timeout)
                    self._assert_target_empty_tx(tx, target_label)
                    node_map, edge_map = {}, {}
                    groups = {}
                    for row in expected["nodes"]:
                        group = tuple(sorted(row["labels"])) if preserve_extra_labels else (target_label,)
                        groups.setdefault(group, []).append(row)
                    for labels, members in groups.items():
                        label_expression = ":".join(identifier(label) for label in labels)
                        for offset in range(0, len(members), batch_size):
                            rows = members[offset:offset + batch_size]
                            result = list(tx.run(f"// risk_map:create_nodes\nUNWIND $rows AS row CREATE (n:{label_expression}) "
                                                 "SET n=row.properties RETURN row.key AS key, elementId(n) AS id", rows=rows))
                            if len(result) != len(rows) or {r["key"] for r in result} != {r["key"] for r in rows}:
                                raise SubgraphError("node_creation_incomplete")
                            node_map.update((r["key"], r["id"]) for r in result)
                    for kind in sorted({r["type"] for r in expected["edges"]}):
                        rows = [{**r, "start": node_map[r["start"]], "end": node_map[r["end"]]}
                                for r in expected["edges"] if r["type"] == kind]
                        for offset in range(0, len(rows), batch_size):
                            batch = rows[offset:offset + batch_size]
                            result = list(tx.run(
                                f"// risk_map:create_edges\nUNWIND $rows AS row MATCH (a:{tag}), (b:{tag}) "
                                "WHERE elementId(a)=row.start AND elementId(b)=row.end "
                                f"CREATE (a)-[r:{identifier(kind)}]->(b) SET r=row.properties "
                                "RETURN row.key AS key, elementId(r) AS id", rows=batch))
                            if len(result) != len(batch) or {r["key"] for r in result} != {r["key"] for r in batch}:
                                raise SubgraphError("relationship_creation_incomplete")
                            edge_map.update((r["key"], r["id"]) for r in result)
                    if set(node_map) & set(node_map.values()) or set(edge_map) & set(edge_map.values()):
                        raise SubgraphError("source_physical_objects_reused")
                    record = {**base, "target_hash": digest(expected), "status": "committed_" + completeness,
                              "commit_confirmed": True, "node_count": len(node_map), "relationship_count": len(edge_map),
                              "nodes_created": len(node_map), "relationships_created": len(edge_map),
                              "node_map": sorted(node_map.items()), "edge_map": sorted(edge_map.items())}
                    self._verify_copy_tx(tx, record, max_nodes=max_nodes,
                                         max_relationships=max_relationships, max_bytes=max_bytes)
                    commit_started = True
                    tx.commit()
                    commit_acknowledged = True
                    # Cleanup errors after an acknowledged commit must not negate it.
                    try:
                        tx.close()
                    except Exception:
                        record["transaction_cleanup_error"] = True
                    return record
                except Exception as exc:
                    code = getattr(exc, "code", "") or type(exc).__name__
                    error = str(exc) if isinstance(exc, SubgraphError) else str(code)
                    if commit_started:
                        return {**base, "status": "commit_unknown", "error": "commit_acknowledgement_unconfirmed"}
                    if tx is None:
                        return {**base, "status": "failed", "rollback_confirmed": True, "error": error}
                    try:
                        tx.rollback()
                        rollback_confirmed = True
                    except Exception:
                        return {**base, "status": "commit_unknown", "error": "rollback_acknowledgement_unconfirmed"}
                    return {**base, "status": "failed", "rollback_confirmed": True, "error": error}
        except Exception as exc:
            if commit_acknowledged:
                return {**record, "session_cleanup_error": True}
            if tx is not None and not rollback_confirmed:
                return {**base, "status": "commit_unknown", "error": "session_exit_unconfirmed"}
            return {**base, "status": "failed", "rollback_confirmed": tx is None or rollback_confirmed,
                    "error": type(exc).__name__}

    def read_subgraph_articles(self, target_label, ids, *, timeout=120):
        """Read a frozen physical batch, never paginate a mutable property selection."""
        tag = identifier(target_label)
        if len(ids) != len(set(ids)):
            raise SubgraphError("duplicate_article_identity")
        with self.driver.session(database=self.database, default_access_mode=READ_ACCESS) as session:
            def read(tx):
                rows = list(tx.run(f"// risk_map:read_articles\nUNWIND $ids AS id MATCH (n:{tag}) "
                                   "WHERE elementId(n)=id AND n.graph_tag=$tag AND n.label=$kind "
                                   "RETURN elementId(n) AS id, properties(n) AS properties",
                                   ids=ids, tag=target_label, kind="法条"))
                if len(rows) != len(ids) or {r["id"] for r in rows} != set(ids):
                    raise SubgraphError("article_inventory_changed")
                return {r["id"]: dict(r["properties"]) for r in rows}
            return session.execute_read(unit_of_work(timeout=timeout)(read))

    def update_subgraph_article_properties(self, target_label, rows, *, timeout=120):
        """Restricted physical-member patch, one explicit transaction, no write replay."""
        tag = identifier(target_label)
        allowed = {"三级指标映射候选列表", "三级指标映射列表", "三级指标映射状态",
                   "三级指标映射Top5", "三级指标映射版本"}
        if target_label.startswith("_RiskMap") or not isinstance(rows, list) or not rows:
            raise SubgraphError("invalid_mapping_patch")
        ids = [row.get("id") for row in rows]
        if (any(not isinstance(i, str) or not i for i in ids) or len(set(ids)) != len(ids)
                or any(not isinstance(row.get("properties"), dict)
                       or not row["properties"] or set(row["properties"]) - allowed for row in rows)):
            raise SubgraphError("invalid_mapping_patch")
        for row in rows:
            for key, value in row["properties"].items():
                if value is None:
                    continue
                if key in {"三级指标映射Top5", "三级指标映射版本", "三级指标映射状态"}:
                    raise SubgraphError("legacy_mapping_fields_removal_only")
                if key in {"三级指标映射候选列表", "三级指标映射列表"}:
                    if not isinstance(value, list) or any(not isinstance(v, str) for v in value):
                        raise SubgraphError("invalid_mapping_list_type")
        started = False
        acknowledged = False
        result = None
        stage = "open_session"
        matched_count = updated_count = None
        matched_ids = updated_ids = None
        began = time.monotonic()
        secrets = (getattr(self, "password", ""),)
        context = redact_diagnostics({"uri": getattr(self, "uri", ""), "database": self.database, "target_label": target_label,
            "row_count": len(rows), "article_ids_sample": ids[:5], "timeout_seconds": timeout}, secrets)
        logger.info("[法条属性写入开始] %s", json.dumps(context, ensure_ascii=False))
        try:
            with self.driver.session(database=self.database) as session:
                stage = "begin_transaction"
                with session.begin_transaction(timeout=timeout) as tx:
                    stage = "verify_members"
                    members = list(tx.run(
                        f"// risk_map:patch_members\nUNWIND $ids AS id MATCH (n:{tag}) "
                        "WHERE elementId(n)=id AND n.graph_tag=$tag AND n.label=$kind "
                        "RETURN elementId(n) AS id", ids=ids, tag=target_label, kind="法条"))
                    matched_count = len(members)
                    matched_ids = [r["id"] for r in members]
                    if len(members) != len(ids) or set(matched_ids) != set(ids):
                        raise SubgraphError("mapping_member_mismatch")
                    stage = "update_properties"
                    updated = list(tx.run(
                        f"// risk_map:patch_articles\nUNWIND $rows AS row MATCH (n:{tag}) "
                        "WHERE elementId(n)=row.id AND n.graph_tag=$tag AND n.label=$kind "
                        "SET n += row.properties RETURN elementId(n) AS id",
                        rows=rows, tag=target_label, kind="法条"))
                    updated_count = len(updated)
                    updated_ids = [r["id"] for r in updated]
                    if len(updated) != len(ids) or set(updated_ids) != set(ids):
                        raise SubgraphError("mapping_update_incomplete")
                    # += merges property maps; each list VALUE is replaced, never appended.
                    stage = "commit"
                    logger.info("[法条属性准备提交] database=%s target=%s rows=%s elapsed=%.3fs",
                                context["database"], context["target_label"], len(rows), time.monotonic() - began)
                    started = True
                    tx.commit()
                    acknowledged = True
                    result = {"status": "committed", "commit_confirmed": True, "ids": sorted(ids)}
                    stage = "cleanup"
            logger.info("[法条属性提交已确认] database=%s target=%s rows=%s elapsed=%.3fs",
                        context["database"], context["target_label"], len(rows), time.monotonic() - began)
            return result
        except Exception as exc:
            details = {**context, **exception_details(exc, secrets), "failure_stage": stage,
                       "commit_started": started, "commit_confirmed": acknowledged,
                       "matched_count": matched_count, "updated_count": updated_count,
                       "missing_member_ids": sorted(set(ids) - set(matched_ids)) if matched_ids is not None else None,
                       "unexpected_member_ids": sorted(set(matched_ids) - set(ids)) if matched_ids is not None else None,
                       "missing_updated_ids": sorted(set(ids) - set(updated_ids)) if updated_ids is not None else None,
                       "elapsed_seconds": round(time.monotonic() - began, 3)}
            logger.log(logging.WARNING if acknowledged else logging.ERROR,
                       "[法条属性%s] %s", "提交后清理异常" if acknowledged else "写入失败",
                       json.dumps(details, ensure_ascii=False))
            if acknowledged:
                return {**result, "cleanup_error": True, "cleanup_diagnostic": details}
            return {"status": "commit_unknown" if started else "failed",
                    **details}

    def run_query(self, query: str, params: dict = None) -> list:
        """
        执行任意 Cypher 查询，返回记录列表。

        Args:
            query: Cypher 查询语句
            params: 查询参数字典

        Returns:
            list[dict]: 查询结果记录列表，每条记录转为 dict
        """
        if not self.driver:
            logger.error(self.DRIVER_NOT_INITIALIZED_ERROR)
            return []
        try:
            with self.driver.session(database=self.database) as session:
                result = session.run(query, params or {})
                return [dict(record) for record in result]
        except Exception as e:
            logger.error(f"Cypher 查询失败: {e}")
            return []

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
                        node_id = node.get('node_id')
                        node_name = node.get('node_name')
                        node_type = node.get('node_type')
                        if not node_id or not node_name or not node_type:
                            continue
                        # 处理其他属性 - 简化策略，直接设置
                        properties = node.get('properties', {}) or {}
                        sanitized_properties = self._sanitize_properties(properties)
                        # 准备参数
                        params = {
                            'id': node_id,
                            'name': node_name,
                            'label': node_type,
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
                        if not subject_id or not predicate or not object_id:
                            continue

                        relation_label = edge.get('properties', {}).get('label', '') if edge.get('properties') else predicate

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

    def get_nodes_by_properties(
            self,
            graph_tag: str,
            properties: Dict[str, Any],
    ):
        """
        通过属性键值对查找neo4j节点数据

        Args:
            graph_tag (str): 图谱标签
            properties (Dict[str, Any]): 要匹配的属性键值对

        Returns:
            list: 符合条件的节点列表
        """
        if not self.driver:
            logger.error(self.DRIVER_NOT_INITIALIZED_ERROR)
            return []

        try:
            with self.driver.session(database=self.database) as session:
                # 构建动态查询条件
                where_conditions = []
                params = {'graph_tag': graph_tag}
                
                for key, value in properties.items():
                    # 为每个属性创建参数和条件
                    param_name = f"prop_{key}"
                    where_conditions.append(f"n.`{key}` = ${param_name}")
                    params[param_name] = value
                
                where_clause = " AND ".join(where_conditions) if where_conditions else "TRUE"
                
                # 查询指定标签和属性条件的节点
                query = f"""
                    MATCH (n:`{graph_tag}`)
                    WHERE {where_clause}
                    RETURN n
                """
                
                result = session.run(query, params)

                nodes = []
                for record in result:
                    node = record["n"]
                    # 转换节点数据
                    node_properties = dict(node)
                    nodes.append(GraphNode(
                        id=node_properties.get('id', ''),
                        name=node_properties.get('name', ''),
                        label=node_properties.get('label', ''),
                        properties=node_properties
                    ))

                return nodes

        except Exception as e:
            logger.error(f"获取节点列表失败: {str(e)}")
            return []
