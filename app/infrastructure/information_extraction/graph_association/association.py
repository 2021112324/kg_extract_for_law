"""
图谱关联核心逻辑。

通过 Neo4jAdapter 直接操作 Neo4j，将中间节点（法规依据、引用依据）
替换为实际节点（法规文件、法条），建立直接引用关系。
"""
import logging
from typing import Dict, List, Any, Tuple

from app.infrastructure.graph_storage.neo4j_adapter import Neo4jAdapter
from app.infrastructure.graph_storage.base import GraphNode

from .matcher import (
    match_clause_node_via_cypher,
    parse_clause_number,
)

logger = logging.getLogger(__name__)


class GraphAssociator:
    """图谱关联器。注入 Neo4jAdapter，用 Cypher 直接操作 Neo4j。"""

    def __init__(self, adapter: Neo4jAdapter):
        self.adapter = adapter
        self.stats: Dict[str, dict] = {}
        self.records: List[Dict[str, Any]] = []  # 处理记录

    # ---- 主入口 ----

    def associate(self, tag_a: str, tag_b: str) -> dict:
        """执行单向关联，收集详细处理记录。"""
        stats = {"matched": 0, "unmatched": 0, "removed": 0}
        self.stats[f"{tag_a}->{tag_b}"] = stats

        seq = [0]  # 序号计数器，用列表以便在闭包中修改

        def _next_seq():
            seq[0] += 1
            return seq[0]

        # 子任务（1）：法规依据 → 法规文件
        self._resolve_regulation_basis(tag_a, tag_b, stats, _next_seq)

        # 子任务（2）：引用依据 → 法规文件 或 法条
        self._resolve_citation_basis(tag_a, tag_b, stats, _next_seq)

        # task2 子任务（3）：具体法律规定 → 法规文件 / 法条
        self._resolve_legal_reference_node(
            tag_a, tag_b, stats, _next_seq,
            node_type="具体法律规定",
            task_prefix="具体法律规定",
        )

        # task2 子任务（4）：法规条款依据 → 法规文件 / 法条
        self._resolve_legal_reference_node(
            tag_a, tag_b, stats, _next_seq,
            node_type="法规条款依据",
            task_prefix="法规条款依据",
            normalize_case_clause_number=True,
        )

        logger.info(
            f"[{tag_a}->{tag_b}] matched={stats['matched']}, "
            f"unmatched={stats['unmatched']}, removed={stats['removed']}"
        )
        return stats

    # ---- 子任务（1）：法规依据 → 法规文件 ----

    def _resolve_regulation_basis(self, tag_a, tag_b, stats, _next_seq):
        basis_nodes = self.adapter.get_nodes_by_type(tag_a, "法规依据")
        target_files = self.adapter.get_nodes_by_type(tag_b, "法规文件")

        if not basis_nodes:
            return
        if not target_files:
            for node in basis_nodes:
                stats["unmatched"] += 1
                self._add_record(
                    _next_seq(), "法规依据 → 法规文件", "失败",
                    intermediate_node=node, source_tag=tag_a,
                    fail_reason=f"目标图谱 {tag_b} 中没有法规文件节点",
                )
            return

        total = len(basis_nodes)
        for idx, node in enumerate(basis_nodes, 1):
            matched, method = match_node_to_file_with_method(node, target_files)
            if matched is None:
                stats["unmatched"] += 1
                logger.info(f"  [{idx}/{total}] 法规依据 '{node.name}' → ✗ 未匹配")
                self._add_record(
                    _next_seq(), "法规依据 → 法规文件", "失败",
                    intermediate_node=node, source_tag=tag_a,
                    fail_reason=_unmatch_reason(node, target_files),
                )
                continue

            old_rels = self._get_relations(tag_a, node.id)
            self._replace_node(tag_a, tag_b, node.id, matched.id)
            stats["matched"] += 1
            stats["removed"] += 1
            logger.info(f"  [{idx}/{total}] 法规依据 '{node.name}' → '{matched.name}' ✓")

            self._add_record(
                _next_seq(), "法规依据 → 法规文件", "成功",
                intermediate_node=node, source_tag=tag_a,
                matched_node=matched, target_tag=tag_b,
                match_method=method,
                old_relations=old_rels,
                new_target_id=matched.id, new_target_tag=tag_b,
            )

    # ---- 子任务（2）：引用依据 → 法规文件 / 法条 ----

    def _resolve_citation_basis(self, tag_a, tag_b, stats, _next_seq):
        citation_nodes = self.adapter.get_nodes_by_type(tag_a, "引用依据")
        target_files = self.adapter.get_nodes_by_type(tag_b, "法规文件")

        if not citation_nodes:
            return
        if not target_files:
            for node in citation_nodes:
                stats["unmatched"] += 1
                self._add_record(
                    _next_seq(), f"引用依据({node.properties.get('引用类型', '?')}) → 目标", "失败",
                    intermediate_node=node, source_tag=tag_a,
                    fail_reason=f"目标图谱 {tag_b} 中没有法规文件节点",
                )
            return

        total = len(citation_nodes)
        for idx, node in enumerate(citation_nodes, 1):
            cit_type = node.properties.get("引用类型", "")
            task_type = f"引用依据({cit_type or '未知'}) → {'法规文件' if cit_type == '文件' else '法条' if cit_type == '条款' else '目标'}"

            if cit_type == "文件":
                matched, method = match_node_to_file_with_method(node, target_files)
                if matched is None:
                    stats["unmatched"] += 1
                    logger.info(f"  [{idx}/{total}] {task_type} '{node.name}' → ✗ 未匹配")
                    self._add_record(
                        _next_seq(), task_type, "失败",
                        intermediate_node=node, source_tag=tag_a,
                        fail_reason=_unmatch_reason(node, target_files),
                    )
                    continue

                old_rels = self._get_relations(tag_a, node.id)
                self._replace_node(tag_a, tag_b, node.id, matched.id)
                stats["matched"] += 1
                stats["removed"] += 1
                logger.info(f"  [{idx}/{total}] {task_type} '{node.name}' → '{matched.name}' ✓")

                self._add_record(
                    _next_seq(), task_type, "成功",
                    intermediate_node=node, source_tag=tag_a,
                    matched_node=matched, target_tag=tag_b,
                    match_method=method,
                    old_relations=old_rels,
                    new_target_id=matched.id, new_target_tag=tag_b,
                )

            elif cit_type == "条款":
                parent_file, method = match_node_to_file_with_method(node, target_files)
                if parent_file is None:
                    stats["unmatched"] += 1
                    logger.info(f"  [{idx}/{total}] {task_type} '{node.name}' → ✗ 父文件未匹配")
                    self._add_record(
                        _next_seq(), task_type, "失败",
                        intermediate_node=node, source_tag=tag_a,
                        fail_reason=f"父法规文件未匹配: {_unmatch_reason(node, target_files)}",
                    )
                    continue

                clause_number = node.properties.get("条款编号", "")
                matched_clause_id = match_clause_node_via_cypher(
                    str(clause_number), parent_file.id, self.adapter, tag_b
                )
                if matched_clause_id is None:
                    stats["unmatched"] += 1
                    logger.info(f"  [{idx}/{total}] {task_type} '{node.name}' → ✗ 法条未找到({clause_number})")
                    self._add_record(
                        _next_seq(), task_type, "失败",
                        intermediate_node=node, source_tag=tag_a,
                        matched_node=parent_file, target_tag=tag_b,
                        match_method=f"{method}(父文件)",
                        fail_reason=f"在法规文件 '{parent_file.name}' 下未找到条款 '{clause_number}' 对应的法条",
                    )
                    continue

                old_rels = self._get_relations(tag_a, node.id)
                clause_info = self.adapter.run_query(
                    f"MATCH (n:`{tag_b}` {{id: $id}}) RETURN n.name AS name",
                    {"id": matched_clause_id},
                )
                clause_name = clause_info[0]["name"] if clause_info else matched_clause_id

                self._replace_node(tag_a, tag_b, node.id, matched_clause_id)
                stats["matched"] += 1
                stats["removed"] += 1
                logger.info(f"  [{idx}/{total}] {task_type} '{node.name}' → '{clause_name}' ✓")

                self._add_record(
                    _next_seq(), task_type, "成功",
                    intermediate_node=node, source_tag=tag_a,
                    matched_node=GraphNode(id=matched_clause_id, name=clause_name,
                                           label="法条", properties={"条": clause_number}),
                    target_tag=tag_b,
                    match_method=f"{method}(父文件) + 条款匹配",
                    old_relations=old_rels,
                    new_target_id=matched_clause_id, new_target_tag=tag_b,
                )

            else:
                stats["unmatched"] += 1
                self._add_record(
                    _next_seq(), task_type, "失败",
                    intermediate_node=node, source_tag=tag_a,
                    fail_reason=f"引用类型无效: '{cit_type}'",
                )

    # ---- task2：具体法律规定 / 法规条款依据 → 法规文件 / 法条 ----

    def _resolve_legal_reference_node(
        self,
        tag_a,
        tag_b,
        stats,
        _next_seq,
        node_type: str,
        task_prefix: str,
        normalize_case_clause_number: bool = False,
    ):
        """按法规名称和条款编号，将新增中间节点替换为法规文件或法条。

        _replace_node 会保留中间节点的全部入边/出边，因此与主语节点之间的
        关系会自然迁移到匹配后的法规文件或法条上。
        """
        nodes = self.adapter.get_nodes_by_type(tag_a, node_type)
        target_files = self.adapter.get_nodes_by_type(tag_b, "法规文件")

        if not nodes:
            return
        if not target_files:
            for node in nodes:
                stats["unmatched"] += 1
                self._add_record(
                    _next_seq(), f"{task_prefix} → 目标", "失败",
                    intermediate_node=node, source_tag=tag_a,
                    fail_reason=f"目标图谱 {tag_b} 中没有法规文件节点",
                )
            return

        total = len(nodes)
        for idx, node in enumerate(nodes, 1):
            if normalize_case_clause_number:
                self._normalize_case_clause_number(tag_a, node)

            clause_number = str(node.properties.get("条款编号", "") or "").strip()
            # 条款编号无效时（如"新修订"），当成无条款处理，直接匹配法规文件
            if clause_number and parse_clause_number(clause_number) is None:
                logger.info(f"  [{idx}/{total}] '{node.name}' 条款编号 '{clause_number}' 非有效编号，回退为匹配法规文件")
                clause_number = ""
            task_type = f"{task_prefix} → {'法条' if clause_number else '法规文件'}"

            # 法规名称为空时，尝试从节点名称中提取
            reg_name = str(node.properties.get("法规名称", "") or "").strip()
            if not reg_name or reg_name == "无":
                extracted = _extract_name_from_node_name(node.name)
                if extracted:
                    node.properties["法规名称"] = extracted
                    logger.info(f"  [{idx}/{total}] 法规名称为空，从节点名提取: '{extracted}'")

            parent_file, method = match_node_to_file_with_method(
                node, target_files, source_name_keys=["法规名称"]
            )
            if parent_file is None:
                stats["unmatched"] += 1
                logger.info(f"  [{idx}/{total}] {task_type} '{node.name}' → ✗ 父文件未匹配")
                self._add_record(
                    _next_seq(), task_type, "失败",
                    intermediate_node=node, source_tag=tag_a,
                    fail_reason=_unmatch_reason(node, target_files, source_name_keys=["法规名称"]),
                )
                continue

            if not clause_number:
                old_rels = self._get_relations(tag_a, node.id)
                self._replace_node(tag_a, tag_b, node.id, parent_file.id)
                stats["matched"] += 1
                stats["removed"] += 1
                logger.info(f"  [{idx}/{total}] {task_type} '{node.name}' → '{parent_file.name}' ✓")

                self._add_record(
                    _next_seq(), task_type, "成功",
                    intermediate_node=node, source_tag=tag_a,
                    matched_node=parent_file, target_tag=tag_b,
                    match_method=method,
                    old_relations=old_rels,
                    new_target_id=parent_file.id, new_target_tag=tag_b,
                )
                continue

            matched_clause_id = match_clause_node_via_cypher(
                clause_number, parent_file.id, self.adapter, tag_b
            )
            if matched_clause_id is None:
                stats["unmatched"] += 1
                logger.info(f"  [{idx}/{total}] {task_type} '{node.name}' → ✗ 法条未找到({clause_number})")
                self._add_record(
                    _next_seq(), task_type, "失败",
                    intermediate_node=node, source_tag=tag_a,
                    matched_node=parent_file, target_tag=tag_b,
                    match_method=f"{method}(父文件)",
                    fail_reason=f"在法规文件 '{parent_file.name}' 下未找到条款 '{clause_number}' 对应的法条",
                )
                continue

            old_rels = self._get_relations(tag_a, node.id)
            clause_info = self.adapter.run_query(
                f"MATCH (n:`{tag_b}` {{id: $id}}) RETURN n.name AS name, n.条 AS tiao",
                {"id": matched_clause_id},
            )
            clause_name = clause_info[0]["name"] if clause_info else matched_clause_id
            clause_tiao = clause_info[0].get("tiao") if clause_info else clause_number

            self._replace_node(tag_a, tag_b, node.id, matched_clause_id)
            stats["matched"] += 1
            stats["removed"] += 1
            logger.info(f"  [{idx}/{total}] {task_type} '{node.name}' → '{clause_name}' ✓")

            self._add_record(
                _next_seq(), task_type, "成功",
                intermediate_node=node, source_tag=tag_a,
                matched_node=GraphNode(id=matched_clause_id, name=clause_name,
                                       label="法条", properties={"条": clause_tiao or clause_number}),
                target_tag=tag_b,
                match_method=f"{method}(父文件) + 条款匹配",
                old_relations=old_rels,
                new_target_id=matched_clause_id, new_target_tag=tag_b,
            )

    def _normalize_case_clause_number(self, tag: str, node: GraphNode):
        """将法规条款依据的 案例条款编号 归一为 条款编号。"""
        if node.properties.get("条款编号") or not node.properties.get("案例条款编号"):
            return
        value = node.properties.get("案例条款编号")
        self.adapter.run_query(
            f"MATCH (n:`{tag}` {{id: $id}}) "
            "SET n.条款编号 = $value REMOVE n.案例条款编号",
            {"id": node.id, "value": value},
        )
        node.properties["条款编号"] = value
        node.properties.pop("案例条款编号", None)

    # ---- 记录构建 ----

    def _add_record(self, seq, task_type, status, **kwargs):
        rec = {
            "序号": seq,
            "处理类型": task_type,
            "状态": status,
            "中间节点": _node_info(kwargs.get("intermediate_node"), kwargs.get("source_tag", "")),
        }
        if status == "成功":
            rec["匹配结果"] = _node_info(kwargs.get("matched_node"), kwargs.get("target_tag", ""))
            rec["匹配方式"] = kwargs.get("match_method", "?")
            rec["关系变更"] = _build_rel_changes(
                kwargs.get("old_relations", []),
                kwargs.get("intermediate_node"),
                kwargs.get("new_target_id", "?"),
                kwargs.get("new_target_tag", ""),
            )
        else:
            rec["失败原因"] = kwargs.get("fail_reason", "未知")
        self.records.append(rec)

    # ---- 关系查询 ----

    def _get_relations(self, tag: str, node_id: str) -> List[Dict]:
        """获取某节点的所有入边和出边信息。"""
        in_rels = self.adapter.run_query(
            f"MATCH (source:`{tag}`)-[r]->(n:`{tag}` {{id: $id}}) "
            "RETURN source.id AS source_id, source.name AS source_name, type(r) AS rtype",
            {"id": node_id},
        )
        out_rels = self.adapter.run_query(
            f"MATCH (n:`{tag}` {{id: $id}})-[r]->(target:`{tag}`) "
            "RETURN target.id AS target_id, target.name AS target_name, type(r) AS rtype",
            {"id": node_id},
        )
        return [
            {"方向": "入", "关联节点": r["source_id"], "关联节点名": r["source_name"], "关系类型": r["rtype"]}
            for r in in_rels
        ] + [
            {"方向": "出", "关联节点": r["target_id"], "关联节点名": r["target_name"], "关系类型": r["rtype"]}
            for r in out_rels
        ]

    # ---- 节点替换（Cypher，事务保护） ----

    def _replace_node(self, tag_a, tag_b, old_id, new_id):
        """将 tag_a 中所有引用 old_id 的关系改为引用 tag_b 中的 new_id。

        在一个 Neo4j 事务内完成全部操作，保证原子性：
          发现关系 → 创建新关系(含全部属性) → 删旧关系 → 删旧节点
        """
        # 1. 查询所有关联关系（含完整属性）
        in_rels = self.adapter.run_query(
            f"MATCH (source:`{tag_a}`)-[r]->(old:`{tag_a}` {{id: $old_id}}) "
            "RETURN source.id AS source_id, type(r) AS rtype, properties(r) AS props",
            {"old_id": old_id},
        )
        out_rels = self.adapter.run_query(
            f"MATCH (old:`{tag_a}` {{id: $old_id}})-[r]->(target:`{tag_a}`) "
            "RETURN target.id AS target_id, type(r) AS rtype, properties(r) AS props",
            {"old_id": old_id},
        )

        # 2. 收集所有需要创建的边，按 (src, tgt, type, src_tag, tgt_tag) 去重
        # 入边: source(tag_a) → new_node(tag_b)
        # 出边: new_node(tag_b) → target(tag_a)
        replacements: Dict[Tuple[str, str, str, str, str], dict] = {}

        for rec in in_rels:
            sid = rec["source_id"]
            if sid == old_id:
                continue
            key = (sid, new_id, rec["rtype"], tag_a, tag_b)
            if key not in replacements:
                replacements[key] = {}
            _merge_props(replacements[key], rec["props"] or {})

        for rec in out_rels:
            tid = rec["target_id"]
            if tid == old_id:
                continue
            key = (new_id, tid, rec["rtype"], tag_b, tag_a)
            if key not in replacements:
                replacements[key] = {}
            _merge_props(replacements[key], rec["props"] or {})

        # 3. 组装查询列表
        queries: List[Tuple[str, dict]] = []

        for (src, tgt, rtype, src_tag, tgt_tag), merged_props in replacements.items():
            safe_type = _safe_rel_type(rtype)
            merged_props["label"] = rtype
            queries.append((
                f"MATCH (src_node:`{src_tag}` {{id: $src_id}}) "
                f"MATCH (tgt_node:`{tgt_tag}` {{id: $tgt_id}}) "
                f"MERGE (src_node)-[r:`{safe_type}`]->(tgt_node) "
                f"ON CREATE SET r = $props "
                f"ON MATCH SET r += $props",
                {"src_id": src, "tgt_id": tgt, "props": merged_props},
            ))

        # 3. 删除旧节点（DETACH 清理残留关系）
        queries.append((
            f"MATCH (old:`{tag_a}` {{id: $old_id}}) DETACH DELETE old",
            {"old_id": old_id},
        ))

        # 4. 在单个事务中执行全部查询
        self._run_in_transaction(queries)

    def _run_in_transaction(self, queries: List[Tuple[str, dict]]):
        """在一个 Neo4j 事务中执行多条查询，保证原子性。"""
        if not queries:
            return
        with self.adapter.driver.session(database=self.adapter.database) as session:
            with session.begin_transaction() as tx:
                for query, params in queries:
                    tx.run(query, params or {})
                tx.commit()


# ---- 辅助函数 ----

def _safe_rel_type(rtype: str) -> str:
    return rtype.replace("`", "``")


def _merge_props(base: dict, incoming: dict):
    """合并属性：标量值后到覆盖，列表值拼接。"""
    for k, v in incoming.items():
        if isinstance(base.get(k), list) and isinstance(v, list):
            base[k] = base[k] + v
        else:
            base[k] = v


def _node_info(node, tag: str) -> Dict:
    """从 GraphNode 提取关键信息。"""
    if node is None:
        return {}
    info = {
        "节点ID": node.id,
        "节点名称": node.name,
        "节点类型": node.label or node.properties.get("label", ""),
    }
    # 附加关键属性
    for key in ["文件全称", "文件别名", "法规名称", "引用类型", "条款编号", "案例条款编号", "条"]:
        val = node.properties.get(key)
        if val:
            info[key] = val
    if tag:
        info["所在图谱"] = tag
    return info


def _build_rel_changes(old_rels, old_node, new_target_id, target_tag):
    """构建关系变更说明列表。"""
    if not old_rels:
        return []
    changes = []
    old_name = old_node.name if old_node else "?"
    old_id = old_node.id if old_node else "?"
    for rel in old_rels:
        if rel["方向"] == "入":
            old_desc = f"{rel['关联节点']}({rel['关联节点名']}) -[{rel['关系类型']}]-> {old_id}({old_name})"
            new_desc = f"{rel['关联节点']}({rel['关联节点名']}) -[{rel['关系类型']}]-> {new_target_id} [{target_tag}]"
        else:
            old_desc = f"{old_id}({old_name}) -[{rel['关系类型']}]-> {rel['关联节点']}({rel['关联节点名']})"
            new_desc = f"{new_target_id} [{target_tag}] -[{rel['关系类型']}]-> {rel['关联节点']}({rel['关联节点名']})"
        changes.append({"旧关系": old_desc, "新关系": new_desc})
    return changes


def _unmatch_reason(node, target_files, source_name_keys=None) -> str:
    """生成未匹配原因的详细说明。"""
    source_name_keys = source_name_keys or ["文件全称", "文件别名"]
    parts = []
    for key in source_name_keys:
        val = node.properties.get(key)
        parts.append(f"{key}='{val if val else '无'}'")
    parts.append(f"在 {len(target_files)} 个目标法规文件中未找到匹配")
    return "；".join(parts)


# ---- 带匹配方式的匹配函数 ----

def match_node_to_file_with_method(
    node: GraphNode,
    target_files: List[GraphNode],
    source_name_keys: List[str] = None,
):
    """同 match_node_to_file，同时返回匹配方式。"""
    from .matcher import _to_str_list, _strip_version_suffix

    source_name_keys = source_name_keys or ["文件全称", "文件别名"]
    search_names: List[Tuple[str, str]] = []
    for key in source_name_keys:
        for value in _to_str_list(node.properties.get(key, "")):
            if value:
                search_names.append((key, value))
                # 同时加入去除书名号的版本
                cleaned = value.strip("\u300a\u300b\u3008\u3009")
                if cleaned != value:
                    search_names.append((key, cleaned))
                # 若名称含《》，提取《》之间的内容（如 原国土资源部《闲置土地处置办法》→ 闲置土地处置办法）
                inner = _extract_bookmark_content(value)
                if inner and inner != value and inner != cleaned:
                    search_names.append((key, inner))

    if not search_names or not target_files:
        return None, ""

    by_fullname: Dict[str, GraphNode] = {}
    by_alias: Dict[str, GraphNode] = {}
    for f in target_files:
        fn = str(f.properties.get("文件全称", ""))
        if fn:
            by_fullname[fn] = f
            # 同时加入去除版本后缀的版本，如 工伤保险条例(2010修订) → 工伤保险条例
            stripped_fn = _strip_version_suffix(fn)
            if stripped_fn != fn and stripped_fn not in by_fullname:
                by_fullname[stripped_fn] = f
        for a in _to_str_list(f.properties.get("文件别名", "")):
            if a:
                by_alias[a] = f
                stripped_a = _strip_version_suffix(a)
                if stripped_a != a and stripped_a not in by_alias:
                    by_alias[stripped_a] = f

    for source_key, name in search_names:
        if name in by_fullname:
            return by_fullname[name], f"{source_key}=文件全称"
        if name in by_alias:
            return by_alias[name], f"{source_key}=文件别名"

    for source_key, name in search_names:
        if not name.startswith("中华人民共和国"):
            prefixed = "中华人民共和国" + name
            if prefixed in by_fullname:
                return by_fullname[prefixed], f"{source_key}加'中华人民共和国'前缀后匹配文件全称"
            if prefixed in by_alias:
                return by_alias[prefixed], f"{source_key}加'中华人民共和国'前缀后匹配文件别名"
        else:
            stripped = name[7:]
            if stripped and stripped in by_fullname:
                return by_fullname[stripped], f"{source_key}去'中华人民共和国'前缀后匹配文件全称"
            if stripped and stripped in by_alias:
                return by_alias[stripped], f"{source_key}去'中华人民共和国'前缀后匹配文件别名"

    # 4. 去除"新修订"等修饰词后再匹配
    for source_key, name in search_names:
        for prefix in ["新修订的", "新修订"]:
            if name.startswith(prefix):
                stripped_name = name[len(prefix):].strip()
                if not stripped_name:
                    continue
                if stripped_name in by_fullname:
                    return by_fullname[stripped_name], f"{source_key}去'{prefix}'后匹配文件全称"
                if stripped_name in by_alias:
                    return by_alias[stripped_name], f"{source_key}去'{prefix}'后匹配文件别名"
                # 再去中华人民共和国前缀
                if not stripped_name.startswith("中华人民共和国"):
                    prefixed2 = "中华人民共和国" + stripped_name
                    if prefixed2 in by_fullname:
                        return by_fullname[prefixed2], f"{source_key}去'{prefix}'加'中华人民共和国'后匹配文件全称"
                    if prefixed2 in by_alias:
                        return by_alias[prefixed2], f"{source_key}去'{prefix}'加'中华人民共和国'后匹配文件别名"
                else:
                    stripped2 = stripped_name[7:]
                    if stripped2 and stripped2 in by_fullname:
                        return by_fullname[stripped2], f"{source_key}去'{prefix}'去'中华人民共和国'后匹配文件全称"
                    if stripped2 and stripped2 in by_alias:
                        return by_alias[stripped2], f"{source_key}去'{prefix}'去'中华人民共和国'后匹配文件别名"

    # 5. 剥离发文机关前缀（如 司法部《xxx》 → xxx）
    for source_key, name in search_names:
        org_prefixes = ["司法部", "国务院", "最高人民法院", "最高人民检察院", "中共中央办公厅", "国务院办公厅"]
        for org in org_prefixes:
            if name.startswith(org):
                stripped_name = name[len(org):].strip().strip("《》〈〉")
                if not stripped_name:
                    continue
                if stripped_name in by_fullname:
                    return by_fullname[stripped_name], f"{source_key}去'{org}'发文机关前缀后匹配文件全称"
                if stripped_name in by_alias:
                    return by_alias[stripped_name], f"{source_key}去'{org}'发文机关前缀后匹配文件别名"

    return None, ""


def _extract_bookmark_content(name: str) -> str:
    """从名称中提取《》之间的内容，丢弃外部发文机关前缀。

    示例:
        '原国土资源部《闲置土地处置办法》' → '闲置土地处置办法'
        '司法部《关于进一步加强行政复议调解工作推动行政争议实质性化解的指导意见》' → '关于进一步加强行政复议调解工作推动行政争议实质性化解的指导意见'
        '中华人民共和国行政复议法' → ''（不含《》，返回空）
    """
    import re
    m = re.search(r'\u300a([^\u300b]+)\u300b', name)
    return m.group(1).strip() if m else ''


def _extract_name_from_node_name(node_name: str) -> str:
    """从节点名提取可能的法规名称。

    示例:
        '中华人民共和国行政复议法_第十条' → '中华人民共和国行政复议法'
        '《工伤保险条例》1' → '《工伤保险条例》'
        '《中华人民共和国行政复议法》第五条' → '《中华人民共和国行政复议法》'
    """
    import re
    name = (node_name or "").strip()
    if not name:
        return ""
    # 去除末尾的条款编号和数字后缀（_分隔或直接连接）
    name = re.sub(r'[_\s]*第[零一二三四五六七八九十百千万\d]+\s*条.*$', '', name)
    # 去除末尾的下划线+数字
    name = re.sub(r'_\d+$', '', name)
    # 去除末尾的数字后缀
    name = re.sub(r'\d+$', '', name)
    return name.strip()
