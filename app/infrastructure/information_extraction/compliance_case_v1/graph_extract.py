"""合规案例单阶段知识图谱抽取主流程。

本文件负责把 compliance_case_parse.py 生成的单案例输入送入 LLM，
再将 LLM 返回的实体、关系规范化为项目通用的 graph 结构。

整体流程：
1. parse_compliance_case_file：规则读取已有结构，构造单案例 LLM 输入。
2. entity_and_relationship_extract：一次性抽取实体和关系。
3. _ComplianceCaseGraphBuilder：合并 parser 案例主节点与 LLM 抽取结果。
4. export_case_knowledge_from_graph：从图谱导出知识库字段视图。

注意：
这里的“单阶段”指知识图谱语义抽取只调用一次 LLM，不按小节拆分多次抽取。
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from pathlib import Path
from typing import Any

from app.infrastructure.information_extraction.base import Entity, Relationship
from app.infrastructure.information_extraction.compliance_case_v1.compliance_case_parse import (
    ComplianceCaseDocument,
    discover_compliance_case_files,
    parse_compliance_case_file,
)
from app.infrastructure.information_extraction.compliance_case_v1.export_case_knowledge import (
    CONTROLLED_RISK_TYPES,
    export_case_knowledge_from_graph,
    _map_to_controlled_risk_type,
)
from app.infrastructure.information_extraction.compliance_case_v1.prompt.example import (
    example_for_compliance_case,
)
from app.infrastructure.information_extraction.compliance_case_v1.prompt.prompt import (
    prompt_for_compliance_case,
)
from app.infrastructure.information_extraction.compliance_case_v1.prompt.schema import (
    schema_for_compliance_case,
    schema_for_compliance_case_runtime,
)
from app.infrastructure.information_extraction.factory import InformationExtractionFactory
from app.infrastructure.information_extraction.method.base import LangextractConfig
from app.infrastructure.string_utils.id_tool import generate_hex_uuid


NODE_TYPES = {node["type"] for node in schema_for_compliance_case_runtime.get("nodes", [])}
ALLOWED_EDGE_TRIPLES = {
    (edge["source"], edge["type"], edge["target"])
    for edge in schema_for_compliance_case_runtime.get("edges", [])
}
CASE_PROTECTED_PROPERTIES = {
    "案例编号",
    "案例名称",
    "案例类型",
    "合规领域",
    "关键词",
    "数据来源类型",
    "原文全文",
    "source_path",
}

COMPLIANCE_CASE_MODEL = os.getenv("COMPLIANCE_CASE_MODEL", "qwen3-30b-a3b-instruct-2507")
COMPLIANCE_CASE_API_KEY = os.getenv(
    "COMPLIANCE_CASE_API_KEY",
    os.getenv("NATIONAL_STANDARD_GRAPH_API_KEY", "gpustack_342609ce423be29a_4371426b285a91dc44fb4e8d72454847"),
)
COMPLIANCE_CASE_API_URL = os.getenv(
    "COMPLIANCE_CASE_API_URL",
    os.getenv("NATIONAL_STANDARD_GRAPH_API_URL", "http://222.171.219.26:20001/v1/chat/completions"),
)
COMPLIANCE_CASE_TIMEOUT = int(os.getenv("COMPLIANCE_CASE_TIMEOUT", "3000"))
COMPLIANCE_CASE_MAX_CHAR_BUFFER = int(os.getenv("COMPLIANCE_CASE_MAX_CHAR_BUFFER", "30000"))
COMPLIANCE_CASE_BATCH_LENGTH = int(os.getenv("COMPLIANCE_CASE_BATCH_LENGTH", "1"))
COMPLIANCE_CASE_MAX_WORKERS = int(os.getenv("COMPLIANCE_CASE_MAX_WORKERS", "1"))


class ResultStats:
    """知识图谱抽取结果统计，记录错误、弱警告、强警告的数量和描述信息。"""

    def __init__(self):
        self.error = 0
        self.error_msg = ""
        self.week_warning = 0
        self.week_warning_msg = ""
        self.strong_warning = 0
        self.strong_warning_msg = ""


class ComplianceCaseGraphExtractor:
    """合规案例单阶段抽取器。

    使用方式：
    - enable_llm=True：正常调用 LLM 进行实体关系抽取。
    - enable_llm=False：只跑 parser 和案例主节点构建，用于本地联调、结构验证。

    enable_llm=False 很有用，因为它不依赖模型服务，也不会产生远程调用成本。
    """

    def __init__(self, enable_llm: bool = True) -> None:
        self.enable_llm = enable_llm
        self.result_stats = ResultStats()
        self.extractor = None
        if enable_llm:
            # LangextractConfig 是项目内 langextract 适配器的模型配置。
            # 默认值允许通过环境变量覆盖，方便在不同机器/模型服务间切换。
            config = LangextractConfig(
                model_name=COMPLIANCE_CASE_MODEL,
                api_key=COMPLIANCE_CASE_API_KEY,
                api_url=COMPLIANCE_CASE_API_URL,
                config={"timeout": COMPLIANCE_CASE_TIMEOUT},
                max_char_buffer=COMPLIANCE_CASE_MAX_CHAR_BUFFER,
                batch_length=COMPLIANCE_CASE_BATCH_LENGTH,
                max_workers=COMPLIANCE_CASE_MAX_WORKERS,
            )
            self.extractor = InformationExtractionFactory.create("langextract", max_retries=3, config=config)

    async def extract(self, input_path: str | Path) -> dict[str, Any]:
        """抽取单个案例文件。

        返回结构包含：
        - parsed_document：规则读取后的中间结果；
        - llm_extraction：LLM 原始实体/关系结果的简化版；
        - graph：规范化后的 nodes/edges；
        - case_knowledge：从 graph 聚合出的知识库字段视图；
        - stats：节点、边、LLM 调用次数。
        """

        # 第一步：只做结构读取，不做语义抽取。
        document = parse_compliance_case_file(input_path)

        # 第二步：一个完整案例调用一次 LLM。no-llm 模式下返回空抽取结果。
        llm_result = await self._extract_with_llm(document) if self.enable_llm else self._empty_llm_result()

        # 第三步：构建图谱。先放 parser 生成的案例主节点，再合并 LLM 结果。
        graph_builder = _ComplianceCaseGraphBuilder(document, self.result_stats)
        graph_builder.add_case_context_node()
        graph_builder.add_llm_result(llm_result)
        graph = graph_builder.to_graph()

        # 第四步：从图谱导出知识库字段。注意这只是下游视图。
        case_knowledge = export_case_knowledge_from_graph(graph, document.to_dict())
        status = "success" if llm_result.get("status") == "success" else llm_result.get("status", "failed")

        # 统计错误：LLM 抽取失败 或 图谱只有案例主节点（无有效抽取内容）
        if status != "success":
            self.result_stats.error += 1
            self.result_stats.error_msg += f"{Path(input_path).name}: {status}; "
        elif len(graph.get("nodes", [])) <= 1:
            self.result_stats.error += 1
            self.result_stats.error_msg += f"{Path(input_path).name}: 图谱只有案例主节点，无有效抽取内容; "
        return {
            "filename": Path(input_path).name,
            "case_id": document.case_id,
            "status": status,
            "extraction_mode": "single_stage_llm" if self.enable_llm else "parser_only",
            "parsed_document": document.to_dict(),
            "llm_extraction": llm_result,
            "graph": graph,
            "case_knowledge": case_knowledge,
            "stats": {
                "node_count": len(graph.get("nodes", [])),
                "edge_count": len(graph.get("edges", [])),
                "llm_call_count": 1 if self.enable_llm else 0,
            },
        }

    async def logging_result_stats(self):
        """输出知识图谱抽取的错误、弱警告、强警告统计信息。"""
        logging.info("📄✅：数据统计")
        logging.info(f"📄✅：错误数: {self.result_stats.error}")
        logging.info(f"📄✅：弱警告数: {self.result_stats.week_warning}")
        logging.info(f"📄✅：强警告数: {self.result_stats.strong_warning}")

    async def _extract_with_llm(self, document: ComplianceCaseDocument) -> dict[str, Any]:
        """调用 LLM 做一次性实体关系抽取。

        langextract 返回的实体/关系可能是 Pydantic 对象，也可能是 dict。
        后续统一交给 _serialize_extract_result 转为普通 dict，便于落盘和图构建。
        """

        try:
            if self.extractor is None:
                return self._empty_llm_result()
            result = await self.extractor.entity_and_relationship_extract(
                user_prompt=prompt_for_compliance_case,
                schema=schema_for_compliance_case,
                input_text=document.llm_input,
                examples=example_for_compliance_case,
            )
            return {"status": "success", **self._serialize_extract_result(result)}
        except Exception as exc:
            # 抽取失败不直接抛出，避免批处理被一个文件中断。
            # 失败信息会写入单文件结果，summary 中计为 failed。
            return {"status": "failed", "error": str(exc), "entities": [], "relations": []}

    @staticmethod
    def _empty_llm_result() -> dict[str, Any]:
        """构造 no-llm 模式或未初始化抽取器时的空结果。"""

        return {"status": "parser_only", "entities": [], "relations": []}

    @staticmethod
    def _serialize_extract_result(result: dict[str, Any]) -> dict[str, Any]:
        """把 langextract 抽取结果统一转成可 JSON 序列化的 dict。

        项目里 Entity/Relationship 是 Pydantic 模型；有些测试或兼容路径可能
        直接返回 dict。本函数同时兼容两种情况。

        返回格式：
        {
            "entities": [{"name": ..., "type": ..., "properties": {...}}],
            "relations": [{"source": ..., "target": ..., "type": ..., "properties": {...}}]
        }
        """

        entities: list[dict[str, Any]] = []
        for entity in result.get("entities", []) or []:
            if isinstance(entity, Entity):
                entities.append(
                    {
                        "name": entity.name,
                        "type": entity.entity_type,
                        "properties": entity.properties,
                    }
                )
            elif isinstance(entity, dict):
                entities.append(
                    {
                        "name": entity.get("name", ""),
                        "type": entity.get("type") or entity.get("entity_type", ""),
                        "properties": entity.get("properties") or entity.get("attributes") or {},
                    }
                )
        relations: list[dict[str, Any]] = []
        for relation in result.get("relations", []) or []:
            if isinstance(relation, Relationship):
                relations.append(
                    {
                        "source": relation.source,
                        "target": relation.target,
                        "type": relation.type,
                        "properties": relation.properties,
                    }
                )
            elif isinstance(relation, dict):
                props = relation.get("properties") or relation.get("attributes") or {}
                # 兼容两类关系表达：
                # 1. source/target/type；
                # 2. attributes 中的 主体/谓词/客体。
                relations.append(
                    {
                        "source": relation.get("source") or relation.get("subject") or props.get("主体", ""),
                        "target": relation.get("target") or relation.get("object") or props.get("客体", ""),
                        "type": relation.get("type") or relation.get("predicate") or props.get("谓词", ""),
                        "properties": props,
                    }
                )
        return {"entities": entities, "relations": relations}


class _ComplianceCaseGraphBuilder:
    """将 parser 上下文节点和 LLM 抽取结果合并为最终图谱。

    关键策略：
    - parser 必定创建一个案例主节点，避免 LLM 漏抽案例导致整张图没有根。
    - LLM 抽到的“案例”实体不新增第二个案例节点，而是合并到 parser 主节点。
    - 法规条款依据允许按“法规名称 + 条款编号”合并，方便跨案例共享。
    - 关系端点必须能解析到已有节点，否则跳过，避免生成悬空边。
    """

    def __init__(self, document: ComplianceCaseDocument, stats: ResultStats | None = None) -> None:
        self.document = document
        self.stats = stats or ResultStats()
        # key 是 (node_type, node_name_or_merge_key)，用于节点去重。
        self.nodes: dict[tuple[str, str], dict[str, Any]] = {}
        self.edges: list[dict[str, Any]] = []
        # LLM 输出关系时通常只给实体名，所以需要 name/alias -> node_id 的索引。
        self.name_to_id: dict[str, str] = {}

    def add_case_context_node(self) -> str:
        """添加 parser 生成的案例主节点。

        这个节点不是 LLM 抽取出来的，而是由文件级信息确定的稳定锚点。
        后续所有案例内实体都通过关系连接到这张图里。
        """

        properties = {
            "案例编号": self.document.case_number,
            "案例名称": self.document.case_title,
            "案例类型": self.document.table_summary.get("备注", self.document.source_type),
            "合规领域": self.document.compliance_domain,
            "案例摘要": self.document.table_summary.get("案例要点", ""),
            "关键词": self.document.keywords,
            "数据来源类型": self.document.source_type,
            "原文全文": self.document.full_text,
            "source_path": self.document.source_path,
        }
        return self.add_node(self.document.case_id, "案例", properties, source="parser")

    def add_llm_result(self, llm_result: dict[str, Any]) -> None:
        """把 LLM 抽取结果合并进图谱。

        处理顺序必须先节点后关系：
        - 先注册所有节点和别名；
        - 再解析关系端点并建边。
        如果先建边，关系端点很可能还找不到 node_id。
        """

        for entity in llm_result.get("entities", []) or []:
            name = str(entity.get("name") or "").strip()
            node_type = str(entity.get("type") or "").strip()
            if not name or not node_type:
                self.stats.week_warning += 1
                continue
            if node_type == "风险点 / 合规指标":
                node_type = "风险点"
            if node_type not in NODE_TYPES:
                self.stats.strong_warning += 1
                continue
            properties = entity.get("properties") or {}
            if node_type == "案例":
                # 一个文件只能有一个案例主实体，所以 LLM 的案例实体只用于补属性。
                self.add_node(self.document.case_id, "案例", self._case_properties_from_llm(name, properties), source="llm")
            else:
                self.add_node(name, node_type, properties, source="llm")

        self.add_structured_fallback_nodes()

        for relation in llm_result.get("relations", []) or []:
            props = relation.get("properties") or {}
            # 优先使用属性里的“主体/谓词/客体”，因为 prompt 明确要求关系保留这些字段。
            source_name = str(props.get("主体") or relation.get("source") or "").strip()
            target_name = str(props.get("客体") or relation.get("target") or "").strip()
            relation_type = str(props.get("谓词") or relation.get("type") or "").strip()
            if not source_name or not target_name or not relation_type:
                self.stats.week_warning += 1
                continue
            source_id = self._resolve_node_id(source_name)
            target_id = self._resolve_node_id(target_name)
            if not source_id or not target_id:
                self.stats.strong_warning += 1
                continue
            self.add_edge(source_id, target_id, relation_type, props, source="llm")

        self.add_structured_fallback_edges()

    def _case_properties_from_llm(self, name: str, properties: dict[str, Any]) -> dict[str, Any]:
        """整理 LLM 案例实体属性，用于合并到 parser 案例主节点。"""

        merged = dict(properties or {})
        merged.setdefault("案例名称", name if name != self.document.case_id else self.document.case_title)
        merged.setdefault("案例编号", self.document.case_number)
        merged.setdefault("数据来源类型", self.document.source_type)
        merged.setdefault("原文全文", self.document.full_text)
        return merged

    def add_structured_fallback_nodes(self) -> None:
        """用 parser 已识别的结构字段补齐 LLM 容易漏掉的关键节点。

        这不是替代 LLM 抽取，而是把原文件中已经有明确标签的“结论、案例启示、
        处置方案、风险点、法律依据”等内容兜底转成图谱节点，避免关键结构缺失。
        """

        fields = self.document.fields
        if not self._has_node_type("案例结果"):
            conclusion = (
                _first_text(fields, ("结论",))
                or _extract_result_sentence(self.document.full_text)
                or str(self.document.table_summary.get("案例要点", "")).strip()
            )
            if conclusion:
                self.stats.week_warning += 1
                self.add_node(
                    "处理结果1",
                    "案例结果",
                    {
                        "裁决决定": _short_text(conclusion, 120),
                        "裁判内容": conclusion,
                    },
                    source="parser",
                )
        if not self._has_node_type("案例启示"):
            insight = (
                _first_text(fields, ("案例启示",))
                or str(self.document.table_summary.get("案例要点", "")).strip()
                or _first_text(fields, ("风险点", "处置方案"))
            )
            if insight:
                self.stats.week_warning += 1
                self.add_node(
                    "案例启示1",
                    "案例启示",
                    {
                        "启示提示": insight,
                        "启示类型": "案例启示" if fields.get("案例启示") else "案例总结",
                    },
                    source="parser",
                )
        if not self._has_node_type("处置方案"):
            disposal = _first_text(fields, ("处置方案",))
            if disposal:
                self.stats.week_warning += 1
                self.add_node(
                    "处置方案1",
                    "处置方案",
                    {
                        "处置方案内容": disposal,
                        "处置类型": "处置方案",
                    },
                    source="parser",
                )
        if not self._has_node_type("风险点"):
            risk_point = _first_text(fields, ("风险点",))
            if risk_point:
                self.stats.week_warning += 1
                self.add_node(
                    "风险点1",
                    "风险点",
                    {
                        "风险点原文": risk_point,
                        "风险类型": _short_text(risk_point, 80),
                    },
                    source="parser",
                )
        if not self._has_node_type("案例分析"):
            analysis = _first_text(fields, ("案例分析", "分析"))
            if analysis:
                self.stats.week_warning += 1
                self.add_node(
                    "分析1",
                    "案例分析",
                    {
                        "分析编号": "分析1",
                        "分析过程": analysis,
                        "分析结论": _short_text(analysis, 120),
                    },
                    source="parser",
                )
        if not self._has_node_type("法规条款依据"):
            legal_basis = _first_text(fields, ("法律依据",))
            if legal_basis:
                self.stats.week_warning += 1
                for index, item in enumerate(_split_legal_basis_items(legal_basis), 1):
                    self.add_node(
                        f"法规依据{index}",
                        "法规条款依据",
                        {
                            "法规名称": _extract_regulation_name(item),
                            "条款编号": _extract_clause_number(item),
                            "法规条款摘要": item,
                        },
                        source="parser",
                    )

    def add_structured_fallback_edges(self) -> None:
        """为 parser 兜底节点建立 schema 允许的最小关系。"""

        case_id = self.name_to_id.get(self.document.case_id, "")
        for node_id in self._node_ids_by_type("案例分析"):
            self.add_edge(case_id, node_id, "引审出", {"证据文本": self._node_text(node_id)}, source="parser")
        for node_id in self._node_ids_by_type("案例启示"):
            self.add_edge(case_id, node_id, "形成", {"证据文本": self._node_text(node_id)}, source="parser")
        for node_id in self._node_ids_by_type("风险点"):
            self.add_edge(case_id, node_id, "关联", {"证据文本": self._node_text(node_id)}, source="parser")
        for node_id in self._node_ids_by_type("处置方案"):
            self.add_edge(case_id, node_id, "关联", {"证据文本": self._node_text(node_id)}, source="parser")

        analysis_ids = self._node_ids_by_type("案例分析")
        regulation_ids = self._node_ids_by_type("法规条款依据")
        result_ids = self._node_ids_by_type("案例结果")
        insight_ids = self._node_ids_by_type("案例启示")
        behavior_ids = self._node_ids_by_type("行为")
        subject_ids = self._node_ids_by_type("案例主体")
        if analysis_ids:
            analysis_id = analysis_ids[0]
            for node_id in regulation_ids:
                self.add_edge(analysis_id, node_id, "依据", {"证据文本": self._node_text(node_id)}, source="parser")
            for node_id in result_ids:
                self.add_edge(analysis_id, node_id, "得出", {"证据文本": self._node_text(node_id)}, source="parser")
            for node_id in insight_ids:
                self.add_edge(analysis_id, node_id, "形成", {"证据文本": self._node_text(node_id)}, source="parser")
            for node_id in behavior_ids:
                self.add_edge(analysis_id, node_id, "包含", {"证据文本": self._node_text(node_id)}, source="parser")
        if behavior_ids:
            behavior_id = behavior_ids[0]
            for node_id in result_ids:
                self.add_edge(behavior_id, node_id, "导致", {"证据文本": self._node_text(node_id)}, source="parser")
            for node_id in regulation_ids:
                self.add_edge(behavior_id, node_id, "违反", {"证据文本": self._node_text(node_id)}, source="parser")
        if subject_ids and result_ids:
            self.add_edge(subject_ids[0], result_ids[0], "承担", {"证据文本": self._node_text(result_ids[0])}, source="parser")

    def add_node(
        self,
        node_name: str,
        node_type: str,
        properties: dict[str, Any] | None = None,
        source: str = "llm",
    ) -> str:
        """新增或合并节点。

        去重规则：
        - 案例：永远合并到 document.case_id 对应的唯一主节点。
        - 法规条款依据：按“法规名称 + 条款编号”合并。
        - 其他实体：按“实体类型 + 实体名称”合并。
        """

        node_name = str(node_name or "").strip()
        node_type = str(node_type or "").strip()
        if not node_name or not node_type:
            return ""
        if node_type == "风险点 / 合规指标":
            # 任务报告中写作“风险点 / 合规指标”，代码中统一为更短的“风险点”。
            node_type = "风险点"
        if node_type == "案例":
            key = ("案例", self.document.case_id)
            node_name = self.document.case_id
        elif node_type == "法规条款依据":
            key = (node_type, self._regulation_key(node_name, properties or {}))
        else:
            key = (node_type, node_name)

        cleaned_properties = self._normalize_node_properties(node_type, properties or {})
        if key in self.nodes:
            # 已存在节点时只合并属性和来源，不新建节点。
            if node_type == "案例":
                self.nodes[key]["properties"].update(
                    {
                        prop_key: prop_value
                        for prop_key, prop_value in cleaned_properties.items()
                        if prop_key not in CASE_PROTECTED_PROPERTIES
                    }
                )
            else:
                self.nodes[key]["properties"].update(cleaned_properties)
            self.nodes[key]["source"] = _merge_source_label(self.nodes[key].get("source", ""), source)
            self._register_aliases(self.nodes[key]["node_id"], node_name, node_type, cleaned_properties)
            return self.nodes[key]["node_id"]

        node_id = self._make_node_id(node_type, key[1])
        node = {
            "node_id": node_id,
            "node_name": node_name,
            "node_type": node_type,
            "properties": cleaned_properties,
            "case_id": self.document.case_id,
            "filename": Path(self.document.source_path).name,
            "source": source,
        }
        self.nodes[key] = node
        # 注册别名后，关系里的“主体/客体”才能解析到 node_id。
        self._register_aliases(node_id, node_name, node_type, cleaned_properties)
        return node_id

    def _normalize_node_properties(self, node_type: str, properties: dict[str, Any]) -> dict[str, Any]:
        """按节点类型规范属性。

        这里是图谱入库前的最后一道结构化约束。尤其是“风险点.风险类型”：
        该字段会进入下游 risk_types，只能使用项目受控词表；模型抽到的细分
        标签保存在“风险类型原文”，避免污染受控字段。
        """

        cleaned_properties = _clean_properties(properties or {})
        if node_type == "风险点":
            self._normalize_risk_point_properties(cleaned_properties)
        return cleaned_properties

    def _normalize_risk_point_properties(self, properties: dict[str, Any]) -> None:
        """将风险点的“风险类型”收敛到六类受控风险类型。"""

        raw_value = str(properties.get("风险类型") or "").strip()
        candidates: list[Any] = [
            raw_value,
            properties.get("风险点原文"),
            properties.get("指标路径"),
            properties.get("指标名称"),
            self.document.compliance_domain,
            *self.document.keywords,
        ]

        mapped = ""
        for candidate in candidates:
            text = str(candidate or "").strip()
            if not text:
                continue
            if text in CONTROLLED_RISK_TYPES:
                mapped = text
                break
            mapped = _map_to_controlled_risk_type(text)
            if mapped:
                break

        if raw_value and raw_value != mapped:
            properties.setdefault("风险类型原文", raw_value)
        if mapped:
            properties["风险类型"] = mapped
        else:
            if raw_value:
                self.stats.week_warning += 1
            properties.pop("风险类型", None)

    def add_edge(
        self,
        source_id: str,
        target_id: str,
        relation_type: str,
        properties: dict[str, Any] | None = None,
        source: str = "llm",
    ) -> None:
        """新增或合并关系边。

        同一 source_id、target_id、relation_type 视为同一条边；
        如果重复出现，只合并属性和 source 标记。
        """

        if not source_id or not target_id or not relation_type:
            return
        if source_id == target_id:
            self.stats.week_warning += 1
            return
        source_type = self._node_type(source_id)
        target_type = self._node_type(target_id)
        if (source_type, relation_type, target_type) not in ALLOWED_EDGE_TRIPLES:
            self.stats.strong_warning += 1
            return
        cleaned_properties = _clean_properties(properties or {})
        cleaned_properties.setdefault("主体", self._node_name(source_id))
        cleaned_properties.setdefault("谓词", relation_type)
        cleaned_properties.setdefault("客体", self._node_name(target_id))
        cleaned_properties.setdefault("证据文本", self._find_evidence_text(source_id, target_id))
        for edge in self.edges:
            if (
                edge.get("source_id") == source_id
                and edge.get("target_id") == target_id
                and edge.get("relation_type") == relation_type
            ):
                edge["properties"].update(cleaned_properties)
                edge["source"] = _merge_source_label(edge.get("source", ""), source)
                return
        self.edges.append(
            {
                "edge_id": f"edge_{generate_hex_uuid()}",
                "source_id": source_id,
                "target_id": target_id,
                "relation_type": relation_type,
                "directionality": "单向",
                "properties": cleaned_properties,
                "case_id": self.document.case_id,
                "filename": Path(self.document.source_path).name,
                "source": source,
            }
        )

    def _resolve_node_id(self, value: str) -> str:
        """把关系端点名称解析为 node_id。

        LLM 关系端点可能写：
        - case_id；
        - 案例标题；
        - 案例编号；
        - 实体原名；
        - 类型_实体名。

        因此这里需要用 name_to_id 做多种别名解析。
        """

        key = str(value or "").strip()
        if key in {self.document.case_id, self.document.case_title, self.document.case_number, "案例"}:
            return self.name_to_id.get(self.document.case_id, "")
        return self.name_to_id.get(key) or self.name_to_id.get(key.split("_", 1)[-1], "")

    def _register_aliases(
        self,
        node_id: str,
        node_name: str,
        node_type: str,
        properties: dict[str, Any],
    ) -> None:
        """为节点注册可用于关系解析的别名。

        例如法规节点可能被 LLM 在关系里写成：
        - 《中华人民共和国刑法》第三百八十五条
        - 法规条款依据_《中华人民共和国刑法》第三百八十五条
        - 《中华人民共和国刑法》 第三百八十五条

        多注册几个别名可以减少关系端点匹配失败。
        """

        aliases = {
            node_name,
            f"{node_type}_{node_name}",
            properties.get("名称", ""),
            properties.get("主体名称", ""),
            properties.get("案例名称", ""),
            properties.get("法规名称", ""),
        }
        if node_type == "案例":
            aliases.update({self.document.case_id, self.document.case_title, self.document.case_number, "案例"})
        if node_type == "法规条款依据":
            regulation = properties.get("法规名称", "")
            clause = properties.get("条款编号", "")
            if regulation and clause:
                aliases.add(f"{regulation}{clause}")
                aliases.add(f"{regulation}{' '}{clause}")
        for alias in aliases:
            alias = str(alias or "").strip()
            if alias:
                self.name_to_id[alias] = node_id

    def _has_node_type(self, node_type: str) -> bool:
        return any(node.get("node_type") == node_type for node in self.nodes.values())

    def _node_ids_by_type(self, node_type: str) -> list[str]:
        return [node["node_id"] for node in self.nodes.values() if node.get("node_type") == node_type]

    def _node_type(self, node_id: str) -> str:
        node = self._node_by_id(node_id)
        return str(node.get("node_type", "")) if node else ""

    def _node_name(self, node_id: str) -> str:
        node = self._node_by_id(node_id)
        return str(node.get("node_name", "")) if node else ""

    def _node_text(self, node_id: str) -> str:
        node = self._node_by_id(node_id)
        if not node:
            return ""
        properties = node.get("properties", {}) or {}
        for key in (
            "裁判内容",
            "启示提示",
            "处置方案内容",
            "风险点原文",
            "分析过程",
            "法规条款摘要",
            "行为描述",
        ):
            value = str(properties.get(key, "")).strip()
            if value:
                return value
        return str(node.get("node_name", "")).strip()

    def _node_by_id(self, node_id: str) -> dict[str, Any] | None:
        for node in self.nodes.values():
            if node.get("node_id") == node_id:
                return node
        return None

    def _find_evidence_text(self, source_id: str, target_id: str) -> str:
        source_text = self._node_text(source_id)
        target_text = self._node_text(target_id)
        for text in (target_text, source_text):
            snippet = _find_sentence_containing(self.document.full_text, text)
            if snippet:
                return snippet
        return _short_text(target_text or source_text, 160)

    def _make_node_id(self, node_type: str, key: str) -> str:
        """生成节点 ID。

        - 案例节点使用稳定 case_id。
        - 法规节点使用 regulation_ + 可读 key，便于跨案例合并。
        - 其他节点使用类型 + UUID，避免复杂事件长文本进入 ID。
        """

        if node_type == "案例":
            return self.document.case_id
        if node_type == "法规条款依据":
            return f"regulation_{_slugify(key)}"
        return f"{node_type}_{generate_hex_uuid()}"

    @staticmethod
    def _regulation_key(node_name: str, properties: dict[str, Any]) -> str:
        """生成法规条款依据的合并键。"""

        regulation = str(properties.get("法规名称") or "").strip()
        clause = str(properties.get("条款编号") or "").strip()
        return f"{regulation}{clause}" if regulation or clause else node_name

    def to_graph(self) -> dict[str, Any]:
        """输出最终图谱结构。"""

        return {"nodes": list(self.nodes.values()), "edges": self.edges}


async def extract_compliance_case_graph(
    input_path: str | Path,
    output_path: str | Path | None = None,
    enable_llm: bool = True,
) -> dict[str, Any]:
    """抽取单个案例文件，并可选写入 JSON 文件。"""

    extractor = ComplianceCaseGraphExtractor(enable_llm=enable_llm)
    result = await extractor.extract(input_path)
    if output_path:
        output = Path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return result


async def extract_compliance_case_graph_batch(
    input_root_dir: str | Path,
    output_dir: str | Path,
    enable_llm: bool = True,
) -> dict[str, Any]:
    """批量抽取目录下的合规案例文件。

    输出内容：
    - 每个输入文件一个独立 JSON 结果；
    - node.json：所有文件节点汇总；
    - edge.json：所有文件关系汇总；
    - case_knowledge.json：所有 case_knowledge 汇总；
    - summary.json：批处理统计信息。
    """

    input_files = discover_compliance_case_files(input_root_dir)
    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    extractor = ComplianceCaseGraphExtractor(enable_llm=enable_llm)
    summary: dict[str, Any] = {
        "input_root_dir": str(input_root_dir),
        "output_dir": str(output_root),
        "total": len(input_files),
        "success": 0,
        "failed": 0,
        "items": [],
    }
    all_nodes: list[dict[str, Any]] = []
    all_edges: list[dict[str, Any]] = []
    all_case_knowledge: list[dict[str, Any]] = []
    for input_file in input_files:
        output_path = output_root / f"{_safe_filename(input_file.stem)}.json"
        try:
            # 逐文件抽取和落盘，避免某个文件失败导致前面结果丢失。
            result = await extractor.extract(input_file)
            output_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
            if result.get("status") == "success":
                summary["success"] += 1
            else:
                summary["failed"] += 1
            graph = result.get("graph", {}) or {}
            all_nodes.extend(graph.get("nodes", []) or [])
            all_edges.extend(graph.get("edges", []) or [])
            if result.get("case_knowledge"):
                all_case_knowledge.append(result["case_knowledge"])
            summary["items"].append(
                {
                    "input_path": str(input_file),
                    "output_path": str(output_path),
                    "status": result.get("status"),
                    "node_count": result.get("stats", {}).get("node_count", 0),
                    "edge_count": result.get("stats", {}).get("edge_count", 0),
                }
            )
        except Exception as exc:
            # 批处理不中断，错误写入 summary，方便后续单独重跑失败文件。
            summary["failed"] += 1
            summary["items"].append({"input_path": str(input_file), "status": "failed", "error": str(exc)})

    (output_root / "node.json").write_text(json.dumps(all_nodes, ensure_ascii=False, indent=2), encoding="utf-8")
    (output_root / "edge.json").write_text(json.dumps(all_edges, ensure_ascii=False, indent=2), encoding="utf-8")
    (output_root / "case_knowledge.json").write_text(
        json.dumps(all_case_knowledge, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (output_root / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def _clean_properties(properties: dict[str, Any]) -> dict[str, Any]:
    """删除空属性，减少 node.json/edge.json 噪声。"""

    return {key: value for key, value in properties.items() if value not in (None, "", [], {})}


def _first_text(mapping: dict[str, str], keys: tuple[str, ...]) -> str:
    """按候选字段顺序取第一个非空文本。"""

    for key in keys:
        value = str(mapping.get(key, "")).strip()
        if value:
            return value
    return ""


def _short_text(value: str, limit: int) -> str:
    """压缩为适合节点摘要属性的短文本。"""

    normalized = re.sub(r"\s+", " ", str(value or "")).strip()
    if len(normalized) <= limit:
        return normalized
    return normalized[:limit].rstrip() + "..."


def _split_legal_basis_items(value: str) -> list[str]:
    """把法律依据字段拆成若干条候选法规依据。"""

    normalized = re.sub(r"\s+", " ", str(value or "")).strip()
    if not normalized:
        return []
    parts = re.split(r"[；;。]\s*", normalized)
    return [part.strip() for part in parts if part.strip()][:12]


def _extract_regulation_name(value: str) -> str:
    """从依据文本中提取法规名称。"""

    match = re.search(r"《[^》]+》", value)
    return match.group(0) if match else _short_text(value, 60)


def _extract_clause_number(value: str) -> str:
    """从依据文本中提取条款编号。"""

    matches = re.findall(r"第[一二三四五六七八九十百千万零〇\d]+条(?:第[一二三四五六七八九十百千万零〇\d]+款)?", value)
    return "、".join(matches)


def _find_sentence_containing(full_text: str, needle: str) -> str:
    """在原文中找包含节点核心短语的证据句。"""

    needle = _short_text(needle, 30)
    if not full_text or not needle:
        return ""
    keyword = re.sub(r"\s+", "", needle)
    if not keyword:
        return ""
    compact_text = re.sub(r"\s+", "", full_text)
    if keyword not in compact_text:
        return ""
    sentences = re.split(r"(?<=[。！？!?；;])\s*", full_text)
    for sentence in sentences:
        if keyword in re.sub(r"\s+", "", sentence):
            return _short_text(sentence, 180)
    index = compact_text.find(keyword)
    if index < 0:
        return ""
    return _short_text(full_text[max(0, index - 60) : index + len(needle) + 60], 180)


def _extract_result_sentence(full_text: str) -> str:
    """从全文中抽取最像案例处理结果的句子。"""

    if not full_text:
        return ""
    result_keywords = (
        "判决",
        "判处",
        "处罚",
        "罚款",
        "没收",
        "赔偿",
        "承担",
        "追究",
        "裁定",
        "处理",
        "整改",
        "被诉",
        "败诉",
        "胜诉",
    )
    sentences = re.split(r"(?<=[。！？!?；;])\s*", full_text)
    for sentence in sentences:
        compact = re.sub(r"\s+", "", sentence)
        if any(keyword in compact for keyword in result_keywords):
            return _short_text(sentence, 240)
    return ""


def _merge_source_label(existing: str, new: str) -> str:
    """合并节点或边的来源标记，如 parser+llm。"""

    if not existing:
        return new
    if not new or new in existing.split("+"):
        return existing
    return f"{existing}+{new}"


def _slugify(value: str) -> str:
    """把中文/英文混合字符串转为适合放进 ID 的短文本。"""

    value = re.sub(r"\s+", "", str(value or ""))
    value = re.sub(r"[^\w一-龥.-]+", "_", value)
    return value[:80] or generate_hex_uuid()


def _safe_filename(value: str) -> str:
    """清理 Windows 文件名非法字符。"""

    return re.sub(r'[<>:"/\\|?*]+', "_", str(value or "")).strip() or generate_hex_uuid()


if __name__ == "__main__":
    # 命令行入口主要用于本地测试和批量离线抽取。
    # 示例：
    # python graph_extract.py data/合规案例库/案例1-1.md --output output/案例1-1.json
    # python graph_extract.py data --output output --batch --no-llm
    import argparse

    parser = argparse.ArgumentParser(description="Extract compliance case graph in single-stage mode.")
    parser.add_argument("input", help="Input case file or directory")
    parser.add_argument("--output", default="output/compliance_case_v1", help="Output file or directory")
    parser.add_argument("--batch", action="store_true", help="Treat input as directory and run batch extraction")
    parser.add_argument("--no-llm", action="store_true", help="Only run parser/context graph without calling LLM")
    args = parser.parse_args()

    if args.batch:
        asyncio.run(extract_compliance_case_graph_batch(args.input, args.output, enable_llm=not args.no_llm))
    else:
        asyncio.run(extract_compliance_case_graph(args.input, args.output, enable_llm=not args.no_llm))
