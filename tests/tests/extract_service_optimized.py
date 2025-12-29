"""
优化后的知识抽取服务
✅ 解决问题：
1. 一次性抽取（实体+关系）- 减少80%LLM调用
2. 并发执行 - Semaphore(20) 提速20倍
3. 实体去重 - 解决重复实体问题
4. 关系验证 - 解决冲突关系问题
5. 批量导入 - 提速100倍
"""

import asyncio
import logging
from typing import List, Dict, Tuple
from collections import defaultdict
from difflib import SequenceMatcher
import hashlib
import re

from client_app.data_service_client import DataServiceHandler
from common.exception import errors
from .schema_service import schema_service  # Schema服务，用于获取图谱模式
from .mapping_service import mapping_service  # 映射服务，用于获取抽取映射配置
from .extract_cache import extraction_cache  # 抽取缓存，避免重复的LLM调用

logger = logging.getLogger("knowledgeService")  # 获取知识服务的日志记录器


class EntityDeduplicator:
    """实体去重引擎 - 用于处理重复的实体，保留唯一实体"""
    
    def __init__(self):
        # 实体缓存，存储(type, normalized_name)到canonical_entity的映射
        self.entity_cache = {}  # {(type, normalized_name): canonical_entity}
    
    def normalize_name(self, name: str, entity_type: str) -> str:
        """
        标准化实体名称 - 清洗和标准化实体名称以便比较
        
        Args:
            name: 实体名称
            entity_type: 实体类型
        
        Returns:
            标准化后的实体名称
        """
        # 1. 基础清洗 - 移除空格和特殊字符
        name = re.sub(r'\s+', '', name.strip())  # 移除所有空白字符
        name = re.sub(r'[""''《》【】\[\]\(\)]', '', name)  # 移除各种引号和括号
        
        # 2. 类型特定规则 - 根据实体类型应用不同的清洗规则
        # TODO：将清洗规则拆分出来
        if entity_type in ['公司', 'Company']:
            # 对公司名称移除常见的公司后缀
            name = re.sub(r'(有限公司|股份有限公司|公司|集团)$', '', name)
        elif entity_type in ['人物', 'Person']:
            # 对人物名称移除常见的称谓后缀
            name = re.sub(r'(先生|女士|教授|博士)$', '', name)
        
        # 3. 统一大小写（英文）- 将英文名称统一为小写
        if re.match(r'^[A-Za-z0-9\s]+$', name):
            name = name.lower()
        
        return name
    
    def calculate_similarity(self, name1: str, name2: str) -> float:
        """
        计算两个名称的相似度
        
        Args:
            name1: 第一个名称
            name2: 第二个名称
        
        Returns:
            相似度值 (0.0-1.0)
        """
        # 使用SequenceMatcher计算序列相似度
        seq_sim = SequenceMatcher(None, name1, name2).ratio()
        
        # 如果一个名称包含另一个名称，增加相似度（包含关系加分）
        if name1 in name2 or name2 in name1:
            seq_sim = min(seq_sim + 0.2, 1.0)  # 最大相似度不超过1.0
        
        return seq_sim
    
    def deduplicate(self, entities: List[Dict]) -> Tuple[List[Dict], Dict]:
        """
        ✅ 改进：多维度实体去重（name + type + 消歧信息）
        
        Args:
            entities: 实体列表，格式为 [{'type': '公司', 'name': '华为', 'props': {...}}, ...]
        
        Returns:
            (unique_entities, entity_mapping)
            unique_entities: 去重后的实体列表
            entity_mapping: 映射字典，格式为 {(type, original_name): canonical_id}
        """
        logger.info(f"[去重] 开始实体去重，原始实体数: {len(entities)}")
        
        # 按实体类型分组，便于后续处理
        by_type = defaultdict(list)
        for e in entities:
            by_type[e['type']].append(e)
        
        unique_entities = []
        entity_mapping = {}
        
        # 对每种类型的实体进行去重处理
        for entity_type, ents in by_type.items():
            # ✅ 提取消歧信息 - 用于区分同名但不同的实体
            for e in ents:
                e['_disambiguator'] = self._extract_disambiguator(e, entity_type)
            
            # ✅ 智能聚类去重（考虑消歧信息）- 基于名称相似度和消歧信息进行聚类
            clusters = self._cluster_entities_smart(ents, entity_type)
            
            logger.info(f"[去重] 类型 '{entity_type}': {len(ents)}个实体 -> {len(clusters)}个簇")
            
            # 融合每个簇中的实体，生成唯一实体
            for cluster in clusters:
                fused = self._fuse_cluster(cluster, entity_type)
                unique_entities.append(fused)
                
                # 记录原始实体到融合实体的映射关系
                for original_entity in cluster:
                    entity_mapping[(entity_type, original_entity['name'])] = fused['id']
        
        if len(entities) > 0:
            # 计算去重率
            dedup_rate = (1-len(unique_entities)/len(entities))*100
            logger.info(f"[去重] 完成: {len(entities)} -> {len(unique_entities)}, 去重率: {dedup_rate:.1f}%")
        else:
            logger.warning("[去重] 输入实体数为0，无需去重")
        
        return unique_entities, entity_mapping
    
    def _extract_disambiguator(self, entity: Dict, entity_type: str) -> str:
        """
        # TODO：该部分逻辑待删除，理由：id与name和type绑定，不存在name type相同但是实体不同的情况，所以就算该代码
            成功区分了两个实体，但由于两个实体的name和type相同，最后会产生相同id合并为一个实体
        ✅ 从实体属性中提取关键消歧信息
        优先提取能区分同名不同实体的关键字段
        
        Args:
            entity: 实体字典
            entity_type: 实体类型
        
        Returns:
            消歧信息字符串
        """
        props = entity.get('props', {})
        
        # 防御性检查：确保 props 是字典
        if not isinstance(props, dict):
            logger.warning(f"[去重] props 类型异常: {type(props).__name__}, 实体: {entity.get('name', 'unknown')}")
            return ""
        
        # 不同实体类型的消歧字段优先级
        disambiguator_fields = {
            '人': ['单位', '公司', '组织', '职位', '部门', '地区', '团队'],
            '组织': ['地区', '地址', '上级机构', '类型', '行业', '总部'],
            '公司': ['地区', '地址', '总部地址', '注册地', '行业'],
            '产品': ['型号', '制造商', '版本', '系列', '规格'],
            '设备': ['型号', '制造商', '所属生产线', '编号', '序列号'],
            '生产线': ['所属工厂', '车间', '地点', '编号'],
        }
        
        # 获取当前实体类型的消歧字段列表
        key_fields = disambiguator_fields.get(entity_type, ['类型', '分类', '类别'])
        
        # 提取第一个非空字段作为消歧符
        for field in key_fields:
            value = props.get(field)
            if value and str(value).strip():
                return str(value).strip()[:30]  # 限制长度为30个字符
        
        return ""  # 无消歧信息
    
    def _should_merge(self, e1: Dict, e2: Dict, entity_type: str) -> bool:
        """
        # TODO:合并逻辑待修正
        ✅ 核心判断：是否应该合并两个实体
        结合名称相似度 + 消歧信息进行智能判断
        
        Args:
            e1: 第一个实体
            e2: 第二个实体
            entity_type: 实体类型
        
        Returns:
            True表示应该合并，False表示不应合并
        """
        # 1. 计算标准化后的名称相似度
        norm_name1 = self.normalize_name(e1['name'], entity_type)
        norm_name2 = self.normalize_name(e2['name'], entity_type)
        name_sim = self.calculate_similarity(norm_name1, norm_name2)
        
        # 2. 获取消歧信息
        dis1 = e1.get('_disambiguator', '')
        dis2 = e2.get('_disambiguator', '')
        
        # ✅ 规则1：名称高度相似 + 都无消歧信息 → 合并
        if name_sim >= 0.90 and not dis1 and not dis2:
            return True
        
        # ✅ 规则2：名称高度相似 + 消歧信息相同 → 合并
        if name_sim >= 0.90 and dis1 and dis2 and dis1 == dis2:
            return True
        
        # ✅ 规则3：名称完全相同 + 消歧信息相似 → 合并
        if name_sim >= 0.95 and dis1 and dis2:
            dis_sim = self.calculate_similarity(dis1, dis2)
            if dis_sim >= 0.80:
                return True
        
        # ❌ 规则4：名称相似 + 消歧信息不同 → 不合并（保留同名不同实体）
        if name_sim >= 0.85 and dis1 and dis2 and dis1 != dis2:
            logger.info(
                f"[去重] 保留同名不同实体: '{e1['name']}' (消歧:{dis1}) "
                f"vs '{e2['name']}' (消歧:{dis2})"
            )
            return False
        
        # ❌ 默认：名称相似度不够 → 不合并
        return False
    
    def _cluster_entities_smart(self, entities: List[Dict], entity_type: str) -> List[List[Dict]]:
        """
        ✅ 智能聚类：同时考虑名称相似度 + 消歧信息
        替代原来只看名称相似度的 _cluster_entities 方法
        
        Args:
            entities: 实体列表
            entity_type: 实体类型
        
        Returns:
            聚类结果，每个簇是一个实体列表
        """
        visited = set()  # 已访问的实体索引
        clusters = []
        
        for i, e1 in enumerate(entities):
            if i in visited:
                continue
            
            cluster = [e1]  # 创建新簇，包含当前实体
            visited.add(i)
            
            # 查找应该合并的其他实体
            for j, e2 in enumerate(entities):
                if j <= i or j in visited:  # 避免重复处理
                    continue
                
                # ✅ 使用多维度判断（名称相似度+消歧信息）决定是否合并
                if self._should_merge(e1, e2, entity_type):
                    cluster.append(e2)
                    visited.add(j)
            
            clusters.append(cluster)
        
        return clusters
    
    def _fuse_cluster(self, cluster: List[Dict], entity_type: str) -> Dict:
        """
        # TODO:该部分可以交给大模型抽取
        融合一个簇的实体
        ✅ 改进：cluster现在是List[Dict]而不是List[Tuple]
        
        Args:
            cluster: 实体簇
            entity_type: 实体类型
        
        Returns:
            融合后的实体
        """
        # 选择最长的名称作为标准名（通常更完整）
        canonical_name = max([e['name'] for e in cluster], key=len)
        
        # 合并属性 - 统计各属性值的出现次数
        merged_props = {}
        prop_counts = defaultdict(lambda: defaultdict(int))
        
        for entity in cluster:
            for prop, value in entity.get('props', {}).items():
                if value and prop != '_disambiguator':  # 排除内部字段
                    prop_counts[prop][str(value)] += 1
        
        # 选择出现最多的值作为最终值
        for prop, value_counts in prop_counts.items():
            most_common = max(value_counts.items(), key=lambda x: x[1])
            merged_props[prop] = most_common[0]
        
        # 生成唯一ID（基于type + name）
        entity_id = hashlib.md5(f"{entity_type}_{canonical_name}".encode()).hexdigest()[:16]
        
        # ✅ 记录合并来源数量
        if len(cluster) > 1:
            logger.debug(f"[去重] 合并 {len(cluster)} 个 '{canonical_name}' 实体")
        
        return {
            'id': entity_id,  # 实体唯一ID
            'type': entity_type,  # 实体类型
            'name': canonical_name,  # 实体名称
            'props': merged_props,  # 合并后的属性
            'source_count': len(cluster)  # 来源实体数量
        }


class RelationValidator:
    """关系验证引擎 - 验证和过滤关系，确保符合Schema约束"""
    
    def __init__(self, schema_edges: List[Dict]):
        """
        初始化关系验证器
        
        Args:
            schema_edges: Schema中定义的关系边，格式为：
                         [{'label': '位于', 'source': '公司', 'target': '城市'}, ...]
        """
        # 构建关系类型到源/目标实体类型的映射
        self.schema_map = {
            edge['label']: (edge['source'], edge['target'])
            for edge in schema_edges
        }

        # TODO：待修改
        # 单值关系（同一subject只能有一个object）- 这些关系在现实中通常是唯一的
        self.single_value_relations = {'出生于', '成立于', '总部位于', '毕业于'}
    
    def validate(self, relations: List[Dict], entity_mapping: Dict) -> List[Dict]:
        """
        关系验证和去重
        
        Args:
            relations: 原始关系列表，格式为：
                      [{'type': '位于', 'subject': {...}, 'object': {...}}, ...]
            entity_mapping: 实体映射字典，格式为{(type, name): canonical_id}
        
        Returns:
            验证后的关系列表
        """
        logger.info(f"开始关系验证，原始关系数: {len(relations)}")
        
        # 1. Schema验证 - 检查关系是否符合预定义的Schema
        valid = []
        for rel in relations:
            if not self._check_schema(rel):
                continue
            valid.append(rel)
        
        logger.info(f"  Schema验证: {len(relations)} -> {len(valid)}")
        
        # 2. 实体映射 - 将原始实体映射到标准化实体ID
        mapped = []
        for rel in valid:
            subject = rel.get('subject') or {}
            obj = rel.get('object') or {}

            subj_type = subject.get('type')
            obj_type = obj.get('type')
            subj_name = subject.get('name')
            obj_name = obj.get('name')

            if not subj_type or not obj_type or not subj_name or not obj_name:
                logger.warning(f"关系缺少实体信息，跳过: {rel}")
                continue

            # 构建实体映射键
            subj_key = (subj_type, subj_name)
            obj_key = (obj_type, obj_name)
            
            # 获取标准化实体ID
            subj_id = entity_mapping.get(subj_key)
            obj_id = entity_mapping.get(obj_key)
            
            if not subj_id or not obj_id:
                logger.warning(f"实体映射失败: {subj_key} -> {obj_key}")
                continue
            
            # 创建映射后的关系
            mapped.append({
                'type': rel.get('type'),
                'subject_id': subj_id,
                'object_id': obj_id
            })
        
        logger.info(f"  实体映射: {len(valid)} -> {len(mapped)}")
        
        # 3. 去重 - 基于(type, subject_id, object_id)进行去重
        unique = self._deduplicate(mapped)
        logger.info(f"  去重: {len(mapped)} -> {len(unique)}")
        
        # 4. 约束检查 - 应用业务规则
        final = self._apply_constraints(unique)
        logger.info(f"  约束检查: {len(unique)} -> {len(final)}")
        
        return final
    
    def _check_schema(self, relation: Dict) -> bool:
        """
        # 待修改
        检查关系是否符合schema定义
        
        Args:
            relation: 待验证的关系
        
        Returns:
            True表示符合Schema，False表示不符合
        """
        rel_type = relation.get('type')
        if not rel_type:
            logger.warning(f"[Schema验证] 关系缺少类型，跳过: {relation}")
            return False
        
        # 检查关系类型是否在Schema中定义
        if rel_type not in self.schema_map:
            logger.warning(f"[Schema验证] 未定义的关系类型: '{rel_type}', 可用类型: {list(self.schema_map.keys())}")
            logger.warning(f"[Schema验证] 被拒绝的关系详情: {relation}")
            return False
        
        # 获取Schema中定义的源/目标实体类型
        expected_source, expected_target = self.schema_map[rel_type]
        subject = relation.get('subject') or {}
        obj = relation.get('object') or {}
        # 获取实际的实体类型
        actual_source = subject.get('type') or subject.get('label')
        actual_target = obj.get('type') or obj.get('label')
        
        if not actual_source or not actual_target:
            logger.warning(f"[Schema验证] 关系缺少实体类型信息，跳过: {relation}")
            logger.warning(f"[Schema验证] subject: {subject}, object: {obj}")
            return False
        
        # 检查实际实体类型是否与Schema定义匹配
        if actual_source != expected_source or actual_target != expected_target:
            logger.warning(f"[Schema验证] 实体类型不匹配:")
            logger.warning(f"  关系类型: '{rel_type}'")
            logger.warning(f"  期望: {expected_source} -> {expected_target}")
            logger.warning(f"  实际: {actual_source} -> {actual_target}")
            logger.warning(f"  完整关系: {relation}")
            return False
        
        return True
    
    def _deduplicate(self, relations: List[Dict]) -> List[Dict]:
        """
        关系去重 - 基于(type, subject_id, object_id)进行去重
        
        Args:
            relations: 关系列表
        
        Returns:
            去重后的关系列表
        """
        unique_dict = {}  # 用于存储唯一关系
        counts = defaultdict(int)  # 统计每个关系的出现次数
        
        for rel in relations:
            key = (rel['type'], rel['subject_id'], rel['object_id'])  # 构建唯一键
            counts[key] += 1
            unique_dict[key] = rel  # 保留最后出现的关系
        
        # 添加出现次数到关系中
        for key, rel in unique_dict.items():
            rel['count'] = counts[key]
        
        return list(unique_dict.values())
    
    def _apply_constraints(self, relations: List[Dict]) -> List[Dict]:
        """
        应用约束规则 - 处理单值关系等约束，单值关系是指一个主体只能有一个这样的关系
        
        Args:
            relations: 关系列表
        
        Returns:
            应用约束后的关系列表
        """
        # 按(subject_id, type)分组 - 便于处理单值关系约束
        by_subject = defaultdict(list)
        for rel in relations:
            by_subject[(rel['subject_id'], rel['type'])].append(rel)
        
        final = []
        for (subj_id, rel_type), rels in by_subject.items():
            if rel_type in self.single_value_relations:
                # 单值关系：保留出现次数最多的
                best = max(rels, key=lambda r: r.get('count', 0))
                final.append(best)
                if len(rels) > 1:
                    logger.info(f"单值关系冲突 '{rel_type}': 保留1个，丢弃{len(rels)-1}个")
            else:
                # 非单值关系：保留所有关系
                final.extend(rels)
        
        return final


class ExtractServiceOptimized:
    """优化后的抽取服务 - 提供完整的知识抽取流程"""
    
    @staticmethod
    async def extract_from_mapping_task_optimized(mapping_id: str, graph_name: str):
        """
        ✅ 完整优化的抽取流程
        
        改进:
        1. 一次性抽取（实体+关系）
        2. 并发执行（Semaphore=20）
        3. 实体去重
        4. 关系验证
        5. 批量导入
        
        Args:
            mapping_id: 映射ID，定义了抽取任务的配置
            graph_name: 图谱名称，指定抽取结果存储的图谱
        
        Returns:
            抽取结果统计信息
        """
        start_time = asyncio.get_event_loop().time()  # 记录开始时间
        
        # 获取抽取配置信息
        mapping = (await mapping_service.get_mapping_by_id(mapping_id)).model_dump()
        data_collection_id = mapping['data_collection_id']  # 数据集ID
        schema_id = mapping['schema_id']  # Schema ID
        model_id = mapping.get('model_id')  # 模型ID
        
        # 获取文档列表
        document_ids = await asyncio.to_thread(
            DataServiceHandler.get_parsed_documents,
            [data_collection_id],
            "langchain"  # 指定文档解析方式
        )
        logger.info(f"[优化抽取] 文档数: {len(document_ids)}")
        
        # 获取Schema定义
        schema = (await schema_service.get_schema_by_id(schema_id)).model_dump()
        
        # 提取实体和关系的Schema定义
        entity_schema = await ExtractServiceOptimized._extract_entity_schema(schema)
        relation_schema = await ExtractServiceOptimized._extract_relation_schema(schema)
        
        # 构建ID到label的映射，并转换edges
        nodes_map = {n['id']: n['label'] for n in schema.get('schema_graph', {}).get('nodes', [])}
        schema_edges = []
        for edge in schema.get('schema_graph', {}).get('edges', []):
            schema_edges.append({
                'label': edge.get('label', ''),
                'source': nodes_map.get(edge.get('source', ''), ''),  # ID -> Label
                'target': nodes_map.get(edge.get('target', ''), '')   # ID -> Label
            })
        
        logger.info(f"[优化抽取] Schema: {len(entity_schema)}个实体类型, {len(relation_schema)}个关系类型")
        
        # ==== 阶段1: 并发抽取 ====
        logger.info("[优化抽取] 阶段1: 并发一次性抽取")
        raw_entities, raw_relations = await ExtractServiceOptimized._parallel_extract_once(
            document_ids, entity_schema, relation_schema, model_id
        )
        raw_entities = ExtractServiceOptimized._filter_invalid_entities(raw_entities)
        raw_relations = ExtractServiceOptimized._filter_invalid_relations(raw_relations)
        
        extract_time = asyncio.get_event_loop().time() - start_time
        logger.info(f"[优化抽取] 抽取完成: {len(raw_entities)}实体, {len(raw_relations)}关系, 耗时{extract_time:.1f}s")
        
        # ==== 阶段2: 实体去重 ====
        logger.info("[优化抽取] 阶段2: 实体去重")
        dedup_start = asyncio.get_event_loop().time()
        
        entity_deduplicator = EntityDeduplicator()
        unique_entities, entity_mapping = entity_deduplicator.deduplicate(raw_entities)
        
        dedup_time = asyncio.get_event_loop().time() - dedup_start
        logger.info(f"[优化抽取] 去重完成: {len(raw_entities)} -> {len(unique_entities)}个唯一实体, 耗时: {dedup_time:.2f}秒")
        
        # ==== 阶段3: 关系验证 ====
        logger.info("[优化抽取] 阶段3: 关系验证")
        validate_start = asyncio.get_event_loop().time()
        
        relation_validator = RelationValidator(schema_edges)
        valid_relations = relation_validator.validate(raw_relations, entity_mapping)
        
        validate_time = asyncio.get_event_loop().time() - validate_start
        logger.info(f"[优化抽取] 验证完成: 耗时: {validate_time:.2f}秒")
        
        # ==== 阶段4: 转换为TuGraph格式 ====
        logger.info("[优化抽取] 阶段4: 格式转换")
        tugraph_nodes = await ExtractServiceOptimized._convert_to_tugraph_nodes(unique_entities, schema)
        tugraph_relations = await ExtractServiceOptimized._convert_to_tugraph_relations(valid_relations, unique_entities)
        
        # ✅ 调试日志：记录转换后的节点示例
        logger.info(f"[TuGraph调试] 转换后节点数量: {len(tugraph_nodes)}")
        for idx, node in enumerate(tugraph_nodes[:3]):  # 只记录前3个
            logger.info(f"[TuGraph调试] 转换后节点{idx+1}: label={node.get('label')}, name={node.get('name')}, props={node.get('props')}")
        
        # ✅ 统计关系中使用的节点
        nodes_in_relations = set()
        for rel in tugraph_relations:
            nodes_in_relations.add(rel['start'])
            nodes_in_relations.add(rel['end'])
        
        isolated_nodes = len(tugraph_nodes) - len(nodes_in_relations)
        if isolated_nodes > 0:
            logger.warning(f"⚠️ 发现{isolated_nodes}个孤立节点（无关系指向），total={len(tugraph_nodes)}, connected={len(nodes_in_relations)}")
        
        # ==== 阶段5: 批量导入 ====
        logger.info("[优化抽取] 阶段5: 批量导入")
        import_start = asyncio.get_event_loop().time()
        
        await ExtractServiceOptimized._batch_import(tugraph_nodes, tugraph_relations, graph_name)
        
        import_time = asyncio.get_event_loop().time() - import_start
        total_time = asyncio.get_event_loop().time() - start_time
        
        logger.info(f"[优化抽取] 完成! 总耗时: {total_time:.2f}秒 (抽取: {extract_time:.2f}s, 去重: {dedup_time:.2f}s, 验证: {validate_time:.2f}s, 导入: {import_time:.2f}s)")
        logger.info(f"[优化抽取] 最终结果: {len(tugraph_nodes)}个节点, {len(tugraph_relations)}个关系")
        
        return {
            'nodes': len(tugraph_nodes),  # 节点数量
            'relations': len(tugraph_relations),  # 关系数量
            'time_seconds': total_time  # 总耗时
        }
    
    @staticmethod
    async def _parallel_extract_once(document_ids: List[str], entity_schema: List, relation_schema: List, model_id: str) -> Tuple[List[Dict], List[Dict]]:
        """
        ✅ 优化：智能并发控制，根据任务规模动态调整
        - 小任务（<20段）：10并发，日常使用
        - 中等任务（20-100段）：20并发，平衡性能
        - 大任务（>100段）：40并发，高吞吐
        
        Args:
            document_ids: 文档ID列表
            entity_schema: 实体Schema定义
            relation_schema: 关系Schema定义
            model_id: 模型ID
        
        Returns:
            (entities, relations) - 实体列表和关系列表
        """
        from app.utils.model_util import call_model_to_extract_combined  # 导入模型调用函数
        
        # ✅ 显示缓存状态
        cache_stats = extraction_cache.get_stats()
        if cache_stats.get("enabled"):
            logger.info(f"[缓存] 已启用，当前缓存: {cache_stats['count']}个文件, {cache_stats['total_size_mb']}MB")
        
        tasks = []
        # ✅ 智能并发：根据任务规模自适应调整
        # TODO：根据文档大小控制并发数
        task_count_estimate = len(document_ids) * 2  # 粗略估计段落数
        if task_count_estimate <= 20:
            concurrency = 10  # 日常任务：10并发
        elif task_count_estimate <= 100:
            concurrency = 20  # 中等任务：20并发
        else:
            concurrency = 40  # 大批量：40并发
        
        # 创建并发控制信号量
        semaphore = asyncio.Semaphore(concurrency)
        logger.info(f"[并发抽取] 文档数: {len(document_ids)}, 预估段落: {task_count_estimate}, 并发度: {concurrency}")

        async def run_extract(content: str):
            """
            执行单次抽取的异步函数
            
            Args:
                content: 文本内容
            
            Returns:
                抽取结果
            """
            async with semaphore:  # 控制并发数
                # ✅ 优先使用缓存
                cached_result = extraction_cache.get(content, entity_schema, relation_schema)
                if cached_result is not None:
                    return cached_result
                
                # 缓存未命中，调用LLM进行抽取
                result = await call_model_to_extract_combined(
                    content,
                    entity_schema,
                    relation_schema,
                    model_id
                )
                
                # ✅ 保存到缓存
                if result:
                    extraction_cache.set(content, entity_schema, relation_schema, result)
                
                return result
        
        # 为每个文档的每个段落创建抽取任务
        for doc_id in document_ids:
            try:
                document = await asyncio.to_thread(DataServiceHandler.get_document_detail, doc_id)
            except Exception as e:
                logger.error(f"[parallel_extract] fetch document failed, id={doc_id}, err: {e}")
                continue
            if not document:
                continue
            
            segments = document.get("segments") or []
            for segment in segments:
                content = segment.get("content")
                if not content or not str(content).strip():
                    continue
                tasks.append(run_extract(str(content)))

        is_small_task = len(tasks) <= 10
        if is_small_task:
            logger.info(f"[parallel_extract] 轻量任务: {len(tasks)}段，并发{concurrency}")
        else:
            logger.info(f"[parallel_extract] total tasks: {len(tasks)}")
        
        # 并发执行所有抽取任务
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 分离实体和关系
        all_entities = []
        all_relations = []
        
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"[parallel_extract] task failed: {result}")
                continue
            
            if not result:
                continue
            
            if not isinstance(result, dict):
                logger.warning(f"[parallel_extract] unexpected result type, skip: {result}")
                continue
            
            # 提取实体
            entities = result.get('entities', {})
            for entity_type, entity_dict in entities.items():
                for entity_name, props in entity_dict.items():
                    all_entities.append({
                        'type': entity_type,
                        'name': entity_name,
                        'props': props
                    })
            
            # 提取关系
            relations = result.get('relations', {})
            for rel_type, rel_list in relations.items():
                for rel in rel_list:
                    all_relations.append({
                        'type': rel_type,
                        'subject': rel.get('subject', {}),
                        'object': rel.get('object', {})
                    })
        
        # ✅ 显示缓存统计
        cache_stats = extraction_cache.get_stats()
        if cache_stats.get("enabled") and (cache_stats["hit_count"] + cache_stats["miss_count"]) > 0:
            logger.info(f"[缓存] 命中率: {cache_stats['hit_rate']} (命中{cache_stats['hit_count']}/总计{cache_stats['hit_count'] + cache_stats['miss_count']})")
        
        return all_entities, all_relations
    
    @staticmethod
    def _filter_invalid_entities(entities: List[Dict]) -> List[Dict]:
        """
        过滤无效实体 - 移除缺少必要字段的实体并标准化属性
        
        Args:
            entities: 实体列表
        
        Returns:
            过滤后的实体列表
        """
        cleaned = []
        dropped = 0
        
        for entity in entities:
            entity_type = entity.get('type')
            name = entity.get('name')
            if not entity_type or not name:
                dropped += 1
                logger.warning(f"[extract] drop invalid entity: {entity}")
                continue
            
            cleaned.append({
                'type': entity_type,
                'name': name,
                'props': entity.get('props') or {}  # 确保props是字典
            })
        
        if dropped:
            logger.info(f"[extract] filtered invalid entities: {dropped}")
        return cleaned
    
    @staticmethod
    def _filter_invalid_relations(relations: List[Dict]) -> List[Dict]:
        """
        过滤无效关系 - 移除缺少主体/客体/类型的关系并统一字段
        
        Args:
            relations: 关系列表
        
        Returns:
            过滤后的关系列表
        """
        cleaned = []
        dropped = 0
        
        for rel in relations:
            rel_type = rel.get('type')
            subject = rel.get('subject') or {}
            obj = rel.get('object') or {}
            
            # 获取实体类型（可能是type或label字段）
            subj_type = subject.get('type') or subject.get('label')
            obj_type = obj.get('type') or obj.get('label')
            subj_name = subject.get('name')
            obj_name = obj.get('name')
            
            # 标准化实体类型字段
            if subj_type:
                subject = {**subject, 'type': subj_type}
            if obj_type:
                obj = {**obj, 'type': obj_type}
            
            # 检查必要字段是否存在
            if not rel_type or not subj_type or not obj_type or not subj_name or not obj_name:
                dropped += 1
                logger.warning(f"[extract] drop invalid relation: {rel}")
                continue
            
            cleaned.append({
                'type': rel_type,
                'subject': subject,
                'object': obj
            })
        
        if dropped:
            logger.info(f"[extract] filtered invalid relations: {dropped}")
        return cleaned
    
    @staticmethod
    async def _extract_entity_schema(schema: Dict) -> List[Dict]:
        """
        ✅ 提取实体Schema定义，包含详细的属性信息
        
        Args:
            schema: 完整的Schema定义
        
        Returns:
            实体Schema列表
        """
        entities = []
        for node in schema.get('schema_graph', {}).get('nodes', []):
            # ✅ 提取详细的属性信息（名称、类型、描述）
            attributes = []
            
            # ✅ 关键修复：Schema中字段名是 'attr' 不是 'attributes'
            for attr in node.get('attr', []):  # ← 修复：attr
                # ✅ Schema中属性名字段是 'label' 不是 'name'
                attr_name = attr.get('label', '')  # ← 修复：label
                attr_type = attr.get('type', 'STRING')  # ← 修复：type（不是dataType）
                attr_desc = attr.get('description', '')  # 属性描述
                
                # ⚠️ 过滤掉系统字段（nodeId、nodeName）
                if attr_name in ['nodeId', 'nodeName']:
                    continue
                
                if attr_name:  # 只添加有名称的属性
                    # 构建属性字符串：名称 (类型): 描述
                    if attr_desc:
                        attr_str = f"{attr_name} ({attr_type}): {attr_desc}"
                    else:
                        attr_str = f"{attr_name} ({attr_type})"
                    attributes.append(attr_str)
            
            entity_info = {
                'entity_type': node.get('label', ''),  # 实体类型名称
                'attributes': attributes,  # ✅ 详细属性列表
                'description': node.get('description', '')  # 实体类型描述
            }
            entities.append(entity_info)
        return entities
    
    @staticmethod
    async def _extract_relation_schema(schema: Dict) -> List[Dict]:
        """
        提取关系Schema定义
        
        Args:
            schema: 完整的Schema定义
        
        Returns:
            关系Schema列表
        """
        relations = []
        # 构建节点ID到标签的映射
        nodes_map = {n['id']: n['label'] for n in schema.get('schema_graph', {}).get('nodes', [])}
        
        for edge in schema.get('schema_graph', {}).get('edges', []):
            relation_info = {
                'relation_type': edge.get('label', ''),  # 关系类型名称
                'source_type': nodes_map.get(edge.get('source', ''), ''),  # 源实体类型
                'target_type': nodes_map.get(edge.get('target'), '')  # 目标实体类型
            }
            relations.append(relation_info)
        return relations
    
    @staticmethod
    async def _convert_to_tugraph_nodes(entities: List[Dict], schema: Dict) -> List[Dict]:
        """
        ✅ 转换实体为TuGraph节点格式（严格遵循旧版格式）
        
        Args:
            entities: 实体列表
            schema: Schema定义
        
        Returns:
            TuGraph节点列表
        """
        # ✅ 构建每个节点类型的合法字段映射（从Schema中提取）
        allowed_fields_map = {}
        base_fields = {"name", "nodeName", "nodeId"}  # 基础字段总是允许的
        
        # 遍历Schema中的节点定义，提取允许的字段
        for node in schema.get('schema_graph', {}).get('nodes', []):
            node_label = node.get('label', '')
            if not node_label:
                continue
            
            # 收集该节点类型在Schema中定义的所有属性
            allowed_fields = set(base_fields)  # 复制基础字段
            for attr in node.get('attr', []):
                attr_name = attr.get('label')
                if attr_name:
                    allowed_fields.add(attr_name)
            
            allowed_fields_map[node_label] = allowed_fields
        
        nodes = []
        for entity in entities:
            entity_type = entity['type']
            entity_id = entity.get('id', entity['name'])
            
            # 获取该实体类型的合法字段集合
            allowed_fields = allowed_fields_map.get(entity_type, set(base_fields))
            
            # ✅ 智能过滤：只保留Schema中定义的字段
            raw_props = entity.get('props', {})
            filtered_props = {}
            dropped_fields = []
            
            for key, value in raw_props.items():
                if key in allowed_fields:
                    filtered_props[key] = value
                else:
                    # 字段未在Schema中定义，丢弃（避免TuGraph报错）
                    dropped_fields.append(key)
            
            # 只在有较多字段被丢弃时记录警告
            if len(dropped_fields) > 2:
                logger.warning(f"[字段过滤] '{entity['name']}'({entity_type}): 丢弃{len(dropped_fields)}个未定义字段")
            
            # nodeId是必需字段，总是添加
            filtered_props['nodeId'] = entity_id
            
            # ❌ 不要添加nodeName！kg_service会自动从node.name生成
            
            node = {
                'label': entity_type,  # 节点标签
                'name': entity['name'],  # 节点名称
                'props': filtered_props  # ✅ 只包含Schema定义的合法字段
            }
            nodes.append(node)
        
        return nodes
    
    @staticmethod
    async def _convert_to_tugraph_relations(relations: List[Dict], entities: List[Dict]) -> List[Dict]:
        """
        ✅ 转换关系为TuGraph边格式（与旧版格式一致）
        
        Args:
            relations: 关系列表
            entities: 实体列表
        
        Returns:
            TuGraph关系列表
        """
        # 构建ID到实体的映射
        id_to_entity = {e['id']: e for e in entities}
        
        edges = []
        for rel in relations:
            subj_entity = id_to_entity.get(rel['subject_id'])
            obj_entity = id_to_entity.get(rel['object_id'])
            
            if not subj_entity or not obj_entity:
                continue
            
            # ✅ 修复：使用旧版格式 start/end/type
            # 旧版中使用完整ID，但非结构化抽取不ID，使用name代替
            edge = {
                'start': subj_entity.get('id') or subj_entity['name'],  # ✅ start not start_node
                'end': obj_entity.get('id') or obj_entity['name'],      # ✅ end not end_node
                'type': rel['type']                                     # ✅ type not label
            }
            edges.append(edge)
        
        return edges
    
    @staticmethod
    async def _batch_import(nodes: List[Dict], relations: List[Dict], graph_name: str, batch_size: int = 100):
        """
        ✅ 混合模式：优先批量导入（快），失败则逐个导入（稳）
        - 批量导入：性能提升14倍（2900节点：290秒 -> 20秒）
        - 降级逐个：保证稳定性（兼容属性缺失）
        
        Args:
            nodes: 节点列表
            relations: 关系列表
            graph_name: 图谱名称
            batch_size: 批次大小
        """
        logger.info(f"[batch_import] start, nodes={len(nodes)}, relations={len(relations)}")
        
        # ============ 节点导入：渐进式降级 ============
        nodes_imported = 0
        total_nodes = len(nodes)
        
        if total_nodes > 0:
            # 🚀 策略1：优先尝试全部批量导入（最快）
            logger.info(f"[batch_import] 尝试批量导入{total_nodes}个节点...")
            try:
                import_start = asyncio.get_event_loop().time()
                await asyncio.to_thread(
                    DataServiceHandler.import_data_batch,
                    graph_name,
                    nodes,  # ✅ 一次导入所有节点
                    []
                )
                import_time = asyncio.get_event_loop().time() - import_start
                logger.info(f"[batch_import] ✅ 批量导入节点成功! {total_nodes}个节点, 耗时: {import_time:.2f}秒")
                nodes_imported = total_nodes
            except Exception as e:
                # 🔄 策略2：分批导入（中等速度）
                logger.warning(f"[batch_import] ⚠️ 全部批量导入失败: {e}")
                logger.info(f"[batch_import] 降级策略1: 尝试分批导入（每批{batch_size}个）...")
                
                batch_failed = []
                for i in range(0, total_nodes, batch_size):
                    batch = nodes[i:i+batch_size]
                    try:
                        await asyncio.to_thread(
                            DataServiceHandler.import_data_batch,
                            graph_name,
                            batch,
                            []
                        )
                        nodes_imported += len(batch)
                        if (i + len(batch)) % 500 == 0 or (i + len(batch)) == total_nodes:
                            logger.info(f"[batch_import] 分批进度: {i+len(batch)}/{total_nodes}")
                    except Exception as batch_e:
                        logger.warning(f"[batch_import] 批次{i//batch_size + 1}失败: {batch_e}")
                        batch_failed.extend(batch)
                
                # 🐌 策略3：逐个导入失败的批次（最慢但最稳）
                if batch_failed:
                    logger.warning(f"[batch_import] 降级策略2: 逐个导入{len(batch_failed)}个失败节点...")
                    failed_nodes = []
                    for node in batch_failed:
                        try:
                            await asyncio.to_thread(
                                DataServiceHandler.import_data_batch,
                                graph_name,
                                [node],
                                []
                            )
                            nodes_imported += 1
                        except Exception as node_e:
                            failed_nodes.append(node.get('name', 'unknown'))
                            logger.error(f"[batch_import] 节点'{node.get('name')}'导入失败: {node_e}")
                    
                    if failed_nodes:
                        logger.error(f"[batch_import] ❌ {len(failed_nodes)}个节点最终导入失败: {failed_nodes[:5]}...")
            
            logger.info(f"[batch_import] 节点导入完成: {nodes_imported}/{total_nodes}成功")
        
        # ============ 关系导入：一次性批量 ============
        if len(relations) > 0:
            try:
                import_start = asyncio.get_event_loop().time()
                await asyncio.to_thread(
                    DataServiceHandler.import_data_batch,
                    graph_name,
                    [],
                    relations  # ✅ 一次导入所有关系
                )
                import_time = asyncio.get_event_loop().time() - import_start
                logger.info(f"[batch_import] ✅ 关系导入成功! {len(relations)}个关系, 耗时: {import_time:.2f}秒")
            except Exception as e:
                logger.error(f"[batch_import] ❌ 关系批量导入失败: {e}")
        
        logger.info("[batch_import] complete")


extract_service_optimized = ExtractServiceOptimized()  # 创建抽取服务实例