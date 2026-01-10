"""
图谱抽取
"""
import logging
import os
import uuid

from dotenv import load_dotenv

from app.infrastructure.information_extraction.factory import InformationExtractionFactory
from app.infrastructure.information_extraction.method.base import LangextractConfig, ModelConfig
from app.infrastructure.information_extraction.sync_task import sync_task_manager
from app.schemas.kg import GraphEdgeBase, GraphNodeBase, TextClass

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

env_path = os.path.join(project_root, '.env')
load_dotenv(env_path)

BATCH_LENGTH = int(os.getenv("BATCH_LENGTH", "5"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "3"))
MAX_CHAR_BUFFER = int(os.getenv("MAX_CHAR_BUFFER", "5000"))

LENGTH_THRESHOLD = 3000

MAX_CHUNK_SIZE = int(os.getenv("MAX_CHUNK_SIZE", "5000"))
OVERLAP_SIZE = int(os.getenv("OVERLAP_SIZE", "500"))

TIMEOUT = int(os.getenv("TIMEOUT", "300"))


class GraphExtraction:
    def __init__(
            self,
            model_config: ModelConfig = ModelConfig(
                model_name="qwen-long",
                api_key="sk-742c7c766efd4426bd60a269259aafaf",
                api_url="https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions",
                config={
                    "timeout": 300
                }
            )
    ):
        # 节点抽取器
        self.node_extractor_config = LangextractConfig(
            model_name=model_config.model_name,
            api_key=model_config.api_key,
            api_url=model_config.api_url,
            config=model_config.config,
            max_char_buffer=MAX_CHAR_BUFFER,
            batch_length=BATCH_LENGTH,
            max_workers=MAX_WORKERS,
            # resolver_params=
        )
        self.node_extractor = InformationExtractionFactory.create(
            "langextract",
            max_retries=5,
            config=self.node_extractor_config
        )
        # 边抽取器
        self.edge_extractor_config = LangextractConfig(
            model_name=model_config.model_name,
            api_key=model_config.api_key,
            api_url=model_config.api_url,
            config=model_config.config,
            max_char_buffer=MAX_CHAR_BUFFER,
            batch_length=BATCH_LENGTH,
            max_workers=MAX_WORKERS,
        )
        self.edge_extractor = InformationExtractionFactory.create(
            "langextract",
            max_retries=3,
            config=self.node_extractor_config
        )

    async def extract_graph(
            self,
            prompt: str,
            schema: dict,
            input_text: str,
            examples: list = None,
    ) -> dict | None:
        # 定义长度阈值，可根据实际需求调整
        length_threshold = LENGTH_THRESHOLD  # 字符数阈值

        if len(input_text) > length_threshold:
            # 使用 one-shot 方法处理长文本
            print(f"使用one-shot方法处理长文本: 文件长{len(input_text)}")
            return await self.extract_graph_oneshot(prompt, schema, input_text, examples)
        else:
            # 使用 multi-step 方法处理短文本
            print(f"使用multi-step方法处理短文本：文件长{len(input_text)}")
            return await self.extract_graph_multistep(prompt, schema, input_text, examples)

    async def extract_graph_multistep(
            self,
            prompt: str,
            schema: dict,
            input_text: str,
            examples: list = None,
    ) -> dict | None:
        """
        从文本中抽取知识，包含重试机制
        其中：
        schema(dicy)的结构如下：
        {
            "nodes":"sth.",
            "edges":"sth."
        }
        examples (list)的结构如下：
        [
            {
                "text":"sth."
                "nodes":[
                    {
                        "name":"sth.",
                        "type":"sth.",
                        "attributes":{
                            ...
                        }
                    },
                    ...
                ],
                "edges":[
                    {
                        "subject": "sth.",
                        "predicate": "sth.",
                        "object": "sth.",
                        "attributes":{              # 暂涉及该部分
                            ...
                        }
                    },
                    ...
                ]
            },
            ...
        ]

        Args:
            prompt (str): 提示词, 若为空。则使用默认提示词
            schema (dict): 抽取结果结构定义, 若为空，则使用默认schema
            input_text (str): 输入文本
            examples (list): 示例数据, 若为空，则使用默认示例
        Returns:
            dict: 抽取结果
        Raises:
            Exception: 如果所有重试都失败则抛出异常
        """
        try:
            if schema:
                node_schema = schema.get("nodes")
                edge_schema = schema.get("edges")
            else:
                node_schema = None
                edge_schema = None
            node_examples = []
            edge_examples = []
            if examples:
                for example in examples:
                    temp_node_example = {
                        "text": example.get("text"),
                        "extractions": example.get("nodes")
                    }
                    temp_edge_example = {
                        "text": example.get("text"),
                        "extractions": example.get("edges")
                    }
                    node_examples.append(temp_node_example)
                    edge_examples.append(temp_edge_example)
            else:
                node_examples = None
                edge_examples = None
            # 提取节点信息
            extract_nodes, nodes_text_classes = await self.node_extractor.entity_extract(
                prompt,
                node_schema,
                input_text,
                examples=node_examples
            )
            if not extract_nodes:
                print("抽取的节点为空")
                return None
            print("节点信息：", extract_nodes)
            # 提取边信息
            extract_edges, edges_text_classes = await self.edge_extractor.relationship_extract(
                prompt,
                extract_nodes,
                edge_schema,
                input_text,
                examples=edge_examples
            )
            if not extract_edges:
                print("抽取的边为空")
                return None
            print("边信息：", extract_edges)

            text_classes = self.node_extractor.merge_text_class(nodes_text_classes, edges_text_classes)
            extract_result = {
                "entities": extract_nodes,
                "relations": extract_edges,
                "text_classes": text_classes
            }
            return self.build_graph_structure(extract_result)
        except Exception as e:
            print("抽取失败：", e)
            raise e

    async def extract_graph_oneshot(
            self,
            prompt: str,
            schema: dict,
            input_text: str,
            examples: list = None,
    ) -> dict | None:
        """
        从文本中抽取知识，包含重试机制
        采用one_shot机制，即一次抽取同时抽取节点和关系
        其中：
        examples (list)的结构如下：
        [
            {
                "text":"sth."
                "nodes":[
                    {
                        "name":"sth.",
                        "type":"sth.",
                        "attributes":{
                            ...
                        }
                    },
                    ...
                ],
                "edges":[
                    {
                        "subject": "sth.",
                        "predicate": "sth.",
                        "object": "sth.",
                        "attributes":{              # 暂不涉及该部分
                            ...
                        }
                    },
                    ...
                ]
            },
            ...
        ]

        Args:
            prompt (str): 提示词
            schema (dict): 抽取结果结构定义
            input_text (str): 输入文本
            examples (list): 示例数据
        Returns:
            dict: 抽取结果
        Raises:
            Exception: 如果所有重试都失败则抛出异常
        """
        try:
            full_examples = []
            if examples:
                for example in examples:
                    temp_example = {
                        "text": example.get("text"),
                        "extractions": (example.get("nodes") or []) + (example.get("edges") or [])
                    }
                    full_examples.append(temp_example)
            # 提取节点信息
            extract_result = await self._split_to_sync_extract(
                prompt,
                schema,
                input_text,
                full_examples
            )
            # print("抽取结果：", extract_result)

            if not isinstance(extract_result, dict):
                print("抽取结果格式错误")
                raise Exception("抽取结果格式错误")
            return self.build_graph_structure(extract_result)
        except Exception as e:
            print("抽取失败：", e)
            raise e

    def build_graph_structure(self, extract_result: dict) -> dict:
        """
        使用GraphNodeBase和GraphEdgeBase构建图谱结构数据
        
        Args:
            extract_result: 包含entities和relations的抽取结果，其中
                            entities是list[Entity]格式，
                            relations是list[Relationship]格式
                            texts_classes是list[TextClass]格式

            graph_data = {
            "nodes": list[GraphNodeBase],
            "edges": list[GraphEdgeBase],
            "texts_classes": list[TextClass]
        }
        Returns:
            dict: 包含GraphNodeBase和GraphEdgeBase对象的图谱结构数据
        """
        graph_data = {
            "nodes": [],
            "edges": [],
            "text_classes": []
        }

        # # 保存有用的text_class
        # text_class_id_set = set()

        # 处理节点数据 (Entity对象列表)
        entities = extract_result.get("entities", [])
        node_map = {}  # 用于后续边处理时查找节点
        node_object_map = {}  # 新增映射

        for entity in entities:
            # 生成节点ID
            node_id = self.generate_node_id(
                getattr(entity, "entity_type", ""),
                getattr(entity, "name", ""),
                getattr(entity, "id", None)
            )

            node_properties = getattr(entity, "properties", {})
            if isinstance(node_properties, dict):
                node_properties = {
                    key: value
                    for key, value in node_properties.items()
                    if value is not None
                }

            node_key = (getattr(entity, "name", ""), getattr(entity, "entity_type", ""))
            if node_key not in node_map:
                # 创建GraphNodeBase对象
                node = GraphNodeBase(
                    node_id=node_id,
                    node_name=getattr(entity, "name", ""),
                    node_type=getattr(entity, "entity_type", ""),
                    properties=node_properties,
                    description=getattr(entity, "description", None)
                )

                # 保存节点溯源文本映射
                source_texts = getattr(entity, "source_texts", None)
                if source_texts and isinstance(source_texts, list):
                    sources_texts_info = {}
                    new_sources_texts_info = self._merge_source_texts_to_info_map(source_texts, sources_texts_info)
                    # for text_id in new_sources_texts_info.keys():
                    #     text_class_id_set.add(text_id)
                    node.source_text_info = new_sources_texts_info
                graph_data["nodes"].append(node)
                # graph_data["node_text_map"][node_id] = getattr(entity, "text", "")
                # 使用name和entity_type组合作为键确保唯一性
                node_map[node_key] = node_id  # 保存节点名称和类型到ID的映射
                node_object_map[node_id] = node  # 维护节点对象映射
            else:
                # 直接通过映射获取节点对象，避免遍历
                existing_node = node_object_map[node_map[node_key]]
                existing_node.properties.update(node_properties)
                # 更安全的描述信息更新
                new_description = getattr(entity, "description", None)
                if new_description:
                    existing_node.description = new_description

                # 保存节点溯源文本映射
                source_texts = getattr(entity, "source_texts", None)
                if source_texts and isinstance(source_texts, list):
                    temp_source_texts_info = existing_node.source_text_info
                    new_sources_texts_info = {}
                    if temp_source_texts_info and isinstance(temp_source_texts_info, dict):
                        new_source_texts_info = self._merge_source_texts_to_info_map(source_texts, temp_source_texts_info)
                    else:
                        new_source_texts_info = self._merge_source_texts_to_info_map(source_texts, {})
                    # for text_id in new_sources_texts_info.keys():
                    #     text_class_id_set.add(text_id)
                    existing_node.source_text_info = new_source_texts_info

        # 处理边数据 (Relationship对象列表)
        relations = extract_result.get("relations", [])
        relation_set = set()  # 用于关系去重

        for relation in relations:
            # 获取源节点和目标节点ID
            source_name = getattr(relation, "source", "")
            target_name = getattr(relation, "target", "")

            # 根据关系中的信息查找对应的节点类型（这里假设关系中没有直接提供节点类型）
            # 在实际应用中，可能需要通过其他方式获取源和目标节点的类型
            # 这里暂时使用默认的查找方式
            source_key = None
            target_key = None

            # 尝试查找源节点和目标节点的键
            for (name, entity_type), node_id in node_map.items():
                if name == source_name and source_key is None:
                    source_key = (source_name, entity_type)
                if name == target_name and target_key is None:
                    target_key = (target_name, entity_type)

                # 如果都找到了，就跳出循环
                if source_key is not None and target_key is not None:
                    break

            source_id = node_map.get(source_key) if source_key else None
            target_id = node_map.get(target_key) if target_key else None

            # 只有当源节点和目标节点都存在时才创建边
            if source_id and target_id:
                edge_properties = getattr(relation, "properties", {})
                if isinstance(edge_properties, dict):
                    edge_properties = {
                        key: value
                        for key, value in edge_properties.items()
                        if value is not None
                    }

                # 关系去重检查
                relation_type = getattr(relation, "type", None)
                if not relation_type:
                    continue
                relation_key = (source_id, relation_type, target_id)
                if relation_key not in relation_set:
                    relation_set.add(relation_key)
                    # 创建GraphEdgeBase对象
                    edge = GraphEdgeBase(
                        source_id=source_id,
                        target_id=target_id,
                        relation_type=relation_type,
                        properties=edge_properties,
                        weight=getattr(relation, "weight", 1.0),
                        bidirectional=getattr(relation, "bidirectional", False)
                    )

                    # 保存边溯源文本映射
                    source_texts = getattr(relation, "source_texts", None)
                    if source_texts and isinstance(source_texts, list):
                        sources_texts_info = {}
                        new_sources_texts_info = self._merge_source_texts_to_info_map(source_texts, sources_texts_info)
                        # for text_id in new_sources_texts_info.keys():
                        #     text_class_id_set.add(text_id)
                        edge.source_text_info = new_sources_texts_info

                    graph_data["edges"].append(edge)
                else:
                    # 重复变关系，更新属性
                    existing_edge = [edge for edge in graph_data["edges"] if edge.source_id == source_id and edge.target_id == target_id and edge.relation_type == relation_type]
                    if existing_edge:
                        edges_properties = existing_edge[0].properties
                        if edges_properties and isinstance(edges_properties, dict):
                            edges_properties.update(existing_edge[0].properties)

                        # 更新边溯源文本映射
                        source_texts = getattr(relation, "source_texts", None)
                        if source_texts and isinstance(source_texts, list):
                            sources_texts_info = existing_edge[0].source_text_info
                            if sources_texts_info and isinstance(sources_texts_info, dict):
                                new_sources_texts_info = self._merge_source_texts_to_info_map(source_texts, sources_texts_info)
                            else:
                                new_sources_texts_info = self._merge_source_texts_to_info_map(source_texts, {})
                            # for text_id in new_sources_texts_info.keys():
                            #     text_class_id_set.add(text_id)
                            existing_edge[0].source_text_info = new_sources_texts_info

            else:
                print(f"Warning: 找不到源节点或目标节点: {source_name} -> {target_name}")

        # 添加节点溯源文本类
        # 方案零：保留所有节点的溯源文本类
        for text_class in extract_result.get("texts_classes", []):
            text_id = getattr(text_class, "id", None)
            text_content = getattr(text_class, "text", None)
            if not text_id or not text_content:
                continue
            graph_data["text_classes"].append(
                TextClass(
                    text_id=text_id,
                    text=text_content
                )
            )
        # 方案一：仅保留Langextract抽取后有节点存在溯源的对应文本类，但某些领域内，大量甚至全部节点都没能成功溯源，会导致溯源文本彻底丢失
        # for text_id in text_class_id_set:
        #     text_classes = extract_result.get("texts_classes", [])
        #     for text_class in text_classes:
        #         temp_class_id = getattr(text_class, "id", None)
        #         if not temp_class_id:
        #             continue
        #         if temp_class_id == text_id:
        #             text_content = getattr(text_class, "text", None)
        #             if not text_content:
        #                 continue
        #             graph_data["text_classes"].append(
        #                 TextClass(
        #                     text_id=text_id,
        #                     text=text_content
        #                 )
        #             )

        return graph_data

    @staticmethod
    def generate_node_id(
            node_type: str,
            node_name: str,
            node_id: str = None
    ) -> str:
        """
        生成节点ID
        """
        if node_id:
            return node_id
        # 使用UUID确保生成的ID是唯一的
        unique_uuid = uuid.uuid4()
        return f"{node_type}_{node_name}_{unique_uuid}"

    async def _split_to_sync_extract(
            self,
            prompt,
            schema,
            input_text,
            examples
    ):
        try:
            if len(input_text) <= 0:
                return None
            chunks = self._split_text_by_paragraphs(input_text, MAX_CHUNK_SIZE, OVERLAP_SIZE)
            parameters_list = []
            print("分块数：", len(chunks))
            for chunk in chunks:
                parameters = {
                    "user_prompt": prompt,
                    "schema": schema,
                    "input_text": chunk,
                    "examples": examples
                }
                parameters_list.append(parameters)
            result_list = await sync_task_manager.run_async_tasks(
                self.node_extractor.entity_and_relationship_extract,
                parameters_list
            )
            # print("抽取结果数：", result_list)
            graph_result = {
                "entities": [],
                "relations": [],
                "texts_classes": []
            }
            for i, result in enumerate(result_list):
                if not isinstance(result, dict):
                    print(f"第{i}个文段块抽取失败")
                graph_result["entities"] = graph_result["entities"] + result.get("entities", [])
                graph_result["relations"] = graph_result["relations"] + result.get("relations", [])
                graph_result["texts_classes"] = self.node_extractor.merge_text_class(
                    graph_result["texts_classes"],
                    result.get("texts_classes", [])
                )
            return graph_result
        except Exception as e:
            print("分段抽取失败：", e)
            raise e

    def _merge_source_texts_to_info_map(
            self,
            source_texts: list,
            text_info_map: dict
    ) -> dict:
        """
        将原文本映射信息数据合并到映射中
        节点溯源文本映射数据格式为：
        {
          "source_text_id":[
              {
                  "start_pos": "",
                  "end_pos": "",
                  "alignment_status": ""
              }
          ]
        }
        :param source_texts:
        :param text_info_map:
        :return:
        """
        for source_text in source_texts:
            source_text_id = getattr(source_text, "id", None)
            start_pos = getattr(source_text, "start_pos", None)
            end_pos = getattr(source_text, "end_pos", None)
            alignment_status = getattr(source_text, "alignment_status", None)
            if not source_text_id:
                continue
            if source_text_id not in text_info_map:
                text_info_map[source_text_id] = [{
                    "start_pos": start_pos,
                    "end_pos": end_pos,
                    "alignment_status": alignment_status
                }]
            else:
                # 方案一：保留所有溯源文本信息
                temp_text_info_list = text_info_map[source_text_id]
                if not temp_text_info_list or not isinstance(temp_text_info_list, list):
                    temp_text_info_list = []
                for temp_text_info in temp_text_info_list:
                    # 避免重复
                    if start_pos == temp_text_info.get("start_pos") or end_pos == temp_text_info.get("end_pos"):
                        continue
                temp_text_info_list.append(
                    {
                        "start_pos": start_pos,
                        "end_pos": end_pos,
                        "alignment_status": alignment_status
                    }
                )
        return text_info_map

    @staticmethod
    def _split_text_by_paragraphs(
            text: str,
            max_chunk_size: int,
            overlap_size: int
    ):
        """
        按段落分割文本，分段间保留重叠内容
        优先在换行符处分割，若无法在合理范围内找到换行符，则按句子分割

        Args:
            text: 输入文本
            max_chunk_size: 单段最大长度
            overlap_size: 分段间重叠内容长度

        Returns:
            List[str]: 分割后的文本片段列表
        """
        if not isinstance(text, str):
            raise ValueError("输入必须为字符串")

        # 按段落分割（以双换行符为分隔）
        paragraphs = text.split("\n\n")
        chunks = []

        for paragraph in paragraphs:
            if len(paragraph) <= max_chunk_size:
                # 段落长度合适，直接添加
                if chunks:
                    # 与前一个块合并（考虑重叠）
                    last_chunk = chunks[-1]
                    if len(last_chunk) + len(paragraph) + 2 <= max_chunk_size:
                        chunks[-1] = last_chunk + "\n\n" + paragraph
                    else:
                        chunks.append(paragraph)
                else:
                    chunks.append(paragraph)
            else:
                # 段落过长，需要进一步分割
                sub_chunks = GraphExtraction._split_long_text(paragraph, max_chunk_size, overlap_size)
                chunks.extend(sub_chunks)

        return chunks

    @staticmethod
    def _split_long_text(text: str, max_chunk_size: int, overlap_size: int) -> list:
        """
        智能分割过长文本块

        Args:
            text: 要分割的文本
            max_chunk_size: 最大块大小
            overlap_size: 重叠大小

        Returns:
            List[str]: 分割后的文本块列表
        """
        chunks = []
        start_idx = 0

        while start_idx < len(text):
            end_idx = start_idx + max_chunk_size

            if end_idx >= len(text):
                # 剩余内容直接作为一个块
                chunks.append(text[start_idx:])
                break

            # 在阈值附近寻找最近的换行符或句子结束符
            search_range = int(max_chunk_size * 0.2)  # 搜索范围为最大长度的20%

            # 优先寻找换行符
            split_pos = GraphExtraction._find_split_position(text, end_idx, search_range, ['\n'])

            if split_pos == -1:
                # 没找到换行符，寻找句子结束符
                split_pos = GraphExtraction._find_split_position(text, end_idx, search_range,
                                                                 ['。', '！', '？', '；', '.', '!', '?', ';', '，', ',',
                                                                  '、'])

            if split_pos != -1 and split_pos > start_idx:
                # 找到了合适的分割点
                chunk = text[start_idx:split_pos + 1].rstrip()  # 去掉末尾可能的换行符
                chunks.append(chunk)
                start_idx = split_pos + 1
            else:
                # 没找到合适的分割点，强制在最大长度处分割
                chunk = text[start_idx:end_idx]
                chunks.append(chunk)
                start_idx = end_idx

        return chunks

    @staticmethod
    def _find_split_position(text: str, target_pos: int, search_range: int, split_chars: list) -> int:
        """
        在目标位置附近寻找最近的分割字符

        Args:
            text: 要搜索的文本
            target_pos: 目标位置
            search_range: 搜索范围
            split_chars: 分割字符列表

        Returns:
            int: 找到的分割位置，-1表示未找到
        """
        # 向前搜索
        for i in range(min(target_pos, len(text) - 1), max(target_pos - search_range, 0), -1):
            if text[i] in split_chars:
                return i

        # 向后搜索
        for i in range(target_pos, min(target_pos + search_range, len(text))):
            if text[i] in split_chars:
                return i

        return -1


graph_extractor = GraphExtraction()
