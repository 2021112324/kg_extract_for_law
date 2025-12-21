"""
图谱抽取
"""
import os

from dotenv import load_dotenv

from app.infrastructure.information_extraction.factory import InformationExtractionFactory
from app.infrastructure.information_extraction.method.base import LangextractConfig, ModelConfig
from app.infrastructure.information_extraction.sync_task import sync_task_manager
from app.schemas.kg import GraphEdgeBase, GraphNodeBase


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
            extract_nodes = await self.node_extractor.entity_extract(
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
            extract_edges = await self.edge_extractor.relationship_extract(
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
            extract_result = {
                "entities": extract_nodes,
                "relations": extract_edges,
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
            extract_result: 包含entities和relations的抽取结果，其中entities是list[Entity]格式，
                           relations是list[Relationship]格式

            graph_data = {
            "nodes": list[GraphNodeBase],
            "edges": list[GraphEdgeBase]
        }
        Returns:
            dict: 包含GraphNodeBase和GraphEdgeBase对象的图谱结构数据
        """
        graph_data = {
            "nodes": [],
            "edges": []
        }

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
                graph_data["nodes"].append(node)
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
                relation_key = (source_id, getattr(relation, "type", ""), target_id)
                if relation_key not in relation_set:
                    relation_set.add(relation_key)
                    # 创建GraphEdgeBase对象
                    edge = GraphEdgeBase(
                        source_id=source_id,
                        target_id=target_id,
                        relation_type=getattr(relation, "type", ""),
                        properties=edge_properties,
                        weight=getattr(relation, "weight", 1.0),
                        bidirectional=getattr(relation, "bidirectional", False)
                    )
                    graph_data["edges"].append(edge)
            else:
                print(f"Warning: 找不到源节点或目标节点: {source_name} -> {target_name}")

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
        return f"{node_type}_{node_name}"

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
            graph_result = {
                "entities": [],
                "relations": []
            }
            for i, result in enumerate(result_list):
                if not isinstance(result, dict):
                    print(f"第{i}个文段块抽取失败")
                graph_result["entities"] = graph_result["entities"] + result.get("entities", [])
                graph_result["relations"] = graph_result["relations"] + result.get("relations", [])
            return graph_result
        except Exception as e:
            print("分段抽取失败：", e)
            raise e

    @staticmethod
    def _split_text_by_paragraphs(
            text: str,
            max_chunk_size: int,
            overlap_size: int
    ):
        """
        按段落分割文本，分段间保留重叠内容

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
        current_chunk = ""

        for paragraph in paragraphs:
            if len(current_chunk) + len(paragraph) > max_chunk_size:
                if current_chunk:  # 非空才添加
                    chunks.append(current_chunk.strip())
                    # 保留重叠部分（取最后overlap_size个字符）
                    current_chunk = current_chunk[-min(overlap_size, len(current_chunk)):] + "\n\n"
            current_chunk += paragraph + "\n\n"

        # 添加最后剩余内容
        if current_chunk.strip():
            chunks.append(current_chunk.strip())

        return chunks


graph_extractor = GraphExtraction()

if __name__ == "__main__":
    print("Project root:", project_root)
    print("Env path exists:", os.path.exists(env_path))
    print("BATCH_LENGTH:", BATCH_LENGTH)
    print("MAX_WORKERS:", MAX_WORKERS)
    print("MAX_CHAR_BUFFER:", MAX_CHAR_BUFFER)
    print("MAX_CHUNK_SIZE:", MAX_CHUNK_SIZE)
    print("OVERLAP_SIZE:", OVERLAP_SIZE)