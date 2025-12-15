import os
import sys
import traceback

from dotenv import load_dotenv
from sqlalchemy.orm import Session

from app.infrastructure.graph_storage.factory import GraphStorageFactory
from app.infrastructure.information_extraction.graph_extraction import GraphExtraction
from app.infrastructure.information_extraction.method.base import ModelConfig
from app.infrastructure.storage.object_storage import StorageFactory
from app.services.tasks.kg_tasks import KGExtractionTaskManager

# 从环境变量获取API基础URL，根据.env文件配置
# host = os.getenv("HOST", "0.0.0.0")
# port = os.getenv("PORT", "8000")
# api_v1_str = os.getenv("API_V1_STR", "/api")
# api_base_url = f"http://{host}:{port}{api_v1_str}".replace("0.0.0.0", "localhost")

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

env_path = os.path.join(project_root, '.env')
load_dotenv(env_path)

GRAPH_TYPE = os.getenv("GRAPH_DB_TYPE", "neo4j")
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://60.205.171.106:7687")
NEO4J_USERNAME = os.getenv("NEO4J_USERNAME", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "hit-wE8sR9wQ3pG1")
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE", "neo4j")

MD_BUCKET = os.getenv("MINIO_BUCKET_MD", "processed-files")


class KGExtractService():
    """
    涉及大模型的知识图谱抽取任务,包含知识图谱抽取和图谱存储
    """

    def __init__(
            self,
    ):
        super().__init__()
        # 知识图谱图存储管理
        self.graph_storage = GraphStorageFactory.create(
            GRAPH_TYPE,
            config={
                "uri": NEO4J_URI,
                "username": NEO4J_USERNAME,
                "password": NEO4J_PASSWORD,
                "database": NEO4J_DATABASE
            }
        )
        self.graph_extract = GraphExtraction(
            ModelConfig(
                model_name="qwen-long",
                api_key="sk-742c7c766efd4426bd60a269259aafaf",
                api_url="https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions",
            )
        )
        self.file_storage = StorageFactory.get_default_storage()
        self.file_storage.initialize()
        # 知识图谱抽取任务管理
        self.kgExtractionTaskManager = KGExtractionTaskManager(
            # db数据库存储抽取任务数据表
            db_session=None
        )
        # TODO：知识图谱抽取文件minio存储库,临时使用
        self.bucket = "graph-test"

    async def extract_kg_from_md_paths(
            self,
            md_paths: list,
            user_prompt: str,
            db: Session,
            prompt_parameters: dict = None,
    ):
        """
        从解析文档(markdown)中抽取图谱
        1.测试大模型是否可用
        2.获取大模型配置
        3.将任务和参数交由线程池
        返回结构：
        {
            "nodes":[GraphNodeBase],
            "edges":[GraphEdgeBase]
        }
        """
        try:
            # print("schema"+str(prompt_parameters))
            parameters_list = []
            for md_path in md_paths:
                parameters = {
                    "md_path": md_path,
                    "prompt": user_prompt,
                    "schema": prompt_parameters.get("schema", {}),
                    "examples": prompt_parameters.get("examples", [])
                }
                parameters_list.append(parameters)
            result_list = await self.kgExtractionTaskManager.run_async_tasks(
                self.extract_kg_from_md,
                parameters_list
            )
            # # 保存result_list到测试数据目录
            # test_data_dir = r"F:\企业大脑知识库系统\洛书\cogmait-backend\tests\test_data"
            # os.makedirs(test_data_dir, exist_ok=True)  # 确保目录存在
            # result_file_path = os.path.join(test_data_dir, "result_list.json")
            # # 将result_list保存为JSON文件
            # with open(result_file_path, 'w', encoding='utf-8') as f:
            #     json.dump(result_list, f, ensure_ascii=False, indent=2, default=lambda obj: obj.__dict__)

            # 合并所有结果中的节点和边
            merged_nodes = {}
            merged_edges = {}
            for result in result_list:
                if result is None:
                    continue
                # 合并节点
                if "nodes" in result:
                    for node in result["nodes"]:
                        node_id = node.node_id  # 改为对象属性访问
                        if node_id in merged_nodes:
                            # 按规则合并节点
                            existing_node = merged_nodes[node_id]
                            existing_node["node_name"] = getattr(node, "node_name", existing_node["node_name"])
                            existing_node["node_type"] = getattr(node, "node_type", existing_node["node_type"])
                            existing_node["description"] = getattr(node, "description", existing_node["description"])
                            node_filename = getattr(node, "filename", [])
                            if isinstance(node_filename, str):
                                node_filename = [node_filename]
                            elif not isinstance(node_filename, list):
                                node_filename = []
                            existing_node_filename = existing_node.get("filename", [])
                            if isinstance(existing_node_filename, str):
                                existing_node_filename = [existing_node_filename]
                            elif not isinstance(existing_node_filename, list):
                                existing_node_filename = []
                            # 合并两个列表并去重
                            existing_node_filename.extend(node_filename)
                            existing_node_filename = list(set(existing_node_filename))

                            existing_node["filename"] = existing_node_filename
                            # 合并properties，后者覆盖前者
                            if hasattr(node, "properties") and node.properties:
                                if "properties" not in existing_node:
                                    existing_node["properties"] = {}
                                existing_node["properties"].update(node.properties)
                        else:
                            # 转换为字典格式存储
                            filename = getattr(node, "filename", [])
                            if isinstance(filename, str):
                                filename = [filename]
                            elif not isinstance(filename, list):
                                filename = []
                            merged_nodes[node_id] = {
                                "node_id": node.node_id,
                                "node_name": node.node_name,
                                "node_type": node.node_type,
                                "description": node.description,
                                "filename": filename,
                                "properties": getattr(node, "properties", {})
                            }

                # 合并边部分
                if "edges" in result:
                    for edge in result["edges"]:
                        source_id = edge.source_id  # 改为对象属性访问
                        target_id = edge.target_id  # 改为对象属性访问
                        relation_type = edge.relation_type  # 改为对象属性访问
                        # 以source_id、target_id、relation_type三者综合为唯一标识符
                        edge_key = (source_id, target_id, relation_type)
                        if edge_key in merged_edges:
                            # 按规则合并边
                            existing_edge = merged_edges[edge_key]
                            existing_edge["weight"] = getattr(edge, "weight", existing_edge["weight"])
                            existing_edge["bidirectional"] = getattr(edge, "bidirectional",
                                                                     existing_edge["bidirectional"])
                            # 合并properties，后者覆盖前者
                            if hasattr(edge, "properties") and edge.properties:
                                if "properties" not in existing_edge:
                                    existing_edge["properties"] = {}
                                existing_edge["properties"].update(edge.properties)
                        else:
                            # 转换为字典格式存储
                            merged_edges[edge_key] = {
                                "source_id": edge.source_id,
                                "target_id": edge.target_id,
                                "relation_type": edge.relation_type,
                                "weight": getattr(edge, "weight", 1.0),
                                "bidirectional": getattr(edge, "bidirectional", False),
                                "properties": getattr(edge, "properties", {})
                            }

            # 转换为列表格式返回
            final_result = {
                "nodes": list(merged_nodes.values()),
                "edges": list(merged_edges.values())
            }
            return final_result
        except Exception as e:
            print(f"从解析文档(markdown)中抽取图谱时出错: {str(e)}")
            # 打印完整的 traceback 信息
            exc_type, exc_value, exc_traceback = sys.exc_info()
            print("完整错误追踪:")
            traceback.print_exception(exc_type, exc_value, exc_traceback)
            raise e

    async def extract_kg_from_md(
            self,
            md_path: str,
            prompt: str,
            schema: dict,
            examples: list = None,
    ):
        """
        从解析文档(markdown)中抽取图谱
        """
        # print(schema)
        if '/' in md_path:
            bucket_name, file_name = md_path.split('/', 1)
        else:
            # 默认存储桶
            bucket_name = MD_BUCKET
            file_name = md_path
        try:
            # 使用MinIO客户端直接获取文件流
            response = self.file_storage.get_file_stream(bucket_name, file_name)
            if response:
                # 读取文件内容
                content = response.read()
                # 尝试解码为UTF-8文本
                try:
                    text_content = content.decode('utf-8')
                except UnicodeDecodeError:
                    # 如果UTF-8解码失败，尝试其他编码
                    try:
                        text_content = content.decode('gbk')
                    except UnicodeDecodeError:
                        # 如果都失败，返回原始字节的十六进制表示
                        text_content = content.hex()
                result = await self.graph_extract.extract_graph(
                    prompt=prompt,
                    schema=schema,
                    input_text=text_content,
                    examples=examples
                )
                result["filename"] = [file_name]
                return result
            else:
                print(f"无法从MinIO获取文件: {self.bucket}/{file_name}")
                return None
                # 在 extract_kg_from_md 方法的 except 块中
        except Exception as e:
            print(f"从MinIO获取文件内容时出错: {str(e)}")
            # 添加详细 traceback
            import traceback
            traceback.print_exc()
            return None


kg_extract_service = KGExtractService()
