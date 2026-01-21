import asyncio
import json
import logging
import os
import sys
import traceback
import gc

from dotenv import load_dotenv
from sqlalchemy.orm import Session

from app.infrastructure.graph_storage.factory import GraphStorageFactory
from app.infrastructure.information_extraction.graph_extraction import GraphExtraction
from app.infrastructure.information_extraction.method.base import ModelConfig
from app.infrastructure.storage.object_storage import StorageFactory
from app.services.tasks.kg_tasks import KGExtractionTaskManager
from app.schemas.kg import GraphNodeBase, GraphEdgeBase, TextClass
from app.utils.text_encoding_utils import TextEncodingUtils

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

# 内存阈值：超过此大小的文件将分块处理（默认50MB）
MEMORY_THRESHOLD = int(os.getenv("MEMORY_THRESHOLD", str(50 * 1024 * 1024)))  # 50MB
# 分块大小：每次读取的字节数（默认10MB）
CHUNK_READ_SIZE = int(os.getenv("CHUNK_READ_SIZE", str(10 * 1024 * 1024)))  # 10MB


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
                # model_name="qwen-long",# 45分钟
                model_name="qwen3-max", # 25分钟  # 14分钟
                api_key="sk-742c7c766efd4426bd60a269259aafaf",
                api_url="https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions",
            )
            # ModelConfig(
            #     model_name="glm-4-plus",  # 25分钟  # 1小时+ 输出json格式总不正确
            #     api_key="9072ce38f0654f809c6e1e488d017da9.E9zeF7NQn5hlHt18",
            #     api_url="https://open.bigmodel.cn/api/paas/v4/chat/completions",
            # )
            # ModelConfig(
            #     model_name="deepseek-chat",  # 25分钟  # 1小时+ 输出json格式总不正确
            #     api_key="sk-01f0b057b3c543cd9cfa6a0f01ca1614",
            #     api_url="https://api.deepseek.com/chat/completions",
            # )
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

    async def extract_kg_from_minio_paths(
            self,
            minio_files: list,
            user_prompt: str,
            prompt_parameters: dict = None,
            kg_level: str = "DomainLevel",
    ) -> dict | list[dict]:
        """
        当kg_level为"DomainLevel"，将多个图谱合并，返回的结果类型为dict
        当kg_level为"DocumentLevel", 保留每个文件的图谱和文件信息，返回的结果类型为list[dict]

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
            for minio_file in minio_files:
                minio_path = minio_file.get("minio_path")
                filename = minio_file.get("filename")
                if not minio_path or not filename:
                    continue
                parameters = {
                    "minio_path": minio_path,
                    "filename": filename,
                    "prompt": user_prompt,
                    "schema": prompt_parameters.get("schema", {}),
                    "examples": prompt_parameters.get("examples", [])
                }
                parameters_list.append(parameters)
            result_list = await self.kgExtractionTaskManager.run_async_tasks(
                self.extract_kg_from_minio,
                parameters_list
            )
            # # 保存result_list到测试数据目录
            # test_data_dir = r"F:\企业大脑知识库系统\洛书\cogmait-backend\tests\test_data"
            # os.makedirs(test_data_dir, exist_ok=True)  # 确保目录存在
            # result_file_path = os.path.join(test_data_dir, "result_list.json")
            # # 将result_list保存为JSON文件
            # with open(result_file_path, 'w', encoding='utf-8') as f:
            #     json.dump(result_list, f, ensure_ascii=False, indent=2, default=lambda obj: obj.__dict__)

            if kg_level == "DomainLevel":
                # 合并所有结果中的节点和边
                merged_nodes = {}
                merged_edges = {}
                # 改进1-7：使用字典合并节点和边
                merged_text_classes = {}
                for result in result_list:
                    if result is None:
                        continue
                    # try:
                    #     formatted_json = json.dumps(
                    #         result,
                    #         indent=2,
                    #         ensure_ascii=False,
                    #         default=lambda obj: str(obj) if hasattr(obj, '__dict__') else obj
                    #     )
                    #     print("提取结果的格式化JSON输出:")
                    #     print(formatted_json)
                    # except Exception as json_error:
                    #     print("JSON格式化错误:", json_error)
                    filename = result.get("filename")
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
                                node_filename = filename
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

                                # 改进1-7:合并source_text_info
                                if hasattr(node, "source_text_info") and node.source_text_info and isinstance(node.source_text_info, dict):
                                    if "source_text_info" not in existing_node:
                                        existing_node["source_text_info"] = {}
                                    for text_id, new_positions in node.source_text_info.items():
                                        if text_id in existing_node["source_text_info"]:
                                            # 合并位置并去重
                                            existing_positions = existing_node["source_text_info"][text_id]
                                            # 方法1: 简单追加（如果不担心重复）
                                            existing_positions.extend(new_positions)
                                            # # 方法2: 去重（基于字典比较）
                                            # # 将字典转为元组进行比较
                                            # seen = set()
                                            # unique_positions = []
                                            # for pos in existing_positions + new_positions:
                                            #     pos_tuple = tuple(sorted(pos.items()))
                                            #     if pos_tuple not in seen:
                                            #         seen.add(pos_tuple)
                                            #         unique_positions.append(pos)
                                            # existing_node["source_text_info"][text_id] = unique_positions
                                        else:
                                            # 新文档
                                            existing_node["source_text_info"][text_id] = new_positions.copy()
                            else:
                                # 转换为字典格式存储
                                # filename = getattr(node, "filename", [])
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
                                    "properties": getattr(node, "properties", {}),
                                    # 改进1-7:添加source_text_info
                                    "source_text_info": getattr(node, "source_text_info", {})
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
                                # 改进1-7:合并source_text_info
                                if hasattr(edge, "source_text_info") and edge.source_text_info and isinstance(edge.source_text_info, dict):
                                    if "source_text_info" not in existing_edge:
                                        existing_edge["source_text_info"] = {}
                                    for text_id, new_positions in edge.source_text_info.items():
                                        if text_id in existing_edge["source_text_info"]:
                                            # 合并位置并去重
                                            existing_positions = existing_edge["source_text_info"][text_id]
                                            # 方法1: 简单追加（如果不担心重复）
                                            existing_positions.extend(new_positions)
                                            # # 方法2: 去重（基于字典比较）
                                            # # 将字典转为元组进行比较
                                            # seen = set()
                                            # unique_positions = []
                                            # for pos in existing_positions + new_positions:
                                            #     pos_tuple = tuple(sorted(pos.items()))
                                            #     if pos_tuple not in seen:
                                            #         seen.add(pos_tuple)
                                            #         unique_positions.append(pos)
                                            # existing_edge["source_text_info"][text_id] = unique_positions
                                        else:
                                            # 新文档
                                            merged_edges[edge_key]["source_text_info"][text_id] = new_positions.copy()
                            else:
                                # 转换为字典格式存储
                                merged_edges[edge_key] = {
                                    "source_id": edge.source_id,
                                    "target_id": edge.target_id,
                                    "relation_type": edge.relation_type,
                                    "weight": getattr(edge, "weight", 1.0),
                                    "bidirectional": getattr(edge, "bidirectional", False),
                                    "properties": getattr(edge, "properties", {}),
                                    # 改进1-7:添加source_text_info
                                    "source_text_info": getattr(edge, "source_text_info", {})
                                }
                    # 合并文本节点
                    if "text_classes" in result:
                        text_classes = result["text_classes"]
                        if not isinstance(text_classes, list):
                            text_classes = []
                        for text_class in text_classes:
                            text_id = getattr(text_class, "text_id", None)
                            text = getattr(text_class, "text", None)
                            if not text_id or not text:
                                continue
                            if text_id in merged_text_classes:
                                existing_text = merged_text_classes[text_id].get("text", "")
                                # 只在新文本更长时才更新
                                if len(text) > len(existing_text):
                                    merged_text_classes[text_id]["text"] = text
                                if "filename" in merged_text_classes[text_id] and filename not in merged_text_classes[text_id]["filename"]:
                                    merged_text_classes[text_id]["filename"].append(filename)

                            else:
                                merged_text_classes[text_id] = {
                                    "text_id": text_id,
                                    "text": text,
                                    "filename": [filename]
                                }
                    # TODO:处理多溯源（可能用不到）
                # 转换为列表格式返回
                final_result = {
                    "nodes": list(merged_nodes.values()),
                    "edges": list(merged_edges.values()),
                    # 改进1-7：增加文本类，
                    "text_classes": list(merged_text_classes.values())
                }
                return final_result
            elif kg_level == "DocumentLevel":
                final_result = []
                for result in result_list:
                    nodes = []
                    edges = []
                    text_classes = []
                    if "nodes" in result:
                        for node in result["nodes"]:
                            nodes.append(node.model_dump())
                    if "edges" in result:
                        for edge in result["edges"]:
                            edges.append(edge.model_dump())
                    if "text_classes" in result:
                        for text_class in result["text_classes"]:
                            text_classes.append(text_class.model_dump())
                    final_result.append({
                        "nodes": nodes,
                        "edges": edges,
                        # TODO：暂时放弃溯源
                        "text_classes": text_classes
                    })
                return final_result
            else:
                logging.error("kg_level不支持!")
        except Exception as e:
            print(f"从解析文档(markdown)中抽取图谱时出错: {str(e)}")
            # 打印完整的 traceback 信息
            exc_type, exc_value, exc_traceback = sys.exc_info()
            print("完整错误追踪:")
            traceback.print_exception(exc_type, exc_value, exc_traceback)
            raise e

    async def extract_kg_from_tasks(
            self,
            kg_extract_params,
            kg_level: str = "DomainLevel"
    ) -> dict | list[dict]:
        """
        当kg_level为"DomainLevel"，将多个图谱合并，返回的结果类型为dict
        当kg_level为"DocumentLevel", 保留每个文件的图谱和文件信息，返回的结果类型为list[dict]

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
            parameters_list = []
            for kg_extract_param in kg_extract_params:
                task_id = kg_extract_param.get("task_id")
                user_prompt = kg_extract_param.get("user_prompt", "")
                prompt_parameters = kg_extract_param.get("prompt_parameters", {})
                minio_path = kg_extract_param.get("minio_path")
                filename = kg_extract_param.get("filename")
                if not minio_path or not filename or not task_id:
                    continue
                parameters = {
                    "task_id": task_id,
                    "minio_path": minio_path,
                    "filename": filename,
                    "prompt": user_prompt,
                    "schema": prompt_parameters.get("schema", {}),
                    "examples": prompt_parameters.get("examples", [])
                }
                parameters_list.append(parameters)
            task_result_list = await self.kgExtractionTaskManager.run_async_tasks(
                self.extract_kg_from_minio_with_kg_name,
                parameters_list
            )
            result_map = {}
            for result in task_result_list:
                task_id = result.get("task_id", "")
                if not task_id:
                    continue
                if task_id not in result_map:
                    result_map[task_id] = [result]
                else:
                    result_map[task_id].append(result)
            final_result_map = {}
            for task_id, result_list in result_map.items():
                if kg_level == "DomainLevel":
                    # 合并所有结果中的节点和边
                    merged_nodes = {}
                    merged_edges = {}
                    # 改进1-7：使用字典合并节点和边
                    merged_text_classes = {}
                    for result in result_list:
                        if result is None:
                            continue
                        filename = result.get("filename")
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
                                    node_filename = filename
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

                                    # 改进1-7:合并source_text_info
                                    if hasattr(node, "source_text_info") and node.source_text_info and isinstance(node.source_text_info, dict):
                                        if "source_text_info" not in existing_node:
                                            existing_node["source_text_info"] = {}
                                        for text_id, new_positions in node.source_text_info.items():
                                            if text_id in existing_node["source_text_info"]:
                                                # 合并位置并去重
                                                existing_positions = existing_node["source_text_info"][text_id]
                                                # 方法1: 简单追加（如果不担心重复）
                                                existing_positions.extend(new_positions)
                                            else:
                                                # 新文档
                                                existing_node["source_text_info"][text_id] = new_positions.copy()
                                else:
                                    # 转换为字典格式存储
                                    # filename = getattr(node, "filename", [])
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
                                        "properties": getattr(node, "properties", {}),
                                        # 改进1-7:添加source_text_info
                                        "source_text_info": getattr(node, "source_text_info", {})
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
                                    # 改进1-7:合并source_text_info
                                    if hasattr(edge, "source_text_info") and edge.source_text_info and isinstance(edge.source_text_info, dict):
                                        if "source_text_info" not in existing_edge:
                                            existing_edge["source_text_info"] = {}
                                        for text_id, new_positions in edge.source_text_info.items():
                                            if text_id in existing_edge["source_text_info"]:
                                                # 合并位置并去重
                                                existing_positions = existing_edge["source_text_info"][text_id]
                                                # 方法1: 简单追加（如果不担心重复）
                                                existing_positions.extend(new_positions)
                                            else:
                                                # 新文档
                                                merged_edges[edge_key]["source_text_info"][text_id] = new_positions.copy()
                                else:
                                    # 转换为字典格式存储
                                    merged_edges[edge_key] = {
                                        "source_id": edge.source_id,
                                        "target_id": edge.target_id,
                                        "relation_type": edge.relation_type,
                                        "weight": getattr(edge, "weight", 1.0),
                                        "bidirectional": getattr(edge, "bidirectional", False),
                                        "properties": getattr(edge, "properties", {}),
                                        # 改进1-7:添加source_text_info
                                        "source_text_info": getattr(edge, "source_text_info", {})
                                    }
                        # 合并文本节点
                        if "text_classes" in result:
                            text_classes = result["text_classes"]
                            if not isinstance(text_classes, list):
                                text_classes = []
                            for text_class in text_classes:
                                text_id = getattr(text_class, "text_id", None)
                                text = getattr(text_class, "text", None)
                                if not text_id or not text:
                                    continue
                                if text_id in merged_text_classes:
                                    existing_text = merged_text_classes[text_id].get("text", "")
                                    # 只在新文本更长时才更新
                                    if len(text) > len(existing_text):
                                        merged_text_classes[text_id]["text"] = text
                                    if "filename" in merged_text_classes[text_id] and filename not in merged_text_classes[text_id]["filename"]:
                                        merged_text_classes[text_id]["filename"].append(filename)

                                else:
                                    merged_text_classes[text_id] = {
                                        "text_id": text_id,
                                        "text": text,
                                        "filename": [filename]
                                    }
                        # TODO:处理多溯源（可能用不到）
                    # 转换为列表格式返回
                    final_result = {
                        "nodes": list(merged_nodes.values()),
                        "edges": list(merged_edges.values()),
                        # 改进1-7：增加文本类，
                        "text_classes": list(merged_text_classes.values())
                    }
                    final_result_map[task_id] = final_result
                elif kg_level == "DocumentLevel":
                    final_result = []
                    for result in result_list:
                        nodes = []
                        edges = []
                        text_classes = []
                        if "nodes" in result:
                            for node in result["nodes"]:
                                nodes.append(node.model_dump())
                        if "edges" in result:
                            for edge in result["edges"]:
                                edges.append(edge.model_dump())
                        if "text_classes" in result:
                            for text_class in result["text_classes"]:
                                text_classes.append(text_class.model_dump())
                        final_result.append({
                            "nodes": nodes,
                            "edges": edges,
                            # TODO：暂时放弃溯源
                            "text_classes": text_classes
                        })
                    final_result_map[task_id] = final_result
                else:
                    logging.error("kg_level不支持!")
            return final_result_map
        except Exception as e:
            print(f"从解析文档(markdown)中抽取图谱时出错: {str(e)}")
            # 打印完整的 traceback 信息
            exc_type, exc_value, exc_traceback = sys.exc_info()
            print("完整错误追踪:")
            traceback.print_exception(exc_type, exc_value, exc_traceback)
            raise e

    async def extract_kg_from_minio(
            self,
            minio_path: str,
            filename: str,
            prompt: str,
            schema: dict,
            examples: list = None,
    ):
        """
        从解析文档(markdown)中抽取图谱
        支持大文件分块处理，避免内存溢出
        """
        # print(schema)
        if '/' in minio_path:
            bucket_name, minio_file_name = minio_path.split('/', 1)
        else:
            # 默认存储桶
            bucket_name = MD_BUCKET
            minio_file_name = minio_path
        try:
            # 先获取文件元数据，判断文件大小
            file_metadata = await asyncio.get_event_loop().run_in_executor(
                None, self.file_storage.get_file_metadata, bucket_name, minio_file_name
            )
            file_size = file_metadata.size if file_metadata else None
            
            # 如果文件过大，使用分块处理
            if file_size and file_size > MEMORY_THRESHOLD:
                print(f"文件大小 {file_size / (1024 * 1024):.2f}MB 超过阈值 {MEMORY_THRESHOLD / (1024 * 1024):.2f}MB，启用分块处理")
                result = await self._extract_kg_from_minio_chunked(
                    bucket_name, minio_file_name, prompt, schema, examples
                )
            else:
                # 小文件，使用原有方式处理
                result = await self._extract_kg_from_minio_normal(
                    bucket_name, minio_file_name, prompt, schema, examples
                )
            if result and isinstance(result, dict):
                result["filename"] = filename
            return result

        except Exception as e:
            print(f"从MinIO获取文件内容时出错: {str(e)}")
            # 添加详细 traceback
            import traceback
            traceback.print_exc()
            return None

    async def extract_kg_from_minio_with_kg_name(
            self,
            task_id: str,
            minio_path: str,
            filename: str,
            prompt: str,
            schema: dict,
            examples: list = None,
    ):
        """
        从解析文档(markdown)中抽取图谱
        支持大文件分块处理，避免内存溢出
        """
        if '/' in minio_path:
            bucket_name, minio_file_name = minio_path.split('/', 1)
        else:
            # 默认存储桶
            bucket_name = MD_BUCKET
            minio_file_name = minio_path
        try:
            # 先获取文件元数据，判断文件大小
            file_metadata = await asyncio.get_event_loop().run_in_executor(
                None, self.file_storage.get_file_metadata, bucket_name, minio_file_name
            )
            file_size = file_metadata.size if file_metadata else None

            # 如果文件过大，使用分块处理
            if file_size and file_size > MEMORY_THRESHOLD:
                print(
                    f"文件大小 {file_size / (1024 * 1024):.2f}MB 超过阈值 {MEMORY_THRESHOLD / (1024 * 1024):.2f}MB，启用分块处理")
                result = await self._extract_kg_from_minio_chunked(
                    bucket_name, minio_file_name, prompt, schema, examples
                )
            else:
                # 小文件，使用原有方式处理
                result = await self._extract_kg_from_minio_normal(
                    bucket_name, minio_file_name, prompt, schema, examples
                )
            if result and isinstance(result, dict):
                result["filename"] = filename
                result["task_id"] = task_id
            # TODO：针对法律文件，将其中的法规文本统一成相同名称

            return result

        except Exception as e:
            print(f"从MinIO获取文件内容时出错: {str(e)}")
            # 添加详细 traceback
            import traceback
            traceback.print_exc()
            return None

    async def _extract_kg_from_minio_normal(
            self,
            bucket_name: str,
            file_name: str,
            prompt: str,
            schema: dict,
            examples: list = None,
    ):
        """
        正常方式处理文件（小文件）
        """
        # 使用线程池执行同步的文件读取操作，避免阻塞事件循环
        loop = asyncio.get_event_loop()
        # 在线程池中执行 get_file_stream 操作
        response = await loop.run_in_executor(None, self.file_storage.get_file_stream, bucket_name, file_name)

        if not response:
            print(f"无法从MinIO获取文件: {bucket_name}/{file_name}")
            return None
        
        try:
            # 读取文件内容
            content = response.read()
            # 使用工具类解码
            text_content = TextEncodingUtils.decode_chunk(content, 'utf-8')
            if not text_content:
                # 如果解码失败，返回原始字节的十六进制表示
                text_content = content.hex()
            
            result = await self.graph_extract.extract_graph(
                prompt=prompt,
                schema=schema,
                input_text=text_content,
                examples=examples
            )
            return result
        finally:
            # 确保资源释放
            if hasattr(response, 'close'):
                response.close()
            del content
            gc.collect()

    async def _extract_kg_from_minio_chunked(
            self,
            bucket_name: str,
            file_name: str,
            prompt: str,
            schema: dict,
            examples: list = None,
    ):
        """
        分块处理大文件，避免内存溢出
        每块处理完后释放内存，最后合并结果
        """
        # 使用线程池执行同步的文件读取操作，避免阻塞事件循环
        loop = asyncio.get_event_loop()
        # 在线程池中执行 get_file_stream 操作
        response = await loop.run_in_executor(None, self.file_storage.get_file_stream, bucket_name, file_name)

        if not response:
            print(f"无法从MinIO获取文件: {bucket_name}/{file_name}")
            return None
        
        # 检测文件编码（使用工具类）
        encoding = TextEncodingUtils.detect_encoding(response)
        # 重置流位置（如果支持）
        if hasattr(response, 'seek'):
            try:
                response.seek(0)
            except Exception:
                # 如果seek失败，重新获取流
                response = self.file_storage.get_file_stream(bucket_name, file_name)
                if not response:
                    print(f"无法重新获取文件流: {bucket_name}/{file_name}")
                    return None
        
        all_results = []
        chunk_index = 0
        buffer = b''
        
        try:
            while True:
                # 读取一块数据
                chunk_bytes = response.read(CHUNK_READ_SIZE)
                if not chunk_bytes:
                    # 没有更多数据，处理剩余的buffer（使用工具类）
                    if buffer:
                        text_chunk = TextEncodingUtils.decode_chunk(buffer, encoding)
                        if text_chunk:
                            result = await self._process_text_chunk(
                                text_chunk, prompt, schema, examples, chunk_index
                            )
                            if result:
                                all_results.append(result)
                            # 释放内存
                            del text_chunk, result
                            gc.collect()
                    break
                
                # 将新读取的数据添加到buffer
                buffer += chunk_bytes
                
                # 尝试解码buffer（使用工具类）
                text_chunk, remaining_buffer = TextEncodingUtils.decode_chunk_safe(buffer, encoding)
                
                if text_chunk:
                    # 成功解码，处理这个文本块
                    logging.info(f"处理第 {chunk_index + 1} 个文本块，大小: {len(text_chunk)} 字符")
                    result = await self._process_text_chunk(
                        text_chunk, prompt, schema, examples, chunk_index
                    )
                    if result:
                        all_results.append(result)
                    
                    # 释放内存
                    del text_chunk, result
                    gc.collect()
                    
                    chunk_index += 1
                
                # 更新buffer为剩余部分
                buffer = remaining_buffer
            
            # 合并所有结果
            if not all_results:
                print("所有文本块处理完成，但未获得任何结果")
                return None
            
            print(f"共处理 {len(all_results)} 个文本块，开始合并结果")
            merged_result = self._merge_chunk_results(all_results)
            
            # 释放内存
            del all_results
            gc.collect()
            
            return merged_result
            
        except Exception as e:
            print(f"分块处理文件时出错: {str(e)}")
            import traceback
            traceback.print_exc()
            return None
        finally:
            # 确保资源释放
            if hasattr(response, 'close'):
                response.close()
            del buffer
            gc.collect()

    async def _process_text_chunk(
            self,
            text_chunk: str,
            prompt: str,
            schema: dict,
            examples: list,
            chunk_index: int
    ):
        """
        处理单个文本块
        """
        if not text_chunk or len(text_chunk.strip()) == 0:
            return None
        
        try:
            result = await self.graph_extract.extract_graph(
                prompt=prompt,
                schema=schema,
                input_text=text_chunk,
                examples=examples
            )
            return result
        except Exception as e:
            print(f"处理第 {chunk_index + 1} 个文本块时出错: {str(e)}")
            return None

    def _merge_chunk_results(self, results: list) -> dict:
        """
        合并多个文本块的抽取结果
        返回格式与 extract_graph 一致，包含 GraphNodeBase 和 GraphEdgeBase 对象
        """
        if not results:
            return {}
        
        # 初始化合并结果 - 使用对象映射
        merged_nodes = {}  # node_id -> GraphNodeBase
        merged_edges = {}  # (source_id, target_id, relation_type) -> GraphEdgeBase
        merged_text_classes = {}  # text_id -> TextClass
        
        for result in results:
            if not result:
                continue
            
            # 合并节点
            if "nodes" in result:
                for node in result["nodes"]:
                    node_id = getattr(node, "node_id", None)
                    if not node_id:
                        continue
                    
                    if node_id in merged_nodes:
                        # 合并节点属性
                        existing_node = merged_nodes[node_id]
                        # 更新属性（保留更详细的信息）
                        if hasattr(node, "node_name") and node.node_name:
                            existing_node.node_name = node.node_name
                        if hasattr(node, "node_type") and node.node_type:
                            existing_node.node_type = node.node_type
                        if hasattr(node, "description") and node.description:
                            existing_node.description = node.description
                        
                        # 合并properties
                        node_properties = getattr(node, "properties", {})
                        if node_properties and isinstance(node_properties, dict):
                            if existing_node.properties is None:
                                existing_node.properties = {}
                            existing_node.properties.update(node_properties)
                        
                        # 合并source_text_info
                        if hasattr(node, "source_text_info") and node.source_text_info:
                            if existing_node.source_text_info is None:
                                existing_node.source_text_info = {}
                            if isinstance(existing_node.source_text_info, dict) and isinstance(node.source_text_info, dict):
                                for key, value in node.source_text_info.items():
                                    if key in existing_node.source_text_info:
                                        # 合并列表
                                        if isinstance(existing_node.source_text_info[key], list) and isinstance(value, list):
                                            existing_node.source_text_info[key].extend(value)
                                        elif isinstance(value, list):
                                            existing_node.source_text_info[key] = value
                                    else:
                                        existing_node.source_text_info[key] = value
                    else:
                        # 新节点 - 创建新的 GraphNodeBase 对象
                        new_node = GraphNodeBase(
                            node_id=node_id,
                            node_name=getattr(node, "node_name", ""),
                            node_type=getattr(node, "node_type", ""),
                            description=getattr(node, "description", None),
                            properties=getattr(node, "properties", {})
                        )
                        # 复制其他属性
                        if hasattr(node, "source_text_info"):
                            new_node.source_text_info = getattr(node, "source_text_info", None)
                        merged_nodes[node_id] = new_node
            
            # 合并边
            if "edges" in result:
                for edge in result["edges"]:
                    source_id = getattr(edge, "source_id", None)
                    target_id = getattr(edge, "target_id", None)
                    relation_type = getattr(edge, "relation_type", None)
                    
                    if not all([source_id, target_id, relation_type]):
                        continue
                    
                    edge_key = (source_id, target_id, relation_type)
                    if edge_key in merged_edges:
                        # 合并边属性
                        existing_edge = merged_edges[edge_key]
                        existing_edge.weight = getattr(edge, "weight", existing_edge.weight)
                        existing_edge.bidirectional = getattr(edge, "bidirectional", existing_edge.bidirectional)
                        
                        # 合并properties
                        edge_properties = getattr(edge, "properties", {})
                        if edge_properties and isinstance(edge_properties, dict):
                            if existing_edge.properties is None:
                                existing_edge.properties = {}
                            existing_edge.properties.update(edge_properties)
                    else:
                        # 新边 - 创建新的 GraphEdgeBase 对象
                        new_edge = GraphEdgeBase(
                            source_id=source_id,
                            target_id=target_id,
                            relation_type=relation_type,
                            weight=getattr(edge, "weight", 1.0),
                            bidirectional=getattr(edge, "bidirectional", False),
                            properties=getattr(edge, "properties", {})
                        )
                        # 复制其他属性
                        if hasattr(edge, "source_text_info"):
                            new_edge.source_text_info = getattr(edge, "source_text_info", None)
                        merged_edges[edge_key] = new_edge
            
            # 合并文本类（去重）
            if "text_classes" in result:
                for text_class in result["text_classes"]:
                    text_id = getattr(text_class, "text_id", None)
                    if text_id and text_id not in merged_text_classes:
                        merged_text_classes[text_id] = TextClass(
                            text_id=text_id,
                            text=getattr(text_class, "text", "")
                        )
        
        # 构建最终结果 - 返回对象列表
        final_result = {
            "nodes": list(merged_nodes.values()),
            "edges": list(merged_edges.values()),
            "text_classes": list(merged_text_classes.values()),
        }
        
        print(f"合并完成: {len(final_result['nodes'])} 个节点, {len(final_result['edges'])} 条边")
        return final_result


kg_extract_service = KGExtractService()
