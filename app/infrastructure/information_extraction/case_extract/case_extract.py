import asyncio
import json
import logging
import re

from app.infrastructure.graph_storage.neo4j_adapter import Neo4jAdapter
from app.infrastructure.information_extraction.base import Entity, Relationship
from app.infrastructure.information_extraction.case_extract.llm_tool.content_AI_chunking import LegalDocumentAIChunker
from app.infrastructure.information_extraction.case_extract.prompt.administrative_judgment_of_second_instance import \
    administrative_judgment_of_second_instance
from app.infrastructure.information_extraction.case_extract.prompt.civil_judgment_of_first_instance import \
    civil_judgment_of_first_instance
from app.infrastructure.information_extraction.case_extract.prompt.civil_judgment_of_retrial import \
    civil_judgment_of_retrial
from app.infrastructure.information_extraction.case_extract.prompt.civil_judgment_of_second_instance import \
    civil_judgment_of_second_instance
from app.infrastructure.information_extraction.case_extract.prompt.administrative_judgment_of_first_instance import \
    administrative_judgment_of_first_instance
from app.infrastructure.information_extraction.case_extract.prompt.default_prompt import default_legal_structure, \
    default_case_structure, default_chunking_prompt_v2
from app.infrastructure.information_extraction.factory import InformationExtractionFactory
from app.infrastructure.information_extraction.method.base import LangextractConfig
from app.infrastructure.string_utils.id_tool import generate_hex_uuid
from app.infrastructure.string_utils.str_clean import clean_string_with_only_words, clean_string_for_neo4j_extended, \
    replace_full_corner_space, replace_zero_width_chars

CASE_MADEL = "qwen3-30b-a3b-instruct-2507"
CASE_MADEL_API = "gpustack_342609ce423be29a_4371426b285a91dc44fb4e8d72454847"
CASE_MADEL_KEY = "http://222.171.219.26:20001/v1/chat/completions"

# MAX_CHUNK_SIZE = int(os.getenv("MAX_CHUNK_SIZE", "5000"))
# BATCH_LENGTH = int(os.getenv("BATCH_LENGTH", "5"))
# MAX_WORKERS = int(os.getenv("MAX_WORKERS", "3"))
# TIMEOUT = int(os.getenv("TIMEOUT", "300"))

MAX_CHAR_BUFFER = 7500
BATCH_LENGTH = 5
MAX_WORKERS = 3
TIMEOUT = 3000


class ResultStats:
    def __init__(self):
        self.error = 0
        self.error_msg = ""
        self.week_warning = 0
        self.week_warning_msg = ""
        self.strong_warning = 0
        self.strong_warning_msg = ""


class CaseCache:
    def __init__(self):
        self.chunks = {}
        self.extracted = {}


class CaseExtractor:
    def __init__(self, max_concurrent: int = 20):
        self.extractor_config = LangextractConfig(
            model_name=CASE_MADEL,
            api_key=CASE_MADEL_API,
            api_url=CASE_MADEL_KEY,
            config={
                "timeout": TIMEOUT
            },
            max_char_buffer=MAX_CHAR_BUFFER,
            batch_length=BATCH_LENGTH,
            max_workers=MAX_WORKERS,
            # resolver_params=
        )
        self.extractor = InformationExtractionFactory.create(
            "langextract",
            max_retries=5,
            config=self.extractor_config
        )
        self.semaphore = asyncio.Semaphore(max_concurrent)  # 添加信号量
        self.chunking_splitter = LegalDocumentAIChunker()
        self.result_stats = ResultStats()

        # 宽松模式标志（True则将处理失败的条款直接作为法条实体添加到图谱中（不考虑法条信息、））
        self.lenient_mode = False

    async def extract_cases(
            self,
            filename: str,
            case_type: str,
            text: str
    ) -> dict:
        """
        实现功能：从诉讼文书中抽取条款知识图谱数据
        :param filename:
        :param case_type:
        :param text:
        :return:
        """
        try:
            logging.info("📄⏳:开始诉讼文书知识图谱抽取")
            # 分割诉讼文本
            logging.info("📄:开始分割诉讼文本")
            chunks_data = await self.split_chunks(
                case_type=case_type,
                text=text
            )
            logging.info("📄:结束分割条款")
            if not chunks_data:
                logging.error(f"📄❌：诉讼分块内容为空！处理文件：{filename}")
                self.result_stats.error += 1
                self.result_stats.error_msg += f"诉讼分块内容为空！处理文件：{filename}"
                raise ValueError("诉讼分块内容为空！处理文件：{filename}")
            # 保存分割缓存
            """
            缓存cache为CaseCache对象，其中的cache.chunks记录结构：
            {
                "结构名称": "分块内容",
                ...
            }
            cache.extracted记录结构
            {
                "结构名称":{
                    nodes: "节点",
                    edges: "边"
                },
                ...
            }
            若要执行缓存，如果cache.chunks不为空，则跳过分块使用cache.chunks；
            如果cache.extracted不为空，则通过 chunks["结构名称"] 等于 缓存extracted的"结构名称"，
            将cache.extracted中已经处理过的数据从chunks中删除，然后对chunks中未处理过的数据进行处理
            """
            logging.info("📄:保存分割缓存")
            case_cache = CaseCache()
            case_cache.chunks = chunks_data
            # 构建初步图谱
            logging.info("📄:开始对分块进行图谱抽取")
            kg_data = await self.extract_chunk_kg(
                filename=filename,
                case_type=case_type,
                chunks_data=chunks_data,
                case_cache=case_cache
            )
            logging.info("📄:结束对分块进行图谱抽取")
            return kg_data
        except Exception as e:
            # TODO: 如果缓存中存在结果，将缓存保存起来
            logging.error("📄❌：条款知识图谱抽取报错: %s", e)
            raise e

    async def split_chunks(
            self,
            case_type: str,
            text: str
    ) -> dict:
        """
实现功能：切分法规文件，按照诉讼文书结构分割文本：
1. 针对不同的诉讼文书类型，使用不同的prompt进行分割抽取；
2. 调用LLM进行分割抽取；
3. 检验切分结果，确保切分的准确性；
4. 返回切分后的数据结构；
    切分后的数据结构：
    {
        "文本结构": "文本结构内容"
    }
        :param case_type:
        :param text:
        :return:
        """
        try:
            # 判断诉讼文书类型
            user_prompt = None
            structure_list = []
            if case_type == "一审民事判决书":
                logging.info("📄:开始处理一审民事判决书分块")
                user_prompt = civil_judgment_of_first_instance.chunking_prompt_v2()
                _, structure_list = civil_judgment_of_first_instance.legal_structure()
            elif case_type == "二审民事判决书":
                logging.info("📄:开始处理二审民事判决书分块")
                user_prompt = civil_judgment_of_second_instance.chunking_prompt_v2()
                _, structure_list = civil_judgment_of_second_instance.legal_structure()
            elif case_type == "再审民事判决书":
                user_prompt = civil_judgment_of_retrial.chunking_prompt_v2()
                _, structure_list = civil_judgment_of_retrial.legal_structure()
                logging.info("📄:开始处理再审民事判决书分块")
            elif case_type == "一审行政判决书":
                user_prompt = administrative_judgment_of_first_instance.chunking_prompt_v2()
                _, structure_list = administrative_judgment_of_first_instance.legal_structure()
                logging.info("📄:开始处理一审行政判决书分块")
            elif case_type == "二审行政判决书":
                user_prompt = administrative_judgment_of_second_instance.chunking_prompt_v2()
                _, structure_list = administrative_judgment_of_second_instance.legal_structure()
                logging.info("📄:开始处理二审行政判决书分块")
            elif case_type == "test":
                logging.info("📄:使用默认的prompt开始文件分块")
                user_prompt = default_chunking_prompt_v2()
                _, structure_list = default_legal_structure()
            else:
                logging.error("📄❌：无效的诉讼文书类型")
                self.result_stats.error += 1
                self.result_stats.error_msg += "无效的诉讼文书类型\n"
                raise ValueError("无效的诉讼文书类型")
            # 抽取分块内容
            logging.info("📄🎯:开始抽取分块内容")
            chunks = await self.chunking_splitter.chunk_legal_document(
                text,
                user_prompt
            )
            # 检验分块结果
            if not isinstance(chunks, dict):
                logging.error("📄❌：分块结果无效")
                self.result_stats.error += 1
                self.result_stats.error_msg += "分块结果无效\n"
                raise ValueError("分块结果无效")
            final_chunks = {}
            for structure in structure_list:
                chunk = chunks.get(structure)
                if not chunk:
                    logging.warning(f"📄⚠️强警告：分块结果中缺少结构{structure}\n分块结果包含：{chunks.keys()}")
                    self.result_stats.strong_warning += 1
                    self.result_stats.strong_warning_msg += f"分块结果中缺少结构{structure}\n分块结果包含：{chunks.keys()}\n"
                    continue
                final_chunks[structure] = chunk
            logging.info("📄🎯:结束抽取分块内容")
            return final_chunks
        except Exception as e:
            logging.error(f"📄❌：分块文件时出错 - {e}")
            self.result_stats.error += 1
            self.result_stats.error_msg += f"分块文件时出错 - {e}\n"
            raise ValueError(f"分块文件时出错 - {e}")

    async def extract_chunk_kg(
            self,
            filename: str,
            case_type: str,
            chunks_data: dict,
            case_cache: CaseCache
    ) -> dict:
        """
        对分块内容进程图谱抽取，先抽取诉讼基础信息，再对分块内容抽取条款知识图谱数据
        根据case_type类型，获取对应structure结构，结构为：
        {
            "诉讼基础信息": structure_schema,
            "诉讼其他结构": [
                structure_schema,
                ...
            ]
        }
        structure_schema结构：
        {
            "chunks": [
                default_head.get("name"),
                ...
            ],
            "nodes": "",
            "edges": "",
            "structure_prompt": ""
        }
        图谱数据结构：
        {
          "node_id": "",
          "node_name": "",
          "node_type": "",
          "properties": {},
        }
        先抽取诉讼基础信息，然后构建基础图谱数据，
        然后对分块内容进行图谱多线程同步抽取，最后合并图谱数据
        :param filename:
        :param case_type:
        :param chunks_data:
        :param case_cache:
        :return:
        """
        try:
            # print(f"chunks_data: {chunks_data}")
            if case_type == "一审民事判决书":
                logging.info("📄:开始处理一审民事判决书分块图谱抽取")
                case_structure = civil_judgment_of_first_instance.case_structure
                pass
            elif case_type == "二审民事判决书":
                case_structure = civil_judgment_of_second_instance.case_structure
                pass
            elif case_type == "再审民事判决书":
                case_structure = civil_judgment_of_retrial.case_structure
                pass
            elif case_type == "一审行政判决书":
                case_structure = administrative_judgment_of_first_instance.case_structure
            elif case_type == "二审行政判决书":
                case_structure = administrative_judgment_of_second_instance.case_structure
            elif case_type == "test":
                logging.info("📄：无效的诉讼文书类型,使用默认结构")
                case_structure = default_case_structure
            else:
                logging.error("📄❌：无效的诉讼文书类型")
                self.result_stats.error += 1
                self.result_stats.error_msg += "无效的诉讼文书类型\n"
                raise ValueError("无效的诉讼文书类型")
            # 最终图谱
            final_kg = {
                "nodes": [],
                "edges": []
            }
            # 诉讼id
            litigation_id = None
            litigation_name = None
            # 节点id映射（type_name->node_id）
            node_id_mapping = {}
            # 诉讼基础信息
            litigation_basic_info_schema = case_structure.get("诉讼基础信息")
            if not isinstance(litigation_basic_info_schema, dict):
                logging.error("📄❌：诉讼基础信息结构无效")
                self.result_stats.error += 1
                self.result_stats.error_msg += "诉讼基础信息结构无效\n"
                raise ValueError("诉讼基础信息结构无效")
            # 诉讼其他结构
            litigation_schemas = case_structure.get("诉讼其他结构")
            if not isinstance(litigation_schemas, list):
                logging.error("📄❌：诉讼其他结构无效")
                self.result_stats.error += 1
                self.result_stats.error_msg += "诉讼其他结构无效\n"
                raise ValueError("诉讼其他结构无效")
            logging.info("📄:开始处理诉讼基础信息")
            # 整理诉讼基础信息
            lbi_user_prompt = litigation_basic_info_schema.get("structure_prompt")
            if not lbi_user_prompt:
                logging.error("📄❌：诉讼基础信息缺少提示词")
                self.result_stats.error += 1
                self.result_stats.error_msg += "诉讼基础信息缺少提示词\n"
                raise ValueError("诉讼基础信息缺少提示词")
            lbi_nodes = litigation_basic_info_schema.get("nodes")
            lbi_edges = litigation_basic_info_schema.get("edges")
            if not lbi_nodes or not lbi_edges:
                logging.error("📄❌：诉讼基础信息缺少节点或边")
                self.result_stats.error += 1
                self.result_stats.error_msg += "诉讼基础信息缺少节点或边\n"
                raise ValueError("诉讼基础信息缺少节点或边")
            lbi_schema = {
                "nodes": lbi_nodes,
                "edges": lbi_edges
            }
            lbi_input_text = ""
            # 诉讼基础信息包含多个诉讼结构
            chunk_structures = litigation_basic_info_schema.get("chunks")
            if not chunk_structures:
                logging.error("📄❌：诉讼基础信息结构无效")
                self.result_stats.error += 1
                self.result_stats.error_msg += "诉讼基础信息结构无效\n"
                raise ValueError("诉讼基础信息结构无效")
            for chunk_structure in chunk_structures:
                chunk_content = chunks_data.get(chunk_structure)
                if not chunk_content:
                    logging.warning(f"📄⚠️警告：分块结果中缺少结构{chunk_structure}")
                    self.result_stats.strong_warning += 1
                    self.result_stats.strong_warning_msg += f"分块结果中缺少结构{chunk_structure}\n"
                    continue
                lbi_input_text += chunk_content + "\n"
            lbi_examples = litigation_basic_info_schema.get("examples", [])
            # print(f"------------lbi_user_prompt------------\n{lbi_user_prompt}")
            # print(f"------------lbi_schema------------\n{lbi_schema}")
            # print(f"------------lbi_input_text------------\n{lbi_input_text}")
            # print(f"------------lbi_examples------------\n{lbi_examples}")
            # temp_config = self.extractor.get_config()
            # if temp_config.temperature is not None:
            #     temp_config.temperature = 0.1
            langextract_config = LangextractConfig(
                model_name=CASE_MADEL,
                api_key=CASE_MADEL_API,
                api_url=CASE_MADEL_KEY,
                config={
                    "timeout": TIMEOUT
                },
                temperature=0.1
            )
            lbi_extract_result = await self.extractor.entity_and_relationship_extract(
                user_prompt=lbi_user_prompt,
                schema=lbi_schema,
                input_text=lbi_input_text,
                examples=lbi_examples,
                langextract_config=langextract_config
            )
            if not lbi_extract_result:
                logging.error("📄❌：诉讼基础信息抽取结果无效")
                self.result_stats.error += 1
                self.result_stats.error_msg += "诉讼基础信息抽取结果无效\n"
                raise ValueError("诉讼基础信息抽取结果无效")
            # 保存诉讼基础信息缓存
            case_cache.extracted["诉讼基础信息"] = lbi_extract_result
            lbi_entities = lbi_extract_result.get("entities", [])
            lbi_relations = lbi_extract_result.get("relations", [])
            # 多余诉讼标志，一个诉讼基础信息中只能有一个诉讼文件，处理一个诉讼后置标志为真
            litigation_processed = False
            for entity in lbi_entities:
                if not isinstance(entity, Entity):
                    logging.error(f"📄❌：实体类型错误 - {entity}")
                    self.result_stats.error += 1
                    self.result_stats.error_msg += f"实体类型错误 - {entity}\n"
                # 保存文件信息
                entity_type = clean_string_with_only_words(entity.entity_type)
                if entity_type == "诉讼":
                    # 处理诉讼节点
                    if litigation_processed:
                        logging.warning("📄警告：一个诉讼基础信息中只能有一个诉讼，请检查问题")
                        self.result_stats.strong_warning += 1
                        self.result_stats.strong_warning_msg += "一个诉讼基础信息中只能有一个诉讼，请检查问题\n"
                        continue
                    node_name = entity.name
                    if not node_name:
                        logging.warning("📄警告：诉讼名称为空")
                        self.result_stats.strong_warning += 1
                        self.result_stats.strong_warning_msg += "诉讼文件名称为空\n"
                        continue
                    node_id = clean_string_for_neo4j_extended(f"{entity_type}_{generate_hex_uuid()}")
                    # 保存诉讼节点数据
                    litigation_id = node_id
                    litigation_name = clean_string(node_name)
                    node_id_mapping[f"{entity_type}_{litigation_name}"] = node_id
                    final_kg["nodes"].append({
                        "node_id": node_id,
                        "node_name": litigation_name,
                        "node_type": entity_type,
                        "properties": entity.properties
                    })
                    litigation_processed = True
                else:
                    # 处理其他节点
                    node_name = clean_string(entity.name)
                    if not node_name:
                        logging.warning("📄警告：法规依据名称为空")
                        continue
                    node_id = clean_string_for_neo4j_extended(f"{entity_type}_{generate_hex_uuid()}")
                    # 保存其他节点数据
                    node_id_mapping[f"{entity_type}_{node_name}"] = node_id
                    final_kg["nodes"].append({
                        "node_id": node_id,
                        "node_name": node_name,
                        "node_type": entity_type,
                        "properties": entity.properties
                    })
            # 处理诉讼基础信息关系
            for relation in lbi_relations:
                try:
                    if not isinstance(relation, Relationship):
                        logging.error(f"📄❌：关系类型错误 - {relation}")
                        self.result_stats.error += 1
                        self.result_stats.error_msg += f"关系类型错误 - {relation}\n"
                        continue
                    source_key = clean_string(relation.source)
                    target_key = clean_string(relation.target)
                    relation_type = clean_string_with_only_words(relation.type)
                    if not source_key or not target_key or not relation_type:
                        logging.warning("📄强警告：关系的源节点或目标节点为空")
                        self.result_stats.strong_warning += 1
                        self.result_stats.strong_warning_msg += f"关系的源节点或目标节点为空:\n{relation}\n"
                        continue
                    source_id = node_id_mapping.get(source_key)
                    target_id = node_id_mapping.get(target_key)
                    if not source_id or not target_id:
                        logging.warning("📄强警告：关系的源节点或目标节点不存在")
                        self.result_stats.strong_warning += 1
                        self.result_stats.strong_warning_msg += f"关系的源节点或目标节点不存在:\n{relation}\n"
                        continue
                    final_kg["edges"].append({
                        "source_id": source_id,
                        "target_id": target_id,
                        "relation_type": relation_type,
                        "properties": relation.properties
                    })
                except Exception as e:
                    logging.error(f"📄❌：处理关系{relation}时出错 - {e}")
                    self.result_stats.error += 1
                    self.result_stats.error_msg += f"处理关系{relation}时出错 - {e}\n"
                    continue
            # 将诉讼其他结构作为节点加入到图谱中
            tasks = []
            tasks_params = []
            for case_schema in litigation_schemas:
                # 诉讼schema可能包含多个诉讼结构
                chunk_structures = case_schema.get("chunks")
                if not chunk_structures:
                    logging.error("📄❌：诉讼结构无效,chunks可能为空")
                    self.result_stats.error += 1
                    self.result_stats.error_msg += "诉讼结构无效\n"
                    raise ValueError("诉讼结构无效")
                # 诉讼结构对应的诉讼文书
                case_structure_input_text = ""
                if_have_chunk_structure = True
                for chunk_structure in chunk_structures:
                    chunk_content = chunks_data.get(chunk_structure)
                    if not chunk_content:
                        logging.warning(f"📄⚠️警告：分块结果中缺少结构{chunk_structure}")
                        self.result_stats.strong_warning += 1
                        self.result_stats.strong_warning_msg += f"分块结果中缺少结构{chunk_structure}\n"
                        if_have_chunk_structure = False
                        continue
                    case_structure_input_text += chunk_content + "\n"
                if not if_have_chunk_structure:
                    continue
                # 诉讼结构名称
                case_structure_type = case_schema.get("type")
                if not case_structure_type:
                    case_structure_type = chunk_structures[0]
                if not litigation_name or not case_structure_type:
                    logging.error("📄❌：诉讼名称或案件名称为空")
                    self.result_stats.error += 1
                    self.result_stats.error_msg += "诉讼名称或案件名称为空\n"
                    raise ValueError("诉讼名称或案件名称为空")
                case_structure_name = f"{litigation_name}_{case_structure_type}"
                case_structure_id = clean_string_for_neo4j_extended(f"{case_structure_type}_{generate_hex_uuid()}")
                properties = {
                    "对应诉讼": case_structure_name,
                    "内容": case_structure_input_text
                }
                final_kg["nodes"].append({
                    "node_id": case_structure_id,
                    "node_name": case_structure_name,
                    "node_type": case_structure_type,
                    "properties": properties
                })
                final_kg["edges"].append({
                    "source_id": litigation_id,
                    "target_id": case_structure_id,
                    "relation_type": "包含",
                    "properties": {}
                })
                # 多线程批量处理诉讼结构
                if case_schema.get("nodes") and case_schema.get("edges") and case_schema.get("examples") and case_schema.get("structure_prompt"):
                    tasks.append(
                        self.kg_extract_from_case_structure(
                            case_cache=case_cache,
                            case_structure_type=case_structure_type,
                            case_schema=case_schema,
                            case_structure_text=case_structure_input_text
                        )
                    )
                    tasks_params.append({
                        "case_structure_id": case_structure_id,
                        "case_structure_type": case_structure_type,
                        "case_schema": case_schema,
                        "case_structure_text": case_structure_input_text
                    })
                else:
                    logging.warning(f"📄⚠️强警告：诉讼结构{case_structure_name}无效，缺少结构节点或结构关系或结构示例或结构提示，不进行抽取")
                    self.result_stats.strong_warning += 1
                    self.result_stats.strong_warning_msg += f"诉讼结构{case_structure_name}无效，缺少结构节点或结构关系或结构示例或结构提示\n"
            # print(json.dumps(final_kg, ensure_ascii=False, indent=4))
            # 使用 asyncio.gather 并发执行所有任务
            logging.info("📄🌐:开始抽取诉讼结构知识图谱")
            results = await asyncio.gather(*tasks, return_exceptions=True)
            logging.info("📄:结束抽取诉讼结构知识图谱")

            logging.info("📄:开始处理诉讼数据结果")
            failed_results = []
            await self.process_case_results(
                filename=filename,
                failed_results=failed_results,
                tasks_params=tasks_params,
                case_results=results,
                final_kg=final_kg
            )
            logging.info("📄:结束处理诉讼数据结果")
            print(json.dumps(final_kg, ensure_ascii=False, indent=4))
            return final_kg
        except Exception as e:
            logging.error(f"📄❌：抽取文件信息时出错 - {e}")
            raise ValueError(f"抽取文件信息时出错 - {e}")

    async def kg_extract_from_case_structure(
        self,
        case_cache: CaseCache,
        case_structure_type: str,
        case_schema: dict,
        case_structure_text: str
    ) -> dict:
        """
        抽取单个诉讼结构内的实体和关系
        图谱数据结构：
        {
            "node_id": "",
            "node_name": "",
            "node_type": "",
            "properties": {
            },
            ...
        }
        :param case_cache: 诉讼结构缓存
        :param case_structure_type: 诉讼结构类型
        :param case_schema: 诉讼结构模式，包含结构名称和结构内容
        :param case_structure_text: 诉讼结构文本
        :return:
        """
        async with self.semaphore:
            try:
                # 整理抽取函数参数
                extract_prompt = case_schema.get("structure_prompt")
                if not extract_prompt:
                    logging.error("📄❌：结构提示为空")
                    raise ValueError("结构提示为空")
                schema_nodes = case_schema.get("nodes")
                schema_edges = case_schema.get("edges")
                if not schema_nodes or not schema_edges:
                    logging.error("📄❌：结构节点或结构关系为空")
                    raise ValueError("结构节点或结构关系为空")
                extract_schema = {
                    "nodes": schema_nodes,
                    "edges": schema_edges
                }
                extract_content = case_structure_text
                if not extract_content:
                    logging.error("📄❌：结构内容为空")
                    raise ValueError("结构内容为空")
                extract_examples = case_schema.get("examples")
                if not extract_examples:
                    logging.error("📄❌：结构示例为空")
                    raise ValueError("结构示例为空")
                # 抽取条款内容中的实体
                """
                {
                    "entities": list[Entity],
                    "relations": list[Relationship],
                    "texts_classes": list[TextClass]
                }
                """
                # print("--------------------------------------------")
                # print("结构提示：", extract_prompt)
                # print("结构模式：", extract_schema)
                # print("结构内容：", extract_content)
                # print("结构示例：", extract_examples)
                # print("--------------------------------------------")
                # temp_config = self.extractor.get_config()
                # print("结构配置：", temp_config)
                # getattr(temp_config, "temperature", None)
                # if temp_config.temperature is not None:
                #     temp_config.temperature = 0.5
                # print("结构配置：", temp_config)
                langextract_config = LangextractConfig(
                    model_name=CASE_MADEL,
                    api_key=CASE_MADEL_API,
                    api_url=CASE_MADEL_KEY,
                    config={
                        "timeout": TIMEOUT
                    },
                    temperature=0.5
                )
                extract_result = await self.extractor.entity_and_relationship_extract(
                    user_prompt=extract_prompt,
                    schema=extract_schema,
                    input_text=extract_content,
                    examples=extract_examples,
                    langextract_config=langextract_config
                )
                if not extract_result or not extract_result.get("entities"):
                    logging.warning(f"📄⚠️抽取强警告：结构{case_structure_type}抽取结果为空：抽取的内容为{extract_content}")
                    self.result_stats.strong_warning += 1
                    self.result_stats.strong_warning_msg += f"结构{case_structure_type}抽取结果为空：抽取的内容为{extract_content}\n"
                    return {"entities": [], "relations": [], "texts_classes": []}
                case_cache.extracted[case_structure_type] = extract_result
                return extract_result
            except Exception as e:
                logging.error(f"📄❌：处理诉讼结构{case_structure_type}时出错 - {e}")
                # logging.error(f"错误的诉讼结构：{case_schema}")
                self.result_stats.error_msg += f"处理诉讼结构{case_structure_type}时出错 - {e}\n"
                self.result_stats.error += 1
                raise ValueError("处理诉讼结构{}时出错".format(case_structure_type))

    async def process_case_results(
            self,
            filename: str,
            failed_results: list,
            tasks_params: list,
            case_results: list,
            final_kg: dict
    ) -> dict:
        """
        处理提取的诉讼知识图谱

        :param filename:
        :param failed_results:
        :param failed_case_structures:
        :param case_results:
        :param final_kg:
        :return:
        """
        try:
            success_results = []
            if not final_kg.get("nodes"):
                logging.error("📄❌：诉讼基础信息不完整")
                raise ValueError("诉讼基础信息不完整")
            for i, result in enumerate(case_results):
                if isinstance(result, Exception):
                    error_msg = str(result)  # ✅ 转换为字符串
                    logging.error(f"结构 {i + 1} 处理失败: {result}")
                    self.result_stats.error += 1
                    self.result_stats.error_msg += f"结构 {i + 1} 处理失败: {result}\n"
                    failed_result = {
                        "id": i + 1,
                        "result": error_msg,  # ✅ 存储字符串而不是Exception对象
                        "error_type": type(result).__name__
                    }
                    # 将对应的clause保存至failed_clauses
                    if i < len(tasks_params):
                        logging.error(f"📄❌：处理诉讼结构{tasks_params[i]['case_structure_type']}时出错 - {result}")
                        self.result_stats.error += 1
                        self.result_stats.error_msg += f"处理诉讼结构{tasks_params[i]['case_structure_type']}时出错 - {result}\n"
                    else:
                        failed_result["data"] = tasks_params[i]
                    failed_results.append(failed_result)
                else:
                    success_results.append(result)
            if failed_results:
                logging.error("📄🔥：以下条款处理失败")
                logging.info("==============================================================")
                for failed_result in failed_results:
                    logging.error(json.dumps(failed_result, ensure_ascii=False, indent=4))
                logging.info("==============================================================")
                raise ValueError("📄🔥:处理诉讼结构时出错")
            # 处理成功的结果
            logging.info("📄🎯：开始处理诉讼结构")
            # type_name到id的映射
            type_name_to_id_mapping = {}
            # 将已经处理过的实体，加入type_name_to_id_mapping映射
            for node in final_kg["nodes"]:
                node_id = node.get("node_id")
                node_type = node.get("node_type")
                node_name = node.get("node_name")
                if not node_id or not node_type or not node_name:
                    logging.warning("📄⚠️强警告：节点信息不完整")
                    self.result_stats.strong_warning += 1
                    self.result_stats.strong_warning_msg += f"节点信息不完整{node}\n"
                type_name_to_id_mapping[f"{node_type}_{node_name}"] = node_id
                # 添加filename属性
                node["filename"] = filename
            for i, success_result in enumerate(success_results):
                try:
                    case_structure_id = tasks_params[i]["case_structure_id"]
                except Exception as e:
                    logging.error(f"📄❌：处理诉讼结构第{i + 1}个时出错 - {e}")
                    self.result_stats.error += 1
                    self.result_stats.error_msg += f"处理诉讼结构第{i+1}个时出错 - {e}\n"
                    raise ValueError(f"处理诉讼结构第{i+1}个时出错")
                # 获取实体和关系
                entities = success_result.get("entities", [])
                relations = success_result.get("relations", [])
                # 处理实体
                for entity in entities:
                    if not isinstance(entity, Entity):
                        logging.error(f"📄❌：实体类型错误 - {entity}")
                        self.result_stats.error += 1
                        self.result_stats.error_msg += f"实体类型错误 - {entity}\n"
                        continue
                    entity_type = clean_string_with_only_words(entity.entity_type)
                    node_name = entity.name
                    if not node_name:
                        logging.warning("📄警告：节点名称为空")
                        self.result_stats.week_warning += 1
                        self.result_stats.week_warning_msg += f"节点名称为空{entity}\n"
                        continue
                    properties = entity.properties
                    exist_entity = None
                    # 判断是否存在相同实体
                    for temp_entity in final_kg.get("nodes", []):
                        if temp_entity.get("node_name") == node_name and temp_entity.get("node_type") == entity_type:
                            exist_entity = temp_entity
                            break
                    # 类型为诉讼参与者，可通过诉讼角色和名称查找
                    if not exist_entity and entity_type == "诉讼参与者":
                        # TODO：高级别待处理事项
                        nodes = final_kg.get("nodes", [])
                        for node in nodes:
                            node_name = node.get("node_name")
                            node_type = node.get("node_type")
                            if not node_name or node_type != "诉讼参与者":
                                continue
                            person_name = properties.get("名称")
                            person_role = properties.get("诉讼角色")
                            temp_properties = node.get("properties", {})
                            temp_person_name = temp_properties.get("名称")
                            temp_person_role = temp_properties.get("诉讼角色")
                            if person_name and temp_person_name and person_name == temp_person_name:
                                exist_entity = node
                                break
                            if person_role and temp_person_role and person_role == temp_person_role:
                                exist_entity = node
                                break
                    if exist_entity:
                        node_id = exist_entity.get("node_id", "")
                        if not node_id:
                            logging.error(f"📄❌：实体ID为空:{exist_entity}")
                            self.result_stats.error += 1
                            self.result_stats.error_msg += f"实体ID为空{exist_entity}\n"
                            raise ValueError(f"实体ID为空{exist_entity}")
                        # 如果存在相同实体，则合并属性
                        exist_properties = exist_entity.get("properties", {})
                        # 遍历当前实体的属性
                        for key, value in entity.properties.items():
                            # 如果 exist_entity 中没有该属性，则添加
                            if key not in exist_properties:
                                exist_properties[key] = value
                        # 更新 exist_entity 的属性
                        exist_entity["properties"] = exist_properties
                    else:
                        # 如果不存在相同实体，则添加新实体
                        node_id = clean_string_for_neo4j_extended(f"{entity_type}_{generate_hex_uuid()}")
                        final_kg["nodes"].append({
                            "node_id": node_id,
                            "node_name": node_name,
                            "node_type": entity_type,
                            "properties": properties,
                            "filename": filename
                        })
                        # 更新映射
                        type_name_to_id_mapping[f"{entity_type}_{node_name}"] = node_id
                    # 添加包含关系
                    final_kg["edges"].append({
                        "source_id": case_structure_id,
                        "target_id": node_id,
                        "relation_type": "包含",
                        "properties": {},
                        "filename": filename
                    })
                # 处理关系
                for relation in relations:
                    try:
                        if not isinstance(relation, Relationship):
                            logging.error(f"📄❌：关系类型错误 - {relation}")
                            self.result_stats.error += 1
                            self.result_stats.error_msg += f"关系类型错误 - {relation}\n"
                            continue
                        source_key = relation.source
                        target_key = relation.target
                        relation_type = clean_string_with_only_words(relation.type)
                        if not source_key or not target_key or not relation_type:
                            logging.warning("📄警告：关系的源节点或目标节点为空")
                            self.result_stats.week_warning += 1
                            self.result_stats.week_warning_msg += f"关系的源节点或目标节点为空{relation}\n"
                            continue
                        source_id = type_name_to_id_mapping.get(source_key)
                        target_id = type_name_to_id_mapping.get(target_key)
                        if not source_id or not target_id:
                            logging.warning(f"📄强警告：关系的源节点或目标节点不存在{relation}\n")
                            self.result_stats.strong_warning += 1
                            self.result_stats.strong_warning_msg += f"关系的源节点或目标节点不存在{relation}\n"
                            continue
                        # TODO 启动模糊匹配，即匹配相似名称的节点
                        final_kg["edges"].append({
                            "source_id": source_id,
                            "target_id": target_id,
                            "relation_type": relation_type,
                            "properties": relation.properties,
                            "filename": filename
                        })
                    except Exception as e:
                        logging.error(f"📄❌：处理关系{relation}时出错 - {e}")
                        continue
            return final_kg
        except Exception as e:
            logging.error(f"📄🔧：处理抽取数据时出错{e}")
            raise e

    @staticmethod
    async def _save_cache_to_json(
            cache_data: CaseCache,
            output_dir,
            filename
    ):
        """
        将条款数据保存为JSON格式
        :param cache_data: 字典数据
        :param output_dir: 输出目录路径
        :param filename: 文件名（不含扩展名）
        """
        # TODO
        pass

    @staticmethod
    async def _load_cache_from_json(
            filepath
    ):
        """
        从JSON文件中加载条款数据，并整理成dict
        :param filepath: 文件路径
        :return: 条款数据（字典或列表）
        """
        # TODO
        pass

    async def logging_result_stats(self):
        # logging.info("📄✅：错误与警告统计结果如下")
        # logging.info("======================================================================")
        # logging.info(f"📄✅：错误信息: {self.result_stats.error_msg}")
        # logging.info("======================================================================")
        # logging.info(f"📄✅：弱警告信息: {self.result_stats.week_warning_msg}")
        # logging.info("======================================================================")
        # logging.info(f"📄✅：强警告信息: {self.result_stats.strong_warning_msg}")
        # logging.info("======================================================================")
        logging.info("📄✅：数据统计")
        logging.info(f"📄✅：错误数: {self.result_stats.error}")
        logging.info(f"📄✅：弱警告数: {self.result_stats.week_warning}")
        logging.info(f"📄✅：强警告数: {self.result_stats.strong_warning}")

def clean_string(text: str) -> str:
    # 替换全角空格
    cleaned = replace_full_corner_space(text)
    # 替换零宽字符
    cleaned = replace_zero_width_chars(cleaned)
    # 将连续的换行符替换为单个换行符
    cleaned = re.sub(r'\n+', '\n', cleaned)
    # 移除行首行尾的空白字符
    cleaned = re.sub(r'^[ \t]+|[ \t]+$', '', cleaned, flags=re.MULTILINE)
    # 移除多余的空白字符
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    return cleaned


if __name__ == "__main__":
    # 读取示例文件
    # file_path = r"F:\企业大脑知识库系统\8.1项目\法律法规\裁判文书网v4\电子信息业制造业\一审民事判决书\高某与某生物科技股份有限公司劳动争议一审民事判决书.txt"
    file_path = r"F:\企业大脑知识库系统\8.1项目\法律法规\裁判文书网v4\电子信息业制造业\一审刑事判决书\陈某等非法获取计算机信息系统数据罪一审刑事判决书.txt"
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except FileNotFoundError:
        # 如果真实文件不存在，创建一个模拟的示例内容进行测试
        raise ValueError("文件不存在")

    extractor = CaseExtractor()
    # result = asyncio.run(
    #     extractor.split_chunks(
    #         # filename="陈某等非法获取计算机信息系统数据罪一审刑事判决书.txt",
    #         case_type="test",
    #         text=content
    #     )
    # )
    result = asyncio.run(
        extractor.extract_cases(
            filename="陈某等非法获取计算机信息系统数据罪一审刑事判决书.txt",
            case_type="test",
            text=content
        )
    )
    print("s5s5s5s5s5s5s5s5s5s5s5s5s5s5s5s5s5s5s")
    print(json.dumps(result, ensure_ascii=False, indent=4))
    neo4j_adapter = Neo4jAdapter()
    # 创建知识图谱
    neo4j_adapter.connect()
    neo4j_adapter.add_subgraph_with_merge(result, "a1_test")
    neo4j_adapter.disconnect()

    # chunks_data = {'文书首部': '判决书名称：陈某等非法获取计算机信息系统数据罪一审刑事判决书\n上海市青浦区人民法院\n刑 事 判 决 书\n（2025）沪0118刑初91号', '诉讼参与者信息': '公诉机关上海市青浦区人民检察院。\n被告人王某1，男，1994年2月26日出生（身份证号码XXXXXXXXXX********），XX，初中文化程度，户籍所在地浙江省桐庐县。2024年6月19日因涉嫌非法获取计算机信息系统数据犯罪被某某局1羁押，同年6月20日被刑事拘留，同年7月24日被依法逮捕。现羁押于上海市青浦区看守所。\n辩护人胡某，某某律师事务所1律师。\n辩护人阴某，某某律师事务所1律师。\n被告人王某2，男，1991年11月12日出生（身份证号码XXXXXXXXXX********），XX，高中文化程度，系快递行业从业者，户籍所在地江苏省灌云县。2024年6月19日因涉嫌非法获取计算机信息系统数据犯罪被某某局1羁押，同年6月20日被刑事拘留，同年7月24日被依法逮捕。现羁押于上海市第三看守所。\n辩护人邵某，某某律师事务所2律师。\n辩护人孟某，某某律师事务所3律师。\n被告人陈某1，男，1994年11月1日出生（身份证号码XXXXXXXXXX********），XX，初中文化程度，系快递行业从业者，户籍所在地河南省潢川县。2024年10月22日因涉嫌非法获取计算机信息系统数据犯罪被某某局2羁押，同年10月25日被某某局1刑事拘留，同年11月26日被依法逮捕。现羁押于上海市第三看守所。\n辩护人何某，某某律师事务所4律师，系某某中心指派。\n被告人焦某，男，1991年10月25日出生（身份证号码XXXXXXXXXX********），XX，中专文化程度，系个体程序员，户籍所在地江苏省灌云县。2024年6月19日因涉嫌非法获取计算机信息系统数据犯罪被某某局1羁押，同年6月20日被刑事拘留，同年7月24日被依法逮捕。现羁押于上海市第三看守所。\n辩护人刘某，某某律师事务所5律师。', '案件审理过程': '上海市青浦区人民检察院以沪青检刑诉[2025]73号起诉书指控被告人王某1、王某2、陈某1、焦某犯非法获取计算机信息系统数据罪，于2025年2月10日向本院提起公诉，本院依法适用普通程序进行审理并组成合议庭，公开开庭审理了本案。上海市青浦区人民检察院指派检察员顾某出庭支持公诉，被告人王某1及其辩护人胡某、阴某，被告人王某2及其辩护人孟某，被告人陈某1及其辩护人何某、被告人焦某及其辩护人刘某到庭参加诉讼。现已审理终结。', '诉辩主张': '上海市青浦区人民检察院根据被告人王某1、王某2、陈某1、焦某的供述笔录，证人王某3、陈某2等人的证言笔录及相关辨认笔录、微信聊天记录、调取证据通知书、协助查询财产通知书、银行交易记录、微信、支付宝交易记录，接受证据材料清单、授权委托书、某某公司2提供的报案材料、某快递业务合作协议、数据安全与保密承诺函、“快递100”用户服务协议、中浦鉴云（上海）信息技术有限公司司法鉴定所司法鉴定意见书，扣押决定书、扣押笔录、扣押清单、扣押物品照片、随案移送赃证款物品清单，谅解书，案发经过、抓获经过，人口信息等证据指控：\n一、2023年10月至2024年2月，被告人王某1在经营某某公司1期间，未经某快递股份有限公司（以下简称“某快递”）授权，违规使用多个某快递内部员工账号，非法获取“某宝盒”应用程序中包含有姓名、地址、电话等信息的计算机信息系统数据并贩卖给他人，非法获利1万余元（币种为人民币，下同）。\n二、2024年4月至同年6月间，被告人王某1、陈某1、王某2伙同被告人焦某、王某3（另案处理）等人为非法牟利，通过网络渠道寻找销路，以制作计算机破解程序的方式，对“某宝盒”、“快递100”等应用程序进行解密，非法获取大量包含收件人姓名、电话、地址等信息的计算机信息系统数据，并贩卖给他人，共计非法获利41万余元。\n其间，由被告人王某1、陈某1等人通过网络对接上游电商收集解密需求，由被告人陈某1将所需解密快递单号反馈给被告人王某2，由被告人王某2联络被告人焦某制作、运行、维护相关解密程序。后由被告人陈某1统一负责收款及向被告人王某1、王某3等人分赃。其中，被告人焦某分成获利8万元。\n经鉴定，被告人焦某制作的应用于“某宝盒”的计算机破解程序系使用特定密码表规则将加密文字符转换为明文数字进行解密的程序；被告人焦某制作的应用于“快递100”的计算机破解程序系调用应用内部API接口的程序。\n三、2024年5月间，被告人王某2伙同王某3利用上述计算机破解程序，通过网络渠道寻找销路，对“某宝盒”、“快递100”等程序进行解密，非法获取大量包含收件人姓名、电话、地址等计算机信息系统数据，并贩卖给他人，非法获利2万余元。\n另查明，被告人王某1、焦某到案后如实供述了上述事实，被告人王某2、陈某1如实供述了主要事实。案发后，被告人王某1、王某2、陈某1、焦某均向某快递赔偿2万元，就非法获取“某宝盒”应用程序数据部分获得了某快递的谅解。\n综上，公诉机关认为，被告人王某1、陈某1、王某2伙同被告人焦某等人违反国家规定，采用侵入手段，获取计算机信息系统中处理的数据，情节特别严重，应当以非法获取计算机信息系统数据罪追究其刑事责任。被告人王某1、焦某认罪认罚，依法均可以从宽处理。被告人王某1、王某2、陈某1起主要作用，系主犯；被告人焦某起次要作用，系从犯，对于从犯应当减轻处罚。被告人王某1、王某2、陈某1、焦某到案后如实供述上述事实，依法均可以从轻处罚。被告人均就非法获取“某宝盒”数据行为获得某快递谅解，酌情予以从轻处罚。综上，提请本院依照《中华人民共和国刑法》第二百八十五条第二款、第二十五条第一款、第二十六条第一款、第四款、第二十七条、第六十七条第三款、《中华人民共和国刑事诉讼法》第十五条之规定，依法审判。\n被告人王某1及其辩护人对公诉机关指控的犯罪事实及罪名均无异议。辩护人以被告人王某1系如实供述事实、认罪认罚、退赃退赔等为由请求对被告人从轻处罚并适用缓刑。\n被告人王某2对公诉机关指控的主要犯罪事实及罪名无异议，但认为其仅负责利用程序批量解密单号，分成也较少，不应被认定为主犯。\n被告人王某2的辩护人对公诉机关指控的主要犯罪事实及罪名无异议，但以被告人王某2并非犯意的发起者，也未起到组织犯罪的作用，分成也较少且不掌握资金的流向等为由认为被告人王某2应被认定为从犯。另外辩护人以被告人退赃、如实供述事实、系初犯等为由请求对被告人王某2从轻处罚并适用缓刑。\n被告人陈某1及其辩护人对公诉机关指控的主要犯罪事实及罪名均无异议，但陈某1辩解其并不负责与上游的电商客户沟通，只是负责将解密好的数据发送给他们。辩护人以被告人陈某1如实供述事实、退赃、系初犯等为由请求对被告人陈某1从轻处罚并适用缓刑。\n被告人焦某及其辩护人对公诉机关指控的犯罪事实及罪名均无异议。辩护人以被告人陈某1系从犯、退赃退赔、系初犯等为由请求对被告人焦某减轻处罚并适用缓刑。', '事实认定': '经审理查明：一、2023年10月至2024年2月，被告人王某1在经营某某公司1期间，未经某快递股份有限公司授权，违规使用多个某快递内部员工账号，非法获取“某宝盒”应用程序中包含有姓名、地址、电话等信息的计算机信息系统数据并贩卖给他人，非法获利1万余元。\n二、2024年4月至同年6月间，被告人王某1、陈某1、王某2伙同被告人焦某、王某3等人为非法牟利，通过网络渠道寻找销路，以制作计算机破解程序的方式，对“某宝盒”、“快递100”等应用程序进行解密，非法获取大量包含收件人姓名、电话、地址等信息的计算机信息系统数据，并贩卖给他人，共计非法获利41万余元。\n其间，由被告人王某1、陈某1等人通过网络对接上游电商收集解密需求，由被告人陈某1将所需解密快递单号反馈给被告人王某2，由被告人王某2联络被告人焦某制作、运行、维护相关解密程序。后由被告人陈某1统一负责收款及向被告人王某1、王某3等人分赃。其中，被告人焦某分成获利8万元。\n经鉴定，被告人焦某制作的应用于“某宝盒”的计算机破解程序系使用特定密码表规则将加密文字符转换为明文数字进行解密的程序；被告人焦某制作的应用于“快递100”的计算机破解程序系调用应用内部API接口的程序。\n三、2024年5月间，被告人王某2伙同王某3利用上述计算机破解程序，通过网络渠道寻找销路，对“某宝盒”、“快递100”等程序进行解密，非法获取大量包含收件人姓名、电话、地址等计算机信息系统数据，并贩卖给他人，非法获利2万余元。\n另查明，被告人王某1、焦某如实供述上述事实；被告人王某2、陈某1如实供述主要事实。\n审理中，被告人王某1退出违法所得11万元；被告人王某2退出违法所得2万元；被告人陈某1退出违法所得8万元；被告人焦某退出违法所得8万元。\n以上事实，有被告人王某1、王某2、陈某1、焦某的供述笔录，证人王某3、陈某2等人的证言笔录及相关辨认笔录、微信聊天记录、调取证据通知书、协助查询财产通知书、银行交易记录、微信、支付宝交易记录，接受证据材料清单、授权委托书、某某公司2提供的报案材料、某快递业务合作协议、数据安全与保密承诺函、“快递100”用户服务协议、中浦鉴云（上海）信息技术有限公司司法鉴定所司法鉴定意见书，扣押决定书、扣押笔录、扣押清单、扣押物品照片、随案移送赃证款物品清单，谅解书，案发经过、抓获经过，人口信息，转账记录等证据证明，并经庭审查证属实。', '法院评议': '本院认为，被告人王某1、王某2、陈某1、焦某违反国家规定，采用侵入手段，获取计算机信息系统中处理的数据，情节特别严重，其行为均已构成非法获取计算机信息系统数据罪，依法均应予惩处。被告人王某1、焦某认罪认罚，依法均可以从宽处理。在共同犯罪中，被告人王某1、王某2、陈某1起主要作用，系主犯；被告人焦某起次要作用，系从犯，对于从犯依法予以减轻处罚。被告人王某1、焦某均如实供述自己的罪行，被告人王某2、陈某1如实供述自己的主要罪行，依法均可以从轻处罚。被告人均就非法获取“某宝盒”数据行为获得某快递公司的谅解，酌情予以从轻处罚；被告人王某1、王某2、陈某1、焦某均退出违法所得，酌情均予以从轻处罚。公诉机关指控被告人王某1、王某2、陈某1、焦某的犯罪罪名及区分主从犯、认定被告人均系如实供述罪行的公诉意见正确，且量刑建议适当，本院予以确认。被告人王某1的辩护人、被告人王某2的辩护人以及被告人陈某1的辩护人分别以被告人系如实供述罪行、退赃退赔等为由分别请求对被告人从轻处罚的辩护意见以及被告人焦某的辩护人以被告人系从犯、系如实供述罪行、认罪认罚等为由请求对被告人从轻、减轻处罚的辩护意见，因与本院查明的事实相符，且于法不悖，本院予以采纳。关于四被告人的辩护人分别请求对其当事人判处缓刑的意见，综合本案的性质、情节、社会危害性，本院不予采纳。关于被告人王某2及辩护人提出被告人王某2系从犯的辩护意见，经查，王某2与另案处理的王某3几乎同时与被告人王某1开始从事非法获取计算机信息系统数据并解密的犯罪行为，王某2不仅用自己的账号为王某1提供的数据进行查询，还主动找到被告人焦某编辑程序实现批量解密操作，虽然被告人陈某1将王某3、王某2、焦某三人的赃款统一交给王某3分配，但这不等同也不能说明被告人王某2就是属于次要地位、起的辅助作用。综合全案证据，被告人王某2在犯罪中与王某1、陈某1以及另案处理的王某3同样起到主要作用，公诉机关认定被告人王某2属于主犯并无不当。关于被告人陈某1辩解其不负责与上游电商客户接洽的意见，经查，同案犯王某1供述“正好陈某1手上也有电商资源，其与陈某1就将手上掌握的电商账号给王某3、王某2他们去解密；单号解密出来获取的明文手机号、地址这些信息也是通过其与陈某1反馈给上游电商”；同案犯王某2供述“陈某1也是在网上找客户的，然后把一些客户需求发给我”；同案犯王某3供述“王某1和陈某1就把从上游电商处获取的单号数据通过微信发给王某2，然后由王某2和焦某解密这些单号”，可见同案犯均一致供述在分工中陈某1负责对接上游的电商客户，另外公诉机关还出示了陈某1与相关电商客户接洽的聊天记录，与前述几名同案犯的供述亦相吻合，故对于被告人陈某1的上述辩解，本院不予采纳。据此，为维护社会管理秩序，保护公司、企业的计算机系统正常运行及数据安全不受侵犯，依照《中华人民共和国刑法》第二百八十五条第二款、第二十五条第一款、第二十六条第一款、第四款、第二十七条、第六十七条第三款、第五十二条、第五十三条、第六十四条、《中华人民共和国刑事诉讼法》第十五条之规定，判决如下：', '裁判主文': '一、被告人王某1犯非法获取计算机信息系统数据罪，判处有期徒刑三年七个月，并处罚金人民币八万元。\n（刑期从判决执行之日起计算。判决执行以前先行羁押的，羁押一日折抵刑期一日，即自2024年6月19日起至2028年1月18日止；罚金已预缴四万元，不足部分应于本判决生效之日起十日内一次性向本院缴纳。）\n二、被告人王某2犯非法获取计算机信息系统数据罪，判处有期徒刑三年四个月，并处罚金人民币六万元。\n（刑期从判决执行之日起计算。判决执行以前先行羁押的，羁押一日折抵刑期一日，即自2024年6月19日起至2027年10月18日止；罚金应于本判决生效之日起十日内一次性向本院缴纳。）\n三、被告人陈某1犯非法获取计算机信息系统数据罪，判处有期徒刑三年三个月，并处罚金人民币五万元。\n（刑期从判决执行之日起计算。判决执行以前先行羁押的，羁押一日折抵刑期一日，即自2024年10月22日起至2028年1月21日止；罚金应于本判决生效之日起十日内一次性向本院缴纳。）\n四、被告人焦某犯非法获取计算机信息系统数据罪，判处有期徒刑二年，并处罚金人民币四万元。\n（刑期从判决执行之日起计算。判决执行以前先行羁押的，羁押一日折抵刑期一日，即自2024年6月19日起至2026年6月18日止；罚金应于本判决生效之日起十日内一次性向本院缴纳。）\n五、扣押在案的赃款及作案工具予以没收。', '上诉权利告知': '如不服本判决，可在接到判决书的第二日起十日内，通过本院或者直接向上海市第二中级人民法院提出上诉。书面上诉的，应当提交上诉状正本一份，副本一份。', '审判组织及日期': '审\u3000判\u3000长\u3000\u3000段美玲\n人民陪审员\u3000\u3000钱淑艳\n人民陪审员\u3000\u3000徐根妹\n二〇二五年三月十九日\n法官\u3000助理\u3000\u3000王婕琼\n书\u3000记\u3000员\u3000\u3000夏琦贤', '附录法文': '附：相关法律条文\n一、《中华人民共和国刑法》\n第二百八十五条……\n违反国家规定，侵入前款规定以外的计算机信息系统或者采用其他技术手段，获取该计算机信息系统中存储、处理或者传输的数据，或者对该计算机信，息系统实施非法控制，情节严重的，处三年以下有期徒刑或者拘役，并处或者羊处罚金；情节特别严重的，处三年以上七年以下有期徒刑，并处罚金。\n……\n第二十五条共同犯罪是指二人以上共同故意犯罪。\n……\n第二十六条组织、领导犯罪集团进行犯罪活动的或者在共同犯罪中起主要作用的，是主犯。\n……\n对于第三款规定以外的主犯，应当按照其所参与的或者组织指挥的全部犯罪处罚。\n第二十七条在共同犯罪中起次要或者辅助作用的，是从犯。\n对于从犯，应当从轻、减轻或者免除处罚。\n第六十七条……\n被告人虽不具有前两款规定的自首情节，但是如实供述自己罪行的，可以从轻处罚；因其如实供述自己罪行，避免特别严重后果发生的，可以减轻处罚。\n第五十二条判处罚金，应当根据犯罪情节决定罚金数额。\n第五十三条罚金在判决指定的期限内一次或者分期缴纳。期满不缴纳的，强制缴纳。对于不能全部缴纳罚金的，人民法院在任何时候发现被执行人有可以执行的财产，应当随时追缴。\n由于遭遇不能抗拒的灾祸等原因缴纳确实有困难的，经人民法院裁定，可以延期缴纳、酌情减少或者免除。\n第六十四条犯罪分子违法所得的一切财物，应当予以追缴或者责令退赔；对受骗人员的合法财产，应当及时返还；违禁品和供犯罪所用的本人财物，应当予以没收。没收的财物和罚金，一律上缴国库，不得挪用和自行处理。\n二、《中华人民共和国刑事诉讼法》\n第十五条犯罪嫌疑人、被告人自愿如实供述自己的罪行，承认指控的犯罪事实，愿意接受处罚的，可以依法从宽处理。'}
    # extractor = CaseExtractor()
    # case_cache = CaseCache()
    # result = asyncio.run(
    #     extractor.extract_chunk_kg(
    #         filename="test.txt",
    #         case_type="test",
    #         chunks_data=chunks_data,
    #         case_cache=case_cache
    #     )
    # )
    # 打印结果
    # for key, value in result.items():
    #     print(f"------------------{key}------------------")
    #     print(f"{value}")

# 1. 统计抽取出来的条款数和最后一项条款的编号是对应上的
