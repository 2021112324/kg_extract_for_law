import asyncio
import json
import logging
import os
import re

from app.infrastructure.information_extraction.base import Entity, Relationship
from app.infrastructure.information_extraction.factory import InformationExtractionFactory
from app.infrastructure.information_extraction.law_extract.prompt.example import example_for_clause, \
    example_for_file_info
from app.infrastructure.information_extraction.law_extract.prompt.prompt import prompt_for_clause, prompt_for_file_info
from app.infrastructure.information_extraction.law_extract.prompt.schema import schema_for_clause, schema_for_file_info
from app.infrastructure.information_extraction.method.base import LangextractConfig
from app.infrastructure.string_utils.id_tool import generate_hex_uuid
from app.infrastructure.string_utils.str_clean import clean_string_with_only_words, clean_string_for_neo4j_extended, \
    replace_full_corner_space, replace_zero_width_chars

CLAUSE_MADEL = "qwen3-30b-a3b-instruct-2507"
CLAUSE_MADEL_API = "gpustack_342609ce423be29a_4371426b285a91dc44fb4e8d72454847"
CLAUSE_MADEL_KEY = "http://222.171.219.26:20001/v1/chat/completions"

# MAX_CHUNK_SIZE = int(os.getenv("MAX_CHUNK_SIZE", "5000"))
# BATCH_LENGTH = int(os.getenv("BATCH_LENGTH", "5"))
# MAX_WORKERS = int(os.getenv("MAX_WORKERS", "3"))
# TIMEOUT = int(os.getenv("TIMEOUT", "300"))

MAX_CHAR_BUFFER = 7500
BATCH_LENGTH = 5
MAX_WORKERS = 3
TIMEOUT = 3000

class ClauseCache:
    def __init__(self):
        self.file_info = {}
        self.clause_cache = {}


class ClauseExtractor:
    def __init__(self, max_concurrent: int = 50):
        self.extractor_config = LangextractConfig(
            model_name=CLAUSE_MADEL,
            api_key=CLAUSE_MADEL_API,
            api_url=CLAUSE_MADEL_KEY,
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

        # 宽松模式标志（True则将处理失败的条款直接作为法条实体添加到图谱中（不考虑法条信息、））
        self.lenient_mode = False

    async def extract_clauses(
            self,
            filename: str,
            text: str
    ) -> dict:
        """
        实现功能：从法规文件中抽取条款知识图谱数据
        :param filename:
        :param text:
        :return:
        """
        try:
            logging.info("📄⏳:开始条款知识图谱抽取")
            # 分割条款数据
            logging.info("📄:开始分割条款")
            clauses_data = await self.split_clause(
                text
            )
            logging.info("📄:结束分割条款")
            # 检查缓存
            """
            缓存cache为ClauseCache对象，其中的cache.clause_cache记录结构：
            {
                "第X条" :{}
            }
            遍历缓存，通过 clauses["条款编号"] 等于 缓存的"第X条"，
            将clauses中已经处理过的条款数据从clauses中删除，
            然后对clauses中未处理过的条款数据进行处理
            """
            logging.info("📄:开始处理文件信息")
            clause_cache = ClauseCache()
            file_info = clauses_data.get("file_info")
            if not file_info:
                logging.error("📄❌：文件信息为空")
                raise ValueError("文件信息为空")
            file_info_result = await self.kg_extract_from_file_info(
                filename=filename,
                clause_cache=clause_cache,
                file_info=file_info
            )

            logging.info("📄:结束处理文件信息")

            logging.info("📄:开始处理条款数据")
            clauses = clauses_data.get("clauses")
            if not clauses:
                logging.error("📄❌：条款数据为空")
                raise ValueError("条款数据为空")
            # 批量处理条款数据
            tasks = [
                self.kg_extract_from_clause(
                    filename=filename,
                    clause_cache=clause_cache,
                    one_clause=clause
                )
                for clause in clauses
            ]
            # 使用 asyncio.gather 并发执行所有任务
            logging.info("📄🌐:开始抽取条款知识图谱")
            results = await asyncio.gather(*tasks, return_exceptions=True)
            logging.info("📄:结束抽取条款知识图谱")

            logging.info("📄:开始处理条款数据结果")
            failed_results = []
            failed_clauses = []
            successful_results = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logging.error(f"条款 {i+1} 处理失败: {result}")
                    failed_results.append((i+1, result))
                    # 将对应的clause保存至failed_clauses
                    failed_clauses.append(clauses[i])
                else:
                    successful_results.append(result)
            if failed_results:
                logging.error("📄🔥：以下条款处理失败")
                logging.info("==============================================================")
                for i, result in failed_results:
                    logging.error(f"条款 {i} 处理失败: {result}")
                logging.info("==============================================================")
            # TODO:DELETE以格式化json输出每个条款处理结果
            print(json.dumps(file_info_result, ensure_ascii=False, indent=4))
            logging.info("==============================================================")
            for i, result in enumerate(successful_results):
                logging.info(f"条款 {i+1} 处理结果: ")
                logging.info(json.dumps(result, ensure_ascii=False, indent=4))
            logging.info("==============================================================")

            if not self.lenient_mode and failed_results:
                logging.error("📄🔴🔴🔴：严谨模式：存在处理失败的法条，请检查问题！！！")
                raise ValueError("存在处理失败的法条，请检查问题！！！")
            final_kg = await self.process_extracted_data(
                filename=filename,
                extracted_file_info=file_info_result,
                extracted_success_clauses=successful_results,
                extracted_failed_clauses=failed_clauses
            )
            logging.info("📄:结束处理条款数据")

            return final_kg
        except Exception as e:
            # 如果缓存中存在结果，将缓存保存起来
            logging.error("📄❌：条款知识图谱抽取报错: %s", e)
            raise e

    async def split_clause(
            self,
            text: str
    ) -> dict:
        """
实现功能：切分法规文件，将其整理成法规文件基础信息、条款的结构化数据，大致逻辑如下：
1. file_info记录文件开头内容，current_chapter记录当前章，current_section记录当前节。
2. 读取文件内容，若读取到章内容（即"第X章 XXX"），则current_chapter记录当前章内容（即"第X章 XXX"）；
    若读取到节内容（即"第X节 XXX"），则current_section记录当前节内容（即"第X节 XXX"）；
    若读取到条款内容（即"第X条 XXX"），则将current_chapter、current_section、条款内容记录到结果中；
3. file_info从开头开始记录，如果读取到"第一节"，则将"第一节"前的内容记录到file_info中，并删除file_info文本末尾的章和节（如果有的话）
4. 条款内容为上一个"第X条"到下一个"第X条"之间的内容，或者读到章或节的标志，或者读到文件末尾，或者读到"附录"、"附件"等条款部分结束标志。
    切分后的数据结构：
    {
        "file_info": "文件开头内容",
        "clauses": [
            {
                "章": "章内容",
                "节": "节内容",
                "条款编号": "第几条"
                "条款内容": "条款内容，不包含开头的"第几条""
            }
        ]
    }
        :param text:
        :return:
        """
        # 定义正则表达式模式 - 匹配行首的"第X章/节/条 "格式（中间可能有空格，但后面必须有空格）
        chapter_pattern = re.compile(r'^第[零一二三四五六七八九十百千万\d]+\s*章\s+.*', re.MULTILINE)
        section_pattern = re.compile(r'^第[零一二三四五六七八九十百千万\d]+\s*节\s+.*', re.MULTILINE)
        clause_pattern = re.compile(r'^第([零一二三四五六七八九十百千万\d]+)\s*条\s*(.*)', re.MULTILINE)

        # 定义结束标志
        end_markers = ['附录', '附件', '附表', '后记', '参考文献', '索引']

        lines = text.split('\n')

        # 初始化变量
        file_info = ""
        current_chapter = ""
        current_section = ""
        clauses = []

        # 记录file_info的结束位置（第一章或第一节）
        file_info_end_idx = None
        clause_start_idx = None
        for i, line in enumerate(lines):
            if not line.strip():
                continue
            # 逐行进行匹配
            if chapter_pattern.match(line.lstrip(' \t\r\n\f\v#-*•·')):
                # 记录开头内容的结束位置（不包含该行）
                file_info_end_idx = i
                # 将章内容记录到current_chapter中
                current_chapter = line.strip().lstrip(' \t\r\n\f\v#-*•·')
            if section_pattern.match(line.lstrip(' \t\r\n\f\v#-*•·')):
                # 将节内容记录到current_section中
                current_section = line.strip().lstrip(' \t\r\n\f\v#-*•·')
            if clause_pattern.match(line.lstrip(' \t\r\n\f\v#-*•·')):
                # 记录条款内容的开始位置（包含该行）
                clause_start_idx = i
                if not file_info_end_idx:
                    file_info_end_idx = i
                break

        if not clause_start_idx:
            logging.error("📄❌：文本中匹配条款失败")
            raise ValueError("文本中匹配条款失败")

        try:
            # 提取file_info：从开头到第一节之前的内容
            file_info = '\n'.join(lines[:file_info_end_idx]).rstrip()
            # 提取第一条的条款内容，并将其拼接进file_info中

            current_clause_content = ""
            current_clause_number = ""

            # 遍历条款内容
            for i in range(clause_start_idx, len(lines)):
                line = lines[i]
                if not line.strip():
                    continue
                # 检查是否到达结束标志
                is_end_marker = False
                for marker in end_markers:
                    if line.lstrip().startswith(marker):
                        is_end_marker = True
                        break
                if is_end_marker:
                    # 如果当前正在收集条款内容，则保存它
                    if current_clause_content:
                        clauses.append({
                            "章": clean_string(current_chapter),
                            "节": clean_string(current_section),
                            "条款编号": clean_string(current_clause_number),
                            "条款内容": clean_string(current_clause_content)
                        })
                        current_clause_content = ""
                        current_clause_number = ""
                    break

                # 检查是否是章（匹配行首）
                if chapter_pattern.match(line.lstrip(' \t\r\n\f\v#-*•·')):
                    # 如果当前正在收集条款内容，则先保存它
                    if current_clause_content:
                        clauses.append({
                            "章": clean_string(current_chapter),
                            "节": clean_string(current_section),
                            "条款编号": clean_string(current_clause_number),
                            "条款内容": clean_string(current_clause_content)
                        })
                        current_clause_content = ""
                        current_clause_number = ""

                    # 更新当前章
                    current_chapter = line.strip().lstrip(' \t\r\n\f\v#-*•·')
                    # 清空当前节
                    current_section = ""
                    continue

                # 检查是否是节（匹配行首）
                if section_pattern.match(line.lstrip(' \t\r\n\f\v#-*•·')):
                    # 如果当前正在收集条款内容，则先保存它
                    if current_clause_content:
                        clauses.append({
                            "章": clean_string(current_chapter),
                            "节": clean_string(current_section),
                            "条款编号": clean_string(current_clause_number),
                            "条款内容": clean_string(current_clause_content)
                        })
                        current_clause_content = ""
                        current_clause_number = ""

                    # 更新当前节
                    current_section = line.strip().lstrip(' \t\r\n\f\v#-*•·')
                    continue

                # 检查是否是条款（匹配行首）
                if clause_pattern.match(line.lstrip(' \t\r\n\f\v#-*•·')):
                    # 如果当前正在收集条款内容，则先保存之前的条款
                    if current_clause_content:
                        clauses.append({
                            "章": clean_string(current_chapter),
                            "节": clean_string(current_section),
                            "条款编号": clean_string(current_clause_number),
                            "条款内容": clean_string(current_clause_content)
                        })

                    # 提取条款编号和内容
                    clause_match = clause_pattern.match(line.lstrip(' \t\r\n\f\v#-*•·'))
                    # 从捕获组直接获取编号
                    clause_num_part = clause_match.group(1)  # 编号部分
                    current_clause_number = f"第{clause_num_part}条"  # 重构完整编号
                    current_clause_content = clause_match.group(2).strip()  # 内容部分
                    continue

                # 如果当前正在收集条款内容，则添加到当前条款内容
                if current_clause_content:
                    if current_clause_content:  # 如果已有内容，在前面加上换行符
                        current_clause_content += '\n' + line
                    else:  # 如果还没有内容，直接赋值
                        current_clause_content = line
            # 处理最后一个条款（如果有）
            if current_clause_content:
                clauses.append({
                    "章": clean_string(current_chapter),
                    "节": clean_string(current_section),
                    "条款编号": clean_string(current_clause_number),
                    "条款内容": clean_string(current_clause_content)
                })

            if not clauses:
                logging.error("📄❌：未找到条款内容")
                raise ValueError("未找到条款内容")
            # 将第一条的条款内容拼接至file_info中
            file_info += '\n' + clauses[0]['条款内容']
            return {
                "file_info": clean_string(file_info),
                "clauses": clauses
            }
        except Exception as e:
            logging.error(f"📄❌：提取文件信息时出错 - {e}")
            raise ValueError(f"提取文件信息时出错 - {e}")

    async def kg_extract_from_file_info(
            self,
            filename: str,
            clause_cache: ClauseCache,
            file_info: str
    ) -> dict:
        """
        抽取单个条款图谱数据
        图谱数据结构：
        {
          "node_id": "",
          "node_name": "",
          "node_type": "",
          "properties": {},
          "文件依据":[node]
        }
        :param filename: 文件名
        :param clause_cache: 条款缓存
        :param file_info: 文件信息
        :return:
        """
        try:
            file_info_result = {
                "node_id": "",
                "node_name": "",
                "node_type": "",
                "properties": {},
                "法规依据": []
            }
            # 抽取参数
            extract_prompt = prompt_for_file_info
            extract_schema = schema_for_file_info
            extract_examples = example_for_file_info
            extract_content = f"{filename} 文件描述：\n{file_info}"

            extract_result = await self.extractor.entity_and_relationship_extract(
                user_prompt=extract_prompt,
                schema=extract_schema,
                input_text=extract_content,
                examples=extract_examples,
            )
            entities = extract_result.get("entities", [])
            # 多余法规文件标志，一个法规文件信息中只能有一个法规文件，处理一个法规文件后置标志为真
            file_info_processed = False
            for entity in entities:
                if not isinstance(entity, Entity):
                    logging.error(f"📄❌：实体类型错误 - {entity}")

                # 保存文件信息
                entity_type = clean_string_with_only_words(entity.entity_type)
                if entity_type == "法规文件":
                    if file_info_processed:
                        logging.warning("📄警告：一个法规文件信息中只能有一个法规文件，请检查问题")
                        continue
                    node_name = entity.name
                    if not node_name:
                        logging.warning("📄警告：法规文件名称为空")
                        continue
                    node_id = clean_string_for_neo4j_extended(f"{entity_type}_{generate_hex_uuid()}")
                    file_info_result["node_id"] = node_id
                    file_info_result["node_name"] = node_name
                    file_info_result["node_type"] = entity_type
                    file_info_result["properties"] = entity.properties
                    file_info_processed = True
                elif entity_type == "法规依据":
                    node_name = entity.name
                    if not node_name:
                        logging.warning("📄警告：法规依据名称为空")
                        continue
                    node_id = clean_string_for_neo4j_extended(f"{entity_type}_{generate_hex_uuid()}")
                    file_info_result["法规依据"].append({
                        "node_id": node_id,
                        "node_name": node_name,
                        "node_type": entity_type,
                        "properties": entity.properties
                    })
                else:
                    logging.warning(f"📄警告：未知实体类型 - {entity}")
            clause_cache.file_info = file_info_result
            return file_info_result
        except Exception as e:
            logging.error(f"📄❌：抽取文件信息时出错 - {e}")
            raise ValueError(f"抽取文件信息时出错 - {e}")

    async def kg_extract_from_clause(
            self,
            filename: str,
            clause_cache: ClauseCache,
            one_clause: dict
    ) -> dict:
        """
        抽取单个条款图谱数据
        图谱数据结构：
        {
            "node_id": "",
            "node_name": "",
            "node_type": "",
            "properties": {
                "章": "current_chapter",
                "节": "current_section",
                "条": "current_clause_number",
                "法条全文": "current_clause_content",
                ...
            },
            "条款单元": [
                {
                    "node_id": "",
                    "node_name": "",
                    "node_type": "",
                    "properties": {},
                    "外部引用依据": [],
                    "内部引用依据": []
                }
            ]
            ...
        }
        :param filename: 文件名
        :param one_clause:单个条款数据
        :param clause_cache:
        :return:
        """
        async with self.semaphore:
            try:
                clause_result = {
                    "node_id": "",
                    "node_name": "",
                    "node_type": "",
                    "properties": {},
                    "条款单元": []
                }

                # 获取条款信息
                chapter = one_clause.get("章", "")
                section = one_clause.get("节", "")
                clause_number = one_clause.get("条款编号", "")
                clause_content = one_clause.get("条款内容", "")
                if not clause_number:
                    logging.error(f"📄❌：条款编号为空:{one_clause}")
                    raise ValueError("条款编号为空")
                # 抽取参数
                extract_prompt = prompt_for_clause
                extract_schema = schema_for_clause
                extract_examples = example_for_clause
                extract_content = f"{filename} "
                if chapter:
                    extract_content += f"{chapter} "
                if section:
                    extract_content += f"{section} "
                extract_content += f"{clause_number}：\n{clause_content}"
                # 抽取条款内容中的实体
                """
                {
                    "entities": list[Entity],
                    "relations": list[Relationship],
                    "texts_classes": list[TextClass]
                }
                """
                extract_result = await self.extractor.entity_and_relationship_extract(
                    user_prompt=extract_prompt,
                    schema=extract_schema,
                    input_text=extract_content,
                    examples=extract_examples,
                )
                # 临时保存条款单元和引用依据
                clause_units = {}
                references = {}
                processed_keys = []
                # 获取实体和关系
                entities = extract_result.get("entities", [])
                relations = extract_result.get("relations", [])
                # 处理实体
                clause_processed = False
                for entity in entities:
                    if not isinstance(entity, Entity):
                        logging.error(f"📄❌：实体类型错误 - {entity}")
                        continue
                    entity_type = clean_string_with_only_words(entity.entity_type)
                    if entity_type == "法条":
                        if clause_processed:
                            logging.warning("📄警告：一个法条中只能有一个法条，请检查问题")
                            continue
                        node_name = entity.name
                        if not node_name:
                            logging.warning("📄警告：法条名称为空")
                            continue
                        node_id = clean_string_for_neo4j_extended(f"{entity_type}_{generate_hex_uuid()}")
                        clause_result["node_id"] = node_id
                        clause_result["node_name"] = clause_number
                        clause_result["node_type"] = "法条"
                        clause_result["properties"] = entity.properties
                        clause_result["properties"]["章"] = chapter
                        clause_result["properties"]["节"] = section
                        clause_result["properties"]["条"] = clause_number
                        clause_result["properties"]["法条全文"] = clause_content
                        clause_processed = True
                    elif entity_type == "条款单元":
                        node_name = entity.name
                        if not node_name:
                            logging.warning("📄警告：条款单元名称为空")
                            continue
                        node_id = clean_string_for_neo4j_extended(f"{entity_type}_{generate_hex_uuid()}")
                        clause_key = f"{entity_type}_{node_name}"
                        clause_units[clause_key] = {
                            "node_id": node_id,
                            "node_name": node_name,
                            "node_type": "条款单元",
                            "properties": entity.properties,
                            "外部引用依据": [],
                            "内部引用依据": []
                        }
                    elif entity_type == "引用依据":
                        node_name = entity.name
                        if not node_name:
                            logging.warning("📄警告：引用依据名称为空")
                            continue
                        node_id = clean_string_for_neo4j_extended(f"{entity_type}_{generate_hex_uuid()}")
                        reference_key = f"{entity_type}_{node_name}"
                        references[reference_key] = {
                            "node_id": node_id,
                            "node_name": node_name,
                            "node_type": "引用依据",
                            "properties": entity.properties
                        }
                    else:
                        logging.error(f"📄❌：未知实体类型 - {entity}")
                # 处理关系
                for relation in relations:
                    try:
                        if not isinstance(relation, Relationship):
                            logging.error(f"📄❌：关系类型错误 - {relation}")
                            continue
                        source_key = relation.source
                        target_key = relation.target
                        relation_type = clean_string_with_only_words(relation.type)
                        if not source_key or not target_key or not relation_type:
                            logging.warning("📄警告：关系的源节点或目标节点为空")
                            continue
                        if relation_type == "引用":
                            # 获取条款单元
                            source_entity = clause_units.get(source_key)
                            if not source_entity:
                                logging.warning(f"📄警告：引用关系的源节点不存在{relation}")
                                # TODO 启动模糊匹配
                                continue
                            # 获取引用依据
                            target_entity = references.get(target_key)
                            if not target_entity:
                                logging.warning(f"📄警告：引用关系的目标节点不存在{relation}")
                                # TODO 启动模糊匹配
                                continue
                            # 获取引用依据是否是内部条款
                            inner_tag = clean_string_with_only_words(target_entity.get("properties", {}).get("是否本文件内引用", "否"))
                            if inner_tag == "是":
                                source_entity["内部引用依据"].append(target_entity)
                            else:
                                source_entity["外部引用依据"].append(target_entity)
                            processed_keys.append(target_key)
                    except Exception as e:
                        logging.error(f"📄❌：处理关系{relation}时出错 - {e}")
                        continue
                # 检验未被使用的引用依据
                for key, _ in references.items():
                    if key not in processed_keys:
                        logging.warning(f"📄警告：引用依据{key}未被使用！")
                # 将条款单元添加到结果中
                for entity in clause_units.values():
                    clause_result["条款单元"].append(entity)
                clause_cache.clause_cache[clause_number] = clause_result
                return clause_result
            except Exception as e:
                logging.error(f"📄❌：处理文件{filename}时出错 - {e}")

    @staticmethod
    async def process_extracted_data(
            filename: str,
            extracted_file_info: dict,
            extracted_success_clauses: list[dict],
            extracted_failed_clauses: list[dict]
    ) -> dict:
        """
        处理提取的法规文件信息

        :param filename:
        :param extracted_file_info:
        :param extracted_success_clauses:
        :param extracted_failed_clauses:
        :return:
        """
        try:
            final_kg = {
                "nodes": [],
                "edges": []
            }
            # 将法规文件和文件依据节点和关系加入
            file_node_id = extracted_file_info.get("node_id")
            file_node_name = extracted_file_info.get("node_name")
            file_node_type = extracted_file_info.get("node_type")
            if not file_node_id or not file_node_name or not file_node_type:
                logging.error(f"📄🔥：法规文件信息不完整{extracted_file_info}")
                raise ValueError("法规文件信息不完整")
            final_kg["nodes"].append(
                {
                    "node_id": file_node_id,
                    "node_name": file_node_name,
                    "node_type": file_node_type,
                    "properties": extracted_file_info.get("properties", {}),
                    "filename": filename
                }
            )
            # 处理法规依据
            file_basis = extracted_file_info.get("法规依据", [])
            for basis in file_basis:
                basis_node_id = basis.get("node_id")
                basis_node_name = basis.get("node_name")
                basis_node_type = basis.get("node_type")
                if not basis_node_id or not basis_node_name or not basis_node_type:
                    logging.warning(f"📄🔧：法规依据信息不完整{basis}")
                    continue
                final_kg["nodes"].append(
                    {
                        "node_id": basis_node_id,
                        "node_name": basis_node_name,
                        "node_type": basis_node_type,
                        "properties": basis.get("properties", {}),
                        "filename": filename
                    }
                )
                final_kg["edges"].append(
                    {
                        "source_id": file_node_id,
                        "target_id": basis_node_id,
                        "relation_type": "依据",
                        "directionality": "单向",
                        "properties": {},
                        "filename": filename
                    }
                )

            # 将extracted_failed_clauses中的法条节点和关系加入
            for clause in extracted_failed_clauses:
                clause_number = clause.get("条款编号")
                clause_text = clause.get("条款内容")
                if not clause_number or not clause_text:
                    logging.warning(f"📄🔧：法条信息不完整{clause}")
                    continue
                clause_node_id = clean_string_for_neo4j_extended(f"法条_{generate_hex_uuid()}")
                clause_node_name = clause_number
                clause_node_type = "法条"
                clause_properties = {
                    "章": clause.get("章"),
                    "节": clause.get("节"),
                    "条": clause_number,
                    "法条全文": clause_text
                }
                final_kg["nodes"].append(
                    {
                        "node_id": clause_node_id,
                        "node_name": clause_node_name,
                        "node_type": clause_node_type,
                        "properties": clause_properties,
                        "filename": filename
                    }
                )
                final_kg["edges"].append(
                    {
                        "source_id": file_node_id,
                        "target_id": clause_node_id,
                        "relation_type": "包含",
                        "directionality": "单向",
                        "properties": {},
                        "filename": filename
                    }
                )

            # 引用依据映射
            inner_reference_mapping = {}
            outer_reference_mapping = {}
            inner_reference_id_mapping = {}
            outer_reference_id_mapping = {}
            # 条款单元到法条的映射
            unit_to_clause_mapping = {}
            # 将extracted_success_clauses中的法条、条款单元、引用依据节点和关系加入
            for clause in extracted_success_clauses:
                clause_node_id = clause.get("node_id")
                clause_node_name = clause.get("node_name")
                clause_node_type = clause.get("node_type")
                if not clause_node_id or not clause_node_name or not clause_node_type:
                    logging.warning(f"📄🔧：法条信息不完整{clause}")
                    continue
                # 添加法条节点和关系
                final_kg["nodes"].append(
                    {
                        "node_id": clause_node_id,
                        "node_name": clause_node_name,
                        "node_type": clause_node_type,
                        "properties": clause.get("properties", {}),
                        "filename": filename
                    }
                )
                final_kg["edges"].append(
                    {
                        "source_id": file_node_id,
                        "target_id": clause_node_id,
                        "relation_type": "包含",
                        "directionality": "单向",
                        "properties": {},
                        "filename": filename
                    }
                )
                # 将法条作为内部引用依据之一
                try:
                    clause_article = clean_string_with_only_words(clause.get("properties", {}).get("条"))
                except Exception:
                    clause_article = ""
                if not clause_article:
                    logging.warning(f"📄🔧：法条信息缺少条编号{clause}")
                else:
                    inner_reference_id_mapping[clause_article] = clause_node_id
                # 处理条款单元节点和关系
                clause_units = clause.get("条款单元", [])
                for unit in clause_units:
                    unit_node_id = unit.get("node_id")
                    unit_node_name = unit.get("node_name")
                    unit_node_type = unit.get("node_type")
                    if not unit_node_id or not unit_node_name or not unit_node_type:
                        logging.warning(f"📄🔧：条款单元信息不完整{unit}")
                        continue
                    final_kg["nodes"].append(
                        {
                            "node_id": unit_node_id,
                            "node_name": unit_node_name,
                            "node_type": unit_node_type,
                            "properties": unit.get("properties", {}),
                            "filename": filename
                        }
                    )
                    final_kg["edges"].append(
                        {
                            "source_id": clause_node_id,
                            "target_id": unit_node_id,
                            "relation_type": "包含",
                            "directionality": "单向",
                            "properties": {},
                            "filename": filename
                        }
                    )
                    # 将条款单元到法条的映射加入
                    unit_to_clause_mapping[unit_node_id] = clause_node_id
                    # 将条款单元作为内部引用依据之一
                    try:
                        unit_article = clean_string_with_only_words(unit.get("properties", {}).get("单元编号"))
                    except Exception:
                        unit_article = ""
                    if not unit_article:
                        logging.warning(f"📄🔧：条款单元信息缺少单元编号{unit}")
                    else:
                        inner_reference_id_mapping[unit_article] = unit_node_id
                    # 添加内部引用依据和外部引用依据
                    inner_reference = unit.get("内部引用依据", [])
                    for ref in inner_reference:
                        inner_reference_mapping[unit_node_id] = ref
                    outer_reference = unit.get("外部引用依据", [])
                    for ref in outer_reference:
                        outer_reference_mapping[unit_node_id] = ref

            # 处理内部引用依据
            for unit_node_id, inner_ref in inner_reference_mapping.items():
                ref_node_id = inner_ref.get("node_id")
                ref_node_name = inner_ref.get("node_name")
                ref_node_type = inner_ref.get("node_type")
                if not ref_node_id or not ref_node_name or not ref_node_type:
                    logging.warning(f"📄🔧：引用依据信息不完整{inner_ref}")
                    continue
                # 匹配内部条款单元
                try:
                    inner_ref_article = clean_string_with_only_words(inner_ref.get("properties", {}).get("条款编号"))
                except Exception:
                    inner_ref_article = ""
                if not inner_ref_article:
                    logging.warning(f"📄🔧：引用依据信息缺少款项编号{inner_ref}")
                else:
                    ref_unit_node_id = inner_reference_id_mapping.get(inner_ref_article)
                    if not ref_unit_node_id:
                        logging.warning(f"📄🔧：引用依据款项编号未找到对应本文件条款单元{inner_ref}")
                        # TODO：加入模糊匹配
                        # 如果是“第X条第X项”，则尝试匹配“第X条第一款第X项”
                        match = re.match(
                            r'^(第[零一二三四五六七八九十百千万\d]+条)(第[零一二三四五六七八九十百千万\d]+项)$',
                            inner_ref_article)
                        if match:
                            article_part, item_part = match.groups()
                            # 尝试"第X条第一款第X项"格式
                            alternative_article = f"{article_part}第一款{item_part}"
                            logging.warning(f"📄🔧：尝试匹配{alternative_article}")
                            ref_unit_node_id = inner_reference_id_mapping.get(alternative_article)
                        if not ref_unit_node_id:
                            # 尝试匹配“第X条第X款”
                            match = re.match(r'^(第[零一二三四五六七八九十百千万\d]+条)(第[零一二三四五六七八九十百千万\d]+款)$',
                                             inner_ref_article)
                            if match:
                                article_part, clause_part = match.groups()
                                alternative_article = f"{article_part}{clause_part}"
                                logging.warning(f"📄🔧：尝试匹配{alternative_article}")
                                ref_unit_node_id = inner_reference_id_mapping.get(alternative_article)
                            if not ref_unit_node_id:
                                # 如果含“第X条”，则尝试匹配“第X条”
                                match = re.match(r'(第[零一二三四五六七八九十百千万\d]+条)', inner_ref_article)
                                if match:
                                    basic_article = match.group(1)
                                    logging.warning(f"📄🔧：尝试匹配{basic_article}")
                                    ref_unit_node_id = inner_reference_id_mapping.get(basic_article)
                    if not ref_unit_node_id:
                        logging.warning(f"📄🔧：模糊匹配后引用依据款项编号未找到对应本文件条款单元{inner_ref}")
                    if ref_unit_node_id == unit_node_id:
                        logging.warning(f"📄🔧：引用依据款项编号与当前条款单元编号一致{inner_ref}")
                    elif ref_unit_node_id == unit_to_clause_mapping.get(unit_node_id):
                        logging.warning(f"📄🔧：引用依据款项编号与当前条款单元对应的法条编号一致{inner_ref}")
                    else:
                        final_kg["edges"].append(
                            {
                                "source_id": unit_node_id,
                                "target_id": ref_unit_node_id,
                                "relation_type": "依据",
                                "directionality": "单向",
                                "properties": {},
                                "filename": filename
                            }
                        )

            # 处理外部引用依据
            for unit_node_id, outer_ref in outer_reference_mapping.items():
                ref_node_id = outer_ref.get("node_id")
                ref_node_name = outer_ref.get("node_name")
                ref_node_type = outer_ref.get("node_type")
                if not ref_node_id or not ref_node_name or not ref_node_type:
                    logging.warning(f"📄🔧：引用依据信息不完整{outer_ref}")
                    continue
                # 如果外部引用依据节点id映射不存在，则创建节点并建立关系
                ref_unit_node_id = outer_reference_id_mapping.get(clean_string_with_only_words(ref_node_name))
                # TODO:处理同义实体
                if not ref_unit_node_id:
                    final_kg["nodes"].append(
                        {
                            "node_id": ref_node_id,
                            "node_name": ref_node_name,
                            "node_type": ref_node_type,
                            "properties": outer_ref.get("properties", {}),
                            "filename": filename
                        }
                    )
                    final_kg["edges"].append(
                        {
                            "source_id": unit_node_id,
                            "target_id": ref_node_id,
                            "relation_type": "涉及",
                            "directionality": "单向",
                            "properties": {},
                            "filename": filename
                        }
                    )
                    outer_reference_id_mapping[clean_string_with_only_words(ref_node_name)] = ref_unit_node_id
                elif ref_unit_node_id == unit_node_id:
                    logging.warning(f"📄🔧：引用依据款项编号与当前条款单元编号一致{outer_ref}")
                # 如果外部引用依据节点id映射存在，则直接创建关系
                else:
                    final_kg["edges"].append(
                        {
                            "source_id": unit_node_id,
                            "target_id": ref_unit_node_id,
                            "relation_type": "涉及",
                            "directionality": "单向",
                            "properties": {},
                            "filename": filename
                        }
                    )
            return final_kg
        except Exception as e:
            logging.error(f"📄🔧：处理抽取数据时出错{e}")
            raise e

    @staticmethod
    async def _save_cache_to_json(
            cache_data: ClauseCache,
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
    file_path = r"F:\企业大脑知识库系统\8.1项目\数据处理\清洗的数据\国家规章库\安全生产\生产安全事故应急预案管理办法.txt"
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except FileNotFoundError:
        # 如果真实文件不存在，创建一个模拟的示例内容进行测试
        raise ValueError("文件不存在")

    extractor = ClauseExtractor()
    result = asyncio.run(
        extractor.split_clause(
            text=content
        )
    )

    # 打印结果
    print("文件信息:", result["file_info"])
    print("\n条款数量:", len(result["clauses"]))
    print("\n条款示例:")
    for i, clause in enumerate(result["clauses"]):
        print(f"\n条款 {i + 1}: ")
        print(f"  章: {clause['章']}")
        print(f"  节: {clause['节']}")
        print(f"  条款编号: {clause['条款编号']}")
        print(f"  条款内容: {clause['条款内容']}")

# 1. 统计抽取出来的条款数和最后一项条款的编号是对应上的
