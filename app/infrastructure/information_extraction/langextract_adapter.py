"""
langextract抽取框架适配器
"""
import hashlib
import json
import logging
import os
import sys
import time
import traceback
import uuid
from typing import Optional

# 添加项目根目录到Python路径，解决相对导入问题
current_file_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_file_dir)
sys.path.insert(0, parent_dir)

from langfuse import Langfuse

from app.infrastructure.information_extraction.method import langextract as lx
# 修改相对导入为绝对导入以支持直接运行模块
try:
    from .base import (
        Entity,
        Relationship,
        IInformationExtraction, SourceText, TextClass
    )
except ImportError:
    # 当直接运行此模块时，使用绝对导入
    from app.infrastructure.information_extraction.base import (
        Entity,
        Relationship,
        IInformationExtraction, SourceText, TextClass
    )

from .method.base import LangextractConfig
from .method.langextract import data
from .method.prompt.examples import general_entity_examples, law_entity_examples, law_relationship_examples, \
    law_graph_examples
from .method.prompt.prompt import get_prompt_for_entity_extraction, get_prompt_for_relation_extraction, general_prompt, \
    get_prompt_for_entity_and_relation_extraction
from .method.prompt.schema import general_entity_schema, general_relation_schema, general_schema

logger = logging.getLogger(__name__)


class LangextractAdapter(IInformationExtraction):
    """langextract抽取框架适配器"""

    def __init__(
            self,
            max_retries: int = 5,
            config: Optional[LangextractConfig] = None
    ):
        """
        初始化langextract抽取框架适配器

        Args:
            max_retries: 最大重试次数
            config: Langextract配置对象
        """
        self.max_retries = max_retries
        self.default_config = config
        self.langfuse = Langfuse(
            secret_key="sk-lf-7d375833-5b71-4e24-924e-78be844889f0",
            public_key="pk-lf-d7448fa2-0fb3-46ef-9e6d-e472a928ac31",
            host="https://cloud.langfuse.com",
            # secret_key="sk-lf-c0a7335b-b826-4071-bc93-3952bac9c3f0",
            # public_key="pk-lf-3eaa9ef7-2d40-4fa9-85c3-ca0fa1d06657",
            # host="http://60.205.171.106:3000",
            flush_interval=10,  # 增加刷新间隔到10秒
            flush_at=50,  # 每50个事件刷新一次
            timeout=10,
        )

    async def entity_and_relationship_extract(
            self,
            user_prompt: str,
            schema: str | dict,
            input_text: str,
            examples: list = None,
            langextract_config: Optional[LangextractConfig] = None
    ) -> dict:
        """
        实体和关系one-shot抽取

         Args:
            user_prompt: 抽取提示
            schema: 实体定义
            input_text: 输入文本
            examples: 示例数据
            langextract_config: Langextract配置对象

        Returns:
            dict: 实体列表
            {
                "entities": list[Entity],
                "relations": list[Relation]
            }
        """
        # 输入验证
        if not user_prompt or not isinstance(user_prompt, str):
            print(f"Warning: 提示词应当为str, got {type(user_prompt)}，使用默认提示词")
            user_prompt = general_prompt

        if not schema or not isinstance(schema, dict | list | str):
            print(f"Warning: schema 应当为 dict、list、str, got {type(schema)}, 使用默认schema")
            schema = general_schema
        # 将schema json转为markdown
        schema_md = change_schema_json_to_md(schema)
        schema = schema_md if schema_md else schema

        if not examples or not isinstance(examples, list):
            print(f"Warning: examples 应当为 list, got {type(examples)}, 使用默认示例数据")
            examples = law_graph_examples

        if not isinstance(input_text, str):
            print(f"Error: input_text should be a string, got {type(input_text)}")
            return {}

        if not input_text.strip():
            print("Warning: input_text is empty or contains only whitespace")
            return {}

        config = langextract_config or self.default_config

        if examples is None or examples == []:
            raise ValueError("langextract需要示例，但未提供示例数据")

        # 从Langfuse获取提示词
        try:
            base_prompt = self.langfuse.get_prompt(
                name="kg_extraction/langextract/prompt_for_entity_and_relation_extraction",
                label="production"
            )
            entity_def_prompt = self.langfuse.get_prompt(
                name="kg_extraction/definition_for_entity",
                label="production"
            )
            relation_def_prompt = self.langfuse.get_prompt(
                name="kg_extraction/definition_for_relation",
                label="production"
            )
            # schema_json = json.dumps(entity_schema, ensure_ascii=False, indent=2)
            prompt = base_prompt.compile(
                user_prompt=user_prompt,
                schema=schema if schema else "",
                entity_definition=entity_def_prompt.prompt,
                relation_definition=relation_def_prompt.prompt
            )
        except Exception as e:
            print(f"从Langfuse获取提示词失败: {e}")
            print("尝试使用默认提示词")
            if schema:
                prompt = get_prompt_for_entity_and_relation_extraction(user_prompt, str(schema))
            else:
                prompt = get_prompt_for_entity_and_relation_extraction(user_prompt, "")
        try:
            input_examples = self.convert_examples_to_example_data(examples)
            extract_result = self.extract_list_of_dict(
                prompt,
                input_examples,
                input_text,
                config
            )
            return self.convert_document_list_to_graph_dict(extract_result)
        except Exception as e:
            print(f"Error extracting nodes: {e}")
            # 打印完整的错误追踪信息
            traceback.print_exc()
            return {}

    async def entity_extract(
            self,
            user_prompt: str,
            entity_schema: dict | list,
            input_text: str,
            examples: list = None,
            langextract_config: Optional[LangextractConfig] = None
    ) -> tuple[list[Entity], list[TextClass]]:
        """
        执行实体抽取

        Args:
            user_prompt: 抽取提示
            entity_schema: 实体定义
            input_text: 输入文本
            examples: 示例数据
            langextract_config: Langextract配置对象

        Returns:
            list[Entity]: 实体列表
        """
        # 输入验证
        if not user_prompt or not isinstance(user_prompt, str):
            print(f"Warning: 提示词应当为str, got {type(user_prompt)}，使用默认提示词")
            user_prompt = general_prompt

        if not entity_schema or not isinstance(entity_schema, dict | list | str):
            print(f"Warning: entity_schema 应当为 dict、list or str, got {type(entity_schema)}，使用默认schema")
            entity_schema = general_entity_schema
        # 将schema json转为markdown
        schema_md = change_schema_json_to_md(entity_schema)
        entity_schema = schema_md if schema_md else entity_schema

        if not examples or not isinstance(examples, list):
            print(f"Warning: 示例应当为 list, got {type(examples)}，使用默认示例")
            examples = law_entity_examples

        if not isinstance(input_text, str):
            print(f"Error: input_text should be a string, got {type(input_text)}")
            return [], []

        if not input_text.strip():
            print("Warning: input_text is empty or contains only whitespace")
            return [], []

        config = langextract_config or self.default_config

        if examples is None:
            raise Exception("langextract需要示例，但未提供示例数据")

        # 从Langfuse获取提示词
        try:
            base_prompt = self.langfuse.get_prompt(
                name="kg_extraction/langextract/prompt_for_entity_extraction",
                label="production"
            )
            entity_def_prompt = self.langfuse.get_prompt(
                name="kg_extraction/definition_for_entity",
                label="production"
            )
            # schema_json = json.dumps(entity_schema, ensure_ascii=False, indent=2)
            prompt_for_entity = base_prompt.compile(
                user_prompt=user_prompt,
                entity_schema=entity_schema,
                entity_definition=entity_def_prompt.prompt,
            )
        except Exception as e:
            print(f"从Langfuse获取提示词失败: {e}")
            print("尝试使用默认提示词")
            prompt_for_entity = get_prompt_for_entity_extraction(user_prompt, str(entity_schema))
            # return []
        try:
            # prompt_for_entity = self.default_prompt.prompt_for_entity(prompt_delete, entity_schema)
            input_examples = self.convert_examples_to_example_data(examples)
            extract_result = self.extract_list_of_dict(
                prompt_for_entity,
                input_examples,
                input_text,
                config
            )
            return self.convert_document_list_to_entity_list(extract_result)
        except Exception as e:
            print(f"Error extracting nodes: {e}")
            # 打印完整的错误追踪信息
            traceback.print_exc()
            return [], []

    async def relationship_extract(
            self,
            user_prompt: str,
            entities_list: list,
            relation_schema: dict | list,
            input_text: str,
            examples: list = None,
            langextract_config: Optional[LangextractConfig] = None
    ) -> tuple[list[Relationship], list[TextClass]]:
        """
        执行关系抽取

        Args:
            user_prompt: 抽取提示
            entities_list: 实体列表
            relation_schema: 关系定义
            input_text: 输入文本
            examples: 示例数据
            langextract_config: Langextract配置对象

        Returns:
            list[Relationship]: 关系列表
        """
        # 输入验证
        if not user_prompt or not isinstance(user_prompt, str):
            print(f"Warning: 提示词应当为str, got {type(user_prompt)}，使用默认提示词")
            user_prompt = general_prompt

        if not isinstance(entities_list, list):
            print(f"Error: entities_list should be a list, got {type(entities_list)}")
            return [], []

        if not relation_schema or not isinstance(relation_schema, dict | list | str):
            print(f"Warning: relation_schema 应当为 dict、list or str, got {type(relation_schema)}，使用默认schema")
            relation_schema = general_relation_schema
        # 将schema json转为markdown
        schema_md = change_schema_json_to_md(relation_schema)
        relation_schema = schema_md if schema_md else relation_schema

        if not examples or not isinstance(examples, list):
            print(f"Error: 示例应当为 list, got {type(examples)}，使用默认示例")
            examples = law_relationship_examples

        if not isinstance(input_text, str):
            print(f"Error: input_text should be a string, got {type(input_text)}")
            return [], []

        if not input_text.strip():
            print("Warning: input_text is empty or contains only whitespace")
            return [], []

        config = langextract_config or self.default_config

        if examples is None:
            raise Exception("langextract需要示例，但未提供示例数据")

        # 对examples做特殊处理。
        temp_examples = []
        for example in examples:
            edges = example.get("extractions", [])
            relations = []
            for edge in edges:
                relations.append(
                    {
                        "name": "",
                        "type": "关系",
                        "attributes": {
                            "主体": edge.get("subject") or edge.get("主体") or edge.get("主语"),
                            "谓词": edge.get("predicate") or edge.get("谓词") or edge.get("关系") or edge.get("谓语"),
                            "客体": edge.get("object") or edge.get("客体") or edge.get("宾语"),
                        }
                    }
                )
            temp_examples.append({
                "text": example.get("text"),
                "extractions": relations
            })
        examples = temp_examples

        # 从Langfuse获取提示词
        try:
            base_prompt = self.langfuse.get_prompt(
                name="kg_extraction/langextract/prompt_for_relation_extraction",
                label="production"
            )
            relation_def_prompt = self.langfuse.get_prompt(
                name="kg_extraction/definition_for_relation",
                label="production"
            )
            # schema_json = json.dumps(relation_schema, ensure_ascii=False, indent=2)
            prompt_for_relation = base_prompt.compile(
                user_prompt=user_prompt,
                node_list=entities_list,
                relation_schema=relation_schema,
                relation_definition=relation_def_prompt.prompt,
            )
        except Exception as e:
            print(f"从Langfuse获取提示词失败: {e}")
            print("尝试使用默认提示词")
            prompt_for_relation = get_prompt_for_relation_extraction(user_prompt, str(entities_list), str(relation_schema))
            # return []

        try:
            # 遍历实体，记录实体名称列表
            entity_names = []
            for i, entity in enumerate(entities_list):
                # 确保entity是Entity对象且包含name属性
                if isinstance(entity, Entity) and hasattr(entity, 'name'):
                    entity_names.append(entity.name)
                else:
                    print(
                        f"Warning: entity at index {i} should be an Entity object with 'name' attribute, got {type(entity)}")

            # prompt_for_relation = self.default_prompt.prompt_for_relation(prompt_delete, entity_names,
            # relation_schema)
            input_examples = self.convert_examples_to_example_data(examples)
            extract_result = self.extract_list_of_dict(
                prompt_for_relation,
                input_examples,
                input_text,
                config
            )

            # 将提取结果转换为关系列表
            return self.convert_document_list_to_relationship_list(extract_result)
        except Exception as e:
            print(f"Error extracting relationships: {e}")
            # 打印完整的错误追踪信息
            traceback.print_exc()
            return [], []

    def convert_document_list_to_graph_dict(
            self,
            document_list: list
    ) -> dict:
        """
        将LangExtract提取结果转换为图谱

        Args:
            document_list (list): LangExtract的提取结果

        Returns:
            dict: 图谱
        """
        # 检验数据格式
        if not isinstance(document_list, list):
            print(f"Error: document_list 应该为 list, got {type(document_list)}")
            logger.error(f"document_list 应该为 list, got {type(document_list)}")
            return {}
        # 分离实体和关系
        entity_list = []
        relationship_list = []
        text_list = []
        for document in document_list:
            if not isinstance(document, dict):
                print(f"Error: document 应该为 dict, got {type(document)}")
                logger.error(f"document 应该为 dict, got {type(document)}")
            extractions = document.get("extractions")
            if not isinstance(extractions, list):
                print(f"Error: extractions 应该为 list, got {type(extractions)}")
                logger.error(f"extractions 应该为 list, got {type(extractions)}")
                continue
            elif not extractions:
                print(f"Warning: extractions 为空")
                logger.warning(f"extractions 为空")
                continue
            # 处理text
            text = document.get("text")
            if not text:
                text_id = None
            else:
                text_id = self._generate_text_id(text)
            need_text_flag = False
            # 处理extractions
            for extraction in extractions:
                if not isinstance(extraction, dict):
                    print(f"Warning: extraction 应该为 dict, got {type(extraction)}")
                    logger.warning(f"extraction 应该为 dict, got {type(extraction)}")
                    continue
                    # 获取必要字段
                extraction_text = extraction.get('extraction_text', '')
                extraction_class = extraction.get('extraction_class', '')
                # 跳过空的类名
                if not extraction_class:
                    continue
                    # 处理实体
                if extraction_class != "关系":
                    # 获取属性，默认为空字典
                    attributes = extraction.get('attributes', {})
                    if not isinstance(attributes, dict):
                        attributes = {}
                    entity = Entity(
                        name=extraction_text,
                        entity_type=extraction_class,
                        properties=attributes,
                    )
                    if text_id:
                        source_text = SourceText(
                            id=text_id,
                        )
                        char_interval = extraction.get('char_interval')
                        if char_interval:
                            start_pos = char_interval.get('start_pos')
                            end_pos = char_interval.get('end_pos')
                            source_text.start_pos = start_pos
                            source_text.end_pos = end_pos
                            alignment_status = char_interval.get('alignment_status')
                            source_text.alignment_status = alignment_status
                            entity.source_texts.append(source_text)
                    entity_list.append(entity)
                    need_text_flag = True
                # 处理关系
                else:
                    # 获取属性，默认为空字典
                    relation = extraction.get('attributes', {})
                    if not isinstance(relation, dict):
                        relation = {}
                    # 检查必需的字段是否存在（根据v2_langextrct_to_graph.py中的逻辑）
                    source = relation.get("主体") or relation.get("主语")
                    relation_type = relation.get("谓词") or relation.get("谓语")
                    target = relation.get("客体") or relation.get("宾语")
                    # 跳过缺少必要字段的关系
                    if not source or not relation_type or not target:
                        continue
                    relationship = Relationship(
                        source=source,
                        target=target,
                        type=relation_type,
                    )
                    if text_id:
                        source_text = SourceText(
                            id=text_id,
                        )
                        char_interval = extraction.get('char_interval')
                        if char_interval:
                            start_pos = char_interval.get('start_pos')
                            end_pos = char_interval.get('end_pos')
                            source_text.start_pos = start_pos
                            source_text.end_pos = end_pos
                            alignment_status = char_interval.get('alignment_status')
                            source_text.alignment_status = alignment_status
                            relationship.source_texts.append(source_text)
                    relationship_list.append(relationship)
                    need_text_flag = True
            if need_text_flag:
                text_entry = TextClass(
                    id=text_id,
                    text=text,
                )
                text_list.append(text_entry)
        # 去重、合并
        processed_entity_list = self._merge_entities(entity_list)
        processed_relationship_list = self._merge_relationships(relationship_list)
        # 提取构建格式化数据
        result = {
            "entities": processed_entity_list,
            "relations": processed_relationship_list,
            "texts_classes": text_list,
        }

        return result

    def convert_document_list_to_entity_list(
            self,
            extraction_result: list
    ) -> tuple[list[Entity], list[TextClass]]:
        """
        将LangExtract提取结果转换为节点列表格式

        Args:
            extraction_result (dict): LangExtract的提取结果

        Returns:
            list[Entity]: 包含实体的列表，每个实体以指定格式表示
        :return:
        """
        if not extraction_result:
            return [], []
        graph_result = self.convert_document_list_to_graph_dict(extraction_result)
        if not graph_result:
            return [], []
        return graph_result.get("entities", []), graph_result.get("texts", [])

    def convert_document_list_to_relationship_list(
            self,
            extraction_result: list
    ) -> tuple[list[Relationship], list[TextClass]]:
        """
        将LangExtract提取结果转换为关系列表格式

        Args:
            extraction_result (dict): LangExtract的提取结果

        Returns:
            list[Relationship]: 包含关系的列表
        """
        if not extraction_result:
            return [], []
        graph_result = self.convert_document_list_to_graph_dict(extraction_result)
        return graph_result.get("relations", []), graph_result.get("texts", [])

    def extract_list_of_dict(
            self,
            prompt: str,
            examples: list,
            input_text: str,
            langextract_config: Optional[LangextractConfig] = None
    ):
        """
        从文本中提取知识，包含重试机制

        Args:
           prompt (str): 提示词
           examples (list): 示例数据
           input_text (str): 输入文本
           langextract_config (LangextractConfig, optional): 模型配置
        Returns:
           list(dict): 提取结果

        Raises:
           Exception: 如果所有重试都失败则抛出异常
        """
        # 使用传入的配置或实例默认配置
        config = langextract_config or self.default_config

        if not config:
            raise ValueError("未提供Langextract配置")

        # 检查输入文本是否为空
        if not input_text or not input_text.strip():
            print("警告: 输入文本为空或只包含空白字符")
            return []

        # 检查示例数据
        if not examples:
            print("警告: 示例数据为空")
            return []

        # 初始化重试参数
        last_exception = None

        for attempt in range(self.max_retries):
            try:
                print(f"尝试第 {attempt + 1}/{self.max_retries} 次提取...")

                # 打印配置信息用于调试
                print(f"配置信息: model_id={config.model_name}, format_type={config.format_type}")

                # 使用附加模型language_model_type=CustomAPIModel时，需要为language_model_params添加参数"api_url"
                if config.language_model_type == lx.inference.CustomAPIModel:
                    config.config["api_url"] = config.api_url
                    print(f"设置自定义API URL: {config.api_url}")

                print("调用lx.extract方法...")
                # print("-----------------------------------------")
                # print("input_text: " + input_text)
                # print("prompt: " + prompt)
                # print("examples: " + str(examples))
                # print("-----------------------------------------")
                # print("分块大小: " + str(config.max_char_buffer))
                result = lx.extract(
                    text_or_documents=input_text,
                    prompt_description=prompt,
                    examples=examples,
                    model_id=config.model_name,
                    api_key=config.api_key,
                    language_model_type=config.language_model_type,
                    format_type=config.format_type,
                    max_char_buffer=config.max_char_buffer,
                    temperature=config.temperature,
                    fence_output=config.fence_output,
                    use_schema_constraints=config.use_schema_constraints,
                    batch_length=config.batch_length,
                    max_workers=config.max_workers,
                    additional_context=config.additional_context,
                    resolver_params=config.resolver_params,
                    debug=config.debug,
                    model_url=config.api_url,
                    extraction_passes=config.extraction_passes,
                    language_model_params=config.config
                )

                print(f"第 {attempt + 1} 次尝试成功!")
                # 转化为统一的list格式
                if isinstance(result, data.AnnotatedDocument):
                    result_list = [result]
                else:
                    result_list = list(result)
                document_dict_list = self.convert_document_to_dict_with_text(result_list)
                # try:
                #     formatted_json = json.dumps(
                #         document_dict_list,
                #         indent=2,
                #         ensure_ascii=False,
                #         default=lambda obj: str(obj) if hasattr(obj, '__dict__') else obj
                #     )
                #     # print("提取结果的格式化JSON输出:")
                #     # print(formatted_json)
                #     logger.info("提取结果的JSON输出:")
                #     logger.info(formatted_json)
                # except Exception as json_error:
                #     logger.warning("格式化JSON输出失败:")
                    # print(f"格式化JSON输出失败: {json_error}")
                return document_dict_list
                # return self.convert_annotated_document_to_dict(result)

            except Exception as e:
                last_exception = e
                print(f"第 {attempt + 1} 次尝试失败: {e}")
                # 打印完整的错误追踪信息
                # traceback.print_exc()
                # 如果不是最后一次尝试，等待一段时间再重试
                if attempt < self.max_retries - 1:
                    # 指数退避策略: 等待 2^attempt 秒
                    wait_time = 30 * (2 ** attempt)
                    print(f"等待 {wait_time} 秒后进行下一次尝试...")
                    time.sleep(wait_time)

                    # 特殊处理API限流错误
                    if "429" in str(e) or "rate limit" in str(e).lower():
                        # 对于限流错误，等待更长时间
                        additional_wait = 5 * (attempt + 1)
                        print(f"检测到限流错误，额外等待 {additional_wait} 秒...")
                        time.sleep(additional_wait)

        # 所有重试都失败
        print(f"所有 {self.max_retries} 次尝试都失败了。")
        # 打印完整的错误追踪信息
        if last_exception:
            traceback.print_exc()
        # TODO 添加自定义异常处理逻辑(是否需要添加，后续可以考虑)
        raise Exception(f"知识提取失败，已重试 {self.max_retries} 次。最后一次错误: {last_exception}") \
            from last_exception

    def convert_document_to_dict_with_text(
            self,
            annotated_doc_list: list[lx.data.AnnotatedDocument]
    ) -> list:
        """
        notatedDocument 列表转换为标准字典结构。
        该结构带有溯源文本

        :param annotated_doc_list:
        :return:
        """
        # 处理空输入
        if not annotated_doc_list:
            return [{'text': '', 'extractions': []}]

        document_list = []
        for annotated_doc in annotated_doc_list:
            document_list.append(self._convert_annotated_document_to_dict(annotated_doc))

        return document_list

    @staticmethod
    def _process_extraction(extraction) -> dict:
        """
        处理单个提取项并转换为字典格式
        
        Args:
            extraction: 单个提取项对象
            
        Returns:
            dict: 提取项的字典表示
        """
        if not extraction:
            return {}

        # 处理 char_interval
        char_interval_dict = None
        if hasattr(extraction, 'char_interval') and extraction.char_interval:
            char_interval_dict = {
                'start_pos': getattr(extraction.char_interval, 'start_pos', None),
                'end_pos': getattr(extraction.char_interval, 'end_pos', None)
            }

        # 处理 alignment_status
        alignment_status_value = None
        if hasattr(extraction, 'alignment_status') and extraction.alignment_status:
            alignment_status_value = extraction.alignment_status.value \
                if hasattr(extraction.alignment_status, 'value') else str(extraction.alignment_status)

        # 处理 token_interval
        token_interval_dict = None
        if hasattr(extraction, 'token_interval') and extraction.token_interval:
            token_interval_dict = {
                'start_index': getattr(extraction.token_interval, 'start_index', None),
                'end_index': getattr(extraction.token_interval, 'end_index', None)
            }

        # 构建每个提取项的字典
        return {
            'extraction_class': getattr(extraction, 'extraction_class', ''),
            'extraction_text': getattr(extraction, 'extraction_text', ''),
            'char_interval': char_interval_dict,
            'alignment_status': alignment_status_value,
            'extraction_index': getattr(extraction, 'extraction_index', None),
            'group_index': getattr(extraction, 'group_index', None),
            'description': getattr(extraction, 'description', None),
            'attributes': getattr(extraction, 'attributes', {}),  # attributes 本身应该是一个字典
            'token_interval': token_interval_dict
        }

    def _convert_annotated_document_to_dict(
            self,
            annotated_doc: lx.data.AnnotatedDocument
    ) -> dict:
        """
        将 AnnotatedDocument 对象转换为标准字典结构。

        参数:
            annotated_doc: AnnotatedDocument 对象

        返回:
            dict: 包含文档原文和所有提取信息的字典
        """
        # 处理空输入
        if not annotated_doc:
            return {'text': '', 'extractions': []}

        # 初始化提取信息列表
        extractions_list = []

        # 遍历每个 Extraction 对象
        if annotated_doc.extractions:
            for extraction in annotated_doc.extractions:
                extraction_dict = self._process_extraction(extraction)
                if extraction_dict:  # 只添加非空的提取项
                    extractions_list.append(extraction_dict)

        # 构建最终返回的文档字典
        return {
            'text': getattr(annotated_doc, 'text', ''),
            'extractions': extractions_list
        }

    @staticmethod
    def convert_examples_to_example_data(
            examples_list: list
    ) -> list[lx.data.ExampleData]:
        """
        根据示例列表生成 ExampleData 对象列表

        Args:
            examples_list (list): 包含示例数据的列表

        Returns:
            list: ExampleData 对象列表
        """
        # 检查输入是否为列表
        if not isinstance(examples_list, list):
            print(f"Warning: examples_list should be a list, got {type(examples_list)}")
            return []

        examples = []
        for i, example in enumerate(examples_list):
            # 确保example是字典格式
            if not isinstance(example, dict):
                print(f"Warning: example at index {i} should be a dict, got {type(example)}")
                continue

            # 获取文本内容，默认为空字符串
            text = example.get("text", "")

            # 获取extractions字段
            extractions_data = example.get("extractions", [])
            if not isinstance(extractions_data, list):
                print(f"Warning: extractions at index {i} should be a list, got {type(extractions_data)}")
                extractions_data = []

            extractions = []
            for j, extraction in enumerate(extractions_data):
                # 确保extraction是字典格式
                if not isinstance(extraction, dict):
                    print(f"Warning: extraction at index {i}-{j} should be a dict, got {type(extraction)}")
                    continue

                # 获取必要字段，提供默认值
                extraction_text = extraction.get("name", "")
                extraction_class = extraction.get("type", "")
                attributes = extraction.get("attributes", {})

                # 确保attributes是字典格式
                if not isinstance(attributes, dict):
                    print(f"Warning: attributes at index {i}-{j} should be a dict, got {type(attributes)}")
                    attributes = {}

                # 跳过空的提取项
                if not extraction_class and not extraction_text:
                    print(f"Warning: skipping empty extraction at index {i}-{j}")
                    continue

                extractions.append(
                    lx.data.Extraction(
                        extraction_class=extraction_class,
                        extraction_text=extraction_text,
                        attributes=attributes
                    )
                )

            examples.append(
                lx.data.ExampleData(
                    text=text,
                    extractions=extractions
                )
            )
        return examples

    def _merge_entities(
            self,
            entity_list: list
    ) -> list:
        """
        去重、合并实体
        # TODO: 去重合并方案待优化
        :return:
        """
        if not entity_list:
            return []
            # 用于存储合并后的实体
        merged_entities_map = {}
        for entity in entity_list:
            # 以name和entity_type组合作为键
            key = (entity.name, entity.entity_type)
            if key in merged_entities_map:
                # 如果键已经存在，则合并属性，现有属性优先级更高
                existing_entity = merged_entities_map[key]
                # 安全地合并属性
                if hasattr(existing_entity, 'properties') and hasattr(entity, 'properties'):
                    # 合并属性，现有属性优先级更高，只添加不存在的属性
                    for attr_key, attr_value in entity.properties.items():
                        if attr_key not in existing_entity.properties:
                            existing_entity.properties[attr_key] = attr_value
                elif hasattr(entity, 'properties'):
                    # 如果existing_entity没有properties但entity有，则设置为entity的properties
                    existing_entity.properties = entity.properties.copy()
                # 安全地合并源文本
                if hasattr(existing_entity, 'source_texts') and hasattr(entity, 'source_texts'):
                    if entity.source_texts:
                        existing_entity.source_texts.extend(entity.source_texts)
                elif hasattr(entity, 'source_texts'):
                    # 如果existing_entity没有source_texts但entity有，则设置为entity的source_texts
                    existing_entity.source_texts = entity.source_texts.copy()
            else:
                # 否则，将实体添加到映射中
                properties = entity.properties.copy() if hasattr(entity, 'properties') else {}
                source_texts = entity.source_texts.copy() if hasattr(entity, 'source_texts') else []
                merged_entities_map[key] = Entity(
                    name=entity.name,
                    entity_type=entity.entity_type,
                    properties=properties,
                    source_texts=source_texts
                )
        return list(merged_entities_map.values())

    def merge_text_class(
            self,
            text_classes_a: list[TextClass],
            text_classes_b: list[TextClass]
    ) -> list[TextClass]:
        """
        去重、合并文本类
        # TODO: 去重合并方案待优化,目前存在的问题：合并重复文本后，节点和边对应关系丢失
        :return:
        """
        flag = 0
        if not text_classes_a:
            text_classes_a = []
        text_classes = text_classes_a.copy()
        if text_classes_b:
            # 方案零：鸵鸟策略，直接合并，存在重复文本取最长
            if flag == 0:
                for text_class_b in text_classes_b:
                    for text_class_a in text_classes_a:
                        if text_class_a.id == text_class_b.id:
                            text_class_b.text = text_class_a.text if len(text_class_a.text) > len(text_class_b.text) else text_class_b.text
                    text_classes.append(text_class_b)
            # 方案一：遍历文本,目前存在的问题：合并重复文本后，节点和边对应关系丢失
            if flag == 1:
                for text_class_b in text_classes_b:
                    for text_class_a in text_classes_a:
                        if text_class_a.text == text_class_b.text:
                            continue
                        if text_class_a.id == text_class_b.id:
                            if text_class_a.text == text_class_b.text:
                                continue
                            else:
                                text_class_b.id = text_class_a.id + "_" + str(uuid.uuid4().hex)[:8]
                                text_classes.append(text_class_b)
                        else:
                            text_classes.append(text_class_b)
        return text_classes

    def _merge_relationships(
            self,
            relationship_list: list
    ) -> list:
        """
        去重、合并关系
        # TODO: 去重合并方案待优化
        :return:
        """
        if not relationship_list:
            return []
        # 用于存储合并后的关系
        merged_relationships_map = {}
        for relationship in relationship_list:
            # 以source和target组合作为键
            key = (relationship.source, relationship.target)
            if key in merged_relationships_map:
                # 如果键已经存在，则合并属性，现有属性优先级更高
                existing_relationship = merged_relationships_map[key]
                # 安全地合并属性
                if hasattr(existing_relationship, 'properties') and hasattr(relationship, 'properties'):
                    # 合并属性，现有属性优先级更高，只添加不存在的属性
                    for attr_key, attr_value in relationship.properties.items():
                        if attr_key not in existing_relationship.properties:
                            existing_relationship.properties[attr_key] = attr_value
                elif hasattr(relationship, 'properties'):
                    # 如果existing_relationship没有properties但relationship有，则设置为relationship的properties
                    existing_relationship.properties = relationship.properties.copy()
                # 安全地合并源文本
                if hasattr(existing_relationship, 'source_texts') and hasattr(relationship, 'source_texts'):
                    if relationship.source_texts:
                        existing_relationship.source_texts.extend(relationship.source_texts)
                elif hasattr(relationship, 'source_texts'):
                    existing_relationship.source_texts = relationship.source_texts.copy()
            else:
                # 否则，将关系添加到映射中
                properties = relationship.properties.copy() if hasattr(relationship, 'properties') else {}
                source_texts = relationship.source_texts.copy() if hasattr(relationship, 'source_texts') else []
                merged_relationships_map[key] = Relationship(
                    source=relationship.source,
                    target=relationship.target,
                    type=relationship.type,
                    properties=properties,
                    source_texts=source_texts
                )
        return list(merged_relationships_map.values())

    @staticmethod
    def _generate_text_id(text_content, max_length=1000):
        # 对超长内容进行截断后再哈希，取前500和后500个字符
        if len(text_content) <= max_length:
            # 如果内容长度不超过最大长度，直接使用原文本
            truncated = text_content
        else:
            # 取前500和后500个字符
            front_part = text_content[:500]
            back_part = text_content[-500:]
            truncated = front_part + back_part
        hash_object = hashlib.sha256(truncated.encode('utf-8'))
        return hash_object.hexdigest()[:32]


def change_schema_json_to_md(
    schema_json,
):
    """
    将json格式的schema转换为markdown形式
    :param schema_json: 输入的schema
    :return:
    """
    try:
        if not schema_json or not isinstance(schema_json, dict):
            logging.warning("将schema转换为markdown时出错：输入的schema格式错误")
            return None
        markdown_output = {}
        # 处理节点定义
        if "nodes" in schema_json:
            nodes = schema_json["nodes"]
            result_nodes = nodes
            try:
                markdown_nodes = "# 节点schema\n"
                for node in nodes:
                    entity_name = node.get("entity", "未知实体")
                    description = node.get("description", "")
                    properties = node.get("properties", [])
                    markdown_nodes += f"## {entity_name}\n"
                    markdown_nodes += f"### 描述\n{description}\n"
                    markdown_nodes += "### 属性\n"
                    for prop in properties:
                        for prop_name, prop_desc in prop.items():
                            markdown_nodes += f"- {prop_name}：{prop_desc}\n"
                result_nodes = markdown_nodes
            except Exception as e:
                logging.warning(f"将schema转换为markdown时出错：节点定义转换错误，错误信息为：{e}")
            markdown_output["nodes"] = result_nodes

        # 处理边定义
        if "edges" in schema_json:
            edges = schema_json["edges"]
            result_edges = edges
            try:
                markdown_edges = "# 关系\n"
                for edge in edges:
                    relation_name = edge.get("relation", "未知关系")
                    source_name = edge.get("source_name", "")
                    target_name = edge.get("target_name", "")
                    description = edge.get("description", "")
                    directionality = edge.get("directionality", "")
                    properties = edge.get("properties", [])
                    markdown_edges += f"## {relation_name}\n"
                    markdown_edges += f"### 关系三元组\n{source_name}-{relation_name}-{target_name}\n"
                    markdown_edges += f"### 描述\n{description}\n"
                    markdown_edges += f"### 关系方向性\n{directionality}\n"
                    markdown_edges += "### 属性\n"
                    for prop in properties:
                        for prop_name, prop_desc in prop.items():
                            markdown_edges += f"- {prop_name}：{prop_desc}\n"
                result_edges = markdown_edges
            except Exception as e:
                logging.warning(f"将schema转换为markdown时出错：关系定义转换错误，错误信息为：{e}")
            markdown_output["edges"] = result_edges
        return markdown_output
    except Exception as e:
        logging.warning(f"将schema转换为markdown格式时出错：{e}")
        return None


if __name__ == "__main__":
    # 获取当前文件所在目录
    current_dir = os.path.dirname(os.path.abspath(__file__))
    output_file = os.path.join(current_dir, "test_results.txt")

    # 重定向输出到文件
    with open(output_file, "w", encoding="utf-8") as f:
        import sys

        original_stdout = sys.stdout
        sys.stdout = f

        # 测试用例1: 包含节点和边的完整schema
        complete_schema = {
            "nodes": [
                {
                    "entity": "Person",
                    "description": "表示一个人物实体",
                    "properties": [
                        {"name": "人物的姓名"},
                        {"age": "人物的年龄"},
                        {"occupation": "人物的职业"}
                    ]
                },
                {
                    "entity": "Company",
                    "description": "表示一个公司实体",
                    "properties": [
                        {"name": "公司的名称"},
                        {"location": "公司的位置"}
                    ]
                }
            ],
            "edges": [
                {
                    "source_name": "Person",
                    "target_name": "Company",
                    "relation": "WORKS_AT",
                    "description": "表示一个人在某个公司工作",
                    "directionality": "单向",
                    "properties": [
                        {"start_date": "开始工作的日期"},
                        {"position": "职位"}
                    ]
                }
            ]
        }
        print("测试用例1: 完整schema")
        result1 = change_schema_json_to_md(complete_schema)
        if result1:
            print("节点部分:")
            print(result1.get("nodes", ""))
            print("边部分:")
            print(result1.get("edges", ""))
        # 测试用例2: 只包含节点的schema
        nodes_only_schema = {
            "nodes": [
                {
                    "entity": "Book",
                    "description": "表示一本书实体",
                    "properties": [
                        {"title": "书的标题"},
                        {"author": "书的作者"},
                        {"year": "出版年份"}
                    ]
                }
            ]
        }
        print("\n测试用例2: 只有节点的schema")
        result2 = change_schema_json_to_md(nodes_only_schema)
        if result2:
            print("节点部分:")
            print(result2.get("nodes", ""))
            print("边部分:", result2.get("edges", "无"))
        # 测试用例3: 只包含边的schema
        edges_only_schema = {
            "edges": [
                {
                    "source_name": "Book",
                    "target_name": "Author",
                    "relation": "WRITTEN_BY",
                    "description": "表示一本书被某个作者写作",
                    "directionality": "单向",
                    "properties": [
                        {"publish_date": "出版日期"}
                    ]
                }
            ]
        }
        print("\n测试用例3: 只有边的schema")
        result3 = change_schema_json_to_md(edges_only_schema)
        if result3:
            print("节点部分:", result3.get("nodes", "无"))
            print("边部分:")
            print(result3.get("edges", ""))
        # 测试用例4: 空schema
        empty_schema = {}
        print("\n测试用例4: 空schema")
        result4 = change_schema_json_to_md(empty_schema)
        print("结果:", result4)
        # 测试用例5: 非字典类型输入
        print("\n测试用例5: 非字典类型输入")
        result5 = change_schema_json_to_md("not_a_dict")
        print("结果:", result5)
        # 测试用例6: 包含缺失字段的schema
        incomplete_schema = {
            "nodes": [
                {
                    "entity": "IncompleteEntity"
                    # 缺少description和properties字段
                }
            ],
            "edges": [
                {
                    "relation": "IncompleteRelation"
                    # 缺少source_name, target_name, description等字段
                }
            ]
        }
        print("\n测试用例6: 包含缺失字段的schema")
        result6 = change_schema_json_to_md(incomplete_schema)
        if result6:
            print("节点部分:")
            print(result6.get("nodes", ""))
            print("边部分:")
            print(result6.get("edges", ""))

        # 恢复标准输出
        sys.stdout = original_stdout

    print(f"测试结果已输出到文件: {output_file}")

