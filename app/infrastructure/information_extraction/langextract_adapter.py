"""
langextract抽取框架适配器
"""
import logging
import time
import traceback
from typing import Optional

from langfuse import Langfuse

from app.infrastructure.information_extraction.method import langextract as lx
from .base import (
    Entity,
    Relationship,
    IInformationExtraction
)
from .method.base import LangextractConfig
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
            return self.convert_extraction_result_to_entity_and_relation_dict(extract_result)
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
    ) -> list[Entity]:
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

        if not examples or not isinstance(examples, list):
            print(f"Warning: 示例应当为 list, got {type(examples)}，使用默认示例")
            examples = law_entity_examples

        if not isinstance(input_text, str):
            print(f"Error: input_text should be a string, got {type(input_text)}")
            return []

        if not input_text.strip():
            print("Warning: input_text is empty or contains only whitespace")
            return []

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
            return self.convert_extraction_result_to_entity_list(extract_result)
        except Exception as e:
            print(f"Error extracting nodes: {e}")
            # 打印完整的错误追踪信息
            traceback.print_exc()
            return []

    async def relationship_extract(
            self,
            user_prompt: str,
            entities_list: list,
            relation_schema: dict | list,
            input_text: str,
            examples: list = None,
            langextract_config: Optional[LangextractConfig] = None
    ) -> list[Relationship]:
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
            return []

        if not relation_schema or not isinstance(relation_schema, dict | list | str):
            print(f"Warning: relation_schema 应当为 dict、list or str, got {type(relation_schema)}，使用默认schema")
            relation_schema = general_relation_schema

        if not examples or not isinstance(examples, list):
            print(f"Error: 示例应当为 list, got {type(examples)}，使用默认示例")
            examples = law_relationship_examples

        if not isinstance(input_text, str):
            print(f"Error: input_text should be a string, got {type(input_text)}")
            return []

        if not input_text.strip():
            print("Warning: input_text is empty or contains only whitespace")
            return []

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
            return self.convert_extraction_result_to_relationship_list(extract_result)
        except Exception as e:
            print(f"Error extracting relationships: {e}")
            # 打印完整的错误追踪信息
            traceback.print_exc()
            return []

    @staticmethod
    def convert_extraction_result_to_entity_and_relation_dict(
            extraction_result: dict
    ) -> dict:
        """
        将LangExtract提取结果转换为图谱

        Args:
            extraction_result (dict): LangExtract的提取结果

        Returns:
            list[Entity]: 包含实体的列表，每个实体以指定格式表示
        """
        entities = []
        entity_keys_set = set()  # 用于去重，以name和entity_type组合作为键
        entity_properties_map = {}  # 用于存储每个实体的属性，以便合并

        relationships = []
        relationships_keys_set = set()

        result = {}

        # 确保输入是字典格式且包含extractions字段
        if not isinstance(extraction_result, dict):
            print(f"Warning: extraction_result should be a dict, got {type(extraction_result)}")
            return result

        extractions = extraction_result.get("extractions", [])
        if not isinstance(extractions, list):
            print(f"Warning: extractions should be a list, got {type(extractions)}")
            return result

        for extraction in extractions:
            # 确保extraction是字典格式
            if not isinstance(extraction, dict):
                print(f"Warning: extraction should be a dict, got {type(extraction)}")
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
                # 以name和entity_type组合作为主键
                entity_key = (extraction_text, extraction_class)
                # 如果实体已存在，合并属性
                if entity_key in entity_keys_set:
                    # 合并属性，已存在的属性优先级更高，新属性只在不存在时添加
                    existing_properties = entity_properties_map.get(entity_key, {})
                    # 只添加不存在的属性
                    for key, value in attributes.items():
                        if key not in existing_properties:
                            existing_properties[key] = value
                    entity_properties_map[entity_key] = existing_properties
                else:
                    # 添加新实体
                    entity_keys_set.add(entity_key)
                    entity_properties_map[entity_key] = attributes
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

                relation_key = (source, relation_type, target)
                if relation_key in relationships_keys_set:
                    # 跳过重复关系
                    continue
                relationships_keys_set.add(relation_key)
                relationship = Relationship(
                    source=source,
                    target=target,
                    type=relation_type,
                )
                relationships.append(relationship)
        # 根据去重后的数据构建实体列表
        for (name, entity_type), properties in entity_properties_map.items():
            entity = Entity(
                name=name,
                entity_type=entity_type,
                properties=properties
            )
            entities.append(entity)
        result = {
            "entities": entities,
            "relations": relationships
        }

        return result

    @staticmethod
    def convert_extraction_result_to_entity_list(
            extraction_result: dict
    ) -> list[Entity]:
        """
        将LangExtract提取结果转换为节点列表格式

        Args:
            extraction_result (dict): LangExtract的提取结果

        Returns:
            list[Entity]: 包含实体的列表，每个实体以指定格式表示
        """
        entities = []
        entity_keys_set = set()  # 用于去重，以name和entity_type组合作为键
        entity_properties_map = {}  # 用于存储每个实体的属性，以便合并

        # 确保输入是字典格式且包含extractions字段
        if not isinstance(extraction_result, dict):
            print(f"Warning: extraction_result should be a dict, got {type(extraction_result)}")
            return entities

        extractions = extraction_result.get("extractions", [])
        if not isinstance(extractions, list):
            print(f"Warning: extractions should be a list, got {type(extractions)}")
            return entities

        for extraction in extractions:
            # 确保extraction是字典格式
            if not isinstance(extraction, dict):
                print(f"Warning: extraction should be a dict, got {type(extraction)}")
                continue

            # 获取必要字段
            extraction_text = extraction.get('extraction_text', '')
            extraction_class = extraction.get('extraction_class', '')

            # 跳过空的提取文本或类名
            if not extraction_text or not extraction_class:
                continue

            # 获取属性，默认为空字典
            attributes = extraction.get('attributes', {})
            if not isinstance(attributes, dict):
                attributes = {}

            # 以name和entity_type组合作为主键
            entity_key = (extraction_text, extraction_class)

            # 如果实体已存在，合并属性
            if entity_key in entity_keys_set:
                # 合并属性，已存在的属性优先级更高，新属性只在不存在时添加
                existing_properties = entity_properties_map.get(entity_key, {})
                # 只添加不存在的属性
                for key, value in attributes.items():
                    if key not in existing_properties:
                        existing_properties[key] = value
                entity_properties_map[entity_key] = existing_properties
            else:
                # 添加新实体
                entity_keys_set.add(entity_key)
                entity_properties_map[entity_key] = attributes

        # 根据去重后的数据构建实体列表
        for (name, entity_type), properties in entity_properties_map.items():
            entity = Entity(
                name=name,
                entity_type=entity_type,
                properties=properties
            )
            entities.append(entity)

        return entities

    @staticmethod
    def convert_extraction_result_to_relationship_list(
            extraction_result: dict
    ) -> list[Relationship]:
        """
        将LangExtract提取结果转换为关系列表格式

        Args:
            extraction_result (dict): LangExtract的提取结果

        Returns:
            list[Relationship]: 包含关系的列表
        """
        relationships = []

        # 确保输入是字典格式且包含extractions字段
        if not isinstance(extraction_result, dict):
            print(f"Warning: extraction_result should be a dict, got {type(extraction_result)}")
            return relationships

        extractions = extraction_result.get("extractions", [])
        if not isinstance(extractions, list):
            print(f"Warning: extractions should be a list, got {type(extractions)}")
            return relationships

        for extraction in extractions:
            # 确保extraction是字典格式
            if not isinstance(extraction, dict):
                print(f"Warning: extraction should be a dict, got {type(extraction)}")
                continue

            # 检查是否为关系类型（根据v2_langextrct_to_graph.py中的逻辑）
            extraction_class = extraction.get('extraction_class', '')
            if extraction_class != "关系":
                continue

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
            relationships.append(relationship)

        return relationships

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
                return self.convert_annotated_document_to_dict(result)

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

    @staticmethod
    def convert_annotated_document_to_dict(
            annotated_doc: lx.data.AnnotatedDocument) -> dict:
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
                extraction_dict = LangextractAdapter._process_extraction(extraction)
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
