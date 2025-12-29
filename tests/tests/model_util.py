import json
import uuid
import re
import logging
import asyncio

from client_app.platform_service_client import PlatformServiceHandler  # 导入平台服务客户端，用于调用LLM模型API

logger = logging.getLogger("knowledgeService")  # 获取知识服务的日志记录器

# 默认模型ID（用于兼容旧数据，新建映射时应由前端选择）
DEFAULT_LLM_MODEL_ID = "67ea5833c5bf44f9672e37b9"


# ✅ Prompt缓存：避免日常使用时重复生成
class PromptCache:
    """轻量级Prompt缓存，优化80%的日常调用场景"""
    _entity_prompt = None  # 实体抽取Prompt缓存
    _combined_prompt = None  # 组合抽取Prompt缓存
    _schema_templates = {}  # schema_hash -> template，Schema模板缓存

    @classmethod
    def get_entity_prompt(cls):
        """
        获取实体抽取Prompt，如果缓存中不存在则生成并缓存
        
        Returns:
            实体抽取Prompt字符串
        """
        if cls._entity_prompt is None:
            cls._entity_prompt = generate_entity_prompt()
        return cls._entity_prompt

    @classmethod
    def get_combined_prompt(cls):
        """
        获取组合抽取Prompt（实体+关系），如果缓存中不存在则生成并缓存
        
        Returns:
            组合抽取Prompt字符串
        """
        if cls._combined_prompt is None:
            cls._combined_prompt = generate_combined_prompt()
        return cls._combined_prompt

    @classmethod
    def get_schema_template(cls, schema):
        """
        缓存Schema模板部分，只替换输入文本
        
        Args:
            schema: Schema定义
        
        Returns:
            Schema模板
        """
        import hashlib
        # 将schema转换为字符串并计算哈希值，用于缓存键
        schema_str = str(sorted(schema.items())) if isinstance(schema, dict) else str(schema)
        schema_hash = hashlib.md5(schema_str.encode()).hexdigest()[:8]

        # 如果缓存中不存在该schema的模板，则生成并缓存
        if schema_hash not in cls._schema_templates:
            cls._schema_templates[schema_hash] = _build_schema_template(schema)
        return cls._schema_templates[schema_hash]

    @classmethod
    def clear(cls):
        """
        清空缓存（测试或Schema变更时使用）
        """
        cls._entity_prompt = None
        cls._combined_prompt = None
        cls._schema_templates.clear()


def _build_schema_template(schema):
    """
    构建Schema模板（不含输入文本）
    
    Args:
        schema: Schema定义，包含实体和关系信息
    
    Returns:
        包含实体类型和关系约束的模板字典
    """
    entity_types = []
    # 处理Schema中的实体定义
    if "entities" in schema:
        for entity in schema["entities"]:
            entity_type = entity.get("entity_type", "")  # 获取实体类型
            attributes = entity.get("attributes", [])  # 获取实体属性列表
            # 格式化实体类型及其属性
            entity_types.append(f"- {entity_type}: {', '.join(attributes)}")

    relation_constraints = []
    # 处理Schema中的关系定义
    if "relations" in schema:
        for relation in schema["relations"]:
            rel_type = relation.get("relation_type", "")  # 获取关系类型
            # 获取源实体类型，兼容不同字段名
            source = relation.get("source_entity_type") or relation.get("source_type", "")
            # 获取目标实体类型，兼容不同字段名
            target = relation.get("target_entity_type") or relation.get("target_type", "")
            # 格式化关系约束信息
            relation_constraints.append(
                f"- **{rel_type}**: 只能连接 [{source}] -> [{target}]\n"
                f"  (subject的type必须是'{source}'，object的type必须是'{target}')"
            )

    # 返回包含实体类型定义和关系约束的模板
    return {
        'entity_types': chr(10).join(entity_types) if entity_types else "无实体类型定义",  # \n字符连接实体类型
        'relation_constraints': chr(10).join(relation_constraints) if relation_constraints else "无关系类型定义"
    }


def generate_entity_prompt():
    """
    生成实体抽取Prompt
    
    Returns:
        实体抽取的完整Prompt字符串
    """
    prompt = """
    # 专业信息抽取系统

    ## 任务概述
    作为一名专业的信息抽取工程师，您的任务是从工业领域的非结构化文本中提取结构化信息，并按照预定义的实体模式(schema)组织成标准JSON格式。

    ## 输出格式规范
    1. 所有实体必须按照以下统一格式返回：
    {
        "<实体类型>": {
            "<实体标识>": {
                "<属性1>": <值1>,
                "<属性2>": <值2>,
                ...
            }
        }
    }

    2. 格式要求：
    - 实体名称作为标识
    - 所有属性值如果是多个，使用数组格式
    - 如果属性值未知，使用 null
    - 严格遵守 schema 中定义的属性名称
    - 如果某个属性在schema中定义为`DATE`类型，请使用ISO日期格式（如`YYYY-MM-DD`）。
    - 输出必须是合法的 JSON 格式
    - **同一实体类型下，实体名称必须唯一，不能重复。遇到同名实体时请合并属性。**


    ## 示例分析

    ### 示例一：工业设备实体抽取

    #### 输入文本：
    智能机床MK-2000是由未来智造公司于2023年在德国汉堡生产的一款高精度数控机床。该设备采用西门子S7-1500PLC控制系统，主轴最高转速可达15000rpm，定位精度达到±0.001mm。

    #### 实体模式(schema)：
    [{
        "entity_type": "工业设备",
        "attributes": ["设备名称", "型号", "生产商", "控制系统", "技术参数"]
    }]

    #### 预期输出：
    {
        "工业设备": {
            "智能机床MK-2000": {
                "设备名称": "智能机床MK-2000",
                "型号": "MK-2000",
                "生产商": "未来智造公司",
                "控制系统": "西门子S7-1500PLC",
                "技术参数": ["主轴最高转速15000rpm", "定位精度±0.001mm"]
            }
        }
    }

    ### 示例二：生产线实体抽取

    #### 输入文本：
    新能源汽车电池组装线PACK-V3是绿源科技于2024年1月在上海松江工业园投产的智能生产线。该生产线采用ABB机器人系统，配备激光焊接工作站。

    #### 实体模式(schema)：
    [{
        "entity_type": "生产线",
        "attributes": ["名称", "型号", "制造商", "自动化设备", "功能模块"]
    }]

    #### 预期输出：
    {
        "生产线": {
            "新能源汽车电池组装线PACK-V3": {
                "名称": "新能源汽车电池组装线PACK-V3",
                "型号": "PACK-V3",
                "制造商": "绿源科技",
                "自动化设备": "ABB机器人系统",
                "功能模块": ["激光焊接工作站"]
            }
        }
    }

    ## 注意事项
    1. 严格按照给定的输出格式返回结果
    2. 确保所有实体类型和属性名称与schema完全一致
    3. 对于数组类型的属性，即使只有一个值也要使用数组格式
    4. 不要添加schema中未定义的属性
    5. 确保输出的JSON格式合法且一致
    6. **同一实体类型下，实体名称必须唯一，不能重复。遇到同名实体时请合并属性。**


    现在，请根据以上规则和示例，对输入文本进行实体抽取。
    """
    return prompt


def generate_relation_prompt(entity_infos=None, schema_edges=None):
    """
    生成关系抽取Prompt
    
    Args:
        entity_infos: 实体信息列表
        schema_edges: Schema中定义的关系边
    
    Returns:
        关系抽取的完整Prompt字符串
    """
    prompt = """
    # 专业关系抽取系统

    ## 任务概述
    作为一名专业的信息抽取工程师，您的任务是从工业领域的非结构化文本中提取实体间的关系，并按照预定义的关系模式(schema)组织成标准JSON格式。

    ## 输出格式规范
    1. 所有关系必须按照以下统一格式返回：
    {
        "<关系类型>": [
            {
                "subject": {"name": <实体名称>, "label": <实体类型>},
                "object": {"name": <实体名称>, "label": <实体类型>}
            },
            ...
        ]
    }
    2. subject和object都必须严格选用下方实体清单中的实体，且类型(label)必须与schema_edges的source/target要求一致。
    3. 不允许自造实体或类型。
    4. 输出必须是合法的JSON格式。

    ## 示例分析
    ### 示例：
    假设实体清单如下：
    [
      {"name": "A设备", "label": "设备"},
      {"name": "B部件", "label": "部件"}
    ]
    schema_edges如下：
    - 包含: subject类型必须是"设备"，object类型必须是"部件"
    则关系抽取输出：
    {
      "包含": [
        {"subject": {"name": "A设备", "label": "设备"}, "object": {"name": "B部件", "label": "部件"}}
      ]
    }

    ## 注意事项
    1. subject和object必须严格来自实体清单，且类型(label)必须与schema_edges要求一致。
    2. 不允许自造实体或类型。
    3. 输出必须是合法的JSON格式。
    """
    # 如果提供了实体信息，在Prompt中添加实体清单
    if entity_infos:
        prompt += "\n\n## 已有实体清单（请严格选用下列实体及类型）\n" + str(entity_infos)
    # 如果提供了Schema边信息，在Prompt中添加关系类型要求
    if schema_edges:
        prompt += "\n\n## 关系类型及要求（source/target类型）\n"
        for edge in schema_edges:
            prompt += f"- {edge['label']}: subject类型必须是“{edge['source']}”，object类型必须是“{edge['target']}”\\n"
    return prompt


def generate_query(schema, input_data):
    """
    ✅ 优化：使用缓存模板，减少80%日常调用的重复计算
    
    Args:
        schema: Schema定义
        input_data: 输入文本数据
    
    Returns:
        完整的查询字符串
    """
    # 从缓存中获取Schema模板，避免重复计算
    template = PromptCache.get_schema_template(schema)

    user_query = f"""
## 输入文本
{input_data}

## ⚠️ Schema约束详情（必须严格遵守）

### 可用实体类型及属性
{template['entity_types']}

### 可用关系类型及连接约束（最关键）
以下每种关系类型只能连接特定的实体类型对：
{template['relation_constraints']}

## ⚠️ 关键抽取规则（违反将导致数据被拒绝）
1. **关系连接严格匹配**：
   - 抽取关系时，必须检查subject和object的类型是否与Schema定义完全一致
   - 如果文本中的关系不符合Schema定义的实体类型对，则不要抽取该关系
   - 例如：如果Schema规定"是火力发电厂中的关键子系统"只能是"锅炉系统分类->汽轮机组件与运行"
     则不能抽取"锅炉系统分类->蒸汽生成与传热"这样的关系
   
2. **字段完整性**：
   - 关系的subject和object必须同时包含"name"和"type"字段
   - type字段必须与Schema中定义的实体类型名称完全一致

3. **宁缺毋滥**：
   - 如果不确定是否符合Schema约束，则不要抽取
   - 宁可少抽取，也不要抽取错误的关系

请按照上述约束进行抽取，返回JSON格式结果。
"""
    return user_query


async def call_model(input_data, generate_prompt_func, schema, model_id):
    """
    调用模型进行抽取
    
    Args:
        input_data: 输入文本数据
        generate_prompt_func: 生成Prompt的函数
        schema: Schema定义
        model_id: 模型ID
    
    Returns:
        抽取结果或None
    """
    # 生成基础Prompt
    prompt = generate_prompt_func()

    # 生成具体的查询字符串
    query = generate_query(schema, input_data)
    
    # 生成唯一用户ID
    user = str(uuid.uuid4()).replace("-", "_")

    # 判定 model_id 是否有效，否则使用默认模型ID
    if not model_id:
        logger.warning(f"[LLM调用] model_id为空，使用默认模型: {DEFAULT_LLM_MODEL_ID}")
        use_model_id = DEFAULT_LLM_MODEL_ID
    else:
        use_model_id = model_id

    try:
        # 调用LLM模型API
        text_result = PlatformServiceHandler.use_llm_model_api(
            model_id=use_model_id,
            prompt=prompt,
            user=user,
            query=query,
            stream=False  # 非流式响应
        )

        if text_result is not None:
            # 使用正则表达式移除Markdown代码块标记
            cleaned_text = re.sub(r'^```json\n|```$', '', text_result, flags=re.M)
            result = json.loads(cleaned_text)
            return result
        else:
            logger.error("[LLM调用] LLM返回None!")
            return None
    except json.JSONDecodeError as e:
        logger.error(f"[LLM调用] JSON解析失败: {e}")
        logger.error(f"[LLM调用] 原始返回: {text_result[:1000] if text_result else 'None'}")
        return None
    except Exception as e:
        logger.error(f"[LLM调用] 调用异常: {type(e).__name__}: {e}")
        import traceback
        logger.error(f"[LLM调用] 异常堆栈:\n{traceback.format_exc()}")
        return None


async def call_model_to_extract_entity(input_data, schema_for_entity, model_id):
    """
    调用模型进行实体抽取
    
    Args:
        input_data: 输入文本数据
        schema_for_entity: 实体Schema定义
        model_id: 模型ID
    
    Returns:
        实体抽取结果
    """
    return await call_model(input_data, generate_entity_prompt, schema_for_entity, model_id)


async def call_model_to_extract_relation(input_data, schema_for_relation, model_id, entity_infos=None,
                                         schema_edges=None):
    """
    调用模型进行关系抽取
    
    Args:
        input_data: 输入文本数据
        schema_for_relation: 关系Schema定义
        model_id: 模型ID
        entity_infos: 实体信息列表
        schema_edges: Schema边定义
    
    Returns:
        关系抽取结果
    """
    return await call_model(input_data, lambda: generate_relation_prompt(entity_infos, schema_edges),
                            schema_for_relation, model_id)


def generate_combined_prompt():
    """
    ✅ 优化：一次性抽取实体和关系的详细prompt

    关键改进：
    1. 明确Schema约束说明
    2. 详细的关系类型和实体类型对应关系
    3. 严格的格式要求
    
    Returns:
        组合抽取的完整Prompt字符串
    """
    prompt = """
# 专业知识图谱抽取系统

## 任务概述
作为专业的信息抽取工程师，从工业技术文本中抽取结构化知识，严格按照预定义Schema进行。

## 输出格式（必须严格遵守）
```json
{
  "entities": {
    "实体类型": {
      "实体名称": {
        "属性名": "属性值"
      }
    }
  },
  "relations": {
    "关系类型": [
      {
        "subject": {"name": "主体实体名", "type": "主体实体类型"},
        "object": {"name": "客体实体名", "type": "客体实体类型"}
      }
    ]
  }
}
```

## 关键约束（违反将导致数据被拒绝）
1. **实体类型约束**：只能使用Schema中定义的实体类型，禁止自造类型
2. **关系类型约束**：只能使用Schema中定义的关系类型，禁止自造关系
3. **关系连接约束（最严格）**：每种关系类型只能连接特定的实体类型对
   - 例如：如果Schema规定"位于"只能是"公司->城市"，则不能抽取"人物->城市"
   - 如果文本中的关系不符合Schema定义的实体类型对，请勿抽取该关系
4. **字段完整性**：subject和object必须同时包含name和type字段
5. **实体一致性**：关系中的实体必须在entities部分存在

## 格式要求
- 实体名称唯一，同名实体合并属性
- 未知属性使用null
- 多值属性使用数组格式
- 日期使用YYYY-MM-DD格式
- 输出必须是合法JSON

## Schema约束说明
请仔细阅读以下Schema定义，严格按照约束进行抽取：

**可用实体类型**：[将在query中动态插入]
**可用关系类型及约束**：[将在query中动态插入]

现在开始抽取：
"""
    return prompt


async def call_model_to_extract_combined(input_data, entity_schema, relation_schema, model_id):
    """
    ✅ 优化：一次性抽取实体和关系

    Args:
        input_data: 输入文本
        entity_schema: 实体schema列表
        relation_schema: 关系schema列表
        model_id: 模型ID

    Returns:
        {
            "entities": {...},
            "relations": {...}
        }
    """
    # 构建组合schema，包含实体和关系定义
    combined_schema = {
        "entities": entity_schema,
        "relations": relation_schema
    }

    # ✅ 使用缓存的Prompt，减少内存分配
    prompt = PromptCache.get_combined_prompt()
    # 生成具体的查询字符串
    query = generate_query(combined_schema, input_data)

    # 生成唯一用户ID
    user = str(uuid.uuid4()).replace("-", "_")

    # 判定 model_id 是否有效
    if not model_id:
        logger.warning(f"[Combined抽取] model_id为空，使用默认模型: {DEFAULT_LLM_MODEL_ID}")
        use_model_id = DEFAULT_LLM_MODEL_ID
    else:
        use_model_id = model_id

    try:
        # ✅ 使用asyncio.to_thread实现真正的并行
        text_result = await asyncio.to_thread(
            PlatformServiceHandler.use_llm_model_api,
            model_id=use_model_id,
            prompt=prompt,
            user=user,
            query=query,
            stream=False  # 非流式响应
        )

        if text_result is not None:
            # 清理Markdown代码块标记
            cleaned_text = re.sub(r'^```json\n|```$', '', text_result, flags=re.M)
            result = json.loads(cleaned_text)

            # 统计抽取结果
            entity_count = sum(len(v) for v in result.get('entities', {}).values())
            relation_count = sum(len(v) for v in result.get('relations', {}).values())
            logger.debug(f"[Combined抽取] 解析: {entity_count}实体, {relation_count}关系")
            
            return result
        else:
            logger.error("[Combined抽取] ❌ LLM返回None!")
            # 返回空结果
            return {"entities": {}, "relations": {}}
    
    except json.JSONDecodeError as e:
        logger.error(f"[Combined抽取] ❌ JSON解析失败: {e}")
        logger.error(f"[Combined抽取] 原始返回内容: {text_result[:1000] if text_result else 'None'}")
        # 返回空结果
        return {"entities": {}, "relations": {}}
    except Exception as e:
        logger.error(f"[Combined抽取] ❌ 异常: {type(e).__name__}: {e}")
        import traceback
        logger.error(f"[Combined抽取] 堆栈:\n{traceback.format_exc()}")
        # 返回空结果
        return {"entities": {}, "relations": {}}