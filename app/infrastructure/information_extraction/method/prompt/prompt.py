import json

from app.infrastructure.information_extraction.method.prompt.defination import definition_for_entity, \
    definition_for_relation


def get_prompt_for_entity_and_relation_extraction(
        user_prompt: str,
        schema: str | dict
):
    if schema and isinstance(schema, str):
        temp_schema = """
# 提取内容
本体任务提取的实体schema如下：
""" + schema
    elif schema and isinstance(schema, dict):
        nodes_schema = schema.get("nodes", "")
        edges_schema = schema.get("edges", "")
        if not isinstance(nodes_schema, str):
            nodes_schema = json.dumps(nodes_schema, ensure_ascii=False, indent=2)
        if not isinstance(edges_schema, str):
            edges_schema = json.dumps(edges_schema, ensure_ascii=False, indent=2)
        temp_schema = """
# 提取内容
本体任务提取的实体schema如下：
# 实体类型
""" + nodes_schema + """
# 关系类型
""" + edges_schema
    else:
        temp_schema = ""
    return user_prompt + """

**以下内容最重要**
""" + temp_schema + """
# 定义
实体和关系的定义如下
""" + definition_for_entity + """
""" + definition_for_relation + """
# 注意事项
1. 严格按照定义的实体类型进行提取
2. 只提取文本中明确表达的信息，不要进行推断
3. 保持文本原始表述，不要改写
# 输出要求：
1. 严格按以下JSON格式输出，不要添加任何额外文本或解释
2. 确保JSON语法正确，可以被直接解析
3. 所有字符串使用双引号(")而非单引号(')
4. 不要包含任何Markdown格式或代码块标记
# 输出格式要求
以JSON格式输出，但请不要以```json````方式输出
请严格按照如下JSON字符串的格式回答：
{
    "extractions": [
        {
            "实体类型A": "实体名称a",
            "实体类型A_attributes": {
                "属性名1": "属性值1",
                "属性名2": "属性值2",
                ...
            }
        },
        ...
        {
            "关系": "关系文本x",
            "关系_attributes": {
                "主体": "实体类型A_实体名称a", 
                "谓词": "关系名称X", 
                "客体": "实体类型B_实体名称b"
            }
        },
        ...
    ]
}
"""


def get_prompt_for_entity_extraction(
        user_prompt: str,
        entity_schema: str
):
    return user_prompt + """

**以下内容最重要**
# 提取内容
为了提高抽取效率，我们将抽取任务分为两部分
本次对话仅提取实体和其内部的属性
本体任务提取的实体schema如下：
""" + entity_schema + """
# 定义
实体的定义如下
""" + definition_for_entity + """
# 注意事项
1. 严格按照定义的实体类型进行提取
2. 只提取文本中明确表达的信息，不要进行推断
3. 保持文本原始表述，不要改写
# 输出要求：
1. 严格按以下JSON格式输出，不要添加任何额外文本或解释
2. 确保JSON语法正确，可以被直接解析
3. 所有字符串使用双引号(")而非单引号(')
4. 不要包含任何Markdown格式或代码块标记
# 输出格式要求
以JSON格式输出，但请不要以```json````方式输出
请严格按照如下JSON字符串的格式回答：
{
    "extractions": [
        {
            "实体类型A": "实体名称a",
            "实体类型A_attributes": {
                "属性名1": "属性值1",
                "属性名2": "属性值2",
                ...
            }
        }
    ]
}
"""


def get_prompt_for_relation_extraction(
        user_prompt: str,
        node_list: str,
        relation_schema: str
):
    return user_prompt + """

**以下内容最重要**
# 提取内容
为了提高抽取效率，我们将抽取任务分为两部分
本次对话请根据实体提取关系
本体任务已提取的实体如下：
""" + node_list + """
本体任务提取的关系schema如下：
""" + relation_schema + """
# 定义
实体的定义如下
""" + definition_for_relation + """
# 注意事项
1. 严格按照定义的关系类型进行提取
2. 只提取文本中明确表达的信息，或根据文本明确内容进行推断
3. 保持文本原始表述，不要改写
# 输出要求：
1. 严格按以下JSON格式输出，不要添加任何额外文本或解释
2. 确保JSON语法正确，可以被直接解析
3. 所有字符串使用双引号(")而非单引号(')
4. 不要包含任何Markdown格式或代码块标记
# 输出格式要求
以JSON格式输出，但请不要以```json````方式输出
请严格按照如下JSON字符串的格式回答：
{
    "extractions": [
        {
            "关系": "关系文本x",
            "关系_attributes": {
                "主体": "实体类型A_实体名称a", 
                "谓词": "关系名称X", 
                "客体": "实体类型B_实体名称b"
            }
        }
    ]
}
"""


general_prompt = """
# 角色
您是一位知识图谱构建领域专家，擅长从文本找那个抽取出知识图谱数据
# 提示
请根据提供的内容，进行实体关系抽取。
# 输入内容
"""
