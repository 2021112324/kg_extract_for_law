import asyncio
import json

from app.infrastructure.information_extraction.graph_extraction import GraphExtraction
from app.tests.law_instances.test_instance import long_text

# prompt 示例
prompt = """
    请从以下法律文本中提取相关的实体信息，包括当事人、案件类型、法律条款等。
    输出格式要求：
    - 实体类型：当事人、案件类型、法律条款、时间、地点等
    - 每个实体需要包含名称和类型
    """

# schema 示例
schema = {
    "nodes": [
        {
            "entity": "当事人",
            "description": "案件中的原告、被告等当事人",
            "properties": [
                {
                    "角色": "角色是当事人的具体特征，如原告/被告/第三人等"
                }
            ]
        },
        {
            "entity": "法院",
            "description": "审理案件的法院",
            "properties": [
                {
                    "级别": "级别是法院的具体特征，表示法院级别"
                }
            ]
        },
        {
            "entity": "案件类型",
            "description": "案件的类型，如合同纠纷、侵权纠纷等",
            "properties": []
        }
    ],
    "edges": [
        {
            "source_name": "法院",
            "target_name": "案件类型",
            "relation": "审理",
            "description": "法院审理案件的关系",
            "directionality": "单向",
            "properties": [
                {
                    "时间": "时间是审理关系的具体特征"
                }
            ]
        },
        {
            "source_name": "当事人",
            "target_name": "当事人",
            "relation": "合同",
            "description": "当事人之间的合同关系",
            "directionality": "双向",
            "properties": []
        }
    ]
}

# examples 示例
examples = [
    {
        "text": "原告张三诉被告李四合同纠纷一案，依据《中华人民共和国合同法》第60条",
        "extractions": [
            {
                "name": "张三",
                "type": "当事人",
                "attributes": {
                    "角色": "原告",
                    "姓名": "张三"
                }
            },
            {
                "name": "李四",
                "type": "当事人",
                "attributes": {
                    "角色": "被告",
                    "姓名": "李四"
                }
            },
            {
                "name": "合同纠纷",
                "type": "案件类型",
                "attributes": {
                    "案件类型": "合同纠纷",
                    "案件性质": "民事纠纷"
                }
            },
            {
                "name": "中华人民共和国合同法",
                "type": "法律条款",
                "attributes": {
                    "法规名称": "中华人民共和国合同法",
                    "条款号": "第60条",
                    "法规类型": "法律"
                }
            },
            {
                "name": "",
                "type": "关系",
                "attributes": {
                    "主体": "张三",
                    "谓词": "起诉",
                    "客体": "李四"
                }
            },
            {
                "name": "",
                "type": "关系",
                "attributes": {
                    "主体": "合同纠纷",
                    "谓词": "依据",
                    "客体": "中华人民共和国合同法"
                }
            }
        ]
    }
]

# input_text 示例
input_text = long_text

extractor = GraphExtraction()


async def graph_extraction_oneshot_test():
    result = await extractor.extract_graph_oneshot(
        prompt,
        schema,
        input_text,
        examples
    )
    result_json = result
    try:
        formatted_json = json.dumps(
            result_json,
            indent=2,
            ensure_ascii=False,
            default=lambda obj: str(obj) if hasattr(obj, '__dict__') else obj
        )
        print("提取结果的格式化JSON输出:")
        print(formatted_json)
    except Exception as json_error:
        print(f"格式化JSON输出失败: {json_error}")


async def graph_extraction_multi_shot_test():
    result = await extractor.extract_graph_oneshot(
        prompt,
        schema,
        input_text,
        examples
    )
    result_json = result
    try:
        formatted_json = json.dumps(
            result_json,
            indent=2,
            ensure_ascii=False,
            default=lambda obj: str(obj) if hasattr(obj, '__dict__') else obj
        )
        print("提取结果的格式化JSON输出:")
        print(formatted_json)
    except Exception as json_error:
        print(f"格式化JSON输出失败: {json_error}")


if __name__ == "__main__":
    asyncio.run(graph_extraction_oneshot_test())
    # asyncio.run(graph_extraction_multi_shot_test())

