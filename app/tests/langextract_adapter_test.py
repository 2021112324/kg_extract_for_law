import json
import os

from app.infrastructure.information_extraction.langextract_adapter import LangextractAdapter, change_schema_json_to_md
from app.infrastructure.information_extraction.method.base import LangextractConfig
from app.tests.law_instances.test_instance import long_text


async def extract_list_of_dict_test():
    # prompt 示例
    prompt = """
    请从以下法律文本中提取相关的实体信息，包括当事人、案件类型、法律条款等。
    输出格式要求：
    - 实体类型：当事人、案件类型、法律条款、时间、地点等
    - 每个实体需要包含名称和类型
    """

    # schema 示例
    schema = {
        "entities": {
            "当事人": {
                "description": "案件中的原告、被告等当事人",
                "attributes": {
                    "角色": {"type": "string", "description": "原告/被告/第三人等"}
                }
            },
            "法院": {
                "description": "审理案件的法院",
                "attributes": {
                    "级别": {"type": "string", "description": "法院级别"}
                }
            },
            "案件类型": {
                "description": "案件的类型，如合同纠纷、侵权纠纷等"
            }
        },
        "relations": {
            "审理": {
                "description": "法院审理案件的关系",
                "properties": {
                    "时间": {"type": "string"}
                }
            },
            "合同": {
                "description": "当事人之间的合同关系"
            }
        }
    }

    # examples 示例
    examples = [
        {
            "text": "原告张三诉被告李四合同纠纷一案，依据《中华人民共和国合同法》第60条",
            "extractions": [
                {
                    "name": "张三",
                    "type": "当事人",
                    "attributes": {"role": "原告"}
                },
                {
                    "name": "李四",
                    "type": "当事人",
                    "attributes": {"role": "被告"}
                },
                {
                    "name": "合同纠纷",
                    "type": "案件类型",
                    "attributes": {}
                },
                {
                    "name": "中华人民共和国合同法",
                    "type": "法律条款",
                    "attributes": {"条款号": "第60条"}
                }
            ]
        }
    ]

    # input_text 示例
    input_text = long_text
#     input_text = """
# 北京市朝阳区人民法院民事判决书
# 案件编号：(2023)京0105民初12345号
# 原告：北京科技创新有限公司，住所地北京市海淀区中关村大街1号，法定代表人李明，总经理。
# 被告：上海贸易发展有限公司，住所地上海市浦东新区陆家嘴环路1000号，法定代表人王华，董事长。
# 第三人：深圳市金融服务有限公司，住所地深圳市南山区科技园南区，法定代表人张伟，总经理。
#
# 审理经过：
# 原告北京科技创新有限公司与被告上海贸易发展有限公司、第三人深圳市金融服务有限公司买卖合同纠纷一案，
# 本院于2023年3月15日立案受理后，依法适用普通程序，公开开庭进行了审理。
# 原告北京科技创新有限公司的委托诉讼代理人赵律师，被告上海贸易发展有限公司的法定代表人王华及其委托诉讼代理人钱律师，
# 第三人深圳市金融服务有限公司的法定代表人张伟到庭参加诉讼。本案现已审理终结。
#
# 案件事实：
# 2022年8月10日，原告北京科技创新有限公司与被告上海贸易发展有限公司签订《技术设备买卖合同》，
# 合同编号为BJKJ-SHMY-20220810，约定由原告向被告出售智能数据处理设备10套，每套价格100万元，总金额1000万元。
# 合同约定交货时间为2022年10月30日，付款方式为合同签订后支付30%定金，货物验收合格后支付剩余款项。
#
# 2022年9月15日，原告按照合同约定将10套智能数据处理设备运至被告指定的上海市浦东新区仓库，并由双方共同验收合格。
# 被告在验收单上签字确认，设备运行正常，符合合同要求。
#
# 2022年10月20日，被告向原告支付了300万元定金，但剩余700万元货款一直未支付。
# 原告多次催要未果，于2023年1月10日向被告发出催款通知书，要求其在2023年1月31日前支付剩余货款及逾期利息。
#
# 2023年2月5日，第三人深圳市金融服务有限公司与原告签订《债权转让协议》，
# 原告将其对被告享有的700万元货款债权转让给第三人，并已通知被告。
#
# 争议焦点：
# 1. 被告是否应当支付剩余700万元货款及逾期利息
# 2. 债权转让的法律效力问题
# 3. 合同履行过程中是否存在违约情形
#
# 法院认为：
# 根据《中华人民共和国民法典》第五百七十七条、第五百七十九条规定，
# 当事人一方不支付价款或者报酬的，对方可以要求其支付价款或者报酬。
# 本案中，原告已按合同约定履行了交付设备的义务，被告应当按约定支付货款。
#
# 根据《中华人民共和国民法典》第五百四十七条规定，
# 债权人转让债权的，受让人取得与债权有关的从权利。
# 本案中，原告与第三人之间的债权转让协议合法有效，第三人有权向被告主张债权。
#
# 判决结果：
# 一、被告上海贸易发展有限公司于本判决生效之日起十日内向第三人深圳市金融服务有限公司支付货款700万元；
# 二、被告上海贸易发展有限公司于本判决生效之日起十日内向第三人深圳市金融服务有限公司支付逾期利息
# （以700万元为基数，自2022年11月1日起至实际付清之日止，按同期全国银行间同业拆借中心公布的贷款市场报价利率计算）；
# 三、驳回原告北京科技创新有限公司的其他诉讼请求。
#
# 案件受理费60800元，由被告上海贸易发展有限公司负担。
#     """

    extractor = LangextractAdapter(
            config=LangextractConfig(
                model_name="qwen-long",
                api_key="sk-742c7c766efd4426bd60a269259aafaf",
                api_url="https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions",
                config={
                    "timeout": 300
                },
                max_char_buffer=5000,
                batch_length=5,
                max_workers=3,
            )
        )

    result = await extractor.entity_and_relationship_extract(
        user_prompt=prompt,
        schema=schema,
        input_text=input_text,
        examples=examples,
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


def change_shchema_to_md():
    # 获取当前文件所在目录
    current_dir = os.path.dirname(os.path.abspath(__file__))
    output_file = os.path.join(current_dir, "law_instances/test_results.txt")

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


if __name__ == "__main__":
    # asyncio.run(extract_list_of_dict_test())
    change_shchema_to_md()
