# 创建一个测试文件来演示如何使用json_repair_test.py中的测试方法来测试resolver.py中的_extract_and_parse_content函数
"""
测试 _extract_and_parse_content 函数的兼容性
"""

import json
from json_repair import repair_json
from app.infrastructure.information_extraction.method.langextract.resolver import Resolver
from app.infrastructure.information_extraction.method.langextract import data


def resolver_with_json_repair_test():
    """测试resolver的_extract_and_parse_content函数与json_repair的集成"""

    print("测试resolver中的_extract_and_parse_content函数")
    print("=" * 50)

    # 创建一个resolver实例
    resolver = Resolver(format_type=data.FormatType.JSON, fence_output=True)

    # 测试用例 - 与json_repair_test.py类似的测试场景
    test_cases = [
        # 测试1: 未加引号的键 (需要fenced格式)
        {
            "name": "unquoted_keys",
            "input": '```json{name: "John", age: 30}```',
            "description": "测试未加引号的对象键名（带fence）"
        },

        # 测试2: 单引号 (需要fenced格式)
        {
            "name": "single_quotes",
            "input": "```json{'name': 'John', 'age': 30}```",
            "description": "测试单引号字符串（带fence）"
        },

        # 测试3: 末尾逗号 (需要fenced格式)
        {
            "name": "trailing_comma",
            "input": '```json{"name": "John", "age": 30,}```',
            "description": "测试末尾逗号（带fence）"
        },

        # 测试4: 不平衡括号 (需要fenced格式)
        {
            "name": "unbalanced_braces",
            "input": '```json{"name": "John", "hobbies": ["reading", "swimming"]```',
            "description": "测试不平衡的括号（带fence）"
        },

        # 测试5: 混合问题 (需要fenced格式)
        {
            "name": "mixed_issues",
            "input": "```json{name: 'John', hobbies: [reading, swimming], 'age': 30,}```",
            "description": "测试多种混合问题（带fence）"
        },

        # 测试6: 已经正确的JSON (需要fenced格式)
        {
            "name": "valid_json",
            "input": '```json{"name": "John", "age": 30}```',
            "description": "测试已经有效的JSON（带fence）"
        }
    ]

    for test_case in test_cases:
        print(f"\n测试: {test_case['description']}")
        print(f"输入: {test_case['input']}")

        try:
            # 尝试直接使用resolver解析（可能失败）
            try:
                result = resolver._extract_and_parse_content(test_case['input'])
                print(f"  解析成功: {result}")
            except Exception as e:
                print(f"  解析失败: {e}")

                # 如果是JSON解析错误，尝试手动使用json_repair修复内容部分
                input_str = test_case['input']
                left_key = "```" + resolver.format_type.value
                left = input_str.find(left_key)
                right = input_str.rfind("```")

                if left != -1 and right != -1 and left < right:
                    prefix_length = len(left_key)
                    content = input_str[left + prefix_length:right].strip()

                    print(f"  原始内容: {content}")

                    # 尝试使用json_repair修复内容
                    try:
                        repaired_content = repair_json(content)
                        print(f"  修复后内容: {repaired_content}")

                        # 手动构建新的fenced内容
                        repaired_fenced = f"```{resolver.format_type.value}{repaired_content}```"
                        result = resolver._extract_and_parse_content(repaired_fenced)
                        print(f"  修复后解析成功: {result}")
                    except Exception as repair_error:
                        print(f"  修复失败: {repair_error}")
                else:
                    print("  无法找到有效标记")

        except Exception as e:
            print(f"  发生异常: {e}")


def edge_cases_with_resolver_test():
    """测试边界情况"""
    print("\n边界情况测试")
    print("-" * 30)

    resolver = Resolver(format_type=data.FormatType.JSON, fence_output=True)

    edge_cases = [
        # 空字符串
        "",
        # 只有括号
        "```json{```",
        "```json[```",
        # 多层嵌套修复
        "```json{name: {first: 'John', last: 'Doe'}, contacts: [{email: 'john@example.com'}]}```",
        # 包含特殊字符
        '```json{"message": "Hello\nWorld", "path": "C:\\Users\\Name"}```'
    ]

    for i, case in enumerate(edge_cases):
        print(f"\n边界测试 {i+1}: {repr(case)}")
        try:
            if case.strip():  # 非空字符串
                result = resolver._extract_and_parse_content(case)
                print(f"  解析结果: {result}")
            else:
                print("  空字符串，跳过解析")
        except Exception as e:
            print(f"  处理失败: {e}")


if __name__ == "__main__":
    resolver_with_json_repair_test()
    edge_cases_with_resolver_test()

    print("\n" + "=" * 50)
    print("测试完成")

