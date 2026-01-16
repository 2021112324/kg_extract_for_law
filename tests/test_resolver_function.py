"""
测试 _extract_and_parse_content 函数
基于提供的示例数据和目标函数
"""

import json
import yaml
import logging
from unittest.mock import Mock
import sys
import os

# 添加项目路径到系统路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

# 导入必要的模块
from app.infrastructure.information_extraction.method.langextract import data
from json_repair import repair_json


class TestableResolver:
    """用于测试的解析器类，实现了部分原始类的功能"""
    
    def __init__(self, format_type=data.FormatType.JSON, fence_output=True):
        self.format_type = format_type
        self.fence_output = fence_output
    
    def _extract_and_parse_content(
        self,
        input_string: str,
    ):
        """修改版的_extract_and_parse_content函数用于测试"""
        from collections.abc import Mapping, Sequence
        ExtractionValueType = str | int | float | dict | list | None
        
        logging.info("Starting string parsing.")
        logging.debug("input_string: %s", input_string)

        if not input_string or not isinstance(input_string, str):
            logging.error("Input string must be a non-empty string.")
            raise ValueError("Input string must be a non-empty string.")

        if self.fence_output:
            left_key = "```" + self.format_type.value
            left = input_string.find(left_key)
            right = input_string.rfind("```")
            prefix_length = len(left_key)
            if left == -1 or right == -1 or left >= right:
                logging.error("Input string does not contain valid markers.")
                raise ValueError("Input string does not contain valid markers.")

            content = input_string[left + prefix_length : right].strip()
            logging.debug("Content: %s", content)
        else:
            content = input_string

        try:
            if self.format_type == data.FormatType.YAML:
                parsed_data = yaml.safe_load(content)
            else:
                parsed_data = json.loads(content)
            logging.debug("Successfully parsed content.")
        except (yaml.YAMLError, json.JSONDecodeError) as e:
            logging.exception("Failed to parse content.")
            if isinstance(e, json.JSONDecodeError):
                logging.error("JSON decode error at line %d column %d: %s", e.lineno, e.colno, e.msg)
                logging.error("Error position: %d", e.pos)
                logging.error("Error context: %s", repr(content[max(0, e.pos-30):e.pos+30]))
                # 输出完整内容的前200个字符，便于调试
                logging.error("Full content (first 200 chars): %s", repr(content[:200]))
                
                # 如果是JSON格式且json_repair可用，则尝试修复
                if self.format_type == data.FormatType.JSON:
                    logging.info("Attempting to repair JSON content...")
                    try:
                        repaired_content = repair_json(content)
                        parsed_data = json.loads(repaired_content)
                        logging.info("Successfully parsed repaired JSON content.")
                        return parsed_data
                    except Exception as repair_error:
                        logging.error("Failed to repair and parse JSON: %s", str(repair_error))
                
                # 检查特殊字符
                for i, char in enumerate(content):
                    if ord(char) < 32 and char not in ['\n', '\r', '\t']:
                        logging.error("Control character found at position %d: %r (ord=%d)", i, char, ord(char))
            elif isinstance(e, yaml.YAMLError):
                logging.error("YAML error: %s", str(e))
            logging.error("Full content that failed to parse (repr): %s", repr(content))
            logging.error("Full content that failed to parse (raw): \n%s", content)
            
            # 为了测试目的，我们不会抛出异常，而是返回None
            # raise ResolverParsingError("Failed to parse content.") from e
            return None

        return parsed_data


def run_tests():
    """运行测试用例"""
    print("=== _extract_and_parse_content 函数测试 ===")
    print("=" * 60)

    # 测试用例，基于json_repair_test.py中的用例
    test_cases = [
        # 测试1: 未加引号的键
        {
            "name": "unquoted_keys",
            "input": '```json\n{name: "John", age: 30}\n```',
            "description": "测试未加引号的对象键名",
            "expected_success": True
        },

        # 测试2: 单引号
        {
            "name": "single_quotes",
            "input": "```json\n{'name': 'John', 'age': 30}\n```",
            "description": "测试单引号字符串",
            "expected_success": True
        },

        # 测试3: 末尾逗号
        {
            "name": "trailing_comma",
            "input": '```json\n{"name": "John", "age": 30,}\n```',
            "description": "测试末尾逗号",
            "expected_success": True
        },

        # 测试4: 不平衡括号
        {
            "name": "unbalanced_braces",
            "input": '```json\n{"name": "John", "hobbies": ["reading", "swimming"]\n```',
            "description": "测试不平衡的括号",
            "expected_success": True
        },

        # 测试5: 混合问题
        {
            "name": "mixed_issues",
            "input": "```json\n{name: 'John', hobbies: [reading, swimming], 'age': 30,}\n```",
            "description": "测试多种混合问题",
            "expected_success": True
        },

        # 测试6: 已经正确的JSON
        {
            "name": "valid_json",
            "input": '```json\n{"name": "John", "age": 30}\n```',
            "description": "测试已经有效的JSON",
            "expected_success": True
        },

        # 测试7: 不带围栏的JSON
        {
            "name": "no_fence_valid_json",
            "input": '{"name": "Alice", "age": 25}',
            "description": "测试不带围栏的有效JSON",
            "fence_output": False,
            "expected_success": True
        },

        # 测试8: 不带围栏的无效JSON
        {
            "name": "no_fence_invalid_json",
            "input": '{name: "Bob", age: 35}',
            "description": "测试不带围栏的无效JSON",
            "fence_output": False,
            "expected_success": True  # 因为有json_repair
        }
    ]

    for test_case in test_cases:
        print(f"\n测试: {test_case['description']}")
        print(f"输入: {test_case.get('input', 'N/A')}")

        # 创建解析器实例
        fence_output = test_case.get('fence_output', True)
        resolver = TestableResolver(format_type=data.FormatType.JSON, fence_output=fence_output)

        try:
            result = resolver._extract_and_parse_content(test_case.get('input', ''))
            if result is not None:
                print(f"  ✓ 解析成功: {result}")
                if test_case.get('expected_success'):
                    print("  ✓ 结果符合预期")
                else:
                    print("  ⚠ 意外成功（期望失败）")
            else:
                print("  ✗ 解析失败")
                if test_case.get('expected_success'):
                    print("  ✗ 结果不符合预期（期望成功）")
                else:
                    print("  ✓ 结果符合预期（期望失败）")
        except Exception as e:
            print(f"  ✗ 发生异常: {e}")
            if test_case.get('expected_success'):
                print("  ✗ 结果不符合预期（期望成功）")
            else:
                print("  ✓ 结果符合预期（期望失败）")

    # YAML测试用例
    print("\n" + "=" * 60)
    print("=== YAML解析测试 ===")
    
    yaml_test_cases = [
        {
            "name": "valid_yaml",
            "input": '```yaml\nname: John\nage: 30\n```',
            "description": "测试有效的YAML",
            "expected_success": True
        },
        {
            "name": "invalid_yaml",
            "input": '```yaml\nname: John\n  age: 30\n```',  # 缩进错误
            "description": "测试无效的YAML",
            "expected_success": False
        }
    ]
    
    for test_case in yaml_test_cases:
        print(f"\n测试: {test_case['description']}")
        print(f"输入: {test_case.get('input', 'N/A')}")

        # 创建YAML解析器实例
        resolver = TestableResolver(format_type=data.FormatType.YAML, fence_output=True)

        try:
            result = resolver._extract_and_parse_content(test_case.get('input', ''))
            if result is not None:
                print(f"  ✓ 解析成功: {result}")
                if test_case.get('expected_success'):
                    print("  ✓ 结果符合预期")
                else:
                    print("  ⚠ 意外成功（期望失败）")
            else:
                print("  ✗ 解析失败")
                if test_case.get('expected_success'):
                    print("  ✗ 结果不符合预期（期望成功）")
                else:
                    print("  ✓ 结果符合预期（期望失败）")
        except Exception as e:
            print(f"  ✗ 发生异常: {e}")
            if test_case.get('expected_success'):
                print("  ✗ 结果不符合预期（期望成功）")
            else:
                print("  ✓ 结果符合预期（期望失败）")

    print("\n" + "=" * 60)
    print("测试完成")


def edge_cases_test():
    """测试边界情况"""
    print("\n=== 边界情况测试 ===")
    print("-" * 40)

    edge_cases = [
        # 空字符串
        {
            "name": "empty_string",
            "input": "",
            "description": "空字符串"
        },
        # 只有围栏
        {
            "name": "only_fence",
            "input": "```json\n```",
            "description": "只有围栏没有内容"
        },
        # 无效围栏
        {
            "name": "invalid_fence",
            "input": "```xml\n{\"name\": \"test\"}\n```",
            "description": "无效的围栏类型"
        },
        # 多层嵌套修复
        {
            "name": "nested_repair",
            "input": "```json\n{name: {first: 'John', last: 'Doe'}, contacts: [{email: 'john@example.com'}]}\n```",
            "description": "多层嵌套需要修复"
        }
    ]

    resolver = TestableResolver(format_type=data.FormatType.JSON, fence_output=True)

    for i, case in enumerate(edge_cases):
        print(f"\n边界测试 {i+1}: {case['description']}")
        print(f"输入: {repr(case['input'])}")
        
        try:
            result = resolver._extract_and_parse_content(case['input'])
            if result is not None:
                print(f"  ✓ 解析结果: {result}")
            else:
                print("  ✗ 解析失败")
        except Exception as e:
            print(f"  ✗ 处理失败: {e}")


if __name__ == "__main__":
    # 设置日志级别为INFO以查看详细输出
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    
    run_tests()
    edge_cases_test()