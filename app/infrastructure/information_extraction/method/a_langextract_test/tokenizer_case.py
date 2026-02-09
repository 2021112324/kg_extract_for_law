"""
tokenize 函数使用示例

此文件演示了 tokenizer.py 中 tokenize 函数的多种使用场景，
包括英文文本、中文文本、混合文本、带标点符号的文本等。
"""

from app.infrastructure.information_extraction.method.langextract import tokenizer


def print_tokenized_result(text: str, tokenized_result: tokenizer.TokenizedText):
    """打印分词结果的辅助函数"""
    print(f"原始文本: {text}")
    print(f"总共有 {len(tokenized_result.tokens)} 个token:")

    for token in tokenized_result.tokens:
        token_text = text[token.char_interval.start_pos:token.char_interval.end_pos]
        token_type_name = tokenizer.TokenType(token.token_type).name
        print(f"  Token {token.index}: '{token_text}' "
              f"[{token.char_interval.start_pos}:{token.char_interval.end_pos}] "
              f"类型: {token_type_name}, "
              f"换行后首token: {token.first_token_after_newline}")
    print()


def example_1_simple_english():
    """示例1: 简单英文文本"""
    print("=== 示例1: 简单英文文本 ===")
    text = "Hello world! This is a simple sentence."
    result = tokenizer.tokenize(text)
    print_tokenized_result(text, result)


def example_2_simple_chinese():
    """示例2: 简单中文文本"""
    print("=== 示例2: 简单中文文本 ===")
    text = "你好世界！这是一个简单的句子。"
    result = tokenizer.tokenize(text)
    print_tokenized_result(text, result)


def example_3_mixed_language():
    """示例3: 中英文混合文本"""
    print("=== 示例3: 中英文混合文本 ===")
    text = "Hello 你好 world 世界 123!"
    result = tokenizer.tokenize(text)
    print_tokenized_result(text, result)


def example_4_with_numbers_and_symbols():
    """示例4: 包含数字和符号的文本"""
    print("=== 示例4: 包含数字和符号的文本 ===")
    text = "The price is $19.99, VAT 13%, discount 10%. Contact: email@example.com"
    result = tokenizer.tokenize(text)
    print_tokenized_result(text, result)


def example_5_with_acronyms():
    """示例5: 包含缩写的文本"""
    print("=== 示例5: 包含缩写的文本 ===")
    text = "The U.S.A. and UK have agreements. Dr. Smith works at MIT."
    result = tokenizer.tokenize(text)
    print_tokenized_result(text, result)


def example_6_with_newlines():
    """示例6: 包含换行符的文本"""
    print("=== 示例6: 包含换行符的文本 ===")
    text = "First line.\nSecond line starts with capital.\nThird line here."
    result = tokenizer.tokenize(text)
    print_tokenized_result(text, result)


def example_7_chinese_with_punctuation():
    """示例7: 中文带标点符号"""
    print("=== 示例7: 中文带标点符号 ===")
    text = "法律第12条规定：「禁止」任何单位和个人从事违法行为！"
    result = tokenizer.tokenize(text)
    print_tokenized_result(text, result)


def example_8_complex_legal_text():
    """示例8: 复杂的法律文本（中英文）"""
    print("=== 示例8: 复杂的法律文本 ===")
    text = """
    Article 3: The parties agree that "confidential information" means any and all 
    non-public information, whether written or oral, including but not limited to 
    trade secrets, business plans, financial information, etc.

    第三条规定：当事人同意"保密信息"是指任何非公开信息，
    无论是书面还是口头形式，包括但不限于商业秘密、商业计划、财务信息等。
    """
    result = tokenizer.tokenize(text)
    print_tokenized_result(text, result)


def example_9_special_cases():
    """示例9: 特殊情况处理"""
    print("=== 示例9: 特殊情况处理 ===")
    text = "Email: user@domain.com, Phone: +1-234-567-8900, URL: https://example.com/path"
    result = tokenizer.tokenize(text)
    print_tokenized_result(text, result)


def example_10_empty_and_edge_cases():
    """示例10: 边界情况"""
    print("=== 示例10: 边界情况 ===")
    print("空字符串:")
    result = tokenizer.tokenize("")
    print_tokenized_result("", result)

    print("只有标点符号:")
    text = "!@#$%^&*()"
    result = tokenizer.tokenize(text)
    print_tokenized_result(text, result)

    print("只有数字:")
    text = "123456789"
    result = tokenizer.tokenize(text)
    print_tokenized_result(text, result)

    print("只有空格:")
    text = "   \t   \n   "
    result = tokenizer.tokenize(text)
    print_tokenized_result(text, result)


if __name__ == "__main__":
    print("开始演示 tokenize 函数的各种使用示例\n")

    example_1_simple_english()
    example_2_simple_chinese()
    example_3_mixed_language()
    example_4_with_numbers_and_symbols()
    example_5_with_acronyms()
    example_6_with_newlines()
    example_7_chinese_with_punctuation()
    example_8_complex_legal_text()
    example_9_special_cases()
    example_10_empty_and_edge_cases()

    print("所有示例演示完成！")