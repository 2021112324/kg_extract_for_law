import re

def replace_full_corner_space(text: str) -> str:
    """
    替换全角空格为半角空格

    Args:
        text: 输入文本

    Returns:
        str: 替换后的文本
    """
    return text.replace('\u3000', ' ')


def replace_zero_width_chars(text: str) -> str:
    """
    替换零宽度字符

    Args:
        text: 输入文本

    Returns:
        str: 替换后的文本
    """
    zero_width_chars = [
        '\u200B',  # ZERO WIDTH SPACE
        '\u200C',  # ZERO WIDTH NON-JOINER
        '\u200D',  # ZERO WIDTH JOINER
        '\uFEFF',  # ZERO WIDTH NO-BREAK SPACE (Byte Order Mark)
    ]

    for char in zero_width_chars:
        text = text.replace(char, '')
    return text


def clean_string_without_cn_punc(text: str) -> str:
    """
    清洗字符串：移除中文标点符号

    Args:
        text: 输入文本

    Returns:
        str: 清洗后的文本
    """
    # 定义中文和英文标点符号的正则表达式
    punctuation_pattern = r'[^\w\s\u4e00-\u9fff]'
    return re.sub(punctuation_pattern, '', text)


def clean_string_with_only_words(text: str) -> str:
    """
    清洗字符串：只保留中文字符和英文单词

    Args:
        text: 输入文本

    Returns:
        str: 清洗后的文本
    """
    return re.sub(r'[^\u4e00-\u9fff\w\s]', '', text)


def clean_string_for_neo4j(text: str) -> str:
    """
    清洗字符串，去除可能影响Neo4j存储的特殊字符

    Args:
        text: 输入文本

    Returns:
        str: 清洗后的文本
    """
    # 移除控制字符（ASCII 0-31，除了制表符、换行符和回车符）
    # Neo4j通常不允许存储这些不可见的控制字符
    cleaned = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', text)

    # 移除Unicode控制字符
    cleaned = re.sub(r'[\u0080-\u009F]', '', cleaned)

    # 替换或移除可能导致Neo4j查询问题的特殊字符
    # 如反斜杠、引号等（根据实际需求调整）
    cleaned = cleaned.replace('\\', '')  # 移除反斜杠，避免转义问题
    cleaned = cleaned.replace('"', "'")  # 将双引号替换为单引号
    cleaned = cleaned.replace("'", "\\'")  # 转义单引号

    # 处理换行符和制表符（可选：替换为普通空格）
    cleaned = re.sub(r'\r\n|\r|\n', ' ', cleaned)  # 将换行符替换为空格
    cleaned = cleaned.replace('\t', ' ')  # 将制表符替换为空格

    return cleaned


def clean_string_for_neo4j_extended(text: str) -> str:
    """
    扩展版清洗字符串，去除更多可能影响Neo4j存储的特殊字符

    Args:
        text: 输入文本

    Returns:
        str: 清洗后的文本
    """
    # 首先应用基本的清洗
    cleaned = clean_string_for_neo4j(text)

    # 移除零宽度字符
    cleaned = re.sub(r'[\u200B-\u200D\uFEFF]', '', cleaned)

    # 处理其他可能引起问题的Unicode字符
    # 如代理对区域（surrogate pairs）
    cleaned = re.sub(r'[\uD800-\uDFFF]', '', cleaned)

    # 移除多余的空白字符
    cleaned = re.sub(r'\s+', ' ', cleaned)  # 将多个空白字符合并为单个空格

    return cleaned.strip()  # 移除首尾空白


if __name__ == '__main__':
    # 测试用例
    test_cases = [
        # 中文文本测试
        "你好，世界！",
        "中华人民共和国成立于1949年。",
        "今天天气不错，适合出门走走？",

        # 英文文本测试
        "Hello, World!",
        "This is a test string with punctuation: @#$%^&*()",
        "Python programming language is great!",

        # 中英混合测试
        "今天Today天气不错Nice",
        "I love 中国China!",
        "学习Study English and 中文Chinese",

        # 包含数字的测试
        "联系电话：123-456-7890",
        "价格：￥100元，折扣：50%",
        "版本号v1.2.3",

        # 特殊符号测试
        "测试@邮箱.com",
        "网址：https://www.example.com",
        "表情符号：😀😃😄😁",

        # 空字符串和纯标点
        "",
        "!@#$%^&*()",
        "，。、；：？！"
    ]

    print("测试 clean_string_with_only_words 函数:")
    for i, test_text in enumerate(test_cases, 1):
        cleaned = clean_string_with_only_words(test_text)
        print(f"测试 {i}:")
        print(f"  原文: {test_text}")
        print(f"  清洗: {cleaned}")
        print()