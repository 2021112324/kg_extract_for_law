import re
from difflib import SequenceMatcher


def clean_and_calculate_similarity(str1: str, str2: str) -> float:
    """
    清洗字符串并使用SequenceMatcher计算两个字符串的相似度

    Args:
        str1: 第一个字符串
        str2: 第二个字符串

    Returns:
        float: 相似度值 [0.0, 1.0]，1.0表示完全相同
    """

    def clean_string(text: str) -> str:
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

    # 1. 清洗字符串
    cleaned_str1 = clean_string(str1)
    cleaned_str2 = clean_string(str2)

    # 2. 使用SequenceMatcher计算相似度
    similarity = SequenceMatcher(None, cleaned_str1, cleaned_str2).ratio()

    return similarity