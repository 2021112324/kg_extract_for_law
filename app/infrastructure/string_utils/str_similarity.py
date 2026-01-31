import re
from difflib import SequenceMatcher

from modelscope.preprocessors.nlp.space.tokenizer import clean_string


def clean_and_calculate_similarity(str1: str, str2: str) -> float:
    """
    清洗字符串并使用SequenceMatcher计算两个字符串的相似度

    Args:
        str1: 第一个字符串
        str2: 第二个字符串

    Returns:
        float: 相似度值 [0.0, 1.0]，1.0表示完全相同
    """



    # 1. 清洗字符串
    cleaned_str1 = clean_string(str1)
    cleaned_str2 = clean_string(str2)

    # 2. 使用SequenceMatcher计算相似度
    similarity = SequenceMatcher(None, cleaned_str1, cleaned_str2).ratio()

    return similarity