# Copyright 2025 Google LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
文本的分词工具。
提供方法将文本拆分为基于正则表达式的词级别（和标点符号级别）的标记。
(1) 分词对于在resolver.py中对齐提取的数据和源文本是必要的。
(2) 分词也用于为较小上下文用例形成句子边界，用于LLM信息提取。
    随着更新更大的上下文LLM的出现，这些较小上下文用例变得不太必要。此模块不在语言模型推理过程中表示标记。
"""

# 导入必要的模块
from collections.abc import Sequence, Set  # 导入抽象基类用于序列和集合
import dataclasses  # 导入数据类模块，用于创建数据容器类
import enum  # 导入枚举模块，用于定义枚举类型
import re  # 导入正则表达式模块，用于文本匹配

from absl import logging  # 导入Google的ABSL日志库

from . import exceptions  # 导入本地异常模块

# 定义基础异常类
class BaseTokenizerError(exceptions.LangExtractError):
    """所有分词相关错误的基类。"""

class InvalidTokenIntervalError(BaseTokenizerError):
    """当标记间隔无效或超出范围时引发的错误。"""

class SentenceRangeError(BaseTokenizerError):
    """当句子的起始标记索引超出范围时引发的错误。"""

# 定义字符区间数据类
@dataclasses.dataclass
class CharInterval:
    """表示原始文本中的字符位置范围。

    属性:
      start_pos: 起始字符索引（包含）。
      end_pos: 结束字符索引（不包含）。
    """

    start_pos: int  # 起始字符位置
    end_pos: int  # 结束字符位置

# 定义标记区间数据类
@dataclasses.dataclass
class TokenInterval:
    """表示分词文本中标记的区间。

    区间由起始索引（包含）和结束索引（不包含）定义。

    属性:
      start_index: 区间中第一个标记的索引。
      end_index: 区间结束后一个位置的索引。
    """

    start_index: int = 0  # 起始索引，默认为0
    end_index: int = 0  # 结束索引，默认为0

# 定义标记类型枚举
class TokenType(enum.IntEnum):
    """分词过程中产生的标记类型枚举。

    属性:
      WORD: 表示字母单词标记。
      NUMBER: 表示数字标记。
      PUNCTUATION: 表示标点符号。
      ACRONYM: 表示缩写或斜杠分隔的缩写。
    """

    WORD = 0  # 单词类型
    NUMBER = 1  # 数字类型
    PUNCTUATION = 2  # 标点符号类型
    ACRONYM = 3  # 缩写类型

# 定义标记数据类
@dataclasses.dataclass
class Token:
    """
    表示从文本中提取的标记。

    每个标记都被分配一个索引并分类为一种类型（单词、数字、标点符号或缩写）。
    标记还记录了与其对应的字符范围（其CharInterval），即原始文本中的子字符串。
    此外，它跟踪它是否跟在换行符之后。

    属性:
      index: 标记在标记序列中的位置。
      token_type: 标记的类型，由TokenType定义。
      char_interval: 此标记跨越的原始文本中的字符区间。
      first_token_after_newline: 如果标记紧跟在换行符或回车符之后，则为True。
    """

    index: int  # 标记在序列中的索引
    token_type: TokenType  # 标记类型
    char_interval: CharInterval = dataclasses.field(  # 字符区间，默认工厂函数创建(0,0)
        default_factory=lambda: CharInterval(0, 0)
    )
    first_token_after_newline: bool = False  # 是否是换行后的第一个标记

# 定义分词文本数据类
@dataclasses.dataclass
class TokenizedText:
    """
    保存文本分词结果。

    属性:
      text: 被分词的原始文本。
      tokens: 从文本中提取的标记对象列表。
    """

    text: str  # 原始文本
    tokens: list[Token] = dataclasses.field(default_factory=list)  # 标记列表，默认为空

# 分词用的正则表达式模式
_LETTERS_PATTERN = r"[A-Za-z]+"  # 匹配一个或多个字母
_DIGITS_PATTERN = r"[0-9]+"  # 匹配一个或多个数字
_SYMBOLS_PATTERN = r"[^A-Za-z0-9\s]+"  # 匹配非字母数字和空白字符
_END_OF_SENTENCE_PATTERN = re.compile(r"[.?!]$")  # 匹配句末标点
_SLASH_ABBREV_PATTERN = r"[A-Za-z0-9]+(?:/[A-Za-z0-9]+)+"  # 匹配斜杠分隔的缩写

# 组合标记模式
_TOKEN_PATTERN = re.compile(
    rf"{_SLASH_ABBREV_PATTERN}|{_LETTERS_PATTERN}|{_DIGITS_PATTERN}|{_SYMBOLS_PATTERN}"  # 组合所有标记模式
)
_WORD_PATTERN = re.compile(rf"(?:{_LETTERS_PATTERN}|{_DIGITS_PATTERN})\Z")  # 匹配字母数字结尾

# 已知的不应视为句末的缩写
# TODO: 鉴于大多数用例是更大上下文，这可能会被移除
_KNOWN_ABBREVIATIONS = frozenset({"Mr.", "Mrs.", "Ms.", "Dr.", "Prof.", "St."})

# 分词函数
def tokenize(text: str) -> TokenizedText:
    """
    将文本拆分为标记（单词、数字或标点符号）。

    每个标记都用其字符位置和类型（单词或标点符号）进行注释。
    如果在标记之前的间隙中有换行符或回车符，则该标记的`first_token_after_newline`设置为True。

    参数:
      text: 要分词的文本。

    返回:
      包含所有提取标记的TokenizedText对象。
    """
    logging.debug("进入tokenize()，文本:\n%r", text)  # 记录调试日志
    tokenized = TokenizedText(text=text)  # 创建分词文本对象
    previous_end = 0  # 记录上一个标记的结束位置
    for token_index, match in enumerate(_TOKEN_PATTERN.finditer(text)):  # 遍历文本中的所有匹配
        start_pos, end_pos = match.span()  # 获取匹配的起始和结束位置
        matched_text = match.group()  # 获取匹配的文本内容
        # 创建新标记
        token = Token(
            index=token_index,  # 标记索引
            char_interval=CharInterval(start_pos=start_pos, end_pos=end_pos),  # 字符区间
            token_type=TokenType.WORD,  # 默认类型为单词
            first_token_after_newline=False,  # 默认不是换行后的第一个标记
        )
        # 检查此标记前是否有换行符
        if token_index > 0:  # 如果不是第一个标记
            gap = text[previous_end:start_pos]  # 获取标记间的间隙文本
            if "\n" in gap or "\r" in gap:  # 如果间隙中有换行符或回车符
                token.first_token_after_newline = True  # 设置为换行后的第一个标记
        # 分类标记类型
        if re.fullmatch(_DIGITS_PATTERN, matched_text):  # 如果完全匹配数字模式
            token.token_type = TokenType.NUMBER  # 设置为数字类型
        elif re.fullmatch(_SLASH_ABBREV_PATTERN, matched_text):  # 如果完全匹配斜杠缩写模式
            token.token_type = TokenType.ACRONYM  # 设置为缩写类型
        elif _WORD_PATTERN.fullmatch(matched_text):  # 如果完全匹配单词模式
            token.token_type = TokenType.WORD  # 设置为单词类型
        else:  # 否则
            token.token_type = TokenType.PUNCTUATION  # 设置为标点符号类型
        tokenized.tokens.append(token)  # 将标记添加到列表中
        previous_end = end_pos  # 更新上一个标记的结束位置
    logging.debug("完成tokenize()。标记总数: %d", len(tokenized.tokens))  # 记录调试日志
    return tokenized  # 返回分词结果

# 获取标记区间对应文本的函数
def tokens_text(
    tokenized_text: TokenizedText,
    token_interval: TokenInterval,
) -> str:
    """
    重建跨越给定标记区间的原始文本的子字符串。

    参数:
      tokenized_text: 包含标记数据的TokenizedText对象。
      token_interval: 指定标记范围[start_index, end_index)的区间。

    返回:
      与标记区间对应的原始文本的确切子字符串。

    引发:
      InvalidTokenIntervalError: 如果标记区间无效或超出范围。
    """
    if (  # 检查区间是否有效
        token_interval.start_index < 0  # 起始索引小于0
        or token_interval.end_index > len(tokenized_text.tokens)  # 结束索引超出范围
        or token_interval.start_index >= token_interval.end_index  # 起始索引大于等于结束索引
    ):

        raise InvalidTokenIntervalError(  # 抛出无效区间异常
            f"无效的标记区间。start_index={token_interval.start_index}, "  # 错误信息
            f"end_index={token_interval.end_index}, "
            f"总标记数={len(tokenized_text.tokens)}。"
        )

    start_token = tokenized_text.tokens[token_interval.start_index]  # 获取起始标记
    end_token = tokenized_text.tokens[token_interval.end_index - 1]  # 获取结束标记
    return tokenized_text.text[  # 返回对应文本
        start_token.char_interval.start_pos : end_token.char_interval.end_pos  # 使用字符区间切片
    ]

# 检查是否为句末标记的辅助函数
def _is_end_of_sentence_token(
    text: str,
    tokens: Sequence[Token],
    current_idx: int,
    known_abbreviations: Set[str] = _KNOWN_ABBREVIATIONS,
) -> bool:
    """检查current_idx处的标点符号标记是否结束一个句子。

    标记被视为句子终结符且不是已知缩写的一部分。只搜索与当前标记对应的文本。

    参数:
      text: 整个输入文本。
      tokens: 标记对象序列。
      current_idx: 要检查的当前标记索引。
      known_abbreviations: 不应视为句末的缩写（例如"Dr."）。

    返回:
      如果current_idx处的标记结束一个句子，则为True，否则为False。
    """
    current_token_text = text[  # 获取当前标记的文本内容
        tokens[current_idx]  # 通过索引获取标记
        .char_interval.start_pos : tokens[current_idx]  # 获取字符区间的起始位置
        .char_interval.end_pos  # 获取字符区间的结束位置
    ]
    if _END_OF_SENTENCE_PATTERN.search(current_token_text):  # 如果当前标记文本匹配句末标点模式
        if current_idx > 0:  # 如果不是第一个标记
            prev_token_text = text[  # 获取前一个标记的文本
                tokens[current_idx - 1]  # 获取前一个标记
                .char_interval.start_pos : tokens[current_idx - 1]  # 获取字符区间的起始位置
                .char_interval.end_pos  # 获取字符区间的结束位置
            ]
            if f"{prev_token_text}{current_token_text}" in known_abbreviations:  # 如果组合文本在已知缩写中
                return False  # 不是句末
        return True  # 是句末
    return False  # 不是句末

# 检查换行后是否为句子开头的辅助函数
def _is_sentence_break_after_newline(
    text: str,
    tokens: Sequence[Token],
    current_idx: int,
) -> bool:
    """检查下一个标记前是否有换行符，且该标记是否以大写字母开头。

    这是确定句子边界的启发式方法。它倾向于过早终止句子而不是错过句子边界，
    如果第一行以换行符结尾且第二行以大写字母开头，它将过早终止句子。

    参数:
      text: 整个输入文本。
      tokens: 标记对象序列。
      current_idx: 当前标记索引。

    返回:
      如果在current_idx和current_idx+1之间找到换行符，且下一个标记（如果有）以大写字母开头，则为True。
    """
    if current_idx + 1 >= len(tokens):  # 如果下一个标记超出范围
        return False  # 返回False

    gap_text = text[  # 获取两个标记之间的文本
        tokens[current_idx]  # 当前标记
        .char_interval.end_pos : tokens[current_idx + 1]  # 当前标记结束位置到下一标记开始位置
        .char_interval.start_pos
    ]
    if "\n" not in gap_text:  # 如果间隙文本中没有换行符
        return False  # 返回False

    next_token_text = text[  # 获取下一个标记的文本
        tokens[current_idx + 1]  # 下一个标记
        .char_interval.start_pos : tokens[current_idx + 1]  # 获取字符区间
        .char_interval.end_pos
    ]
    return bool(next_token_text) and next_token_text[0].isupper()  # 返回是否非空且首字符大写

# 查找句子范围的函数
def find_sentence_range(
    text: str,
    tokens: Sequence[Token],
    start_token_index: int,
) -> TokenInterval:
    """
    从给定的起始索引查找"句子"区间。

    句子边界由以下定义：
      - _END_OF_SENTENCE_PATTERN中的标点符号标记
      - 跟随大写字母的换行符
      - 不是_KNOWN_ABBREVIATIONS中的缩写（例如"Dr."）

    这倾向于过早终止句子而不是错过句子边界，如果第一行以换行符结尾且第二行以大写字母开头，它将过早终止句子。

    参数:
      text: 原始文本。
      tokens: 构成`text`的标记。
      start_token_index: 开始句子的标记索引。

    返回:
      表示句子范围[start_token_index, end)的TokenInterval。如果找不到句子边界，
      结束索引将是`tokens`的长度。

    引发:
      SentenceRangeError: 如果`start_token_index`超出范围。
    """
    if start_token_index < 0 or start_token_index >= len(tokens):  # 检查起始索引是否有效
        raise SentenceRangeError(  # 抛出范围错误
            f"start_token_index={start_token_index} 超出范围。 "  # 错误信息
            f"总标记数: {len(tokens)}。"
        )

    i = start_token_index  # 从起始索引开始
    while i < len(tokens):  # 遍历标记
        if tokens[i].token_type == TokenType.PUNCTUATION:  # 如果是标点符号
            if _is_end_of_sentence_token(text, tokens, i, _KNOWN_ABBREVIATIONS):  # 检查是否为句末
                return TokenInterval(start_index=start_token_index, end_index=i + 1)  # 返回区间
        if _is_sentence_break_after_newline(text, tokens, i):  # 检查换行后是否为句子开头
            return TokenInterval(start_index=start_token_index, end_index=i + 1)  # 返回区间
        i += 1  # 移动到下一个标记

    return TokenInterval(start_index=start_token_index, end_index=len(tokens))  # 返回完整区间
