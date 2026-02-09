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
用于解析 LLM 输出的库。

在此模块的语境中，"resolver"（解析器）是一个专门设计用来解析并将 LLM 的文本输出转换为结构化数据的组件。
"""

import abc
import collections
from collections.abc import Iterator, Mapping, Sequence
import difflib
import functools
import itertools
import json
import operator

from absl import logging
import yaml

try:
    from json_repair import repair_json
except ImportError:
    repair_json = None
    logging.warning(
        "json_repair not available. Install it with 'pip install json-repair' to enable JSON repair functionality.")

from . import data
from . import exceptions
from . import schema
from . import tokenizer

_FUZZY_ALIGNMENT_MIN_THRESHOLD = 0.75


class AbstractResolver(abc.ABC):
    """
    将 LLM（大语言模型）的文本输出解析为结构化数据。
    """

    # TODO: Review value and requirements for abstract class.
    def __init__(
            self,
            fence_output: bool = True,
            constraint: schema.Constraint = schema.Constraint(),
            format_type: data.FormatType = data.FormatType.JSON,
    ):
        """
初始化 BaseResolver。

分隔符用于解析文本块，主要针对不支持约束解码的模型。

参数:
  fence_output: 是否期望/生成带边框的输出（```json or
        ```yaml）。当为 True 时，
    模型被提示生成带边框的输出，解析器也期望这样的输出。当为 False 时，
    期望原始的 JSON/YAML。如果你的模型使用了模式约束，通常可以将其设置为 False，
    除非约束也考虑了代码边框分隔符。
  constraint: 解码输出时应用约束。默认为无约束。
  format_type: 输出的格式类型（JSON 或 YAML）。
"""
        self._fence_output = fence_output
        self._constraint = constraint
        self._format_type = format_type

    @property
    def fence_output(self) -> bool:
        """返回是否期望带边框的输出。"""
        return self._fence_output

    @fence_output.setter
    def fence_output(self, fence_output: bool) -> None:
        """
    设置是否期望带边框的输出。
    Args:
      fence_output: Whether to expect fenced output.
    """
        self._fence_output = fence_output

    @property
    def format_type(self) -> data.FormatType:
        """返回格式类型."""
        return self._format_type

    @format_type.setter
    def format_type(self, new_format_type: data.FormatType) -> None:
        """设置新的格式类型."""
        self._format_type = new_format_type

    @abc.abstractmethod
    def resolve(
            self,
            input_text: str,
            **kwargs,
    ) -> Sequence[data.Extraction]:
        """
    在输入文本上运行解析函数。

参数:
    input_text: 要处理的输入文本。
    **kwargs: 子类实现的附加参数。

返回:
    以 Extraction 形式表示的带注释文本。
    """

    @abc.abstractmethod
    def align(
            self,
            extractions: Sequence[data.Extraction],
            source_text: str,
            token_offset: int,
            char_offset: int | None = None,
            enable_fuzzy_alignment: bool = True,
            fuzzy_alignment_threshold: float = _FUZZY_ALIGNMENT_MIN_THRESHOLD,
            accept_match_lesser: bool = True,
    ) -> Iterator[data.Extraction]:
        """
将抽取结果与源文本对齐，设置token/字符区间和对齐状态。

先使用精确匹配（difflib），如果启用的话再使用模糊对齐作为备选方案。

对齐状态结果:
- MATCH_EXACT: 完美的token级匹配
- MATCH_LESSER: 部分精确匹配（抽取内容比匹配的文本长）
- MATCH_FUZZY: 最佳重叠窗口满足阈值（≥ fuzzy_alignment_threshold）
- None: 未找到对齐

参数:
  extractions: 要与源文本对齐的带注释的抽取结果。
  source_text: 要在其中对齐抽取结果的文本。
  token_offset: 对应于块的起始token索引的token_offset。
  char_offset: 对应于块的起始字符索引的char_offset。
  enable_fuzzy_alignment: 精确匹配失败时是否使用模糊对齐。
  fuzzy_alignment_threshold: 模糊对齐的最小token重叠比率（0-1）。
  accept_match_lesser: 是否接受部分精确匹配（MATCH_LESSER状态）。

产出:
  带有更新的token区间和对齐状态的对齐抽取结果。
    """


ExtractionValueType = str | int | float | dict | list | None


class ResolverParsingError(exceptions.LangExtractError):
    """当内容无法解析为指定格式时引发的错误。"""


class Resolver(AbstractResolver):
    """
基于YAML/JSON的信息抽取解析器。

允许对文本内的YAML或JSON内容进行自定义解析。抽取的结果要么按照指定的索引后缀排序，
要么，在没有索引后缀的情况下，按照抽取项出现的顺序排列。如果提供了属性后缀，
则会提取与抽取项关联的属性。索引和属性后缀都由prompt.txt示例决定。
  """

    def __init__(
            self,
            fence_output: bool = True,
            extraction_index_suffix: str | None = "_index",
            extraction_attributes_suffix: str | None = "_attributes",
            constraint: schema.Constraint = schema.Constraint(),
            format_type: data.FormatType = data.FormatType.JSON,
    ):
        """
    构造函数。

    参数:
      fence_output: 是否期望带边框的输出（```json or ```yaml）。
      extraction_index_suffix: 用于识别确定抽取顺序的索引键的后缀。
      extraction_attributes_suffix: 用于识别与抽取相关联的属性键的后缀。
      constraint: 解码输出时应用约束。
      format_type: 要解析的格式（YAML 或 JSON）。
    """
        super().__init__(
            fence_output=fence_output,
            constraint=constraint,
        )
        self.extraction_index_suffix = extraction_index_suffix
        self.extraction_attributes_suffix = extraction_attributes_suffix
        self.format_type = format_type

    def resolve(
            self,
            input_text: str,
            suppress_parse_errors: bool = False,
            **kwargs,
    ) -> Sequence[data.Extraction]:
        """
    ## resolve 函数功能解释

    ### 主要功能
    `resolve` 函数是 LangExtract 库中的核心解析器，用于处理包含 YAML/JSON 格式抽取数据的文本内容，并将其转换为标准化的抽取对象序列。

    ### 输入参数
    - `input_text`: 待处理的输入文本（包含YAML/JSON格式的抽取数据）
    - `suppress_parse_errors`: 控制是否抑制解析错误（True则记录错误并继续执行，False则抛出异常）
    - `**kwargs`: 额外的关键字参数

    ### 处理流程
    1. **日志记录**: 开始解析过程并记录输入文本信息
    2. **文本解析**: 调用 `string_to_extraction_data` 方法将输入文本转换为抽取数据格式
    3. **错误处理**: 捕获解析异常，根据 `suppress_parse_errors` 参数决定是否继续执行
    4. **数据处理**: 调用 `extract_ordered_extractions` 方法处理抽取数据
    5. **结果返回**: 返回处理后的 `data.Extraction` 对象序列

    ### 返回值
    - 类型: `Sequence[data.Extraction]`
    - 内容: 经过解析和处理的标准化抽取对象序列

    ### 异常处理
    - 抛出 `ResolverParsingError` 异常当内容无法解析或格式错误时
    - 支持错误抑制模式，允许在解析失败时继续执行流程

    ### 应用场景
    主要用于知识图谱信息抽取流程中，将非结构化的文本数据解析为结构化的抽取对象，便于后续处理和存储。
    """
        logging.info("Starting resolver process for input text.")  # 记录解析过程开始的日志
        logging.debug("Input Text: %s", input_text)  # 记录输入文本的调试日志

        try:  # 开始try-except块进行错误处理
            extraction_data = self.string_to_extraction_data(input_text)  # 调用string_to_extraction_data方法解析输入文本
            logging.debug("Parsed content: %s", extraction_data)  # 记录解析后内容的调试日志

        except (ResolverParsingError, ValueError) as e:  # 捕获ResolverParsingError或ValueError异常
            if suppress_parse_errors:  # 如果设置了抑制解析错误
                logging.exception(  # 记录异常日志
                    "Failed to parse input_text: %s, error: %s", input_text, e  # 解析input_text失败：输入文本，错误信息
                )
                return []  # 返回空列表
            raise ResolverParsingError("Failed to parse content.") from e  # 抛出ResolverParsingError异常，原因为捕获的异常

        processed_extractions = self.extract_ordered_extractions(
            extraction_data)  # 调用extract_ordered_extractions方法处理抽取数据

        logging.debug("Completed the resolver process.")  # 记录解析过程完成的调试日志

        return processed_extractions  # 返回处理后的抽取结果

    def align(
            self,
            extractions: Sequence[data.Extraction],
            source_text: str,
            token_offset: int,
            char_offset: int | None = None,
            enable_fuzzy_alignment: bool = True,
            fuzzy_alignment_threshold: float = _FUZZY_ALIGNMENT_MIN_THRESHOLD,
            accept_match_lesser: bool = True,
    ) -> Iterator[data.Extraction]:
        """
对源文本进行对齐标注的提取。

这使用基于Python的difflib SequenceMatcher的WordAligner来匹配源文本中的标记和标注提取中的标记。
如果提取顺序与源文本顺序显著不同，difflib可能会跳过某些匹配，使某些提取未匹配。

参数:
  extractions: 标注的提取结果。
  source_text: 在其中对提取进行对齐的文本块。
  token_offset: 文本块的起始标记索引。
  char_offset: 文本块的起始字符索引。
  enable_fuzzy_alignment: 是否启用模糊对齐回退。
  fuzzy_alignment_threshold: 模糊对齐所需的最小重叠比率。
  accept_match_lesser: 是否接受部分精确匹配(MATCH_LESSER状态)。

返回:
    对齐提取结果的迭代器。
    """
        logging.info("Starting alignment process for provided chunk text.")  # 记录开始对齐过程的日志信息

        if not extractions:  # 如果没有提取结果
            logging.debug(  # 记录调试日志
                "No extractions found in the annotated text; exiting alignment"  # 在标注文本中未找到提取结果；退出对齐
                " process."  # 过程
            )
            return  # 直接返回
        else:  # 如果有提取结果
            extractions_group = [extractions]  # 将提取结果包装成列表
        # TODO
        logging.info(
            f"阶段一:enable_fuzzy_alignment:{enable_fuzzy_alignment}\naccept_match_lesser:{accept_match_lesser}")  # 记录开始对齐过程的日志信息

        aligner = WordAligner()  # 创建WordAligner实例
        # TODO
        logging.info("阶段二")  # 记录开始对齐过程的日志信息

        # 调用aligner的align_extractions方法对提取结果进行对齐
        aligned_yaml_extractions = aligner.align_extractions(
            extractions_group,  # 提取结果组
            source_text,  # 源文本
            token_offset,  # 标记偏移量
            char_offset or 0,  # 字符偏移量，如果没有则默认为0
            enable_fuzzy_alignment=enable_fuzzy_alignment,  # 启用模糊对齐设置
            fuzzy_alignment_threshold=fuzzy_alignment_threshold,  # 模糊对齐阈值设置
            accept_match_lesser=accept_match_lesser,  # 接受较小匹配设置
        )
        # TODO
        logging.info("阶段三")  # 记录开始对齐过程的日志信息

        # 记录对齐后的提取结果数量
        logging.debug(
            "Aligned extractions count: %d",  # 对齐的提取结果数量：%d
            sum(len(group) for group in aligned_yaml_extractions),  # 计算所有组中提取结果的总数
        )

        # 遍历所有对齐后的提取结果
        for extraction in itertools.chain(*aligned_yaml_extractions):  # 使用itertools.chain展开嵌套的提取结果
            logging.debug("Yielding aligned extraction: %s", extraction)  # 记录正在返回对齐提取结果的日志
            yield extraction  # 生成对齐后的提取结果

        logging.info("Completed alignment process for the provided source_text.")  # 记录完成对齐过程的日志信息

    def _extract_and_parse_content(
            self,
            input_string: str,
    ) -> (
            Mapping[str, ExtractionValueType]
            | Sequence[Mapping[str, ExtractionValueType]]
    ):
        """
    # 函数定义
    _extract_and_parse_content 函数用于从输入字符串中提取并解析YAML或JSON格式的内容。
    # 功能说明
    此函数是一个辅助函数，用于根据分隔符提取和解析内容，并将其解析为YAML或JSON格式。它首先检查输入字符串是否为空或非字符串类型，然后根据配置决定是否从三重反引号标记中提取内容，最后将内容解析为Python对象。
    # 参数
    self: 类实例引用
    input_string: 待处理的输入字符串
    # 返回值
    解析后的Python对象（字典或列表）
    类型为 Mapping[str, ExtractionValueType] 或 Sequence[Mapping[str, ExtractionValueType]]
    # 处理流程
    记录开始解析的日志信息
    验证输入字符串是否非空且为字符串类型
    如果 fence_output 为真，则：
    查找开始标记（``` + 格式类型值）和结束标记（```）
    提取标记之间的内容并去除首尾空白
    如果 fence_output 为假，则直接使用整个输入字符串作为内容
    根据 format_type 属性选择解析器：
    如果是YAML格式，使用 yaml.safe_load 解析
    否则使用 json.loads 解析
    解析成功时记录调试日志
    解析失败时进行异常处理，记录详细的错误信息
    返回解析后的数据
    # 异常处理
    ValueError: 当输入字符串为空、非字符串类型或不包含有效标记时抛出
    ResolverParsingError: 当解析内容失败时抛出
    yaml.YAMLError: YAML解析错误时捕获并处理
    json.JSONDecodeError: JSON解析错误时捕获并处理
    # 错误诊断功能
    详细记录JSON解析错误的位置、列号和消息
    显示错误位置附近的上下文内容
    输出待解析内容的前200个字符用于调试
    检查并报告控制字符问题
    记录完整的待解析内容以便调试
    # 应用场景
    主要用于从大语言模型输出的包含代码块标记的文本中提取真正的YAML/JSON内容，并将其解析为Python数据结构。
    """
        # 记录开始解析字符串的信息日志
        logging.info("Starting string parsing.")
        # 记录输入字符串的调试日志
        logging.debug("input_string: %s", input_string)

        # 检查输入字符串是否为空或非字符串类型
        if not input_string or not isinstance(input_string, str):
            # 如果输入无效，记录错误日志
            logging.error("Input string must be a non-empty string.")
            # 抛出ValueError异常
            raise ValueError("Input string must be a non-empty string.")

        # 根据fence_output标志决定如何处理输入内容
        if self.fence_output:
            # 构建查找的左边界键
            left_key = "```" + self.format_type.value
            # 查找左边界位置
            left = input_string.find(left_key)
            # 查找右边界位置（最后一次出现的位置）
            right = input_string.rfind("```")
            # 获取左边界键的长度
            prefix_length = len(left_key)
            # 验证边界是否有效（都存在且左边界在右边界之前）
            if left == -1 or right == -1 or left >= right:
                # 如果边界无效，记录错误日志
                logging.error("Input string does not contain valid markers.")
                # 抛出ValueError异常
                raise ValueError("Input string does not contain valid markers.")

            # 提取边界之间的内容并去除首尾空白
            content = input_string[left + prefix_length: right].strip()
            # 记录提取内容的调试日志
            logging.debug("Content: %s", content)
        else:
            # 如果不需要边界，直接使用整个输入字符串作为内容
            content = input_string

            # 尝试解析内容
        try:
            # 根据格式类型选择相应的解析器
            if self.format_type == data.FormatType.YAML:
                # 使用YAML安全加载器解析内容
                parsed_data = yaml.safe_load(content)
            else:
                # 使用JSON解析器解析内容
                parsed_data = json.loads(content)
            # 解析成功时记录调试日志
            logging.debug("Successfully parsed content.")
            # 捕获YAML和JSON解析错误
        except (yaml.YAMLError, json.JSONDecodeError) as e:
            # 记录解析失败的警告日志
            logging.warning("Failed to parse content.")
            # 如果是JSON解析错误，提供更多详细信息
            if isinstance(e, json.JSONDecodeError):
                # 记录JSON解析错误的行号、列号和消息
                logging.warning("JSON decode error at line %d column %d: %s", e.lineno, e.colno, e.msg)
                # 记录错误位置
                logging.warning("Failed position: %d", e.pos)
                # 记录错误位置周围的上下文内容
                logging.warning("Failed context: %s", repr(content[max(0, e.pos - 30):e.pos + 30]))
                # 输出完整内容的前200个字符，便于调试
                logging.warning("Full content (first 200 chars): %s", repr(content[:200]))

                # TODO: 优化1-14 如果是JSON格式且json_repair可用，则尝试修复
                # 检查是否为JSON格式且修复库可用
                if self.format_type == data.FormatType.JSON and repair_json is not None:
                    # 记录尝试修复JSON内容的信息
                    logging.info("Attempting to repair JSON content...")
                    try:
                        # 尝试修复JSON内容
                        repaired_content = repair_json(content)
                        # 解析修复后的内容
                        parsed_data = json.loads(repaired_content)
                        # 记录修复和解析成功的信息
                        logging.info("Successfully parsed repaired JSON content.")
                        # 返回修复后解析的数据
                        return parsed_data
                    # 捕获修复过程中可能出现的异常
                    except Exception as repair_error:
                        # 记录修复失败的错误
                        logging.error("Failed to repair and parse JSON: %s", str(repair_error))

                # 检查特殊字符
                # 遍历内容中的每个字符
                for i, char in enumerate(content):
                    # 检查是否为控制字符（ASCII值小于32）但不是常见的换行符、回车符或制表符
                    if ord(char) < 32 and char not in ['\n', '\r', '\t']:
                        # 记录发现控制字符的错误日志
                        logging.error("Control character found at position %d: %r (ord=%d)", i, char, ord(char))
            # 如果是YAML错误，记录错误信息
            elif isinstance(e, yaml.YAMLError):
                logging.error("YAML error: %s", str(e))
            # 记录无法解析的完整内容（repr格式）
            logging.error("Full content that failed to parse (repr): %s", repr(content))
            # 记录无法解析的完整内容（原始格式）
            logging.error("Full content that failed to parse (raw): \n%s", content)
            # 抛出ResolverParsingError异常，保留原始异常作为原因
            raise ResolverParsingError("Failed to parse content.") from e

            # 返回解析成功的数据
        return parsed_data

    def string_to_extraction_data(
            self,
            input_string: str,
    ) -> Sequence[Mapping[str, ExtractionValueType]]:
        """
    ## string_to_extraction_data 函数功能解释

    ### 主要功能
    `string_to_extraction_data` 函数用于将包含 YAML 或 JSON 格式的字符串解析为抽取数据，是信息抽取流程中的关键解析组件。

    ### 输入参数
    - `input_string`: 包含YAML或JSON格式内容的字符串，通常用三重反引号包围
    ### 处理流程
    内容提取: 调用 _extract_and_parse_content 方法从输入字符串中提取并解析内容
    数据验证: 验证解析结果是否为字典类型
    键值检查: 确认内容包含 'extractions' 键
    列表验证: 验证 'extractions' 键对应的值是否为列表类型
    元素校验: 遍历列表中的每个项目，验证它们是否为字典类型
    类型检查: 检查每个项目的键值对类型（键必须为字符串，值必须为允许的类型）
    ### 返回值
    类型: Sequence[Mapping[str, ExtractionValueType]]
    内容: 解析后的抽取对象序列，每个对象都是字符串到抽取值类型的映射
    ### 异常处理
    抛出 ResolverParsingError 当内容格式不符合预期时
    抛出 ValueError 当输入无效时
    ### 数据验证规则
    解析后的内容必须是字典类型
    字典必须包含 'extractions' 键
    'extractions' 键对应的值必须是列表
    列表中的每个元素必须是字典
    字典中的键必须是字符串类型
    字典中的值必须是字符串、整数、浮点数、字典或None类型
    ### 应用场景
    主要用于将大语言模型输出的YAML/JSON格式文本转换为结构化的抽取数据，为后续的信息抽取处理提供标准化输入。
    """
        # 调用 _extract_and_parse_content 方法解析输入字符串，返回解析后的数据
        parsed_data = self._extract_and_parse_content(input_string)

        # 检查解析后的数据是否为字典类型，如果不是则抛出异常
        if not isinstance(parsed_data, dict):
            logging.error("Expected content to be a mapping (dict).")
            raise ResolverParsingError(
                f"Content must be a mapping with an '{schema.EXTRACTIONS_KEY}' key."
            )

        # 检查解析后的数据是否包含 'extractions' 键，如果没有则抛出异常
        if schema.EXTRACTIONS_KEY not in parsed_data:
            logging.error("Content does not contain 'extractions' key.")
            raise ResolverParsingError("Content must contain an 'extractions' key.")

        # 获取 'extractions' 键对应的值
        extractions = parsed_data[schema.EXTRACTIONS_KEY]

        # 检查 'extractions' 的值是否为列表类型，如果不是则抛出异常
        if not isinstance(extractions, list):
            logging.error("The content must be a sequence (list) of mappings.")
            raise ResolverParsingError(
                "The extractions must be a sequence (list) of mappings."
            )

        # 遍历 'extractions' 列表中的每个元素
        for item in extractions:
            # 检查每个元素是否为字典类型，如果不是则抛出异常
            if not isinstance(item, dict):
                logging.error("Each item in the sequence must be a mapping.")
                raise ResolverParsingError(
                    "Each item in the sequence must be a mapping."
                )

            # 遍历字典中的每个键值对
            for key, value in item.items():
                # 检查键是否为字符串类型，值是否为允许的类型（字符串、整数、浮点数、字典或None）
                if not isinstance(key, str) or not isinstance(
                        value, ExtractionValueType
                ):
                    logging.error("Invalid key or value type detected in content.")
                    raise ResolverParsingError(
                        "All keys must be strings and values must be either strings,"
                        " integers, floats, dicts, or None."
                    )

        # 记录解析完成的日志
        logging.info("Completed parsing of string.")
        # 返回解析后的抽取数据
        return extractions

    def extract_ordered_extractions(
            self,
            extraction_data: Sequence[Mapping[str, ExtractionValueType]],
    ) -> Sequence[data.Extraction]:
        """
    根据关联的索引提取并排序抽取数据。

    该函数处理一个字典列表，每个字典包含抽取类别键及其对应值的键值对，
    以及可选的索引键（通过 index_suffix 标识）。它按索引升序对这些键值对进行排序，
    并排除没有索引键的键值对，返回一个元组列表（extraction_class: str, extraction_text: str）。

    参数:
        extraction_data: 一个字典列表。每个字典包含抽取类别键及其值的键值对，
          以及可选的索引键。

    返回:
        按索引属性或出现顺序排序的抽取结果。如果两个抽取结果具有相同的索引，
        则它们的组顺序决定排序顺序。

    异常:
        ValueError: 如果抽取文本不是字符串或整数，或者索引不是整数。
    """
        # 记录开始提取和排序抽取数据的日志
        logging.info("Starting to extract and order extractions from data.")

        # 如果传入的抽取数据为空，则记录调试日志
        if not extraction_data:
            logging.debug("Received empty extraction data.")

        # 初始化一个空列表，用于存储处理后的抽取结果
        processed_extractions = []
        # 初始化抽取索引为0，用于在没有索引后缀时自动递增
        extraction_index = 0
        # 获取索引后缀，用于识别索引键
        index_suffix = self.extraction_index_suffix
        # 获取属性后缀，用于识别属性键
        attributes_suffix = self.extraction_attributes_suffix

        # 遍历抽取数据中的每个组（group），同时获取组索引
        for group_index, group in enumerate(extraction_data):
            # 遍历组中的每个键值对（extraction_class 和 extraction_value）
            for extraction_class, extraction_value in group.items():
                # 如果存在索引后缀，并且当前键以索引后缀结尾
                if index_suffix and extraction_class.endswith(index_suffix):
                    # 检查索引值是否为整数，如果不是则抛出异常
                    if not isinstance(extraction_value, int):
                        logging.error(
                            "Index must be a string or integer. Found: %s",
                            type(extraction_value),
                        )
                        raise ValueError(
                            "Extraction text must must be a string or integer."
                        )
                    # 跳过索引键的处理，继续下一个键值对
                    continue

                # 如果存在属性后缀，并且当前键以属性后缀结尾
                if attributes_suffix and extraction_class.endswith(attributes_suffix):
                    # 检查属性值是否为字典或None，如果不是则抛出异常
                    if not isinstance(extraction_value, (dict, type(None))):
                        logging.error(
                            "Attributes must be a dict or None. Found: %s",
                            type(extraction_value),
                        )
                        raise ValueError(
                            "Extraction value must be a dict or None for attributes."
                        )
                    # 跳过属性键的处理，继续下一个键值对
                    continue

                # 检查抽取值是否为允许的类型（字符串、整数、浮点数、字典或None）
                if not isinstance(extraction_value, ExtractionValueType):
                    logging.error(
                        "Extraction text must be a string or integer. Found: %s",
                        type(extraction_value),
                    )
                    raise ValueError("Extraction text must must be a string or integer.")

                # 如果抽取值不是字符串类型，则将其转换为字符串
                if not isinstance(extraction_value, str):
                    extraction_value = str(extraction_value)

                # 如果存在索引后缀，则根据索引后缀查找对应的索引值
                if index_suffix:
                    index_key = extraction_class + index_suffix
                    extraction_index = group.get(index_key, None)
                    # 如果找不到索引值，则记录调试日志并跳过该抽取项
                    if extraction_index is None:
                        logging.debug(
                            "No index value for %s. Skipping extraction.", extraction_class
                        )
                        continue
                else:
                    # 如果没有索引后缀，则自动递增索引
                    extraction_index += 1

                # 如果存在属性后缀，则根据属性后缀查找对应的属性值
                attributes = None
                if attributes_suffix:
                    attributes_key = extraction_class + attributes_suffix
                    attributes = group.get(attributes_key, None)

                # 将处理后的抽取项添加到结果列表中
                processed_extractions.append(
                    data.Extraction(
                        extraction_class=extraction_class,  # 抽取类别
                        extraction_text=extraction_value,  # 抽取文本
                        extraction_index=extraction_index,  # 抽取索引
                        group_index=group_index,  # 组索引
                        attributes=attributes,  # 属性（如果有）
                    )
                )

        # 根据抽取索引对结果进行排序
        processed_extractions.sort(key=operator.attrgetter("extraction_index"))
        # 记录完成抽取和排序的日志
        logging.info("Completed extraction and ordering of extractions.")
        # 返回处理后的抽取结果
        return processed_extractions


class WordAligner:
    """该类用于在两个 token 序列之间进行词语对齐。"""

    def __init__(self):
        """
        使用 difflib SequenceMatcher 初始化 WordAligner。
        """
        self.matcher = difflib.SequenceMatcher(autojunk=False)
        self.source_tokens: Sequence[str] | None = None
        self.extraction_tokens: Sequence[str] | None = None

    def _set_seqs(
            self,
            source_tokens: Sequence[str] | Iterator[str],
            extraction_tokens: Sequence[str] | Iterator[str],
    ):
        """
使用 difflib SequenceMatcher 初始化 WordAligner。
    """

        if isinstance(source_tokens, Iterator):
            source_tokens = list(source_tokens)
        if isinstance(extraction_tokens, Iterator):
            extraction_tokens = list(extraction_tokens)

        if not source_tokens or not extraction_tokens:
            raise ValueError("Source tokens and extraction tokens cannot be empty.")

        self.source_tokens = source_tokens
        self.extraction_tokens = extraction_tokens
        self.matcher.set_seqs(a=source_tokens, b=extraction_tokens)

    def _get_matching_blocks(self) -> Sequence[tuple[int, int, int]]:
        """
注解解释
这段代码中的注解提供了关于 _get_matching_blocks 方法的重要信息：
返回值说明
返回类型: Sequence[tuple[int, int, int]]
三元组 (i, j, n) 的含义:
i: 在源文本 token 序列 (source_tokens) 中的起始索引
j: 在抽取文本 token 序列 (extraction_tokens) 中的起始索引
n: 匹配的 token 数量（匹配块的长度）
核心特性
匹配关系: 每个匹配块满足 S[i:i+n] == E[j:j+n]，即源序列从 i 开始的 n 个 token 与抽取序列从 j 开始的 n 个 token 相等
单调性保证: 所有匹配块在 j 坐标上是单调递增的，确保了匹配顺序的一致性
虚拟终结符: 最后一个元素是虚拟条目 (len(S), len(E), 0)，用于标识序列结束
作用
这个注解说明了该方法基于 difflib.SequenceMatcher 算法，用于找出两个 token 序列之间的所有连续匹配子序列，是文本对齐功能的基础工具，能够精确定位抽取内容在源文本中的位置。
    """
        if self.source_tokens is None or self.extraction_tokens is None:
            raise ValueError(
                "Source tokens and extraction tokens must be set before getting"
                " matching blocks."
            )
        return self.matcher.get_matching_blocks()

    def _fuzzy_align_extraction(
            self,
            # extraction: 需要对齐的提取结果对象
            extraction: data.Extraction,
            # source_tokens: 源文本的token列表
            source_tokens: list[str],
            # tokenized_text: 分词后的源文本对象
            tokenized_text: tokenizer.TokenizedText,
            # token_offset: 当前块的token偏移量
            token_offset: int,
            # char_offset: 当前块的字符偏移量
            char_offset: int,
            # fuzzy_alignment_threshold: 模糊匹配的最小比率阈值，默认为最小阈值
            fuzzy_alignment_threshold: float = _FUZZY_ALIGNMENT_MIN_THRESHOLD,
    ) -> data.Extraction | None:
        """使用difflib.SequenceMatcher在tokens上进行模糊对齐。

    该算法扫描source_tokens中的每个候选窗口，并选择具有最高SequenceMatcher比率的窗口。
    它使用高效的token计数交集作为快速预检查，以排除无法达到对齐阈值的窗口。
    当比率≥fuzzy_alignment_threshold时接受匹配。这只在未匹配的提取上运行，
    这通常是总提取的一个小子集。

    参数:
      extraction: 需要对齐的提取结果。
      source_tokens: 源文本的tokens。
      tokenized_text: 分词后的源文本。
      token_offset: 当前块的token偏移量。
      char_offset: 当前块的字符偏移量。
      fuzzy_alignment_threshold: 模糊匹配的最小比率。

    返回:
      如果成功则返回对齐的data.Extraction，否则返回None。
    """

        # 将提取文本分词为小写的token列表
        extraction_tokens = list(
            _tokenize_with_lowercase(extraction.extraction_text)
        )
        # 使用轻微词干化的tokens，以便复数形式不会阻止对齐
        extraction_tokens_norm = [_normalize_token(t) for t in extraction_tokens]

        # 如果提取tokens为空，则返回None
        if not extraction_tokens:
            return None

        # 记录调试信息：模糊对齐提取文本及其token数量
        logging.debug(
            "Fuzzy aligning %r (%d tokens)",
            extraction.extraction_text,
            len(extraction_tokens),
        )

        # 初始化最佳比率为0.0
        best_ratio = 0.0
        # 初始化最佳跨度为None（包含起始索引和窗口大小）
        best_span: tuple[int, int] | None = None  # (start_idx, window_size)

        # 获取提取文本的长度
        len_e = len(extraction_tokens)
        # 设置最大窗口大小为源tokens的长度
        max_window = len(source_tokens)

        # 计算提取tokens的计数器
        extraction_counts = collections.Counter(extraction_tokens_norm)
        # 计算最小重叠数量（长度乘以阈值）
        min_overlap = int(len_e * fuzzy_alignment_threshold)

        # 创建SequenceMatcher对象，autojunk设为False，b参数为标准化的提取tokens
        matcher = difflib.SequenceMatcher(autojunk=False, b=extraction_tokens_norm)

        # 遍历窗口大小（从提取文本长度到最大窗口大小）
        for window_size in range(len_e, max_window + 1):
            # 如果窗口大小超过源tokens长度，则跳出循环
            if window_size > len(source_tokens):
                break

            # 初始化滑动窗口：创建包含前window_size个源tokens的双端队列
            window_deque = collections.deque(source_tokens[0:window_size])
            # 计算窗口中标准化token的计数
            window_counts = collections.Counter(
                [_normalize_token(t) for t in window_deque]
            )

            # 遍历可能的起始索引（确保窗口不会超出源tokens范围）
            for start_idx in range(len(source_tokens) - window_size + 1):
                # 优化：在进行昂贵的序列匹配之前，检查是否存在足够的重叠tokens。
                # 这是对匹配计数的上限估计。
                # 如果提取tokens和窗口tokens的交集总数大于等于最小重叠数
                if (extraction_counts & window_counts).total() >= min_overlap:
                    # 将窗口中的tokens标准化
                    window_tokens_norm = [_normalize_token(t) for t in window_deque]
                    # 设置matcher的第一个序列
                    matcher.set_seq1(window_tokens_norm)
                    # 获取匹配块并计算匹配总数
                    matches = sum(size for _, _, size in matcher.get_matching_blocks())
                    # 计算匹配比率
                    if len_e > 0:
                        ratio = matches / len_e
                    else:
                        ratio = 0.0
                    # 如果当前比率优于最佳比率
                    if ratio > best_ratio:
                        # 更新最佳比率
                        best_ratio = ratio
                        # 更新最佳跨度
                        best_span = (start_idx, window_size)

                # 将窗口向右滑动
                # 如果起始索引+窗口大小小于源tokens长度
                if start_idx + window_size < len(source_tokens):
                    # 从计数中移除最左边的token
                    old_token = window_deque.popleft()
                    old_token_norm = _normalize_token(old_token)
                    window_counts[old_token_norm] -= 1
                    # 如果该token计数变为0，则从计数器中删除
                    if window_counts[old_token_norm] == 0:
                        del window_counts[old_token_norm]

                    # 将新的最右边token添加到队列和计数中
                    new_token = source_tokens[start_idx + window_size]
                    window_deque.append(new_token)
                    new_token_norm = _normalize_token(new_token)
                    window_counts[new_token_norm] += 1

        # 如果找到了最佳跨度且最佳比率大于等于模糊对齐阈值
        if best_span and best_ratio >= fuzzy_alignment_threshold:
            # 解包最佳跨度为起始索引和窗口大小
            start_idx, window_size = best_span

            try:
                # 设置提取结果的token区间
                extraction.token_interval = tokenizer.TokenInterval(
                    # 设置起始索引（加上偏移量）
                    start_index=start_idx + token_offset,
                    # 设置结束索引（加上偏移量和窗口大小）
                    end_index=start_idx + window_size + token_offset,
                )

                # 获取起始token
                start_token = tokenized_text.tokens[start_idx]
                # 获取结束token
                end_token = tokenized_text.tokens[start_idx + window_size - 1]
                # 设置提取结果的字符区间
                extraction.char_interval = data.CharInterval(
                    # 设置起始位置（加上字符偏移量和起始token的起始位置）
                    start_pos=char_offset + start_token.char_interval.start_pos,
                    # 设置结束位置（加上字符偏移量和结束token的结束位置）
                    end_pos=char_offset + end_token.char_interval.end_pos,
                )

                # 设置对齐状态为模糊匹配
                extraction.alignment_status = data.AlignmentStatus.MATCH_FUZZY
                # 返回对齐后的提取结果
                return extraction
            except IndexError:
                # 如果发生索引错误，记录异常信息
                logging.exception(
                    "Index error while setting intervals during fuzzy alignment."
                )
                # 返回None
                return None

        # 如果没有找到满足条件的最佳跨度，返回None
        return None

    def align_extractions(
            self,
            # extraction_groups: 提取结果的分组，每个组包含多个Extraction对象
            extraction_groups: Sequence[Sequence[data.Extraction]],
            # source_text: 源文本，用于与提取结果进行对齐
            source_text: str,
            # token_offset: token区间起始和结束索引的偏移量，默认为0
            token_offset: int = 0,
            # char_offset: 字符区间起始和结束位置的偏移量，默认为0
            char_offset: int = 0,
            # delim: 用于分隔多token提取结果的分隔符，默认为Unicode单元分隔符
            delim: str = "\u241F",  # Unicode Symbol for unit separator
            # enable_fuzzy_alignment: 是否在精确匹配失败时使用模糊对齐，默认为True
            enable_fuzzy_alignment: bool = True,
            # fuzzy_alignment_threshold: 模糊对齐的最小token重叠比率(0-1)，默认为最小阈值
            fuzzy_alignment_threshold: float = _FUZZY_ALIGNMENT_MIN_THRESHOLD,
            # accept_match_lesser: 是否接受部分精确匹配，默认为True
            accept_match_lesser: bool = True,
    ) -> Sequence[Sequence[data.Extraction]]:
        """将提取结果与源文本中的位置对齐。

    此方法接收一系列提取结果和源文本，将每个提取结果与源文本中的相应位置对齐。
    它返回一系列提取结果以及指示每个提取结果在源文本中起始和结束位置的token区间。
    如果某个提取结果无法对齐，则其token区间被设置为None。

    参数:
      extraction_groups: 序列的序列，其中每个内部序列包含一个Extraction对象。
      source_text: 用于对齐提取结果的源文本。
      token_offset: 要添加到token区间起始和结束索引的偏移量。
      char_offset: 要添加到字符区间起始和结束位置的偏移量。
      delim: 用于分隔多token提取结果的分隔符。
      enable_fuzzy_alignment: 当精确匹配失败时是否使用模糊对齐。
      fuzzy_alignment_threshold: 模糊对齐的最小token重叠比率 (0-1)。
      accept_match_lesser: 是否接受部分精确匹配 (MATCH_LESSER状态)。

    返回:
      与源文本对齐的提取结果序列，包括token区间。
    """
        # 记录调试信息：开始对提取结果与源文本进行对齐，打印待对齐的提取组
        logging.debug(
            "WordAligner: Starting alignment of extractions with the source text."
            " Extraction groups to align: %s",
            extraction_groups,
        )
        # 如果没有提取组，则记录信息并返回空列表
        if not extraction_groups:
            logging.info("No extraction groups provided; returning empty list.")
            return []
        # TODO
        logging.info("阶段二-1")  # 记录开始对齐过程的日志信息
        # 将源文本分词为小写形式的token列表
        source_tokens = list(_tokenize_with_lowercase(source_text))
        # TODO
        logging.info("阶段二-2")  # 记录开始对齐过程的日志信息
        # 获取分隔符的token长度
        delim_len = len(list(_tokenize_with_lowercase(delim)))
        # 如果分隔符不是单个token，则抛出ValueError异常
        if delim_len != 1:
            raise ValueError(f"Delimiter {delim!r} must be a single token.")

        # 记录调试信息：使用指定分隔符进行提取对齐
        logging.debug("Using delimiter %r for extraction alignment", delim)
        # TODO
        logging.info("阶段二-3")  # 记录开始对齐过程的日志信息
        # 将所有提取结果的文本用分隔符连接起来并进行分词
        extraction_tokens = _tokenize_with_lowercase(
            f" {delim} ".join(
                # 遍历所有提取组中的提取结果，获取其文本内容
                extraction.extraction_text
                for extraction in itertools.chain(*extraction_groups)
            )
        )
        # TODO
        logging.info("阶段二-4")  # 记录开始对齐过程的日志信息
        # 设置源token序列和提取token序列
        self._set_seqs(source_tokens, extraction_tokens)

        # 创建索引到提取组的映射字典
        index_to_extraction_group = {}
        # 初始化提取索引
        extraction_index = 0
        # 遍历提取组及其索引
        # TODO
        logging.info("阶段二-5")  # 记录开始对齐过程的日志信息
        for group_index, group in enumerate(extraction_groups):
            # 记录调试信息：处理提取组及其包含的提取数量
            logging.debug(
                "Processing extraction group %d with %d extractions.",
                group_index,
                len(group),
            )
            # 遍历当前组中的每个提取结果
            for extraction in group:
                # 验证分隔符是否出现在提取文本中
                if delim in extraction.extraction_text:
                    # 如果分隔符出现在提取文本中，则抛出ValueError异常
                    raise ValueError(
                        f"Delimiter {delim!r} appears inside extraction text"
                        f" {extraction.extraction_text!r}. This would corrupt alignment"
                        " mapping."
                    )

                # 将提取索引映射到提取对象和组索引
                index_to_extraction_group[extraction_index] = (extraction, group_index)
                # 将当前提取文本分词为小写token列表
                extraction_text_tokens = list(
                    _tokenize_with_lowercase(extraction.extraction_text)
                )
                # 更新提取索引（加上当前提取文本的token数量和分隔符长度）
                extraction_index += len(extraction_text_tokens) + delim_len
        # TODO
        logging.info("阶段二-6")  # 记录开始对齐过程的日志信息
        # 创建对齐后的提取组列表，为每个提取组初始化空列表
        aligned_extraction_groups: list[list[data.Extraction]] = [
            [] for _ in extraction_groups
        ]
        # TODO
        logging.info("阶段二-7")  # 记录开始对齐过程的日志信息
        # 对源文本进行分词处理
        tokenized_text = tokenizer.tokenize(source_text)
        # TODO
        logging.info("阶段二-8")  # 记录开始对齐过程的日志信息
        # 跟踪在精确匹配阶段对齐的提取结果
        aligned_extractions = []
        # 初始化精确匹配计数器
        exact_matches = 0
        # 初始化较短匹配计数器
        lesser_matches = 0
        # TODO
        logging.info("阶段二-9")  # 记录开始对齐过程的日志信息
        # 精确匹配阶段
        # 遍历匹配块（i: 源文本起始位置, j: 提取文本起始位置, n: 匹配块大小）
        for i, j, n in self._get_matching_blocks()[:-1]:
            # 根据j（提取文本起始位置）获取对应的提取对象和组索引
            extraction, _ = index_to_extraction_group.get(j, (None, None))
            # 如果没有找到对应的提取对象，则跳过当前迭代
            if extraction is None:
                # 记录调试信息：在difflib匹配块中未找到干净的起始索引
                logging.debug(
                    "No clean start index found for extraction index=%d iterating"
                    " Difflib matching_blocks",
                    j,
                )
                continue

            # 设置提取结果的token区间
            extraction.token_interval = tokenizer.TokenInterval(
                # 设置起始索引（加上偏移量）
                start_index=i + token_offset,
                # 设置结束索引（加上偏移量和匹配块大小）
                end_index=i + n + token_offset,
            )

            # 尝试设置字符区间
            try:
                # 获取起始token
                start_token = tokenized_text.tokens[i]
                # 获取结束token
                end_token = tokenized_text.tokens[i + n - 1]
                # 设置字符区间（加上偏移量）
                extraction.char_interval = data.CharInterval(
                    # 设置起始位置（加上字符偏移量和起始token的起始位置）
                    start_pos=char_offset + start_token.char_interval.start_pos,
                    # 设置结束位置（加上字符偏移量和结束token的结束位置）
                    end_pos=char_offset + end_token.char_interval.end_pos,
                )
            except IndexError as e:
                # 如果索引越界，则抛出IndexError异常
                raise IndexError(
                    "Failed to align extraction with source text. Extraction token"
                    f" interval {extraction.token_interval} does not match source text"
                    f" tokens {tokenized_text.tokens}."
                ) from e

            # 计算提取文本的token数量
            extraction_text_len = len(
                list(_tokenize_with_lowercase(extraction.extraction_text))
            )
            # 如果提取文本长度小于匹配块大小
            if extraction_text_len < n:
                # 抛出ValueError异常：分隔符阻止了大于提取长度的块
                raise ValueError(
                    "Delimiter prevents blocks greater than extraction length: "
                    f"extraction_text_len={extraction_text_len}, block_size={n}"
                )
            # 如果提取文本长度等于匹配块大小（精确匹配）
            if extraction_text_len == n:
                # 设置对齐状态为精确匹配
                extraction.alignment_status = data.AlignmentStatus.MATCH_EXACT
                # 精确匹配计数加1
                exact_matches += 1
                # 将当前提取结果添加到已对齐列表中
                aligned_extractions.append(extraction)
            else:
                # 部分匹配（提取文本比匹配文本长）
                # 如果接受较短匹配
                if accept_match_lesser:
                    # 设置对齐状态为较短匹配
                    extraction.alignment_status = data.AlignmentStatus.MATCH_LESSER
                    # 较短匹配计数加1
                    lesser_matches += 1
                    # 将当前提取结果添加到已对齐列表中
                    aligned_extractions.append(extraction)
                else:
                    # 如果不接受较短匹配，则重置区间
                    # 设置token区间为None
                    extraction.token_interval = None
                    # 设置字符区间为None
                    extraction.char_interval = None
                    # 设置对齐状态为None
                    extraction.alignment_status = None
        # TODO
        logging.info("阶段二-10")  # 记录开始对齐过程的日志信息
        # 收集未对齐的提取结果
        unaligned_extractions = []
        # 遍历所有提取对象及其组索引
        for extraction, _ in index_to_extraction_group.values():
            # 如果提取结果不在已对齐列表中，则添加到未对齐列表
            if extraction not in aligned_extractions:
                unaligned_extractions.append(extraction)

        # 对剩余的提取结果应用模糊对齐
        # 如果启用了模糊对齐且存在未对齐的提取结果
        # TODO
        logging.info("阶段二-11")  # 记录开始对齐过程的日志信息
        if enable_fuzzy_alignment and unaligned_extractions:
            # 记录调试信息：开始对未对齐的提取结果进行模糊对齐
            logging.debug(
                "Starting fuzzy alignment for %d unaligned extractions",
                len(unaligned_extractions),
            )
            # 遍历未对齐的提取结果
            for extraction in unaligned_extractions:
                # 调用模糊对齐方法对提取结果进行对齐
                aligned_extraction = self._fuzzy_align_extraction(
                    extraction,
                    source_tokens,
                    tokenized_text,
                    token_offset,
                    char_offset,
                    fuzzy_alignment_threshold,
                )
                # 如果模糊对齐成功
                if aligned_extraction:
                    # 将对齐后的提取结果添加到已对齐列表中
                    aligned_extractions.append(aligned_extraction)
                    # 记录调试信息：模糊对齐成功
                    logging.debug(
                        "Fuzzy alignment successful for extraction: %s",
                        extraction.extraction_text,
                    )
        # TODO
        logging.info("阶段二-12")  # 记录开始对齐过程的日志信息
        # 将对齐后的提取结果分配到对应的组中
        for extraction, group_index in index_to_extraction_group.values():
            # 将提取结果添加到对应组的列表中
            aligned_extraction_groups[group_index].append(extraction)

        # 记录调试信息：最终对齐的提取组
        logging.debug(
            "Final aligned extraction groups: %s", aligned_extraction_groups
        )
        # 返回对齐后的提取组
        return aligned_extraction_groups


def _tokenize_with_lowercase(text: str) -> Iterator[str]:
    """从输入文本中提取并转换为小写的token。

  该函数利用tokenizer模块对文本进行分词，并产生小写的单词。

  参数:
    text (str): 需要分词的文本。

  产出:
    Iterator[str]: 一个迭代器，用于遍历分词后的单词。
  """
    # 使用tokenizer对文本进行分词处理，返回TokenizedText对象
    tokenized_pb2 = tokenizer.tokenize(text)
    # 获取原始文本
    original_text = tokenized_pb2.text
    # 遍历分词后的token列表
    for token in tokenized_pb2.tokens:
        # 获取token的字符区间起始位置
        start = token.char_interval.start_pos
        # 获取token的字符区间结束位置
        end = token.char_interval.end_pos
        # 从原始文本中提取token对应的字符串
        token_str = original_text[start:end]
        # 将token字符串转换为小写
        token_str = token_str.lower()
        # 产出小写的token字符串
        yield token_str


@functools.lru_cache(maxsize=10000)
def _normalize_token(token: str) -> str:
    """Lowercases and applies light pluralisation stemming."""
    token = token.lower()
    if len(token) > 3 and token.endswith("s") and not token.endswith("ss"):
        token = token[:-1]
    return token
