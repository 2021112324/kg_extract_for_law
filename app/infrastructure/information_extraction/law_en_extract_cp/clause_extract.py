"""
英文法律法条抽取器。

本模块仿照 `law_extract.clause_extract` 的处理流程，但面向英文法律文本：
1. 先按 Title / Chapter / Section / Article 等英文法律层级切分文本；
2. 再调用 Langextract 大模型抽取英文图谱实体和关系；
3. 最后将抽取结果整理为项目统一的图谱结构 nodes / edges。

重要约定：
- 图谱标签、属性名、关系名均保持英文，例如 Legal Document、Legal Provision、CONTAINS。
- 源码注释使用中文解释，便于维护；但真正发给模型的 prompt 仍以英文为主，避免模型输出中文图谱。
"""

# =============================================================================
# 标准库导入
# =============================================================================
import asyncio  # 异步 IO 支持，用于并发调用 LLM 抽取多个法条
import json  # JSON 序列化/反序列化，用于保存切分结果和缓存
import logging  # 日志输出，记录抽取过程中的错误、警告和调试信息
import os  # 文件和路径操作，读取环境变量和保存结果文件
import re  # 正则表达式，用于英文章节标题匹配、引用提取、文本清洗
from typing import Optional  # 类型注解，标注可选返回值

# =============================================================================
# 项目内部模块导入
# =============================================================================
# Entity 和 Relationship：项目统一的图谱实体和关系数据类
from app.infrastructure.information_extraction.base import Entity, Relationship
# 英文法条抽取的 few-shot 示例（发给模型的样例）
from app.infrastructure.information_extraction.law_en_extract_cp.prompt.example import (
    example_for_clause,  # 单条法条抽取的示例输入输出对
    example_for_file_info,  # 文件头信息抽取的示例输入输出对
)
# 英文法条抽取的提示词模板（发给模型的指令）
from app.infrastructure.information_extraction.law_en_extract_cp.prompt.prompt import (
    prompt_for_clause,  # 单条法条抽取的 user prompt
    prompt_for_file_info,  # 文件头信息抽取的 user prompt
)
# 英文法条抽取的 schema 定义（定义实体和关系的结构）
from app.infrastructure.information_extraction.law_en_extract_cp.prompt.schema import (
    schema_for_clause,  # 单条法条抽取的实体/关系 schema
    schema_for_file_info,  # 文件头信息抽取的实体/关系 schema
)
# 唯一 ID 生成工具：生成 32 位十六进制 UUID，用于图谱节点 ID
from app.infrastructure.string_utils.id_tool import generate_hex_uuid
# 字符串清洗工具集
from app.infrastructure.string_utils.str_clean import (
    clean_string_for_neo4j_extended,  # Neo4j 兼容的字符串清洗（去掉特殊字符）
    clean_string_with_only_words,  # 只保留字母数字和空格的清洗
    replace_full_corner_space,  # 替换全角空格为标准空格
    replace_zero_width_chars,  # 去除零宽字符（如零宽空格）
)

# =============================================================================
# LLM 模型配置（可通过环境变量覆盖）
# =============================================================================
# 英文法条抽取使用的 LLM 模型名称，默认使用 qwen3-30b 模型
LAW_EN_CLAUSE_MODEL = os.getenv("LAW_EN_CLAUSE_MODEL", "qwen3-30b-a3b-instruct-2507")
# LLM API 密钥，用于身份认证
LAW_EN_CLAUSE_MODEL_API_KEY = os.getenv(
    "LAW_EN_CLAUSE_MODEL_API_KEY",
    "gpustack_342609ce423be29a_4371426b285a91dc44fb4e8d72454847",
)
# LLM API 服务地址，指向 OpenAI 兼容的 chat/completions 接口
LAW_EN_CLAUSE_MODEL_API_URL = os.getenv(
    "LAW_EN_CLAUSE_MODEL_API_URL",
    "http://222.171.219.26:20001/v1/chat/completions",
)

# =============================================================================
# Langextract 抽取器参数配置
# =============================================================================
# MAX_CHAR_BUFFER：单次送入模型的最大字符缓冲长度。超过此长度的文本会被截断，
#   用于控制每次 LLM 调用的 token 消耗。默认 7500 字符。
MAX_CHAR_BUFFER = int(os.getenv("LAW_EN_MAX_CHAR_BUFFER", "7500"))
# BATCH_LENGTH：Langextract 内部批处理大小时，一批处理多少个 extraction item。
#   值越大单次调用抽取越多，但可能降低抽取质量。默认 5。
BATCH_LENGTH = int(os.getenv("LAW_EN_BATCH_LENGTH", "5"))
# MAX_WORKERS：Langextract 内部并发 worker 数。控制同时发起的 LLM 请求数。
#   过大会触发 API 限流，过小会降低吞吐。默认 3。
MAX_WORKERS = int(os.getenv("LAW_EN_MAX_WORKERS", "3"))
# TIMEOUT：单次 LLM 调用的超时时间（秒）。默认 3000 秒。
TIMEOUT = int(os.getenv("LAW_EN_TIMEOUT", "3000"))

# =============================================================================
# 英文法规层级定义
# =============================================================================
# 英文法规常见的层级顺序，按从大到小排列。切分时如果遇到上级层级标题，
# 会清空该层级以下的子层级值，避免前一章节的层级信息串到后续法条中。
# title      = 篇/标题（如 Title 50. War and National Defense）
# subtitle   = 副标题
# chapter    = 章（如 Chapter 35. International Emergency Economic Powers）
# subchapter = 子章
# part       = 部/编
# subpart    = 子部
# division   = 分部
# article    = 条/款组
HIERARCHY_LEVELS = [
    "title",
    "subtitle",
    "chapter",
    "subchapter",
    "part",
    "subpart",
    "division",
    "article",
]

# =============================================================================
# 英文层级标题正则表达式
# =============================================================================
# 每项为 (层级名, 正则) 元组，用于匹配类似以下的英文层级标题行：
#   Title 50. War and National Defense
#   Chapter 35 - International Emergency Economic Powers
#   Part 500. General Provisions
# 正则说明：
#   ^(LevelName\s+[\w.\-]+)  匹配"层级名 编号"前缀
#   (?:\s*[.\-\u2013\u2014:]\s*|\s+)  匹配分隔符（点号、短横、短破折号、冒号）或至少一个空格
#   .*$  匹配标题剩余部分
HIERARCHY_PATTERNS = [
    ("title", re.compile(r"^(Title\s+[\w.\-]+(?:\s*[.\-\u2013\u2014:]\s*|\s+).*)$", re.IGNORECASE)),
    ("subtitle", re.compile(r"^(Subtitle\s+[\w.\-]+(?:\s*[.\-\u2013\u2014:]\s*|\s+).*)$", re.IGNORECASE)),
    ("chapter", re.compile(r"^(Chapter\s+[\w.\-]+(?:\s*[.\-\u2013\u2014:]\s*|\s+).*)$", re.IGNORECASE)),
    ("subchapter", re.compile(r"^(Subchapter\s+[\w.\-]+(?:\s*[.\-\u2013\u2014:]\s*|\s+).*)$", re.IGNORECASE)),
    ("part", re.compile(r"^(Part\s+[\w.\-]+(?:\s*[.\-\u2013\u2014:]\s*|\s+).*)$", re.IGNORECASE)),
    ("subpart", re.compile(r"^(Subpart\s+[\w.\-]+(?:\s*[.\-\u2013\u2014:]\s*|\s+).*)$", re.IGNORECASE)),
    ("division", re.compile(r"^(Division\s+[\w.\-]+(?:\s*[.\-\u2013\u2014:]\s*|\s+).*)$", re.IGNORECASE)),
]

# =============================================================================
# Section / Article 法条标题正则
# =============================================================================
# SECTION_PATTERN：匹配带标题的英文法条编号行。
# 例如：
#   Section 1701. Unusual and extraordinary threat
#   Sec. 1701 - Unusual and extraordinary threat
#   Article 5: General provisions
#   § 1701. Unusual and extraordinary threat
# 兼容 § 符号（\u00a7）以及部分乱码文本中常见的 \u6402。
# 分组说明：
#   section_symbol: § / \u6402 符号
#   section_number: 符号后的编号
#   section_label: Section / Sec. / Article / Art. 标签词
#   label_number: 标签词后的编号
#   section_heading: 标题文本（分隔符之后的部分）
SECTION_PATTERN = re.compile(
    r"^(?:(?P<section_symbol>[\u00a7\u6402])\s*(?P<section_number>[\dA-Za-z](?:[\dA-Za-z.\-:]*[\dA-Za-z])?)|"
    r"(?P<section_label>Section|Sec\.?|Article|Art\.?)\s*(?P<label_number>[\dA-Za-z](?:[\dA-Za-z.\-:]*[\dA-Za-z])?))"
    r"(?:\s*[.\-\u2013\u2014:]\s*|\s+)(?P<section_heading>.+)$",
    re.IGNORECASE,
)

# BARE_SECTION_PATTERN：匹配只有编号、标题在下一行的情况。
# 例如：
#   Section 1
#   Short title
# 这种情况下第一行只匹配编号，下一行的文本作为标题。
BARE_SECTION_PATTERN = re.compile(
    r"^(?:(?P<section_symbol>[\u00a7\u6402])\s*(?P<section_number>[\dA-Za-z](?:[\dA-Za-z.\-:]*[\dA-Za-z])?)|"
    r"(?P<section_label>Section|Sec\.?|Article|Art\.?)\s*(?P<label_number>[\dA-Za-z](?:[\dA-Za-z.\-:]*[\dA-Za-z])?))\.?$",
    re.IGNORECASE,
)

# =============================================================================
# 正文结束标记
# =============================================================================
# 当扫描到以下标题时，通常说明法条正文已经结束，后续是附录、索引、修订说明等非正文内容。
# 切分器遇到这些标记行会停止收集后续内容，避免把附录等非正文内容当作法条正文。
END_MARKERS = (
    "Appendix",  # 附录
    "Attachment",  # 附件
    "Annex",  # 附件
    "Schedule",  # 附表
    "Table of Contents",  # 目录
    "Historical and Statutory Notes",  # 历史与法规注释
    "Editorial Notes",  # 编辑注释
    "Credits",  # 出处说明
    "References",  # 参考文献
    "References in Text",  # 文中引用说明
    "Codification",  # 法典化说明
    "Amendments",  # 修订记录
    "Effective Date",  # 生效日期说明
    "Index",  # 索引
)

# =============================================================================
# 实体类型别名映射（归一化）
# =============================================================================
# 模型可能输出同义但不同写的实体类型，这里统一归一化为项目内部使用的英文图谱标签。
# 键：模型可能输出的实体类型（去除非字母数字后的小写形式）
# 值：归一化后的标准英文图谱标签
#   Legal Document  = 法规文件（如 Act、Statute、Order）
#   Legal Basis     = 法规依据（当前法规的上位法或制定依据）
#   Legal Provision = 法条（Section / Article 层级的条文）
#   Provision Unit  = 条款单元（法条内部的 subsection / paragraph / clause）
#   Citation        = 引用依据（法条引用的其他法律依据）
ENTITY_TYPE_ALIASES = {
    "legaldocument": "Legal Document",
    "document": "Legal Document",
    "statute": "Legal Document",
    "act": "Legal Document",
    "legalbasis": "Legal Basis",
    "basis": "Legal Basis",
    "authority": "Legal Basis",
    "legalprovision": "Legal Provision",
    "provision": "Legal Provision",
    "section": "Legal Provision",
    "article": "Legal Provision",
    "provisionunit": "Provision Unit",
    "unit": "Provision Unit",
    "subsection": "Provision Unit",
    "paragraph": "Provision Unit",
    "citation": "Citation",
    "reference": "Citation",
    "legalcitation": "Citation",
}

# =============================================================================
# 关系类型别名映射（归一化）
# =============================================================================
# 模型可能输出自然语言描述的关系名，这里统一归一化为英文关系名。
# 键：模型可能输出的关系名（去除非字母数字后的小写形式）
# 值：归一化后的标准大写下划线格式
#   CONTAINS = 包含关系（法规包含法条、法条包含条款单元）
#   BASED_ON = 依据关系（法规依据其上位法制定）
#   CITES    = 引用关系（条款单元引用其他法条或文件）
RELATION_TYPE_ALIASES = {
    "contains": "CONTAINS",
    "contain": "CONTAINS",
    "includes": "CONTAINS",
    "includedin": "CONTAINS",
    "basedon": "BASED_ON",
    "basis": "BASED_ON",
    "pursuantto": "BASED_ON",
    "cites": "CITES",
    "cite": "CITES",
    "references": "CITES",
    "reference": "CITES",
    "refersto": "CITES",
}

# =============================================================================
# 内部引用判断值集合
# =============================================================================
# 当 Citation 节点的 is_internal_reference 属性值为以下任一值时，
# 表示该引用指向当前法规文件内部（如同一个 Act 中的另一个 Section）。
YES_VALUES = {"yes", "y", "true", "internal", "same document", "this document"}

# =============================================================================
# 英文图谱字段中文翻译索引（注释用途，运行时不依赖）
# =============================================================================
# official_title            = 文件全称
# document_number           = 文号/编号
# alias                     = 别名/简称
# document_type             = 文件类型（Act, Code, Regulation, Order 等）
# publication_effective_info = 发布生效信息
# purpose                   = 制定目的
# domain                    = 应用领域（如 sanctions、trade、environment）
# applicable_industry        = 适用行业
# scope_of_application      = 适用范围
# issuing_authority         = 发布单位
# publication_date          = 发布日期
# effective_date            = 生效日期
# status                    = 时效状态（现行有效/已废止等）
# core_topic                = 核心主题（法条核心议题）
# scope_of_effect           = 效力范围（地域/时间/主体/客体范围）
# section_number            = 法条编号（如 Section 1701）
# section_heading           = 法条标题
# provision_text            = 法条全文
# unit_content              = 条款单元内容
# unit_heading              = 条款单元主题
# unit_level                = 单元层级（subsection, paragraph, clause 等）
# unit_number               = 单元编号
# function_type             = 功能类型（obligation, prohibition, authorization 等）
# applicable_subject        = 适用主体
# responsible_role          = 责任角色
# conduct_description       = 行为描述
# condition                 = 适用前提/条件
# legal_consequence         = 法律后果
# exception                 = 例外情形
# time_element              = 时间要素（期限、生效时间、持续时间等）
# quantitative_standard     = 量化标准（数值、阈值、比例、金额等）
# other_information         = 其他信息
# citation_type             = 引用类型（Document 全文引用 / Provision 具体条款引用）
# provision_number          = 被引用条款编号
# is_internal_reference     = 是否本文件内引用（Yes/No）
# citation_relation         = 引用关系（pursuant to / subject to / defined in 等）
# citation_purpose          = 引用目的（authority, exception, definition, penalty 等）


class ResultStats:
    """抽取过程的错误与警告统计。

    字段名沿用中文抽取器的结构，便于服务层统一调用和展示。
    - error: 错误计数（影响最终结果的严重问题）
    - error_msg: 错误详情（累积拼接）
    - week_warning: 弱警告计数（轻微问题，不影响最终结果）
    - week_warning_msg: 弱警告详情
    - strong_warning: 强警告计数（较严重但不阻断流程的问题）
    - strong_warning_msg: 强警告详情
    """

    def __init__(self):
        self.error = 0
        self.error_msg = ""
        self.week_warning = 0
        self.week_warning_msg = ""
        self.strong_warning = 0
        self.strong_warning_msg = ""


class ClauseCache:
    """单次文件抽取过程中的临时缓存。

    用于在整个文件的抽取流程中共享文件信息和各法条结果：
    - file_info: 文件头信息抽取结果（Legal Document 节点 + Legal Basis 列表）
    - clause_cache: 各法条抽取结果字典，键为 section_number（如 "Section 1701"），
      值为该法条的完整抽取结果（包括节点信息和条款单元列表）
    """

    def __init__(self):
        self.file_info = {}  # 文件头信息抽取结果
        self.clause_cache = {}  # section_number -> clause_result 的映射


# =============================================================================
# 文本清洗工具函数
# =============================================================================

def clean_string(text: str) -> str:
    """清洗文本：去掉零宽字符、合并空白，并修正常见的 Section 符号乱码。

    处理步骤：
    1. 将 \u6402（常见乱码）替换为 "Section"
    2. 替换全角空格为标准半角空格
    3. 去除零宽字符（如零宽空格，BOM 等）
    4. 统一换行符为 \n
    5. 合并连续空行为单行
    6. 去除每行首尾的空白字符
    7. 将所有连续空白合并为一个空格

    Args:
        text: 原始文本，可能为 None

    Returns:
        清洗后的文本字符串。如果输入为 None，返回空字符串。
    """
    if text is None:
        return ""
    cleaned = str(text)
    # \u6402 是常见的 § 符号乱码，在英文法律文本中表示 Section
    cleaned = cleaned.replace("\u6402", "Section")
    # 替换全角空格（中文空格）为标准半角空格
    cleaned = replace_full_corner_space(cleaned)
    # 去除零宽字符（零宽空格、零宽连接符等不可见字符）
    cleaned = replace_zero_width_chars(cleaned)
    # 统一换行符：\r\n（Windows）和 \r（老 Mac）都转为 \n（Unix）
    cleaned = re.sub(r"\r\n|\r", "\n", cleaned)
    # 合并连续空行为单行
    cleaned = re.sub(r"\n+", "\n", cleaned)
    # 去除每行首尾的空白字符（空格和 tab）
    cleaned = re.sub(r"^[ \t]+|[ \t]+$", "", cleaned, flags=re.MULTILINE)
    # 将所有连续空白（空格、tab 等）合并为一个空格
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def clean_line(line: str) -> str:
    """清洗单行前缀，去掉 Markdown 标题符号、列表符号等不属于正文层级的字符。

    例如：
    - "# Title" → "Title"（去掉 Markdown 标题符号）
    - "- item"  → "item"（去掉无序列表符号）
    - "* item"  → "item"（去掉星号列表符号）
    - "   text" → "text"（去掉前导空白）

    Args:
        line: 待清洗的单行文本

    Returns:
        去除前导空白和 Markdown/列表符号后的文本
    """
    return line.strip().lstrip(" \t\r\n\f\v#-*")


def normalize_text_key(value: str) -> str:
    """生成宽松匹配用的文本键，主要用于内部引用和节点名匹配。

    处理步骤：
    1. 将 § 符号和 \u6402 乱码统一替换为 " Section "（带空格，防止粘连）
    2. 将 "sec." / "secs." 缩写统一为 "section"
    3. 将 "sections" 复数统一为 "section" 单数
    4. 只保留字母、数字和空格，其余字符移除
    5. 转小写
    6. 合并连续空白为单空格

    这种规范化的文本键用于在两个不同来源的文本之间进行模糊匹配，
    例如将 Citation 中引用的 "Sec. 1702" 与已有的 "Section 1702" 节点名匹配。

    Args:
        value: 原始文本值，可能为 None

    Returns:
        规范化后的文本键，用于宽松匹配。如果输入为 None，返回空字符串。
    """
    if value is None:
        return ""
    normalized = str(value)
    # § 符号（\u00a7）替换为 " Section "
    normalized = normalized.replace("\u00a7", " Section ")
    # \u6402 乱码替换为 " Section "
    normalized = normalized.replace("\u6402", " Section ")
    # "sec." / "secs." 缩写统一为 "section"
    normalized = re.sub(r"\bsecs?\.", "section", normalized, flags=re.IGNORECASE)
    # "sections" 复数统一为 "section"
    normalized = re.sub(r"\bsections\b", "section", normalized, flags=re.IGNORECASE)
    # 只保留字母、数字和空格
    normalized = clean_string_with_only_words(normalized).lower()
    # 合并连续空白
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def canonical_type(entity_type: str) -> str:
    """将模型输出的实体类型归一化为固定英文图谱标签。

    模型可能输出各种变体（如 "legalprovision"、"Provision"、"section"），
    此函数将其统一为标准的图谱标签（如 "Legal Provision"）。

    归一化步骤：
    1. 去掉输入中的所有非字母数字字符
    2. 转小写
    3. 在 ENTITY_TYPE_ALIASES 映射表中查找
    4. 如果找到匹配，返回标准标签；否则返回原值

    Args:
        entity_type: 模型输出的实体类型字符串

    Returns:
        归一化后的标准英文图谱标签
    """
    # 去掉所有非字母数字字符后转小写，作为查找键
    type_key = re.sub(r"[^a-z0-9]+", "", str(entity_type or "").lower())
    # 在映射表中查找，找不到则返回原值
    return ENTITY_TYPE_ALIASES.get(type_key, entity_type or "")


def canonical_relation_type(relation_type: str) -> str:
    """将模型输出的关系类型归一化为固定英文关系名。

    模型可能输出 "contains"、"based on"、"pursuant to" 等不同形式，
    此函数统一为大写下划线格式如 "CONTAINS"、"BASED_ON"。

    归一化步骤：
    1. 去掉非字母数字字符后转小写，在 RELATION_TYPE_ALIASES 中查找
    2. 如果找到，返回标准关系名
    3. 否则将原值转大写、空格替换为下划线

    Args:
        relation_type: 模型输出的关系类型字符串

    Returns:
        归一化后的英文关系名（大写下划线格式）
    """
    relation_key = re.sub(r"[^a-z0-9]+", "", str(relation_type or "").lower())
    if relation_key in RELATION_TYPE_ALIASES:
        return RELATION_TYPE_ALIASES[relation_key]
    # 兜底：转大写并以空格转下划线
    return str(relation_type or "").strip().upper().replace(" ", "_")


def is_internal_reference(properties: dict) -> bool:
    """根据 Citation 属性判断该引用是否指向当前英文法规文件内部。

    检查多个可能的属性名（模型可能输出不同的属性名）：
    - is_internal_reference: 标准属性名
    - internal_reference: 简写变体
    - same_document: 同文件引用变体

    如果任一属性值在 YES_VALUES 集合中（如 "yes", "true", "internal"），
    则判定为内部引用。

    Args:
        properties: Citation 节点的属性字典

    Returns:
        True 表示该引用指向当前文件内部；False 表示外部引用
    """
    value = (
        properties.get("is_internal_reference")
        or properties.get("internal_reference")
        or properties.get("same_document")
        or ""  # 都取不到时默认为空字符串
    )
    return str(value).strip().lower() in YES_VALUES


# =============================================================================
# 英文法律文本切分函数
# =============================================================================

def match_hierarchy(line: str) -> Optional[tuple[str, str]]:
    """识别一行是否是 Title / Chapter / Part 等英文法规层级标题。

    依次尝试 HIERARCHY_PATTERNS 中的每个正则模式，
    匹配成功则返回层级名和标题值。

    Args:
        line: 待匹配的单行文本

    Returns:
        如果匹配成功，返回 (层级名, 层级标题值) 元组；
        如果不匹配任何层级模式，返回 None。
    """
    for level, pattern in HIERARCHY_PATTERNS:
        match = pattern.match(line)
        if match:
            return level, match.group(1).strip()
    return None


def match_section(line: str) -> Optional[re.Match]:
    """识别一行是否是 Section / Article 法条标题。

    先尝试完整匹配（带标题的法条行，如 "Section 1701. Unusual threat"），
    如果失败再尝试仅匹配编号（标题在下一行的情况，如 "Section 1701" 后跟标题行）。

    Args:
        line: 待匹配的单行文本

    Returns:
        如果匹配成功，返回正则匹配对象（Match）；
        如果不匹配任何模式，返回 None。
    """
    # 先尝试匹配带标题的完整法条行
    section_match = SECTION_PATTERN.match(line)
    if section_match:
        return section_match
    # 再尝试匹配仅有编号的法条行（标题在下一行）
    return BARE_SECTION_PATTERN.match(line)


def update_hierarchy(hierarchy: dict, level: str, value: str):
    """更新当前层级状态，并清空该层级以下的子层级。

    例如，当遇到一个新的 Chapter 标题时，需要将当前层级设置为新 Chapter，
    同时将 subchapter、part、subpart、division、article 等子层级清空，
    因为这些属于上一个 Chapter 的上下文，不应串到新 Chapter 中。

    Args:
        hierarchy: 层级状态字典，键为层级名，值为层级标题文本
        level: 当前匹配到的层级名（如 "chapter"）
        value: 当前层级的标题值（如 "Chapter 35. International Emergency..."）
    """
    hierarchy[level] = value
    # 找到当前层级在 HIERARCHY_LEVELS 中的位置
    level_idx = HIERARCHY_LEVELS.index(level)
    # 清空该层级之后的所有子层级
    for child_level in HIERARCHY_LEVELS[level_idx + 1:]:
        hierarchy[child_level] = ""


def build_clause(
        hierarchy: dict,
        section_number: str,
        section_heading: str,
        clause_content: str
) -> dict:
    """将当前层级、法条编号、标题和正文组装为切分后的法条对象。

    组装后的字典结构如下：
    {
        "title": "Title 50. War and National Defense",
        "subtitle": "",
        "chapter": "Chapter 35. International Emergency Economic Powers",
        ...,
        "classification": {"title": "...", "chapter": "..."},  # 仅包含非空层级
        "section_number": "Section 1701",
        "section_heading": "Unusual and extraordinary threat",
        "clause_content": "实际法条正文（不含编号行）"
    }

    Args:
        hierarchy: 当前生效的层级状态字典
        section_number: 解析后的标准化法条编号（如 "Section 1701"）
        section_heading: 法条标题文本
        clause_content: 法条正文内容

    Returns:
        组装后的法条对象字典
    """
    # 构建 classification：仅保留非空的层级值
    classification = {
        key: clean_string(value)
        for key, value in hierarchy.items()
        if value  # 过滤掉空字符串的层级
    }
    return {
        "title": classification.get("title", ""),
        "subtitle": classification.get("subtitle", ""),
        "chapter": classification.get("chapter", ""),
        "subchapter": classification.get("subchapter", ""),
        "part": classification.get("part", ""),
        "subpart": classification.get("subpart", ""),
        "division": classification.get("division", ""),
        "article": classification.get("article", ""),
        "classification": classification,
        "section_number": clean_string(section_number),
        "section_heading": clean_string(section_heading),
        "clause_content": clean_string(clause_content),
    }


def parse_section_number(section_match: re.Match) -> str:
    """把原文中的 § / Sec. / Section / Article 编号统一为英文规范格式。

    - 如果是 § 符号匹配的，统一输出 "Section {编号}"
    - 如果标签词以 "art" 开头（如 Article / Art.），统一输出 "Article {编号}"
    - 否则（Section / Sec. 等），统一输出 "Section {编号}"

    Args:
        section_match: match_section 返回的正则匹配对象

    Returns:
        标准化后的法条编号字符串（如 "Section 1701"、"Article 5"）
    """
    # 情况1：§ 或 \u6402 符号匹配
    if section_match.groupdict().get("section_symbol"):
        return f"Section {section_match.group('section_number')}"

    # 情况2：Section / Article 标签词匹配
    label = section_match.group("section_label") or "Section"
    number = section_match.group("label_number")
    if label.lower().startswith("art"):  # Article / Art. 等
        return f"Article {number}"
    return f"Section {number}"


def split_clause(text: str) -> dict:
    """
    将英文法律文本切分为"文件头信息 + 多个 Section/Article 法条"。

    整体切分算法：
    1. 将文本按行拆分
    2. 扫描第一遍：找到第一个 Section/Article 标题行，其前的所有内容作为文件头信息；
       同时在此过程中累积层级信息（Title、Chapter 等）
    3. 扫描第二遍：从第一个法条行开始逐行处理：
       - 遇到新的 Section/Article → 保存前一个法条，开始新法条
       - 遇到新的层级标题（Title/Chapter 等）→ 保存前一个法条，更新层级
       - 遇到结束标记（Appendix 等）→ 停止收集
       - 其他行 → 追加到当前法条正文
    4. 保存最后一个法条
    5. 清洗并返回结果

    输出结构：
    {
        "file_info": "文件头文本",
        "clauses": [
            {
                "title": "Title 50. War and National Defense",
                "chapter": "Chapter 35. International Emergency Economic Powers",
                "classification": {...},
                "section_number": "Section 1701",
                "section_heading": "Unusual and extraordinary threat",
                "clause_content": "去掉法条编号行后的正文内容"
            }
        ]
    }

    Args:
        text: 英文法律文本全文

    Returns:
        包含 file_info 和 clauses 的字典

    Raises:
        ValueError: 如果文本中找不到任何 Section/Article 标题，或切分后无任何法条内容
    """
    # 统一换行符后按行拆分
    lines = text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    # 初始化层级状态：所有层级默认为空字符串
    hierarchy = {level: "" for level in HIERARCHY_LEVELS}
    clauses = []  # 切分结果列表

    # --- 第一遍扫描：定位第一个法条并累积前置层级 ---
    first_section_idx = None  # 第一个 Section/Article 所在的行索引
    for i, raw_line in enumerate(lines):
        line = clean_line(raw_line)
        if not line:
            continue

        # 如果匹配到 Section/Article 标题，记录位置并退出
        if match_section(line):
            first_section_idx = i
            break

        # 如果匹配到层级标题，更新当前层级状态
        hierarchy_match = match_hierarchy(line)
        if hierarchy_match:
            level, value = hierarchy_match
            update_hierarchy(hierarchy, level, value)

    # 如果整个文本中找不到任何 Section/Article 标题，无法切分
    if first_section_idx is None:
        raise ValueError("No English section or article heading found in text")

    # 第一个法条之前的所有行作为文件头信息
    file_info = "\n".join(lines[:first_section_idx]).rstrip()

    # 初始化法条切分的状态变量
    current_hierarchy = hierarchy.copy()  # 复制当前层级状态（后续可能被新增层级更新）
    current_section_number = ""  # 当前法条的标准化编号
    current_section_heading = ""  # 当前法条的标题
    current_clause_content = ""  # 当前法条的正文内容（累积）

    def append_current_clause():
        """内部函数：将当前累积的法条信息保存到 clauses 列表。

        - 如果没有 section_number，说明还没有开始收集法条，直接跳过
        - 如果正文为空但标题不为空，用标题作为正文内容
        """
        if not current_section_number:
            return
        content = current_clause_content.strip()
        # 特殊情况：如果正文为空但有标题（如仅有编号行的法条），用标题作为正文
        if not content and current_section_heading:
            content = current_section_heading
        clauses.append(
            build_clause(
                hierarchy=current_hierarchy,
                section_number=current_section_number,
                section_heading=current_section_heading,
                clause_content=content,
            )
        )

    # --- 第二遍扫描：从第一个法条开始逐行处理 ---
    for raw_line in lines[first_section_idx:]:
        line = clean_line(raw_line)
        if not line:
            continue  # 跳过空行

        # 检查是否是正文结束标记（如 Appendix / Schedule / Index）
        if any(line.startswith(marker) for marker in END_MARKERS):
            append_current_clause()
            # 清空状态，停止继续收集
            current_section_number = ""
            current_section_heading = ""
            current_clause_content = ""
            break  # 遇到结束标记，立即停止处理后续行

        # 检查是否是新的 Section/Article 标题行
        section_match = match_section(line)
        if section_match:
            # 保存前一个法条
            append_current_clause()
            # 解析新法条的编号和标题
            current_section_number = parse_section_number(section_match)
            current_section_heading = (section_match.groupdict().get("section_heading") or "").strip()
            # 正文初始为标题文本（当正文为空时会用标题兜底）
            current_clause_content = current_section_heading
            continue

        # 检查是否是新的层级标题（Title / Chapter 等）
        hierarchy_match = match_hierarchy(line)
        if hierarchy_match:
            # 保存前一个法条（因为层级变更，旧的 Chapter 下不会再有新法条）
            append_current_clause()
            # 清空法条状态
            current_section_number = ""
            current_section_heading = ""
            current_clause_content = ""
            # 更新层级（会清空子层级）
            level, value = hierarchy_match
            update_hierarchy(current_hierarchy, level, value)
            continue

        # 普通正文行：追加到当前法条正文
        if current_section_number:
            # 特殊情况：如果还没有标题，说明法条编号出现在单独一行，
            # 当前这行可能就是标题
            if not current_section_heading:
                current_section_heading = line
                current_clause_content = line
            else:
                # 正常情况：将原始行内容追加到正文（保留原始格式）
                current_clause_content += "\n" + raw_line.strip()

    # 保存最后一个法条（循环结束后仍有一个未保存的法条）
    append_current_clause()

    if not clauses:
        raise ValueError("No English clause content found in text")

    # 清洗文件头信息
    file_info = clean_string(file_info)
    # 注意：第一个法条的正文内容也会被加入文件头，因为文件头末尾和第一个法条之间
    # 可能包含了与法条开篇相关的过渡文本
    if clauses:
        file_info = clean_string(f"{file_info}\n{clauses[0]['clause_content']}")

    return {
        "file_info": file_info,
        "clauses": clauses,
    }


# =============================================================================
# 文件 I/O 辅助函数
# =============================================================================

def save_clause_result(result: dict, output_dir: str, filename: str) -> str:
    """保存切分结果到 JSON 文件，主要供调试或离线预处理使用。

    生成的文件名为 "{原文件名（不含扩展名）}_clauses.json"。

    Args:
        result: split_clause 返回的切分结果字典
        output_dir: 输出目录路径
        filename: 原始文件名（用于生成输出文件名）

    Returns:
        保存的 JSON 文件路径
    """
    os.makedirs(output_dir, exist_ok=True)  # 确保输出目录存在
    base_name = os.path.splitext(os.path.basename(filename))[0]
    output_path = os.path.join(output_dir, f"{base_name}_clauses.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    return output_path


def split_clause_file(
        input_path: str,
        output_dir: str = os.path.join("data", "temp")
) -> tuple[dict, str]:
    """读取本地英文法律文本文件并保存切分结果。

    这是 split_clause + save_clause_result 的组合包装，
    方便直接处理本地文件。

    Args:
        input_path: 输入文件路径（UTF-8 编码的英文法律文本）
        output_dir: 输出目录路径，默认为 data/temp

    Returns:
        (切分结果字典, 输出 JSON 文件路径) 元组
    """
    with open(input_path, "r", encoding="utf-8") as f:
        text = f.read()
    result = split_clause(text)
    output_path = save_clause_result(result, output_dir, input_path)
    return result, output_path


# =============================================================================
# 英文法条知识图谱抽取器（核心类）
# =============================================================================

class ClauseEnExtractor:
    """
    英文法条知识图谱抽取器。

    与中文 `ClauseExtractor` 区分命名，便于服务层同时挂载中文法条抽取和英文法条抽取。

    核心流程：
    1. split_clause: 将英文字律文本按 Section/Article 切分为法条列表
    2. kg_extract_from_file_info: 从文件头抽取 Legal Document 和 Legal Basis
    3. kg_extract_from_clause: 并发抽取每个法条的 Legal Provision / Provision Unit / Citation
    4. process_extracted_data: 将所有抽取结果组装为最终图谱（nodes + edges）

    兜底策略：
    默认开启 `fallback_on_llm_failure`，当本地环境缺少 LLM 依赖或模型调用失败时，
    会使用确定性规则兜底，至少生成英文 Legal Document / Legal Provision / Provision Unit 图谱。
    """

    def __init__(
            self,
            max_concurrent: int = 50,
            fallback_on_llm_failure: bool = True,
    ):
        """
        初始化英文法条抽取器。

        Args:
            max_concurrent: 最大并发法条抽取数。通过 asyncio.Semaphore 控制，
                           避免同时发起过多 LLM 请求导致 OOM 或 API 限流。
            fallback_on_llm_failure: 是否在 LLM 调用失败时使用规则兜底。
                                     True 时即使模型不可用也能生成基础图谱；
                                     False 时 LLM 失败会直接抛出异常。
        """
        # Langextract 配置对象，惰性初始化
        self.extractor_config = None
        # Langextract 抽取器实例，惰性初始化
        self.extractor = None
        # 并发控制信号量：限制同时进行的法条 LLM 抽取任务数
        self.semaphore = asyncio.Semaphore(max_concurrent)
        # 错误/警告统计对象
        self.result_stats = ResultStats()
        # 宽松模式：True 时即使部分法条失败也继续返回已成功的部分
        # （当前版本未从外部设置，保留作为未来扩展点）
        self.lenient_mode = False
        # LLM 失败时是否使用规则兜底
        self.fallback_on_llm_failure = fallback_on_llm_failure

    def _get_extractor(self):
        """惰性加载 Langextract 抽取器实例。

        使用惰性加载的原因：避免仅使用 split_clause（纯文本切分）时也
        必须安装完整的 LLM 依赖包（如 langextract 库）。只有当真正需要
        调用 LLM 进行实体关系抽取时才初始化和加载。

        Returns:
            配置好的 Langextract 抽取器实例
        """
        if self.extractor is None:
            # 惰性导入：避免非 LLM 使用场景下的依赖冲突
            from app.infrastructure.information_extraction.factory import InformationExtractionFactory
            from app.infrastructure.information_extraction.method.base import LangextractConfig

            # 使用模块级环境变量配置（首次调用时创建）
            if self.extractor_config is None:
                self.extractor_config = LangextractConfig(
                    model_name=LAW_EN_CLAUSE_MODEL,
                    api_key=LAW_EN_CLAUSE_MODEL_API_KEY,
                    api_url=LAW_EN_CLAUSE_MODEL_API_URL,
                    config={
                        "timeout": TIMEOUT,  # 单次 LLM 调用超时（秒）
                    },
                    max_char_buffer=MAX_CHAR_BUFFER,  # 最大输入文本长度
                    batch_length=BATCH_LENGTH,  # 批处理大小
                    max_workers=MAX_WORKERS,  # 内部 worker 并发数
                )

            # 通过工厂方法创建 Langextract 抽取器
            # max_retries=5: LLM 调用失败时的最大重试次数
            self.extractor = InformationExtractionFactory.create(
                "langextract",
                max_retries=5,
                config=self.extractor_config,
            )
        return self.extractor

    # =========================================================================
    # 顶层抽取入口
    # =========================================================================

    async def extract_clauses(
            self,
            filename: str,
            text: str
    ) -> dict:
        """
        抽取单个英文法规文件的英文法条知识图谱。

        这是外部调用的主入口方法，完成完整的抽取流水线：
        1. 文本切分（split_clause）
        2. 文件头信息抽取（kg_extract_from_file_info）
        3. 所有法条并发抽取（kg_extract_from_clause）
        4. 抽取结果组装（process_extracted_data）

        输出约定：
        - nodes: 英文节点列表，每个节点含 node_id / node_name / node_type / properties
        - edges: 英文关系列表，每个关系含 source_id / target_id / relation_type
        - 所有类型名和属性名均为英文

        Args:
            filename: 原始文件名（用于日志追踪和输出关联）
            text: 英文法规文件全文

        Returns:
            最终图谱字典 {"nodes": [...], "edges": [...]}

        Raises:
            ValueError: 文件信息为空、法条数据为空、或非宽松模式下部分法条失败
        """
        try:
            logging.info("Starting English legal provision graph extraction")
            # --- 步骤1：切分文本 ---
            clauses_data = await self.split_clause(text)

            # --- 步骤2：抽取文件头信息 ---
            clause_cache = ClauseCache()
            file_info = clauses_data.get("file_info")
            if not file_info:
                self.result_stats.error += 1
                self.result_stats.error_msg += "File information is empty\n"
                raise ValueError("File information is empty")

            file_info_result = await self.kg_extract_from_file_info(
                filename=filename,
                clause_cache=clause_cache,
                file_info=file_info,
            )

            # --- 步骤3：并发抽取所有法条 ---
            clauses = clauses_data.get("clauses")
            if not clauses:
                self.result_stats.error += 1
                self.result_stats.error_msg += "Clause data is empty\n"
                raise ValueError("Clause data is empty")

            # 为每个法条创建异步抽取任务
            tasks = [
                self.kg_extract_from_clause(
                    filename=filename,
                    clause_cache=clause_cache,
                    one_clause=clause,
                )
                for clause in clauses
            ]
            # 并发执行所有任务，return_exceptions=True 避免单个失败导致全部取消
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # --- 步骤4：分类处理结果 ---
            failed_results = []  # 抛出异常的任务结果
            failed_clauses = []  # 失败的法条原文
            successful_results = []  # 成功抽取的结果
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    # 任务抛出了异常
                    self.result_stats.error += 1
                    self.result_stats.error_msg += f"Clause {i + 1} failed: {result}\n"
                    failed_results.append((i + 1, result))
                    failed_clauses.append(clauses[i])
                elif result:
                    # 成功返回非空结果
                    successful_results.append(result)
                else:
                    # 返回了空结果（可能被内部兜底处理返回了空字典）
                    self.result_stats.error += 1
                    self.result_stats.error_msg += f"Clause {i + 1} returned empty result\n"
                    failed_clauses.append(clauses[i])

            # 非宽松模式下，任何失败都抛出异常
            if failed_results and not self.lenient_mode:
                raise ValueError("Some English legal provisions failed to process")

            # --- 步骤5：组装最终图谱 ---
            return await self.process_extracted_data(
                filename=filename,
                extracted_file_info=file_info_result,
                extracted_success_clauses=successful_results,
                extracted_failed_clauses=failed_clauses,
            )
        except Exception as e:
            logging.error("English legal provision graph extraction failed: %s", e)
            raise

    async def split_clause(self, text: str) -> dict:
        """异步包装：切分英文法规文本。

        将同步的 split_clause 函数包装为异步方法，方便在异步流水线中调用。
        当前实现直接调用同步函数，如果文本很大且需要非阻塞执行，
        可改为 run_in_executor。

        Args:
            text: 英文法律文本全文

        Returns:
            切分结果字典 {"file_info": ..., "clauses": [...]}
        """
        return split_clause(text)

    async def split_clause_to_json(
            self,
            input_path: str,
            output_dir: str = os.path.join("data", "temp")
    ) -> dict:
        """异步包装：切分英文法规文件并保存为 JSON。

        用于离线预处理场景：将英文法规 TXT 文件预切分为 JSON，
        后续抽取时直接读取切分好的 JSON 文件提高效率。

        Args:
            input_path: 输入文件路径
            output_dir: 输出目录

        Returns:
            包含 output_path 和切分结果的字典
        """
        result, output_path = split_clause_file(input_path, output_dir)
        return {
            "output_path": output_path,
            **result,
        }

    # =========================================================================
    # 文件头信息抽取
    # =========================================================================

    async def kg_extract_from_file_info(
            self,
            filename: str,
            clause_cache: ClauseCache,
            file_info: str
    ) -> dict:
        """
        从文件头信息中抽取 Legal Document（法规文件）和 Legal Basis（法规依据）。

        使用 prompt_for_file_info + schema_for_file_info + example_for_file_info
        作为 LLM 的提示词、格式定义和示例。

        抽取结果结构：
        {
            "node_id": "Legal_Document_xxx",  # Legal Document 节点 ID
            "node_name": "Executive Order 14024",  # 法规标题
            "node_type": "Legal Document",  # 节点类型
            "properties": {...},  # 法规属性（发布单位、日期、适用范围等）
            "legal_basis": [  # 法规依据列表
                {
                    "node_id": "Legal_Basis_xxx",
                    "node_name": "International Emergency Economic Powers Act",
                    "node_type": "Legal Basis",
                    "properties": {...}
                }
            ]
        }

        如果模型没有抽到 Legal Document，且开启兜底模式，则根据文件名和
        文件头文本生成基础节点。

        Args:
            filename: 原始文件名
            clause_cache: 缓存对象，抽取结果会写入 clause_cache.file_info
            file_info: 切分后的文件头文本

        Returns:
            文件信息抽取结果字典
        """
        try:
            # 初始化结果结构
            file_info_result = {
                "node_id": "",  # Legal Document 节点 ID
                "node_name": "",  # Legal Document 名称
                "node_type": "",  # Legal Document 类型
                "properties": {},  # Legal Document 属性
                "legal_basis": [],  # Legal Basis 列表
            }

            # 调用 LLM 进行实体关系抽取
            # input_text 包含文件名和文件头描述，帮助模型理解上下文
            extract_result = await self._get_extractor().entity_and_relationship_extract(
                user_prompt=prompt_for_file_info,  # 用户提示词（角色 + 任务）
                schema=schema_for_file_info,  # Schema 定义（实体和关系结构）
                input_text=f"{filename} document description:\n{file_info}",  # 输入文本
                examples=example_for_file_info,  # Few-shot 示例
            )

            entities = extract_result.get("entities", []) if extract_result else []
            file_info_processed = False  # 标记：是否已经处理了 Legal Document

            # --- 遍历 LLM 返回的实体 ---
            for entity in entities:
                # 类型校验：确保是 Entity 对象
                if not isinstance(entity, Entity):
                    self.result_stats.error += 1
                    self.result_stats.error_msg += f"Invalid entity type: {entity}\n"
                    continue

                # 归一化实体类型
                entity_type = canonical_type(entity.entity_type)

                if entity_type == "Legal Document":
                    # 每个文件只能有一个 Legal Document，重复的出现视为强警告
                    if file_info_processed:
                        self.result_stats.strong_warning += 1
                        self.result_stats.strong_warning_msg += (
                            f"Only one Legal Document is expected in file info: {entity}\n"
                        )
                        continue
                    node_name = clean_string(entity.name)
                    if not node_name:
                        continue
                    # 填充 Legal Document 节点信息
                    file_info_result["node_id"] = self._new_node_id(entity_type)
                    file_info_result["node_name"] = node_name
                    file_info_result["node_type"] = entity_type
                    file_info_result["properties"] = entity.properties
                    file_info_processed = True

                elif entity_type == "Legal Basis":
                    # 法规依据可以有多个（如同时依据 A Act 和 B Act）
                    node_name = clean_string(entity.name)
                    if not node_name:
                        continue
                    file_info_result["legal_basis"].append(
                        {
                            "node_id": self._new_node_id(entity_type),
                            "node_name": node_name,
                            "node_type": entity_type,
                            "properties": entity.properties,
                        }
                    )

                elif entity_type:
                    # 未知的实体类型 → 强警告
                    self.result_stats.strong_warning += 1
                    self.result_stats.strong_warning_msg += f"Unknown file-info entity type: {entity}\n"

            # 如果没有抽到 Legal Document 节点
            if not file_info_result["node_id"]:
                if not self.fallback_on_llm_failure:
                    raise ValueError("Legal Document entity was not extracted")
                # 规则兜底：从文件名和文件头推断
                file_info_result = self._fallback_file_info(filename, file_info)

            # 写入缓存
            clause_cache.file_info = file_info_result
            return file_info_result

        except Exception as e:
            # 异常兜底
            if self.fallback_on_llm_failure:
                logging.warning("Falling back to deterministic file-info extraction: %s", e)
                file_info_result = self._fallback_file_info(filename, file_info)
                clause_cache.file_info = file_info_result
                return file_info_result
            # 不兜底则记录错误并重新抛出
            self.result_stats.error += 1
            self.result_stats.error_msg += f"File-info extraction failed: {e}\n{file_info[:500]}\n"
            raise ValueError(f"File-info extraction failed: {e}") from e

    # =========================================================================
    # 单条法条抽取
    # =========================================================================

    async def kg_extract_from_clause(
            self,
            filename: str,
            clause_cache: ClauseCache,
            one_clause: dict
    ) -> dict:
        """
        从单个 Section / Article 中抽取图谱实体和关系。

        这是并发执行的核心方法，每个法条的抽取通过 semaphore 控制并发数。

        抽取目标：
        - Legal Provision：法条节点（每个 Section/Article 一个）
        - Provision Unit：条款单元（法条内部的 subsection/paragraph 等）
        - Citation：引用依据（法条中引用的其他文件或条款）
        - CITES 关系：条款单元到引用依据的引用关系
        - CONTAINS 关系：法条包含条款单元（在 process_extracted_data 阶段添加）

        抽取结果结构：
        {
            "node_id": "Legal_Provision_xxx",
            "node_name": "Section 1701",
            "node_type": "Legal Provision",
            "properties": {...},  # 含 section_number, section_heading, provision_text 等
            "provision_units": [
                {
                    "node_id": "Provision_Unit_xxx",
                    "node_name": "Section 1701 subsection (a)",
                    "node_type": "Provision Unit",
                    "properties": {...},
                    "internal_citations": [...],  # 指向当前文件内部的引用
                    "external_citations": [...],  # 指向外部文件的引用
                }
            ]
        }

        Args:
            filename: 原始文件名
            clause_cache: 缓存对象，抽取结果会写入 clause_cache.clause_cache
            one_clause: split_clause 切分出的单条法条字典

        Returns:
            单条法条的抽取结果字典
        """
        # 通过信号量控制并发数
        async with self.semaphore:
            try:
                # 初始化结果结构
                clause_result = {
                    "node_id": "",
                    "node_name": "",
                    "node_type": "",
                    "properties": {},
                    "provision_units": [],
                }

                # 取法条编号、标题和正文
                section_number = one_clause.get("section_number", "")
                section_heading = one_clause.get("section_heading", "")
                clause_content = one_clause.get("clause_content", "")
                if not section_number:
                    raise ValueError(f"Section number is empty: {one_clause}")

                # 拼装发送给 LLM 的上下文文本（含文件名、层级、编号、正文）
                extract_content = self._build_clause_extract_content(filename, one_clause)

                # 调用 LLM 抽取实体和关系
                extract_result = await self._get_extractor().entity_and_relationship_extract(
                    user_prompt=prompt_for_clause,
                    schema=schema_for_clause,
                    input_text=extract_content,
                    examples=example_for_clause,
                )

                # 临时字典：用于按类型+名称索引和管理临时实体
                clause_units = {}  # key -> Provision Unit 临时节点
                citations = {}  # key -> Citation 临时节点
                processed_citation_keys = set()  # 已被 CITES 关系使用的 Citation key

                entities = extract_result.get("entities", []) if extract_result else []
                relations = extract_result.get("relations", []) if extract_result else []
                provision_processed = False  # 标记是否已处理 Legal Provision

                # --- 遍历 LLM 返回的实体 ---
                for entity in entities:
                    if not isinstance(entity, Entity):
                        self.result_stats.error += 1
                        self.result_stats.error_msg += f"Invalid entity type: {entity}\n"
                        continue

                    entity_type = canonical_type(entity.entity_type)
                    node_name = clean_string(entity.name)
                    if not node_name:
                        continue

                    if entity_type == "Legal Provision":
                        # 每个法条只有一个 Legal Provision，多余的视为强警告
                        if provision_processed:
                            self.result_stats.strong_warning += 1
                            self.result_stats.strong_warning_msg += (
                                f"Only one Legal Provision is expected per section: {entity}\n"
                            )
                            continue
                        # 填充 Legal Provision 节点
                        clause_result["node_id"] = self._new_node_id(entity_type)
                        # 注意：名称使用标准化的 section_number（如 "Section 1701"）而非模型返回的 name
                        clause_result["node_name"] = section_number
                        clause_result["node_type"] = entity_type
                        clause_result["properties"] = entity.properties
                        # 补充切分层面的元信息（法条编号、标题、全文）
                        clause_result["properties"]["section_number"] = section_number
                        clause_result["properties"]["section_heading"] = section_heading
                        clause_result["properties"]["provision_text"] = clause_content
                        # 写入层级信息（Title / Chapter 等）
                        self._add_classification_properties(clause_result["properties"], one_clause)
                        provision_processed = True

                    elif entity_type == "Provision Unit":
                        # 条款单元：法条内部的细分段落（subsection 等）
                        node_id = self._new_node_id(entity_type)
                        # 构建唯一键：类型 + 名称
                        unit_key = f"{entity_type}_{node_name}"
                        clause_units[unit_key] = {
                            "node_id": node_id,
                            "node_name": node_name,
                            "node_type": entity_type,
                            "properties": entity.properties,
                            "internal_citations": [],  # 内部引用列表（后续由 CITES 关系填充）
                            "external_citations": [],  # 外部引用列表
                        }

                    elif entity_type == "Citation":
                        # 引用依据：被条款单元引用的其他法条或文件
                        node_id = self._new_node_id(entity_type)
                        citation_key = f"{entity_type}_{node_name}"
                        citations[citation_key] = {
                            "node_id": node_id,
                            "node_name": node_name,
                            "node_type": entity_type,
                            "properties": entity.properties,
                        }

                    elif entity_type:
                        # 未知实体类型 → 错误
                        self.result_stats.error += 1
                        self.result_stats.error_msg += f"Unknown clause entity type: {entity}\n"

                # --- 遍历 LLM 返回的关系 ---
                for relation in relations:
                    if not isinstance(relation, Relationship):
                        self.result_stats.error += 1
                        self.result_stats.error_msg += f"Invalid relation type: {relation}\n"
                        continue

                    # 归一化关系类型
                    relation_type = canonical_relation_type(relation.type)
                    # 当前阶段只处理 CITES 关系（CONTAINS 关系在 process_extracted_data 阶段统一添加）
                    if relation_type != "CITES":
                        continue

                    # 将关系中的 source 和 target 匹配到本地临时实体（宽松匹配）
                    source_entity = self._lookup_entity_by_key(clause_units, relation.source)
                    target_entity = self._lookup_entity_by_key(citations, relation.target)

                    if not source_entity or not target_entity:
                        # 无法匹配的关系 → 强警告
                        self.result_stats.strong_warning += 1
                        self.result_stats.strong_warning_msg += f"Unmatched citation relation: {relation}\n"
                        continue

                    # 根据 Citation 的属性判断是内部引用还是外部引用
                    if is_internal_reference(target_entity.get("properties", {})):
                        source_entity["internal_citations"].append(target_entity)
                    else:
                        source_entity["external_citations"].append(target_entity)

                    # 记录已被使用的 Citation key
                    processed_citation_keys.add(self._matched_key(citations, target_entity))

                # 标记未被任何 CITES 关系使用的 Citation → 强警告（可能表示模型抽取不完整）
                unused_citations = set(citations.keys()) - processed_citation_keys
                for key in unused_citations:
                    self.result_stats.strong_warning += 1
                    self.result_stats.strong_warning_msg += f"Citation entity was not used by a CITES relation: {key}\n"

                # --- 兜底处理 ---
                if not clause_result["node_id"]:
                    # Legal Provision 未被抽取 → 使用规则兜底
                    if not self.fallback_on_llm_failure:
                        raise ValueError(f"Legal Provision was not extracted for {section_number}")
                    clause_result = self._fallback_clause(filename, one_clause)
                else:
                    # Legal Provision 抽取成功但没有 Provision Unit → 规则兜底生成
                    if not clause_units and self.fallback_on_llm_failure:
                        fallback_units = self._fallback_provision_units(filename, one_clause)
                        clause_units = {
                            f"{unit['node_type']}_{unit['node_name']}": unit
                            for unit in fallback_units
                        }
                    clause_result["provision_units"] = list(clause_units.values())

                # 写入缓存
                clause_cache.clause_cache[section_number] = clause_result
                return clause_result

            except Exception as e:
                # 异常兜底
                if self.fallback_on_llm_failure:
                    logging.warning("Falling back to deterministic clause extraction: %s", e)
                    fallback_result = self._fallback_clause(filename, one_clause)
                    clause_cache.clause_cache[one_clause.get("section_number", "")] = fallback_result
                    return fallback_result
                self.result_stats.error += 1
                self.result_stats.error_msg += f"Clause extraction failed for {filename}: {e}\n"
                raise ValueError(f"Clause extraction failed for {filename}: {e}") from e

    # =========================================================================
    # 最终图谱组装
    # =========================================================================

    async def process_extracted_data(
            self,
            filename: str,
            extracted_file_info: dict,
            extracted_success_clauses: list[dict],
            extracted_failed_clauses: list[dict]
    ) -> dict:
        """
        将文件信息抽取结果和各法条抽取结果合并为最终图谱。

        这里完成三类关键连接（边）：
        1. Legal Document -CONTAINS-> Legal Provision
           （法规文件包含法条）
        2. Legal Provision -CONTAINS-> Provision Unit
           （法条包含条款单元）
        3. Provision Unit -CITES-> Citation 或同文件内已存在的 Provision/Unit
           （条款单元引用引用依据或同文件内其他条款）

        对于内部引用，优先尝试连接到同文件内已生成的实际节点
        （Legal Provision 或 Provision Unit），而不是创建新的 Citation 节点。
        这样可以避免图谱中出现冗余节点，同时保持引用关系的准确性。

        对于失败的法条，使用规则兜底生成基础结果后合并。

        Args:
            filename: 原始文件名
            extracted_file_info: 文件头信息抽取结果
            extracted_success_clauses: 成功抽取的法条结果列表
            extracted_failed_clauses: 失败法条的原始切分数据列表

        Returns:
            最终图谱 {"nodes": [...], "edges": [...]}
        """
        try:
            final_kg = {
                "nodes": [],
                "edges": [],
            }

            # --- 处理 Legal Document 节点 ---
            file_node_id = extracted_file_info.get("node_id")
            file_node_name = extracted_file_info.get("node_name")
            file_node_type = extracted_file_info.get("node_type")
            if not file_node_id or not file_node_name or not file_node_type:
                raise ValueError(f"Legal Document information is incomplete: {extracted_file_info}")

            # 添加 Legal Document 节点到最终图谱
            final_kg["nodes"].append(
                {
                    "node_id": file_node_id,
                    "node_name": file_node_name,
                    "node_type": file_node_type,
                    "properties": extracted_file_info.get("properties", {}),
                    "filename": filename,
                }
            )

            # --- 处理 Legal Basis 节点及其 BASED_ON 关系 ---
            for basis in extracted_file_info.get("legal_basis", []):
                basis_node_id = basis.get("node_id")
                basis_node_name = basis.get("node_name")
                basis_node_type = basis.get("node_type")
                if not basis_node_id or not basis_node_name or not basis_node_type:
                    self.result_stats.strong_warning += 1
                    self.result_stats.strong_warning_msg += f"Legal Basis is incomplete: {basis}\n"
                    continue
                # 添加 Legal Basis 节点
                final_kg["nodes"].append(
                    {
                        "node_id": basis_node_id,
                        "node_name": basis_node_name,
                        "node_type": basis_node_type,
                        "properties": basis.get("properties", {}),
                        "filename": filename,
                    }
                )
                # 添加 BASED_ON 边：Legal Document -BASED_ON-> Legal Basis
                final_kg["edges"].append(
                    self._build_edge(
                        source_id=file_node_id,
                        target_id=basis_node_id,
                        relation_type="BASED_ON",
                        filename=filename,
                    )
                )

            # 对失败的法条进行兜底处理
            for clause in extracted_failed_clauses:
                fallback_clause = self._fallback_clause(filename, clause)
                extracted_success_clauses.append(fallback_clause)

            # --- 建立引用解析映射表 ---
            # internal_reference_id_mapping: 规范化文本键 -> 节点 ID
            #   用于将内部引用字符串匹配到已生成的 Provision/Unit 节点
            internal_reference_id_mapping = {}
            # external_reference_id_mapping: 规范化文本键 -> Citation 节点 ID
            #   用于去重：相同的外部引用共享同一个 Citation 节点
            external_reference_id_mapping = {}
            # unit_to_provision_mapping: Provision Unit 节点 ID -> 其所属的 Legal Provision 节点 ID
            #   用于防止自引用（条款引用自己所在的法条）
            unit_to_provision_mapping = {}
            # internal_reference_mapping: Provision Unit 节点 ID -> [内部引用 Citation 列表]
            internal_reference_mapping = {}
            # external_reference_mapping: Provision Unit 节点 ID -> [外部引用 Citation 列表]
            external_reference_mapping = {}

            # --- 处理每个成功抽取的法条 ---
            for clause in extracted_success_clauses:
                provision_node_id = clause.get("node_id")
                provision_node_name = clause.get("node_name")
                provision_node_type = clause.get("node_type")
                if not provision_node_id or not provision_node_name or not provision_node_type:
                    self.result_stats.strong_warning += 1
                    self.result_stats.strong_warning_msg += f"Legal Provision is incomplete: {clause}\n"
                    continue

                provision_properties = clause.get("properties", {})

                # 添加 Legal Provision 节点到最终图谱
                final_kg["nodes"].append(
                    {
                        "node_id": provision_node_id,
                        "node_name": provision_node_name,
                        "node_type": provision_node_type,
                        "properties": provision_properties,
                        "filename": filename,
                    }
                )
                # 添加 CONTAINS 边：Legal Document -CONTAINS-> Legal Provision
                final_kg["edges"].append(
                    self._build_edge(
                        source_id=file_node_id,
                        target_id=provision_node_id,
                        relation_type="CONTAINS",
                        filename=filename,
                    )
                )

                # 将法条名称、编号等信息注册到内部引用映射表
                # 用于后续内部引用解析（如 section 1702 of this title → 映射到具体的 Provision 节点）
                for key in (
                    provision_node_name,
                    provision_properties.get("section_number"),
                    provision_properties.get("provision_number"),
                ):
                    normalized_key = normalize_text_key(key)
                    if normalized_key:
                        internal_reference_id_mapping[normalized_key] = provision_node_id

                # --- 处理法条内的 Provision Unit ---
                for unit in clause.get("provision_units", []):
                    unit_node_id = unit.get("node_id")
                    unit_node_name = unit.get("node_name")
                    unit_node_type = unit.get("node_type")
                    if not unit_node_id or not unit_node_name or not unit_node_type:
                        self.result_stats.strong_warning += 1
                        self.result_stats.strong_warning_msg += f"Provision Unit is incomplete: {unit}\n"
                        continue

                    unit_properties = unit.get("properties", {})

                    # 添加 Provision Unit 节点到最终图谱
                    final_kg["nodes"].append(
                        {
                            "node_id": unit_node_id,
                            "node_name": unit_node_name,
                            "node_type": unit_node_type,
                            "properties": unit_properties,
                            "filename": filename,
                        }
                    )
                    # 添加 CONTAINS 边：Legal Provision -CONTAINS-> Provision Unit
                    final_kg["edges"].append(
                        self._build_edge(
                            source_id=provision_node_id,
                            target_id=unit_node_id,
                            relation_type="CONTAINS",
                            filename=filename,
                        )
                    )

                    # 建立 Unit 到 Provision 的关系映射（用于后续自引用检测）
                    unit_to_provision_mapping[unit_node_id] = provision_node_id

                    # 将 Provision Unit 的标识信息也注册到内部引用映射表
                    # （因为内部引用可能直接引用某个具体的 subsection/paragraph）
                    for key in (
                        unit_node_name,
                        unit_properties.get("unit_number"),
                        unit_properties.get("provision_number"),
                    ):
                        normalized_key = normalize_text_key(key)
                        if normalized_key:
                            internal_reference_id_mapping[normalized_key] = unit_node_id

                    # 收集内部和外部引用，稍后统一处理
                    internal_reference_mapping.setdefault(unit_node_id, []).extend(
                        unit.get("internal_citations", [])
                    )
                    external_reference_mapping.setdefault(unit_node_id, []).extend(
                        unit.get("external_citations", [])
                    )

            # --- 处理内部引用（连接到同文件内的已有节点）---
            for unit_node_id, internal_refs in internal_reference_mapping.items():
                for ref in internal_refs:
                    # 尝试将内部引用解析到已有的 Provision/Unit 节点
                    target_id = self._resolve_internal_reference(ref, internal_reference_id_mapping)

                    if not target_id:
                        # 无法解析 → 保留为独立的 Citation 节点（外部引用方式处理）
                        self._append_unresolved_citation_node(
                            final_kg=final_kg,
                            source_id=unit_node_id,
                            citation=ref,
                            filename=filename,
                            id_mapping=external_reference_id_mapping,
                        )
                        continue

                    # 防止自引用：引用目标不能是当前 Unit 自己或其所属的 Provision
                    if target_id in {unit_node_id, unit_to_provision_mapping.get(unit_node_id)}:
                        continue

                    # 添加 CITES 边：Provision Unit -CITES-> 同文件内的 Provision/Unit 节点
                    final_kg["edges"].append(
                        self._build_edge(
                            source_id=unit_node_id,
                            target_id=target_id,
                            relation_type="CITES",
                            filename=filename,
                        )
                    )

            # --- 处理外部引用（创建新的 Citation 节点）---
            for unit_node_id, external_refs in external_reference_mapping.items():
                for ref in external_refs:
                    self._append_unresolved_citation_node(
                        final_kg=final_kg,
                        source_id=unit_node_id,
                        citation=ref,
                        filename=filename,
                        id_mapping=external_reference_id_mapping,
                    )

            return final_kg

        except Exception as e:
            self.result_stats.error += 1
            self.result_stats.error_msg += f"Failed to process extracted data: {e}\n"
            raise

    # =========================================================================
    # 缓存持久化
    # =========================================================================

    @staticmethod
    async def _save_cache_to_json(
            cache_data: ClauseCache,
            output_dir,
            filename
    ):
        """保存英文法条抽取缓存到 JSON 文件，便于失败后排查或续跑。

        缓存文件格式：
        {
            "file_info": {...},       # 文件头信息抽取结果
            "clause_cache": {...}     # section_number -> clause_result 映射
        }

        Args:
            cache_data: ClauseCache 实例
            output_dir: 输出目录
            filename: 原始文件名

        Returns:
            保存的文件路径
        """
        os.makedirs(output_dir, exist_ok=True)
        base_name = os.path.splitext(os.path.basename(filename))[0]
        output_path = os.path.join(output_dir, f"{base_name}_law_en_cache.json")
        payload = {
            "file_info": cache_data.file_info,
            "clause_cache": cache_data.clause_cache,
        }
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        return output_path

    @staticmethod
    async def _load_cache_from_json(filepath):
        """读取英文法条抽取缓存。

        用于从之前保存的缓存文件恢复抽取状态，
        实现断点续跑或缓存重放。

        Args:
            filepath: 缓存 JSON 文件路径

        Returns:
            恢复的 ClauseCache 实例
        """
        with open(filepath, "r", encoding="utf-8") as f:
            payload = json.load(f)
        cache = ClauseCache()
        cache.file_info = payload.get("file_info", {})
        cache.clause_cache = payload.get("clause_cache", {})
        return cache

    async def logging_result_stats(self):
        """打印本轮英文抽取的错误和警告统计。

        分别输出错误数、弱警告数、强警告数，用于日志监控和问题排查。
        """
        logging.info("English legal extraction stats")
        logging.info("Errors: %s", self.result_stats.error)
        logging.info("Weak warnings: %s", self.result_stats.week_warning)
        logging.info("Strong warnings: %s", self.result_stats.strong_warning)

    # =========================================================================
    # 辅助工具方法
    # =========================================================================

    @staticmethod
    def _new_node_id(entity_type: str) -> str:
        """生成 Neo4j 友好的英文节点 ID。

        格式：{实体类型（空格替换为下划线）}_{32位十六进制UUID}
        例如：Legal_Document_a1b2c3d4e5f6...

        Args:
            entity_type: 英文实体类型名（如 "Legal Document"）

        Returns:
            唯一节点 ID 字符串
        """
        safe_type = entity_type.replace(" ", "_")  # 空格转下划线，兼容 Neo4j
        return clean_string_for_neo4j_extended(f"{safe_type}_{generate_hex_uuid()}")

    @staticmethod
    def _matched_key(mapping: dict, value: dict) -> str:
        """根据值对象反查字典键，用于记录哪些 Citation 已被关系使用。

        通过身份比较（is）查找，而非值比较（==）。

        Args:
            mapping: 键到值的映射字典
            value: 目标值对象

        Returns:
            找到的键；如果值不在字典中，返回空字符串
        """
        for key, item in mapping.items():
            if item is value:
                return key
        return ""

    @staticmethod
    def _lookup_entity_by_key(mapping: dict, key: str) -> Optional[dict]:
        """宽松匹配：在临时实体字典中查找模型关系中的 source/target。

        匹配策略（按优先级递减）：
        1. 精确键匹配：key 直接存在于 mapping 中
        2. 规范化文本匹配：将 key 和所有候选键做 normalize_text_key 后比较
        3. 名称部分匹配：去掉类型前缀后比较名称部分
           （如 "Provision Unit_Section 1701 subsection (a)" → "Section 1701 subsection (a)"）

        这种多层匹配是必要的，因为模型输出的 source/target 字符串不一定
        完全等于本代码中生成的键值。

        Args:
            mapping: key -> value 的临时实体字典
            key: 需要匹配的键（来自模型关系输出）

        Returns:
            匹配到的值；如果完全无法匹配，返回 None
        """
        # 策略1：精确匹配
        if key in mapping:
            return mapping[key]

        # 策略2：规范化后匹配
        normalized_key = normalize_text_key(key)
        for candidate_key, candidate_value in mapping.items():
            if normalize_text_key(candidate_key) == normalized_key:
                return candidate_value

        # 策略3：去掉类型前缀后按名称匹配
        # 键格式为 "类型_名称"，如 "Provision Unit_Section 1701 subsection (a)"
        for candidate_key, candidate_value in mapping.items():
            candidate_name = candidate_key.split("_", 1)[-1]  # 去掉类型前缀
            key_name = str(key or "").split("_", 1)[-1]  # 去掉类型前缀
            if normalize_text_key(candidate_name) == normalize_text_key(key_name):
                return candidate_value

        return None

    @staticmethod
    def _build_edge(source_id: str, target_id: str, relation_type: str, filename: str) -> dict:
        """构造项目统一格式的图谱边（关系）。

        边结构：
        {
            "source_id": "起始节点ID",
            "target_id": "目标节点ID",
            "relation_type": "CONTAINS" | "BASED_ON" | "CITES",
            "directionality": "directed",  # 所有关系都是有向的
            "properties": {},  # 边属性（当前为空，预留扩展）
            "filename": "原始文件名",
        }

        Args:
            source_id: 起始节点 ID
            target_id: 目标节点 ID
            relation_type: 关系类型（英文大写下划线格式）
            filename: 原始文件名

        Returns:
            图谱边字典
        """
        return {
            "source_id": source_id,
            "target_id": target_id,
            "relation_type": relation_type,
            "directionality": "directed",
            "properties": {},
            "filename": filename,
        }

    @staticmethod
    def _add_classification_properties(properties: dict, one_clause: dict):
        """把 Title / Chapter 等英文法规层级写入法条节点属性。

        将切分阶段提取的层级信息（title, subtitle, chapter, subchapter,
        part, subpart, division, article）和完整的 classification 字典
        写入法条节点的 properties 中。

        Args:
            properties: 法条节点的属性字典（原地修改）
            one_clause: 切分后的单条法条字典（含层级信息）
        """
        for key in HIERARCHY_LEVELS:
            value = one_clause.get(key, "")
            if value:
                properties[key] = value
        # 同时保留完整的 classification 字典（包含所有非空层级）
        if one_clause.get("classification"):
            properties["classification"] = one_clause.get("classification")

    @staticmethod
    def _build_clause_extract_content(filename: str, one_clause: dict) -> str:
        """拼装发给 LLM 的单条法条上下文文本。

        组装顺序：
        1. 文件名（提供整体语境）
        2. 从高到低的层级标题（Title → Chapter → Part → ...）
        3. 法条编号（如 "Section 1701"）
        4. 法条标题（如果有）
        5. 法条正文（冒号后跟实际条文）

        输出示例：
        "IEEPA.txt Title 50. War and National Defense Chapter 35. International
        Emergency Economic Powers Section 1701 Unusual and extraordinary threat:
        (a) Any authority granted to the President by section 1702..."

        Args:
            filename: 原始文件名
            one_clause: 切分后的单条法条字典

        Returns:
            组装好的 LLM 输入文本
        """
        parts = [filename]
        # 按层级顺序添加非空层级标题
        for level in HIERARCHY_LEVELS:
            value = one_clause.get(level, "")
            if value:
                parts.append(value)
        # 添加法条编号
        parts.append(one_clause.get("section_number", ""))
        # 添加法条标题（如果有）
        heading = one_clause.get("section_heading", "")
        if heading:
            parts.append(heading)
        # 用空格连接上下文部分，然后加冒号和正文
        return " ".join(parts) + ":\n" + one_clause.get("clause_content", "")

    # =========================================================================
    # 兜底/规则方法（LLM 不可用时的确定性抽取）
    # =========================================================================

    def _fallback_file_info(self, filename: str, file_info: str) -> dict:
        """模型不可用时，根据文件名和文件头生成基础 Legal Document 节点。

        使用规则方法：
        - 从文件头推断法规标题
        - 从标题关键词推断法规类型
        - 从文件头中提取 Legal Basis（依据制定法规）
        - 截取文件头前 1000 字符作为 source_summary

        Args:
            filename: 原始文件名
            file_info: 文件头文本

        Returns:
            兜底的文件信息抽取结果
        """
        title = self._infer_document_title(filename, file_info)
        return {
            "node_id": self._new_node_id("Legal Document"),
            "node_name": title,
            "node_type": "Legal Document",
            "properties": {
                "official_title": title,
                "document_number": "",
                "alias": "",
                "document_type": self._infer_document_type(title),
                "publication_effective_info": "",
                "purpose": "",
                "domain": "",
                "applicable_industry": "",
                "scope_of_application": "",
                "issuing_authority": "",
                "publication_date": "",
                "effective_date": "",
                "status": "",
                "source_summary": clean_string(file_info[:1000]),
            },
            "legal_basis": self._extract_legal_basis_from_file_info(file_info),
        }

    def _fallback_clause(self, filename: str, one_clause: dict) -> dict:
        """模型不可用时，根据切分结果生成基础 Legal Provision 和 Provision Unit。

        属性填充策略：
        - 从切分数据中取 section_number / section_heading / clause_content
        - core_topic 使用 section_heading
        - 其他属性留空

        Args:
            filename: 原始文件名
            one_clause: 切分后的单条法条字典

        Returns:
            兜底的法条抽取结果
        """
        section_number = one_clause.get("section_number", "")
        section_heading = one_clause.get("section_heading", "")
        clause_content = one_clause.get("clause_content", "")
        properties = {
            "core_topic": section_heading,
            "scope_of_effect": "",
            "applicable_industry": "",
            "section_number": section_number,
            "section_heading": section_heading,
            "provision_text": clause_content,
        }
        # 写入层级信息
        self._add_classification_properties(properties, one_clause)
        return {
            "node_id": self._new_node_id("Legal Provision"),
            "node_name": section_number,
            "node_type": "Legal Provision",
            "properties": properties,
            "provision_units": self._fallback_provision_units(filename, one_clause),
        }

    def _fallback_provision_units(self, filename: str, one_clause: dict) -> list[dict]:
        """确定性地按顶层 subsection/paragraph 切分 Provision Unit。

        切分策略：
        1. 调用 _split_top_level_units 按 (a)/(b)/(c) 或 (1)/(2)/(3) 切分法条正文
        2. 对每个切分单元，用规则提取引用依据
        3. 提供元属性的基础值（功能类型通过关键词推断，其余属性留空）

        Args:
            filename: 原始文件名
            one_clause: 切分后的单条法条字典

        Returns:
            兜底的 Provision Unit 列表
        """
        section_number = one_clause.get("section_number", "")
        clause_content = one_clause.get("clause_content", "")
        # 按顶层编号切分
        unit_specs = self._split_top_level_units(section_number, clause_content)
        units = []
        for unit_number, unit_level, unit_content in unit_specs:
            # 从单元内容中规则提取引用
            citations = self._extract_citations(unit_content, filename)
            units.append(
                {
                    "node_id": self._new_node_id("Provision Unit"),
                    "node_name": unit_number,
                    "node_type": "Provision Unit",
                    "properties": {
                        "unit_content": clean_string(unit_content),
                        "unit_heading": "",
                        "unit_level": unit_level,
                        "unit_number": unit_number,
                        "applicable_industry": "",
                        "function_type": self._infer_function_type(unit_content),
                        "applicable_subject": "",
                        "responsible_role": "",
                        "conduct_description": "",
                        "condition": "",
                        "legal_consequence": "",
                        "exception": "",
                        "time_element": "",
                        "quantitative_standard": "",
                        "other_information": "",
                    },
                    # 按内部/外部分类引用
                    "internal_citations": [
                        citation for citation in citations
                        if is_internal_reference(citation["properties"])
                    ],
                    "external_citations": [
                        citation for citation in citations
                        if not is_internal_reference(citation["properties"])
                    ],
                }
            )
        return units

    # =========================================================================
    # 规则提取方法
    # =========================================================================

    def _extract_legal_basis_from_file_info(self, file_info: str) -> list[dict]:
        """从文件头中用规则识别制定依据（Legal Basis）。

        识别关键词模式：
        - "pursuant to X Act"  → X Act 是制定依据
        - "under X Act"        → X Act 是制定依据
        - "according to X Act" → X Act 是制定依据
        - "in accordance with X Act" → X Act 是制定依据

        正则模式：
        \b(?:pursuant to|under|according to|in accordance with)\s+
        (?:the\s+)?                                     # 可选的 "the"
        ([A-Z][A-Za-z]*(?:\s+[A-Z][A-Za-z]*)+\s+       # 大写开头的连续单词
        Act(?:\s+of\s+\d{4})?)                           # Act of 年份

        例如匹配：
        - "pursuant to the International Emergency Economic Powers Act"
        - "under the National Emergencies Act"
        - "in accordance with the Clean Air Act of 1990"

        Args:
            file_info: 文件头文本

        Returns:
            Legal Basis 节点字典列表（已去重）
        """
        bases = []
        seen = set()  # 用于去重（基于规范化文本键）
        for match in re.finditer(
            r"\b(?:pursuant to|under|according to|in accordance with)\s+(?:the\s+)?"
            r"([A-Z][A-Za-z]*(?:\s+[A-Z][A-Za-z]*)+\s+Act(?:\s+of\s+\d{4})?)",
            file_info
        ):
            title = clean_string(match.group(1))
            key = normalize_text_key(title)
            # 去重
            if not title or key in seen:
                continue
            seen.add(key)
            bases.append(
                {
                    "node_id": self._new_node_id("Legal Basis"),
                    "node_name": title,
                    "node_type": "Legal Basis",
                    "properties": {
                        "official_title": title,
                        "alias": "",
                    },
                }
            )
        return bases

    @staticmethod
    def _infer_document_title(filename: str, file_info: str) -> str:
        """从文件头优先推断英文法规标题，失败时使用文件名。

        推断策略：
        遍历文件头的每一行，找到第一个满足以下条件的行作为标题：
        - 长度 ≤ 160 字符（排除过长的描述性段落）
        - 包含法规关键词：Act, Code, Regulation, Rules, Order, Directive, Statute, Law

        如果文件头中没有找到符合条件的行，使用文件名（不含扩展名）作为标题。

        Args:
            filename: 原始文件名
            file_info: 文件头文本

        Returns:
            推断的法规标题字符串
        """
        base_name = os.path.splitext(os.path.basename(filename))[0]
        for raw_line in file_info.replace("\r\n", "\n").replace("\r", "\n").split("\n"):
            line = clean_string(raw_line)
            if not line:
                continue
            # 标题行特征：较短，且包含法规类型关键词
            if len(line) <= 160 and re.search(
                r"\b(Act|Code|Regulation|Rules|Order|Directive|Statute|Law)\b", line
            ):
                return line
        return base_name or "Unknown Legal Document"

    @staticmethod
    def _infer_document_type(title: str) -> str:
        """根据标题关键词推断英文法规类型。

        按优先级匹配标题中的法规类型关键词：
        Act → Code → Regulation → Rules → Order → Directive → Statute → Law

        如果都不匹配，返回空字符串。

        Args:
            title: 法规标题字符串

        Returns:
            推断的法规类型关键词（首字母大写），如 "Act"、"Order"
        """
        for keyword in ("Act", "Code", "Regulation", "Rules", "Order", "Directive", "Statute", "Law"):
            if re.search(rf"\b{keyword}\b", title, flags=re.IGNORECASE):
                return keyword
        return ""

    @staticmethod
    def _split_top_level_units(section_number: str, text: str) -> list[tuple[str, str, str]]:
        """按英文法律文本中常见的顶层编号切分条款单元。

        切分策略（按优先级）：
        1. 先尝试按 (a)/(b)/(c) 字母编号切分，成功则 unit_level = "subsection"
        2. 再尝试按 (1)/(2)/(3) 数字编号切分，成功则 unit_level = "paragraph"
        3. 都没找到 → 整个法条作为一个单元，unit_level = "section"

        正则说明：
        (?<!\S)\(([a-z])\)\s  匹配行首或空格后的 "(a) "、"(b) " 等
        (?<!\S)\((\d+)\)\s    匹配行首或空格后的 "(1) "、"(2) " 等

        Args:
            section_number: 法条标准化编号（如 "Section 1701"）
            text: 法条正文

        Returns:
            [(unit_number, unit_level, unit_content), ...] 列表
        """
        content = clean_string(text)
        # 先尝试字母编号 (a)/(b)/(c)
        subsection_matches = list(re.finditer(r"(?<!\S)\(([a-z])\)\s", content))
        unit_level = "subsection"
        if not subsection_matches:
            # 再尝试数字编号 (1)/(2)/(3)
            subsection_matches = list(re.finditer(r"(?<!\S)\((\d+)\)\s", content))
            unit_level = "paragraph"
        if not subsection_matches:
            # 都没有 → 整个法条作为一个单元
            return [(section_number, "section", content)]

        # 按编号位置切分
        units = []
        for index, match in enumerate(subsection_matches):
            start = match.start()  # 当前编号的起始位置
            # 结束位置为下一个编号的起始位置，最后一个到文本末尾
            end = subsection_matches[index + 1].start() if index + 1 < len(subsection_matches) else len(content)
            marker = match.group(1)  # 编号标记（如 "a" 或 "1"）
            unit_number = f"{section_number} {unit_level} ({marker})"
            units.append((unit_number, unit_level, content[start:end].strip()))
        return units

    def _extract_citations(self, text: str, filename: str) -> list[dict]:
        """
        规则识别英文法条中的引用依据（Citation）。

        使用三种正则模式识别不同类型的引用：

        1. Section 引用模式（section X of this Act / section X of Y Act）：
           匹配 "section 1702 of this title"、"sections 301 and 302 of the Clean Air Act" 等。
           如果目标文件以 "this " 开头，则为内部引用。

        2. 美国法典引用模式（XX U.S.C. XX）：
           匹配 "50 U.S.C. 1705"、"50 U.S.C. § 1705" 等标准引用格式。

        3. Act 名称引用模式：
           匹配独立出现的 Act 名称，如 "International Emergency Economic Powers Act"。
           排除当前文件同名的 Act。

        Args:
            text: 待提取引用的文本
            filename: 原始文件名

        Returns:
            去重后的 Citation 节点字典列表
        """
        citations = []
        seen = set()  # 去重用
        doc_title = os.path.splitext(os.path.basename(filename))[0]

        # --- 模式1：Section X of [this|Y] Act ---
        # sections? 匹配 "section" 或 "sections"
        # [0-9A-Za-z.\-]+ 匹配编号（可能含点号、横杠）
        # (?:\([A-Za-z0-9]+\))? 匹配可选的子编号（如 "(a)"）
        # (?:\s*(?:,|and|or)\s*...) 匹配并列引用（如 "sections 301, 302 and 303"）
        # of this title / of this chapter / of Y Act 等
        section_ref_pattern = re.compile(
            r"\bsections?\s+([0-9A-Za-z.\-]+(?:\([A-Za-z0-9]+\))?"
            r"(?:\s*(?:,|and|or)\s*[0-9A-Za-z.\-]+(?:\([A-Za-z0-9]+\))?)*)"
            r"\s+of\s+"
            r"(this\s+(?:title|chapter|section|Act)|that\s+Act|"
            r"the\s+[A-Z][A-Za-z]*(?:\s+[A-Z][A-Za-z]*)*\s+Act(?:\s+of\s+\d{4})?)",
            flags=re.IGNORECASE,
        )
        for match in section_ref_pattern.finditer(text):
            # 处理并列引用：按逗号、and、or 拆分编号
            numbers = re.split(r"\s*(?:,|and|or)\s*", match.group(1))
            target_doc = clean_string(match.group(2))  # 目标文件名
            internal = target_doc.lower().startswith("this ")  # 是否内部引用
            for number in numbers:
                if not number:
                    continue
                provision_number = f"Section {number.strip()}"
                # 内部引用使用当前文件名，外部引用使用提取的目标文件名
                title = doc_title if internal else re.sub(
                    r"^the\s+", "", target_doc, flags=re.IGNORECASE
                )
                node_name = f"{title} {provision_number}".strip()
                citation = self._build_citation(
                    node_name=node_name,
                    official_title=title,
                    provision_number=provision_number,
                    citation_type="Provision",  # 条款级引用
                    internal=internal,
                )
                key = normalize_text_key(node_name)
                if key not in seen:
                    seen.add(key)
                    citations.append(citation)

        # --- 模式2：XX U.S.C. XX（美国法典引用）---
        usc_pattern = re.compile(
            r"\b(\d+)\s+U\.S\.C\.?\s*(?:\u00a7|section|Sec\.)?\s*"
            r"([0-9A-Za-z.\-]+(?:\([A-Za-z0-9]+\))?)",
            flags=re.IGNORECASE,
        )
        for match in usc_pattern.finditer(text):
            provision_number = f"{match.group(1)} U.S.C. Section {match.group(2)}"
            citation = self._build_citation(
                node_name=provision_number,
                official_title="United States Code",
                provision_number=provision_number,
                citation_type="Provision",
                internal=False,  # U.S.C. 引用始终是外部引用
            )
            key = normalize_text_key(provision_number)
            if key not in seen:
                seen.add(key)
                citations.append(citation)

        # --- 模式3：独立 Act 名称引用 ---
        # 匹配 Xxx Xxx Act（至少两个大写开头单词后跟 Act）
        act_pattern = re.compile(
            r"\b(?:the\s+)?([A-Z][A-Za-z]*(?:\s+[A-Z][A-Za-z]*)+\s+Act(?:\s+of\s+\d{4})?)\b"
        )
        for match in act_pattern.finditer(text):
            title = clean_string(match.group(1))
            # 排除当前文件同名的引用（避免自引用）
            if normalize_text_key(title) == normalize_text_key(doc_title):
                continue
            citation = self._build_citation(
                node_name=title,
                official_title=title,
                provision_number="",
                citation_type="Document",  # 文件级引用
                internal=False,
            )
            key = normalize_text_key(title)
            if key not in seen:
                seen.add(key)
                citations.append(citation)

        return citations

    def _build_citation(
            self,
            node_name: str,
            official_title: str,
            provision_number: str,
            citation_type: str,
            internal: bool,
    ) -> dict:
        """构造 Citation（引用依据）节点。

        Citation 节点表示法条中引用的另一个法规文件或具体条款。

        Args:
            node_name: 引用节点名称（如 "International Emergency Economic Powers Act Section 1702"）
            official_title: 被引用文件的正式标题
            provision_number: 被引用的具体条款编号（如 "Section 1702"）；文件级引用时为空
            citation_type: 引用类型：
                          "Document"  - 引用整个文件
                          "Provision" - 引用文件中的具体条款
            internal: True 表示指向当前文件内部的引用

        Returns:
            Citation 节点字典
        """
        return {
            "node_id": self._new_node_id("Citation"),
            "node_name": clean_string(node_name),
            "node_type": "Citation",
            "properties": {
                "citation_type": citation_type,
                "official_title": clean_string(official_title),
                "alias": "",
                "document_type": self._infer_document_type(official_title),
                "provision_number": clean_string(provision_number),
                "is_internal_reference": "Yes" if internal else "No",
                "citation_relation": "reference",
                "citation_purpose": "",
            },
        }

    @staticmethod
    def _infer_function_type(text: str) -> str:
        """根据英文法律关键词推断条款功能类型。

        匹配优先级（按代码顺序）：
        1. prohibition（禁止性）：shall not, may not, must not, prohibit, unlawful
        2. obligation（义务性）：shall, must, required, require
        3. authorization（授权性）：may, authorized, authority, power
        4. liability（责任性）：penalty, liable, fine, imprisoned, sanction
        5. definition（定义性）：means, defined, definition
        6. rule（一般规则，默认）：以上都不匹配时

        Args:
            text: 条款单元文本

        Returns:
            功能类型字符串：prohibition / obligation / authorization /
            liability / definition / rule
        """
        lowered = text.lower()
        # 禁止性：关键词 shall not / may not / must not / prohibit / unlawful
        if re.search(r"\bshall not|may not|must not|prohibit|unlawful\b", lowered):
            return "prohibition"
        # 义务性：关键词 shall / must / required / require
        if re.search(r"\bshall|must|required|require\b", lowered):
            return "obligation"
        # 授权性：关键词 may / authorized / authority / power
        if re.search(r"\bmay|authorized|authority|power\b", lowered):
            return "authorization"
        # 责任性：关键词 penalty / liable / fine / imprisoned / sanction
        if re.search(r"\bpenalty|liable|fine|imprisoned|sanction\b", lowered):
            return "liability"
        # 定义性：关键词 means / defined / definition
        if re.search(r"\bmeans|defined|definition\b", lowered):
            return "definition"
        # 默认：一般规则
        return "rule"

    @staticmethod
    def _resolve_internal_reference(ref: dict, internal_reference_id_mapping: dict) -> Optional[str]:
        """将同文件内部 Citation 匹配到已经生成的 Legal Provision 或 Provision Unit 节点。

        匹配策略（按优先级）：
        1. 用 provision_number 规范化后查找
        2. 用 node_name 规范化后查找
        3. 从 provision_number 中提取 "Section/Article XXX" 格式再查找

        匹配成功返回已有节点的 ID，避免创建冗余的 Citation 节点。

        Args:
            ref: 内部引用 Citation 节点字典
            internal_reference_id_mapping: 规范化文本键 -> 已有节点 ID 的映射表

        Returns:
            匹配到的节点 ID；如果无法匹配，返回 None
        """
        properties = ref.get("properties", {})

        # 候选匹配文本列表
        candidates = [
            properties.get("provision_number"),  # 如 "Section 1702"
            ref.get("node_name"),  # 如 "International Emergency Economic Powers Act Section 1702"
        ]
        for candidate in candidates:
            normalized = normalize_text_key(candidate)
            if normalized and normalized in internal_reference_id_mapping:
                return internal_reference_id_mapping[normalized]

        # 备选：从 provision_number 中解析出 "Section/Article XXX"
        provision_number = properties.get("provision_number", "")
        number_match = re.search(
            r"\b(?:Section|Article)\s+[\w.\-]+(?:\([A-Za-z0-9]+\))*",
            provision_number
        )
        if number_match:
            normalized = normalize_text_key(number_match.group(0))
            return internal_reference_id_mapping.get(normalized)

        return None

    def _append_unresolved_citation_node(
            self,
            final_kg: dict,
            source_id: str,
            citation: dict,
            filename: str,
            id_mapping: dict,
    ):
        """对无法解析到内部节点的引用，保留为 Citation 节点并建立 CITES 边。

        处理逻辑：
        1. 检查该 Citation 是否已经在之前的处理中创建过节点（通过 id_mapping 去重）
        2. 如果未创建，则创建新的 Citation 节点加入 final_kg["nodes"]
        3. 创建 CITES 边：source Provision Unit -CITES-> Citation 节点

        注意：会对节点进行去重——如果两个不同的 Provision Unit 引用了相同的
        外部文件/条款，它们共享同一个 Citation 节点。

        Args:
            final_kg: 最终图谱字典（原地修改）
            source_id: 引用来源的 Provision Unit 节点 ID
            citation: Citation 节点字典
            filename: 原始文件名
            id_mapping: Citation 去重映射表（规范化键 -> 节点 ID）
        """
        ref_node_name = citation.get("node_name", "")
        ref_node_type = citation.get("node_type", "Citation")
        if not ref_node_name:
            return

        # 构建去重键：类型 + 名称 + 引用编号
        ref_key = normalize_text_key(
            f"{ref_node_type}_{ref_node_name}_"
            f"{citation.get('properties', {}).get('provision_number', '')}"
        )

        # 查找是否已有相同引用节点
        ref_node_id = id_mapping.get(ref_key)
        if not ref_node_id:
            # 未创建过：使用 citation 中已有的 ID 或生成新 ID
            ref_node_id = citation.get("node_id") or self._new_node_id(ref_node_type)
            # 添加 Citation 节点
            final_kg["nodes"].append(
                {
                    "node_id": ref_node_id,
                    "node_name": ref_node_name,
                    "node_type": ref_node_type,
                    "properties": citation.get("properties", {}),
                    "filename": filename,
                }
            )
            # 记录到去重映射
            id_mapping[ref_key] = ref_node_id

        # 防止自引用边
        if source_id != ref_node_id:
            # 添加 CITES 边
            final_kg["edges"].append(
                self._build_edge(
                    source_id=source_id,
                    target_id=ref_node_id,
                    relation_type="CITES",
                    filename=filename,
                )
            )


# =============================================================================
# 模块独立运行入口（调试用）
# =============================================================================
if __name__ == "__main__":
    # 调试示例：读取 IEEPA 法案文本文件并切分
    result_data, saved_path = split_clause_file(
        os.path.join("data", "International Emergency Economic Powers Act.txt")
    )
    print(saved_path)  # 输出保存的文件路径
    print(len(result_data["clauses"]))  # 输出切分出的法条数量
