"""
English legal clause splitter and LLM-based extractor.
英文法律法条切分器与 LLM 抽取器。

本模块实现英文法律文本处理的两个核心任务：

任务1 —— 结构化切分（纯规则，不依赖大模型）：
  将英文法律文本按 Title / Chapter / Part / Section 等层级切分为：
  - file_info：文件头信息（法规标题、发布单位、目录等）
  - clauses：逐条法条（Section/Article 级别，含层级路径和正文）

任务2 —— LLM 实体关系抽取（依赖 Langextract 大模型）：
  对切分后的每条法条调用 LLM 抽取：
  - Legal Provision（法条）→ Provision Unit（条款单元）→ Citation（引用）
  - Legal Document（法规文件）→ Legal Basis（法规依据）
  - 注意：codification_text 是 Legal Document 的属性，不是独立实体

处理流程：
  原始英文法律文本
    → split_clause() [任务1：规则切分]
    → *_clauses.json (中间产物)
    → llm_extract_from_split_result() [任务2：LLM抽取]
    → *_llm_extract.json (最终产物)
"""

import json
import os
import re
import asyncio
import logging
from copy import deepcopy
from typing import Any, Optional

from app.infrastructure.information_extraction.base import Entity, Relationship, TextClass
from app.infrastructure.information_extraction.law_en_extract.prompt.example import (
    example_for_clause,       # 法条抽取的 few-shot 示例
    example_for_file_info,    # 文件信息抽取的 few-shot 示例
)
from app.infrastructure.information_extraction.law_en_extract.prompt.prompt import (
    prompt_for_clause,        # 法条抽取的系统提示词
    prompt_for_file_info,     # 文件信息抽取的系统提示词
)
from app.infrastructure.information_extraction.law_en_extract.prompt.schema import (
    schema_for_clause,        # 法条抽取的实体/关系本体定义
    schema_for_file_info,     # 文件信息抽取的实体/关系本体定义
)


# =============================================================================
# 英文法律文本的层级结构定义
# =============================================================================
# 定义从大到小的法律层级：Title(编) → Subtitle(副编) → Chapter(章) →
# Subchapter(节) → Part(部/分编) → Subpart(分部)
# 这些层级对应 U.S. Code / CFR 等英文法规的标准结构
HIERARCHY_LEVELS = [
    "title",       # 编（如 Title 50. War and National Defense）
    "subtitle",    # 副编
    "chapter",     # 章（如 Chapter 35. International Emergency Economic Powers）
    "subchapter",  # 节（如 Subchapter M — International Traffic in Arms Regulations）
    "part",        # 部/分编（如 Part 123 Licenses for the Export...）
    "subpart",     # 分部（如 Subpart A — General Information）
]


# =============================================================================
# LLM 抽取配置 —— 环境变量可覆盖
# =============================================================================
# 默认值沿用中文法条抽取模块中的模型配置；
# 实际部署时通过环境变量覆盖，避免把模型地址/密钥写死在调用方代码中。

# 模型名称（默认使用 Qwen3 30B 蒸馏版）
LAW_EN_CLAUSE_MODEL = os.getenv("LAW_EN_CLAUSE_MODEL", "qwen3-30b-a3b-instruct-2507")
# API 密钥
LAW_EN_CLAUSE_MODEL_API_KEY = os.getenv(
    "LAW_EN_CLAUSE_MODEL_API_KEY",
    "gpustack_342609ce423be29a_4371426b285a91dc44fb4e8d72454847",
)
# API 地址（OpenAI 兼容接口）
LAW_EN_CLAUSE_MODEL_API_URL = os.getenv(
    "LAW_EN_CLAUSE_MODEL_API_URL",
    "http://222.171.219.26:20001/v1/chat/completions",
)
# 单次送入模型的最大字符数（超过则自动截断或分批）
LAW_EN_MAX_CHAR_BUFFER = int(os.getenv("LAW_EN_MAX_CHAR_BUFFER", "7500"))
# 每批处理的法条数量
LAW_EN_BATCH_LENGTH = int(os.getenv("LAW_EN_BATCH_LENGTH", "5"))
# 并发 worker 数量
LAW_EN_MAX_WORKERS = int(os.getenv("LAW_EN_MAX_WORKERS", "3"))
# 单次 API 调用超时（秒）
LAW_EN_TIMEOUT = int(os.getenv("LAW_EN_TIMEOUT", "3000"))


# =============================================================================
# Section 标题匹配正则模式
# =============================================================================
# 当前数据集中的英文法律材料主要使用两种风格：
# 1. U.S.C. / Act 条款： "§ 1701. ..." 或 "SEC. 1701. ..."
# 2. CFR 条款：       "§ 123.1 ..."
#
# 以下正则仅识别法条级别的编号，不包含法律含义。
# 带括号的条款引用如 "§ 126.9(b) of this subchapter" 被故意排除，
# 因为它们是交叉引用（cross-reference），不是独立的法条标题。
SECTION_PATTERNS = [
    # 模式1：双 § 符号 —— §§ 1701
    # 用于引用连续多个 Section 的标题行
    # (?P<label>   ) 捕获标签符号；(?P<number>   ) 捕获编号；(?P<heading>   ) 捕获标题
    re.compile(
        r"^(?P<label>§\s*§|§§)\s*"
        r"(?P<number>[0-9A-Za-z](?:[0-9A-Za-z.\-–—]*[0-9A-Za-z])?)"
        r"\s+(?P<heading>.+)$",
        re.IGNORECASE,
    ),
    # 模式2：单 § 符号 —— § 1701. Unusual and extraordinary threat...
    # 最常见的美国法典/CFR Section 标题格式
    # 分隔符可以是句点、短横（含 en-dash/em-dash）或纯空格
    re.compile(
        r"^(?P<label>§)\s*"
        r"(?P<number>[0-9A-Za-z](?:[0-9A-Za-z.\-–—]*[0-9A-Za-z])?)"
        r"(?:\s*[.\-–—]\s+|\s+)"
        r"(?P<heading>.+)$",
        re.IGNORECASE,
    ),
    # 模式3：Sec. / SEC. 格式 —— Sec. 1701. ...
    # 常见于公法（Public Law）文本
    re.compile(
        r"^(?P<label>Sec\.|SEC\.)\s*"
        r"(?P<number>[0-9A-Za-z](?:[0-9A-Za-z.\-–—]*[0-9A-Za-z])?)"
        r"(?:\s*[.\-–—]\s+|\s+)"
        r"(?P<heading>.+)$",
        re.IGNORECASE,
    ),
]


# =============================================================================
# 层级标题匹配模式
# =============================================================================
# 识别 TITLE / Subtitle / Chapter / Subchapter / PART / Subpart 层级标题
# 每条为 (层级名, 匹配正则) 对
HIERARCHY_PATTERNS = [
    ("title", re.compile(r"^(TITLE|Title)\s+.+$", re.IGNORECASE)),
    ("subtitle", re.compile(r"^Subtitle\s+.+$", re.IGNORECASE)),
    ("chapter", re.compile(r"^Chapter\s+.+$", re.IGNORECASE)),
    ("subchapter", re.compile(r"^Subchapter\s+.+$", re.IGNORECASE)),
    ("subpart", re.compile(r"^Subpart\s+.+$", re.IGNORECASE)),
    ("part", re.compile(r"^(PART|Part)\s+.+$", re.IGNORECASE)),
]


# =============================================================================
# 正文结束标记
# =============================================================================
# 当遇到以下附录/附加内容标记时，视为法条正文结束，
# 后续内容放入 tail_info（尾部信息），不再作为法条处理
END_MARKERS = (
    "Appendix",                    # 附录
    "Attachment",                  # 附件
    "Annex",                       # 附件（国际条约常用）
    "Table of Contents",           # 目录
    "Editorial Notes",             # 编辑注释
    "Effective Date Note",         # 生效日期说明
    "Historical and Statutory Notes",  # 历史和法定注释
    "List of Subjects",            # 主题列表
    "Supplementary Information",   # 补充信息
)


# =============================================================================
# 文本清洗函数
# =============================================================================

def clean_line(line: str) -> str:
    """
    清洗单行文本，去除行首的 Markdown 标题符和列表符，保留法律文本内容。

    处理内容：
    - 去除首尾空白
    - 去除行首的 #（Markdown 标题）、-（列表）、*（列表）等符号

    Normalize one physical line while preserving legal text content.
    """
    return str(line or "").strip().lstrip(" \t\r\n\f\v#-*")


def clean_text(text: str) -> str:
    """
    清洗文本中的空白字符，用于 JSON 输出前的规范化。

    处理内容：
    - 统一换行符（\r\n、\r → \n）
    - 去除行尾空格
    - 合并连续3个以上的空行（最多保留1个空行）
    - 去除首尾空白

    Normalize whitespace for JSON output without changing wording.
    """
    if text is None:
        return ""
    cleaned = str(text).replace("\r\n", "\n").replace("\r", "\n")
    cleaned = re.sub(r"[ \t]+$", "", cleaned, flags=re.MULTILINE)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned.strip()


def normalize_key(value: str) -> str:
    """
    生成用于目录重复检测的宽松匹配键。

    处理内容：
    - 转小写
    - § 替换为 "section "
    - "sec"/"section"/"sec." 统一替换为 "section"
    - 移除非字母数字字符，用空格代替
    - 合并多余空格

    用途：CFR 和公法文本常常以目录开头，目录中的 Section 行会在正文中重复出现。
    通过 normalize_key 可以比较两行的"本质编号"是否相同，从而找出正文开始位置。

    Build a loose key used only for duplicate table-of-contents detection.
    """
    normalized = str(value or "").lower()
    normalized = normalized.replace("§", " section ")
    normalized = re.sub(r"\b(sec|section)\.?\b", "section", normalized)
    normalized = re.sub(r"[^a-z0-9]+", " ", normalized)
    return re.sub(r"\s+", " ", normalized).strip()


# =============================================================================
# Section 和层级识别函数
# =============================================================================

def match_section(line: str) -> Optional[dict]:
    """
    识别一行文本是否为法条标题（Section/Article 级别）。

    遍历 SECTION_PATTERNS 中的三个正则模式，匹配则返回标准化的元数据 dict。
    特别排除以括号开头的交叉引用（如 "(b) of this section"），
    因为它们不是独立的 Section 标题。

    Returns:
        dict 或 None:
        {
            "section_number": "§ 1701",       # 标准化编号（如 § 1701、Sec. 1701）
            "section_key": "1701",            # 用于重复检测的宽松键
            "section_heading": "Unusual...",  # Section 标题文本
            "section_type": "section" | "section_range",  # 普通条款或条款范围
            "raw_heading_line": "§ 1701..."   # 原始标题行（已清洗）
        }

    Return normalized section metadata when a line starts a legal provision.
    """
    cleaned = clean_line(line)
    for pattern in SECTION_PATTERNS:
        match = pattern.match(cleaned)
        if not match:
            continue
        # 提取匹配组
        label = re.sub(r"\s+", "", match.group("label"))  # §/Sec./§§
        number = match.group("number").strip()            # 编号如 1701、123.1
        heading = clean_text(match.group("heading"))      # 标题文本

        # 排除交叉引用：标题以括号开头如 "(b) of this section"
        if re.match(r"^\([A-Za-z0-9]+\)(?:\s|$)", heading):
            return None

        # 根据标签符号确定 Section 类型
        if label.startswith("§§") or label == "§§":
            # 双 §：表示连续多个 Section 的范围
            section_number = f"§§ {number}"
            section_type = "section_range"  # 条款范围
        elif label == "§":
            # 单 §：最常见的 Section 编号
            section_number = f"§ {number}"
            section_type = "section"
        else:
            # Sec./SEC.：公法文本格式
            normalized_label = "Section" if label.lower().startswith("section") else "Sec."
            section_number = f"{normalized_label} {number}"
            section_type = "section"

        return {
            "section_number": section_number,
            "section_key": normalize_key(number),  # 用于目录-正文重复检测
            "section_heading": heading,
            "section_type": section_type,
            "raw_heading_line": cleaned,
        }
    return None


def match_hierarchy(line: str) -> Optional[tuple[str, str]]:
    """
    识别一行文本是否为层级标题（Title/Chapter/Subchapter/Part/Subpart）。

    Returns:
        (层级名, 清洗后的行文本) 或 None

    Identify Title/Chapter/Subchapter/Part/Subpart headings.
    """
    cleaned = clean_line(line)
    for level, pattern in HIERARCHY_PATTERNS:
        if pattern.match(cleaned):
            return level, cleaned
    return None


def update_hierarchy(hierarchy: dict, level: str, value: str):
    """
    更新当前层级状态，并清空该层级以下的所有子层级。

    例如：遇到新的 Chapter 时，不仅要设置 chapter，还要清空 subchapter/part/subpart。
    因为新的 Chapter 意味着旧的子层级不再有效。

    特殊处理：CFR 目录行有时将 Part 和 Subpart 写在同一行，
    如 "Part 120 Purpose and Definitions Subpart A General Information"。
    此时自动拆分 Part 和 Subpart。

    Update current hierarchy and clear lower levels.
    """
    hierarchy[level] = value
    # 找到当前层级的索引，清空其下所有子层级
    level_index = HIERARCHY_LEVELS.index(level)
    for child in HIERARCHY_LEVELS[level_index + 1:]:
        hierarchy[child] = ""

    # 特殊处理：CFR 目录行将 Part 和 Subpart 合并在一行
    # 例如："Part 120 Purpose and Definitions Subpart A General Information"
    # 此时自动拆分为 Part 和 Subpart 两部分
    if level == "part":
        subpart_match = re.search(r"\bSubpart\s+[A-Z0-9]+.+$", value, flags=re.IGNORECASE)
        if subpart_match:
            hierarchy["part"] = value[:subpart_match.start()].strip()
            hierarchy["subpart"] = subpart_match.group(0).strip()


# =============================================================================
# 正文定位与法条构建
# =============================================================================

def find_section_matches(lines: list[str]) -> list[dict]:
    """
    扫描所有行，收集全部 Section 标题及其行位置。

    为每行调用 match_section()，匹配成功则记录行号。

    Collect all section-like headings with their line positions.
    """
    matches = []
    for index, line in enumerate(lines):
        section = match_section(line)
        if section:
            section["line_index"] = index  # 记录行号，后续用于定位正文起点
            matches.append(section)
    return matches


def find_body_start(section_matches: list[dict]) -> int:
    """
    定位法条正文的起始行号。

    核心算法：
    CFR 和公法文本常在开头有一个目录（Table of Contents），
    目录中的 Section 行与正文中的 Section 行编号相同。
    通过检测重复的 section_key 来定位正文起点：
    - 如果存在重复编号，取第一批重复的第一行作为正文起点
    - 如果不存在重复（如 U.S.C. 节选文本没有目录），第一行即为正文

    Raises:
        ValueError: 文本中未找到任何英文法律条款标题

    Locate the first real body section.
    """
    if not section_matches:
        raise ValueError("No English legal section heading found in text")

    seen = set()
    duplicate_starts = []
    for section in section_matches:
        key = section["section_key"]
        if key in seen:
            # 第二次出现该编号：这是正文中的重复
            duplicate_starts.append(section["line_index"])
        seen.add(key)

    # 如果有重复，取最早的第二次出现位置（正文起点）
    # 如果没有重复（纯正文，无目录），取第一个 Section 位置
    return min(duplicate_starts) if duplicate_starts else section_matches[0]["line_index"]


def infer_document_type(file_info: str, clauses: list[dict]) -> str:
    """
    根据文件的层级标题和法条编号风格推断文件类型。

    推断规则（优先级从高到低）：
    1. file_info 中出现 "CFR" / "Code of Federal Regulations" → cfr_part（CFR 法规）
    2. 法条编号以 "Sec." 开头 → public_law（公法/国会立法）
    3. 法条编号以 "§" 开头 → usc_or_statute（美国法典/成文法）
    4. 以上都不匹配 → unknown

    Infer a broad structural type from stable headings.
    """
    info = file_info or ""
    # CFR 关键词检测
    if re.search(r"\bCFR\b|Code of Federal Regulations|^Title\s+\d+\s+[—-]", info, re.IGNORECASE | re.MULTILINE):
        return "cfr_part"
    # 公法格式：Sec. 开头
    if any(clause.get("section_number", "").startswith("Sec.") for clause in clauses):
        return "public_law"
    # 美国法典格式：§ 开头
    if any(clause.get("section_number", "").startswith("§") for clause in clauses):
        return "usc_or_statute"
    return "unknown"


def split_heading_from_content(section_number: str, heading_and_content: str) -> str:
    """
    尽力从 Section 标题行中分离出纯标题文本。

    背景：CFR 格式中，Section 标题常以句号结尾，正文紧跟在同一行。
    例如："§ 123.1 Requirement for export or temporary import licenses. (a) Any person..."
    本函数提取第一个句号前的内容作为标题。

    注意：完整的标题行文本仍然保留在 clause_content 中，
    此函数只是为了方便单独提取 section_heading 字段。

    Best-effort section heading extraction.
    """
    content = clean_text(heading_and_content)
    if not content:
        return ""

    # 仅对 § + 数字.数字格式（如 § 123.1）应用句号分割逻辑
    if re.match(r"^§\s+\d+\.\d+", section_number):
        # 匹配第一个完整句子（以句号结尾，后跟大写字母/数字/括号）
        sentence_match = re.match(r"^(.+?\.)\s+[A-Z0-9(]", content)
        if sentence_match:
            return sentence_match.group(1).strip()
    return content


def build_clause(section: dict, hierarchy: dict, content_lines: list[str], start_line: int, end_line: int) -> dict:
    """
    组装一条完整的法条记录（section-level clause）。

    将 section 元数据、层级上下文、正文内容行合并为一个标准化的 dict。

    Args:
        section: match_section() 返回的法条元数据
        hierarchy: 当前累积的层级上下文（Title/Chapter/Part 等）
        content_lines: 该法条的正文内容行列表
        start_line: 该法条在原文本中的起始行号
        end_line: 该法条在原文本中的结束行号

    Assemble one section-level clause record.
    """
    raw_content = "\n".join(content_lines)
    clause_content = clean_text(raw_content)
    section_heading = split_heading_from_content(
        section["section_number"],
        section.get("section_heading", ""),
    )
    return {
        "title": hierarchy.get("title", ""),
        "subtitle": hierarchy.get("subtitle", ""),
        "chapter": hierarchy.get("chapter", ""),
        "subchapter": hierarchy.get("subchapter", ""),
        "part": hierarchy.get("part", ""),
        "subpart": hierarchy.get("subpart", ""),
        "section_number": section["section_number"],
        "section_heading": section_heading,
        "section_type": section.get("section_type", "section"),
        "raw_heading_line": section.get("raw_heading_line", ""),
        "clause_content": clause_content,
        "source_start_line": start_line + 1,
        "source_end_line": end_line + 1,
    }


# =============================================================================
# 核心切分函数：split_clause
# =============================================================================

def split_clause(text: str) -> dict:
    """
    将英文法律文本切分为文件头信息（file_info）和法条列表（clauses）。

    处理流程：
    1. 统一换行符，按行拆分
    2. 扫描所有 Section 标题行，检测目录/正文边界（find_body_start）
    3. 第一遍扫描 body_start 之前的行，累积层级信息
    4. 第二遍扫描 body_start 及之后的行，逐行构建法条：
       - 遇到新 Section → 保存前一条法条，开启新法条
       - 遇到层级标题 → 更新层级状态
       - 遇到 END_MARKERS → 停止切分，后续内容放入 tail_info
       - 其他行 → 累加到当前法条的正文
    5. 保存最后一条法条

    Returns:
        {
            "file_info": "...",       # 文件头信息（目录、标题页等）
            "tail_info": "...",       # 尾部信息（附录、注解等）
            "document_type": "...",   # 文件类型：cfr_part/public_law/usc_or_statute/unknown
            "clauses": [...]          # 法条列表
        }

    Raises:
        ValueError: 文本为空或未找到任何法条

    Split an English legal document into document header information and clauses.
    """
    if not text or not str(text).strip():
        raise ValueError("English legal text is empty")

    # 第一步：统一换行符，按行拆分
    lines = str(text).replace("\r\n", "\n").replace("\r", "\n").split("\n")

    # 第二步：扫描 Section 标题，定位正文起点
    section_matches = find_section_matches(lines)
    body_start = find_body_start(section_matches)

    # 第三步：在 body_start 之前的行中累积层级信息
    # 遍历前面的层级标题（TITLE/Chapter/Part等），建立初始层级上下文
    hierarchy = {level: "" for level in HIERARCHY_LEVELS}
    for raw_line in lines[:body_start]:
        line = clean_line(raw_line)
        if not line:
            continue
        hierarchy_match = match_hierarchy(line)
        if hierarchy_match:
            level, value = hierarchy_match
            update_hierarchy(hierarchy, level, value)

    # 第四步：逐行处理正文区域
    clauses = []
    current_section = None         # 当前正在构建的法条元数据
    current_hierarchy = deepcopy(hierarchy)  # 快照当前层级（法条开始时固定）
    current_lines = []             # 当前法条的累计正文行
    current_start = body_start     # 当前法条的起始行
    tail_info = ""                 # 尾部信息（遇到 END_MARKERS 后的内容）

    def append_current(end_index: int):
        """
        内部函数：将当前累积的法条（current_section + current_lines）保存到 clauses 列表，
        然后重置临时状态，准备接收下一条法条。
        """
        nonlocal current_section, current_lines, current_hierarchy, current_start
        if not current_section:
            return
        clauses.append(
            build_clause(
                section=current_section,
                hierarchy=current_hierarchy,
                content_lines=current_lines,
                start_line=current_start,
                end_line=max(current_start, end_index),
            )
        )
        current_section = None
        current_lines = []

    for index, raw_line in enumerate(lines):
        line = clean_line(raw_line)

        # 空行处理：如果在法条内，保留空行（用于保持段落结构）
        if not line:
            if current_section:
                current_lines.append("")
            continue

        # 层级标题处理：更新层级状态
        hierarchy_match = match_hierarchy(line)
        if hierarchy_match:
            # 如果当前正在构建法条且层级出现在正文区域，先保存当前法条
            if index >= body_start and current_section:
                append_current(index - 1)
            level, value = hierarchy_match
            update_hierarchy(hierarchy, level, value)
            continue

        # 跳过 body_start 之前的行（已在第三步中处理）
        if index < body_start:
            continue

        # END_MARKERS 检测：遇到附录标记则停止切分
        if any(line.startswith(marker) for marker in END_MARKERS):
            append_current(index - 1)
            tail_info = clean_text("\n".join(lines[index:]))
            break

        # Section 标题检测：开始新法条
        section = match_section(line)
        if section:
            append_current(index - 1)          # 保存上一条法条
            current_section = section           # 设置新法条元数据
            current_hierarchy = deepcopy(hierarchy)  # 快照当前层级
            current_start = index
            current_lines = [section["section_heading"]]  # 首行：法条标题
            continue

        # 普通正文行：如果正在构建法条，累加到当前正文
        if current_section:
            current_lines.append(raw_line.strip())

    # 第五步：保存最后一条法条
    append_current(len(lines) - 1)

    if not clauses:
        raise ValueError("No English legal clauses were extracted")

    # 组装文件头信息和尾部信息
    file_info = clean_text("\n".join(lines[:body_start]))
    return {
        "file_info": file_info,
        "tail_info": tail_info,
        "document_type": infer_document_type(file_info, clauses),
        "clauses": clauses,
    }


# =============================================================================
# 文件 I/O 辅助函数
# =============================================================================

def save_clause_result(result: dict, output_dir: str, filename: str) -> str:
    """
    保存切分结果到 *_clauses.json 文件，便于调试和离线检查。

    Save split result for debugging or offline inspection.
    """
    os.makedirs(output_dir, exist_ok=True)
    base_name = os.path.splitext(os.path.basename(filename))[0]
    output_path = os.path.join(output_dir, f"{base_name}_clauses.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    return output_path


def split_clause_file(input_path: str, output_dir: str = os.path.join("data", "temp")) -> tuple[dict, str]:
    """
    读取本地英文法律文本文件，执行切分并保存 JSON 结果。

    Returns:
        (切分结果 dict, 输出文件路径)

    Read a local English legal text file, split it, and save JSON output.
    """
    with open(input_path, "r", encoding="utf-8") as f:
        text = f.read()
    result = split_clause(text)
    output_path = save_clause_result(result, output_dir, input_path)
    return result, output_path


# =============================================================================
# LLM 抽取结果序列化
# =============================================================================

def _pydantic_to_dict(value: Any) -> Any:
    """
    将 Langextract 返回的 Pydantic 模型递归转换为普通 JSON 安全的 dict。

    支持 Pydantic v1（.dict()）和 v2（.model_dump()）两种 API。

    Convert Pydantic models returned by Langextract into JSON-safe dicts.
    """
    if hasattr(value, "model_dump"):       # Pydantic v2
        return value.model_dump()
    if hasattr(value, "dict"):             # Pydantic v1
        return value.dict()
    if isinstance(value, list):
        return [_pydantic_to_dict(item) for item in value]
    if isinstance(value, dict):
        return {key: _pydantic_to_dict(item) for key, item in value.items()}
    return value


def serialize_extract_result(extract_result: dict) -> dict:
    """
    将 Langextract 原始输出序列化为普通 dict。

    任务2只要求保存抽取结果供人工查看，不在这里组装 Neo4j 节点和边。
    因此将 Entity / Relationship / TextClass 原样转为普通 dict 输出。

    Serialize raw Langextract output while preserving the entity/relation split.
    """
    return {
        "entities": [
            _pydantic_to_dict(entity)
            for entity in extract_result.get("entities", [])
            if isinstance(entity, Entity)
        ],
        "relations": [
            _pydantic_to_dict(relation)
            for relation in extract_result.get("relations", [])
            if isinstance(relation, Relationship)
        ],
        "texts_classes": [
            _pydantic_to_dict(text_class)
            for text_class in extract_result.get("texts_classes", [])
            if isinstance(text_class, TextClass)
        ],
    }


# =============================================================================
# LLM 输入文本构建
# =============================================================================

def build_file_info_llm_input(filename: str, split_result: dict) -> str:
    """
    为 file_info 抽取构建 LLM 输入文本。

    将文件名、文件类型、文件头信息拼接为结构化文本。

    Build the LLM input text for file_info extraction.
    """
    return clean_text(
        "\n".join(
            [
                f"Filename: {filename}",
                f"Document type: {split_result.get('document_type', '')}",
                "File information:",
                split_result.get("file_info", ""),
            ]
        )
    )


def build_clause_llm_input(filename: str, clause: dict) -> str:
    """
    为单条法条（Legal Provision）抽取构建 LLM 输入文本。

    直接把除 source_start_line / source_end_line 外的整条 clause JSON
    发给模型，省去手动拼接字段的冗余逻辑。

    Build the LLM input text for one section-level Legal Provision.
    """
    # 去掉仅供调试的行号字段，其余全部发给模型
    llm_clause = {
        key: value
        for key, value in clause.items()
        if key not in ("source_start_line", "source_end_line")
    }
    return clean_text(
        f"Filename: {filename}\n"
        f"Clause: {json.dumps(llm_clause, ensure_ascii=False)}"
    )


def save_llm_extraction_result(result: dict, output_dir: str, filename: str) -> str:
    """
    保存任务2的 LLM 抽取结果到 *_llm_extract.json 文件。

    注意：*_clauses.json 是任务1的中间产物，此处去除 "_clauses" 后缀，
    生成 *_llm_extract.json 作为任务2的最终产物。

    Save task-2 LLM extraction result without converting it into a KG.
    """
    os.makedirs(output_dir, exist_ok=True)
    base_name = os.path.splitext(os.path.basename(filename))[0]
    base_name = re.sub(r"_clauses$", "", base_name)  # 去除任务1中间产物的后缀
    output_path = os.path.join(output_dir, f"{base_name}_llm_extract.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    return output_path


# =============================================================================
# 顶层便捷异步函数
# =============================================================================

async def extract_clause_json_file(
        input_json_path: str,
        output_dir: str = os.path.join("data", "temp"),
        max_concurrent: int = 5,
) -> tuple[dict, str]:
    """
    快捷函数：对任务1输出的 *_clauses.json 文件执行任务2 LLM 抽取。

    Convenience function: run task-2 extraction on one task-1 *_clauses.json.
    """
    extractor = ClauseEnExtractor(max_concurrent=max_concurrent)
    return await extractor.extract_clause_json_to_json(input_json_path, output_dir)


# =============================================================================
# 主类：ClauseEnExtractor
# =============================================================================

class ClauseEnExtractor:
    """
    英文法律法条抽取器。

    提供以下功能：
    - 任务1（纯规则）：split_clause / split_clause_to_json — 文本切分
    - 任务2（LLM）：llm_extract_from_file_info / llm_extract_from_clause — LLM 抽取
    - 完整流程：llm_extract_from_split_result / extract_clause_json_to_json /
                extract_text_to_llm_json — 任务1+2一键执行

    English law extractor for task-1 splitting and task-2 LLM extraction.
    """

    def __init__(self, max_concurrent: int = 5):
        """
        Args:
            max_concurrent: 最大并发法条 LLM 抽取数（通过 asyncio.Semaphore 控制）
        """
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)  # 并发控制信号量
        self._extractor = None  # 延迟加载的 Langextract 适配器

    def _get_extractor(self):
        """
        延迟创建 Langextract 适配器。

        设计意图：切分法条（任务1）不需要 LLM 依赖；只有执行任务2时才加载。
        这样在仅做文本切分的场景下，可以不安装 Langextract 相关依赖。

        Raises:
            RuntimeError: 缺少 Langextract 依赖时抛出

        Lazy-create the Langextract adapter only when task-2 extraction runs.
        """
        if self._extractor is not None:
            return self._extractor

        # 延迟导入：切分法条时不需要 LLM 依赖；只有执行任务 2 时才加载
        try:
            from app.infrastructure.information_extraction.factory import InformationExtractionFactory
            from app.infrastructure.information_extraction.method.base import LangextractConfig
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "English LLM extraction requires the project's Langextract dependencies. "
                f"Missing dependency: {exc.name}"
            ) from exc

        # 配置 Langextract 参数
        extractor_config = LangextractConfig(
            model_name=LAW_EN_CLAUSE_MODEL,
            api_key=LAW_EN_CLAUSE_MODEL_API_KEY,
            api_url=LAW_EN_CLAUSE_MODEL_API_URL,
            config={"timeout": LAW_EN_TIMEOUT},    # API 调用超时
            max_char_buffer=LAW_EN_MAX_CHAR_BUFFER, # 单次最大输入字符数
            batch_length=LAW_EN_BATCH_LENGTH,       # 批量处理大小
            max_workers=LAW_EN_MAX_WORKERS,          # 最大 worker 数
        )
        # 通过工厂创建 Langextract 抽取器实例，最多重试 5 次
        self._extractor = InformationExtractionFactory.create(
            "langextract",
            max_retries=5,
            config=extractor_config,
        )
        return self._extractor

    # ---- 任务1：文本切分（纯规则） ----

    async def split_clause(self, text: str) -> dict:
        """异步包装：对纯文本执行法条切分。"""
        return split_clause(text)

    async def split_clause_to_json(
            self,
            input_path: str,
            output_dir: str = os.path.join("data", "temp"),
    ) -> dict:
        """
        读取本地文本文件，执行法条切分，保存 JSON 并返回结果。

        Returns:
            {"output_path": "...", "file_info": "...", "clauses": [...], ...}
        """
        result, output_path = split_clause_file(input_path, output_dir)
        return {
            "output_path": output_path,
            **result,
        }

    # ---- 任务2：LLM 抽取 ----

    async def llm_extract_from_file_info(self, filename: str, split_result: dict) -> dict:
        """
        从文件头信息（file_info）中通过 LLM 抽取：
        - Legal Document（法规文件，codification_text 作为属性记录）
        - Legal Basis（法规依据）

        返回值保留为原始抽取结构（entity/relation 列表），不在此处组装 Neo4j 节点边，
        便于任务2结果的人工审阅。

        Extract Legal Document / Legal Basis from file_info.
        """
        file_info = split_result.get("file_info", "")
        if not file_info:
            raise ValueError("file_info is empty; cannot run file-info LLM extraction")
        # 构建 LLM 输入文本
        input_text = build_file_info_llm_input(filename, split_result)
        # 调用 Langextract 执行实体关系抽取
        extract_result = await self._get_extractor().entity_and_relationship_extract(
            user_prompt=prompt_for_file_info,
            schema=schema_for_file_info,
            input_text=input_text,
            examples=example_for_file_info,
        )
        return {
            "input_text": input_text,
            "extraction": serialize_extract_result(extract_result),
        }

    async def llm_extract_from_clause(self, filename: str, clause: dict) -> dict:
        """
        从单条法条（clause）中通过 LLM 抽取：
        - Legal Provision（法条）→ Provision Unit（条款单元）→ Citation（引用）

        使用 asyncio.Semaphore 控制并发数，避免同时发送过多 API 请求。

        Extract Legal Provision / Provision Unit / Citation from one clause record.
        """
        async with self.semaphore:  # 并发控制
            input_text = build_clause_llm_input(filename, clause)
            extract_result = await self._get_extractor().entity_and_relationship_extract(
                user_prompt=prompt_for_clause,
                schema=schema_for_clause,
                input_text=input_text,
                examples=example_for_clause,
            )
            return {
                "section_number": clause.get("section_number", ""),
                "section_heading": clause.get("section_heading", ""),
                "source_start_line": clause.get("source_start_line"),
                "source_end_line": clause.get("source_end_line"),
                "input_clause": clause,
                "input_text": input_text,
                "extraction": serialize_extract_result(extract_result),
            }

    async def llm_extract_from_split_result(self, filename: str, split_result: dict) -> dict:
        """
        对任务1的切分结果执行完整的任务2 LLM 抽取。

        流程：
        1. 对 file_info 执行 LLM 抽取（Legal Document / Legal Basis 等）
        2. 对所有 clauses 并发执行 LLM 抽取（Legal Provision / Provision Unit / Citation）
        3. 使用 asyncio.gather() 并发处理，通过 return_exceptions=True 容错
        4. 将失败的 clause 抽取分离到 failed_clause_extractions

        Returns:
            {
                "source_filename": "...",
                "document_type": "...",
                "file_info_extraction": {...},
                "clause_extractions": [...],
                "failed_clause_extractions": [...],
                "stats": { "clause_total": N, "clause_success": N, "clause_failed": N }
            }

        Run task-2 LLM extraction for a task-1 split result.
        """
        clauses = split_result.get("clauses", [])
        if not clauses:
            raise ValueError("clauses is empty; cannot run clause LLM extraction")

        # 先抽取文件头信息
        file_info_result = await self.llm_extract_from_file_info(filename, split_result)

        # 为每条法条创建异步任务，并发执行 LLM 抽取
        tasks = [
            self.llm_extract_from_clause(filename, clause)
            for clause in clauses
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # 分离成功和失败的结果
        clause_results = []
        failed_clause_results = []
        for clause, result in zip(clauses, results):
            if isinstance(result, Exception):
                # 记录失败的 clause（日志 + 保留到失败列表）
                logging.error("English clause LLM extraction failed for %s: %s", clause.get("section_number"), result)
                failed_clause_results.append(
                    {
                        "section_number": clause.get("section_number", ""),
                        "input_clause": clause,
                        "error": str(result),
                    }
                )
            else:
                clause_results.append(result)

        return {
            "source_filename": filename,
            "document_type": split_result.get("document_type", ""),
            "file_info": split_result.get("file_info", ""),
            "tail_info": split_result.get("tail_info", ""),
            "file_info_extraction": file_info_result,
            "clause_extractions": clause_results,
            "failed_clause_extractions": failed_clause_results,
            "stats": {
                "clause_total": len(clauses),          # 总法条数
                "clause_success": len(clause_results),  # 成功抽取的法条数
                "clause_failed": len(failed_clause_results),  # 失败的法条数
            },
        }

    # ---- 完整流程入口 ----

    async def extract_clause_json_to_json(
            self,
            input_json_path: str,
            output_dir: str = os.path.join("data", "temp"),
    ) -> tuple[dict, str]:
        """
        完整流程入口（从任务1输出 JSON 开始）：
        读取 *_clauses.json → LLM 抽取 → 保存 *_llm_extract.json

        Read a task-1 *_clauses.json file, run task-2 LLM extraction, and save it.
        """
        with open(input_json_path, "r", encoding="utf-8") as f:
            split_result = json.load(f)
        filename = os.path.basename(input_json_path)
        result = await self.llm_extract_from_split_result(filename, split_result)
        output_path = save_llm_extraction_result(result, output_dir, input_json_path)
        return result, output_path

    async def extract_text_to_llm_json(
            self,
            input_path: str,
            output_dir: str = os.path.join("data", "temp"),
    ) -> tuple[dict, str]:
        """
        完整流程入口（从原始文本开始）：
        读取原始文本 → 任务1切分 → 任务2 LLM抽取 → 保存结果

        Split a raw English legal text, then run task-2 LLM extraction and save it.
        """
        # 任务1：切分
        split_result, split_output_path = split_clause_file(input_path, output_dir)
        # 任务2：LLM抽取
        result = await self.llm_extract_from_split_result(os.path.basename(input_path), split_result)
        result["split_output_path"] = split_output_path  # 记录中间产物路径
        output_path = save_llm_extraction_result(result, output_dir, input_path)
        return result, output_path
