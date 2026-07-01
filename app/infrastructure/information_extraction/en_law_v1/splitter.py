"""格式一英文法规 Markdown 的规则切分器。

本文件负责“不依赖 LLM”的一阶段结构化切分，核心目标是把完整法规文件拆成：
1. 文件头和法规基础信息候选文本。
2. Whereas / recital 前言条款。
3. Article 正文条文。
4. Annex 附件块。
5. TITLE / CHAPTER / Section / PART 等层级上下文。

设计原则：
- 先用规则稳定识别法规结构，再把较小的 Article 单元交给 LLM。
- Whereas 不作为图谱节点批量入库，只选取部分内容作为文件级属性补充依据。
- Annex 默认只保留元数据，不把超大附件原文直接放入节点属性。
"""

from __future__ import annotations

# re 用于识别 Article、Annex、Whereas、结构单元和修订型表述。
import re
# deepcopy 用于复制当前层级上下文，避免后续层级变化影响已生成 Article。
from copy import deepcopy
# Any 用于描述切分结果中混合类型的字典属性。
from typing import Any

# 合规风险类型允许值来自配置文件，用于校验文首业务分类。
from app.infrastructure.information_extraction.en_law_v1.config import RISK_TYPE_VALUES


# Article 标题正则：匹配 `Article 1`、`## Article 1`、`Article 1a` 等格式。
ARTICLE_RE = re.compile(
    r"^\s*#{0,6}\s*Article\s+(?P<number>\d+[A-Za-z]?)\b(?P<tail>.*)$",
    re.IGNORECASE,
)
# Annex 标题正则：匹配 `ANNEX`、`ANNEX I`、`## ANNEX 1` 等格式。
ANNEX_RE = re.compile(
    r"^\s*#{0,6}\s*ANNEX(?:\s+(?P<id>[IVXLCDM]+|\d+|[A-Z]))?\b(?P<tail>.*)$",
    re.IGNORECASE,
)
# Whereas 区块标题正则。
WHEREAS_RE = re.compile(r"^\s*#{0,6}\s*Whereas\s*:?\s*$", re.IGNORECASE)
# 单条 recital 正则：匹配 `(1) This Regulation ...`。
RECITAL_RE = re.compile(r"^\s*\((?P<number>\d+)\)\s*(?P<content>.*)$")
# 文首合规风险类型正则：匹配 `合规风险类型：产品法律风险`。
RISK_HEADER_RE = re.compile(r"^\s*合规风险类型\s*[:：]\s*(?P<value>.+?)\s*$")

# 层级标题识别规则；不同层级会挂载到 Article 的 classification_context 中。
HIERARCHY_PATTERNS = [
    ("title", re.compile(r"^\s*#{0,6}\s*TITLE\b.+$", re.IGNORECASE)),
    ("chapter", re.compile(r"^\s*#{0,6}\s*CHAPTER\b.+$", re.IGNORECASE)),
    ("section", re.compile(r"^\s*#{0,6}\s*Section\b.+$", re.IGNORECASE)),
    ("part", re.compile(r"^\s*#{0,6}\s*PART\b.+$", re.IGNORECASE)),
]
# 层级顺序，供后续扩展或审查时理解上下文重置关系。
HIERARCHY_ORDER = ["title", "chapter", "section", "part"]

# Article 内部结构单元识别规则。
STRUCTURAL_UNIT_PATTERNS = [
    # 数字段落，例如 `1. Operators shall ...`。
    ("paragraph", re.compile(r"^\s*(?P<number>\d+)\.\s+(?P<content>.+)$")),
    # 字母项，例如 `(a) operators ...`。
    ("point", re.compile(r"^\s*\((?P<number>[a-z])\)\s+(?P<content>.+)$")),
    # 罗马数字子项，例如 `(i) ...`。
    ("subpoint", re.compile(r"^\s*\((?P<number>[ivxlcdm]+)\)\s+(?P<content>.+)$", re.IGNORECASE)),
    # 破折号清单项；保留为 dash_item。
    ("dash_item", re.compile(r"^\s*[-–]\s+(?P<content>.+)$")),
]

# v1 ProvisionClause 边界：只允许 Article 正文行首一级数字编号，如 `1. ...`。
# `(1)`、`(a)`、`(i)` 等编号不得作为 ProvisionClause 边界。
PROVISION_CLAUSE_RE = re.compile(r"^\s*(?P<number>\d+)\.\s+(?P<content>.+)$")

# 修订型 Article 识别规则，用于给 Article 打 is_amendment_article 标记。
AMENDMENT_RE = re.compile(
    r"\b(is|are|shall be|has been|have been)\s+"
    r"(amended|replaced|inserted|deleted|repealed)\b|"
    r"\bamendments?\s+to\b|\bis amended as follows\b",
    re.IGNORECASE,
)


def normalize_newlines(text: str) -> str:
    """统一换行符，不改变法规原文措辞。"""
    # Windows、旧 Mac、Unix 换行统一转换为 `\n`，方便按行扫描。
    return str(text or "").replace("\r\n", "\n").replace("\r", "\n")


def clean_line(line: str) -> str:
    """清理单行首尾空白，用于保存原始标题行。"""
    # 这里只做 strip，不删除正文中的合法空格。
    return str(line or "").strip()


def strip_markdown_heading(line: str) -> str:
    """去掉 Markdown 标题前缀 `#`，便于统一匹配标题文本。"""
    # 只移除行首 1-6 个 #，不改变正文中的 # 符号。
    return re.sub(r"^\s*#{1,6}\s*", "", str(line or "").strip()).strip()


def _looks_like_inline_article_heading(raw_tail: str) -> bool:
    """判断 Article 编号同一行后面的文本是否像标题而不是正文引用。"""
    raw = str(raw_tail or "").strip()
    if not raw:
        return True
    # 标题行不应以括号、逗号等正文延续符号开头，例如 Article 3(5) 或 Article 1, Article 2。
    if re.match(r"^[,;()]|^\([^)]+\)", raw):
        return False
    tail = re.sub(r"^[\s.\-–—:]+", "", raw).strip()
    if not tail:
        return True
    # 真正标题通常较短；长句更可能是正文中的引用、适用说明或修订说明。
    if len(tail) > 120 or len(tail.split()) > 12:
        return False
    # 句号结尾通常表示正文句子，不应作为新 Article 标题。
    if tail.endswith("."):
        return False
    # 标题一般以大写字母或数字开头；小写开头如 "of Directive ..." 多为正文引用延续。
    first = tail[0]
    if first.isalpha() and not first.isupper():
        return False
    return True


def clean_text(text: str) -> str:
    """规范文本空白，同时尽量保留段落边界。"""
    # 先统一换行。
    value = normalize_newlines(text)
    # 删除每行末尾多余空格和制表符。
    value = re.sub(r"[ \t]+$", "", value, flags=re.MULTILINE)
    # 连续 3 个以上换行收敛为 2 个，避免输出过度稀疏。
    value = re.sub(r"\n{3,}", "\n\n", value)
    # 删除整体首尾空白。
    return value.strip()


def parse_risk_header(lines: list[str]) -> tuple[dict[str, Any], list[str]]:
    """解析并移除文首中文“合规风险类型”行。"""
    # 空文件直接返回空元数据和原始行列表。
    if not lines:
        return {}, lines

    # 找到第一条非空行；风险类型必须位于文首有效行。
    first_non_empty = None
    for index, line in enumerate(lines):
        if line.strip():
            first_non_empty = index
            break
    # 全空文件没有风险类型。
    if first_non_empty is None:
        return {}, lines

    # 尝试匹配 `合规风险类型：...`。
    match = RISK_HEADER_RE.match(lines[first_non_empty])
    if not match:
        return {}, lines

    # 抽取风险类型值。
    value = match.group("value").strip()
    # 记录风险类型、原始行、行号和是否属于允许值。
    metadata = {
        "compliance_risk_type": value,
        "risk_header_line": lines[first_non_empty].strip(),
        "risk_header_line_number": first_non_empty + 1,
        "risk_header_valid": value in RISK_TYPE_VALUES,
    }
    # 从正文行列表中移除风险类型行，避免干扰英文法规结构识别。
    new_lines = lines[:first_non_empty] + lines[first_non_empty + 1 :]
    return metadata, new_lines


def match_article(line: str) -> dict[str, str] | None:
    """匹配 Article 标题行，并返回 Article 编号和行内标题。"""
    # 去掉 Markdown # 后再匹配 Article。
    match = ARTICLE_RE.match(strip_markdown_heading(line))
    if not match:
        return None
    if not _looks_like_inline_article_heading(match.group("tail") or ""):
        return None
    # number 是纯编号，例如 `1` 或 `1a`。
    number = match.group("number")
    # tail 是 Article 同一行后面的标题片段，例如 `Subject matter`。
    tail = re.sub(r"^[\s.\-–—:]+", "", match.group("tail") or "").strip()
    return {
        "article_number": f"Article {number}",
        "article_index": number,
        "inline_heading": tail,
        "raw_heading_line": clean_line(line),
    }


def match_annex(line: str) -> dict[str, str] | None:
    """匹配 Annex 标题行，并返回附件编号和标题。"""
    # Annex 标题也可能是 Markdown 标题，因此先去掉 #。
    stripped = strip_markdown_heading(line)
    match = ANNEX_RE.match(stripped)
    if not match:
        return None
    # Annex 后缀可能是罗马数字、数字或单个字母。
    annex_suffix = (match.group("id") or "").strip()
    # tail 是 Annex 同一行中编号后的标题。
    tail = re.sub(r"^[\s.\-–—:]+", "", match.group("tail") or "").strip()
    # 无后缀时就是 ANNEX，有后缀时形成 ANNEX I 等稳定编号。
    annex_id = f"ANNEX {annex_suffix}".strip()
    return {
        "annex_id": annex_id,
        "heading": stripped,
        "title": tail,
    }


def match_hierarchy(line: str) -> tuple[str, str] | None:
    """匹配 TITLE / CHAPTER / Section / PART 等层级标题。"""
    # 去掉 Markdown # 后再检查层级。
    stripped = strip_markdown_heading(line)
    # 按预定义规则顺序匹配层级类型。
    for level, pattern in HIERARCHY_PATTERNS:
        if pattern.match(stripped):
            return level, stripped
    return None


def update_hierarchy(context: dict[str, str], level: str, value: str):
    """更新当前层级上下文，并清空低层级上下文。"""
    # 设置当前命中的层级值。
    context[level] = value
    if level == "title":
        # 新 TITLE 下，旧 CHAPTER/Section/PART 都不再适用。
        for child in ("chapter", "section", "part"):
            context[child] = ""
    elif level == "chapter":
        # 新 CHAPTER 下，旧 Section/PART 不再适用。
        for child in ("section", "part"):
            context[child] = ""
    elif level == "section":
        # 新 Section 下，旧 PART 不再适用。
        context["part"] = ""


def _is_heading_or_boundary(line: str) -> bool:
    """判断某行是否是 Article 标题、Annex 标题、层级标题或 Whereas 边界。"""
    return bool(match_article(line) or match_annex(line) or match_hierarchy(line) or WHEREAS_RE.match(line))


def _looks_like_article_title(line: str) -> bool:
    """判断 Article 标题是否可能位于 Article 下一行。"""
    # 去掉 Markdown # 后判断标题形态。
    stripped = strip_markdown_heading(line)
    if not stripped:
        return False
    # 过长文本更可能是正文，不当作标题。
    if len(stripped) > 180:
        return False
    # 数字段落不是标题。
    if re.match(r"^\d+\.\s+", stripped):
        return False
    # `(a)`、`(i)` 等条文项不是标题。
    if re.match(r"^\([a-zivxlcdm]+\)\s+", stripped, re.IGNORECASE):
        return False
    # Article 标题通常不以句号结束；句号结尾的单行更可能是正文。
    if stripped.endswith("."):
        return False
    # 通过以上排除后，认为它可以作为 Article 标题。
    return True


def _current_clause_ends_with_colon(current: dict[str, Any] | None) -> bool:
    """判断当前 ProvisionClause 是否刚进入冒号引导清单。"""
    if not current:
        return False
    for line in reversed(current.get("_lines") or []):
        stripped = str(line or "").strip()
        if stripped:
            return stripped.endswith(":")
    return False


def _looks_like_numbered_list_item(content: str) -> bool:
    """判断 `1. xxx` 是否更像冒号后的清单项，而非 Article 一级条款。"""
    text = str(content or "").strip()
    if not text:
        return False
    if text[:1].islower():
        return True
    words = text.split()
    has_legal_verb = re.search(r"\b(shall|must|may|is|are|has|have|means|applies|apply)\b", text, re.IGNORECASE)
    return len(words) <= 10 and text.endswith((";", ".")) and not has_legal_verb


def _is_quoted_article_heading(line: str) -> bool:
    """识别修订替换文本中被引用的 Article 标题行。"""
    stripped = strip_markdown_heading(line).strip()
    return bool(re.match(r"^['\"“‘]?Article\s+\d+[A-Za-z]?\b", stripped, re.IGNORECASE))


def _line_number_map(lines: list[str]) -> list[int]:
    """为当前行列表生成 1-based 行号映射。"""
    return list(range(1, len(lines) + 1))


def _extract_recitals(lines: list[str], start_index: int, end_index: int) -> list[dict[str, Any]]:
    """在指定行范围内抽取 Whereas/recital 条款。"""
    # recitals 保存最终输出列表。
    recitals = []
    # current 保存当前正在收集的 recital。
    current = None
    # 只扫描 Whereas 区间，不进入 Article 正文。
    for index in range(start_index, end_index):
        line = lines[index]
        match = RECITAL_RE.match(line)
        if match:
            # 遇到新 recital 时，先收尾上一条 recital。
            if current:
                current["content"] = clean_text("\n".join(current.pop("_lines")))
                recitals.append(current)
            # 初始化新的 recital，行号使用 1-based。
            current = {
                "number": match.group("number"),
                "line_start": index + 1,
                "line_end": index + 1,
                "_lines": [match.group("content").strip()],
            }
        elif current:
            # 非新编号行会被追加到当前 recital，处理跨行 recital。
            current["_lines"].append(line)
            current["line_end"] = index + 1
    # 循环结束后收尾最后一条 recital。
    if current:
        current["content"] = clean_text("\n".join(current.pop("_lines")))
        recitals.append(current)
    return recitals


def _build_selected_recitals(recitals: list[dict[str, Any]], max_keyword_hits: int = 8) -> list[dict[str, Any]]:
    """从完整 Whereas 中选择有限条目作为文件级 LLM 输入。"""
    # 没有 recitals 时直接返回空列表。
    if not recitals:
        return []
    # 用编号集合去重，避免首尾和关键词命中的 recital 重复加入。
    selected_numbers = set()
    # selected 按加入顺序保存。
    selected = []

    def add(items: list[dict[str, Any]]):
        """把若干 recital 加入 selected，同时按编号去重。"""
        for item in items:
            number = item.get("number")
            if number not in selected_numbers:
                selected_numbers.add(number)
                selected.append(item)

    # 开头若干 recital 通常交代背景、目的或制定必要性。
    add(recitals[:5])
    # 末尾若干 recital 通常交代实施、比例原则或成员国相关事项。
    add(recitals[-3:])

    # 关键词命中的 recital 更可能对法规文件属性有帮助。
    keyword_re = re.compile(
        r"\b(objective|aim|purpose|scope|risk|member states|economic operators|"
        r"regulation\s+\(eu\)|directive|tfeu|charter|paris agreement|necessary|proportionate)\b",
        re.IGNORECASE,
    )
    # 只取前 max_keyword_hits 条，控制 LLM 输入长度。
    keyword_hits = [item for item in recitals if keyword_re.search(item.get("content", ""))]
    add(keyword_hits[:max_keyword_hits])
    return selected


def _extract_article_units(article: dict[str, Any]) -> list[dict[str, Any]]:
    """从 Article 正文中抽取 paragraph/point/subpoint/dash_item 结构单元。"""
    # units 保存输出结构单元列表。
    units = []
    # current 保存当前正在收集的结构单元。
    current = None
    # Article 正文按行扫描。
    content_lines = article.get("content", "").splitlines()
    # 结构单元行号要相对原文件行号，因此需要 Article 正文起始行。
    start_line = int(article.get("content_line_start") or article.get("line_start") or 1)

    def flush(end_line: int):
        """收尾当前结构单元。"""
        nonlocal current
        if not current:
            return
        # 把临时 _lines 合并成 text 字段。
        current["text"] = clean_text("\n".join(current.pop("_lines")))
        # 记录结束行号。
        current["line_end"] = end_line
        # 写入输出列表。
        units.append(current)
        # 清空当前状态。
        current = None

    # 逐行识别结构单元起点。
    for offset, raw_line in enumerate(content_lines):
        # absolute_line 是原始法规文件中的 1-based 行号。
        absolute_line = start_line + offset
        # stripped 用于正则匹配，不改变原文 raw_line。
        stripped = raw_line.strip()
        # matched 保存命中的结构单元类型和正则结果。
        matched = None
        for level, pattern in STRUCTURAL_UNIT_PATTERNS:
            match = pattern.match(stripped)
            if match:
                matched = (level, match)
                break
        if matched:
            # 新结构单元开始前，先收尾上一个结构单元。
            flush(max(start_line, absolute_line - 1))
            level, match = matched
            # number 对 paragraph/point/subpoint 有值，dash_item 用 `-` 兜底。
            number = match.groupdict().get("number") or "-"
            # content 是去掉编号后的正文片段。
            content = match.groupdict().get("content") or stripped
            current = {
                "unit_level": level,
                "unit_number": number,
                "line_start": absolute_line,
                "line_end": absolute_line,
                "_lines": [content],
            }
        elif current:
            # 没有新单元但当前单元存在，说明这是该单元的续行。
            current["_lines"].append(raw_line)
            current["line_end"] = absolute_line
    # 扫描结束后收尾最后一个结构单元。
    flush(start_line + len(content_lines) - 1)
    return units


def extract_provision_clauses(article: dict[str, Any]) -> list[dict[str, Any]]:
    """从单个 Article 正文中抽取 v1 ProvisionClause。

    显式 ProvisionClause 只由行首 `1. `、`2. `、`3. ` 等一级编号生成。
    若 Article 正文不存在该类一级编号，则整个 Article 正文作为隐式 ProvisionClause。
    """
    article_number = str(article.get("article_number") or "").strip()
    content = article.get("content") or ""
    content_lines = content.splitlines()
    start_line = int(article.get("content_line_start") or article.get("line_start") or 1)
    clauses: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None

    def flush(end_line: int) -> None:
        nonlocal current
        if not current:
            return
        current.pop("_inside_numbered_list", None)
        current.pop("_inside_quoted_article", None)
        current["unit_content"] = clean_text("\n".join(current.pop("_lines")))
        current["line_end"] = max(int(current.get("line_start") or end_line), end_line)
        clauses.append(current)
        current = None

    for offset, raw_line in enumerate(content_lines):
        absolute_line = start_line + offset
        match = PROVISION_CLAUSE_RE.match(raw_line)
        if match:
            if current and (
                (_current_clause_ends_with_colon(current) and _looks_like_numbered_list_item(match.group("content")))
                or (current.get("_inside_numbered_list") and _looks_like_numbered_list_item(match.group("content")))
                or current.get("_inside_quoted_article")
            ):
                if _looks_like_numbered_list_item(match.group("content")):
                    current["_inside_numbered_list"] = True
                current["_lines"].append(raw_line)
                current["line_end"] = absolute_line
                continue
            flush(max(start_line, absolute_line - 1))
            local_number = match.group("number")
            current = {
                "unit_number": f"{article_number}({local_number})" if article_number else f"({local_number})",
                "unit_level": "paragraph",
                "source_article_number": article_number,
                "clause_index": len(clauses) + 1,
                "explicit_boundary": True,
                "line_start": absolute_line,
                "line_end": absolute_line,
                "_lines": [match.group("content").strip()],
            }
        elif current:
            if _is_quoted_article_heading(raw_line):
                current["_inside_quoted_article"] = True
            current["_lines"].append(raw_line)
            current["line_end"] = absolute_line

    flush(start_line + len(content_lines) - 1)

    if clauses:
        return clauses

    stripped_content = clean_text(content)
    if not stripped_content:
        return []
    return [
        {
            "unit_number": article_number,
            "unit_level": "paragraph",
            "unit_content": stripped_content,
            "source_article_number": article_number,
            "clause_index": 1,
            "explicit_boundary": False,
            "line_start": start_line,
            "line_end": start_line + max(len(content_lines), 1) - 1,
        }
    ]


def _infer_document_title(file_header: str, filename: str = "") -> str:
    """从文件头中推断法规标题。"""
    # 逐行扫描文件头，寻找包含 REGULATION/DIRECTIVE/DECISION 的标题行。
    for raw_line in file_header.splitlines():
        line = strip_markdown_heading(raw_line)
        if not line:
            continue
        # 到 Having regard 或 Whereas 后通常已离开标题区。
        if line.lower().startswith(("having regard", "whereas")):
            break
        if re.search(r"\b(REGULATION|DIRECTIVE|DECISION)\b", line, re.IGNORECASE):
            return line
    # 找不到标题时退回文件名。
    return filename


def _infer_document_type(title: str) -> str:
    """根据标题推断法规类型。"""
    # 统一大写后进行关键词判断。
    title_upper = (title or "").upper()
    if "IMPLEMENTING REGULATION" in title_upper:
        return "Implementing Regulation"
    if "DELEGATED REGULATION" in title_upper:
        return "Delegated Regulation"
    if "REGULATION" in title_upper:
        return "Regulation"
    if "DIRECTIVE" in title_upper:
        return "Directive"
    if "DECISION" in title_upper:
        return "Decision"
    return ""


def _infer_document_number(title: str) -> str:
    """根据标题推断法规编号。"""
    # 支持 `(EU) 2023/956`、`No 1907/2006` 等常见编号格式。
    match = re.search(r"(?:No\s+)?(?:\((?:EU|EC|EEC|Euratom)\)\s*)?\d{4}/\d+|No\s+\d+/\d{4}", title or "")
    return match.group(0).strip() if match else ""


def _infer_publication_date(title: str) -> str:
    """根据标题推断发布日期或通过日期。"""
    # 格式一标题中常见 `of 10 May 2023` 形式。
    match = re.search(r"\bof\s+(\d{1,2}\s+[A-Za-z]+\s+\d{4})\b", title or "")
    return match.group(1) if match else ""


def _infer_issuing_authority(title: str, file_header: str) -> str:
    """根据标题和文件头推断发布机关。"""
    # 同时查看标题和文件头，提高 Commission/Council 等关键词命中率。
    source = f"{title}\n{file_header}".upper()
    if "EUROPEAN PARLIAMENT AND OF THE COUNCIL" in source or "EUROPEAN PARLIAMENT AND THE COUNCIL" in source:
        return "European Parliament and Council"
    if "THE COUNCIL" in source or "COUNCIL DIRECTIVE" in source or "COUNCIL REGULATION" in source:
        return "Council"
    if "COMMISSION" in source:
        return "Commission"
    return ""


def split_format_one_document(
    text: str,
    filename: str = "",
    include_annex_content: bool = False,
) -> dict[str, Any]:
    """将格式一英文法规 Markdown 切分成文件头、Whereas、Article 和 Annex。"""
    # 第一步：统一换行，得到原始行列表。
    normalized = normalize_newlines(text)
    original_lines = normalized.split("\n")
    # 第二步：解析并移除文首合规风险类型。
    risk_metadata, lines = parse_risk_header(original_lines)
    # 第三步：生成行号映射；移除风险类型行后，行号对应处理后的行列表。
    line_numbers = _line_number_map(lines)

    # 找到第一条 Article，作为正文条文起点。
    first_article_index = None
    for index, line in enumerate(lines):
        if match_article(line):
            first_article_index = index
            break
    if first_article_index is None:
        raise ValueError("No Article heading found in format-one document")

    # 第一条 Article 之前的内容包含文件头、Having regard、Whereas 等。
    pre_article_lines = lines[:first_article_index]
    # whereas_index 用于定位 Whereas 区块起点。
    whereas_index = None
    for index, line in enumerate(pre_article_lines):
        if WHEREAS_RE.match(line):
            whereas_index = index
            break
    # 有些文件没有显式 Whereas 标题，但直接出现 `(1)` recital。
    if whereas_index is None:
        for index, line in enumerate(pre_article_lines):
            if RECITAL_RE.match(line):
                whereas_index = index
                break

    # 如果没有 Whereas，则第一条 Article 前全部作为文件头。
    if whereas_index is None:
        file_header_lines = pre_article_lines
        recital_start = first_article_index
    else:
        # 如果有 Whereas，则 Whereas 之前是文件头。
        file_header_lines = pre_article_lines[:whereas_index]
        # 如果当前行是 Whereas 标题，recital 从下一行开始；否则从当前 `(1)` 开始。
        recital_start = whereas_index + 1 if WHEREAS_RE.match(pre_article_lines[whereas_index]) else whereas_index

    # file_header 不含 Whereas，用于法规文件基础信息抽取。
    file_header = clean_text("\n".join(file_header_lines))
    # file_info 包含第一条 Article 前全部内容，用于保留更完整的文件级上下文。
    file_info = clean_text("\n".join(pre_article_lines))
    # recitals 只在识别到 Whereas 区间时抽取。
    recitals = _extract_recitals(lines, recital_start, first_article_index) if whereas_index is not None else []
    # selected_recitals 是喂给文件级 LLM 的受控子集。
    selected_recitals = _build_selected_recitals(recitals)

    # 规则兜底元数据，后续 LLM 失败或缺字段时会被 graph_builder 使用。
    title = _infer_document_title(file_header, filename)
    fallback_metadata = {
        "document_name": title,
        "document_type": _infer_document_type(title),
        "document_number": _infer_document_number(title),
        "publication_date": _infer_publication_date(title),
        "issuing_authority": _infer_issuing_authority(title, file_header),
    }

    # 正文扫描默认从第一条 Article 开始。
    body_scan_start = first_article_index
    # 如果文件头中存在 `HAVE ADOPTED THIS REGULATION`，则从其后开始扫描层级，保留 Article 前层级标题。
    for index, line in enumerate(lines[:first_article_index]):
        if re.search(r"\bHAVE\s+ADOPTED\s+THIS\s+(REGULATION|DIRECTIVE|DECISION)\b", line, re.IGNORECASE):
            body_scan_start = index + 1
            break

    # 当前层级上下文，随 TITLE/CHAPTER/Section/PART 更新。
    context = {"title": "", "chapter": "", "section": "", "part": ""}
    # articles 保存切分出的 Article 列表。
    articles: list[dict[str, Any]] = []
    # annexes 保存切分出的 Annex 元数据或可选全文。
    annexes: list[dict[str, Any]] = []
    # current_article 保存正在收集的 Article 元数据。
    current_article = None
    # current_article_lines 保存当前 Article 正文行。
    current_article_lines: list[str] = []
    # current_article_start 预留用于调试/扩展。
    current_article_start = None
    # current_article_content_start 记录 Article 正文起始行号。
    current_article_content_start = None
    # current_annex 保存正在收集的 Annex 元数据。
    current_annex = None
    # current_annex_lines 保存当前 Annex 原文行。
    current_annex_lines: list[str] = []

    def finish_article(end_line_index: int):
        """结束当前 Article 收集，并写入 articles。"""
        nonlocal current_article, current_article_lines, current_article_start, current_article_content_start
        if not current_article:
            return
        # 合并 Article 正文行。
        content = clean_text("\n".join(current_article_lines))
        # 写入正文和行号范围。
        current_article["content"] = content
        current_article["line_end"] = max(current_article.get("line_start", 1), end_line_index + 1)
        current_article["content_line_start"] = current_article_content_start or current_article.get("line_start")
        current_article["content_line_end"] = current_article["line_end"]
        # 标记该 Article 是否包含修订型表述。
        current_article["is_amendment_article"] = bool(AMENDMENT_RE.search(content))
        # 抽取 Article 内部 paragraph/point/subpoint 等结构单元。
        current_article["structural_units"] = _extract_article_units(current_article)
        # v1: Article 下一级条款单元由代码确定，供后续独立 ProvisionClause 抽取使用。
        current_article["provision_clauses"] = extract_provision_clauses(current_article)
        # 加入最终 Article 列表。
        articles.append(current_article)
        # 清空当前 Article 状态。
        current_article = None
        current_article_lines = []
        current_article_start = None
        current_article_content_start = None

    def finish_annex(end_line_index: int):
        """结束当前 Annex 收集，并写入 annexes。"""
        nonlocal current_annex, current_annex_lines
        if not current_annex:
            return
        # Annex 内容可能很大，默认只用于计算 char_count。
        content = clean_text("\n".join(current_annex_lines))
        # 记录 Annex 行号和字符数。
        current_annex["line_end"] = max(current_annex.get("line_start", 1), end_line_index + 1)
        current_annex["char_count"] = len(content)
        # 默认不抽取 Annex 正文入图。
        current_annex["content_extracted"] = False
        current_annex["skipped_by_default"] = True
        current_annex["skip_reason"] = "first_version_ignores_annex_content"
        # 调试或单独处理附件时可以显式打开 Annex 原文输出。
        if include_annex_content:
            current_annex["content"] = content
        annexes.append(current_annex)
        # 清空当前 Annex 状态。
        current_annex = None
        current_annex_lines = []

    # 从正文扫描起点开始运行状态机。
    index = body_scan_start
    while index < len(lines):
        line = lines[index]
        # Annex 只有在至少进入正文后才识别，避免误把文件头中的 Annex 文字当成附件。
        annex_match = match_annex(line) if articles or current_article or current_annex else None
        # Article 标题匹配。
        article_match = match_article(line)
        # 层级标题匹配。
        hierarchy_match = match_hierarchy(line)

        # 如果当前已经进入 Annex，则直到下一个 Annex 或文件末尾都归属 Annex。
        if current_annex:
            if annex_match:
                # 新 Annex 出现时，先收尾旧 Annex，再开启新 Annex。
                finish_annex(index - 1)
                current_annex = {
                    **annex_match,
                    "line_start": line_numbers[index],
                    "context": deepcopy(context),
                }
                current_annex_lines = [line]
            else:
                # 普通行继续追加到当前 Annex。
                current_annex_lines.append(line)
            index += 1
            continue

        # 如果识别到 Annex 起点，则先结束当前 Article，再进入 Annex 状态。
        if annex_match:
            finish_article(index - 1)
            current_annex = {
                **annex_match,
                "line_start": line_numbers[index],
                "context": deepcopy(context),
            }
            current_annex_lines = [line]
            index += 1
            continue

        # 如果识别到 Article 起点，则先结束上一个 Article，再开启新 Article。
        if article_match:
            finish_article(index - 1)
            # Article 标题可能在同一行，也可能在下一行。
            article_heading = article_match["inline_heading"]
            # 默认正文从 Article 标题下一行开始。
            content_start_index = index + 1
            if not article_heading:
                # 如果同一行没有标题，则向下跳过空行寻找可能标题。
                probe = index + 1
                while probe < len(lines) and not lines[probe].strip():
                    probe += 1
                # 下一条非空行如果不像边界也不像正文编号，则当作 Article 标题。
                if probe < len(lines) and not _is_heading_or_boundary(lines[probe]) and _looks_like_article_title(lines[probe]):
                    article_heading = strip_markdown_heading(lines[probe])
                    content_start_index = probe + 1
            # 创建当前 Article 元数据，并复制此时的层级上下文。
            current_article = {
                "article_number": article_match["article_number"],
                "article_index": article_match["article_index"],
                "article_heading": article_heading,
                "raw_heading_line": article_match["raw_heading_line"],
                "line_start": line_numbers[index],
                "classification_context": deepcopy(context),
                "title": context.get("title", ""),
                "chapter": context.get("chapter", ""),
                "section": context.get("section", ""),
                "part": context.get("part", ""),
            }
            # 记录当前 Article 的开始位置和正文开始行号。
            current_article_start = index
            current_article_content_start = line_numbers[content_start_index] if content_start_index < len(lines) else line_numbers[index]
            current_article_lines = []
            # 跳到正文起点继续扫描。
            index = content_start_index
            continue

        # 如果识别到层级标题，则收尾当前 Article，并更新上下文。
        if hierarchy_match:
            finish_article(index - 1)
            level, value = hierarchy_match
            update_hierarchy(context, level, value)
            index += 1
            continue

        # 普通行如果处于 Article 状态，就追加到当前 Article 正文。
        if current_article:
            current_article_lines.append(line)
        index += 1

    # 文件扫描完成后，收尾最后一个 Article 和 Annex。
    finish_article(len(lines) - 1)
    finish_annex(len(lines) - 1)

    # warnings 记录非致命结构问题。
    warnings = []
    if not recitals:
        warnings.append("no_recitals_detected")
    if not risk_metadata:
        warnings.append("missing_compliance_risk_type_header")
    elif not risk_metadata.get("risk_header_valid"):
        warnings.append("unknown_compliance_risk_type")

    # 返回完整切分结果，供 LLM 输入构造和图谱装配使用。
    return {
        **risk_metadata,
        "filename": filename,
        "document_format": "format_one_eu_regulation_directive",
        "file_info": file_info,
        "file_header": file_header,
        "recitals": recitals,
        "selected_recitals": selected_recitals,
        "clauses": articles,
        "annexes": annexes,
        "annexes_metadata": [
            {key: value for key, value in annex.items() if key != "content"}
            for annex in annexes
        ],
        "fallback_metadata": fallback_metadata,
        "stats": {
            "article_count": len(articles),
            "recital_count": len(recitals),
            "annex_count": len(annexes),
            "skipped_annex_count": len(annexes),
            "has_whereas": bool(recitals),
            "has_hierarchy": any(
                any(clause.get(key) for key in ("title", "chapter", "section", "part"))
                for clause in articles
            ),
        },
        "warnings": warnings,
    }

