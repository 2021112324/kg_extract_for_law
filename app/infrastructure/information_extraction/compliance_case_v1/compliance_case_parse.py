"""合规案例单阶段抽取的一阶段“输入整理”模块。

本文件只负责把原始案例文件整理成更稳定的单案例 LLM 输入，不负责抽取知识图谱。

设计边界非常重要：
1. 一个文件就是一个案例，本模块不做“案例边界识别”。
2. 本模块会识别 Markdown 标题、HTML 表格、TXT 字段标签等“已有结构”。
3. 本模块不会判断哪些文本是实体、哪些文本是关系，也不会生成最终图谱节点。
4. 输出的 ComplianceCaseDocument 是给 graph_extract.py 调 LLM 使用的中间结构。

换句话说，这里做的是“让模型看得更清楚”，不是“替模型抽取知识”。
"""

from __future__ import annotations

import html
import re
from dataclasses import asdict, dataclass, field
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Iterable


DEFAULT_DATA_DIR = Path(__file__).resolve().parent / "data"
CASE_LIBRARY_DIRNAME = "合规案例库"
RISK_CASE_DIRNAME = "合规风险案例"
SUPPORTED_SUFFIXES = {".md", ".txt"}


@dataclass
class ComplianceCaseDocument:
    """单个案例文件的结构化中间表示。

    字段含义：
    - case_id：本系统内部稳定案例 ID，用于图谱主节点 ID。
    - case_number：原始案例编号，如“案例1-1”中的 1-1 或 TXT 中的序号。
    - case_title：短标题，优先来自 Markdown 案例标题或 TXT 结论。
    - source_type：数据来源类型，目前为“合规案例库”或“合规风险案例”。
    - compliance_domain：合规领域，Markdown 案例一般可由章节标题得到。
    - keywords：关键词，优先来自 Markdown 表格或 TXT 风险点。
    - table_summary：Markdown 开头 HTML 表格中的摘要字段。
    - fields：按原始结构标题/字段标签提取出的结构内容。
    - full_text：清理后的全文，作为兜底和可追溯原文。
    - llm_input：最终送入大模型的单案例输入文本。
    """

    case_id: str
    case_number: str
    case_title: str
    source_type: str
    source_path: str
    compliance_domain: str = ""
    keywords: list[str] = field(default_factory=list)
    table_summary: dict[str, Any] = field(default_factory=dict)
    fields: dict[str, str] = field(default_factory=dict)
    full_text: str = ""
    llm_input: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class _TableCellParser(HTMLParser):
    """解析 Markdown 文件中的 HTML 表格。

    合规案例库的 Markdown 开头常有一段 `<table>...</table>`，里面包含：
    序号、案例、关键词、案例要点、主要相关法条、备注。

    这里不用正则硬拆 td/tr，而用 HTMLParser，原因是表格里可能有换行、
    HTML 转义字符或轻微格式变化，用 parser 更稳一点。
    """

    def __init__(self) -> None:
        super().__init__()
        self.rows: list[list[str]] = []
        self._current_row: list[str] | None = None
        self._current_cell: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        # 进入一行时创建行缓冲；进入单元格时创建单元格缓冲。
        if tag.lower() == "tr":
            self._current_row = []
        elif tag.lower() in {"td", "th"}:
            self._current_cell = []

    def handle_data(self, data: str) -> None:
        # HTMLParser 会把一个单元格的数据分多次回调，所以先累积。
        if self._current_cell is not None:
            self._current_cell.append(data)

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag in {"td", "th"} and self._current_cell is not None and self._current_row is not None:
            # 单元格结束时进行空白归一化，避免表格换行影响后续字段使用。
            self._current_row.append(_normalize_space("".join(self._current_cell)))
            self._current_cell = None
        elif tag == "tr" and self._current_row is not None:
            # 行结束时把非空行加入 rows。最终 rows[0] 通常是表头，rows[1] 是值。
            if self._current_row:
                self.rows.append(self._current_row)
            self._current_row = None


def discover_compliance_case_files(root_dir: str | Path | None = None) -> list[Path]:
    """发现待抽取的合规案例文件。

    参数：
    - root_dir 为空时，默认扫描当前模块下的 data 目录。
    - root_dir 是文件时，只在后缀受支持时返回该文件。
    - root_dir 是目录时，递归扫描 .md/.txt。

    返回值按路径排序，保证批处理顺序稳定。
    """

    root = Path(root_dir) if root_dir else DEFAULT_DATA_DIR
    if root.is_file():
        return [root] if root.suffix.lower() in SUPPORTED_SUFFIXES else []
    if not root.exists():
        return []
    return sorted(
        p
        for p in root.rglob("*")
        if p.is_file() and p.suffix.lower() in SUPPORTED_SUFFIXES and not p.name.startswith(".")
    )


def parse_compliance_case_file(file_path: str | Path) -> ComplianceCaseDocument:
    """解析单个案例文件，返回 ComplianceCaseDocument。

    这里是解析入口：
    1. 读取文本；
    2. 判断来源类型；
    3. 分别走 Markdown 或 TXT 解析；
    4. 构造最终 llm_input。

    注意：这里不会调用 LLM，也不会生成实体关系。
    """

    path = Path(file_path)
    text = _read_text(path)
    source_type = _detect_source_type(path, text)
    if source_type == CASE_LIBRARY_DIRNAME:
        document = _parse_case_library_markdown(path, text)
    else:
        document = _parse_risk_case_txt(path, text)
    document.llm_input = build_compliance_case_llm_input(document)
    return document


def build_compliance_case_llm_input(document: ComplianceCaseDocument) -> str:
    """把结构化中间结果组装成单案例 LLM 输入。

    设计原则：
    - 用【字段名】显式保留原有结构，减少模型自己猜标题边界。
    - 先放解析出的稳定信息，再放结构字段，最后放全文。
    - 全文必须保留，因为规则解析可能漏掉不规范小节，全文可作为兜底。
    """

    # parts 使用列表累积，最后 join，避免字符串反复拼接。
    parts = [
        "【数据来源】",
        document.source_type,
        "",
        "【案例ID】",
        document.case_id,
        "",
        "【案例编号】",
        document.case_number,
        "",
        "【案例标题】",
        document.case_title,
    ]
    if document.compliance_domain:
        parts.extend(["", "【合规领域】", document.compliance_domain])
    if document.keywords:
        parts.extend(["", "【关键词】", "、".join(document.keywords)])
    if document.table_summary:
        parts.extend(["", "【表格摘要】", _format_mapping(document.table_summary)])
    for key, value in document.fields.items():
        # fields 是原始结构内容；空字段不放入输入，减少模型噪声。
        if value.strip():
            parts.extend(["", f"【{key}】", value.strip()])
    parts.extend(["", "【原文全文】", document.full_text.strip()])
    return "\n".join(parts).strip()


def _parse_case_library_markdown(path: Path, text: str) -> ComplianceCaseDocument:
    """解析“合规案例库”Markdown 文件。

    这类文件通常包含：
    - 开头 HTML 表格：序号、案例、关键词、案例要点、主要相关法条、备注。
    - Markdown 标题：章节、合规领域、案例标题、案情简介、案例分析等。
    """

    clean_text = _clean_markdown_text(text)
    # 表格摘要是案例标题、关键词、案例类型、案例要点的重要来源。
    table_summary = _extract_first_table_summary(clean_text)
    # 标题用于识别合规领域和正文结构。
    headings = _extract_headings(clean_text)
    case_number, case_title = _extract_case_number_and_title(path, clean_text, table_summary)
    compliance_domain = _extract_compliance_domain(headings)
    keywords = _split_keywords(str(table_summary.get("关键词", "")))
    fields = _extract_markdown_sections(clean_text)
    return ComplianceCaseDocument(
        case_id=_make_case_id(CASE_LIBRARY_DIRNAME, path.stem),
        case_number=case_number or path.stem,
        case_title=case_title or str(table_summary.get("案例", "")) or path.stem,
        source_type=CASE_LIBRARY_DIRNAME,
        source_path=str(path),
        compliance_domain=compliance_domain,
        keywords=keywords,
        table_summary=table_summary,
        fields=fields,
        full_text=clean_text,
    )


def _parse_risk_case_txt(path: Path, text: str) -> ComplianceCaseDocument:
    """解析“合规风险案例”TXT 文件。

    这类文件一般是固定字段：
    序号、企业材料、结论、分析、风险点、法律依据、处置方案。
    """

    clean_text = _normalize_newlines(text).strip()
    fields = _extract_txt_fields(clean_text)
    case_number = fields.get("序号") or _extract_digits(path.stem) or path.stem
    title = _build_risk_case_title(case_number, fields)
    keywords = _infer_keywords_from_risk_points(fields.get("风险点", ""))
    return ComplianceCaseDocument(
        case_id=_make_case_id(RISK_CASE_DIRNAME, case_number),
        case_number=case_number,
        case_title=title,
        source_type=RISK_CASE_DIRNAME,
        source_path=str(path),
        keywords=keywords,
        fields={k: v for k, v in fields.items() if k != "序号"},
        full_text=clean_text,
    )


def _read_text(path: Path) -> str:
    """按常见中文文本编码读取文件。

    项目数据大多是 UTF-8，但历史数据可能混有 UTF-8 BOM 或 GB18030。
    如果这些编码都失败，最后用 errors=ignore 兜底，保证批处理不中断。
    """

    for encoding in ("utf-8", "utf-8-sig", "gb18030"):
        try:
            return path.read_text(encoding=encoding)
        except UnicodeDecodeError:
            continue
    return path.read_text(encoding="utf-8", errors="ignore")


def _detect_source_type(path: Path, text: str) -> str:
    """判断案例来源类型。

    优先使用目录名和文件后缀：
    - 位于“合规案例库”目录或 .md：按 Markdown 案例库处理。
    - 位于“合规风险案例”目录或包含“企业材料”：按 TXT 风险案例处理。
    """

    parts = set(path.parts)
    if CASE_LIBRARY_DIRNAME in parts or path.suffix.lower() == ".md":
        return CASE_LIBRARY_DIRNAME
    if RISK_CASE_DIRNAME in parts or "企业材料" in text:
        return RISK_CASE_DIRNAME
    return RISK_CASE_DIRNAME if path.suffix.lower() == ".txt" else CASE_LIBRARY_DIRNAME


def _clean_markdown_text(text: str) -> str:
    """清理 Markdown 文本中的展示噪声。

    这里只做轻量清理：
    - 统一换行；
    - 删除脚注上标；
    - 反转义 HTML 字符。

    不会删除正文内容，也不会做语义压缩。
    """

    text = _normalize_newlines(text)
    text = re.sub(r"<sup>.*?</sup>", "", text, flags=re.I | re.S)
    text = re.sub(r"\$\{\s*\}\^\{?\d+\}?", "", text)
    return html.unescape(text).strip()


def _normalize_newlines(text: str) -> str:
    """统一 Windows/macOS/Linux 换行，方便后续正则按行匹配。"""

    return text.replace("\r\n", "\n").replace("\r", "\n")


def _normalize_space(text: str) -> str:
    """把连续空白折叠成一个空格，并反转义 HTML 字符。"""

    return re.sub(r"\s+", " ", html.unescape(text or "")).strip()


def _extract_first_table_summary(text: str) -> dict[str, str]:
    """抽取 Markdown 文件中的第一张 HTML 表格摘要。

    当前案例库一般只有开头一张摘要表，所以只取第一张表。
    返回格式示例：
    {
        "序号": "1.",
        "案例": "国企境外公司高管受贿案",
        "关键词": "国企高管、海外工程项目、受贿",
        ...
    }
    """

    match = re.search(r"<table.*?</table>", text, flags=re.I | re.S)
    if not match:
        return {}
    parser = _TableCellParser()
    parser.feed(match.group(0))
    rows = parser.rows
    if len(rows) < 2:
        return {}
    headers = rows[0]
    values = rows[1]
    return {headers[i]: values[i] for i in range(min(len(headers), len(values))) if headers[i]}


def _extract_headings(text: str) -> list[str]:
    """提取 Markdown 标题文本，不保留 # 层级。"""

    return [
        _normalize_space(match.group(1))
        for match in re.finditer(r"^#{1,6}\s+(.+?)\s*$", text, flags=re.M)
        if _normalize_space(match.group(1))
    ]


def _extract_case_number_and_title(path: Path, text: str, table_summary: dict[str, str]) -> tuple[str, str]:
    """提取案例编号和案例标题。

    优先匹配正文标题，例如：
    # 案例1：国企境外公司高管受贿案

    如果正文标题不存在，则退回到文件名数字和表格“案例”列。
    """

    match = re.search(r"^#\s*案例\s*([0-9一二三四五六七八九十百千万-]+)\s*[：:]\s*(.+?)\s*$", text, flags=re.M)
    if match:
        return match.group(1).strip(), _normalize_space(match.group(2))
    number = _extract_digits(path.stem)
    return number or path.stem, str(table_summary.get("案例", "")).strip()


def _extract_compliance_domain(headings: list[str]) -> str:
    """从标题序列中识别合规领域。

    经验规则：
    - 第一个标题通常是“章节”，跳过。
    - 第二个标题通常是合规领域，如“反腐败反商业贿赂”。
    - 遇到“案例...”标题后停止，避免把案例标题误当领域。
    """

    ignored = {"章节"}
    for heading in headings:
        if heading in ignored:
            continue
        if heading.startswith("案例"):
            break
        return heading
    return ""


def _extract_markdown_sections(text: str) -> dict[str, str]:
    """按 Markdown 标题抽取主要结构段落。

    这一步只是切出结构内容，例如“案情简介”“案例分析”“法律依据”。
    如果同一结构在正文中出现多次，会合并为同一个字段，保留原文顺序。
    """

    section_aliases = [
        ("案情简介", r"[（(]\s*[一1]\s*[)）]\s*案情简介|案情简介"),
        ("案例分析", r"[（(]\s*[二2]\s*[)）]\s*案例分析|案例分析"),
        ("法律依据", r"法律依据"),
        ("结论", r"结论"),
        ("案例启示", r"案例提示|案例启示|合规提示|风险提示"),
    ]
    heading_re = re.compile(r"^#{1,6}\s*(.+?)\s*$", re.M)
    matches = list(heading_re.finditer(text))
    fields: dict[str, str] = {}
    for idx, match in enumerate(matches):
        heading = _normalize_space(match.group(1))
        key = _match_section_key(heading, section_aliases)
        if not key:
            continue
        # 当前标题到下一个标题之间的文本，就是该结构的内容。
        start = match.end()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(text)
        content = text[start:end].strip()
        if content:
            if key in fields:
                fields[key] = f"{fields[key]}\n\n{content}"
            else:
                fields[key] = content
    return fields


def _match_section_key(heading: str, aliases: list[tuple[str, str]]) -> str:
    """把标题文本映射到标准结构字段名。"""

    normalized = re.sub(r"^[·\\-]\s*", "", heading)
    for key, pattern in aliases:
        if re.search(pattern, normalized):
            return key
    return ""


def _extract_txt_fields(text: str) -> dict[str, str]:
    """按固定字段标签切分合规风险案例 TXT。

    字段标签必须单独成行，例如：
    企业材料
    ...
    结论
    ...

    如果找不到任何标签，则把全文放入“企业材料”，保证不会丢文本。
    """

    labels = ["序号", "企业材料", "结论", "分析", "风险点", "法律依据", "处置方案"]
    label_re = re.compile(rf"^({'|'.join(map(re.escape, labels))})\s*$", re.M)
    matches = list(label_re.finditer(text))
    fields: dict[str, str] = {}
    for idx, match in enumerate(matches):
        label = match.group(1)
        start = match.end()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(text)
        fields[label] = text[start:end].strip()
    if not fields:
        fields["企业材料"] = text.strip()
    return fields


def _split_keywords(value: str) -> list[str]:
    """拆分表格中的关键词字段。"""

    if not value:
        return []
    parts = re.split(r"[、,，;；\s]+", value)
    return [p.strip() for p in parts if p.strip()]


def _infer_keywords_from_risk_points(value: str) -> list[str]:
    """从风险点文本中提取可用关键词。

    这不是指标映射，只是为了给案例节点提供关键词候选。
    指标 ID 仍然必须由后处理匹配指标知识库生成，不能在这里臆造。
    """

    if not value:
        return []
    names: list[str] = []
    for quoted in re.findall(r"《([^》]+)》", value):
        if quoted not in names:
            names.append(quoted)
    for match in re.findall(r"\d+(?:\.\d+)*\s*([^；;。]+)", value):
        name = _normalize_space(match).strip("- ")
        if name and name not in names:
            names.append(name)
    return names[:12]


def _build_risk_case_title(case_number: str, fields: dict[str, str]) -> str:
    """为 TXT 风险案例生成短标题。

    TXT 文件通常没有标题，因此优先使用“结论”的前 40 个字符；
    没有结论时再用“企业材料”的前 40 个字符。
    """

    conclusion = _normalize_space(fields.get("结论", ""))
    if conclusion:
        return conclusion[:40]
    material = _normalize_space(fields.get("企业材料", ""))
    return material[:40] if material else f"合规风险案例{case_number}"


def _extract_digits(value: str) -> str:
    """从文件名或标题中提取数字编号，如 合规风险案例100 -> 100。"""

    match = re.search(r"\d+(?:-\d+)?", value)
    return match.group(0) if match else ""


def _make_case_id(source_type: str, raw_id: str) -> str:
    """生成稳定案例 ID。

    案例 ID 会作为“案例”主节点 node_id，因此要尽量短、稳定、可读。
    """

    safe_source = re.sub(r"\s+", "", source_type)
    safe_raw = re.sub(r"[^\w一-龥.-]+", "_", str(raw_id).strip())
    return f"case_{safe_source}_{safe_raw}".strip("_")


def _format_mapping(value: dict[str, Any]) -> str:
    """把字典格式化为多行“键：值”，用于 LLM 输入。"""

    return "\n".join(f"{key}：{item}" for key, item in value.items() if item not in (None, ""))


def iter_parsed_documents(root_dir: str | Path | None = None) -> Iterable[ComplianceCaseDocument]:
    """惰性遍历解析结果，适合批处理逐文件处理。"""

    for file_path in discover_compliance_case_files(root_dir):
        yield parse_compliance_case_file(file_path)
