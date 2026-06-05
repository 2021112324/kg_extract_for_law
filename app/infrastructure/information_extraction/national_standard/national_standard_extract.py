"""国家标准第一阶段结构解析模块。

本模块对应 national_standard/task/task2.md 中的“第一阶段：结构解析”任务。
它的职责不是做知识抽取本身，而是把 MinerU 输出目录、content_list JSON、
full.md、普通 Markdown/TXT 等输入整理成稳定的结构化中间结果，供第二阶段
大模型按照文件信息 schema 和通用树节点 schema 继续抽取。

核心产物包括：
1. 文件基本信息 file_info。
2. 前言、引言、目次文本。
3. 正文章节树 body_tree。
4. 附录树 appendix_tree。
5. 表格、图片资源节点。
6. 规范性引用文件候选。
7. 结构校验和编号修复日志。
"""

import json
import logging
import mimetypes
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import quote

from app.infrastructure.string_utils.id_tool import generate_hex_uuid
from app.infrastructure.storage.object_storage.base import StorageConfig
from app.infrastructure.storage.object_storage.minio_adapter import MinIOAdapter


NATIONAL_STANDARD_MINIO_URL = os.getenv("NATIONAL_STANDARD_MINIO_URL", "http://60.205.171.106:9001/")
NATIONAL_STANDARD_MINIO_API_URL = os.getenv(
    "NATIONAL_STANDARD_MINIO_API_URL",
    NATIONAL_STANDARD_MINIO_URL.rstrip("/").replace(":9001", ":9000"),
)
NATIONAL_STANDARD_MINIO_ACCESS_KEY = os.getenv("NATIONAL_STANDARD_MINIO_ACCESS_KEY", "admin")
NATIONAL_STANDARD_MINIO_SECRET_KEY = os.getenv("NATIONAL_STANDARD_MINIO_SECRET_KEY", "hit-wE8sR9wQ3pG1")
NATIONAL_STANDARD_MINIO_BUCKET = os.getenv("NATIONAL_STANDARD_MINIO_BUCKET", "2-3project")
NATIONAL_STANDARD_MINIO_PREFIX = os.getenv("NATIONAL_STANDARD_MINIO_PREFIX", "国家标准")


# 标准编号识别规则。
# 主要用于文件封面、引用文件章节和表格中识别 GB、GB/T、HJ、ISO 等标准编号。
# 示例：GB 15258—2009、GB/T 16483、HJ 75、ISO 14001。
STANDARD_NO_RE = re.compile(
    r"\b(?P<code>GB(?:/T|/Z)?|HJ(?:/T)?|ISO|IEC|YY(?:/T)?|NB|DL|JB|AQ|SN|QB|HG|CJ|CJJ)"
    r"\s*[A-Z0-9./-]*\s*\d+(?:[—-]\d{4})?\b",
    re.IGNORECASE,
)

# 引用文件行识别规则。
# 与 STANDARD_NO_RE 不同，这里要求整行以标准编号开头，并把编号后的内容作为标准名称候选。
# 示例：GB 12268 危险货物品名表。
REFERENCE_RE = re.compile(
    r"^(?P<number>(?:GB(?:/T|/Z)?|HJ(?:/T)?|ISO|IEC|YY(?:/T)?|NB|DL|JB|AQ|SN|QB|HG|CJ|CJJ)"
    r"\s*[A-Z0-9./-]*\s*\d+(?:[—-]\d{4})?)\s*(?P<name>.*)$",
    re.IGNORECASE,
)

# 日期识别规则。
# 支持 2009-06-21、2009年6月21日、2009.06.21 等常见写法。
DATE_RE = re.compile(r"(?P<date>\d{4}[-年.]\d{1,2}[-月.]\d{1,2}日?)")

# 正文章节标题识别规则。
# 示例：1 范围、4.2 安全要求、4.2.1 标签内容。
SECTION_TITLE_RE = re.compile(r"^(?P<number>\d+(?:\.\d+)*)(?:\s+|　+)(?P<title>.+)$")

# 附录标题识别规则。
# 示例：附录A（规范性附录）标签样例、附录 B（资料性附录）说明。
APPENDIX_TITLE_RE = re.compile(
    r"^附录\s*(?P<code>[A-Za-zＡ-Ｚａ-ｚ])\s*(?:[（(](?P<type>规范性附录|资料性附录)[）)])?\s*(?P<title>.*)$"
)

# 附录内部章节标题识别规则。
# 示例：A.1 样例说明、B.2.1 计算方法。
APPENDIX_SECTION_RE = re.compile(r"^(?P<number>[A-Za-zＡ-Ｚ]\.\d+(?:\.\d+)*)(?:\s+|　+)(?P<title>.+)$")

# Markdown 图片链接识别规则。
# 示例：![图1 标签样例](images/fig1.png)。
MARKDOWN_IMAGE_RE = re.compile(r"!\[(?P<caption>[^\]]*)]\((?P<path>[^)]+)\)")

# Markdown 中常见的 HTML 图片与 HTML 表格。
# MinerU 生成的 full.md 经常把复杂表格输出成单行 <table>...</table>，
# 表格内的图片则以 <img src="images/xxx.jpg"/> 形式出现。
HTML_IMAGE_RE = re.compile(
    r"<img\b[^>]*\bsrc=[\"'](?P<path>[^\"']+)[\"'][^>]*>",
    re.IGNORECASE,
)
HTML_TABLE_RE = re.compile(r"<table\b.*?</table>", re.IGNORECASE | re.DOTALL)

# Markdown 标题识别规则。
# 示例：# 1 范围、## 4.2 标签内容。
HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<text>.+?)\s*$")


def _clean_text(text: Any) -> str:
    """清理用于规则判断的文本。

    该函数只用于标题、编号、日期等规则匹配前的轻量归一化：
    - None 转为空字符串。
    - 全角空格转半角空格。
    - 连续空白压缩成一个空格。
    """
    return re.sub(r"\s+", " ", str(text or "").replace("\u3000", " ")).strip()


def _normalize_fullwidth_alpha(text: str) -> str:
    """将全角英文字母转为半角英文字母。

    MinerU 或 OCR 结果中，附录编号可能出现全角字母，例如 `Ａ.1`。
    后续要把它统一成 `A.1`，便于路径、编号和校验逻辑处理。
    """
    result = []
    for char in str(text or ""):
        code = ord(char)
        if 0xFF21 <= code <= 0xFF3A:
            result.append(chr(code - 0xFF21 + ord("A")))
        elif 0xFF41 <= code <= 0xFF5A:
            result.append(chr(code - 0xFF41 + ord("a")))
        else:
            result.append(char)
    return "".join(result)


def _section_level(number: str) -> int:
    """根据章节编号计算树层级。

    例如：
    - `1` 对应 level 1。
    - `4.2` 对应 level 2。
    - `4.2.1` 对应 level 3。
    """
    return len(str(number or "").split("."))


def _parent_number(number: str) -> str:
    """返回编号的父级编号。

    示例：`4.2.1` 的父级是 `4.2`。
    """
    parts = str(number or "").split(".")
    return ".".join(parts[:-1]) if len(parts) > 1 else ""


def _previous_sibling(number: str) -> str:
    """返回同级前一个编号。

    示例：`4.2.3` 的前一个同级编号是 `4.2.2`。
    该函数主要用于判断 `4.22` 是否应修复为 `4.2.2`。
    """
    parts = str(number or "").split(".")
    if not parts or not parts[-1].isdigit() or int(parts[-1]) <= 1:
        return ""
    parts[-1] = str(int(parts[-1]) - 1)
    return ".".join(parts)


def _line_list(text: str) -> list[str]:
    """把多行文本拆成去空白后的非空行列表。"""
    return [line.strip() for line in str(text or "").splitlines() if line.strip()]


def _compact_label(text: str) -> str:
    """压缩结构标题中的空白，例如 `目 次` -> `目次`。"""
    return re.sub(r"\s+", "", str(text or ""))


def _normalize_md_heading_text(text: str) -> str:
    """归一化 Markdown 标题/行首编号中的 OCR 常见点号。

    只处理章节编号前缀，例如 `3．1 标志尺寸` -> `3.1 标志尺寸`。
    正文里的全角符号不在这里做全局替换，避免改变标准原文内容。
    """
    value = str(text or "").strip()
    match = re.match(r"^(?P<number>\d+(?:[.．。]\d+)*)(?P<rest>\s+|　+.+|.+)?$", value)
    if not match:
        return value
    number = match.group("number").replace("．", ".").replace("。", ".")
    rest = value[len(match.group("number")) :]
    return f"{number}{rest}"


def _sanitize_minio_path_part(text: str) -> str:
    """清理 MinIO 对象路径中的非法或高风险字符。

    MinIO 的 object name 可以包含 `/` 表示层级，但任务要求“文件名需过滤非法字符”，
    因此对文件名、图片名等路径片段过滤 Windows/URL 中常见的危险字符。
    """
    cleaned = re.sub(r'[\\/:*?"<>|\x00-\x1f]', "_", str(text or "").strip())
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" .")
    return cleaned or "未命名"


def _normalize_minio_endpoint(minio_url: str) -> tuple[str, bool, str]:
    """把任务给出的 MinIO URL 拆成 endpoint、secure 和 public_base_url。"""
    url = str(minio_url or "").strip().rstrip("/")
    secure = url.startswith("https://")
    endpoint = re.sub(r"^https?://", "", url)
    return endpoint, secure, url


@dataclass
class StandardBlock:
    """解析过程中的统一 block 结构。

    不同输入来源的结构差别很大：
    - MinerU content_list 里可能有 title、paragraph、table、image。
    - full.md 中只有 Markdown 行。
    - TXT 中甚至没有显式类型。

    因此先统一成 StandardBlock，后续构建章节树时只处理这一种格式。
    """

    # block_id 用于溯源；如果原始输入没有 ID，就自动生成。
    block_id: str

    # 标准化类型：title、paragraph、table、image、equation 等。
    block_type: str

    # block 的文本主体。表格可保存 Markdown/HTML 文本，图片可保存图注。
    text: str = ""

    # 页码、坐标和资源路径用于后续溯源、图片检查、可能的页面范围恢复。
    page_idx: int | None = None
    bbox: Any = None
    path: str = ""

    # 保留原始 block，避免第一阶段丢信息，后续可以继续扩展解析字段。
    raw: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """输出可 JSON 序列化的轻量 block 信息。"""
        return {
            "block_id": self.block_id,
            "type": self.block_type,
            "text": self.text,
            "page_idx": self.page_idx,
            "bbox": self.bbox,
            "path": self.path,
        }


class NationalStandardExtractor:
    """国家标准第一阶段结构解析器。

    该类只做规则解析和结构校验，不调用大模型。输出结果用于第二阶段
    `文件信息 schema` 和 `通用树节点 schema` 的输入构造。
    """

    def __init__(
        self,
        upload_images: bool = True,
        minio_url: str = NATIONAL_STANDARD_MINIO_URL,
        minio_api_url: str = NATIONAL_STANDARD_MINIO_API_URL,
        minio_access_key: str = NATIONAL_STANDARD_MINIO_ACCESS_KEY,
        minio_secret_key: str = NATIONAL_STANDARD_MINIO_SECRET_KEY,
        minio_bucket: str = NATIONAL_STANDARD_MINIO_BUCKET,
        minio_prefix: str = NATIONAL_STANDARD_MINIO_PREFIX,
    ):
        """初始化国家标准解析器。

        upload_images=True 时，会在第一阶段解析后把图片上传到任务指定的 MinIO，
        并把图片节点中的 minio_url 更新为线上链接。上传失败不会中断解析流程，
        而是记录到 validation.image_uploads 中。
        """
        self.upload_images = upload_images
        self.minio_url = minio_url
        self.minio_api_url = minio_api_url
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key
        self.minio_bucket = minio_bucket
        self.minio_prefix = minio_prefix
        self._minio_adapter: MinIOAdapter | None = None
        self._minio_public_base_url = minio_url.rstrip("/")

    def extract(self, input_path: str | Path) -> dict[str, Any]:
        """对一个国家标准输入路径执行完整第一阶段解析。

        input_path 可以是：
        - MinerU 输出目录。
        - full_fixed.md / full.md / Markdown / TXT 文件。
        """
        # 第一步：识别目录内可用的结构化文件、Markdown 文件、图片目录等。
        source = self.discover_source(input_path)

        # 第二步：把来源文件转换成统一的 StandardBlock 序列。
        blocks = self.load_blocks(source)

        # 第三步：在 block 序列上构建文件信息、章节树、资源节点和校验结果。
        return self.extract_from_blocks(blocks, source)

    def discover_source(self, input_path: str | Path) -> dict[str, Path | None]:
        """发现输入路径中的可用文件。

        这个函数只负责“找文件”，不做解析。这样后续可以明确知道：
        - 是否有人工审查后的 full_fixed.md。
        - 是否有 full.md / Markdown / TXT。
        - 图片目录在哪里。
        - 是否存在 layout/model/pdf 等未来可利用的文件。
        """
        path = Path(input_path)
        if not path.exists():
            raise FileNotFoundError(f"国家标准输入路径不存在: {path}")

        source: dict[str, Path | None] = {
            # root 用于解析相对路径，例如 images/xxx.png。
            "root": path if path.is_dir() else path.parent,
            "content_list": None,
            "full_md": None,
            "reviewed_md": None,
            "layout": None,
            "model": None,
            "pdf": None,
            "images_dir": None,
            "risk_type_file": None,
            "input_file": None,
        }
        if path.is_file():
            # V2 只接受 Markdown/TXT 作为直接输入，避免误用未审查的 JSON 结构。
            source["input_file"] = path
            if path.suffix.lower() in {".md", ".txt"}:
                source["full_md"] = path
                if path.stem.lower() in {"full_fixed", "reviewed", "reviewed_full"}:
                    source["reviewed_md"] = path
            return source

        # 目录输入时，按 V2 规则只寻找 Markdown/TXT 主输入。
        json_files = list(path.glob("*.json"))
        source["reviewed_md"] = self._first_existing(
            [
                path / "full_fixed.md",
                path / "reviewed.md",
                path / "reviewed_full.md",
                path / "full_fixed.txt",
                path / "reviewed.txt",
            ]
        )
        source["full_md"] = source["reviewed_md"] or self._first_existing(
            [
                path / "full.md",
                *path.glob("*.md"),
                *[item for item in path.glob("*.txt") if item.name != "合规风险类型.txt"],
            ]
        )
        source["layout"] = self._first_existing([p for p in json_files if "layout" in p.name.lower()])
        source["model"] = self._first_existing([p for p in json_files if "model" in p.name.lower()])
        source["pdf"] = self._first_existing(list(path.glob("*origin.pdf")) + list(path.glob("*.pdf")))
        source["images_dir"] = path / "images" if (path / "images").exists() else None
        source["risk_type_file"] = path / "合规风险类型.txt" if (path / "合规风险类型.txt").exists() else None
        return source

    @staticmethod
    def _first_existing(paths: Iterable[Path]) -> Path | None:
        """返回候选路径列表中第一个真实存在的文件。"""
        for path in paths:
            if path.exists():
                return path
        return None

    def load_blocks(self, source: dict[str, Path | None]) -> list[StandardBlock]:
        """根据 source 加载 Markdown/TXT 为 StandardBlock 列表。

        V2 以人工可审查的 Markdown 为唯一主输入：
        - 优先 full_fixed.md / reviewed.md。
        - 其次 full.md。
        - 不再默认读取 content_list_v2.json。
        """
        if source.get("full_md"):
            return self._load_markdown_blocks(Path(source["full_md"]))  # type: ignore[arg-type]
        raise ValueError("未发现可解析的 full_fixed.md、reviewed.md、full.md、md 或 txt 文件")

    def extract_from_blocks(self, blocks: list[StandardBlock], source: dict[str, Path | None] | None = None) -> dict[str, Any]:
        """在标准 block 列表上执行完整结构解析。

        该函数是最适合单元测试的入口：测试时可以直接构造 StandardBlock，
        不必依赖真实 MinerU 目录。
        """
        source = source or {"root": None, "images_dir": None}

        # 文件级信息只从封面/正文开始前区域进行规则抽取。
        file_info = self.extract_file_info(blocks, source)

        # 章节树、附录树、表格、图片在一次顺序扫描中完成构建。
        body_tree, appendix_tree, tables, images, repairs = self.build_trees(blocks, source)

        # 任务2要求：将图片上传到 MinIO，并把原图片链接替换为线上链接。
        image_uploads = self.upload_and_replace_image_links(images, tables, blocks, source)

        # 引用候选依赖章节树和表格，所以在 build_trees 后执行。
        references = self.extract_reference_candidates(body_tree, appendix_tree, tables)

        # 校验结果和编号修复日志统一写入 validation。
        validation = self.validate_structure(body_tree, appendix_tree, images, source)
        validation["number_repairs"] = repairs
        validation["image_uploads"] = image_uploads
        validation["review_status"] = "reviewed" if source.get("reviewed_md") else "unreviewed"
        validation["failed_data"] = (
            self.collect_validation_failed_data(body_tree, appendix_tree, images, validation)
            if not validation.get("passed")
            else []
        )

        # 输出结构保持清晰分区，便于第二阶段分别构造不同 schema 的输入。
        return {
            "source": {key: str(value) if value else None for key, value in source.items()},
            "review_status": validation["review_status"],
            "file_info": file_info,
            "preface": self._collect_named_section_text(blocks, "前言"),
            "introduction": self._collect_named_section_text(blocks, "引言"),
            "toc": self._collect_named_section_text(blocks, "目次"),
            "reference_text": self._collect_reference_text(blocks),
            "body_tree": body_tree,
            "appendix_tree": appendix_tree,
            "tables": tables,
            "images": images,
            "reference_candidates": references,
            "validation": validation,
            "blocks": [block.to_dict() for block in blocks],
        }

    def _load_content_list_blocks(self, path: Path) -> list[StandardBlock]:
        """读取 MinerU content_list JSON 并转换为 StandardBlock。

        兼容几种常见 JSON 结构：
        - 顶层就是 list。
        - 顶层 dict 中包含 content_list。
        - 顶层 dict 中包含 blocks/items。

        同时使用递归 walk，是为了兼容某些解析结果把 block 嵌在 children/items/content 中。
        """
        data = json.loads(path.read_text(encoding="utf-8"))
        items = data if isinstance(data, list) else data.get("content_list") or data.get("blocks") or data.get("items") or []
        blocks: list[StandardBlock] = []

        def walk(value: Any) -> None:
            """递归展开 content_list 中可能存在的嵌套 block。"""
            if isinstance(value, list):
                for item in value:
                    walk(item)
                return
            if not isinstance(value, dict):
                return

            # 尽可能兼容不同字段命名：type、block_type、category 都可能表示块类型。
            block_type = str(value.get("type") or value.get("block_type") or value.get("category") or "").lower()
            text = self._extract_block_text(value)

            # 有类型、有文本或有图片路径，才认为这是一个有效 block。
            if block_type or text or value.get("img_path") or value.get("image_path"):
                normalized_type = self._normalize_block_type(block_type, value, text)
                blocks.append(
                    StandardBlock(
                        block_id=value.get("id") or value.get("block_id") or generate_hex_uuid(),
                        block_type=normalized_type,
                        text=text,
                        page_idx=value.get("page_idx") or value.get("page") or value.get("page_no"),
                        bbox=value.get("bbox") or value.get("poly") or value.get("position"),
                        path=self._extract_block_path(value),
                        raw=value,
                    )
                )

            # 继续展开可能的子 block。若 child 是普通文本，walk 会自动忽略。
            for child_key in ("value", "children", "items", "blocks"):
                child = value.get(child_key)
                if isinstance(child, (list, dict)):
                    walk(child)

        walk(items)
        return blocks

    def _load_markdown_blocks(self, path: Path) -> list[StandardBlock]:
        """读取 Markdown/TXT，并用简单规则切成 StandardBlock。

        full.md 是 content_list 不可用时的重要兜底输入。这里不试图做复杂 Markdown
        AST 解析，而是按国家标准常见结构进行轻量识别：
        - `# 标题` 转为 title block。
        - 连续 Markdown 表格行合并为 table block。
        - 图片链接转为 image block。
        - 其他连续文本合并为 paragraph block。
        """
        lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
        blocks: list[StandardBlock] = []
        paragraph_buffer: list[str] = []
        table_buffer: list[str] = []

        def flush_paragraph() -> None:
            """把累计的普通段落写入 blocks。"""
            if paragraph_buffer:
                blocks.append(StandardBlock(generate_hex_uuid(), "paragraph", "\n".join(paragraph_buffer).strip()))
                paragraph_buffer.clear()

        def flush_table() -> None:
            """把累计的 Markdown 表格写入 blocks。"""
            if table_buffer:
                blocks.append(StandardBlock(generate_hex_uuid(), "table", "\n".join(table_buffer).strip()))
                table_buffer.clear()

        def append_html_images(text: str) -> None:
            """把一段 HTML/Markdown 文本里的图片链接额外登记为 image block。"""
            for match in HTML_IMAGE_RE.finditer(text):
                blocks.append(
                    StandardBlock(
                        generate_hex_uuid(),
                        "image",
                        "",
                        path=match.group("path").strip(),
                        raw={"source": "markdown_html_image"},
                    )
                )

        for line in lines:
            stripped = line.strip()
            if not stripped:
                flush_table()
                flush_paragraph()
                continue

            heading = HEADING_RE.match(stripped)
            image = MARKDOWN_IMAGE_RE.search(stripped)
            html_images = list(HTML_IMAGE_RE.finditer(stripped))
            is_html_table = bool(HTML_TABLE_RE.search(stripped))
            is_table_line = stripped.startswith("|") and stripped.endswith("|")

            if heading:
                # 标题会打断当前段落或表格。
                flush_table()
                flush_paragraph()
                blocks.append(
                    StandardBlock(
                        generate_hex_uuid(),
                        "title",
                        _normalize_md_heading_text(heading.group("text").strip()),
                    )
                )
            elif is_html_table:
                flush_table()
                flush_paragraph()
                blocks.append(
                    StandardBlock(
                        generate_hex_uuid(),
                        "table",
                        stripped,
                        raw={"source_format": "markdown_html"},
                    )
                )
                append_html_images(stripped)
            elif image:
                # 图片独立成 block，方便后续建资源节点并校验路径。
                flush_table()
                flush_paragraph()
                blocks.append(
                    StandardBlock(
                        generate_hex_uuid(),
                        "image",
                        image.group("caption").strip(),
                        path=image.group("path").strip(),
                    )
                )
            elif html_images:
                flush_table()
                flush_paragraph()
                append_html_images(stripped)
                text_without_images = HTML_IMAGE_RE.sub("", stripped).strip()
                if text_without_images:
                    paragraph_buffer.append(text_without_images)
            elif is_table_line:
                # 连续表格行合并为一个 table block。
                flush_paragraph()
                table_buffer.append(stripped)
            else:
                flush_table()

                # 某些 TXT 或 Markdown 中，标题没有 #，但行文本像 `4.2.1 标签内容`。
                title_text = self._title_text_from_line(_normalize_md_heading_text(stripped))
                if title_text:
                    flush_paragraph()
                    blocks.append(StandardBlock(generate_hex_uuid(), "title", title_text))
                else:
                    paragraph_buffer.append(stripped)

        flush_table()
        flush_paragraph()
        return blocks

    @staticmethod
    def _extract_block_text(value: dict[str, Any]) -> str:
        """从 MinerU block 中尽可能提取可读文本。

        不同版本或不同解析器字段名可能不一致，因此这里按多个常见字段兜底。
        如果存在 lines 列表，则把每一行的 text/content 拼接起来。
        """
        for key in ("text", "content", "md", "markdown", "table_body", "table_html", "html", "caption"):
            item = value.get(key)
            if isinstance(item, str) and item.strip():
                return item.strip()
            if isinstance(item, (dict, list)):
                nested_text = NationalStandardExtractor._extract_nested_content_text(item)
                if nested_text:
                    return nested_text
        lines = value.get("lines")
        if isinstance(lines, list):
            texts = []
            for line in lines:
                if isinstance(line, dict):
                    texts.append(str(line.get("text") or line.get("content") or ""))
                else:
                    texts.append(str(line))
            return "\n".join(text for text in texts if text.strip()).strip()
        return ""

    @staticmethod
    def _extract_block_path(value: dict[str, Any]) -> str:
        """从 MinerU block 中提取图片或资源路径。"""
        for key in ("img_path", "image_path", "path"):
            item = value.get(key)
            if isinstance(item, str) and item.strip():
                return item.strip()
        content = value.get("content")
        if isinstance(content, dict):
            image_source = content.get("image_source")
            if isinstance(image_source, dict):
                path = image_source.get("path")
                if isinstance(path, str) and path.strip():
                    return path.strip()
        return ""

    @staticmethod
    def _extract_nested_content_text(value: Any) -> str:
        """递归提取 MinerU 嵌套 content 结构中的文本。

        当前测试数据中常见结构类似：

        ```json
        {
          "content": {
            "title_content": [
              {"type": "text", "content": "1 范围"}
            ]
          }
        }
        ```

        因此需要递归遍历 dict/list，把内部 text 或 equation_inline 等 content 拼起来。
        """
        texts: list[str] = []

        def collect(item: Any) -> None:
            if isinstance(item, str):
                if item.strip():
                    texts.append(item.strip())
                return
            if isinstance(item, list):
                for child in item:
                    collect(child)
                return
            if not isinstance(item, dict):
                return

            item_type = str(item.get("type") or "")
            content = item.get("content")
            if isinstance(content, str) and item_type in {"text", "equation_inline", "equation", ""}:
                if content.strip():
                    texts.append(content.strip())
                return
            if isinstance(content, (dict, list)):
                collect(content)

            for key in (
                "title_content",
                "paragraph_content",
                "table_content",
                "image_caption",
                "page_header_content",
                "page_footer_content",
                "page_number_content",
            ):
                if key in item:
                    collect(item[key])

        collect(value)
        return " ".join(texts).strip()

    @staticmethod
    def _normalize_block_type(block_type: str, value: dict[str, Any], text: str) -> str:
        """把原始 block 类型归一为少数几类。

        后续树构建主要关心 title、paragraph、table、image。
        对没有明确类型但文本像标题的 block，也归一成 title。
        """
        lowered = block_type.lower()
        if "table" in lowered:
            return "table"
        if "image" in lowered or "figure" in lowered or value.get("img_path") or value.get("image_path"):
            return "image"
        if "title" in lowered:
            return "title"
        if "equation" in lowered:
            return "equation"
        if HEADING_RE.match(text) or SECTION_TITLE_RE.match(text) or APPENDIX_TITLE_RE.match(text):
            return "title"
        return "paragraph"

    @staticmethod
    def _title_text_from_line(line: str) -> str:
        """判断一行普通文本是否可以当成标题。

        这个函数用于 Markdown/TXT 兜底解析，因为很多 full.md 中标题可能没有 `#`。
        """
        if SECTION_TITLE_RE.match(line) or APPENDIX_TITLE_RE.match(line) or APPENDIX_SECTION_RE.match(line):
            return line
        if _compact_label(line) in {"前言", "引言", "目次", "范围", "规范性引用文件", "术语和定义"}:
            return line
        return ""

    def extract_file_info(
        self,
        blocks: list[StandardBlock],
        source: dict[str, Path | None] | None = None,
    ) -> dict[str, Any]:
        """从封面区域抽取文件基本信息。

        当前实现是规则抽取，不追求完全覆盖，只先抓取第二阶段最有价值的元数据。
        封面区域从文档开头开始，到前言、引言、目次、正文第一章或附录标题为止。
        """
        cover_lines: list[str] = []
        for block in blocks:
            text = _clean_text(block.text)
            if not text:
                continue

            # 遇到正文起点后停止收集封面信息，避免把正文里的标准编号误当文件编号。
            if self._is_body_start(text) or APPENDIX_TITLE_RE.match(text):
                break
            cover_lines.extend(_line_list(block.text))

        cover_text = "\n".join(cover_lines)

        # dict.fromkeys 用于在保持顺序的同时去重。
        standard_numbers = list(dict.fromkeys(match.group(0).strip() for match in STANDARD_NO_RE.finditer(cover_text)))
        dates = [match.group("date") for match in DATE_RE.finditer(cover_text)]

        # 优先从带“发布/实施”关键词的行中找日期；找不到时退化为按出现顺序取日期。
        release_date = self._date_near_keyword(cover_text, ("发布", "发布日期")) or (dates[0] if dates else "")
        implement_date = self._date_near_keyword(cover_text, ("实施", "实施日期")) or (dates[1] if len(dates) > 1 else "")
        chinese_name, english_name = self._guess_standard_names(cover_lines, standard_numbers[0] if standard_numbers else "")
        compliance_risk_type = self._read_compliance_risk_type(source or {})

        return {
            "标准中文名称": chinese_name,
            "标准英文名称": english_name,
            "标准编号": standard_numbers[0] if standard_numbers else "",
            "合规风险类型": compliance_risk_type,
            "标准性质": self._guess_standard_type(standard_numbers[0] if standard_numbers else "", cover_text),
            "发布日期": release_date,
            "实施日期": implement_date,
            "发布单位": self._guess_release_org(cover_text),
            "代替标准": self._extract_replaced_standards(cover_text),
            "ICS": self._extract_code_after_label(cover_text, "ICS"),
            "CCS": self._extract_code_after_label(cover_text, "CCS"),
            "标准状态": self._guess_standard_status(cover_text),
            "适用范围摘要": "",
            "封面文本": cover_text,
        }

    @staticmethod
    def _read_compliance_risk_type(source: dict[str, Path | None]) -> str:
        """读取标准目录下的 `合规风险类型.txt`。

        文件中可能只写枚举值，例如 `产品法律风险`；也可能写成
        `合规风险类型：产品法律风险`。输出统一为纯枚举值文本。
        """
        path = source.get("risk_type_file")
        if not path:
            return ""
        try:
            text = Path(path).read_text(encoding="utf-8", errors="ignore").strip()
        except Exception as exc:
            logging.warning("读取合规风险类型文件失败: %s, %s", path, exc)
            return ""
        text = re.sub(r"\s+", " ", text).strip()
        if not text:
            return ""
        match = re.match(r"^合规风险类型\s*[:：]\s*(?P<value>.+)$", text)
        return match.group("value").strip() if match else text

    def build_trees(
        self,
        blocks: list[StandardBlock],
        source: dict[str, Path | None],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
        """顺序扫描 block，构建正文树、附录树、表格节点和图片节点。

        该函数是第一阶段的核心。

        设计要点：
        - 正文和附录分别建树，避免附录被错误挂到最后一个正文节点下。
        - 使用 stack 维护当前正文路径，使用 appendix_stack 维护当前附录路径。
        - 表格和图片独立成资源节点，同时把资源 ID 挂到当前章节节点。
        - 目录行直接跳过，避免目次中的章节被重复建节点。
        """
        root_body: list[dict[str, Any]] = []
        root_appendix: list[dict[str, Any]] = []
        appendix_by_code: dict[str, dict[str, Any]] = {}

        # stack 保存当前正文路径，如 [4, 4.2, 4.2.1]。
        stack: list[dict[str, Any]] = []

        # appendix_stack 保存当前附录路径，如 [附录A, A.1]。
        appendix_stack: list[dict[str, Any]] = []
        current_node: dict[str, Any] | None = None

        # in_appendix 表示当前扫描位置是否已经进入附录区。
        in_appendix = False
        in_reference = False

        # in_toc 表示当前处在目次区域。目次中的章节和附录只是索引，
        # 不能参与正文树/附录树构建。
        in_toc = False

        # seen_numbers 用于编号修复评分。例如如果已经见过 4.2 和 4.2.1，
        # 那么后续出现 4.22 时更可能是 4.2.2。
        seen_numbers: set[str] = set()
        tables: list[dict[str, Any]] = []
        images: list[dict[str, Any]] = []

        # repairs 记录所有自动编号修复，后续写入 validation.number_repairs。
        repairs: list[dict[str, Any]] = []

        for block in blocks:
            text = _clean_text(block.text)
            if not text and block.block_type not in {"table", "image"}:
                continue

            # 预先计算各种标题匹配，避免在分支里重复正则。
            appendix_title = APPENDIX_TITLE_RE.match(text)
            appendix_section = APPENDIX_SECTION_RE.match(text)
            section_title = SECTION_TITLE_RE.match(text)
            compact_text = _compact_label(text)
            named_section = compact_text in {"前言", "引言", "目次"}
            reference_section = compact_text in {"参考文献", "参考文件", "参考资料"}

            if block.block_type == "title" and named_section:
                if compact_text == "目次":
                    in_toc = True
                else:
                    in_toc = False
                # 前言/引言/目次不进入正文树。它们通过 _collect_named_section_text 独立输出。
                # 同时清空栈，避免后续段落被误挂到之前章节。
                current_node = None
                stack = []
                appendix_stack = []
                in_appendix = False
                in_reference = False
                continue

            if block.block_type == "title" and reference_section:
                # 参考文献是文件级“标准依据”的抽取来源，不应挂到最后一个附录节点下。
                current_node = None
                stack = []
                appendix_stack = []
                in_appendix = False
                in_reference = True
                continue

            if in_reference:
                # 参考文献全文由 _collect_reference_text 单独输出给 file_info LLM。
                continue

            if in_toc:
                # 真实正文一般从 `1 范围` 开始。如果没有前言/引言分隔，
                # 遇到非目录形式的 1 号章节时退出目次并继续处理该 block。
                if section_title and section_title.group("number") == "1" and not self._looks_like_toc_line(text):
                    in_toc = False
                else:
                    continue

            if self._looks_like_toc_line(text):
                # 目次中的 `1 范围 .... 1`、`附录A .... 8` 不是正文节点，必须跳过。
                continue

            if block.block_type == "title" and appendix_title:
                # 进入新的附录。附录作为 appendix_tree 的根节点。
                in_appendix = True
                code = _normalize_fullwidth_alpha(appendix_title.group("code")).upper()
                title = appendix_title.group("title").strip()
                app_type = appendix_title.group("type") or ""
                if code in appendix_by_code:
                    current_node = appendix_by_code[code]
                    if title and current_node.get("title") in {"", f"附录{code}"}:
                        current_node["title"] = title
                    if app_type and not current_node.get("appendix_type"):
                        current_node["appendix_type"] = app_type
                    current_node["source_blocks"].append(block.block_id)
                    appendix_stack = [current_node]
                    continue
                node = self._new_node(
                    number=f"附录{code}",
                    title=title or f"附录{code}",
                    level=1,
                    block=block,
                    is_appendix=True,
                    appendix_code=code,
                    appendix_type=app_type,
                )
                root_appendix.append(node)
                appendix_by_code[code] = node
                appendix_stack = [node]
                current_node = node
                continue

            if block.block_type == "title" and section_title and self._looks_like_real_section(text):
                # 处理正文章节标题。若已经进入附录，则这类数字编号也会挂到附录树。
                original_number = section_title.group("number")
                title = section_title.group("title").strip()

                if in_appendix and re.match(r"^\d+$", original_number):
                    previous_roots = [
                        int(node.get("number"))
                        for node in root_body
                        if str(node.get("number", "")).isdigit()
                    ]
                    if previous_roots and int(original_number) > max(previous_roots):
                        if root_appendix and current_node is root_appendix[-1]:
                            removed = root_appendix.pop()
                            appendix_by_code.pop(str(removed.get("appendix_code") or ""), None)
                        in_appendix = False
                        appendix_stack = []

                # 对 4.22 / 422 这类疑似漏点编号做保守修复。
                number, repair = self._repair_number(original_number, title, seen_numbers)
                if repair:
                    repairs.append(repair)
                seen_numbers.add(number)
                level = _section_level(number)
                node = self._new_node(number=number, title=title, level=level, block=block, is_appendix=in_appendix)
                if original_number != number:
                    node["original_number"] = original_number
                    node["normalized_number"] = number
                self._push_node(node, appendix_stack if in_appendix else stack, root_appendix if in_appendix else root_body)
                current_node = node
                continue

            if block.block_type == "title" and self._looks_like_missing_top_section_title(text, current_node, in_appendix):
                previous_roots = [
                    int(node.get("number"))
                    for node in root_body
                    if str(node.get("number", "")).isdigit()
                ]
                if previous_roots:
                    number = str(max(previous_roots) + 1)
                    node = self._new_node(number=number, title=text, level=1, block=block, is_appendix=False)
                    node["inferred_number"] = True
                    node["repair_reason"] = "标题疑似缺失一级章节编号，按前序章节顺序补全"
                    root_body.append(node)
                    stack = [node]
                    current_node = node
                    seen_numbers.add(number)
                    repairs.append(
                        {
                            "original_number": "",
                            "normalized_number": number,
                            "repair_reason": f"标题“{text}”疑似缺失一级章节编号，按前序章节顺序补全",
                        }
                    )
                    continue

            if block.block_type == "title" and in_appendix and appendix_section:
                # 附录内部编号，例如 A.1、A.1.1。
                number = _normalize_fullwidth_alpha(appendix_section.group("number")).upper()
                title = appendix_section.group("title").strip()
                node = self._new_node(number=number, title=title, level=_section_level(number), block=block, is_appendix=True)
                self._push_node(node, appendix_stack, root_appendix)
                current_node = node
                continue

            if block.block_type == "table":
                # 表格独立成资源节点，同时关联到当前章节。
                table = self._build_table_node(block, current_node)
                tables.append(table)
                if current_node is not None:
                    current_node["tables"].append(table["table_id"])
                    current_node["source_blocks"].append(block.block_id)
                continue

            if block.block_type == "image":
                # 图片独立成资源节点，同时关联到当前章节。
                image = self._build_image_node(block, current_node, source)
                images.append(image)
                if current_node is not None:
                    current_node["images"].append(image["image_id"])
                    current_node["source_blocks"].append(block.block_id)
                continue

            if current_node is not None:
                # 普通段落归属到最近的章节/附录节点。
                current_node["content"] = (current_node.get("content", "") + "\n" + block.text).strip()
                current_node["source_blocks"].append(block.block_id)

        # 术语和定义章节中，MinerU 常把 `3.1` 与术语标题拆成不同 block。
        # 树构建完成后做专项后处理，避免术语分点全部堆在父节点 content 中。
        self._split_term_definition_nodes(root_body, repairs)

        # 树构建完成后统一刷新 path 和 full_path_title。
        self._refresh_paths(root_body)
        self._refresh_paths(root_appendix)
        return root_body, root_appendix, tables, images, repairs

    def extract_reference_candidates(
        self,
        body_tree: list[dict[str, Any]],
        appendix_tree: list[dict[str, Any]],
        tables: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """抽取规范性引用文件候选。

        第一阶段只做“候选抽取”，不直接断言最终知识图谱实体。
        后续第二阶段可以基于这些候选再用 schema 做精抽和归一化。

        候选来源：
        - 标题像 `2 规范性引用文件` 的章节。
        - 其他正文中出现标准编号的章节。
        - 表格中出现标准编号的内容。
        """
        candidates: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()

        def add_from_text(text: str, source_number: str, source_title: str) -> None:
            """从一段文本中逐行识别引用文件。"""
            for line in _line_list(text):
                match = REFERENCE_RE.match(_clean_text(line))
                if not match:
                    continue
                number = match.group("number").strip()
                name = match.group("name").strip()
                key = (number, name)
                if key in seen:
                    # 同一编号和名称只保留一次，避免章节和表格重复产生候选。
                    continue
                seen.add(key)
                candidates.append(
                    {
                        "reference_id": generate_hex_uuid(),
                        "标准编号": number,
                        "标准名称": name,
                        "是否注日期": bool(re.search(r"[—-]\d{4}$", number)),
                        "年份": self._year_from_number(number),
                        "标准类型": self._standard_type_from_number(number),
                        "引用说明": line,
                        "source_section_number": source_number,
                        "source_section_title": source_title,
                    }
                )

        for node in self._walk_nodes(body_tree + appendix_tree):
            # 规范性引用文件章节是主来源；正文其他位置出现标准编号也作为候选保留。
            if self._is_reference_section(node) or STANDARD_NO_RE.search(node.get("content", "")):
                add_from_text(node.get("content", ""), node.get("number", ""), node.get("title", ""))
        for table in tables:
            # 表格里的引用标准也不能丢，尤其是某些标准会用表格列出引用文件。
            if STANDARD_NO_RE.search(table.get("table_markdown", "")):
                add_from_text(table.get("table_markdown", ""), table.get("related_section_number", ""), table.get("related_section_title", ""))
        return candidates

    def validate_structure(
        self,
        body_tree: list[dict[str, Any]],
        appendix_tree: list[dict[str, Any]],
        images: list[dict[str, Any]],
        source: dict[str, Path | None],
    ) -> dict[str, Any]:
        """对第一阶段解析结果做结构校验。

        校验分为两类：
        - issues：相对严重的问题，会让 passed=False。
        - warnings：提示性问题，不阻断流程，例如图片文件不存在。
        """
        issues: list[dict[str, Any]] = []
        warnings: list[dict[str, Any]] = []

        # 校验正文一级章节编号，如 1、2、3、4 是否连续。
        top_numbers = [node["number"] for node in body_tree if str(node.get("number", "")).isdigit()]
        self._validate_number_continuity(top_numbers, "正文一级章节", issues)

        # 校验每个节点的同级子章节是否可能有跳号。
        for node in self._walk_nodes(body_tree):
            children = [child.get("number", "") for child in node.get("children", []) if child.get("number")]
            numeric_children = [num for num in children if re.match(r"^\d+(?:\.\d+)*$", num)]
            self._validate_sibling_last_number_continuity(numeric_children, node.get("number", ""), warnings)

        # 附录编号不能重复，例如两个“附录A”通常说明目录行或标题识别有问题。
        appendix_codes = [node.get("appendix_code") for node in appendix_tree if node.get("appendix_code")]
        if len(appendix_codes) != len(set(appendix_codes)):
            issues.append({"type": "duplicate_appendix", "message": "存在重复附录编号", "appendix_codes": appendix_codes})

        # 图片路径校验：只做存在性检查，不读取图片内容。
        images_dir = source.get("images_dir")
        root = source.get("root")
        for image in images:
            local_path = image.get("local_path")
            if not local_path:
                warnings.append({"type": "image_path_empty", "message": "图片路径为空", "image_id": image["image_id"]})
                continue
            candidate = Path(local_path)
            if not candidate.is_absolute():
                # MinerU 的 Markdown 通常写成 images/xxx.jpg。
                # 若直接相对 images_dir 拼接，会变成 images/images/xxx.jpg。
                # 因此带 images 前缀时优先相对 root，普通文件名才相对 images_dir。
                first_part = candidate.parts[0].lower() if candidate.parts else ""
                base = root if first_part == "images" else (images_dir or root)
                candidate = Path(base) / candidate if base else candidate
            if not candidate.exists():
                warnings.append(
                    {
                        "type": "image_missing",
                        "message": "图片文件不存在",
                        "image_id": image["image_id"],
                        "path": str(candidate),
                    }
                )

        return {
            "passed": not issues,
            "issues": issues,
            "warnings": warnings,
        }

    def collect_validation_failed_data(
        self,
        body_tree: list[dict[str, Any]],
        appendix_tree: list[dict[str, Any]],
        images: list[dict[str, Any]],
        validation: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """记录触发结构校验问题的数据内容，方便人工回查。

        这里不尝试“自动修正文义”，只把问题对应的节点/图片摘出来。
        """
        records: list[dict[str, Any]] = []
        node_by_number = {
            str(node.get("number", "")): node
            for node in self._walk_nodes(body_tree + appendix_tree)
            if node.get("number")
        }
        image_by_id = {str(image.get("image_id", "")): image for image in images}

        for issue in validation.get("issues", []) or []:
            if issue.get("type") == "number_continuity":
                for number in issue.get("numbers", []) or []:
                    node = node_by_number.get(str(number))
                    if node:
                        records.append(self._validation_node_record("issue", issue, node))
            elif issue.get("type") == "duplicate_appendix":
                for node in appendix_tree:
                    if node.get("appendix_code") in set(issue.get("appendix_codes", []) or []):
                        records.append(self._validation_node_record("issue", issue, node))

        for warning in validation.get("warnings", []) or []:
            if warning.get("type") == "sibling_number_continuity":
                parent = str(warning.get("parent", ""))
                node = node_by_number.get(parent)
                if node:
                    records.append(self._validation_node_record("warning", warning, node))
            elif warning.get("type") in {"image_missing", "image_path_empty"}:
                image = image_by_id.get(str(warning.get("image_id", "")))
                if image:
                    records.append(
                        {
                            "level": "warning",
                            "type": warning.get("type", ""),
                            "message": warning.get("message", ""),
                            "image_id": image.get("image_id", ""),
                            "local_path": image.get("local_path", ""),
                            "related_section_number": image.get("related_section_number", ""),
                            "related_section_title": image.get("related_section_title", ""),
                        }
                    )
        return records

    @staticmethod
    def _validation_node_record(level: str, problem: dict[str, Any], node: dict[str, Any]) -> dict[str, Any]:
        """构造结构校验问题对应的节点摘录。"""
        content = str(node.get("content") or "")
        return {
            "level": level,
            "type": problem.get("type", ""),
            "message": problem.get("message", ""),
            "number": node.get("number", ""),
            "title": node.get("title", ""),
            "path": node.get("path", ""),
            "full_path_title": node.get("full_path_title", ""),
            "content_preview": content[:500],
            "child_numbers": [child.get("number", "") for child in node.get("children", [])],
        }

    def _new_node(
        self,
        number: str,
        title: str,
        level: int,
        block: StandardBlock,
        is_appendix: bool,
        appendix_code: str = "",
        appendix_type: str = "",
    ) -> dict[str, Any]:
        """创建一个章节或附录节点。

        注意：这里先不填 path/full_path_title，因为节点的父子关系要等整棵树构建完
        才能稳定确定；最后由 _refresh_paths 统一回填。
        """
        return {
            "node_id": generate_hex_uuid(),
            "node_type": "标准附录" if appendix_code else "标准结构节点",
            "number": number,
            "title": title,
            "level": level,
            "path": "",
            "full_path_title": "",
            "content": "",
            "tables": [],
            "images": [],
            "page_range": [block.page_idx, block.page_idx] if block.page_idx is not None else [],
            "source_blocks": [block.block_id],
            "children": [],
            "is_appendix_node": is_appendix,
            "appendix_code": appendix_code,
            "appendix_type": appendix_type,
        }

    @staticmethod
    def _push_node(node: dict[str, Any], stack: list[dict[str, Any]], roots: list[dict[str, Any]]) -> None:
        """把新节点压入当前树的栈，并维护父子关系。

        栈顶始终表示当前路径的最后一个节点。遇到同级或更高层级节点时，
        先弹出不可能成为父节点的旧节点，再把新节点挂到新的栈顶下面。
        """
        while stack and stack[-1].get("level", 0) >= node.get("level", 0):
            stack.pop()
        if stack:
            stack[-1]["children"].append(node)
        else:
            roots.append(node)
        stack.append(node)

    def _build_table_node(self, block: StandardBlock, current_node: dict[str, Any] | None) -> dict[str, Any]:
        """把 table block 转为独立表格资源节点。"""
        caption = self._extract_table_caption(block.text)
        return {
            "table_id": generate_hex_uuid(),
            "node_type": "表格",
            "table_number": caption.get("number", ""),
            "table_caption": caption.get("caption", ""),
            "table_markdown": block.text,
            "table_html": block.raw.get("table_html") or block.raw.get("html") or (block.text if HTML_TABLE_RE.search(block.text) else ""),
            "page_idx": block.page_idx,
            "bbox": block.bbox,
            "source_format": "content_list" if block.raw else "markdown",
            "parsed_rows": self._parse_markdown_table(block.text),
            "related_section_number": current_node.get("number", "") if current_node else "",
            "related_section_title": current_node.get("title", "") if current_node else "",
            "source_block": block.block_id,
        }

    def _build_image_node(
        self,
        block: StandardBlock,
        current_node: dict[str, Any] | None,
        source: dict[str, Path | None],
    ) -> dict[str, Any]:
        """把 image block 转为独立图片资源节点。"""
        local_path = block.path or self._extract_image_path_from_text(block.text)
        return {
            "image_id": generate_hex_uuid(),
            "node_type": "图片",
            "local_path": local_path,
            "minio_url": "",
            "caption": block.text,
            "image_description": block.raw.get("image_caption") or block.raw.get("description") or "",
            "page_idx": block.page_idx,
            "bbox": block.bbox,
            "related_section_number": current_node.get("number", "") if current_node else "",
            "related_section_title": current_node.get("title", "") if current_node else "",
            "image_type": self._guess_image_type(block.text),
            "source_block": block.block_id,
        }

    def _split_term_definition_nodes(
        self,
        roots: list[dict[str, Any]],
        repairs: list[dict[str, Any]],
    ) -> None:
        """拆分“术语和定义”章节中的独立术语分点。

        国家标准中术语章节常见结构是：

        ```text
        3.1
        术语中文名 english term
        定义内容
        3.2
        ...
        ```

        MinerU 经常把 `3.1` 识别为 paragraph，把术语名识别为 title，
        导致当前通用章节树无法创建 `3.1` 子节点。本函数只针对标题包含
        “术语和定义”的节点做二次切分，避免放宽全局标题识别造成误判。
        """
        for node in list(self._walk_nodes(roots)):
            if not self._is_terms_section_node(node):
                continue
            if node.get("children"):
                continue
            content = str(node.get("content") or "")
            term_nodes, lead_text, term_repairs = self._parse_terms_from_content(node, content)
            if not term_nodes:
                continue
            node["content"] = lead_text
            node["children"] = term_nodes
            repairs.extend(term_repairs)

    @staticmethod
    def _is_terms_section_node(node: dict[str, Any]) -> bool:
        """判断节点是否为术语和定义章节。"""
        title = _compact_label(str(node.get("title") or ""))
        return "术语和定义" in title or "术语定义" in title

    def _parse_terms_from_content(
        self,
        parent_node: dict[str, Any],
        content: str,
    ) -> tuple[list[dict[str, Any]], str, list[dict[str, Any]]]:
        """从术语章节 content 中解析术语子节点。"""
        lines = _line_list(content)
        if not lines:
            return [], "", []

        parent_number = str(parent_node.get("number") or "")
        terms: list[dict[str, Any]] = []
        repairs: list[dict[str, Any]] = []
        lead_lines: list[str] = []
        current: dict[str, Any] | None = None
        current_content: list[str] = []

        def flush_current() -> None:
            nonlocal current, current_content
            if current is None:
                return
            current["content"] = "\n".join(current_content).strip()
            terms.append(current)
            current = None
            current_content = []

        index = 0
        while index < len(lines):
            line = lines[index].strip()
            normalized_number = self._normalize_term_number(line, parent_number)
            if normalized_number:
                flush_current()
                title = ""
                source_line = line
                if index + 1 < len(lines):
                    title = lines[index + 1].strip()
                    index += 1
                current = {
                    "node_id": generate_hex_uuid(),
                    "node_type": "标准结构节点",
                    "number": normalized_number,
                    "title": title,
                    "level": _section_level(normalized_number),
                    "path": "",
                    "full_path_title": "",
                    "content": "",
                    "tables": [],
                    "images": [],
                    "page_range": list(parent_node.get("page_range") or []),
                    "source_blocks": list(parent_node.get("source_blocks") or []),
                    "children": [],
                    "is_appendix_node": bool(parent_node.get("is_appendix_node")),
                    "appendix_code": parent_node.get("appendix_code", ""),
                    "appendix_type": parent_node.get("appendix_type", ""),
                }
                if source_line != normalized_number:
                    current["original_number"] = source_line
                    current["normalized_number"] = normalized_number
                    repairs.append(
                        {
                            "original_number": source_line,
                            "normalized_number": normalized_number,
                            "repair_reason": "术语章节独立编号行疑似漏点，按父级编号和上下文补全",
                        }
                    )
            elif current is None:
                lead_lines.append(line)
            else:
                current_content.append(line)
            index += 1

        flush_current()
        return terms, "\n".join(lead_lines).strip(), repairs

    @staticmethod
    def _normalize_term_number(line: str, parent_number: str) -> str:
        """识别并规范化术语编号行。

        支持：
        - `3.1`
        - `3.1.1`
        - `36` 在父节点为 `3` 时修复为 `3.6`
        """
        text = str(line or "").strip()
        if re.fullmatch(r"\d+(?:\.\d+)+", text):
            return text
        if parent_number and re.fullmatch(r"\d{2,3}", text) and text.startswith(parent_number):
            suffix = text[len(parent_number):]
            if suffix and suffix.isdigit():
                return ".".join([parent_number, *list(suffix)])
        return ""

    def upload_and_replace_image_links(
        self,
        images: list[dict[str, Any]],
        tables: list[dict[str, Any]],
        blocks: list[StandardBlock],
        source: dict[str, Path | None],
    ) -> list[dict[str, Any]]:
        """上传图片到 MinIO，并用线上链接替换结构化结果中的图片路径。

        任务2要求图片保存路径为：

        `国家标准/文件名/images/图片名`

        其中“文件名”来自输入目录最后一层名称，例如：
        `...\国家标准\城镇污水处理厂污染物排放标准`
        的文件名就是 `城镇污水处理厂污染物排放标准`。

        返回值记录每张图片的上传结果。失败时只记录错误，不中断正文结构解析。
        """
        upload_logs: list[dict[str, Any]] = []
        if not self.upload_images:
            for image in images:
                upload_logs.append(
                    {
                        "image_id": image.get("image_id", ""),
                        "status": "skipped",
                        "reason": "upload_images=False",
                    }
                )
            return upload_logs

        if not images:
            return upload_logs

        try:
            storage = self._get_minio_adapter()
        except Exception as exc:
            message = f"初始化 MinIO 客户端失败: {exc}"
            logging.error(message)
            for image in images:
                image["upload_status"] = "failed"
                image["upload_error"] = message
                upload_logs.append(
                    {
                        "image_id": image.get("image_id", ""),
                        "status": "failed",
                        "error": message,
                    }
                )
            return upload_logs

        document_name = _sanitize_minio_path_part(self._document_name_from_source(source))
        block_map = {block.block_id: block for block in blocks}
        path_replacements: dict[str, str] = {}

        for image in images:
            image_id = image.get("image_id", "")
            local_path_text = image.get("local_path", "")
            local_path = self._resolve_image_path(local_path_text, source)
            if not local_path or not local_path.exists():
                message = f"图片文件不存在: {local_path_text}"
                image["upload_status"] = "failed"
                image["upload_error"] = message
                upload_logs.append(
                    {
                        "image_id": image_id,
                        "status": "failed",
                        "local_path": local_path_text,
                        "error": message,
                    }
                )
                continue

            image_name = _sanitize_minio_path_part(local_path.name)
            object_name = f"{self.minio_prefix}/{document_name}/images/{image_name}"
            content_type = mimetypes.guess_type(str(local_path))[0] or "application/octet-stream"

            try:
                with local_path.open("rb") as file_stream:
                    ok = storage.upload_file_stream(
                        file_stream=file_stream,
                        bucket_name=self.minio_bucket,
                        object_name=object_name,
                        content_type=content_type,
                        file_size=local_path.stat().st_size,
                    )
                if not ok:
                    raise RuntimeError("MinIO upload_file_stream 返回 False")
                if not storage.file_exists(self.minio_bucket, object_name):
                    raise RuntimeError(f"MinIO 上传后校验失败，未找到对象: {self.minio_bucket}/{object_name}")

                online_url = self._build_public_minio_url(object_name)
                path_replacements[local_path_text] = online_url
                path_replacements[local_path.name] = online_url
                path_replacements[str(Path("images") / local_path.name).replace("\\", "/")] = online_url
                image["minio_bucket"] = self.minio_bucket
                image["minio_object_name"] = object_name
                image["minio_url"] = online_url
                image["online_path"] = online_url
                image["upload_status"] = "success"

                # 同步替换 blocks 中的图片链接，保证最终输出里的 blocks 也是线上链接。
                block = block_map.get(image.get("source_block", ""))
                if block is not None:
                    block.raw["original_path"] = block.path
                    block.path = online_url

                upload_logs.append(
                    {
                        "image_id": image_id,
                        "status": "success",
                        "local_path": str(local_path),
                        "bucket": self.minio_bucket,
                        "object_name": object_name,
                        "url": online_url,
                    }
                )
            except Exception as exc:
                message = str(exc)
                image["upload_status"] = "failed"
                image["upload_error"] = message
                upload_logs.append(
                    {
                        "image_id": image_id,
                        "status": "failed",
                        "local_path": str(local_path),
                        "bucket": self.minio_bucket,
                        "object_name": object_name,
                        "error": message,
                    }
                )
        if path_replacements:
            self._replace_uploaded_image_links(tables, blocks, path_replacements)
        return upload_logs

    @staticmethod
    def _replace_uploaded_image_links(
        tables: list[dict[str, Any]],
        blocks: list[StandardBlock],
        replacements: dict[str, str],
    ) -> None:
        """把 Markdown/HTML 文本中的本地图片链接替换为线上链接。"""
        if not replacements:
            return

        def replace_text(text: str) -> str:
            result = str(text or "")
            for old, new in sorted(replacements.items(), key=lambda item: len(item[0]), reverse=True):
                if old:
                    result = result.replace(old, new)
            return result

        for table in tables:
            table["table_markdown"] = replace_text(table.get("table_markdown", ""))
            table["table_html"] = replace_text(table.get("table_html", ""))

        for block in blocks:
            block.text = replace_text(block.text)
            block.path = replacements.get(block.path, block.path)

    def _get_minio_adapter(self) -> MinIOAdapter:
        """懒加载 MinIO 客户端，避免没有图片时建立网络连接。"""
        if self._minio_adapter is None:
            endpoint, secure, _ = _normalize_minio_endpoint(self.minio_api_url)
            _, _, public_base_url = _normalize_minio_endpoint(self.minio_url)
            self._minio_public_base_url = public_base_url
            self._minio_adapter = MinIOAdapter(
                StorageConfig(
                    endpoint=endpoint,
                    access_key=self.minio_access_key,
                    secret_key=self.minio_secret_key,
                    secure=secure,
                )
            )
            self._minio_adapter.ensure_bucket_exists(self.minio_bucket)
        return self._minio_adapter

    def _build_public_minio_url(self, object_name: str) -> str:
        """构造写入结果中的线上图片链接。"""
        return f"{self._minio_public_base_url}/{self.minio_bucket}/{quote(object_name, safe='/')}"

    @staticmethod
    def _document_name_from_source(source: dict[str, Path | None]) -> str:
        """根据输入目录最后一层确定文件名。"""
        root = source.get("root")
        input_file = source.get("input_file")
        if root:
            return Path(root).name
        if input_file:
            return Path(input_file).stem
        return "未命名国家标准"

    @staticmethod
    def _resolve_image_path(local_path: str, source: dict[str, Path | None]) -> Path | None:
        """把图片相对路径解析为本地绝对路径。

        Markdown 中常见写法可能是：
        - `images/fig1.png`
        - `fig1.png`
        - 绝对路径

        因此按 absolute、root/path、images_dir/name、images_dir/path 的顺序尝试。
        """
        if not local_path:
            return None
        raw_path = Path(str(local_path).strip())
        if raw_path.is_absolute():
            return raw_path

        root = source.get("root")
        images_dir = source.get("images_dir")
        candidates: list[Path] = []
        if root:
            candidates.append(Path(root) / raw_path)
        if images_dir:
            candidates.append(Path(images_dir) / raw_path.name)
            candidates.append(Path(images_dir) / raw_path)

        for candidate in candidates:
            if candidate.exists():
                return candidate
        return candidates[0] if candidates else raw_path

    def _refresh_paths(self, roots: list[dict[str, Any]]) -> None:
        """递归刷新树节点的 path 和 full_path_title。

        path 偏机器可读，用 `/` 连接编号。
        full_path_title 偏人类可读，用 `>` 连接完整标题。
        """
        def walk(node: dict[str, Any], parent_numbers: list[str], parent_titles: list[str]) -> None:
            """深度优先遍历并继承父路径。"""
            number = node.get("number") or node.get("title", "")
            title = node.get("title", "")
            numbers = parent_numbers + ([number] if number else [])
            titles = parent_titles + ([f"{number} {title}".strip()] if title or number else [])
            node["path"] = "/".join(numbers)
            node["full_path_title"] = " > ".join(titles)
            for child in node.get("children", []):
                walk(child, numbers, titles)

        for root in roots:
            walk(root, [], [])

    def _repair_number(self, number: str, title: str, seen_numbers: set[str]) -> tuple[str, dict[str, Any] | None]:
        """对疑似漏点章节编号做保守修复。

        典型错误：
        - `4.22` 实际应为 `4.2.2`。
        - `422` 实际应为 `4.2.2`。

        修复原则：
        - 只生成少量候选。
        - 用父级是否存在、前序兄弟是否存在等上下文打分。
        - 候选得分明显高于原始编号时才修复。
        """
        candidates = self._number_repair_candidates(number)
        if re.match(r"^\d$", number) and number in seen_numbers:
            top_numbers = sorted(int(item) for item in seen_numbers if re.match(r"^\d$", item))
            if top_numbers:
                candidates.append(f"{top_numbers[-1]}.{number}")
        if not candidates:
            return number, None
        original_score = self._score_number_candidate(number, title, seen_numbers)
        best = max(candidates, key=lambda item: self._score_number_candidate(item, title, seen_numbers))
        best_score = self._score_number_candidate(best, title, seen_numbers)
        if best != number and best_score >= original_score + 2:
            return best, {
                "original_number": number,
                "normalized_number": best,
                "repair_reason": "父级或前序兄弟编号已存在，疑似 MinerU 编号漏点",
            }
        return number, None

    @staticmethod
    def _number_repair_candidates(number: str) -> list[str]:
        """根据原始编号生成可能的修复候选。"""
        candidates: list[str] = []
        if re.match(r"^\d{2}$", number):
            candidates.append(f"{number[0]}.{number[1]}")
        if re.match(r"^\d\.\d{2}$", number):
            candidates.append(f"{number[0]}.{number[2]}.{number[3]}")
        if re.match(r"^\d{3}$", number):
            candidates.append(f"{number[0]}.{number[1]}.{number[2]}")
        return candidates

    @staticmethod
    def _score_number_candidate(number: str, title: str, seen_numbers: set[str]) -> int:
        """给编号候选打分。

        分数越高，说明该编号越符合已经出现的上下文结构。
        """
        score = 0
        parent = _parent_number(number)
        prev = _previous_sibling(number)
        if parent and parent in seen_numbers:
            score += 2
        if prev and prev in seen_numbers:
            score += 2
        if len(title) <= 80:
            score += 1
        if number in seen_numbers:
            score -= 3
        return score

    @staticmethod
    def _looks_like_real_section(text: str) -> bool:
        """判断编号标题是否像真实章节，而不是目录行或误识别文本。"""
        match = SECTION_TITLE_RE.match(text)
        if not match:
            return False
        if match.group("number") == "0":
            return False
        title = match.group("title").strip()
        if re.match(r"^\d{3,}$", match.group("number")) and re.search(r"[，,。；;、]", title):
            return False
        if len(title) > 120:
            return False
        if re.search(r"\.{4,}\s*\d+$", title):
            return False
        return True

    @staticmethod
    def _looks_like_toc_line(text: str) -> bool:
        """判断一行是否像目次中的点线页码行。"""
        return bool(re.search(r"(?:…{1,}|(?:\.\s*){3,})\s*\d+\s*$", text or ""))

    @staticmethod
    def _looks_like_missing_top_section_title(
        text: str,
        current_node: dict[str, Any] | None,
        in_appendix: bool,
    ) -> bool:
        """判断标题是否可能是漏掉一级编号的章标题。

        例如测试数据中存在：
        `电器电子产品分类及有害物质限制使用要求`
        它位于 `3 术语和定义` 之后、`5 限量要求` 之前，实际应为第 4 章。

        为避免把术语标题误判为章标题，这里要求：
        - 当前不在附录中。
        - 当前已有正文节点。
        - 文本包含中文，不包含明显英文术语。
        - 标题长度适中，且包含“要求/分类/方法/规则”等章节标题常见词。
        """
        # V2 以人工审查后的 Markdown 为主输入，不再自动推断缺失的一级章节编号。
        # 自动补号容易把 `645 若采用...` 这类漏点小节误判成新的一级章节。
        return False

    @staticmethod
    def _is_body_start(text: str) -> bool:
        """判断文本是否已经进入正文或结构性章节。"""
        return bool(SECTION_TITLE_RE.match(text)) or _compact_label(text) in {"前言", "引言", "目次"} or bool(APPENDIX_TITLE_RE.match(text))

    @staticmethod
    def _date_near_keyword(text: str, keywords: tuple[str, ...]) -> str:
        """从包含指定关键词的行中提取日期。"""
        for line in _line_list(text):
            if any(keyword in line for keyword in keywords):
                match = DATE_RE.search(line)
                if match:
                    return match.group("date")
        return ""

    @staticmethod
    def _guess_standard_names(lines: list[str], standard_number: str) -> tuple[str, str]:
        """从封面行中粗略推断标准中英文名称。

        当前是快速验证阶段规则：
        - 中文名：包含中文、长度合理、不是发布单位或日期类文本。
        - 英文名：包含英文但不含中文，且不是标准编号行。
        - 如果没有找到中文名，则尝试取标准编号上一行。
        """
        cleaned = [_clean_text(line) for line in lines if _clean_text(line)]
        cn_name = ""
        en_name = ""
        for line in cleaned:
            if re.search(r"[\u4e00-\u9fff]", line) and not STANDARD_NO_RE.search(line):
                if 4 <= len(line) <= 80 and not re.search(r"(发布|实施|ICS|CCS|中华人民共和国|国家市场监督)", line):
                    cn_name = line
                    break
        for line in cleaned:
            if re.search(r"[A-Za-z]", line) and not re.search(r"[\u4e00-\u9fff]", line) and not STANDARD_NO_RE.search(line):
                if 4 <= len(line) <= 120:
                    en_name = line
                    break
        if not cn_name and standard_number:
            for index, line in enumerate(cleaned):
                if standard_number in line and index > 0:
                    cn_name = cleaned[index - 1]
                    break
        return cn_name, en_name

    @staticmethod
    def _guess_standard_type(standard_number: str, cover_text: str) -> str:
        """根据标准编号前缀或封面文本判断标准性质。"""
        if standard_number.upper().startswith("GB/T"):
            return "推荐性国家标准"
        if standard_number.upper().startswith("GB/Z"):
            return "国家标准化指导性技术文件"
        if standard_number.upper().startswith("GB"):
            return "强制性国家标准"
        if "行业标准" in cover_text:
            return "行业标准"
        if "国家标准" in cover_text:
            return "国家标准"
        return ""

    @staticmethod
    def _guess_release_org(text: str) -> str:
        """从封面文本中粗略提取发布单位。"""
        orgs = []
        for line in _line_list(text):
            if any(keyword in line for keyword in ("发布", "国家市场监督管理总局", "国家标准化管理委员会", "生态环境部")):
                orgs.append(line.replace("发布", "").strip())
        return "；".join(dict.fromkeys(orgs))

    @staticmethod
    def _extract_replaced_standards(text: str) -> list[str]:
        """提取“代替/替代”行中的标准编号。"""
        result: list[str] = []
        for line in _line_list(text):
            if any(keyword in line for keyword in ("代替", "替代")):
                result.extend(match.group(0).strip() for match in STANDARD_NO_RE.finditer(line))
        return list(dict.fromkeys(result))

    @staticmethod
    def _extract_code_after_label(text: str, label: str) -> str:
        """提取 ICS、CCS 等标签后的编码。"""
        match = re.search(rf"{re.escape(label)}\s*[:：]?\s*([A-Z0-9.\-/]+)", text, re.IGNORECASE)
        return match.group(1).strip() if match else ""

    @staticmethod
    def _guess_standard_status(text: str) -> str:
        """根据文本中的明确词判断标准状态。"""
        if "现行" in text:
            return "现行"
        if "废止" in text:
            return "废止"
        if "即将实施" in text:
            return "即将实施"
        return ""

    def _collect_named_section_text(self, blocks: list[StandardBlock], section_name: str) -> str:
        """收集前言、引言或目次全文。

        这些内容不放入正文树，但对第二阶段文件信息抽取有价值。
        """
        collecting = False
        lines: list[str] = []
        for block in blocks:
            text = _clean_text(block.text)
            if not text:
                continue
            compact_text = _compact_label(text)
            if collecting and (compact_text in {"前言", "引言", "目次"} or SECTION_TITLE_RE.match(text) or APPENDIX_TITLE_RE.match(text)):
                break
            if compact_text == section_name:
                collecting = True
                continue
            if collecting:
                lines.append(block.text)
        return "\n".join(lines).strip()

    def _collect_reference_text(self, blocks: list[StandardBlock]) -> str:
        """收集参考文献全文，供第二阶段 file_info 抽取标准依据。"""
        collecting = False
        lines: list[str] = []
        for block in blocks:
            text = _clean_text(block.text)
            if not text:
                continue
            compact_text = _compact_label(text)
            if compact_text in {"参考文献", "参考文件", "参考资料"}:
                collecting = True
                continue
            if collecting:
                # 参考文献通常位于文档末尾；如果后续又出现正文一级标题，说明已经离开参考区。
                if SECTION_TITLE_RE.match(text) or APPENDIX_TITLE_RE.match(text):
                    break
                lines.append(block.text)
        return "\n".join(lines).strip()

    @staticmethod
    def _extract_table_caption(text: str) -> dict[str, str]:
        """从表格文本中提取表号和表题。"""
        for line in _line_list(text):
            match = re.match(r"^(表\s*[A-Za-z]?\s*\d+(?:\.\d+)*)\s*(.*)$", line)
            if match:
                return {"number": match.group(1).replace(" ", ""), "caption": match.group(2).strip()}
        return {"number": "", "caption": ""}

    @staticmethod
    def _parse_markdown_table(text: str) -> list[list[str]]:
        """把 Markdown 表格解析成二维数组。

        这里只做轻量解析，目的是为后续表格理解提供基础结构。
        """
        rows: list[list[str]] = []
        for line in _line_list(text):
            if not (line.startswith("|") and line.endswith("|")):
                continue
            cells = [cell.strip() for cell in line.strip("|").split("|")]
            if cells and not all(re.fullmatch(r":?-{3,}:?", cell) for cell in cells):
                rows.append(cells)
        return rows

    @staticmethod
    def _extract_image_path_from_text(text: str) -> str:
        """从 Markdown/HTML 图片语法中提取图片路径。"""
        match = MARKDOWN_IMAGE_RE.search(text or "")
        if match:
            return match.group("path").strip()
        html_match = HTML_IMAGE_RE.search(text or "")
        return html_match.group("path").strip() if html_match else ""

    @staticmethod
    def _guess_image_type(text: str) -> str:
        """根据图注关键词粗略判断图片类型。"""
        if "流程" in text:
            return "流程图"
        if "标签" in text or "标志" in text:
            return "标签样例"
        if "结构" in text:
            return "结构图"
        if "照片" in text:
            return "照片"
        return "其他"

    @staticmethod
    def _walk_nodes(nodes: list[dict[str, Any]]) -> Iterable[dict[str, Any]]:
        """深度优先遍历章节树。"""
        for node in nodes:
            yield node
            yield from NationalStandardExtractor._walk_nodes(node.get("children", []))

    @staticmethod
    def _is_reference_section(node: dict[str, Any]) -> bool:
        """判断节点是否为规范性引用文件章节。"""
        text = f"{node.get('number', '')} {node.get('title', '')}"
        return "规范性引用文件" in text or text.startswith("2 ")

    @staticmethod
    def _year_from_number(number: str) -> str:
        """从带年份的标准编号中提取年份。"""
        match = re.search(r"[—-](\d{4})$", number)
        return match.group(1) if match else ""

    @staticmethod
    def _standard_type_from_number(number: str) -> str:
        """根据引用标准编号前缀判断引用文件类型。"""
        upper = number.upper()
        if upper.startswith("GB/T"):
            return "推荐性国家标准"
        if upper.startswith("GB/Z"):
            return "国家标准化指导性技术文件"
        if upper.startswith("GB"):
            return "强制性国家标准"
        if upper.startswith(("ISO", "IEC")):
            return "国际标准"
        return "行业标准或其他标准"

    @staticmethod
    def _validate_number_continuity(numbers: list[str], scope: str, issues: list[dict[str, Any]]) -> None:
        """校验一级编号是否连续。"""
        ints = [int(num) for num in numbers if str(num).isdigit()]
        if not ints:
            return
        expected = list(range(min(ints), max(ints) + 1))
        if ints != expected:
            issues.append({"type": "number_continuity", "scope": scope, "numbers": numbers, "expected": expected})

    @staticmethod
    def _validate_sibling_last_number_continuity(numbers: list[str], parent: str, warnings: list[dict[str, Any]]) -> None:
        """校验同级子章节末位编号是否连续。

        这是提示性校验，只写入 warnings。因为有些标准本身可能允许缺号或解析缺失，
        不能直接阻断流程。
        """
        last_values = []
        for number in numbers:
            parts = number.split(".")
            if parts[-1].isdigit():
                last_values.append(int(parts[-1]))
        if len(last_values) <= 1:
            return
        expected = list(range(min(last_values), max(last_values) + 1))
        if last_values != expected:
            warnings.append(
                {
                    "type": "sibling_number_continuity",
                    "parent": parent,
                    "numbers": numbers,
                    "message": "同级章节编号可能不连续",
                }
            )


def extract_national_standard(input_path: str | Path) -> dict[str, Any]:
    """便捷函数：解析一个国家标准输入路径并返回结构化结果。"""
    return NationalStandardExtractor().extract(input_path)


def save_national_standard_parse_result(input_path: str | Path, output_path: str | Path) -> dict[str, Any]:
    """便捷函数：解析输入路径，并把结构化结果保存为 JSON 文件。"""
    result = extract_national_standard(input_path)
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return result


def extract_national_standard_batch(
    input_root_dir: str | Path,
    output_dir: str | Path,
    upload_images: bool = True,
) -> dict[str, Any]:
    """批量解析国家标准目录，并将结果保存到 output_dir。

    任务2约定：功能接受的参数是目录地址，目录最后一层为文件名。
    因此当 input_root_dir 下存在多个子目录时，每个子目录会被视为一份国家标准；
    如果 input_root_dir 自身就是一份标准目录，也可以直接被解析。
    """
    input_root = Path(input_root_dir)
    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)

    if not input_root.exists():
        raise FileNotFoundError(f"国家标准输入目录不存在: {input_root}")

    standard_dirs = _discover_markdown_standard_dirs(input_root)
    if not standard_dirs and input_root.is_dir():
        standard_dirs = [input_root]

    extractor = NationalStandardExtractor(upload_images=upload_images)
    summary: dict[str, Any] = {
        "input_root_dir": str(input_root),
        "output_dir": str(output_root),
        "total": len(standard_dirs),
        "success": 0,
        "failed": 0,
        "validation_failed": 0,
        "items": [],
    }

    for standard_dir in standard_dirs:
        output_name = _sanitize_minio_path_part(standard_dir.name) + ".json"
        output_path = output_root / output_name
        try:
            result = extractor.extract(standard_dir)
            output_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
            validation_passed = bool(result.get("validation", {}).get("passed"))
            status = "success" if validation_passed else "validation_failed"
            if validation_passed:
                summary["success"] += 1
            else:
                summary["failed"] += 1
                summary["validation_failed"] += 1
            summary["items"].append(
                {
                    "name": standard_dir.name,
                    "input_dir": str(standard_dir),
                    "output_path": str(output_path),
                    "status": status,
                    "body_root_count": len(result.get("body_tree", [])),
                    "appendix_root_count": len(result.get("appendix_tree", [])),
                    "table_count": len(result.get("tables", [])),
                    "image_count": len(result.get("images", [])),
                    "reference_candidate_count": len(result.get("reference_candidates", [])),
                    "validation_passed": validation_passed,
                    "failed_data_count": len(result.get("validation", {}).get("failed_data", []) or []),
                    "error": "结构校验未通过" if not validation_passed else "",
                }
            )
        except Exception as exc:
            logging.exception("国家标准解析失败: %s", standard_dir)
            summary["failed"] += 1
            summary["items"].append(
                {
                    "name": standard_dir.name,
                    "input_dir": str(standard_dir),
                    "output_path": str(output_path),
                    "status": "failed",
                    "error": str(exc),
                }
            )

    summary_path = output_root / "summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    summary["summary_path"] = str(summary_path)
    return summary


def _discover_markdown_standard_dirs(input_root: Path) -> list[Path]:
    """递归发现包含 Markdown 主输入的国家标准目录。"""
    if input_root.is_file():
        return [input_root.parent]
    md_names = {
        "full_fixed.md",
        "reviewed.md",
        "reviewed_full.md",
        "full.md",
        "full_fixed.txt",
        "reviewed.txt",
    }
    result: list[Path] = []
    for path in input_root.rglob("*"):
        if not path.is_file():
            continue
        if any(part.lower() == "images" for part in path.parts):
            continue
        if path.name.lower() in md_names:
            if path.parent not in result:
                result.append(path.parent)
    result.sort(key=lambda item: str(item))
    return result
