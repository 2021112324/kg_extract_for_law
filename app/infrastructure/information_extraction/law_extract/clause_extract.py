import asyncio
import json
import logging
import os
import re

from app.infrastructure.information_extraction.base import Entity, Relationship
from app.infrastructure.information_extraction.factory import InformationExtractionFactory
from app.infrastructure.information_extraction.law_extract.prompt.example import example_for_clause, \
    example_for_file_info
from app.infrastructure.information_extraction.law_extract.prompt.prompt import prompt_for_clause, prompt_for_file_info
from app.infrastructure.information_extraction.law_extract.prompt.schema import schema_for_clause, schema_for_file_info
from app.infrastructure.information_extraction.method.base import LangextractConfig
from app.infrastructure.string_utils.id_tool import generate_hex_uuid
from app.infrastructure.string_utils.str_clean import clean_string_with_only_words, clean_string_for_neo4j_extended, \
    replace_full_corner_space, replace_zero_width_chars

CLAUSE_MADEL = "qwen3-30b-a3b-instruct-2507"
CLAUSE_MADEL_API = "gpustack_342609ce423be29a_4371426b285a91dc44fb4e8d72454847"
CLAUSE_MADEL_KEY = "http://222.171.219.26:20001/v1/chat/completions"

# MAX_CHUNK_SIZE = int(os.getenv("MAX_CHUNK_SIZE", "5000"))
# BATCH_LENGTH = int(os.getenv("BATCH_LENGTH", "5"))
# MAX_WORKERS = int(os.getenv("MAX_WORKERS", "3"))
# TIMEOUT = int(os.getenv("TIMEOUT", "300"))

MAX_CHAR_BUFFER = 7500
BATCH_LENGTH = 5
MAX_WORKERS = 3
TIMEOUT = 3000

CN_NUM = {
    "零": 0,
    "一": 1,
    "二": 2,
    "三": 3,
    "四": 4,
    "五": 5,
    "六": 6,
    "七": 7,
    "八": 8,
    "九": 9,
    "十": 10,
    "百": 100,
    "千": 1000,
    "万": 10000,
}

RISK_TYPE_VALUES = [
    "产品法律风险",
    "供应链合规风险",
    "劳动用工法律合规风险",
    "企业关联方合规风险",
    "企业国际化经营合规风险",
    "企业信用风险",
]

QUANT_CONDITION_KEYS = ["原文件", "量化值类型", "最小值", "最大值", "单位", "约束关系"]


def _cn_number_to_int(text: str) -> int:
    """将中文数字或阿拉伯数字转换为整数。"""
    text = str(text or "").strip()
    if not text:
        raise ValueError("编号为空")
    if text.isdigit():
        return int(text)

    total = 0
    section = 0
    number = 0
    for char in text:
        value = CN_NUM.get(char)
        if value is None:
            raise ValueError(f"无法解析中文数字: {text}")
        if value == 10000:
            section = (section + number) * value
            total += section
            section = 0
            number = 0
        elif value >= 10:
            if number == 0:
                number = 1
            section += number * value
            number = 0
        else:
            number = value
    return total + section + number


def _parse_clause_order_number(clause_number: str) -> int:
    """从“第X条”或“第X条之一”中解析 X 对应的整数。"""
    match = re.match(r"^第([零一二三四五六七八九十百千万\d]+)\s*条", str(clause_number or "").strip())
    if not match:
        raise ValueError(f"无法解析条款编号: {clause_number}")
    return _cn_number_to_int(match.group(1))


def _split_to_list(value) -> list:
    if value is None:
        return []
    if isinstance(value, list):
        raw_items = value
    elif isinstance(value, tuple) or isinstance(value, set):
        raw_items = list(value)
    else:
        raw_items = re.split(r"[、,，;；/|]+", str(value))
    items = []
    for item in raw_items:
        text = str(item or "").strip()
        if text and text not in items:
            items.append(text)
    return items


def _normalize_risk_type(value) -> list:
    return [item for item in _split_to_list(value) if item in RISK_TYPE_VALUES]


def _normalize_economic_industry(value) -> list:
    items = _split_to_list(value)
    if not items:
        return ["通用"]
    if "其他" in items and len(items) == 1:
        return ["通用"]
    specific_items = [item for item in items if item not in {"通用", "其他"}]
    return specific_items or ["通用"]


def _normalize_function_type(value, text: str = "") -> str:
    raw = str(value or "").strip()
    if raw in {"禁止", "必要", "可选"}:
        return raw
    judge_text = f"{raw}\n{text or ''}"
    if re.search(r"不得|禁止|严禁|不准|不得以外", judge_text):
        return "禁止"
    if re.search(r"应当|必须|应予|应由|应向|应将|应\b|需|须", judge_text):
        return "必要"
    if re.search(r"可以|可向|可由|有权|授权|鼓励|自愿", judge_text):
        return "可选"
    if raw in {"义务", "强制", "规范", "程序", "责任"}:
        return "必要"
    if raw in {"权利", "授权", "许可"}:
        return "可选"
    return "必要"


def _parse_number_token(token: str):
    token = str(token or "").strip()
    if not token:
        return None
    try:
        return float(token) if "." in token else int(token)
    except ValueError:
        try:
            return _cn_number_to_int(token)
        except ValueError:
            return None


def _infer_quant_type(text: str) -> str:
    if re.search(r"元|万元|罚款|金额|所得|费用|赔偿", text):
        return "金额"
    if re.search(r"%|％|百分|比例|率", text):
        return "比例"
    if re.search(r"年|月|日|天|小时|期限|期间|届满|以前|以内|前|后", text):
        return "期限"
    if "次" in text:
        return "次数"
    if "倍" in text:
        return "倍数"
    return "其他"


def _infer_quant_unit(text: str) -> str:
    for unit in ["万元", "元", "%", "％", "年", "月", "日", "天", "小时", "次", "倍"]:
        if unit in text:
            return "%" if unit == "％" else unit
    return ""


def _normalize_quant_value_by_unit(value, unit: str):
    if value is None:
        return None
    if unit == "万元":
        return value * 10000
    return value


def _build_quant_condition_from_text(text: str) -> dict | None:
    raw = str(text or "").strip()
    if not raw:
        return None
    unit = _infer_quant_unit(raw)
    numbers = []
    for match in re.finditer(r"(\d+(?:\.\d+)?|[零一二三四五六七八九十百千万]+)", raw):
        value = _parse_number_token(match.group(1))
        if value is not None:
            numbers.append(_normalize_quant_value_by_unit(value, unit))

    relation = "其他"
    min_value = None
    max_value = None
    has_lower = bool(re.search(r"以上|不少于|不低于|不得低于|超过", raw))
    has_upper = bool(re.search(r"以下|以内|不超过|不得超过|低于", raw))
    if has_lower and has_upper:
        relation = "区间"
        if len(numbers) >= 2:
            min_value = numbers[0]
            max_value = numbers[1]
    elif has_lower:
        relation = "下限"
        if numbers:
            min_value = numbers[0]
    elif has_upper:
        relation = "上限"
        if numbers:
            max_value = numbers[0]
    elif numbers:
        relation = "等于"
        min_value = numbers[0]
        max_value = numbers[0]

    return {
        "原文件": raw,
        "量化值类型": _infer_quant_type(raw),
        "最小值": min_value,
        "最大值": max_value,
        "单位": unit,
        "约束关系": relation,
    }


def _normalize_quant_condition(value):
    if isinstance(value, dict):
        if not any(str(value.get(key, "") or "").strip() for key in QUANT_CONDITION_KEYS):
            return None
        result = {key: value.get(key) for key in QUANT_CONDITION_KEYS}
        result["原文件"] = result.get("原文件") or value.get("原文") or value.get("raw_text") or ""
        return result
    if isinstance(value, list):
        conditions = [_normalize_quant_condition(item) for item in value]
        conditions = [item for item in conditions if item]
        if not conditions:
            return None
        return conditions[0] if len(conditions) == 1 else conditions
    return _build_quant_condition_from_text(str(value or "").strip())


def _normalize_law_properties(
    properties: dict | None,
    text: str = "",
    is_file_info: bool = False,
    node_type: str = "",
) -> dict:
    props = dict(properties or {})
    if "合规风险类型" in props:
        props["合规风险类型"] = _normalize_risk_type(props.get("合规风险类型"))
    elif is_file_info:
        props["合规风险类型"] = []

    if "合规域" in props:
        props["合规域"] = _split_to_list(props.get("合规域"))
    elif "应用领域" in props:
        props["合规域"] = _split_to_list(props.get("应用领域"))
    else:
        props["合规域"] = []

    if "经济行业" in props:
        props["经济行业"] = _normalize_economic_industry(props.get("经济行业"))
    else:
        props["经济行业"] = _normalize_economic_industry(props.get("适用行业"))

    props.pop("应用领域", None)
    props.pop("适用行业", None)

    if not is_file_info:
        content_text = text or props.get("条款单元内容") or props.get("法条全文") or ""
        if "功能类型" in props:
            original_function_type = str(props.get("功能类型") or "").strip()
            props["功能类型"] = _normalize_function_type(original_function_type, content_text)
            if original_function_type and original_function_type not in {"禁止", "必要", "可选"}:
                other_info = str(props.get("其他信息") or "").strip()
                note = f"原功能类型：{original_function_type}"
                props["其他信息"] = f"{other_info}；{note}" if other_info else note

        if node_type == "条款单元":
            quant_condition = _normalize_quant_condition(props.get("量化条件"))
            props["量化条件"] = quant_condition
            props["量化特征"] = "定量" if quant_condition else "定性"
        else:
            props.pop("量化条件", None)
            props.pop("量化特征", None)
    return props


def _is_inserted_clause_number(clause_number: str) -> bool:
    """判断是否为“第X条之一/之二”等修法插入条。"""
    return bool(
        re.match(
            r"^第[零一二三四五六七八九十百千万\d]+\s*条\s*之[零一二三四五六七八九十百千万\d]+$",
            str(clause_number or "").strip(),
        )
    )


class ResultStats:
    def __init__(self):
        self.error = 0
        self.error_msg = ""
        self.week_warning = 0
        self.week_warning_msg = ""
        self.strong_warning = 0
        self.strong_warning_msg = ""


class ClauseCache:
    def __init__(self):
        self.file_info = {}
        self.clause_cache = {}


class ClauseExtractor:
    def __init__(self, max_concurrent: int = 50):
        self.extractor_config = LangextractConfig(
            model_name=CLAUSE_MADEL,
            api_key=CLAUSE_MADEL_API,
            api_url=CLAUSE_MADEL_KEY,
            config={
                "timeout": TIMEOUT
            },
            max_char_buffer=MAX_CHAR_BUFFER,
            batch_length=BATCH_LENGTH,
            max_workers=MAX_WORKERS,
            # resolver_params=
        )
        self.extractor = InformationExtractionFactory.create(
            "langextract",
            max_retries=5,
            config=self.extractor_config
        )
        self.semaphore = asyncio.Semaphore(max_concurrent)  # 添加信号量
        self.result_stats = ResultStats()

        # 宽松模式标志（True则将处理失败的条款直接作为法条实体添加到图谱中（不考虑法条信息、））
        self.lenient_mode = False

    async def extract_clauses(
            self,
            filename: str,
            text: str
    ) -> dict:
        """
        实现功能：从法规文件中抽取条款知识图谱数据
        :param filename:
        :param text:
        :return:
        """
        try:
            logging.info("📄⏳:开始条款知识图谱抽取")
            # 分割条款数据
            logging.info("📄:开始分割条款")
            clauses_data = await self.split_clause(
                text
            )
            logging.info("📄:结束分割条款")
            # 检查缓存
            """
            缓存cache为ClauseCache对象，其中的cache.clause_cache记录结构：
            {
                "第X条" :{}
            }
            遍历缓存，通过 clauses["条款编号"] 等于 缓存的"第X条"，
            将clauses中已经处理过的条款数据从clauses中删除，
            然后对clauses中未处理过的条款数据进行处理
            """
            logging.info("📄:开始处理文件信息")
            clause_cache = ClauseCache()
            file_info = clauses_data.get("file_info")
            if not file_info:
                logging.error("📄❌：文件信息为空")
                self.result_stats.error += 1
                self.result_stats.error_msg += "文件信息为空\n"
                raise ValueError("文件信息为空")
            file_info_result = await self.kg_extract_from_file_info(
                filename=filename,
                clause_cache=clause_cache,
                file_info=file_info
            )

            logging.info("📄:结束处理文件信息")

            logging.info("📄:开始处理条款数据")
            clauses = clauses_data.get("clauses")
            if not clauses:
                logging.error("📄❌：条款数据为空")
                self.result_stats.error += 1
                self.result_stats.error_msg += "条款数据为空\n"
                raise ValueError("条款数据为空")
            # 批量处理条款数据
            tasks = [
                self.kg_extract_from_clause(
                    filename=filename,
                    clause_cache=clause_cache,
                    one_clause=clause
                )
                for clause in clauses
            ]
            # 使用 asyncio.gather 并发执行所有任务
            logging.info("📄🌐:开始抽取条款知识图谱")
            results = await asyncio.gather(*tasks, return_exceptions=True)
            logging.info("📄:结束抽取条款知识图谱")

            logging.info("📄:开始处理条款数据结果")
            failed_results = []
            failed_clauses = []
            successful_results = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logging.error(f"条款 {i + 1} 处理失败: {result}")
                    self.result_stats.error += 1
                    self.result_stats.error_msg += f"条款 {i + 1} 处理失败: {result}\n"
                    failed_results.append((i + 1, result))
                    # 将对应的clause保存至failed_clauses
                    failed_clauses.append(clauses[i])
                else:
                    successful_results.append(result)
            if failed_results:
                logging.error("📄🔥：以下条款处理失败")
                logging.info("==============================================================")
                for i, result in failed_results:
                    logging.error(f"条款 {i} 处理失败: {result}")
                logging.info("==============================================================")

            if not self.lenient_mode and failed_results:
                # TODO
                logging.error("📄🔴🔴🔴：严谨模式：存在处理失败的法条，请检查问题！！！")
                raise ValueError("存在处理失败的法条，请检查问题！！！")
            final_kg = await self.process_extracted_data(
                filename=filename,
                extracted_file_info=file_info_result,
                extracted_success_clauses=successful_results,
                extracted_failed_clauses=failed_clauses
            )
            logging.info("📄:结束处理条款数据")

            return final_kg
        except Exception as e:
            # TODO: 如果缓存中存在结果，将缓存保存起来
            logging.error("📄❌：条款知识图谱抽取报错: %s", e)
            raise e

    async def split_clause(
            self,
            text: str
    ) -> dict:
        """
实现功能：切分法规文件，将其整理成法规文件基础信息、条款的结构化数据，大致逻辑如下：
1. file_info记录文件开头内容，current_part记录当前编，current_subpart记录当前分编，current_chapter记录当前章，current_section记录当前节。
2. 读取文件内容，若读取到编内容（即"第X编 XXX"），则current_part记录当前编内容；若读取到分编内容（即"第X分编 XXX"），则current_subpart记录当前分编内容；
    若读取到章内容（即"第X章 XXX"），则current_chapter记录当前章内容（即"第X章 XXX"）；
    若读取到节内容（即"第X节 XXX"），则current_section记录当前节内容（即"第X节 XXX"）；
    若读取到条款内容（即"第X条 XXX"），则将current_part、current_subpart、current_chapter、current_section、条款内容记录到结果中；
3. file_info从开头开始记录，如果读取到"第一节"，则将"第一节"前的内容记录到file_info中，并删除file_info文本末尾的章和节（如果有的话）
4. 条款内容为上一个"第X条"到下一个"第X条"之间的内容，或者读到章或节的标志，或者读到文件末尾，或者读到"附录"、"附件"等条款部分结束标志。
    切分后的数据结构：
    {
        "file_info": "文件介绍相关内容",
        "clauses": [
            {
                "编": "编内容",
                "分编": "分编内容",
                "章": "章内容",
                "节": "节内容",
                "条款编号": "第几条"
                "条款内容": "条款内容，不包含开头的"第几条""
            }
        ]
    }
        :param text:
        :return:
        """
        # 定义正则表达式模式 - 匹配行首的"第X编/分编/章/节/条 "格式（中间可能有空格，但后面必须有空格）
        part_pattern = re.compile(r'^第[零一二三四五六七八九十百千万\d]+\s*编\s+.*', re.MULTILINE)
        subpart_pattern = re.compile(r'^第[零一二三四五六七八九十百千万\d]+\s*分编\s+.*', re.MULTILINE)
        chapter_pattern = re.compile(r'^第[零一二三四五六七八九十百千万\d]+\s*章\s+.*', re.MULTILINE)
        section_pattern = re.compile(r'^第[零一二三四五六七八九十百千万\d]+\s*节\s+.*', re.MULTILINE)
        clause_pattern = re.compile(
            r'^第([零一二三四五六七八九十百千万\d]+)\s*条\s*(之[零一二三四五六七八九十百千万\d]+)?\s*(.*)',
            re.MULTILINE
        )

        # 定义结束标志
        end_markers = ['附录', '附件', '附表', '后记', '参考文献', '索引']

        lines = text.split('\n')

        # 初始化变量
        file_info = ""
        current_part = ""
        current_subpart = ""
        current_chapter = ""
        current_section = ""
        clauses = []
        heading_strip_chars = ' \t\r\n\f\v#-*•·'

        def _normalize_heading(raw_line: str) -> str:
            return raw_line.strip().lstrip(heading_strip_chars)

        def _build_clause_result(clause_number: str, clause_content: str) -> dict:
            return {
                "编": clean_string(current_part),
                "分编": clean_string(current_subpart),
                "章": clean_string(current_chapter),
                "节": clean_string(current_section),
                "条款编号": clean_string(clause_number),
                "条款内容": clean_string(clause_content)
            }

        # 记录file_info的结束位置（第一章或第一节）
        file_info_end_idx = None
        clause_start_idx = None
        for i, line in enumerate(lines):
            if not line.strip():
                continue
            normalized_line = _normalize_heading(line)
            # 逐行进行匹配
            if part_pattern.match(normalized_line):
                file_info_end_idx = i
                current_part = normalized_line
                current_subpart = ""
                current_chapter = ""
                current_section = ""
            if subpart_pattern.match(normalized_line):
                file_info_end_idx = i
                current_subpart = normalized_line
                current_chapter = ""
                current_section = ""
            if chapter_pattern.match(normalized_line):
                # 记录开头内容的结束位置（不包含该行）
                file_info_end_idx = i
                # 将章内容记录到current_chapter中
                current_chapter = normalized_line
                current_section = ""
            if section_pattern.match(normalized_line):
                # 将节内容记录到current_section中
                current_section = normalized_line
            if clause_pattern.match(normalized_line):
                # 记录条款内容的开始位置（包含该行）
                clause_start_idx = i
                if file_info_end_idx is None:
                    file_info_end_idx = i
                break

        if clause_start_idx is None:
            logging.error("📄❌：文本中匹配条款失败")
            self.result_stats.error += 1
            self.result_stats.error_msg += f"文本中匹配条款失败:\n{text[:500]}\n"
            raise ValueError("文本中匹配条款失败")

        try:
            # 提取file_info：从开头到第一节之前的内容
            file_info = '\n'.join(lines[:file_info_end_idx]).rstrip()
            # 提取第一条的条款内容，并将其拼接进file_info中

            current_clause_content = ""
            current_clause_number = ""

            # 遍历条款内容
            for i in range(clause_start_idx, len(lines)):
                line = lines[i]
                if not line.strip():
                    continue
                # 检查是否到达结束标志
                is_end_marker = False
                for marker in end_markers:
                    if line.lstrip().startswith(marker):
                        is_end_marker = True
                        break
                if is_end_marker:
                    # 如果当前正在收集条款内容，则保存它
                    if current_clause_content:
                        clauses.append(_build_clause_result(current_clause_number, current_clause_content))
                        current_clause_content = ""
                        current_clause_number = ""
                    break

                normalized_line = _normalize_heading(line)

                # 检查是否是编（匹配行首）
                if part_pattern.match(normalized_line):
                    # 如果当前正在收集条款内容，则先保存它
                    if current_clause_content:
                        clauses.append(_build_clause_result(current_clause_number, current_clause_content))
                        current_clause_content = ""
                        current_clause_number = ""

                    # 更新当前编；新编下的分编、章、节重新开始
                    current_part = normalized_line
                    current_subpart = ""
                    current_chapter = ""
                    current_section = ""
                    continue

                # 检查是否是分编（匹配行首）
                if subpart_pattern.match(normalized_line):
                    # 如果当前正在收集条款内容，则先保存它
                    if current_clause_content:
                        clauses.append(_build_clause_result(current_clause_number, current_clause_content))
                        current_clause_content = ""
                        current_clause_number = ""

                    # 更新当前分编；新分编下的章、节重新开始
                    current_subpart = normalized_line
                    current_chapter = ""
                    current_section = ""
                    continue

                # 检查是否是章（匹配行首）
                if chapter_pattern.match(normalized_line):
                    # 如果当前正在收集条款内容，则先保存它
                    if current_clause_content:
                        clauses.append(_build_clause_result(current_clause_number, current_clause_content))
                        current_clause_content = ""
                        current_clause_number = ""

                    # 更新当前章
                    current_chapter = normalized_line
                    # 清空当前节
                    current_section = ""
                    continue

                # 检查是否是节（匹配行首）
                if section_pattern.match(normalized_line):
                    # 如果当前正在收集条款内容，则先保存它
                    if current_clause_content:
                        clauses.append(_build_clause_result(current_clause_number, current_clause_content))
                        current_clause_content = ""
                        current_clause_number = ""

                    # 更新当前节
                    current_section = normalized_line
                    continue

                # 检查是否是条款（匹配行首）
                if clause_pattern.match(normalized_line):
                    # 如果当前正在收集条款内容，则先保存之前的条款
                    if current_clause_content:
                        clauses.append(_build_clause_result(current_clause_number, current_clause_content))

                    # 提取条款编号和内容
                    clause_match = clause_pattern.match(normalized_line)
                    # 从捕获组直接获取编号
                    clause_num_part = clause_match.group(1)  # 编号部分
                    clause_suffix = clause_match.group(2) or ""  # 如“之一”“之二”等补充条编号
                    current_clause_number = f"第{clause_num_part}条{clause_suffix}"  # 重构完整编号
                    current_clause_content = clause_match.group(3).strip()  # 内容部分
                    continue

                # 如果当前正在收集条款内容，则添加到当前条款内容
                if current_clause_content:
                    if current_clause_content:  # 如果已有内容，在前面加上换行符
                        current_clause_content += '\n' + line
                    else:  # 如果还没有内容，直接赋值
                        current_clause_content = line
            # 处理最后一个条款（如果有）
            if current_clause_content:
                clauses.append(_build_clause_result(current_clause_number, current_clause_content))

            if not clauses:
                logging.error("📄❌：未找到条款内容")
                self.result_stats.error += 1
                self.result_stats.error_msg += "未找到条款内容\n" + text[:500] + "\n"
                raise ValueError("未找到条款内容")

            base_clauses = [
                clause for clause in clauses
                if not _is_inserted_clause_number(clause.get("条款编号", ""))
            ]
            if not base_clauses:
                logging.error("📄❌：未找到基础条款内容")
                self.result_stats.error += 1
                self.result_stats.error_msg += "未找到基础条款内容\n" + text[:500] + "\n"
                raise ValueError("未找到基础条款内容")

            last_base_clause_number = base_clauses[-1].get("条款编号", "")
            last_base_clause_order = _parse_clause_order_number(last_base_clause_number)
            base_clause_count = len(base_clauses)
            inserted_clause_count = len(clauses) - base_clause_count
            if last_base_clause_order != base_clause_count:
                error_msg = (
                    f"条款编号校验失败：最后基础条为 {last_base_clause_number}，"
                    f"解析序号为 {last_base_clause_order}，"
                    f"但基础条切分数为 {base_clause_count}，"
                    f"插入条切分数为 {inserted_clause_count}，"
                    f"总切分数为 {len(clauses)}"
                )
                logging.error(f"📄❌：{error_msg}")
                self.result_stats.error += 1
                self.result_stats.error_msg += error_msg + "\n" + text[:500] + "\n"
                raise ValueError(error_msg)

            # 将第一条、第二条、倒数第二条、最后一条的条款内容拼接至 file_info 中，数量不足时去重避免重复追加。
            info_clause_indexes = [0, 1, len(clauses) - 2, len(clauses) - 1]
            for idx in dict.fromkeys(i for i in info_clause_indexes if 0 <= i < len(clauses)):
                file_info += '\n' + clauses[idx]['条款内容']
            return {
                "file_info": clean_string(file_info),
                "clauses": clauses
            }
        except Exception as e:
            logging.error(f"📄❌：提取文件信息时出错 - {e}")
            self.result_stats.error += 1
            self.result_stats.error_msg += f"提取文件信息时出错 - {e}\n" + text[:500] + "\n"
            raise ValueError(f"提取文件信息时出错 - {e}")

    async def kg_extract_from_file_info(
            self,
            filename: str,
            clause_cache: ClauseCache,
            file_info: str
    ) -> dict:
        """
        抽取单个条款图谱数据
        图谱数据结构：
        {
          "node_id": "",
          "node_name": "",
          "node_type": "",
          "properties": {},
          "文件依据":[node]
        }
        :param filename: 文件名
        :param clause_cache: 条款缓存
        :param file_info: 文件信息
        :return:
        """
        try:
            file_info_result = {
                "node_id": "",
                "node_name": "",
                "node_type": "",
                "properties": {},
                "法规依据": []
            }
            # 抽取参数
            extract_prompt = prompt_for_file_info
            extract_schema = schema_for_file_info
            extract_examples = example_for_file_info
            extract_content = f"{filename} 文件描述：\n{file_info}"

            extract_result = await self.extractor.entity_and_relationship_extract(
                user_prompt=extract_prompt,
                schema=extract_schema,
                input_text=extract_content,
                examples=extract_examples,
            )
            entities = extract_result.get("entities", [])
            # 多余法规文件标志，一个法规文件信息中只能有一个法规文件，处理一个法规文件后置标志为真
            file_info_processed = False
            for entity in entities:
                if not isinstance(entity, Entity):
                    logging.error(f"📄❌：实体类型错误 - {entity}")
                    self.result_stats.error += 1
                    self.result_stats.error_msg += f"实体类型错误 - {entity}\n"

                # 保存文件信息
                entity_type = clean_string_with_only_words(entity.entity_type)
                if entity_type == "法规文件":
                    if file_info_processed:
                        logging.warning("📄强警告：一个法规文件信息中只能有一个法规文件，请检查问题")
                        self.result_stats.strong_warning += 1
                        self.result_stats.strong_warning_msg += f"一个法规文件信息中只能有一个法规文件，请检查问题:\n{entity}\n"
                        continue
                    node_name = entity.name
                    if not node_name:
                        logging.warning("📄强警告：法规文件名称为空")
                        self.result_stats.strong_warning += 1
                        self.result_stats.strong_warning_msg += f"法规文件名称为空:\n{entity}\n"
                        continue
                    node_id = clean_string_for_neo4j_extended(f"{entity_type}_{generate_hex_uuid()}")
                    file_info_result["node_id"] = node_id
                    file_info_result["node_name"] = node_name
                    file_info_result["node_type"] = entity_type
                    file_info_result["properties"] = _normalize_law_properties(
                        entity.properties,
                        text=file_info,
                        is_file_info=True,
                    )
                    file_info_processed = True
                elif entity_type == "法规依据":
                    node_name = entity.name
                    if not node_name:
                        logging.warning("📄强警告：法规依据名称为空")
                        self.result_stats.strong_warning += 1
                        self.result_stats.strong_warning_msg += f"法规依据名称为空:\n{entity}\n"
                        continue
                    node_id = clean_string_for_neo4j_extended(f"{entity_type}_{generate_hex_uuid()}")
                    file_info_result["法规依据"].append({
                        "node_id": node_id,
                        "node_name": node_name,
                        "node_type": entity_type,
                        "properties": entity.properties
                    })
                else:
                    logging.warning(f"📄强警告：未知实体类型 - {entity}")
                    self.result_stats.strong_warning += 1
                    self.result_stats.strong_warning_msg += f"未知实体类型 - {entity}\n"
            clause_cache.file_info = file_info_result
            return file_info_result
        except Exception as e:
            logging.error(f"📄❌：抽取文件信息时出错 - {e}")
            self.result_stats.error += 1
            self.result_stats.error_msg += f"抽取文件信息时出错 - {e}\n" + file_info[:500] + "\n"
            raise ValueError(f"抽取文件信息时出错 - {e}")

    async def kg_extract_from_clause(
            self,
            filename: str,
            clause_cache: ClauseCache,
            one_clause: dict
    ) -> dict:
        """
        抽取单个条款图谱数据
        图谱数据结构：
        {
            "node_id": "",
            "node_name": "",
            "node_type": "",
            "properties": {
                "编": "current_part",
                "分编": "current_subpart",
                "章": "current_chapter",
                "节": "current_section",
                "条": "current_clause_number",
                "法条全文": "current_clause_content",
                ...
            },
            "条款单元": [
                {
                    "node_id": "",
                    "node_name": "",
                    "node_type": "",
                    "properties": {},
                    "外部引用依据": [],
                    "内部引用依据": []
                }
            ]
            ...
        }
        :param filename: 文件名
        :param one_clause:单个条款数据
        :param clause_cache:
        :return:
        """
        async with self.semaphore:
            try:
                clause_result = {
                    "node_id": "",
                    "node_name": "",
                    "node_type": "",
                    "properties": {},
                    "条款单元": []
                }

                # 获取条款信息
                part = one_clause.get("编", "")
                subpart = one_clause.get("分编", "")
                chapter = one_clause.get("章", "")
                section = one_clause.get("节", "")
                clause_number = one_clause.get("条款编号", "")
                clause_content = one_clause.get("条款内容", "")
                if not clause_number:
                    logging.error(f"📄❌：条款编号为空:{one_clause}")
                    self.result_stats.error += 1
                    self.result_stats.error_msg += f"条款编号为空:{one_clause}\n"
                    raise ValueError("条款编号为空")
                # 抽取参数
                extract_prompt = prompt_for_clause
                extract_schema = schema_for_clause
                extract_examples = example_for_clause
                extract_content = f"{filename} "
                if part:
                    extract_content += f"{part} "
                if subpart:
                    extract_content += f"{subpart} "
                if chapter:
                    extract_content += f"{chapter} "
                if section:
                    extract_content += f"{section} "
                extract_content += f"{clause_number}：\n{clause_content}"
                # 抽取条款内容中的实体
                """
                {
                    "entities": list[Entity],
                    "relations": list[Relationship],
                    "texts_classes": list[TextClass]
                }
                """
                extract_result = await self.extractor.entity_and_relationship_extract(
                    user_prompt=extract_prompt,
                    schema=extract_schema,
                    input_text=extract_content,
                    examples=extract_examples,
                )
                # 临时保存条款单元和引用依据
                clause_units = {}
                references = {}
                processed_keys = []
                # 获取实体和关系
                entities = extract_result.get("entities", [])
                relations = extract_result.get("relations", [])
                # 处理实体
                clause_processed = False
                for entity in entities:
                    if not isinstance(entity, Entity):
                        logging.error(f"📄❌：实体类型错误 - {entity}")
                        self.result_stats.error += 1
                        self.result_stats.error_msg += f"实体类型错误 - {entity}\n"
                        continue
                    entity_type = clean_string_with_only_words(entity.entity_type)
                    if entity_type == "法条":
                        if clause_processed:
                            logging.warning("📄强警告：一个法条中只能有一个法条，请检查问题")
                            self.result_stats.strong_warning += 1
                            self.result_stats.strong_warning_msg += f"一个法条中只能有一个法条，请检查问题:\n{entity}\n"
                            continue
                        node_name = entity.name
                        if not node_name:
                            logging.warning("📄强警告：法条名称为空")
                            self.result_stats.strong_warning += 1
                            self.result_stats.strong_warning_msg += f"法条名称为空:\n{entity}\n"
                            continue
                        node_id = clean_string_for_neo4j_extended(f"{entity_type}_{generate_hex_uuid()}")
                        clause_result["node_id"] = node_id
                        clause_result["node_name"] = clause_number
                        clause_result["node_type"] = "法条"
                        clause_result["properties"] = _normalize_law_properties(
                            entity.properties,
                            text=clause_content,
                            node_type="法条",
                        )
                        clause_result["properties"]["编"] = part
                        clause_result["properties"]["分编"] = subpart
                        clause_result["properties"]["章"] = chapter
                        clause_result["properties"]["节"] = section
                        clause_result["properties"]["条"] = clause_number
                        clause_result["properties"]["法条全文"] = clause_content
                        clause_processed = True
                    elif entity_type == "条款单元":
                        node_name = entity.name
                        if not node_name:
                            logging.warning("📄强警告：条款单元名称为空")
                            self.result_stats.strong_warning += 1
                            self.result_stats.strong_warning_msg += f"条款单元名称为空:\n{entity}\n"
                            continue
                        node_id = clean_string_for_neo4j_extended(f"{entity_type}_{generate_hex_uuid()}")
                        clause_key = f"{entity_type}_{node_name}"
                        clause_units[clause_key] = {
                            "node_id": node_id,
                            "node_name": node_name,
                            "node_type": "条款单元",
                            "properties": _normalize_law_properties(
                                entity.properties,
                                text=entity.properties.get("条款单元内容") or clause_content,
                                node_type="条款单元",
                            ),
                            "外部引用依据": [],
                            "内部引用依据": []
                        }
                    elif entity_type == "引用依据":
                        node_name = entity.name
                        if not node_name:
                            logging.warning("📄强警告：引用依据名称为空")
                            self.result_stats.strong_warning += 1
                            self.result_stats.strong_warning_msg += f"引用依据名称为空:\n{entity}\n"
                            continue
                        node_id = clean_string_for_neo4j_extended(f"{entity_type}_{generate_hex_uuid()}")
                        reference_key = f"{entity_type}_{node_name}"
                        references[reference_key] = {
                            "node_id": node_id,
                            "node_name": node_name,
                            "node_type": "引用依据",
                            "properties": entity.properties
                        }
                    else:
                        logging.error(f"📄❌：未知实体类型 - {entity}")
                        self.result_stats.error += 1
                        self.result_stats.error_msg += f"未知实体类型 - {entity}\n"
                # 处理关系
                for relation in relations:
                    try:
                        if not isinstance(relation, Relationship):
                            logging.error(f"📄❌：关系类型错误 - {relation}")
                            self.result_stats.error += 1
                            self.result_stats.error_msg += f"关系类型错误 - {relation}\n"
                            continue
                        source_key = relation.source
                        target_key = relation.target
                        relation_type = clean_string_with_only_words(relation.type)
                        if not source_key or not target_key or not relation_type:
                            logging.warning("📄强警告：关系的源节点或目标节点为空")
                            self.result_stats.strong_warning += 1
                            self.result_stats.strong_warning_msg += f"关系的源节点或目标节点为空:\n{relation}\n"
                            continue
                        if relation_type == "引用":
                            # 获取条款单元
                            source_entity = clause_units.get(source_key)
                            if not source_entity:
                                logging.warning(f"📄强警告：引用关系的源节点不存在{relation}")
                                self.result_stats.strong_warning += 1
                                self.result_stats.strong_warning_msg += f"引用关系的源节点不存在:\n{relation}\n"
                                # TODO 启动模糊匹配
                                continue
                            # 获取引用依据
                            target_entity = references.get(target_key)
                            if not target_entity:
                                logging.warning(f"📄强警告：引用关系的目标节点不存在{relation}")
                                self.result_stats.strong_warning += 1
                                self.result_stats.strong_warning_msg += f"引用关系的目标节点不存在:\n{relation}\n"
                                # TODO 启动模糊匹配
                                continue
                            # 获取引用依据是否是内部条款
                            inner_tag = clean_string_with_only_words(
                                target_entity.get("properties", {}).get("是否本文件内引用", "否"))
                            if inner_tag == "是":
                                source_entity["内部引用依据"].append(target_entity)
                            else:
                                source_entity["外部引用依据"].append(target_entity)
                            processed_keys.append(target_key)
                    except Exception as e:
                        logging.error(f"📄❌：处理关系{relation}时出错 - {e}")
                        self.result_stats.error += 1
                        self.result_stats.error_msg += f"处理关系{relation}时出错 - {e}\n"
                        continue
                # 检验未被使用的引用依据
                for key, _ in references.items():
                    if key not in processed_keys:
                        logging.warning(f"📄强警告：引用依据{key}未被使用！")
                        self.result_stats.strong_warning += 1
                        self.result_stats.strong_warning_msg += f"引用依据{key}未被使用！\n"
                # 将条款单元添加到结果中
                for entity in clause_units.values():
                    clause_result["条款单元"].append(entity)
                clause_cache.clause_cache[clause_number] = clause_result
                return clause_result
            except Exception as e:
                logging.error(f"📄❌：处理文件{filename}时出错 - {e}")
                self.result_stats.error += 1
                self.result_stats.error_msg += f"处理文件{filename}时出错 - {e}\n"

    async def process_extracted_data(
            self,
            filename: str,
            extracted_file_info: dict,
            extracted_success_clauses: list[dict],
            extracted_failed_clauses: list[dict]
    ) -> dict:
        """
        处理提取的法规文件信息

        :param filename:
        :param extracted_file_info:
        :param extracted_success_clauses:
        :param extracted_failed_clauses:
        :return:
        """
        try:
            final_kg = {
                "nodes": [],
                "edges": []
            }
            # 将法规文件和文件依据节点和关系加入
            file_node_id = extracted_file_info.get("node_id")
            file_node_name = extracted_file_info.get("node_name")
            file_node_type = extracted_file_info.get("node_type")
            if not file_node_id or not file_node_name or not file_node_type:
                logging.error(f"📄🔥：法规文件信息不完整{extracted_file_info}")
                self.result_stats.error += 1
                self.result_stats.error_msg += f"法规文件信息不完整{extracted_file_info}\n"
                raise ValueError("法规文件信息不完整")
            final_kg["nodes"].append(
                {
                    "node_id": file_node_id,
                    "node_name": file_node_name,
                    "node_type": file_node_type,
                    "properties": extracted_file_info.get("properties", {}),
                    "filename": filename
                }
            )
            # 处理法规依据
            file_basis = extracted_file_info.get("法规依据", [])
            for basis in file_basis:
                basis_node_id = basis.get("node_id")
                basis_node_name = basis.get("node_name")
                basis_node_type = basis.get("node_type")
                if not basis_node_id or not basis_node_name or not basis_node_type:
                    logging.warning(f"📄🔧强警告：法规依据信息不完整{basis}")
                    self.result_stats.strong_warning += 1
                    self.result_stats.strong_warning_msg += f"法规依据信息不完整{basis}\n"
                    continue
                final_kg["nodes"].append(
                    {
                        "node_id": basis_node_id,
                        "node_name": basis_node_name,
                        "node_type": basis_node_type,
                        "properties": basis.get("properties", {}),
                        "filename": filename
                    }
                )
                final_kg["edges"].append(
                    {
                        "source_id": file_node_id,
                        "target_id": basis_node_id,
                        "relation_type": "依据",
                        "directionality": "单向",
                        "properties": {},
                        "filename": filename
                    }
                )

            # 将extracted_failed_clauses中的法条节点和关系加入
            for clause in extracted_failed_clauses:
                clause_number = clause.get("条款编号")
                clause_text = clause.get("条款内容")
                if not clause_number or not clause_text:
                    logging.warning(f"📄🔧强警告：法条信息不完整{clause}")
                    self.result_stats.strong_warning += 1
                    self.result_stats.strong_warning_msg += f"法条信息不完整{clause}\n"
                    continue
                clause_node_id = clean_string_for_neo4j_extended(f"法条_{generate_hex_uuid()}")
                clause_node_name = clause_number
                clause_node_type = "法条"
                clause_properties = {
                    "编": clause.get("编"),
                    "分编": clause.get("分编"),
                    "章": clause.get("章"),
                    "节": clause.get("节"),
                    "条": clause_number,
                    "法条全文": clause_text
                }
                final_kg["nodes"].append(
                    {
                        "node_id": clause_node_id,
                        "node_name": clause_node_name,
                        "node_type": clause_node_type,
                        "properties": clause_properties,
                        "filename": filename
                    }
                )
                final_kg["edges"].append(
                    {
                        "source_id": file_node_id,
                        "target_id": clause_node_id,
                        "relation_type": "包含",
                        "directionality": "单向",
                        "properties": {},
                        "filename": filename
                    }
                )

            # 引用依据映射
            inner_reference_mapping = {}
            outer_reference_mapping = {}
            inner_reference_id_mapping = {}
            outer_reference_id_mapping = {}
            # 条款单元到法条的映射
            unit_to_clause_mapping = {}
            # 将extracted_success_clauses中的法条、条款单元、引用依据节点和关系加入
            for clause in extracted_success_clauses:
                clause_node_id = clause.get("node_id")
                clause_node_name = clause.get("node_name")
                clause_node_type = clause.get("node_type")
                if not clause_node_id or not clause_node_name or not clause_node_type:
                    logging.warning(f"📄🔧强警告：法条信息不完整{clause}")
                    self.result_stats.strong_warning += 1
                    self.result_stats.strong_warning_msg += f"法条信息不完整{clause}\n"
                    continue
                # 添加法条节点和关系
                final_kg["nodes"].append(
                    {
                        "node_id": clause_node_id,
                        "node_name": clause_node_name,
                        "node_type": clause_node_type,
                        "properties": clause.get("properties", {}),
                        "filename": filename
                    }
                )
                final_kg["edges"].append(
                    {
                        "source_id": file_node_id,
                        "target_id": clause_node_id,
                        "relation_type": "包含",
                        "directionality": "单向",
                        "properties": {},
                        "filename": filename
                    }
                )
                # 将法条作为内部引用依据之一
                try:
                    clause_article = clean_string_with_only_words(clause.get("properties", {}).get("条"))
                except Exception:
                    clause_article = ""
                if not clause_article:
                    logging.warning(f"📄🔧法条信息缺少条编号{clause}")
                    self.result_stats.week_warning += 1
                    self.result_stats.week_warning_msg += f"法条信息缺少条编号{clause}\n"
                else:
                    inner_reference_id_mapping[clause_article] = clause_node_id
                # 处理条款单元节点和关系
                clause_units = clause.get("条款单元", [])
                for unit in clause_units:
                    unit_node_id = unit.get("node_id")
                    unit_node_name = unit.get("node_name")
                    unit_node_type = unit.get("node_type")
                    if not unit_node_id or not unit_node_name or not unit_node_type:
                        logging.warning(f"📄🔧强警告：条款单元信息不完整{unit}")
                        self.result_stats.strong_warning += 1
                        self.result_stats.strong_warning_msg += f"条款单元信息不完整{unit}\n"
                        continue
                    final_kg["nodes"].append(
                        {
                            "node_id": unit_node_id,
                            "node_name": unit_node_name,
                            "node_type": unit_node_type,
                            "properties": unit.get("properties", {}),
                            "filename": filename
                        }
                    )
                    final_kg["edges"].append(
                        {
                            "source_id": clause_node_id,
                            "target_id": unit_node_id,
                            "relation_type": "包含",
                            "directionality": "单向",
                            "properties": {},
                            "filename": filename
                        }
                    )
                    # 将条款单元到法条的映射加入
                    unit_to_clause_mapping[unit_node_id] = clause_node_id
                    # 将条款单元作为内部引用依据之一
                    try:
                        unit_article = clean_string_with_only_words(unit.get("properties", {}).get("单元编号"))
                    except Exception:
                        unit_article = ""
                    if not unit_article:
                        logging.warning(f"📄🔧：条款单元信息缺少单元编号{unit}")
                        self.result_stats.week_warning += 1
                        self.result_stats.week_warning_msg += f"条款单元信息缺少单元编号{unit}\n"
                    else:
                        inner_reference_id_mapping[unit_article] = unit_node_id
                    # 添加内部引用依据和外部引用依据
                    inner_reference = unit.get("内部引用依据", [])
                    for ref in inner_reference:
                        inner_reference_mapping[unit_node_id] = ref
                    outer_reference = unit.get("外部引用依据", [])
                    for ref in outer_reference:
                        outer_reference_mapping[unit_node_id] = ref

            # 处理内部引用依据
            for unit_node_id, inner_ref in inner_reference_mapping.items():
                ref_node_id = inner_ref.get("node_id")
                ref_node_name = inner_ref.get("node_name")
                ref_node_type = inner_ref.get("node_type")
                if not ref_node_id or not ref_node_name or not ref_node_type:
                    logging.warning(f"📄🔧强警告：引用依据信息不完整{inner_ref}")
                    self.result_stats.strong_warning += 1
                    self.result_stats.strong_warning_msg += f"引用依据信息不完整{inner_ref}\n"
                    continue
                # 匹配内部条款单元
                try:
                    inner_ref_article = clean_string_with_only_words(inner_ref.get("properties", {}).get("条款编号"))
                except Exception:
                    inner_ref_article = ""
                if not inner_ref_article:
                    logging.warning(f"📄🔧：引用依据信息缺少款项编号{inner_ref}")
                    self.result_stats.week_warning += 1
                    self.result_stats.week_warning_msg += f"引用依据信息缺少款项编号{inner_ref}\n"
                else:
                    ref_unit_node_id = inner_reference_id_mapping.get(inner_ref_article)
                    if not ref_unit_node_id:
                        logging.warning(f"📄🔧：引用依据款项编号未找到对应本文件条款单元{inner_ref}")
                        self.result_stats.week_warning += 1
                        self.result_stats.week_warning_msg += f"引用依据款项编号未找到对应本文件条款单元{inner_ref}\n"
                        # TODO：加入模糊匹配
                        # 如果是“第X条第X项”，则尝试匹配“第X条第一款第X项”
                        match = re.match(
                            r'^(第[零一二三四五六七八九十百千万\d]+条)(第[零一二三四五六七八九十百千万\d]+项)$',
                            inner_ref_article)
                        if match:
                            article_part, item_part = match.groups()
                            # 尝试"第X条第一款第X项"格式
                            alternative_article = f"{article_part}第一款{item_part}"
                            logging.warning(f"📄🔧：尝试匹配{alternative_article}")
                            self.result_stats.week_warning += 1
                            self.result_stats.week_warning_msg += f"尝试匹配{alternative_article}未找到对应本文件条款单元{inner_ref}\n"
                            ref_unit_node_id = inner_reference_id_mapping.get(alternative_article)
                        if not ref_unit_node_id:
                            # 尝试匹配“第X条第X款”
                            match = re.match(
                                r'^(第[零一二三四五六七八九十百千万\d]+条)(第[零一二三四五六七八九十百千万\d]+款)$',
                                inner_ref_article)
                            if match:
                                article_part, clause_part = match.groups()
                                alternative_article = f"{article_part}{clause_part}"
                                logging.warning(f"📄🔧：尝试匹配{alternative_article}")
                                self.result_stats.week_warning += 1
                                self.result_stats.week_warning_msg += f"尝试匹配{alternative_article}未找到对应本文件条款单元{inner_ref}\n"
                                ref_unit_node_id = inner_reference_id_mapping.get(alternative_article)
                            if not ref_unit_node_id:
                                # 如果含“第X条”，则尝试匹配“第X条”
                                match = re.match(r'(第[零一二三四五六七八九十百千万\d]+条)', inner_ref_article)
                                if match:
                                    basic_article = match.group(1)
                                    logging.warning(f"📄🔧：尝试匹配{basic_article}")
                                    ref_unit_node_id = inner_reference_id_mapping.get(basic_article)
                    if not ref_unit_node_id:
                        logging.warning(f"📄🔧强警告：模糊匹配后引用依据款项编号未找到对应本文件条款单元{inner_ref}")
                        self.result_stats.strong_warning += 1
                        self.result_stats.strong_warning_msg += f"模糊匹配后引用依据款项编号未找到对应本文件条款单元{inner_ref}\n"
                    if ref_unit_node_id == unit_node_id:
                        logging.warning(f"📄🔧：引用依据款项编号与当前条款单元编号一致{inner_ref}")
                        self.result_stats.week_warning += 1
                        self.result_stats.week_warning_msg += f"引用依据款项编号与当前条款单元编号一致{inner_ref}\n"
                    elif ref_unit_node_id == unit_to_clause_mapping.get(unit_node_id):
                        logging.warning(f"📄🔧：引用依据款项编号与当前条款单元对应的法条编号一致{inner_ref}")
                        self.result_stats.week_warning += 1
                        self.result_stats.week_warning_msg += f"引用依据款项编号与当前条款单元对应的法条编号一致{inner_ref}\n"
                    else:
                        final_kg["edges"].append(
                            {
                                "source_id": unit_node_id,
                                "target_id": ref_unit_node_id,
                                "relation_type": "依据",
                                "directionality": "单向",
                                "properties": {},
                                "filename": filename
                            }
                        )

            # 处理外部引用依据
            for unit_node_id, outer_ref in outer_reference_mapping.items():
                ref_node_id = outer_ref.get("node_id")
                ref_node_name = outer_ref.get("node_name")
                ref_node_type = outer_ref.get("node_type")
                if not ref_node_id or not ref_node_name or not ref_node_type:
                    logging.warning(f"📄🔧强警告：引用依据信息不完整{outer_ref}")
                    self.result_stats.strong_warning += 1
                    self.result_stats.strong_warning_msg += f"引用依据信息不完整{outer_ref}\n"
                    continue
                # 如果外部引用依据节点id映射不存在，则创建节点并建立关系
                ref_unit_node_id = outer_reference_id_mapping.get(clean_string_with_only_words(ref_node_name))
                # TODO:处理同义实体
                if not ref_unit_node_id:
                    final_kg["nodes"].append(
                        {
                            "node_id": ref_node_id,
                            "node_name": ref_node_name,
                            "node_type": ref_node_type,
                            "properties": outer_ref.get("properties", {}),
                            "filename": filename
                        }
                    )
                    final_kg["edges"].append(
                        {
                            "source_id": unit_node_id,
                            "target_id": ref_node_id,
                            "relation_type": "涉及",
                            "directionality": "单向",
                            "properties": {},
                            "filename": filename
                        }
                    )
                    outer_reference_id_mapping[clean_string_with_only_words(ref_node_name)] = ref_unit_node_id
                elif ref_unit_node_id == unit_node_id:
                    logging.warning(f"📄🔧：引用依据款项编号与当前条款单元编号一致{outer_ref}")
                    self.result_stats.week_warning += 1
                    self.result_stats.week_warning_msg += f"引用依据款项编号与当前条款单元编号一致{outer_ref}\n"
                # 如果外部引用依据节点id映射存在，则直接创建关系
                else:
                    final_kg["edges"].append(
                        {
                            "source_id": unit_node_id,
                            "target_id": ref_unit_node_id,
                            "relation_type": "涉及",
                            "directionality": "单向",
                            "properties": {},
                            "filename": filename
                        }
                    )
            return final_kg
        except Exception as e:
            logging.error(f"📄🔧：处理抽取数据时出错{e}")
            self.result_stats.error += 1
            self.result_stats.error_msg += f"处理抽取数据时出错{e}\n"
            raise e

    @staticmethod
    async def _save_cache_to_json(
            cache_data: ClauseCache,
            output_dir,
            filename
    ):
        """
        将条款数据保存为JSON格式
        :param cache_data: 字典数据
        :param output_dir: 输出目录路径
        :param filename: 文件名（不含扩展名）
        """
        # TODO
        pass

    @staticmethod
    async def _load_cache_from_json(
            filepath
    ):
        """
        从JSON文件中加载条款数据，并整理成dict
        :param filepath: 文件路径
        :return: 条款数据（字典或列表）
        """
        # TODO
        pass

    async def logging_result_stats(self):
        # logging.info("📄✅：错误与警告统计结果如下")
        # logging.info("======================================================================")
        # logging.info(f"📄✅：错误信息: {self.result_stats.error_msg}")
        # logging.info("======================================================================")
        # logging.info(f"📄✅：弱警告信息: {self.result_stats.week_warning_msg}")
        # logging.info("======================================================================")
        # logging.info(f"📄✅：强警告信息: {self.result_stats.strong_warning_msg}")
        # logging.info("======================================================================")
        logging.info("📄✅：数据统计")
        logging.info(f"📄✅：错误数: {self.result_stats.error}")
        logging.info(f"📄✅：弱警告数: {self.result_stats.week_warning}")
        logging.info(f"📄✅：强警告数: {self.result_stats.strong_warning}")


def clean_string(text: str) -> str:
    # 替换全角空格
    cleaned = replace_full_corner_space(text)
    # 替换零宽字符
    cleaned = replace_zero_width_chars(cleaned)
    # 将连续的换行符替换为单个换行符
    cleaned = re.sub(r'\n+', '\n', cleaned)
    # 移除行首行尾的空白字符
    cleaned = re.sub(r'^[ \t]+|[ \t]+$', '', cleaned, flags=re.MULTILINE)
    # 移除多余的空白字符
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    return cleaned


if __name__ == "__main__":
    # 读取示例文件
    file_path = r"F:\企业大脑知识库系统\8.1项目\数据处理\清洗的数据\国家规章库\安全生产\生产安全事故应急预案管理办法.txt"
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except FileNotFoundError:
        # 如果真实文件不存在，创建一个模拟的示例内容进行测试
        raise ValueError("文件不存在")

    extractor = ClauseExtractor()
    result = asyncio.run(
        extractor.split_clause(
            text=content
        )
    )

    # 打印结果
    print("文件信息:", result["file_info"])
    print("\n条款数量:", len(result["clauses"]))
    print("\n条款示例:")
    for i, clause in enumerate(result["clauses"]):
        print(f"\n条款 {i + 1}: ")
        print(f"  章: {clause['章']}")
        print(f"  节: {clause['节']}")
        print(f"  条款编号: {clause['条款编号']}")
        print(f"  条款内容: {clause['条款内容']}")

# 1. 统计抽取出来的条款数和最后一项条款的编号是对应上的
