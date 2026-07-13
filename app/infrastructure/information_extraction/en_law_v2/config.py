"""格式二美国法案文本知识图谱抽取配置。"""

from __future__ import annotations

import os


FORMAT_TWO_DOCUMENT_FORMAT = "format_two_us_act_section"

EN_LAW_MODEL = os.getenv("EN_LAW_MODEL", "qwen3-30b-a3b-instruct-2507")
EN_LAW_MODEL_API_KEY = os.getenv(
    "EN_LAW_MODEL_API_KEY",
    "gpustack_342609ce423be29a_4371426b285a91dc44fb4e8d72454847",
)
EN_LAW_MODEL_API_URL = os.getenv(
    "EN_LAW_MODEL_API_URL",
    "http://222.171.219.26:20001/v1/chat/completions",
)

EN_LAW_MAX_CHAR_BUFFER = int(os.getenv("EN_LAW_MAX_CHAR_BUFFER", "7500"))
EN_LAW_BATCH_LENGTH = int(os.getenv("EN_LAW_BATCH_LENGTH", "5"))
EN_LAW_MAX_WORKERS = int(os.getenv("EN_LAW_MAX_WORKERS", "3"))
EN_LAW_TIMEOUT = int(os.getenv("EN_LAW_TIMEOUT", "3000"))
EN_LAW_MAX_TOKENS = int(os.getenv("EN_LAW_MAX_TOKENS", "48000"))
EN_LAW_EXTRACTION_TIMEOUT = int(os.getenv("EN_LAW_EXTRACTION_TIMEOUT", "3600"))
EN_LAW_MAX_RETRIES = int(os.getenv("EN_LAW_MAX_RETRIES", "5"))
EN_LAW_MAX_CONCURRENT = int(os.getenv("EN_LAW_MAX_CONCURRENT", "5"))
EN_LAW_LENIENT_MODE = os.getenv("EN_LAW_LENIENT_MODE", "true").lower() in {"1", "true", "yes"}

RISK_TYPE_VALUES = [
    "产品法律风险",
    "供应链合规风险",
    "劳动用工法律合规风险",
    "企业关联方合规风险",
    "企业国际化经营合规风险",
    "企业信用风险",
]

DEFAULT_ACTUAL_DATA_DIR = (
    r"D:\CogmAIT\8.1项目\8.1数据\爬取的数据\风险法规\第三版\分类"
    r"\英文抽取所用数据\法文汇总\格式二"
)

DEFAULT_REPORT_DIR = (
    r"D:\CogmAIT\en_law\kg_extract_for_law"
    r"\app\infrastructure\information_extraction\en_law_v2\report"
)
