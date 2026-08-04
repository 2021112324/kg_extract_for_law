"""其他语种法规抽取配置。

本模块只保存翻译和编排所需的默认值。具体运行时可通过环境变量覆盖，
以便在真实批量抽取时调整模型、输出长度、重试次数和缓存位置。
"""

from __future__ import annotations

import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[4]
PACKAGE_DIR = Path(__file__).resolve().parent
DEFAULT_REPORT_DIR = PACKAGE_DIR / "report"
DEFAULT_TEST_DATA_DIR = PACKAGE_DIR / "test_data"

DEFAULT_OTHER_LANGUAGE_DATA_ROOT = (
    Path("D:/CogmAIT")
    / "8.1项目"
    / "8.1数据"
    / "爬取的数据"
    / "风险法规"
    / "第三版"
    / "分类"
    / "其他语言抽取所用数据"
)
DEFAULT_SOURCE_DIR = DEFAULT_OTHER_LANGUAGE_DATA_ROOT / "德国"
DEFAULT_TRANSLATED_DIR = DEFAULT_OTHER_LANGUAGE_DATA_ROOT / "德国_翻译"
DEFAULT_CACHE_PATH = DEFAULT_OTHER_LANGUAGE_DATA_ROOT / ".local_llm_translation_cache.jsonl"

OTHER_LANGUAGE_MODEL = os.getenv("OTHER_LANGUAGE_MODEL", os.getenv("EN_LAW_MODEL", "qwen3-30b-a3b-instruct-2507"))
OTHER_LANGUAGE_MODEL_API_KEY = os.getenv(
    "OTHER_LANGUAGE_MODEL_API_KEY",
    os.getenv("EN_LAW_MODEL_API_KEY", "gpustack_342609ce423be29a_4371426b285a91dc44fb4e8d72454847"),
)
OTHER_LANGUAGE_MODEL_API_URL = os.getenv(
    "OTHER_LANGUAGE_MODEL_API_URL",
    os.getenv("EN_LAW_MODEL_API_URL", "http://222.171.219.26:20001/v1/chat/completions"),
)

TRANSLATION_SOURCE_LANG = os.getenv("OTHER_LANGUAGE_SOURCE_LANG", "auto")
TRANSLATION_TARGET_LANG = os.getenv("OTHER_LANGUAGE_TARGET_LANG", "Simplified Chinese")
TRANSLATION_MAX_QUERY_CHARS = int(os.getenv("OTHER_LANGUAGE_TRANSLATION_MAX_QUERY_CHARS", "2200"))
TRANSLATION_MAX_TOKENS = int(os.getenv("OTHER_LANGUAGE_TRANSLATION_MAX_TOKENS", "4096"))
TRANSLATION_TEMPERATURE = float(os.getenv("OTHER_LANGUAGE_TRANSLATION_TEMPERATURE", "0"))
TRANSLATION_SLEEP_SECONDS = float(os.getenv("OTHER_LANGUAGE_TRANSLATION_SLEEP_SECONDS", "0"))
TRANSLATION_MAX_RETRIES = int(os.getenv("OTHER_LANGUAGE_TRANSLATION_MAX_RETRIES", "3"))
TRANSLATION_OVERWRITE = os.getenv("OTHER_LANGUAGE_TRANSLATION_OVERWRITE", "false").lower() in {"1", "true", "yes"}

SUPPORTED_FILE_SUFFIXES = {".md", ".txt"}
