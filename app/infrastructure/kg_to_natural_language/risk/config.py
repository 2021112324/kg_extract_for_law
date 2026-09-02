"""风险指标知识生成与排序的路径及环境配置。"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

from dotenv import load_dotenv


MODULE_DIR = Path(__file__).resolve().parent
PACKAGE_DIR = MODULE_DIR.parent
PROJECT_ROOT = PACKAGE_DIR.parents[2]
load_dotenv(PROJECT_ROOT / ".env")

DEFAULT_RISK_TREE_PATH = MODULE_DIR / "tree" / "risk2file_tree.json"
DEFAULT_RISK_TEMP_ROOT = PACKAGE_DIR / "temp" / "risk"
DEFAULT_RISK_DATA_ROOT = PACKAGE_DIR / "data" / "risk"
DEFAULT_RISK_STATS_ROOT = PACKAGE_DIR / "data" / "stats"

RISK_RELEVANCE_MODEL_NAME = os.getenv(
    "RISK_RELEVANCE_MODEL_NAME",
    "qwen3-30b-a3b-instruct-2507",
)
RISK_RELEVANCE_MODEL_API_URL = os.getenv(
    "RISK_RELEVANCE_MODEL_API_URL",
    "http://222.171.219.26:20001/v1/chat/completions",
)
RISK_RELEVANCE_MODEL_API_KEY_ENV = "RISK_RELEVANCE_MODEL_API_KEY"
RISK_RELEVANCE_MODEL_API_KEY = (
    "gpustack_342609ce423be29a_4371426b285a91dc44fb4e8d72454847"
)
RISK_RELEVANCE_MAX_CONCURRENCY = int(
    os.getenv("RISK_RELEVANCE_MAX_CONCURRENCY", "100")
)
RISK_RELEVANCE_TOTAL_TIMEOUT_SECONDS = float(
    os.getenv("RISK_RELEVANCE_TOTAL_TIMEOUT_SECONDS", "120")
)
RISK_RELEVANCE_MAX_RETRIES = int(
    os.getenv("RISK_RELEVANCE_MAX_RETRIES", "2")
)
RISK_RELEVANCE_CHECKPOINT_INTERVAL = int(
    os.getenv("RISK_RELEVANCE_CHECKPOINT_INTERVAL", "20")
)


class RiskRelevanceConfigError(ValueError):
    """关联度评分配置错误。"""


@dataclass(frozen=True)
class RiskRelevanceModelConfig:
    """一次评分运行使用的模型配置。"""

    model_name: str
    api_url: str
    api_key: str
    max_concurrency: int
    total_timeout_seconds: float
    max_retries: int
    checkpoint_interval: int

    @classmethod
    def from_env(
        cls,
        *,
        max_concurrency: int | None = None,
    ) -> "RiskRelevanceModelConfig":
        """在调用时读取环境变量，避免在模块导入时缓存密钥。"""
        config = cls(
            model_name=os.getenv(
                "RISK_RELEVANCE_MODEL_NAME",
                RISK_RELEVANCE_MODEL_NAME,
            ).strip(),
            api_url=os.getenv(
                "RISK_RELEVANCE_MODEL_API_URL",
                RISK_RELEVANCE_MODEL_API_URL,
            ).strip(),
            api_key=(
                os.getenv(
                    RISK_RELEVANCE_MODEL_API_KEY_ENV,
                    RISK_RELEVANCE_MODEL_API_KEY,
                ).strip()
            ),
            max_concurrency=(
                max_concurrency
                if max_concurrency is not None
                else int(
                    os.getenv(
                        "RISK_RELEVANCE_MAX_CONCURRENCY",
                        str(RISK_RELEVANCE_MAX_CONCURRENCY),
                    )
                )
            ),
            total_timeout_seconds=float(
                os.getenv(
                    "RISK_RELEVANCE_TOTAL_TIMEOUT_SECONDS",
                    str(RISK_RELEVANCE_TOTAL_TIMEOUT_SECONDS),
                )
            ),
            max_retries=int(
                os.getenv(
                    "RISK_RELEVANCE_MAX_RETRIES",
                    str(RISK_RELEVANCE_MAX_RETRIES),
                )
            ),
            checkpoint_interval=int(
                os.getenv(
                    "RISK_RELEVANCE_CHECKPOINT_INTERVAL",
                    str(RISK_RELEVANCE_CHECKPOINT_INTERVAL),
                )
            ),
        )
        config.validate()
        return config

    def validate(self) -> None:
        if not self.model_name:
            raise RiskRelevanceConfigError("关联度评分模型名称不能为空")
        parsed = urlsplit(self.api_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise RiskRelevanceConfigError("关联度评分模型URL必须是有效的HTTP(S)地址")
        if not self.api_key:
            raise RiskRelevanceConfigError(
                f"关联度评分模型密钥为空，可通过环境变量"
                f"{RISK_RELEVANCE_MODEL_API_KEY_ENV}覆盖默认配置"
            )
        if self.max_concurrency <= 0:
            raise RiskRelevanceConfigError("关联度评分并发数必须大于0")
        if self.total_timeout_seconds <= 0:
            raise RiskRelevanceConfigError("关联度评分单条总时限必须大于0")
        if self.max_retries < 0:
            raise RiskRelevanceConfigError("关联度评分重试次数不能小于0")
        if self.checkpoint_interval <= 0:
            raise RiskRelevanceConfigError("关联度评分检查点间隔必须大于0")

    @property
    def sanitized_api_url(self) -> str:
        """返回不含用户名、密码、查询参数和片段的日志地址。"""
        parsed = urlsplit(self.api_url)
        host = parsed.hostname or ""
        if parsed.port:
            host = f"{host}:{parsed.port}"
        return urlunsplit((parsed.scheme, host, parsed.path, "", ""))


__all__ = [
    "DEFAULT_RISK_DATA_ROOT",
    "DEFAULT_RISK_STATS_ROOT",
    "DEFAULT_RISK_TEMP_ROOT",
    "DEFAULT_RISK_TREE_PATH",
    "RISK_RELEVANCE_CHECKPOINT_INTERVAL",
    "RISK_RELEVANCE_MAX_CONCURRENCY",
    "RISK_RELEVANCE_MAX_RETRIES",
    "RISK_RELEVANCE_MODEL_API_KEY",
    "RISK_RELEVANCE_MODEL_API_KEY_ENV",
    "RISK_RELEVANCE_MODEL_API_URL",
    "RISK_RELEVANCE_MODEL_NAME",
    "RISK_RELEVANCE_TOTAL_TIMEOUT_SECONDS",
    "RiskRelevanceConfigError",
    "RiskRelevanceModelConfig",
]
