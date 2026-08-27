"""Configuration for the V2 Chinese regulation extractor."""

from __future__ import annotations

import os
from dataclasses import dataclass


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class ZhLawConfig:
    model_name: str
    api_key: str
    api_url: str
    max_char_buffer: int
    batch_length: int
    max_workers: int
    timeout: int
    max_retries: int
    max_concurrent: int
    lenient_mode: bool

    @classmethod
    def from_env(cls) -> "ZhLawConfig":
        return cls(
            model_name=os.getenv("ZH_LAW_MODEL", "qwen3-30b-a3b-instruct-2507"),
            api_key=os.getenv(
                "ZH_LAW_MODEL_API_KEY",
                "gpustack_342609ce423be29a_4371426b285a91dc44fb4e8d72454847",
            ),
            api_url=os.getenv(
                "ZH_LAW_MODEL_API_URL",
                "http://222.171.219.26:20001/v1/chat/completions",
            ),
            max_char_buffer=int(os.getenv("ZH_LAW_MAX_CHAR_BUFFER", "7500")),
            batch_length=int(os.getenv("ZH_LAW_BATCH_LENGTH", "5")),
            max_workers=int(os.getenv("ZH_LAW_MAX_WORKERS", "3")),
            timeout=int(os.getenv("ZH_LAW_TIMEOUT", "3000")),
            max_retries=int(os.getenv("ZH_LAW_MAX_RETRIES", "5")),
            max_concurrent=int(os.getenv("ZH_LAW_MAX_CONCURRENT", "50")),
            lenient_mode=_env_bool("ZH_LAW_LENIENT_MODE", False),
        )


DEFAULT_CONFIG = ZhLawConfig.from_env()

