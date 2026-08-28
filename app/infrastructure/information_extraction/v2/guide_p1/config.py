"""Runtime configuration for point-form compliance guide extraction."""

from __future__ import annotations

import os
from dataclasses import dataclass


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class GuideP1Config:
    model_name: str
    api_key: str
    api_url: str
    timeout: int
    max_retries: int
    max_char_buffer: int
    batch_length: int
    max_workers: int
    max_concurrent: int
    semantic_block_chars: int
    strict_mode: bool
    supported_suffixes: tuple[str, ...]

    @classmethod
    def from_env(cls) -> "GuideP1Config":
        suffixes = tuple(
            item.strip().lower()
            for item in os.getenv("GUIDE_P1_SUPPORTED_SUFFIXES", ".txt,.md").split(",")
            if item.strip()
        )
        return cls(
            model_name=os.getenv("GUIDE_P1_MODEL", "qwen3-30b-a3b-instruct-2507"),
            api_key=os.getenv(
                "GUIDE_P1_MODEL_API_KEY",
                "gpustack_342609ce423be29a_4371426b285a91dc44fb4e8d72454847",
            ),
            api_url=os.getenv(
                "GUIDE_P1_MODEL_API_URL",
                "http://222.171.219.26:20001/v1/chat/completions",
            ),
            timeout=int(os.getenv("GUIDE_P1_TIMEOUT", "3000")),
            max_retries=int(os.getenv("GUIDE_P1_MAX_RETRIES", "5")),
            max_char_buffer=int(os.getenv("GUIDE_P1_MAX_CHAR_BUFFER", "7500")),
            batch_length=int(os.getenv("GUIDE_P1_BATCH_LENGTH", "5")),
            max_workers=int(os.getenv("GUIDE_P1_MAX_WORKERS", "3")),
            max_concurrent=max(1, int(os.getenv("GUIDE_P1_MAX_CONCURRENT", "5"))),
            semantic_block_chars=max(500, int(os.getenv("GUIDE_P1_SEMANTIC_BLOCK_CHARS", "6000"))),
            strict_mode=_env_bool("GUIDE_P1_STRICT_MODE", True),
            supported_suffixes=suffixes or (".txt", ".md"),
        )


DEFAULT_CONFIG = GuideP1Config.from_env()
