"""Configuration for the v2 national-standard extraction flow."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class NationalStandardConfig:
    model_name: str = os.getenv("NATIONAL_STANDARD_GRAPH_MODEL", "qwen3-30b-a3b-instruct-2507")
    api_key: str = os.getenv(
        "NATIONAL_STANDARD_GRAPH_API_KEY",
        "gpustack_342609ce423be29a_4371426b285a91dc44fb4e8d72454847",
    )
    api_url: str = os.getenv(
        "NATIONAL_STANDARD_GRAPH_API_URL",
        "http://222.171.219.26:20001/v1/chat/completions",
    )
    timeout: int = int(os.getenv("NATIONAL_STANDARD_GRAPH_TIMEOUT", "3000"))
    max_char_buffer: int = int(os.getenv("NATIONAL_STANDARD_GRAPH_MAX_CHAR_BUFFER", "7500"))
    batch_length: int = int(os.getenv("NATIONAL_STANDARD_GRAPH_BATCH_LENGTH", "5"))
    max_workers: int = int(os.getenv("NATIONAL_STANDARD_GRAPH_MAX_WORKERS", "5"))
    max_retries: int = int(os.getenv("NATIONAL_STANDARD_GRAPH_MAX_RETRIES", "3"))
    include_resource_descriptions: bool = os.getenv(
        "NATIONAL_STANDARD_INCLUDE_RESOURCE_DESCRIPTIONS", "true"
    ).strip().lower() not in {"0", "false", "no", "off"}
