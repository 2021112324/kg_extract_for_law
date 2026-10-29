"""Explicit configuration; parse mode needs neither credentials nor services."""
from dataclasses import dataclass, field
import math
import os


@dataclass(frozen=True)
class ParagraphConfig:
    model_name: str = field(default_factory=lambda: os.getenv("PARAGRAPH_MODEL", ""))
    api_url: str = field(default_factory=lambda: os.getenv("PARAGRAPH_API_URL", ""))
    api_key: str = field(default_factory=lambda: os.getenv("PARAGRAPH_API_KEY", os.getenv("LITELLM_API_KEY", "")), repr=False)
    timeout: float = 180
    max_attempts: int = 5
    max_concurrent: int = 3
    block_chars: int = 6000
    context_chars: int = 12000
    request_chars: int = 40000
    output_reserve_chars: int = 8192
    max_output_tokens: int = 8192
    heartbeat_seconds: float = 60
    retry_delay: float = 1
    paragraph_mode: str = "blank_lines"
    publication: str = "normative"  # reference is a separate, explicit view.

    def __post_init__(self):
        for key in ("max_attempts", "max_concurrent", "block_chars", "context_chars",
                    "request_chars", "output_reserve_chars", "max_output_tokens"):
            if type(getattr(self, key)) is not int or getattr(self, key) <= 0:
                raise ValueError(key + " must be a positive integer")
        if self.max_attempts > 5:
            raise ValueError("At most five total attempts, including the first")
        for key in ("timeout", "heartbeat_seconds", "retry_delay"):
            value = getattr(self, key)
            if isinstance(value, bool) or not math.isfinite(value) or value < 0 or key != "retry_delay" and value == 0:
                raise ValueError(key + " must be finite and positive (delay may be zero)")
        if self.paragraph_mode not in {"blank_lines", "one_line"}:
            raise ValueError("paragraph_mode must be blank_lines or one_line")
        if self.publication not in {"normative", "reference"}:
            raise ValueError("publication must be normative or reference")
