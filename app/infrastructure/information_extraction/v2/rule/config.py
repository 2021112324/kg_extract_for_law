"""Configuration without embedded credentials or implicit remote connections."""
from dataclasses import dataclass, field
import os


@dataclass(frozen=True)
class RuleConfig:
    model_name: str = field(default_factory=lambda: os.getenv("RULE_MODEL", ""))
    api_key: str = field(default_factory=lambda: os.getenv("RULE_API_KEY", os.getenv("LITELLM_API_KEY", "")), repr=False)
    api_url: str = field(default_factory=lambda: os.getenv("RULE_API_URL", ""))
    timeout: float = 180
    max_attempts: int = 5
    max_concurrent: int = 3
    block_chars: int = 6000
    context_chars: int = 12000
    heartbeat_seconds: float = 60
    retry_delay: float = 1

    def __post_init__(self):
        if not 1 <= self.max_attempts <= 5:
            raise ValueError("max_attempts must be between 1 and 5, including the first attempt")
        if min(self.timeout, self.max_concurrent, self.block_chars, self.context_chars, self.heartbeat_seconds) <= 0:
            raise ValueError("Timeout, limits and heartbeat must be positive")
