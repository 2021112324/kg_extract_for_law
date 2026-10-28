from dataclasses import dataclass, field
import os
from ..rule.config import RuleConfig


@dataclass(frozen=True)
class GuideP2Config(RuleConfig):
    model_name: str = field(default_factory=lambda: os.getenv("GUIDE_P2_MODEL", ""))
    api_url: str = field(default_factory=lambda: os.getenv("GUIDE_P2_API_URL", ""))
    api_key: str = field(default_factory=lambda: os.getenv("GUIDE_P2_API_KEY", os.getenv("LITELLM_API_KEY", "")), repr=False)
    batch_size: int = 12

    def __post_init__(self):
        super().__post_init__()
        for name in ("max_attempts", "max_concurrent", "batch_size", "block_chars", "context_chars"):
            if type(getattr(self, name)) is not int or getattr(self, name) < 1:
                raise ValueError(name + " must be a positive integer")
