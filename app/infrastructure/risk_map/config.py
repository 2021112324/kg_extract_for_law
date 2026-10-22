"""Copy-first mapping contracts. No JSON graph input or ranking settings."""

from dataclasses import dataclass, field
import math
from pathlib import Path

from .common.io import content_hash
from .common.json_output import POLICY, dependency_version
from .preparation.config import DEFAULT_TREE_PATH, REGIONS

PROTOCOL = "copy-first-many-to-many-v3-audit-fixes"
REVIEW_PROTOCOL = "removal-reasons-v1"
CANDIDATES = "三级指标映射候选列表"
FINAL = "三级指标映射列表"
LEGACY = ("三级指标映射Top5", "三级指标映射版本", "三级指标映射状态")
MANAGED = (CANDIDATES, FINAL, *LEGACY)


@dataclass(frozen=True)
class ModelConfig:
    model: str = ""
    api_url: str = ""
    api_key: str = field(default="", repr=False, compare=False)
    request_timeout: float = 300
    max_attempts: int = 5
    retry_delay: float = 1
    max_input_chars: int = 60000
    max_tokens: int = 4096
    review_max_group_candidates: int = 8

    def validate(self):
        if type(self.max_attempts) is not int or not 1 <= self.max_attempts <= 5:
            raise ValueError("max_attempts必须为1到5次，包含首次")
        if (not math.isfinite(self.request_timeout) or self.request_timeout <= 0
                or not math.isfinite(self.retry_delay) or self.retry_delay < 0
                or type(self.max_input_chars) is not int or self.max_input_chars < 128):
            raise ValueError("模型超时、等待或输入预算不合法")
        if type(self.max_tokens) is not int or self.max_tokens <= 0:
            raise ValueError("max_tokens必须为正数")
        if type(self.review_max_group_candidates) is not int or self.review_max_group_candidates <= 0:
            raise ValueError("review_max_group_candidates必须为正整数")

    @property
    def semantic(self):
        return {"model": self.model, "endpoint": self.api_url, "max_input_chars": self.max_input_chars,
                "max_tokens": self.max_tokens, "protocol": PROTOCOL,
                "review_protocol": REVIEW_PROTOCOL, "review_max_group_candidates": self.review_max_group_candidates,
                "json_policy": POLICY, "json_repair_version": dependency_version(),
                "filename_policy": "exact-family-cn-prefix-v1", "empty_basis_policy": "skip-v1"}


@dataclass(frozen=True)
class MappingConfig:
    source_label: str
    target_label: str
    tree_path: Path = DEFAULT_TREE_PATH
    output_root: Path = Path(__file__).parent / "output" / "copy_first"
    region: str = "中国"
    batch_size: int = 20
    write_enabled: bool = False
    exclusive_target: bool = False
    model: ModelConfig = field(default_factory=ModelConfig)
    read_options: dict = field(default_factory=lambda: {
        "fetch_size": 1000, "max_nodes": 50000, "max_relationships": 200000,
        "max_bytes": 536870912, "timeout": 600})

    def validate(self, *, writing=True):
        if (not isinstance(self.source_label, str) or not isinstance(self.target_label, str)
                or not self.source_label.strip() or not self.target_label.strip()
                or self.source_label == self.target_label
                or self.target_label.startswith("_RiskMap")):
            raise ValueError("必须配置不同的源/目标业务标签")
        if self.region not in REGIONS or self.batch_size != 20:
            raise ValueError("地区不合法；法条批次固定为20")
        if writing and not (self.write_enabled and self.exclusive_target):
            raise ValueError("写入需显式确认write_enabled和exclusive_target")
        self.model.validate()

    @property
    def semantic_hash(self):
        return content_hash({"source": self.source_label, "target": self.target_label,
                             "region": self.region, "protocol": PROTOCOL,
                             "model": self.model.semantic})
