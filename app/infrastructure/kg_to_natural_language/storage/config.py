"""Explicit, serializable source configuration; contains no authentication."""

from dataclasses import asdict, dataclass, field
from pathlib import Path

from .records import KnowledgeStorageError, RULE_VERSION, digest

CONVERTERS = frozenset(("chinese_law", "rule", "national_standard", "guide", "english_law", "paragraph"))


@dataclass
class SourceConfig:
    converter: str
    category: str
    graph_label: str
    node_path: str | None = None
    edge_path: str | None = None
    graph_source: str = "local-export"
    source_type: str = "json"
    max_bytes: int = 536870912

    def validate(self):
        if self.converter not in CONVERTERS or not self.category.strip() or not self.graph_label.strip():
            raise KnowledgeStorageError("Explicit converter, category and graph label are required")
        if self.source_type not in {"json", "neo4j"}:
            raise KnowledgeStorageError("Source type must be json or neo4j")
        if self.source_type == "json" and (not self.node_path or not self.edge_path):
            raise KnowledgeStorageError("Both node and relationship JSON paths are required")
        if type(self.max_bytes) is not int or self.max_bytes <= 0:
            raise KnowledgeStorageError("max_bytes must be positive")
        if any(mark in self.graph_source for mark in ("://", "@")):
            raise KnowledgeStorageError("graph_source must be a credential-free dataset alias, not a URI")
        if any(ord(c) < 32 for c in self.graph_label):
            raise KnowledgeStorageError("Invalid graph label")


@dataclass
class StorageConfig:
    dataset_version: str
    sources: list[SourceConfig]
    output_root: str
    batch_size: int = 200
    tags: list[str] = field(default_factory=list)
    industry_mapping: dict = field(default_factory=dict)
    filename_industries: dict = field(default_factory=dict)
    formal_indicator_property: str = "三级指标映射列表"
    file_indicator_properties: dict = field(default_factory=dict)
    indicator_system_version: str | None = None
    rule_version: str = RULE_VERSION

    def validate(self):
        if (not isinstance(self.dataset_version, str) or not self.dataset_version.strip()
                or len(self.dataset_version) > 96):
            raise KnowledgeStorageError("An explicit dataset_version of at most 96 characters is required")
        if not self.sources or type(self.batch_size) is not int or not 1 <= self.batch_size <= 5000:
            raise KnowledgeStorageError("Sources and batch_size (1..5000) are required")
        if not all(isinstance(x, str) and x.strip() for x in self.tags):
            raise KnowledgeStorageError("Tags must be nonempty strings")
        if not self.formal_indicator_property or len(self.rule_version) > 96:
            raise KnowledgeStorageError("Invalid formal property or rule version")
        for name in ("industry_mapping", "filename_industries", "file_indicator_properties"):
            if not isinstance(getattr(self, name), dict):
                raise KnowledgeStorageError(name + " must be an object")
        if self.indicator_system_version is not None and not isinstance(self.indicator_system_version, str):
            raise KnowledgeStorageError("Indicator system version must be a string or None")
        if any(not isinstance(k, str) or not isinstance(v, str) or not v.strip()
               for k, v in self.file_indicator_properties.items()):
            raise KnowledgeStorageError("File-node indicator properties must map strings to property names")
        for source in self.sources:
            source.validate()
            if source.source_type == "json":
                root = Path(self.output_root).resolve()
                for name in (source.node_path, source.edge_path):
                    path = Path(name).resolve()
                    if root == path or root in path.parents:
                        raise KnowledgeStorageError("Input must not be under the storage output root")

    def semantic(self):
        value = asdict(self)
        for key in ("output_root", "batch_size"):
            value.pop(key)
        for source in value["sources"]:
            source.pop("node_path", None)
            source.pop("edge_path", None)
            source.pop("max_bytes", None)
        return value

    @property
    def classification_hash(self):
        return digest({k: self.semantic()[k] for k in (
            "industry_mapping", "filename_industries", "file_indicator_properties",
            "formal_indicator_property", "indicator_system_version", "tags")})
