"""Authoritative mapping-tree records, independent of model protocols."""

from dataclasses import dataclass, field


class RiskMapInputError(ValueError):
    pass


@dataclass
class IndicatorContext:
    number: str
    name: str
    description: str
    risk_type: str
    path: tuple[tuple[str, str], ...]
    attachment_ids: list[int] = field(default_factory=list)


@dataclass
class FileAttachment:
    indicator_number: str
    risk_type: str
    region: str
    mapped_name: str
    basis_name: str
    original_text: str
    found_filename: str
    knowledge: str
    tree_path: tuple[str, ...]
    identity: tuple[str, str]
    provenance: list[dict] = field(default_factory=list)


@dataclass
class EvidencePool:
    identity: tuple[str, str]
    risk_type: str
    content_hash: str
    text: str
    attachment_ids: list[int] = field(default_factory=list)
    indicator_numbers: set[str] = field(default_factory=set)


@dataclass
class TreeIndex:
    indicators: dict[str, IndicatorContext] = field(default_factory=dict)
    attachments: list[FileAttachment] = field(default_factory=list)
    names: dict[tuple[str, str], set[tuple[str, str]]] = field(default_factory=dict)
    families: dict[tuple[str, str], set[tuple[str, str]]] = field(default_factory=dict)
    files: dict[tuple[str, str], list[int]] = field(default_factory=dict)
    pools: dict[tuple[str, str, str, str], EvidencePool] = field(default_factory=dict)
