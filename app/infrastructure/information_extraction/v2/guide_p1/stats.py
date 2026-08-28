"""Structured runtime statistics for guide_p1."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class ResultStats:
    total_files: int = 0
    success_files: int = 0
    incomplete_files: int = 0
    error: int = 0
    extraction_error: int = 0
    storage_error: int = 0
    file_processing_error: int = 0
    weak_warning: int = 0
    strong_warning: int = 0
    structure_node_count: int = 0
    planned_block_count: int = 0
    llm_success_count: int = 0
    llm_empty_count: int = 0
    llm_failed_count: int = 0
    knowledge_unit_count: int = 0
    node_count: int = 0
    edge_count: int = 0
    skipped_attachment_count: int = 0
    error_messages: list[str] = field(default_factory=list)
    weak_warning_messages: list[str] = field(default_factory=list)
    strong_warning_messages: list[str] = field(default_factory=list)

    def add_extraction_error(self, message: str) -> None:
        self.error += 1
        self.extraction_error += 1
        self.llm_failed_count += 1
        self.error_messages.append(message)

    def add_file_processing_error(self, message: str) -> None:
        self.error += 1
        self.file_processing_error += 1
        self.error_messages.append(message)

    def add_storage_error(self, message: str) -> None:
        self.error += 1
        self.storage_error += 1
        self.error_messages.append(message)

    def add_weak_warning(self, message: str) -> None:
        self.weak_warning += 1
        self.weak_warning_messages.append(message)

    def add_strong_warning(self, message: str) -> None:
        self.strong_warning += 1
        self.strong_warning_messages.append(message)

    def merge(self, other: "ResultStats") -> None:
        for name in (
            "total_files",
            "success_files",
            "incomplete_files",
            "error",
            "extraction_error",
            "storage_error",
            "file_processing_error",
            "weak_warning",
            "strong_warning",
            "structure_node_count",
            "planned_block_count",
            "llm_success_count",
            "llm_empty_count",
            "llm_failed_count",
            "knowledge_unit_count",
            "node_count",
            "edge_count",
            "skipped_attachment_count",
        ):
            setattr(self, name, getattr(self, name) + getattr(other, name))
        self.error_messages.extend(other.error_messages)
        self.weak_warning_messages.extend(other.weak_warning_messages)
        self.strong_warning_messages.extend(other.strong_warning_messages)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
