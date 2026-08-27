"""Structured runtime statistics for V2 Chinese regulation extraction."""

from __future__ import annotations

from dataclasses import dataclass


def _messages(value: str) -> list[str]:
    return [line.strip() for line in str(value or "").splitlines() if line.strip()]


@dataclass
class ResultStats:
    total_files: int = 0
    success_files: int = 0
    error: int = 0
    extraction_error: int = 0
    file_processing_error: int = 0
    error_msg: str = ""
    week_warning: int = 0
    week_warning_msg: str = ""
    strong_warning: int = 0
    strong_warning_msg: str = ""

    @property
    def weak_warning(self) -> int:
        """Correctly spelled alias retained alongside the V1 field name."""
        return self.week_warning

    def add_extraction_error(self, message: str) -> None:
        self.error += 1
        self.extraction_error += 1
        self.error_msg += f"{message}\n"

    def add_file_processing_error(self, message: str) -> None:
        self.error += 1
        self.file_processing_error += 1
        self.error_msg += f"{message}\n"

    def add_weak_warning(self, message: str) -> None:
        self.week_warning += 1
        self.week_warning_msg += f"{message}\n"

    def add_strong_warning(self, message: str) -> None:
        self.strong_warning += 1
        self.strong_warning_msg += f"{message}\n"

    def to_dict(self) -> dict:
        return {
            "total_files": self.total_files,
            "success_files": self.success_files,
            "error": self.error,
            "extraction_error": self.extraction_error,
            "file_processing_error": self.file_processing_error,
            "weak_warning": self.week_warning,
            "strong_warning": self.strong_warning,
            "error_messages": _messages(self.error_msg),
            "weak_warning_messages": _messages(self.week_warning_msg),
            "strong_warning_messages": _messages(self.strong_warning_msg),
        }

