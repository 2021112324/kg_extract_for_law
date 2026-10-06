"""Runtime statistics for the v2 national-standard extraction flow."""

from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass
class ResultStats:
    error: int = 0
    error_msg: str = ""
    extraction_errors: int = 0
    file_errors: int = 0
    storage_errors: int = 0
    week_warning: int = 0
    week_warning_msg: str = ""
    strong_warning: int = 0
    strong_warning_msg: str = ""
    skipped_image_blocks: int = 0
    skipped_image_texts: int = 0
    table_contexts: int = 0
    flowchart_contexts: int = 0
    valid_descriptions: int = 0
    invalid_descriptions: int = 0
    filtered_nodes: int = 0
    filtered_properties: int = 0
    filtered_relations: int = 0

    def to_dict(self) -> dict[str, int | str]:
        return asdict(self)

