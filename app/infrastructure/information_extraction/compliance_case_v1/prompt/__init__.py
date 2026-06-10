"""Prompt assets for compliance case v1 extraction."""

from app.infrastructure.information_extraction.compliance_case_v1.prompt.example import example_for_compliance_case
from app.infrastructure.information_extraction.compliance_case_v1.prompt.prompt import prompt_for_compliance_case
from app.infrastructure.information_extraction.compliance_case_v1.prompt.schema import (
    schema_for_compliance_case,
    schema_for_compliance_case_runtime,
)

__all__ = [
    "example_for_compliance_case",
    "prompt_for_compliance_case",
    "schema_for_compliance_case",
    "schema_for_compliance_case_runtime",
]
