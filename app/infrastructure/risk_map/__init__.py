"""Standalone Neo4j copy-first many-to-many indicator mapping extension."""

from .config import MappingConfig, ModelConfig
from .pipeline import execute_mapping

__all__ = ["MappingConfig", "ModelConfig", "execute_mapping"]
