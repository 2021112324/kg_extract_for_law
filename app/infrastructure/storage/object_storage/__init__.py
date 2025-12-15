"""
Object Storage Infrastructure Module

Provides abstracted object storage interfaces supporting multiple cloud providers.
"""

from .base import ObjectStorageInterface, StorageConfig, FileMetadata
from .minio_adapter import MinIOAdapter
from .factory import StorageFactory

__all__ = [
    'ObjectStorageInterface',
    'StorageConfig',
    'FileMetadata',
    'MinIOAdapter',
    'StorageFactory'
]