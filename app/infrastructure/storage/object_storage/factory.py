"""
Object Storage Factory

Creates appropriate storage adapters based on configuration.
"""

from typing import Optional
from app.core.config import settings
from .base import ObjectStorageInterface, StorageConfig
from .minio_adapter import MinIOAdapter


class StorageFactory:
    """Factory for creating object storage instances"""
    
    @staticmethod
    def create_storage(
        storage_type: str = "minio",
        config: Optional[StorageConfig] = None
    ) -> ObjectStorageInterface:
        """
        Create object storage instance based on type
        
        Args:
            storage_type: Type of storage ("minio", "s3", "oss", etc.)
            config: Optional custom configuration
            
        Returns:
            ObjectStorageInterface implementation
        """
        if config is None:
            config = StorageFactory._get_default_config(storage_type)
        
        if storage_type.lower() == "minio":
            return MinIOAdapter(config)
        # Future implementations:
        # elif storage_type.lower() == "s3":
        #     return S3Adapter(config)
        # elif storage_type.lower() == "oss":
        #     return OSSAdapter(config)
        else:
            raise ValueError(f"Unsupported storage type: {storage_type}")
    
    @staticmethod
    def _get_default_config(storage_type: str) -> StorageConfig:
        """Get default configuration from settings"""
        if storage_type.lower() == "minio":
            return StorageConfig(
                endpoint=getattr(settings, 'MINIO_ENDPOINT', 'localhost:9000'),
                access_key=getattr(settings, 'MINIO_ACCESS_KEY', 'xkk'),
                secret_key=getattr(settings, 'MINIO_SECRET_KEY', 'xkkxkkxkk'),
                secure=getattr(settings, 'MINIO_SECURE', False),
                raw_bucket=getattr(settings, 'RAW_BUCKET', 'raw-files'),
                processed_bucket=getattr(settings, 'PROCESSED_BUCKET', 'processed-files'),
                image_bucket=getattr(settings, 'IMAGE_BUCKET', 'image-files')
            )
        else:
            raise ValueError(f"No default configuration for storage type: {storage_type}")
    
    @staticmethod
    def get_default_storage() -> ObjectStorageInterface:
        """Get default storage instance (MinIO)"""
        return StorageFactory.create_storage("minio")