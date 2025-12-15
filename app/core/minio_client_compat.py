"""
MinIO Client Compatibility Layer
DEPRECATED: Use app.infrastructure.storage.object_storage instead

This module provides backward compatibility for existing code using minio_client.py
All functions are now wrappers around the new ObjectStorageInterface.
"""

import warnings
from typing import Optional, BinaryIO, Union, List, Dict, Any
from datetime import timedelta
from fastapi.responses import StreamingResponse

from app.infrastructure.storage.object_storage import StorageFactory, ObjectStorageInterface
from app.core.config import settings

# Emit deprecation warning
warnings.warn(
    "app.core.minio_client is deprecated. Use app.infrastructure.storage.object_storage instead.",
    DeprecationWarning,
    stacklevel=2
)

# Initialize default storage instance
_storage_instance: Optional[ObjectStorageInterface] = None

def _get_storage() -> ObjectStorageInterface:
    """Get default storage instance"""
    global _storage_instance
    if _storage_instance is None:
        _storage_instance = StorageFactory.get_default_storage()
    return _storage_instance

# Legacy bucket constants - maintained for compatibility
RAW_BUCKET = getattr(settings, 'RAW_BUCKET', 'raw-files')
PROCESSED_BUCKET = getattr(settings, 'PROCESSED_BUCKET', 'processed-files') 
IMAGE_BUCKET = getattr(settings, 'IMAGE_BUCKET', 'image-files')
MINIO_ENDPOINT = getattr(settings, 'MINIO_ENDPOINT', 'localhost:9000')

# Expose MinIO client for direct access (if needed)
client = _get_storage().client if hasattr(_get_storage(), 'client') else None

def ensure_bucket_exists(bucket_name: str) -> bool:
    """DEPRECATED: Ensure bucket exists, create if it doesn't"""
    return _get_storage().ensure_bucket_exists(bucket_name)

def upload_file_object(file_data: Union[bytes, BinaryIO], bucket_name: str, object_name: str, content_type: Optional[str] = None) -> bool:
    """DEPRECATED: Upload file object to storage"""
    return _get_storage().upload_file_object(file_data, bucket_name, object_name, content_type)

def upload_file_stream(file_stream: BinaryIO, bucket_name: str, object_name: str, content_type: Optional[str] = None, file_size: Optional[int] = None) -> bool:
    """DEPRECATED: Upload file stream to storage"""  
    return _get_storage().upload_file_stream(file_stream, bucket_name, object_name, content_type, file_size)

def upload_file(local_path: str, bucket_name: str, object_name: str) -> bool:
    """DEPRECATED: Upload local file to storage"""
    return _get_storage().upload_file_path(local_path, bucket_name, object_name)

def upload_file_minIO(local_path: str, bucket_name: str, object_name: str) -> bool:
    """DEPRECATED: Upload local file to storage (alternative name)"""
    return upload_file(local_path, bucket_name, object_name)

async def upload_file_async(local_path: str, bucket_name: str, object_name: str) -> bool:
    """DEPRECATED: Async upload local file to storage"""
    import asyncio
    return await asyncio.to_thread(upload_file, local_path, bucket_name, object_name)

def download_file(bucket_name: str, object_name: str, local_path: str) -> bool:
    """DEPRECATED: Download file from storage to local path"""
    return _get_storage().download_file(bucket_name, object_name, local_path)

def get_file_stream(bucket_name: str, object_name: str) -> Optional[BinaryIO]:
    """DEPRECATED: Get file as a stream"""
    return _get_storage().get_file_stream(bucket_name, object_name)

def get_file_url(bucket_name: str, object_name: str, expires: Union[int, timedelta] = 3600) -> Optional[str]:
    """DEPRECATED: Generate presigned URL for file access"""
    return _get_storage().get_file_url(bucket_name, object_name, expires)

def delete_file(bucket_name: str, object_name: str) -> bool:
    """DEPRECATED: Delete file from storage"""
    return _get_storage().delete_file(bucket_name, object_name)

def get_streaming_response(bucket_name: str, object_name: str, media_type: Optional[str] = None, filename: Optional[str] = None) -> Optional[StreamingResponse]:
    """DEPRECATED: Get file as streaming response"""
    try:
        response = _get_storage().get_file_stream(bucket_name, object_name)
        if response is None:
            return None
            
        headers = {}
        if filename:
            headers["Content-Disposition"] = f'attachment; filename="{filename}"'
        
        return StreamingResponse(
            response.stream(32*1024) if hasattr(response, 'stream') else response,
            media_type=media_type or 'application/octet-stream',
            headers=headers
        )
    except Exception:
        return None

def list_files(bucket_name: str, prefix: Optional[str] = None) -> List[Dict[str, Any]]:
    """DEPRECATED: List files in storage bucket"""
    files_metadata = _get_storage().list_files(bucket_name, prefix)
    
    # Convert to legacy format
    return [
        {
            "name": f.object_name,
            "size": f.size,
            "last_modified": f.last_modified,
            "content_type": f.content_type,
            "etag": f.etag
        }
        for f in files_metadata
    ]

def initialize_minio() -> None:
    """DEPRECATED: Initialize storage service"""
    storage = _get_storage()
    result = storage.initialize()
    if not result:
        raise Exception("Failed to initialize storage service")
    
    # Log legacy message for compatibility
    import logging
    logger = logging.getLogger(__name__)
    logger.info(f"✅ Storage service initialized (endpoint: {storage.config.endpoint})")