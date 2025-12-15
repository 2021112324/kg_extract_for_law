"""
Object Storage Abstract Base Classes

Defines the interface for object storage implementations to ensure
consistency across different cloud providers (MinIO, AWS S3, Alibaba OSS, etc.).
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, BinaryIO, Union
from dataclasses import dataclass
from datetime import timedelta
import io


@dataclass
class StorageConfig:
    """Configuration for object storage services"""
    endpoint: str
    access_key: str
    secret_key: str
    secure: bool = True
    region: Optional[str] = None
    
    # Bucket configurations
    raw_bucket: str = "raw-files"
    processed_bucket: str = "processed-files"
    image_bucket: str = "image-files"


@dataclass
class FileMetadata:
    """File metadata information"""
    object_name: str
    size: int
    content_type: Optional[str] = None
    last_modified: Optional[str] = None
    etag: Optional[str] = None


class ObjectStorageInterface(ABC):
    """
    Abstract interface for object storage operations
    
    This interface defines the contract that all object storage
    implementations must follow, enabling easy switching between
    different providers (MinIO, AWS S3, Alibaba OSS, etc.).
    """
    
    def __init__(self, config: StorageConfig):
        self.config = config
    
    @abstractmethod
    def ensure_bucket_exists(self, bucket_name: str) -> bool:
        """
        Ensure bucket exists, create if it doesn't
        
        Args:
            bucket_name: Name of the bucket
            
        Returns:
            True if successful, False otherwise
        """
        pass
    
    @abstractmethod
    def upload_file_object(
        self, 
        file_data: Union[bytes, BinaryIO], 
        bucket_name: str, 
        object_name: str, 
        content_type: Optional[str] = None
    ) -> bool:
        """
        Upload file object (bytes or stream) to storage
        
        Args:
            file_data: File content as bytes or binary stream
            bucket_name: Target bucket name
            object_name: Object name in storage
            content_type: MIME content type
            
        Returns:
            True if successful, False otherwise
        """
        pass
    
    @abstractmethod
    def upload_file_stream(
        self, 
        file_stream: BinaryIO, 
        bucket_name: str, 
        object_name: str, 
        content_type: Optional[str] = None,
        file_size: Optional[int] = None
    ) -> bool:
        """
        Upload file stream to storage
        
        Args:
            file_stream: File stream to upload
            bucket_name: Target bucket name
            object_name: Object name in storage
            content_type: MIME content type
            file_size: Size of file in bytes
            
        Returns:
            True if successful, False otherwise
        """
        pass
    
    @abstractmethod
    def upload_file_path(
        self, 
        local_path: str, 
        bucket_name: str, 
        object_name: str
    ) -> bool:
        """
        Upload local file to storage
        
        Args:
            local_path: Path to local file
            bucket_name: Target bucket name
            object_name: Object name in storage
            
        Returns:
            True if successful, False otherwise
        """
        pass
    
    @abstractmethod
    def download_file(
        self, 
        bucket_name: str, 
        object_name: str, 
        local_path: str
    ) -> bool:
        """
        Download file from storage to local path
        
        Args:
            bucket_name: Source bucket name
            object_name: Object name in storage
            local_path: Local destination path
            
        Returns:
            True if successful, False otherwise
        """
        pass
    
    @abstractmethod
    def get_file_stream(self, bucket_name: str, object_name: str) -> Optional[BinaryIO]:
        """
        Get file as a stream
        
        Args:
            bucket_name: Source bucket name
            object_name: Object name in storage
            
        Returns:
            File stream or None if failed
        """
        pass
    
    @abstractmethod
    def get_file_url(
        self, 
        bucket_name: str, 
        object_name: str, 
        expires: Union[int, timedelta] = 3600
    ) -> Optional[str]:
        """
        Generate presigned URL for file access
        
        Args:
            bucket_name: Source bucket name
            object_name: Object name in storage
            expires: Expiration time in seconds or timedelta
            
        Returns:
            Presigned URL or None if failed
        """
        pass
    
    @abstractmethod
    def delete_file(self, bucket_name: str, object_name: str) -> bool:
        """
        Delete file from storage
        
        Args:
            bucket_name: Source bucket name
            object_name: Object name in storage
            
        Returns:
            True if successful, False otherwise
        """
        pass
    
    @abstractmethod
    def list_files(
        self, 
        bucket_name: str, 
        prefix: Optional[str] = None
    ) -> List[FileMetadata]:
        """
        List files in bucket with optional prefix filter
        
        Args:
            bucket_name: Source bucket name
            prefix: Optional prefix filter
            
        Returns:
            List of file metadata
        """
        pass
    
    @abstractmethod
    def file_exists(self, bucket_name: str, object_name: str) -> bool:
        """
        Check if file exists in storage
        
        Args:
            bucket_name: Source bucket name
            object_name: Object name in storage
            
        Returns:
            True if file exists, False otherwise
        """
        pass
    
    @abstractmethod 
    def get_file_metadata(self, bucket_name: str, object_name: str) -> Optional[FileMetadata]:
        """
        Get file metadata
        
        Args:
            bucket_name: Source bucket name
            object_name: Object name in storage
            
        Returns:
            File metadata or None if not found
        """
        pass
    
    @abstractmethod
    def initialize(self) -> bool:
        """
        Initialize storage service (create required buckets, etc.)
        
        Returns:
            True if successful, False otherwise
        """
        pass