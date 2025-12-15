"""
MinIO Object Storage Adapter

Implements ObjectStorageInterface for MinIO object storage.
This adapter wraps the MinIO client to provide a consistent interface.
"""

import logging
import mimetypes
import io
from typing import Dict, List, Optional, BinaryIO, Union
from datetime import datetime, timedelta

from minio import Minio
from minio.error import S3Error

from .base import ObjectStorageInterface, StorageConfig, FileMetadata

logger = logging.getLogger(__name__)


class MinIOAdapter(ObjectStorageInterface):
    """
    MinIO implementation of ObjectStorageInterface
    
    Provides object storage capabilities using MinIO server.
    """
    
    def __init__(self, config: StorageConfig):
        super().__init__(config)
        self.client = Minio(
            endpoint=config.endpoint,
            access_key=config.access_key,
            secret_key=config.secret_key,
            secure=config.secure
        )
    
    def ensure_bucket_exists(self, bucket_name: str) -> bool:
        """Ensure bucket exists, create if it doesn't"""
        try:
            if not self.client.bucket_exists(bucket_name):
                self.client.make_bucket(bucket_name)
                logger.info(f"✅ 创建存储桶 '{bucket_name}' 成功")
            return True
        except S3Error as e:
            logger.error(f"❌ 存储桶操作失败: {e}")
            return False
    
    def upload_file_object(
        self, 
        file_data: Union[bytes, BinaryIO], 
        bucket_name: str, 
        object_name: str, 
        content_type: Optional[str] = None
    ) -> bool:
        """Upload file object (bytes or stream) to MinIO"""
        try:
            self.ensure_bucket_exists(bucket_name)
            
            # Handle different input types
            if isinstance(file_data, bytes):
                data_stream = io.BytesIO(file_data)
                file_size = len(file_data)
            else:
                data_stream = file_data
                # Try to get size if available
                try:
                    current_pos = data_stream.tell()
                    data_stream.seek(0, 2)  # Seek to end
                    file_size = data_stream.tell()
                    data_stream.seek(current_pos)  # Restore position
                except (OSError, io.UnsupportedOperation):
                    # If seeking is not supported, we'll need to read the entire content
                    content = data_stream.read()
                    data_stream = io.BytesIO(content)
                    file_size = len(content)
            
            logger.debug(f"正在上传对象: {bucket_name}/{object_name} (大小: {file_size}字节)")
            
            self.client.put_object(
                bucket_name=bucket_name,
                object_name=object_name,
                data=data_stream,
                length=file_size,
                content_type=content_type
            )
            
            logger.info(f"✅ 文件对象上传成功: {bucket_name}/{object_name}")
            return True
            
        except S3Error as e:
            logger.error(f"❌ 文件上传失败 {bucket_name}/{object_name}: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ 上传过程中发生错误: {str(e)}")
            return False
    
    def upload_file_stream(
        self, 
        file_stream: BinaryIO, 
        bucket_name: str, 
        object_name: str, 
        content_type: Optional[str] = None,
        file_size: Optional[int] = None
    ) -> bool:
        """Upload file stream to MinIO"""
        try:
            self.ensure_bucket_exists(bucket_name)
            
            # Handle bytes input
            if isinstance(file_stream, bytes):
                file_stream = io.BytesIO(file_stream)
                if file_size is None:
                    file_size = len(file_stream.getvalue())
            
            # Try to determine file size if not provided
            if file_size is None:
                try:
                    current_pos = file_stream.tell()
                    file_stream.seek(0, 2)
                    file_size = file_stream.tell()
                    file_stream.seek(current_pos)
                except (OSError, io.UnsupportedOperation):
                    logger.warning("无法确定文件流大小，将读取全部内容")
                    content = file_stream.read()
                    file_stream = io.BytesIO(content)
                    file_size = len(content)
            
            logger.debug(f"正在上传文件流: {bucket_name}/{object_name} (大小: {file_size}字节)")
            
            self.client.put_object(
                bucket_name=bucket_name,
                object_name=object_name,
                data=file_stream,
                length=file_size,
                content_type=content_type
            )
            
            logger.info(f"✅ 文件流上传成功: {bucket_name}/{object_name}")
            return True
            
        except S3Error as e:
            logger.error(f"❌ 文件流上传失败 {bucket_name}/{object_name}: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ 上传过程中发生错误: {str(e)}")
            return False
    
    def upload_file_path(
        self, 
        local_path: str, 
        bucket_name: str, 
        object_name: str
    ) -> bool:
        """Upload local file to MinIO"""
        try:
            self.ensure_bucket_exists(bucket_name)
            logger.debug(f"正在上传本地文件: {local_path} → {bucket_name}/{object_name}")
            
            # Determine content type
            content_type = None
            if '.' in local_path:
                content_type = mimetypes.guess_type(local_path)[0]
            
            self.client.fput_object(
                bucket_name=bucket_name,
                object_name=object_name,
                file_path=local_path,
                content_type=content_type
            )
            
            logger.info(f"✅ 文件上传成功: {local_path} → {bucket_name}/{object_name}")
            return True
            
        except S3Error as e:
            logger.error(f"❌ 文件上传失败 {local_path} → {bucket_name}/{object_name}: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ 上传文件时发生未知错误: {str(e)}")
            return False
    
    def download_file(
        self, 
        bucket_name: str, 
        object_name: str, 
        local_path: str
    ) -> bool:
        """Download file from MinIO to local path"""
        try:
            logger.debug(f"正在下载文件: {bucket_name}/{object_name} → {local_path}")
            
            self.client.fget_object(
                bucket_name=bucket_name,
                object_name=object_name,
                file_path=local_path
            )
            
            logger.info(f"✅ 文件下载成功: {bucket_name}/{object_name} → {local_path}")
            return True
            
        except S3Error as e:
            logger.error(f"❌ 文件下载失败: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ 下载过程中发生错误: {str(e)}")
            return False
    
    def get_file_stream(self, bucket_name: str, object_name: str) -> Optional[BinaryIO]:
        """Get file as a stream"""
        try:
            logger.debug(f"正在获取文件流: {bucket_name}/{object_name}")
            response = self.client.get_object(
                bucket_name=bucket_name,
                object_name=object_name
            )
            return response
        except S3Error as e:
            logger.error(f"❌ 获取文件失败: {e}")
            return None
        except Exception as e:
            logger.error(f"❌ 获取文件流时发生错误: {str(e)}")
            return None
    
    def get_file_url(
        self, 
        bucket_name: str, 
        object_name: str, 
        expires: Union[int, timedelta] = 3600
    ) -> Optional[str]:
        """Generate presigned URL for file access"""
        try:
            # Convert int to timedelta if necessary
            if isinstance(expires, int):
                expires_delta = timedelta(seconds=expires)
            else:
                expires_delta = expires
            
            url = self.client.presigned_get_object(
                bucket_name=bucket_name,
                object_name=object_name,
                expires=expires_delta
            )
            
            logger.debug(f"生成临时URL: {bucket_name}/{object_name} (过期时间: {expires_delta})")
            return url
            
        except S3Error as e:
            logger.error(f"❌ 生成URL失败: {e}")
            return None
        except Exception as e:
            logger.error(f"❌ 生成URL时发生错误: {str(e)}")
            return None
    
    def delete_file(self, bucket_name: str, object_name: str) -> bool:
        """Delete file from MinIO"""
        try:
            logger.debug(f"正在删除文件: {bucket_name}/{object_name}")
            
            self.client.remove_object(
                bucket_name=bucket_name,
                object_name=object_name
            )
            
            logger.info(f"✅ 文件删除成功: {bucket_name}/{object_name}")
            return True
            
        except S3Error as e:
            logger.error(f"❌ 删除文件失败: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ 删除过程中发生错误: {str(e)}")
            return False
    
    def list_files(
        self, 
        bucket_name: str, 
        prefix: Optional[str] = None
    ) -> List[FileMetadata]:
        """List files in bucket with optional prefix filter"""
        try:
            objects = self.client.list_objects(
                bucket_name, 
                prefix=prefix, 
                recursive=True
            )
            
            files = []
            for obj in objects:
                files.append(FileMetadata(
                    object_name=obj.object_name,
                    size=obj.size,
                    content_type=getattr(obj, 'content_type', None),
                    last_modified=obj.last_modified.isoformat() if obj.last_modified else None,
                    etag=getattr(obj, 'etag', None)
                ))
            
            logger.debug(f"列出文件成功: {bucket_name}/{prefix or ''} (共{len(files)}个文件)")
            return files
            
        except S3Error as e:
            logger.error(f"❌ 列出文件失败: {e}")
            return []
        except Exception as e:
            logger.error(f"❌ 列出文件时发生错误: {str(e)}")
            return []
    
    def file_exists(self, bucket_name: str, object_name: str) -> bool:
        """Check if file exists in MinIO"""
        try:
            self.client.stat_object(bucket_name, object_name)
            return True
        except S3Error:
            return False
        except Exception as e:
            logger.error(f"❌ 检查文件存在性时发生错误: {str(e)}")
            return False
    
    def get_file_metadata(self, bucket_name: str, object_name: str) -> Optional[FileMetadata]:
        """Get file metadata"""
        try:
            stat = self.client.stat_object(bucket_name, object_name)
            
            return FileMetadata(
                object_name=object_name,
                size=stat.size,
                content_type=stat.content_type,
                last_modified=stat.last_modified.isoformat() if stat.last_modified else None,
                etag=stat.etag
            )
            
        except S3Error as e:
            logger.error(f"❌ 获取文件元数据失败: {e}")
            return None
        except Exception as e:
            logger.error(f"❌ 获取元数据时发生错误: {str(e)}")
            return None
    
    def initialize(self) -> bool:
        """Initialize MinIO service (create required buckets)"""
        logger.info(f"开始初始化MinIO存储桶, 终端: {self.config.endpoint}")
        
        try:
            # Test connection
            buckets = self.client.list_buckets()
            logger.info(f"已连接到MinIO服务器, 当前存在{len(buckets)}个存储桶")
        except Exception as e:
            logger.error(f"❌ 连接MinIO服务器失败: {e}")
            return False
        
        # Initialize required buckets
        required_buckets = [
            self.config.raw_bucket,
            self.config.processed_bucket,
            self.config.image_bucket
        ]
        
        success = True
        for bucket in required_buckets:
            if not self.ensure_bucket_exists(bucket):
                success = False
        
        if success:
            logger.info("✅ MinIO存储桶初始化完成")
        else:
            logger.error("❌ MinIO存储桶初始化失败")
            
        return success