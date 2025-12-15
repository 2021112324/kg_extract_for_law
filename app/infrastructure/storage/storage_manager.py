import os
import uuid
import logging
import shutil
from pathlib import Path
from typing import Tuple, Dict, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class StorageInfo:
    """存储信息数据类"""
    output_dir: str
    images_dir: str
    temp_dir: Optional[str] = None
    
class StorageManager:
    """
    文件存储管理器
    
    负责文件和目录的创建、管理和清理
    不包含具体的文件处理逻辑，保持职责单一
    """
    
    # 默认的基础目录配置
    BASE_UPLOAD_DIR = "uploads"
    PROCESSED_DIR = "processd"
    IMAGES_SUBDIR = "images"
    TEMP_DIR = "temp"
    
    @classmethod
    def create_output_directory(cls, knowledge_id: str, file_uuid: str) -> StorageInfo:
        """
        为文件处理创建输出目录结构
        
        Args:
            knowledge_id: 知识库ID
            file_uuid: 文件UUID
            
        Returns:
            StorageInfo: 存储信息对象
        """
        # 构建目录路径
        output_dir = os.path.join(
            cls.BASE_UPLOAD_DIR, 
            cls.PROCESSED_DIR, 
            knowledge_id, 
            file_uuid
        )
        images_dir = os.path.join(output_dir, cls.IMAGES_SUBDIR)
        
        # 创建目录
        os.makedirs(output_dir, exist_ok=True)
        os.makedirs(images_dir, exist_ok=True)
        
        logger.info(f"创建输出目录: {output_dir}")
        logger.info(f"创建图片目录: {images_dir}")
        
        return StorageInfo(
            output_dir=output_dir,
            images_dir=images_dir
        )
    
    @classmethod
    def create_temp_directory(cls, prefix: str = "temp") -> str:
        """
        创建临时目录
        
        Args:
            prefix: 目录名前缀
            
        Returns:
            str: 临时目录路径
        """
        temp_dir_name = f"{prefix}_{uuid.uuid4().hex[:8]}"
        temp_dir = os.path.join(cls.BASE_UPLOAD_DIR, cls.TEMP_DIR, temp_dir_name)
        
        os.makedirs(temp_dir, exist_ok=True)
        logger.info(f"创建临时目录: {temp_dir}")
        
        return temp_dir
    
    @staticmethod
    def save_content_to_file(content: str, file_path: str, encoding: str = 'utf-8') -> bool:
        """
        保存内容到文件
        
        Args:
            content: 要保存的内容
            file_path: 目标文件路径
            encoding: 文件编码
            
        Returns:
            bool: 是否保存成功
        """
        try:
            # 确保目录存在
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            with open(file_path, 'w', encoding=encoding) as f:
                f.write(content)
            
            logger.info(f"内容已保存到: {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"保存文件失败 {file_path}: {str(e)}")
            return False
    
    @staticmethod
    def copy_file(source_path: str, dest_path: str) -> bool:
        """
        复制文件
        
        Args:
            source_path: 源文件路径
            dest_path: 目标文件路径
            
        Returns:
            bool: 是否复制成功
        """
        try:
            # 确保目标目录存在
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
            
            shutil.copy2(source_path, dest_path)
            logger.info(f"文件已复制: {source_path} -> {dest_path}")
            return True
            
        except Exception as e:
            logger.error(f"复制文件失败: {str(e)}")
            return False
    
    @staticmethod
    def cleanup_directory(dir_path: str, remove_parent: bool = False) -> bool:
        """
        清理目录内容
        
        Args:
            dir_path: 要清理的目录路径
            remove_parent: 是否删除父目录
            
        Returns:
            bool: 是否清理成功
        """
        try:
            if not os.path.exists(dir_path):
                return True
            
            if remove_parent:
                shutil.rmtree(dir_path)
                logger.info(f"已删除目录: {dir_path}")
            else:
                # 只清理目录内容，保留目录
                for item in os.listdir(dir_path):
                    item_path = os.path.join(dir_path, item)
                    if os.path.isdir(item_path):
                        shutil.rmtree(item_path)
                    else:
                        os.remove(item_path)
                logger.info(f"已清理目录内容: {dir_path}")
            
            return True
            
        except Exception as e:
            logger.error(f"清理目录失败 {dir_path}: {str(e)}")
            return False
    
    @classmethod
    def get_storage_stats(cls, knowledge_id: str) -> Dict[str, any]:
        """
        获取存储统计信息
        
        Args:
            knowledge_id: 知识库ID
            
        Returns:
            Dict: 存储统计信息
        """
        base_dir = os.path.join(cls.BASE_UPLOAD_DIR, cls.PROCESSED_DIR, knowledge_id)
        
        if not os.path.exists(base_dir):
            return {
                "total_files": 0,
                "total_size": 0,
                "directories": 0
            }
        
        total_files = 0
        total_size = 0
        directories = 0
        
        try:
            for root, dirs, files in os.walk(base_dir):
                directories += len(dirs)
                for file in files:
                    file_path = os.path.join(root, file)
                    if os.path.exists(file_path):
                        total_files += 1
                        total_size += os.path.getsize(file_path)
        
        except Exception as e:
            logger.error(f"获取存储统计失败: {str(e)}")
        
        return {
            "total_files": total_files,
            "total_size": total_size,
            "directories": directories,
            "base_dir": base_dir
        }
    
    @staticmethod
    def ensure_unique_filename(dir_path: str, filename: str) -> str:
        """
        确保文件名在目录中唯一
        
        Args:
            dir_path: 目录路径
            filename: 原始文件名
            
        Returns:
            str: 唯一的文件名
        """
        if not os.path.exists(os.path.join(dir_path, filename)):
            return filename
        
        name, ext = os.path.splitext(filename)
        counter = 1
        
        while True:
            new_filename = f"{name}_{counter}{ext}"
            if not os.path.exists(os.path.join(dir_path, new_filename)):
                return new_filename
            counter += 1