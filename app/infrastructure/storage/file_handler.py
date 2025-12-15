import os
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class FileInfo:
    """文件基础信息数据类"""
    file_path: str
    file_name: str
    file_ext: str
    file_size: int
    process_time: str

class FileHandler:
    """
    基础文件操作处理器
    
    负责文件的基础操作：验证、信息获取、类型检测等
    不包含具体的文件处理逻辑，保持职责单一
    """
    
    @staticmethod
    def validate_file_exists(file_path: str) -> bool:
        """
        验证文件是否存在
        
        Args:
            file_path: 文件路径
            
        Returns:
            bool: 文件是否存在
        """
        if not file_path:
            return False
        return os.path.exists(file_path)
    
    @staticmethod
    def get_file_info(file_path: str) -> FileInfo:
        """
        获取文件基本信息
        
        Args:
            file_path: 文件路径
            
        Returns:
            FileInfo: 文件信息对象
        """
        if not FileHandler.validate_file_exists(file_path):
            raise FileNotFoundError(f"文件不存在: {file_path}")
        
        file_name = os.path.basename(file_path)
        file_ext = os.path.splitext(file_name)[1].lower()
        file_size = os.path.getsize(file_path)
        process_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        return FileInfo(
            file_path=file_path,
            file_name=file_name,
            file_ext=file_ext,
            file_size=file_size,
            process_time=process_time
        )
    
    @staticmethod
    def detect_file_type(file_path: str) -> str:
        """
        检测文件类型分类
        
        Args:
            file_path: 文件路径
            
        Returns:
            str: 文件类型分类 (pdf, office, image, text, unknown)
        """
        file_info = FileHandler.get_file_info(file_path)
        file_ext = file_info.file_ext
        
        if file_ext == '.pdf':
            return 'pdf'
        elif file_ext in ['.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx']:
            return 'office'
        elif file_ext in ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.tiff']:
            return 'image'
        elif file_ext in ['.txt', '.md', '.csv', '.json', '.xml']:
            return 'text'
        else:
            return 'unknown'
    
    @staticmethod
    def get_safe_filename(filename: str) -> str:
        """
        获取安全的文件名（移除特殊字符）
        
        Args:
            filename: 原始文件名
            
        Returns:
            str: 安全的文件名
        """
        import re
        # 移除或替换不安全的字符
        safe_filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
        # 移除多余的空格和点号
        safe_filename = re.sub(r'[\s.]+', '_', safe_filename)
        return safe_filename
    
    @staticmethod
    def ensure_directory_exists(dir_path: str) -> None:
        """
        确保目录存在，如果不存在则创建
        
        Args:
            dir_path: 目录路径
        """
        if not os.path.exists(dir_path):
            os.makedirs(dir_path, exist_ok=True)
            logger.info(f"创建目录: {dir_path}")
    
    @staticmethod
    def get_file_extension_info(file_ext: str) -> Dict[str, str]:
        """
        获取文件扩展名的详细信息
        
        Args:
            file_ext: 文件扩展名
            
        Returns:
            Dict: 包含类型和描述的字典
        """
        ext_info = {
            '.pdf': {'type': 'document', 'description': 'PDF文档'},
            '.doc': {'type': 'document', 'description': 'Word文档'},
            '.docx': {'type': 'document', 'description': 'Word文档'},
            '.xls': {'type': 'spreadsheet', 'description': 'Excel表格'},
            '.xlsx': {'type': 'spreadsheet', 'description': 'Excel表格'},
            '.ppt': {'type': 'presentation', 'description': 'PowerPoint演示'},
            '.pptx': {'type': 'presentation', 'description': 'PowerPoint演示'},
            '.jpg': {'type': 'image', 'description': 'JPEG图片'},
            '.jpeg': {'type': 'image', 'description': 'JPEG图片'},
            '.png': {'type': 'image', 'description': 'PNG图片'},
            '.gif': {'type': 'image', 'description': 'GIF图片'},
            '.txt': {'type': 'text', 'description': '文本文件'},
            '.md': {'type': 'text', 'description': 'Markdown文件'},
            '.csv': {'type': 'data', 'description': 'CSV数据文件'},
            '.json': {'type': 'data', 'description': 'JSON数据文件'},
        }
        
        return ext_info.get(file_ext.lower(), {
            'type': 'unknown', 
            'description': '未知类型文件'
        })