import os
import logging
from typing import Dict, Any, Optional, Type
from .base_processor import BaseProcessor, ProcessResult
from .pdf_processor import PDFProcessor
from .office_processor import OfficeProcessor
from .image_processor import ImageProcessor
from .text_processor import TextProcessor

logger = logging.getLogger(__name__)

class ProcessorFactory:
    """
    格式处理器工厂类
    
    负责根据文件类型创建和管理相应的格式处理器
    """
    
    def __init__(self):
        self._processors: Dict[str, Type[BaseProcessor]] = {}
        self._instances: Dict[str, BaseProcessor] = {}
        self._register_default_processors()
    
    def _register_default_processors(self):
        """注册默认的格式处理器"""
        # PDF处理器
        self.register_processor(PDFProcessor, ['.pdf'])
        
        # Office处理器
        self.register_processor(OfficeProcessor, ['.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx'])
        
        # 图片处理器
        self.register_processor(ImageProcessor, ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.tiff', '.svg'])
        
        # 文本处理器
        self.register_processor(TextProcessor, ['.txt', '.md', '.csv', '.json', '.xml', '.yml', '.yaml'])
    
    def register_processor(self, processor_class: Type[BaseProcessor], extensions: list):
        """
        注册格式处理器
        
        Args:
            processor_class: 处理器类
            extensions: 支持的文件扩展名列表
        """
        for ext in extensions:
            ext = ext.lower()
            self._processors[ext] = processor_class
            logger.debug(f"注册处理器 {processor_class.__name__} 用于扩展名 {ext}")
    
    def get_processor(self, file_path: str) -> Optional[BaseProcessor]:
        """
        根据文件路径获取对应的处理器实例
        
        Args:
            file_path: 文件路径
            
        Returns:
            BaseProcessor: 对应的处理器实例，如果不支持则返回None
        """
        file_ext = os.path.splitext(file_path)[1].lower()
        
        if file_ext not in self._processors:
            logger.warning(f"不支持的文件格式: {file_ext}")
            return None
        
        processor_class = self._processors[file_ext]
        
        # 使用单例模式，避免重复创建处理器实例
        if file_ext not in self._instances:
            self._instances[file_ext] = processor_class()
            logger.debug(f"创建处理器实例: {processor_class.__name__}")
        
        return self._instances[file_ext]
    
    def get_processor_by_type(self, file_type: str) -> Optional[BaseProcessor]:
        """
        根据文件类型获取处理器实例
        
        Args:
            file_type: 文件扩展名（如 '.pdf'）
            
        Returns:
            BaseProcessor: 对应的处理器实例，如果不支持则返回None
        """
        file_type = file_type.lower()
        if file_type not in self._processors:
            return None
        
        processor_class = self._processors[file_type]
        
        if file_type not in self._instances:
            self._instances[file_type] = processor_class()
        
        return self._instances[file_type]
    
    def supports_file(self, file_path: str) -> bool:
        """
        检查是否支持指定文件
        
        Args:
            file_path: 文件路径
            
        Returns:
            bool: 是否支持
        """
        file_ext = os.path.splitext(file_path)[1].lower()
        return file_ext in self._processors
    
    def get_supported_extensions(self) -> list:
        """
        获取所有支持的文件扩展名
        
        Returns:
            list: 支持的文件扩展名列表
        """
        return list(self._processors.keys())
    
    def get_processor_info(self) -> Dict[str, Any]:
        """
        获取所有注册的处理器信息
        
        Returns:
            Dict: 处理器信息
        """
        info = {}
        
        for ext, processor_class in self._processors.items():
            processor_name = processor_class.__name__
            if processor_name not in info:
                info[processor_name] = {
                    'class': processor_name,
                    'extensions': []
                }
            info[processor_name]['extensions'].append(ext)
        
        return info
    
    async def process_file(self, file_path: str, output_dir: str, **kwargs) -> ProcessResult:
        """
        通用文件处理方法
        
        Args:
            file_path: 文件路径
            output_dir: 输出目录
            **kwargs: 额外参数
            
        Returns:
            ProcessResult: 处理结果
        """
        if not os.path.exists(file_path):
            error_msg = f"文件不存在: {file_path}"
            logger.error(error_msg)
            return ProcessResult(
                success=False,
                file_path=file_path,
                file_type="unknown",
                error_message=error_msg
            )
        
        processor = self.get_processor(file_path)
        if processor is None:
            file_ext = os.path.splitext(file_path)[1].lower()
            error_msg = f"不支持的文件格式: {file_ext}"
            logger.error(error_msg)
            return ProcessResult(
                success=False,
                file_path=file_path,
                file_type=file_ext,
                error_message=error_msg
            )
        
        try:
            return await processor.process_file(file_path, output_dir, **kwargs)
        except Exception as e:
            error_msg = f"文件处理失败: {str(e)}"
            logger.error(error_msg)
            return ProcessResult(
                success=False,
                file_path=file_path,
                file_type=os.path.splitext(file_path)[1].lower(),
                error_message=error_msg
            )


# 全局工厂实例
processor_factory = ProcessorFactory()


def get_processor_factory() -> ProcessorFactory:
    """
    获取全局处理器工厂实例
    
    Returns:
        ProcessorFactory: 处理器工厂实例
    """
    return processor_factory


async def process_file(file_path: str, output_dir: str, **kwargs) -> ProcessResult:
    """
    便捷的文件处理函数
    
    Args:
        file_path: 文件路径
        output_dir: 输出目录
        **kwargs: 额外参数
        
    Returns:
        ProcessResult: 处理结果
    """
    return await processor_factory.process_file(file_path, output_dir, **kwargs)


def supports_file(file_path: str) -> bool:
    """
    检查是否支持指定文件
    
    Args:
        file_path: 文件路径
        
    Returns:
        bool: 是否支持
    """
    return processor_factory.supports_file(file_path)


def get_supported_extensions() -> list:
    """
    获取所有支持的文件扩展名
    
    Returns:
        list: 支持的文件扩展名列表
    """
    return processor_factory.get_supported_extensions()