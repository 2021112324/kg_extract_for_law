from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)

@dataclass
class ProcessResult:
    """文件处理结果数据类"""
    success: bool
    file_path: str
    file_type: str
    extracted_text: str = ""
    output_files: List[str] = None
    metadata: Dict[str, Any] = None
    error_message: str = ""
    processing_time: float = 0.0
    
    def __post_init__(self):
        if self.output_files is None:
            self.output_files = []
        if self.metadata is None:
            self.metadata = {}

class BaseProcessor(ABC):
    """
    文件处理器抽象基类
    
    定义所有文件处理器的通用接口和基础功能
    每个具体的处理器都应该继承此类
    """
    
    def __init__(self):
        self.supported_extensions: List[str] = []
        self.processor_name: str = self.__class__.__name__
    
    @abstractmethod
    async def process_file(self, file_path: str, output_dir: str, **kwargs) -> ProcessResult:
        """
        处理文件的主方法
        
        Args:
            file_path: 输入文件路径
            output_dir: 输出目录
            **kwargs: 额外参数
            
        Returns:
            ProcessResult: 处理结果
        """
        pass
    
    @abstractmethod
    async def extract_text(self, file_path: str) -> str:
        """
        提取文件中的文本内容
        
        Args:
            file_path: 文件路径
            
        Returns:
            str: 提取的文本内容
        """
        pass
    
    def supports_file(self, file_path: str) -> bool:
        """
        检查是否支持此文件类型
        
        Args:
            file_path: 文件路径
            
        Returns:
            bool: 是否支持
        """
        import os
        file_ext = os.path.splitext(file_path)[1].lower()
        return file_ext in self.supported_extensions
    
    def get_metadata(self, file_path: str) -> Dict[str, Any]:
        """
        获取文件元数据
        
        Args:
            file_path: 文件路径
            
        Returns:
            Dict: 文件元数据
        """
        import os
        from datetime import datetime
        
        try:
            stat = os.stat(file_path)
            return {
                'file_size': stat.st_size,
                'created_time': datetime.fromtimestamp(stat.st_ctime).isoformat(),
                'modified_time': datetime.fromtimestamp(stat.st_mtime).isoformat(),
                'processor': self.processor_name,
                'supported_extensions': self.supported_extensions
            }
        except Exception as e:
            logger.error(f"获取文件元数据失败: {str(e)}")
            return {}
    
    def validate_input(self, file_path: str) -> bool:
        """
        验证输入文件
        
        Args:
            file_path: 文件路径
            
        Returns:
            bool: 是否通过验证
        """
        import os
        
        if not file_path:
            logger.error("文件路径为空")
            return False
        
        if not os.path.exists(file_path):
            logger.error(f"文件不存在: {file_path}")
            return False
        
        if not self.supports_file(file_path):
            logger.error(f"不支持的文件类型: {file_path}")
            return False
        
        return True
    
    def create_error_result(self, file_path: str, error_message: str) -> ProcessResult:
        """
        创建错误结果
        
        Args:
            file_path: 文件路径
            error_message: 错误消息
            
        Returns:
            ProcessResult: 错误结果
        """
        import os
        file_ext = os.path.splitext(file_path)[1].lower() if file_path else "unknown"
        
        return ProcessResult(
            success=False,
            file_path=file_path or "unknown",
            file_type=file_ext,
            error_message=error_message,
            metadata=self.get_metadata(file_path) if file_path and os.path.exists(file_path) else {}
        )
    
    async def process_with_error_handling(self, file_path: str, output_dir: str, **kwargs) -> ProcessResult:
        """
        带错误处理的文件处理方法
        
        Args:
            file_path: 文件路径
            output_dir: 输出目录
            **kwargs: 额外参数
            
        Returns:
            ProcessResult: 处理结果
        """
        import time
        
        start_time = time.time()
        
        try:
            # 验证输入
            if not self.validate_input(file_path):
                return self.create_error_result(file_path, "输入文件验证失败")
            
            logger.info(f"开始处理文件: {file_path} 使用 {self.processor_name}")
            
            # 调用子类实现
            result = await self.process_file(file_path, output_dir, **kwargs)
            
            # 记录处理时间
            result.processing_time = time.time() - start_time
            
            if result.success:
                logger.info(f"文件处理成功: {file_path}, 耗时: {result.processing_time:.2f}秒")
            else:
                logger.warning(f"文件处理失败: {file_path}, 错误: {result.error_message}")
            
            return result
            
        except Exception as e:
            processing_time = time.time() - start_time
            error_msg = f"{self.processor_name}处理失败: {str(e)}"
            logger.error(error_msg)
            
            result = self.create_error_result(file_path, error_msg)
            result.processing_time = processing_time
            return result