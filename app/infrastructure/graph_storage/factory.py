"""
图数据库工厂
"""
from typing import Dict, Any
import logging

from .base import IGraphStorage
from .neo4j_adapter import Neo4jAdapter

logger = logging.getLogger(__name__)


class GraphStorageFactory:
    """图数据库工厂类"""
    
    _adapters = {
        "neo4j": Neo4jAdapter,
    }
    
    @classmethod
    def create(cls, adapter_type: str, config: Dict[str, Any]) -> IGraphStorage:
        """
        创建图数据库适配器实例
        
        Args:
            adapter_type: 适配器类型 (neo4j, arangodb等)
            config: 配置参数
            
        Returns:
            IGraphStorage: 图数据库适配器实例
            
        Raises:
            ValueError: 不支持的适配器类型
            KeyError: 缺少必要的配置参数
        """
        if adapter_type not in cls._adapters:
            raise ValueError(f"不支持的图数据库类型: {adapter_type}")
        
        adapter_class = cls._adapters[adapter_type]
        
        try:
            return adapter_class(**config)
        except TypeError as e:
            logger.error(f"创建{adapter_type}适配器失败，配置参数错误: {e}")
            raise KeyError(f"缺少必要的配置参数: {e}")
    
    @classmethod
    def register_adapter(cls, name: str, adapter_class):
        """
        注册新的适配器类型
        
        Args:
            name: 适配器名称
            adapter_class: 适配器类（必须实现IGraphStorage接口）
        """
        if not issubclass(adapter_class, IGraphStorage):
            raise ValueError("适配器类必须实现IGraphStorage接口")
        
        cls._adapters[name] = adapter_class
        logger.info(f"注册图数据库适配器: {name}")
    
    @classmethod
    def get_supported_adapters(cls) -> list:
        """获取支持的适配器类型列表"""
        return list(cls._adapters.keys())