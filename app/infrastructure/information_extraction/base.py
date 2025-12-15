"""
信息抽取类抽象接口
"""
from abc import ABC, abstractmethod
from typing import Dict, Any

from pydantic import Field, BaseModel


class Entity(BaseModel):
    """实体数据类"""
    # id: str = Field(..., description="实体ID")
    name: str = Field(..., description="实体名称")
    entity_type: str = Field(..., description="实体类型")
    properties: Dict[str, Any] = Field(default_factory=dict, description="实体属性")


class Relationship(BaseModel):
    """关系数据类"""
    # id: str = Field(..., description="关系ID")
    source: str = Field(..., description="源节点ID")
    target: str = Field(..., description="目标节点ID")
    type: str = Field(..., description="关系类型")
    properties: Dict[str, Any] = Field(default_factory=dict, description="关系属性")


class IInformationExtraction(ABC):
    """信息抽取抽象接口"""

    def __init__(self):
        pass

    @abstractmethod
    async def entity_and_relationship_extract(
            self,
            prompt: str,
            entity_schema: dict,
            relation_schema: dict,
            input_text: str,
            **kwargs
    ) -> dict:
        """信息抽取"""
        pass


    @abstractmethod
    async def entity_extract(
            self,
            prompt: str,
            entity_schema: dict,
            input_text: str,
            **kwargs
    ) -> list[Entity]:
        """实体抽取"""
        pass

    @abstractmethod
    async def relationship_extract(
            self,
            prompt: str,
            entities_list: list,
            relation_schema: dict,
            input_text: str,
            **kwargs
    ) -> list[Relationship]:
        """关系抽取"""
        pass
