"""
信息抽取类抽象接口
"""
from abc import ABC, abstractmethod
from typing import Dict, Any

from pydantic import Field, BaseModel


class TextClass(BaseModel):
    """文本数据类"""
    id: str = Field(..., description="文本ID")
    text: str = Field(..., description="文本内容")


class SourceText(BaseModel):
    """源文本数据类"""
    id: str = Field(..., description="源文本ID")
    start_pos: int = Field(None, description="源文本起始位置")
    end_pos: int = Field(None, description="源文本结束位置")
    alignment_status: str = Field(None, description="对齐状态")


class Entity(BaseModel):
    """实体数据类"""
    # id: str = Field(..., description="实体ID")
    name: str = Field(..., description="实体名称")
    entity_type: str = Field(..., description="实体类型")
    properties: Dict[str, Any] = Field(default_factory=dict, description="实体属性")
    source_texts: list[SourceText] = Field(default=[], description="源文本信息")


class Relationship(BaseModel):
    """关系数据类"""
    # id: str = Field(..., description="关系ID")
    source: str = Field(..., description="源节点ID")
    target: str = Field(..., description="目标节点ID")
    type: str = Field(..., description="关系类型")
    properties: Dict[str, Any] = Field(default_factory=dict, description="关系属性")
    source_texts: list[SourceText] = Field(default=[], description="源文本信息")


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
    ) -> tuple[list[Entity], list[TextClass]]:
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
    ) -> tuple[list[Relationship], list[TextClass]]:
        """关系抽取"""
        pass

    @abstractmethod
    def merge_text_class(
            self,
            text_classes_a: list[TextClass],
            text_classes_b: list[TextClass]
    ) -> list[TextClass]:
        """合并文本类"""
        pass
