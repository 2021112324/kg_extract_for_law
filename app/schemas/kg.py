from datetime import datetime
from typing import Optional, List, Dict, Any

from fastapi import UploadFile, File
from pydantic import BaseModel


class KGCreate(BaseModel):
    """
    知识图谱创建请求模型

    用于API接口创建知识图谱的请求数据
    """
    name: str
    description: Optional[str] = None
    config: Optional[dict] = None


class KGSchema(BaseModel):
    """
    知识图谱Schema模型

    用于表示知识图谱的Schema定义信息
    """
    nodes: str
    edges: str


class KGTaskCreate(BaseModel):
    """
    知识抽取任务创建模型

    用于API接口创建知识抽取任务的请求数据
    """
    name: str
    description: Optional[str] = None
    prompt: Optional[str] = None
    schema: KGSchema = None
    examples: Optional[List[dict]] = None
    # parameters: Optional[dict] = None
    # files: List[UploadFile] = File(...)


class KGTaskCreateByFile(BaseModel):
    """
    针对每个

    用于API接口创建知识抽取任务的请求数据
    """
    dir: str
    prompt: Optional[str] = None
    schema: KGSchema = None
    examples: Optional[List[dict]] = None
    # parameters: Optional[dict] = None
    # files: List[UploadFile] = File(...)


class GraphNodeBase(BaseModel):
    """图谱节点基础模式"""
    node_id: str
    node_name: str
    node_type: str
    properties: Optional[Dict[str, Any]] = None
    description: Optional[str] = None


class GraphEdgeBase(BaseModel):
    """图谱边基础模式"""
    source_id: str
    target_id: str
    relation_type: str
    properties: Optional[Dict[str, Any]] = None
    weight: float = 1.0
    bidirectional: bool = False
