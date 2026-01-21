"""
图数据库抽象接口
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass

from pydantic import BaseModel, Field


class GraphNode(BaseModel):
    """图节点数据类"""
    id: str = Field(..., description="节点ID")
    label: str = Field(..., description="节点类型")
    name: str = Field(..., description="节点名称")
    properties: dict = Field(default_factory=dict, description="节点属性")

    def to_dict(self) -> dict:
        """转换为字典"""
        return {
            "id": self.id,
            "label": self.label,
            "name": self.name,
            "properties": self.properties
        }


class GraphEdge(BaseModel):
    """图边数据类"""
    id: str = Field(..., description="边ID")
    source_id: str = Field(..., description="源节点ID")
    target_id: str = Field(..., description="目标节点ID")
    type: str = Field(..., description="关系类型")
    properties: dict = Field(default_factory=dict, description="边属性")

    def to_dict(self) -> dict:
        """转换为字典"""
        return {
            "id": self.id,
            "source_id": self.source_id,
            "target_id": self.target_id,
            "type": self.type,
            "properties": self.properties
        }


class GraphStats(BaseModel):
    """图统计信息数据类"""
    node_count: int = Field(..., description="节点数量")
    edge_count: int = Field(..., description="边数量")
    # entity_type_count: int = Field(..., description="实体类型数量")
    # relation_type_count: int = Field(..., description="关系类型数量")
    error: str = Field(default=None, description="错误信息")

    def to_dict(self) -> dict:
        """转换为字典"""
        return {
            "node_count": self.node_count,
            "edge_count": self.edge_count,
            # "entity_type_count": self.entity_type_count,
            # "relation_type_count": self.relation_type_count,
            "error": self.error
        }


class GraphVisualizationData(BaseModel):
    """图可视化数据类"""
    nodes: List[GraphNode] = Field(default_factory=list, description="节点列表")
    relationships: List[GraphEdge] = Field(default_factory=list, description="边列表")
    error: str = Field(default=None, description="错误信息")

    def to_dict(self) -> dict:
        """转换为字典"""
        result = {
            "nodes": [node.to_dict() for node in self.nodes],
            "edges": [relationship.to_dict() for relationship in self.relationships],
        }
        if self.error:
            result["error"] = self.error
        return result


class IGraphStorage(ABC):
    """图数据库存储抽象接口"""

    @abstractmethod
    def connect(self) -> bool:
        """建立连接"""
        pass

    @abstractmethod
    def disconnect(self):
        """断开连接"""
        pass

    @abstractmethod
    def is_connected(self) -> bool:
        """检查连接状态"""
        pass

    def add_subgraph_with_merge(self, kg_data, graph_tag, **kwargs):
        """添加子图并合并"""
        pass

    @abstractmethod
    def delete_subgraph(self, name: str) -> bool:
        """删除子图"""
        pass

    @abstractmethod
    def get_subgraph_stats(self, name: str) -> GraphStats:
        """获取子图统计信息"""
        pass

    @abstractmethod
    def get_graph_full_stats(self, graph_tag: str) -> dict:
        """获取图全量统计信息"""
        pass

    @abstractmethod
    def create_vector_index(self, name: str, dimension: int = 1536) -> bool:
        """创建向量索引"""
        pass

    # @abstractmethod
    # def build_knowledge_graph(
    #     self,
    #     graph_id: str,
    #     file_text: str,
    #     potential_schema: Optional[Dict[str, Any]] = None
    # ) -> Dict[str, Any]:
    #     """构建知识图谱"""
    #     pass

    @abstractmethod
    def get_visualization_data(self, graph_id: str, limit: Optional[int] = None) -> GraphVisualizationData:
        """获取可视化数据"""
        pass

    @abstractmethod
    def merge_graphs(self, graph_name, graph_name1):
        """合并图"""
        pass

    @abstractmethod
    def merge_graphs_with_match_node(
            self,
            source_graph_tag: str,
            target_graph_tag: str,
            matched_node_id: str,
    ):
        """合并图并匹配节点"""
        pass

    @abstractmethod
    def get_nodes_by_type(
            self,
            graph_tag: str,
            node_type: str,
    ):
        pass

    @abstractmethod
    def get_nodes_by_properties(
            self,
            graph_tag: str,
            properties: Dict[str, Any],
    ):
        pass
