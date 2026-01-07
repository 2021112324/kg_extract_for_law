from typing import Dict, Any

from sqlalchemy import Column, JSON, BIGINT, VARCHAR, \
    TEXT, INT, DateTime
from sqlalchemy.dialects.mssql import TINYINT

from app.db.base import Base, get_cn_datetime
from app.utils.snowflake_id import generate_snowflake_id


class KG(Base):
    """
    知识图谱数据库模型

    存储知识图谱基本信息，如名称、描述等
    """
    __tablename__ = "t_kg"

    id = Column(BIGINT, primary_key=True, index=True, default=lambda: generate_snowflake_id())
    name = Column(VARCHAR(255), nullable=False)
    description = Column(TEXT, nullable=True)
    entity_count = Column(INT, default=0)
    relation_count = Column(INT, default=0)
    config = Column(JSON, nullable=True)  # 存储图谱特定配置
    status = Column(TINYINT, default=0)  # 图谱状态：0-活跃，1-不活跃
    graph_name = Column(VARCHAR(255), nullable=True)  # 子图名称
    graph_status = Column(TINYINT, default=0)  # 图数据库状态：0-pending，1-created，2-error
    graph_config = Column(JSON, nullable=True)  # 存储graph相关配置
    del_flag = Column(TINYINT, default=0)  # 删除标志：0-正常，1-已删除
    create_time = Column(DateTime, default=get_cn_datetime)


    def to_dict(self) -> Dict[str, Any]:
        """将知识图谱转换为字典表示形式"""
        result = {
            "id": str(self.id),
            "name": self.name,
            "description": self.description or "",
            "entity_count": self.entity_count,
            "relation_count": self.relation_count,
            "config": self.config or {},
            "status": self.status,
            "graph_name": self.graph_name,
            "graph_status": self.graph_status,
            "graph_config": self.graph_config,
            "del_flag": self.del_flag,
        }

        return result


class KGExtractionTask(Base):
    """
    知识抽取任务数据库模型

    存储知识图谱抽取任务相关信息
    """
    # 定义数据库表名
    __tablename__ = "t_task"

    id = Column(BIGINT, primary_key=True, index=True, default=lambda: generate_snowflake_id())
    name = Column(VARCHAR(255), nullable=False)
    description = Column(TEXT, nullable=True)
    status = Column(TINYINT, default=0)  # 图谱状态：0-pending, 1-running, 2-completed, 3-merged, 4-failed, 5-cancelled
    prompt = Column(TEXT, nullable=True)
    parameters = Column(JSON, nullable=True)
    message = Column(TEXT, nullable=True)
    entity_count = Column(INT, default=0)
    relation_count = Column(INT, default=0)
    retry_count = Column(INT, default=0)  # 重试次数
    kg_id = Column(BIGINT, nullable=False)
    graph_name = Column(VARCHAR(255), nullable=True)  # 子图名称
    graph_status = Column(TINYINT, default=0)  # 图数据库状态：0-pending，1-created，2-error
    graph_config = Column(JSON, nullable=True)  # 存储graph相关配置
    del_flag = Column(TINYINT, default=0)  # 删除标志：0-正常，1-已删除
    create_time = Column(DateTime, default=get_cn_datetime)

    def to_dict(self) -> Dict[str, Any]:
        """将抽取任务转换为字典表示形式"""
        result = {
            "id": str(self.id),
            "name": self.name,
            "description": self.description or "",
            "status": self.status,
            "prompt": self.prompt,
            "parameters": self.parameters or {},
            "message": self.message or "",
            "entity_count": self.entity_count,
            "relation_count": self.relation_count,
            "retry_count": self.retry_count,
            "kg_id": str(self.kg_id),
            "graph_name": self.graph_name,
            "graph_status": self.graph_status,
            "graph_config": self.graph_config or {},
            "del_flag": self.del_flag,
        }
        return result


class KGFile(Base):
    """
    知识图谱文件数据库模型

    存储知识图谱文件相关信息
    """
    __tablename__ = "t_file"

    id = Column(BIGINT, primary_key=True, index=True, default=lambda: generate_snowflake_id())
    kg_id = Column(BIGINT, nullable=False)
    task_id = Column(BIGINT, nullable=False)
    minio_filename = Column(VARCHAR(255), nullable=True)
    filename = Column(VARCHAR(255), nullable=True)
    minio_bucket = Column(TEXT, nullable=True)
    minio_path = Column(TEXT, nullable=True)
    create_time = Column(DateTime, default=get_cn_datetime)

    def to_dict(self) -> Dict[str, Any]:
        """将文件转换为字典表示形式"""
        result = {
            "id": str(self.id),
            "kg_id": str(self.kg_id),
            "task_id": str(self.task_id),
            "minio_filename": self.minio_filename,
            "filename": self.filename,
            "minio_bucket": self.minio_bucket,
            "minio_path": self.minio_path,
        }
        return result
