import secrets
import os
import json
from typing import Any, Dict, List, Optional, Union

from dotenv import load_dotenv
from pydantic_settings import BaseSettings
from pydantic import field_validator, Field, AnyHttpUrl

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
env_path = os.path.join(project_root, '.env')
load_dotenv(env_path)

class Settings(BaseSettings):
    # 基本设置
    API_V1_STR: str = "/api"
    PROJECT_NAME: str = "CogmAIt"
    
    # 安全设置
    SECRET_KEY: str = Field(default_factory=lambda: secrets.token_urlsafe(32))
    # 60 分钟 * 24 小时 * 8 天 = 8 天
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24 * 8

    # CORS 设置
    CORS_ORIGINS: List[str] = ["http://localhost:3000", "http://localhost:5173", "http://localhost:8080", "*"]

    @field_validator("CORS_ORIGINS", mode="before")
    @classmethod
    def assemble_cors_origins(cls, v: Union[str, List[str]]) -> List[str]:
        if isinstance(v, str):
            # 如果是一个字符串，尝试将其解析为JSON数组
            try:
                return json.loads(v)
            except json.JSONDecodeError:
                # 如果无法解析为JSON，则尝试以逗号分隔
                if v.startswith("[") and v.endswith("]"):
                    # 可能是格式不正确的JSON，尝试手动处理
                    v = v.strip("[]").strip()
                    if v:
                        return [i.strip().strip('"\'') for i in v.split(",")]
                    return []
                else:
                    # 普通的逗号分隔字符串
                    return [i.strip() for i in v.split(",")]
        
        # 如果已经是列表，直接返回
        if isinstance(v, list):
            return v
        
        # 默认返回空列表
        return []

    # 数据源加密密钥
    DATASOURCE_ENCRYPTION_KEY: Optional[str] = None

    # 数据库设置
    DB_HOST: str = "localhost"
    DB_PORT: str = "3306"
    DB_USER: str = "root"
    DB_PASSWORD: str = "password"
    DB_NAME: str = os.getenv("DB_NAME", "cogmait")
    
    # 默认使用MySQL数据库
    DATABASE_URI: Optional[str] = None
    
    @property
    def SQLALCHEMY_DATABASE_URI(self) -> str:
        """
        获取数据库URI
        """
        if self.DATABASE_URI:
            return self.DATABASE_URI
        
        # 默认使用MySQL
        return f"mysql+pymysql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
    
    # 是否自动创建数据库表结构
    CREATE_TABLES: bool = True
    
    # 模型供应商设置
    OPENAI_API_KEY: Optional[str] = None
    QWEN_API_KEY: Optional[str] = None
    GPUSTACK_API_KEY: Optional[str] = None
    # ANTHROPIC_API_KEY: Optional[str] = None
    # GOOGLE_API_KEY: Optional[str] = None
    TAVILY_API_KEY: Optional[str] = None
    ALIYUN_API_KEY: Optional[str] = None

    
    # 模型路径设置
    PROVIDERS_PACKAGE: str = "app.providers"
    
    # MinIO配置
    MINIO_ENDPOINT: str = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    MINIO_ACCESS_KEY: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    MINIO_SECRET_KEY: str = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    MINIO_SECURE: bool = os.getenv("MINIO_SECURE", False)
    
    # 添加KG_EXTRACT配置
    KG_EXTRACT_METHOD: str = os.getenv("KG_EXTRACT_METHOD", "langextract")  # 图谱抽取采取的方法框架
    KG_EXTRACT_BUCKET: str = os.getenv("KG_EXTRACT_BUCKET", "kg-extract")   # 图谱抽取的存储桶名称
    # THREAD_POOL_MAX_WORKERS: int = int(os.getenv("THREAD_POOL_MAX_WORKERS", 5))  # 图谱抽取使用的线程池最大工作线程数
    
    # 图数据库配置
    GRAPH_DB_TYPE: str = "neo4j"  # 支持的类型: neo4j, arangodb 等
    
    # Neo4j配置
    NEO4J_URI: str = os.getenv("NEO4J_URI", "http://localhost:7687")
    NEO4J_USERNAME: str = os.getenv("NEO4J_USERNAME", "neo4j")
    NEO4J_PASSWORD: str = os.getenv("NEO4J_PASSWORD", "password")
    NEO4J_DATABASE: str = os.getenv("NEO4J_DATABASE", "neo4j")

    # Langfuse配置
    LANGFUSE_SECRET_KEY: str = os.getenv("LANGFUSE_SECRET_KEY", "sk-lf-c0a7335b-b826-4071-bc93-3952bac9c3f0")
    LANGFUSE_PUBLIC_KEY: str = os.getenv("LANGFUSE_PUBLIC_KEY", "pk-lf-3eaa9ef7-2d40-4fa9-85c3-ca0fa1d06657")
    LANGFUSE_HOST: str = os.getenv("LANGFUSE_HOST", "http://60.205.171.106:3000")
    @property
    def GRAPH_DB_CONFIG(self) -> Dict[str, Any]:
        """
        获取图数据库配置
        """
        if self.GRAPH_DB_TYPE == "neo4j":
            return {
                "uri": self.NEO4J_URI,
                "username": self.NEO4J_USERNAME,
                "password": self.NEO4J_PASSWORD,
                "database": self.NEO4J_DATABASE
            }
        # 未来可以添加其他图数据库配置
        # elif self.GRAPH_DB_TYPE == "arangodb":
        #     return {...}
        else:
            raise ValueError(f"不支持的图数据库类型: {self.GRAPH_DB_TYPE}")
    
    # MinerU API配置
    MINERU_API_BASE: str = os.getenv("MINERU_API_BASE", "https://api.mineru.ai")
    MINERU_API_KEY: Optional[str] = os.getenv("MINERU_API_KEY", None)
    
    # Milvus向量数据库配置
    MILVUS_URI: str = "http://localhost:19530"
    MILVUS_TOKEN: Optional[str] = None
    MILVUS_USER: Optional[str] = None
    MILVUS_PASSWORD: Optional[str] = None
    MILVUS_DB_NAME: Optional[str] = None
    MILVUS_SECURE: bool = False
    
    # 服务器启动配置
    HOST: str = "0.0.0.0"
    PORT: int = 8092
    RELOAD: bool = True

    # Swift / Fine-tune 配置
    SWIFT_DEFAULT_OUTPUT_DIR: str = "output"
    SWIFT_DEFAULT_SYSTEM_PROMPT: str = "You are a helpful assistant."
    SWIFT_DATA_SEED: int = 42

    BATCH_LENGTH: int = int(os.getenv("BATCH_LENGTH", 5))
    MAX_WORKERS: int = int(os.getenv("MAX_WORKERS", 3))
    MAX_CHAR_BUFFER: int = int(os.getenv("MAX_CHAR_BUFFER", 5000))
    MAX_CHUNK_SIZE: int = int(os.getenv("MAX_CHUNK_SIZE", 5000))
    OVERLAP_SIZE: int = int(os.getenv("OVERLAP_SIZE", 500))
    THREAD_POOL_MAX_WORKERS: int = int(os.getenv("THREAD_POOL_MAX_WORKERS", 10))
    TIMEOUT: int = int(os.getenv("TIMEOUT", 300))

    class Config:
        case_sensitive = True
        env_file = ".env"


# 创建设置实例
settings = Settings() 
