"""
抽取模型框架
"""
from typing import Type

from pydantic import BaseModel, Field
from app.infrastructure.information_extraction.method import langextract as lx


class ModelConfig(BaseModel):
    """
    LLMs 常规配置
    """
    model_name: str = Field(..., description="模型名")
    api_key: str = Field(..., description="模型密钥")
    api_url: str = Field(..., description="模型地址")
    config: dict = Field(default_factory=dict, description="模型配置")


class LangextractConfig(ModelConfig):
    """
    Langextract 框架模型配置
    """
    language_model_type: Type[lx.LanguageModelT] = lx.inference.CustomAPIModel  # 用于推理的语言模型类型
    format_type: lx.data.FormatType = lx.data.FormatType.JSON  # 输出格式类型（JSON或YAML）
    max_char_buffer: int = 1000  # 推理时的最大字符数
    temperature: float = 0.5  # 生成时的采样温度，较高值可以减少重复输出
    fence_output: bool = False  # 是否期望/生成带围栏的输出
    use_schema_constraints: bool = True  # 是否为模型生成模式约束以启用结构化输出
    batch_length: int = 10  # 每批处理的文本块数量
    max_workers: int = 10  # 用于并发处理的最大并行工作线程数
    additional_context: str | None = None  # 推理期间添加到提示中的额外上下文
    resolver_params: dict | None = None  # 解析器参数，用于解析原始语言模型输出
    # language_model_params: dict | None = None  # 语言模型的额外参数
    debug: bool = True  # 是否填充调试字段
    extraction_passes: int = 1  # 顺序提取尝试次数，用于提高召回率
