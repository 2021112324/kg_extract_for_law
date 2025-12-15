"""MinerU API 配置模块"""

import os
import logging
from pathlib import Path
from tempfile import TemporaryDirectory

# 配置日志
logger = logging.getLogger(__name__)


def ensure_output_dir(output_dir: str = None) -> Path:
    """
    确保输出目录存在
    
    Args:
        output_dir: 输出目录路径，如果为None则使用临时目录
        
    Returns:
        Path: 输出目录路径
    """
    if output_dir:
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        return output_path
    else:
        # 使用临时目录
        temp_dir = TemporaryDirectory()
        return Path(temp_dir.name)