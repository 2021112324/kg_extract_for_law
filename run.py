#!/usr/bin/env python3
import uvicorn
import logging
import os
from datetime import datetime
from app.core.config import settings

# 创建logs目录（如果不存在）
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# 配置日志
# 创建logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 创建控制台处理器
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# 创建文件处理器，按日期生成日志文件
log_filename = f"logs/app_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
file_handler = logging.FileHandler(log_filename, encoding='utf-8')
file_handler.setLevel(logging.INFO)

# 创建格式器
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

# 清除可能已存在的处理器，然后添加新的处理器
logger.handlers = []
logger.addHandler(console_handler)
logger.addHandler(file_handler)


if __name__ == "__main__":
    # 启动FastAPI应用
    logger.info(f"启动API服务 - 监听 {settings.HOST}:{settings.PORT}")
    logger.info(f"日志文件路径: {log_filename}")
    uvicorn.run("app.main:app", host=settings.HOST, port=settings.PORT, reload=settings.RELOAD)
