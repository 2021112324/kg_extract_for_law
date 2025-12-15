from typing import Generator

from app.db.base import SessionLocal


def get_db() -> Generator:
    """
    获取数据库会话的依赖函数
    
    用于FastAPI依赖注入系统，提供数据库会话
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close() 