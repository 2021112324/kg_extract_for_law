import logging
from sqlalchemy.orm import Session

from app.db.base import Base, engine

# 创建所有表
def create_tables() -> None:
    Base.metadata.create_all(bind=engine)

# 初始化数据库
def init_db(db: Session) -> None:
    create_tables()

# 清空数据库
def reset_db() -> None:
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    create_tables()
    logging.info("数据库表已创建") 