from sqlalchemy import create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import pymysql
from datetime import datetime, timedelta, timezone

from app.core.config import settings

# 定义东八区时区（中国标准时间 UTC+8）
CST_TIMEZONE = timezone(timedelta(hours=8))

# 创建获取中国标准时间的函数
def get_cn_datetime():
    """获取当前的中国标准时间（东八区，UTC+8）"""
    return datetime.now(CST_TIMEZONE)

# 创建数据库引擎
engine = create_engine(
    settings.SQLALCHEMY_DATABASE_URI, 
    pool_pre_ping=True,
    # 必要时可以添加更多连接池设置
    pool_size=10,
    max_overflow=20,
    pool_recycle=3600,
    # 启用回显SQL语句，便于调试
    echo=False
)

# 创建数据库会话
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# 创建基本模型类
Base = declarative_base()

# 创建数据库和表
def init_db():
    """
    初始化数据库，如果表不存在则创建
    """

    # 创建所有表
    if settings.CREATE_TABLES:
        # 这里可以添加数据库初始化的代码
        # 检查数据库是否存在，如果不存在则创建（仅适用于MySQL）
        try:
            # 尝试连接到数据库
            db_uri = settings.SQLALCHEMY_DATABASE_URI
            # 从连接字符串中提取数据库名称
            db_name = settings.DB_NAME
            
            # 创建不指定数据库的连接URI
            server_uri = db_uri.rsplit('/', 1)[0]
            
            # 创建一个临时引擎来执行创建数据库的操作
            temp_engine = create_engine(server_uri)
            with temp_engine.connect() as connection:
                # 检查数据库是否存在（使用text()函数包装SQL语句）
                result = connection.execute(text(f"SHOW DATABASES LIKE '{db_name}'"))
                if not result.fetchone():
                    # 创建数据库（使用text()函数包装SQL语句）
                    connection.execute(text(f"CREATE DATABASE {db_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"))
                    print(f"数据库 {db_name} 已创建")
                else:
                    print(f"数据库 {db_name} 已存在")
            
            # 创建所有表
            Base.metadata.create_all(bind=engine)
            print("所有表已创建或已存在")
            
            # 这里可以添加初始数据的创建，例如默认角色和管理员用户
            db = SessionLocal()

            db.close()
                
        except Exception as e:
            print(f"初始化数据库时出错: {str(e)}")
            raise  # 重新抛出异常，以便在应用启动时捕获
    else:
        print("自动创建表功能已禁用")
