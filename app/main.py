from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import logging
import sys
import traceback
import json
from typing import Union, Dict, Any, Callable
import os

from app.api.v1.api import api_router
from app.core.config import settings
from app.db.base import init_db
from app.infrastructure.response import standard_response, error_response
from app.core.minio_client import initialize_minio

# 降低watchfiles日志级别，避免频繁输出
logging.getLogger('watchfiles').setLevel(logging.ERROR)
logging.getLogger('watchfiles.main').setLevel(logging.ERROR)
logging.getLogger('watchdog').setLevel(logging.ERROR)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title=settings.PROJECT_NAME,
    version="0.1.0",
    description="CogmAIt AI 模型管理API"
)

# 配置CORS - 重要: 必须在其他中间件之前添加
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 允许所有源
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"]
)

# 添加统一响应格式中间件
@app.middleware("http")
async def uniform_response_middleware(request: Request, call_next: Callable) -> Response:
    """
    统一响应格式中间件，包装所有API响应为统一的格式
    {
        "code": int,
        "data": Any,
        "msg": str
    }
    """
    # 对于非API路径的请求，不做处理
    if not request.url.path.startswith(settings.API_V1_STR):
        return await call_next(request)
    
    # 处理API路径的请求
    try:
        # 调用下一个处理程序
        response = await call_next(request)
        
        # 如果是OPTIONS请求（预检请求），直接返回
        if request.method == "OPTIONS":
            return response
        
        # 如果响应状态码是204或没有内容，直接返回
        if response.status_code == 204 or "content-length" not in response.headers or response.headers.get("content-length") == "0":
            return response
        
        # 如果不是JSON响应，直接返回（兼容含charset）
        content_type = response.headers.get("content-type", "")
        if not content_type.startswith("application/json"):
            return response
        
        # 仅当可直接获取到 body 时才包装；否则直接返回，避免阻塞
        body_bytes = getattr(response, "body", None)
        if body_bytes is None:
            return response
        
        # 解析JSON响应
        try:
            body = json.loads(body_bytes.decode())
        except json.JSONDecodeError:
            # 非JSON响应，直接返回不做处理
            return Response(
                content=body_bytes,
                status_code=response.status_code,
                headers=dict(response.headers),
                media_type=response.media_type
            )
        
        # 检查是否已经是标准格式
        if isinstance(body, dict) and "code" in body and "data" in body and "msg" in body:
            # 已经是标准格式，不需要再包装
            return Response(
                content=body_bytes,
                status_code=response.status_code,
                headers=dict(response.headers),
                media_type=response.media_type
            )
        
        # 错误响应（4xx, 5xx）
        if response.status_code >= 400:
            error_msg = body.get("detail", "请求失败") if isinstance(body, dict) else "请求失败"
            wrapped_response = error_response(
                msg=error_msg,
                code=response.status_code,
                data=body if isinstance(body, dict) else None
            )
            # 不保留原始响应头中的Content-Length，让FastAPI重新计算
            headers = dict(response.headers)
            if "content-length" in headers:
                del headers["content-length"]
                
            return JSONResponse(
                content=wrapped_response,
                status_code=200,  # 统一返回200状态码，错误信息在code字段中表示
                headers=headers
            )
        
        # 成功响应，包装为标准格式
        wrapped_response = standard_response(
            data=body,
            code=200,
            msg="操作成功"
        )
        
        # 不保留原始响应头中的Content-Length，让FastAPI重新计算
        headers = dict(response.headers)
        if "content-length" in headers:
            del headers["content-length"]
            
        return JSONResponse(
            content=wrapped_response,
            status_code=200,
            headers=headers
        )
    except Exception as e:
        # 处理中间件内部错误
        logger.error(f"中间件处理错误: {str(e)}")
        logger.error(traceback.format_exc())
        return JSONResponse(
            content=error_response(msg="服务器内部错误", code=500),
            status_code=200  # 统一返回200状态码
        )

# 包含API路由
app.include_router(api_router, prefix=settings.API_V1_STR)

@app.on_event("startup")
async def startup_db_client():
    """
    应用启动时初始化数据库
    """
    logger.info("正在初始化数据库...")
    try:
        init_db()
        logger.info("数据库初始化成功")
    except Exception as e:
        logger.error(f"数据库初始化失败: {str(e)}")
        # 打印详细的堆栈跟踪信息，便于调试
        logger.error(traceback.format_exc())
        # 对于致命错误，可以选择关闭应用，但通常我们希望应用能够继续运行
        # 如果没有数据库连接，应用的其他部分可能仍然可用
        logger.warning("应用将继续启动，但数据库功能可能不可用")

    # 初始化MinIO存储桶
    try:
        logger.info("初始化MinIO存储...")
        initialize_minio()
        logger.info("MinIO初始化完成")
    except Exception as e:
        logger.error(f"MinIO初始化失败: {str(e)}")

@app.get("/")
async def root():
    """健康检查接口"""
    return standard_response(
        data={
            "status": "online",
            "version": "0.1.0"
        },
        msg="CogmAIt API服务正在运行"
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True) 