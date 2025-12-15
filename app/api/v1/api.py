from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.api.v1.endpoints import kg


api_router = APIRouter()

# 包含各模块的路由

api_router.include_router(kg.router, prefix="/kg", tags=["知识图谱_重构版"])
