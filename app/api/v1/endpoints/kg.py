"""
知识图谱相关API接口模块

本模块提供了一套完整的知识图谱管理API接口，包括图谱的增删改查、任务管理以及文件管理等功能。
通过FastAPI框架实现RESTful API，支持异步处理和后台任务执行。
"""
import json
import logging
from typing import List

# FastAPI核心组件
from fastapi import APIRouter, Depends, BackgroundTasks, UploadFile, File, Form
# SQLAlchemy数据库ORM
from sqlalchemy.orm import Session

# 数据库会话管理器
from app.db.session import get_db
# 统一响应格式工具
from app.infrastructure.response import success_response, error_response
# 数据传输对象定义
from app.schemas.kg import KGCreate, KGTaskCreate
# 核心业务服务层
from app.services.core.kg_service import kg_service
# 异步任务管理器
from app.services.tasks.kg_tasks import kg_task_manager

# 配置日志记录器
logger = logging.getLogger(__name__)

# 创建API路由实例
router = APIRouter()


# 获取图谱列表接口
@router.get("/kgs")
async def get_kgs(
        db: Session = Depends(get_db),  # 数据库会话依赖注入
        page: int = 1,  # 页码，默认第1页
        limit: int = 10,  # 每页条数，默认10条
):
    """
    获取知识图谱列表

    Args:
        db (Session): 数据库会话对象，通过依赖注入自动获取
        page (int): 页码，从1开始，默认为1
        limit (int): 每页返回的记录数量，默认为10

    Returns:
        dict: 包含图谱列表和分页信息的成功响应
            {
                "code": 200,
                "msg": "success",
                "data": {
                    "items": [...],  # 图谱列表
                    "total": int,    # 总记录数
                    "page": int,     # 当前页码
                    "limit": int     # 每页条数
                }
            }

    Raises:
        Exception: 当获取图谱列表失败时返回错误响应
    """
    try:
        return await kg_service.get_kgs(db, page, limit)
    except Exception as e:
        return error_response(
            msg=f"获取图谱列表失败: {str(e)}",
            code=500,
            data=None
        )


# 获取图谱详情接口
@router.get("/kgs/{kg_id}")
async def get_kg(
        kg_id,  # 图谱ID参数，从URL路径中提取
        db: Session = Depends(get_db),  # 数据库会话依赖注入
):
    """
    根据ID获取知识图谱详情

    Args:
        kg_id (str/int): 知识图谱唯一标识符
        db (Session): 数据库会话对象，通过依赖注入自动获取

    Returns:
        dict: 包含图谱详细信息的成功响应
            {
                "code": 200,
                "msg": "success",
                "data": {...}  # 图谱详细信息
            }

    Raises:
        Exception: 当获取图谱详情失败时返回错误响应
    """
    try:
        return await kg_service.get_kg_detail_by_id(kg_id, db)
    except Exception as e:
        return error_response(
            msg=f"获取图谱详情失败: {str(e)}",
            code=500,
            data=None
        )


# 创建图谱接口
@router.post("/kgs")
async def create_kg(
        kg_data: KGCreate,  # 图谱创建请求体数据
        db: Session = Depends(get_db),  # 数据库会话依赖注入
):
    """
    创建新的知识图谱

    Args:
        kg_data (KGCreate): 图谱创建所需的数据模型，包含图谱名称、描述等信息
        db (Session): 数据库会话对象，通过依赖注入自动获取

    Returns:
        dict: 包含新创建图谱信息的成功响应
            {
                "code": 200,
                "msg": "创建成功",
                "data": {...}  # 新创建的图谱信息
            }

    Raises:
        Exception: 当创建图谱失败时返回错误响应
    """
    try:
        return await kg_service.create_kg(kg_data, db)
    except Exception as e:
        return error_response(
            msg=f"创建图谱失败: {str(e)}",
            code=500,
            data=None
        )


# 删除图谱接口
@router.delete("/kgs/{kg_id}")
async def delete_kg(
        kg_id,  # 图谱ID参数，从URL路径中提取
        db: Session = Depends(get_db),  # 数据库会话依赖注入
):
    """
    根据ID删除知识图谱

    Args:
        kg_id (str/int): 待删除的知识图谱唯一标识符
        db (Session): 数据库会话对象，通过依赖注入自动获取

    Returns:
        dict: 删除操作结果的响应
            {
                "code": 200,
                "msg": "删除成功",
                "data": null
            }

    Raises:
        Exception: 当删除图谱失败时返回错误响应
    """
    try:
        return await kg_service.delete_kg(kg_id, db)
    except Exception as e:
        return error_response(
            msg=f"删除图谱失败: {str(e)}",
            code=500,
            data=None
        )


# 获取图谱任务列表接口
@router.get("/kgs/{kg_id}/tasks")
async def get_kg_tasks(
        kg_id,  # 图谱ID参数，从URL路径中提取
        db: Session = Depends(get_db),  # 数据库会话依赖注入
        page: int = 1,  # 页码，默认第1页
        limit: int = 10,  # 每页条数，默认10条
):
    """
    获取指定知识图谱的任务列表

    Args:
        kg_id (str/int): 知识图谱唯一标识符
        db (Session): 数据库会话对象，通过依赖注入自动获取
        page (int): 页码，从1开始，默认为1
        limit (int): 每页返回的记录数量，默认为10

    Returns:
        dict: 包含任务列表和分页信息的成功响应
            {
                "code": 200,
                "msg": "success",
                "data": {
                    "items": [...],  # 任务列表
                    "total": int,    # 总记录数
                    "page": int,     # 当前页码
                    "limit": int     # 每页条数
                }
            }

    Raises:
        Exception: 当获取任务列表失败时返回错误响应
    """
    try:
        return await kg_service.get_kg_task_list(kg_id, db, page, limit)
    except Exception as e:
        return error_response(
            msg=f"获取图谱任务列表失败: {str(e)}",
            code=500,
            data=None
        )


# 获取图谱任务详情接口
@router.get("/kgs/{kg_id}/tasks/{task_id}")
async def get_kg_task(
        kg_id,  # 图谱ID参数，从URL路径中提取
        task_id,  # 任务ID参数，从URL路径中提取
        db: Session = Depends(get_db),  # 数据库会话依赖注入
):
    """
    获取指定知识图谱任务的详细信息

    Args:
        kg_id (str/int): 知识图谱唯一标识符
        task_id (str/int): 任务唯一标识符
        db (Session): 数据库会话对象，通过依赖注入自动获取

    Returns:
        dict: 包含任务详细信息的成功响应
            {
                "code": 200,
                "msg": "success",
                "data": {...}  # 任务详细信息
            }

    Raises:
        Exception: 当获取任务详情失败时返回错误响应
    """
    try:
        return await kg_service.get_kg_task_detail(kg_id, task_id, db)
    except Exception as e:
        return error_response(
            msg=f"获取图谱任务详情失败: {str(e)}",
            code=500,
            data=None
        )


# 获取图谱任务状态接口
@router.get("/kgs/{kg_id}/tasks/{task_id}")
async def get_kg_task_status(
        kg_id,  # 图谱ID参数，从URL路径中提取
        task_id,  # 任务ID参数，从URL路径中提取
        db: Session = Depends(get_db),  # 数据库会话依赖注入
):
    """
    获取指定知识图谱任务的执行状态

    Args:
        kg_id (str/int): 知识图谱唯一标识符
        task_id (str/int): 任务唯一标识符
        db (Session): 数据库会话对象，通过依赖注入自动获取

    Returns:
        dict: 包含任务状态信息的成功响应
            {
                "code": 200,
                "msg": "success",
                "data": {
                    "status": str,      # 任务状态
                    "progress": float,  # 任务进度(0-100)
                    ...
                }
            }

    Raises:
        Exception: 当获取任务状态失败时返回错误响应
    """
    try:
        return await kg_service.get_task_status(kg_id, task_id, db)
    except Exception as e:
        return error_response(
            msg=f"获取图谱任务状态失败: {str(e)}",
            code=500,
            data=None
        )


# 创建图谱任务接口
@router.post("/kgs/{kg_id}/tasks")
async def create_kg_task(
        kg_id,  # 图谱ID参数，从URL路径中提取
        background_tasks: BackgroundTasks,  # 后台任务管理器
        task_data: str = Form(..., description="任务创建数据"),  # 任务创建数据模型
        files: List[UploadFile] | UploadFile = File(...),  # 上传文件列表
        db: Session = Depends(get_db),  # 数据库会话依赖注入
):
    """
    为指定知识图谱创建新的任务（支持文件上传）

    Args:
        background_tasks (BackgroundTasks): FastAPI后台任务管理器
        kg_id (str/int): 知识图谱唯一标识符
        task_data (KGTaskCreate): 任务创建所需的数据模型
        files (List[UploadFile]): 上传的文件列表，必填项
        db (Session): 数据库会话对象，通过依赖注入自动获取

    Returns:
        dict: 任务创建结果的响应
            {
                "code": 200,
                "msg": "任务开始执行",
                "data": null
            }

    Raises:
        Exception: 当创建任务失败时返回错误响应
    """
    try:
        task_data = json.loads(task_data)
        task_data = KGTaskCreate(**task_data)
        if isinstance(files, UploadFile):
            files = [files]
        # 在这里就读取文件内容，避免在后台任务中读取已关闭的文件对象
        file_contents = []
        for file in files:
            content = await file.read()  # 立即读取文件内容
            file_contents.append({
                'filename': file.filename,
                'content': content,
                'content_type': file.content_type
            })
        background_tasks.add_task(
            kg_task_manager.run_async_function,
            kg_service.create_kg_task,
            {"kg_id": kg_id, "task_data": task_data, "db": db, "file_contents": file_contents}
        )
        return success_response(
            msg="任务开始执行",
            data=None
        )
    except Exception as e:
        return error_response(
            msg=f"执行任务失败: {str(e)}",
            code=500,
            data=None
        )


# 创建图谱任务接口
@router.post("/kgs/{kg_id}/tasks")
async def create_kg_task_by_batch_file_name(
        kg_id,  # 图谱ID参数，从URL路径中提取
        background_tasks: BackgroundTasks,  # 后台任务管理器
        task_data: str = Form(..., description="任务创建数据"),  # 任务创建数据模型
        files: List[UploadFile] | UploadFile = File(...),  # 上传文件列表
        db: Session = Depends(get_db),  # 数据库会话依赖注入
):
    """
    为指定知识图谱创建新的任务（支持文件上传）

    Args:
        background_tasks (BackgroundTasks): FastAPI后台任务管理器
        kg_id (str/int): 知识图谱唯一标识符
        task_data (KGTaskCreate): 任务创建所需的数据模型
        files (List[UploadFile]): 上传的文件列表，必填项
        db (Session): 数据库会话对象，通过依赖注入自动获取

    Returns:
        dict: 任务创建结果的响应
            {
                "code": 200,
                "msg": "任务开始执行",
                "data": null
            }

    Raises:
        Exception: 当创建任务失败时返回错误响应
    """
    try:
        task_data = json.loads(task_data)
        task_data = KGTaskCreate(**task_data)
        if isinstance(files, UploadFile):
            files = [files]
        # 在这里就读取文件内容，避免在后台任务中读取已关闭的文件对象
        file_contents = []
        for file in files:
            content = await file.read()  # 立即读取文件内容
            file_contents.append({
                'filename': file.filename,
                'content': content,
                'content_type': file.content_type
            })
        background_tasks.add_task(
            kg_task_manager.run_async_function,
            kg_service.create_kg_task,
            {"kg_id": kg_id, "task_data": task_data, "db": db, "file_contents": file_contents}
        )
        return success_response(
            msg="任务开始执行",
            data=None
        )
    except Exception as e:
        return error_response(
            msg=f"执行任务失败: {str(e)}",
            code=500,
            data=None
        )


# 重新执行图谱任务
@router.post("/kgs/{kg_id}/tasks/{task_id}/rerun")
async def rerun_kg_task(
        kg_id,  # 图谱ID参数，从URL路径中提取
        task_id,  # 任务ID参数，从URL路径中提取
        background_tasks: BackgroundTasks,  # 后台任务管理器
        db: Session = Depends(get_db),  # 数据库会话依赖注入
):
    try:
        background_tasks.add_task(
            kg_task_manager.run_async_function,
            kg_service.rerun_kg_task,
            {"kg_id": kg_id, "task_id": task_id, "db": db}
        )
        return success_response(
            msg="任务开始执行",
            data=None
        )
    except Exception as e:
        return error_response(
            msg=f"重新执行任务失败: {str(e)}",
            code=500,
            data=None
        )


# 合并图谱任务接口
@router.post("/kgs/{kg_id}/tasks/{task_id}/merge")
async def merge_kg_task(
        kg_id,  # 图谱ID参数，从URL路径中提取
        task_id,  # 任务ID参数，从URL路径中提取
        db: Session = Depends(get_db),  # 数据库会话依赖注入
):
    """
    将已完成的图谱任务结果合并到主图谱中

    Args:
        kg_id (str/int): 知识图谱唯一标识符
        task_id (str/int): 任务唯一标识符
        db (Session): 数据库会话对象，通过依赖注入自动获取

    Returns:
        dict: 合并操作结果的响应
            {
                "code": 200,
                "msg": "合并成功",
                "data": null
            }

    Raises:
        Exception: 当合并任务失败时返回错误响应
    """
    try:
        return await kg_service.merge_kg_task(
            kg_id=kg_id,
            task_id=task_id,
            merge_flag=True,
            db=db
        )
    except Exception as e:
        return error_response(
            msg=f"合并任务失败: {str(e)}",
            code=500,
            data=None
        )


# 删除图谱任务接口
@router.delete("/kgs/{kg_id}/tasks/{task_id}")
async def delete_kg_task(
        kg_id,  # 图谱ID参数，从URL路径中提取
        task_id,  # 任务ID参数，从URL路径中提取
        db: Session = Depends(get_db),  # 数据库会话依赖注入
):
    """
    删除指定的知识图谱任务

    Args:
        kg_id (str/int): 知识图谱唯一标识符
        task_id (str/int): 任务唯一标识符
        db (Session): 数据库会话对象，通过依赖注入自动获取

    Returns:
        dict: 删除操作结果的响应
            {
                "code": 200,
                "msg": "删除成功",
                "data": null
            }

    Raises:
        Exception: 当删除任务失败时返回错误响应
    """
    try:
        return await kg_service.delete_kg_task(kg_id, task_id, db)
    except Exception as e:
        return error_response(
            msg=f"删除任务失败: {str(e)}",
            code=500,
            data=None
        )


# 获取图谱抽取文件列表接口
@router.get("/kgs/{kg_id}/files")
async def get_kg_files(
        kg_id,  # 图谱ID参数，从URL路径中提取
        db: Session = Depends(get_db),  # 数据库会话依赖注入
        page: int = 1,  # 页码，默认第1页
        limit: int = 10,  # 每页条数，默认10条
):
    """
    获取知识图谱相关的抽取文件列表

    Args:
        kg_id (str/int): 知识图谱唯一标识符
        db (Session): 数据库会话对象，通过依赖注入自动获取
        page (int): 页码，从1开始，默认为1
        limit (int): 每页返回的记录数量，默认为10

    Returns:
        dict: 包含抽取文件列表和分页信息的成功响应
            {
                "code": 200,
                "msg": "success",
                "data": {
                    "items": [...],  # 文件列表
                    "total": int,    # 总记录数
                    "page": int,     # 当前页码
                    "limit": int     # 每页条数
                }
            }

    Raises:
        Exception: 当获取文件列表失败时返回错误响应
    """
    try:
        return await kg_service.get_kg_file_list(kg_id, db, page, limit)
    except Exception as e:
        return error_response(
            msg=f"获取图谱抽取文件列表失败: {str(e)}",
            code=500,
            data=None
        )


# 获取图谱任务抽取文件列表接口
@router.get("/kgs/{kg_id}/tasks/{task_id}/files")
async def get_kg_task_files(
        kg_id,  # 图谱ID参数，从URL路径中提取
        task_id,  # 任务ID参数，从URL路径中提取
        db: Session = Depends(get_db),  # 数据库会话依赖注入
        page: int = 1,  # 页码，默认第1页
        limit: int = 10,  # 每页条数，默认10条
):
    """
    获取特定任务的抽取文件列表

    Args:
        kg_id (str/int): 知识图谱唯一标识符
        task_id (str/int): 任务唯一标识符
        db (Session): 数据库会话对象，通过依赖注入自动获取
        page (int): 页码，从1开始，默认为1
        limit (int): 每页返回的记录数量，默认为10

    Returns:
        dict: 包含任务抽取文件列表和分页信息的成功响应
            {
                "code": 200,
                "msg": "success",
                "data": {
                    "items": [...],  # 文件列表
                    "total": int,    # 总记录数
                    "page": int,     # 当前页码
                    "limit": int     # 每页条数
                }
            }

    Raises:
        Exception: 当获取任务文件列表失败时返回错误响应
    """
    try:
        return await kg_service.get_kg_task_file_list(kg_id, task_id, db, page, limit)
    except Exception as e:
        return error_response(
            msg=f"获取图谱任务抽取文件列表失败: {str(e)}",
            code=500,
            data=None
        )
