"""
知识图谱相关API接口模块

本模块提供了一套完整的知识图谱管理API接口，包括图谱的增删改查、任务管理以及文件管理等功能。
通过FastAPI框架实现RESTful API，支持异步处理和后台任务执行。
"""
import importlib.util
import json
import logging
import os
import subprocess
from typing import List

from dotenv import load_dotenv
# FastAPI核心组件
from fastapi import APIRouter, Depends, BackgroundTasks, UploadFile, File, Form
# SQLAlchemy数据库ORM
from sqlalchemy.orm import Session

# 数据库会话管理器
from app.db.session import get_db
from app.infrastructure.graph_storage.neo4j_adapter import Neo4jAdapter
# 统一响应格式工具
from app.infrastructure.response import success_response, error_response
# 数据传输对象定义
from app.schemas.kg import KGCreate, KGTaskCreate, KGTaskCreateByFile, KGSchema
# 核心业务服务层
from app.services.core.kg_service import kg_service
# 异步任务管理器
from app.services.tasks.kg_tasks import kg_task_manager

# 配置日志记录器
logger = logging.getLogger(__name__)

# 创建API路由实例
router = APIRouter()

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
env_path = os.path.join(project_root, '.env')
load_dotenv(env_path)
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://60.205.171.106:7687")
NEO4J_USERNAME = os.getenv("NEO4J_USERNAME", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "hit-wE8sR9wQ3pG1")
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE", "neo4j")


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
@router.post("/kgs/{kg_id}/tasks_batch")
async def create_kg_task_by_batch_filename(
        kg_id,  # 图谱ID参数，从URL路径中提取
        background_tasks: BackgroundTasks,  # 后台任务管理器
        task_data: str = Form(..., description="任务创建数据"),  # 任务创建数据模型
        files: List[UploadFile] | UploadFile = File(...),  # 上传文件列表
        db: Session = Depends(get_db),  # 数据库会话依赖注入
):
    """
    功能特例化：
    批量创建抽取任务，每个任务对应对应一个文件，每个任务的任务名为文件名，
    以便后续合并图谱时将节点与文件节点关联
    以此，json请求格式为：
    {
        "dir": "文件夹名称，即上传的所有文件所属类别",
        "prompt": "# 角色您是一个法律规章文本分析专家，能够从法律法规、规章制度等文本中准确提取实体、属性和关系信息，用于构建企业合规法务条令知识图谱。# 任务说明请从给定的法律法规文本中提取知识图谱所需的节点和关系，构建企业合规法务知识图谱。请严格按照下面的节点类型、属性和关系类型进行识别和提取， 并以JSON格式输出。",
        "schema": {
            "nodes": "## 法规### 介绍：表示一部具体的法律法规、规章或规范性文件### 属性：- 法规名称：法规的正式名称- 发布单位：发布该法规的行政机关或立法机关- 公布年份：法规公布的年月日- 实施年份：法规开始生效执行的年月日- 时效性：法规当前的状态，如'现行有效'、'已废止'、'已修订'- 法规层级：法规的法律效力层级，如'部门规章'、'行政法规'、'地方性法规'- 适用行业：法规适用的具体行业领域，如'安全生产'、'煤矿监察'- 法规方向：法规调整的法律关系方向，如'行政复议'、'行政许可'、'行政处罚'- 法规内容摘要：法规主要内容的简要概括## 条款### 介绍：法规中的具体条款或条文### 属性：- 条款编号：条款在法规中的编号，如'第五条'、'第十三条'- 所属法规：该条款所属的法规名称- 条款内容：条款的具体文本内容- 条款类型：条款的性质分类，如'总则'、'行政复议范围'、'程序规定'、'法律责任'等## 机构### 介绍：在行政复议中涉及的组织机构### 属性：- 机构名称：机构的正式全称- 机构类型：机构的性质分类，如'行政复议机关'、'监管部门'、'委托机构'- 层级：机构的行政层级，如'国家级'、'省级'、'市级'- 职责描述：机构在行政复议中的主要职责和权限## 行政行为### 介绍：安全监管监察部门作出的具体行政决定或措施### 属性：- 行为类型：行政行为的种类，如'行政处罚决定'、'行政强制措施'、'行政许可变更'- 行为依据条款：作出该行政行为所依据的法规条款- 行为对象：行政行为针对的对象，如'公民'、'法人'、'其他组织'- 是否可复议：该行政行为是否可以被申请行政复议## 申请主体### 介绍：有权提起行政复议的法律主体### 属性：- 主体类型：主体的法律性质，如'公民'、'法人'、'其他组织'- 申请事由：提起行政复议的具体原因- 是否具备利害关系：申请人与行政行为是否存在法律上的利害关系## 复议程序### 介绍：行政复议过程中的具体程序环节### 属性：- 程序类型：程序的种类，如'申请'、'受理'、'审理'、'决定'- 时限要求：程序环节的法定期限，如'3日'、'5日'、'7日'- 参与主体：参与该程序环节的主体- 程序依据条款：规定该程序的法规条款## 证据材料### 介绍：行政复议中使用的证明材料### 属性：- 证据类型：证据的形式种类，如'书证'、'物证'、'鉴定意见'- 提交主体：提供证据的主体，如'申请人'、'被申请人'、'第三人'- 是否必须提供：该证据是否必须由相关主体提供- 相关条款：规定该证据要求的法规条款## 文书### 介绍：行政复议过程中形成的法律文书### 属性：- 文书名称：文书的正式名称，如'行政复议决定书'、'听证笔录'- 文书用途：文书的主要作用和功能- 签发机构：制作和签发文书的机构- 相关条款：规定该文书的法规条款# 关系类型## 属于### 介绍：表示条款属于某一部法规的包含关系### 关系三元组：- 条款-属于-法规",
            "edges": "## 属于### 介绍：表示条款属于某一部法规的包含关系### 关系三元组：- 条款-属于-法规## 发布单位### 介绍：表示法规由某一机构发布或制定### 关系三元组：- 法规-发布单位-机构## 适用于### 介绍：表示法规适用于特定行业领域### 关系三元组：- 法规-适用于-行业（行业作为节点或属性值）## 依据### 介绍：表示行政行为基于法规条款作出### 关系三元组：- 行政行为-依据-条款## 可申请复议### 介绍：表示某一行政行为可以被特定主体申请行政复议### 关系三元组：- 行政行为-可申请复议-申请主体## 受理机关### 介绍：表示行政复议程序由特定机构受理### 关系三元组：- 复议程序-受理机关-机构## 提交证据### 介绍：表示在行政复议过程中主体提交证据材料### 关系三元组：- 申请主体-提交证据-证据材料- 被申请人-提交证据-证据材料## 使用文书### 介绍：表示在复议程序中使用特定法律文书### 关系三元组：- 复议程序-使用文书-文书## 引用条款### 介绍：表示文书或程序中引用了法规条款### 关系三元组：- 文书-引用条款-条款- 复议程序-引用条款-条款## 层级关系### 介绍：表示机构之间的行政隶属关系### 关系三元组：- 机构-上级机构-机构- 机构-下级机构-机构## 共同作出### 介绍：表示多个机构共同作出某一行政行为### 关系三元组：- 机构-共同作出-行政行为## 程序前后### 介绍：表示复议程序中各环节的时间顺序关系### 关系三元组：- 复议程序-前序程序-复议程序- 复议程序-后续程序-复议程序"
        },
        "examples": []
    }

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
        task_data = KGTaskCreateByFile(**task_data)
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
            kg_service.create_kg_task_by_file,
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


@router.post("/kgs/{kg_id}/tasks/{task_id}/merge_all")
async def merge_all_kg_task(
        kg_id,  # 图谱ID参数，从URL路径中提取
        db: Session = Depends(get_db),  # 数据库会话依赖注入
):
    """
    将已完成的图谱任务结果合并到主图谱中

    Args:
        kg_id (str/int): 知识图谱唯一标识符
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
        return await kg_service.merge_all_graph(
            kg_id=kg_id,
            db=db
        )
    except Exception as e:
        return error_response(
            msg=f"合并任务失败: {str(e)}",
            code=500,
            data=None
        )


# 将已完成的图谱任务结果合并到主图谱中，并将节点和文件节点关联，包括两步：
# 1. 获取与子图匹配的文件节点，供前端确认
# 2. 将单个子图与文件节点关联，合并图谱
# 3. 将所有子图与文件节点关联，合并图谱
@router.post("/kgs/{kg_id}/tasks/{task_id}/match_node")
async def match_node(
        kg_id,  # 图谱ID参数，从URL路径中提取
        task_id,  # 任务ID参数，从URL路径中提取
        db: Session = Depends(get_db),  # 数据库会话依赖注入
):
    """
    获取与子图匹配的文件节点，供前端确认

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
        return await kg_service.match_node_with_type(
            kg_id=kg_id,
            task_id=task_id,
            node_type="规章文件",
            db=db
        )
    except Exception as e:
        return error_response(
            msg=f"合并任务失败: {str(e)}",
            code=500,
            data=None
        )


# 合并图谱任务接口
@router.post("/kgs/{kg_id}/tasks/{task_id}/merge_with_match")
async def merge_with_match(
        kg_id,  # 图谱ID参数，从URL路径中提取
        task_id,  # 任务ID参数，从URL路径中提取
        db: Session = Depends(get_db),  # 数据库会话依赖注入
):
    """
    将单个子图与文件节点关联，合并图谱

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
        return await kg_service.merge_graph_with_match(
            kg_id=kg_id,
            task_id=task_id,
            db=db
        )
    except Exception as e:
        return error_response(
            msg=f"合并任务失败: {str(e)}",
            code=500,
            data=None
        )


@router.post("/kgs/{kg_id}/tasks/{task_id}/merge_all_with_match")
async def merge_all_with_match(
        kg_id,  # 图谱ID参数，从URL路径中提取
        db: Session = Depends(get_db),  # 数据库会话依赖注入
):
    """
    将单个子图与文件节点关联，合并图谱

    Args:
        kg_id (str/int): 知识图谱唯一标识符
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
        return await kg_service.merge_all_graph_with_match(
            kg_id=kg_id,
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


@router.post("/kgs/{kg_id}/kg_extract_by_dir")
async def kg_extract_by_local_dir(
        background_tasks: BackgroundTasks,  # 后台任务管理器
        data_dir: str,
        prompt: str,
        type: int = 1,
        db: Session = Depends(get_db),  # 数据库会话依赖注入
):
    """
    功能特例化：
    输入目录、提示词文件位置，
    以目录为基础创建一个kg，
    然后遍历目录下的每个文件，
    每个文件创建一个任务，
    目录下的文件对应任务全执行完后合并图谱

    以此，json请求格式为：
    {
        "data_dir": "文件位置",
        "prompt": "提示词位置"
    }

    Args:
        background_tasks (BackgroundTasks): FastAPI后台任务管理器
        data_dir (str): 本体抽取的文件目录位置
        prompt (str): 提示词文件位置
        type (int): 任务类型，默认为1
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
        # 检验参数
        file_dir = data_dir.replace('\\', '/')
        if not os.path.exists(file_dir):
            raise Exception(f"文件目录不存在: {file_dir}")
        if not os.path.isdir(file_dir):
            raise Exception(f"文件目录不是目录: {file_dir}")
        prompt_path = prompt.replace('\\', '/')
        if not os.path.exists(prompt_path):
            raise Exception(f"Python文件不存在: {prompt_path}")
        # 动态导入Python文件
        spec = importlib.util.spec_from_file_location("prompt_module", prompt_path)
        prompt_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(prompt_module)
        # 获取需要的变量（假设文件中有prompt、schema、examples等变量）
        prompt_dict = {
            'prompt': getattr(prompt_module, 'prompt', ''),
            'schema': getattr(prompt_module, 'schema', {}),
            'examples': getattr(prompt_module, 'examples', [])
        }
        # if not os.path.exists(prompt_path):
        #     raise Exception(f"提示词文件不存在: {prompt_path}")
        # prompt_dict = {}
        # try:
        #     with open(prompt_path, 'r', encoding='utf-8') as f:
        #         prompt_dict = json.load(f)
        # except FileNotFoundError:
        #     logger.error(f"提示词文件未找到: {prompt_path}")
        # except json.JSONDecodeError as e:
        #     logger.error(f"提示词文件格式错误: {str(e)}")
        # except Exception as e:
        #     logger.error(f"读取提示词文件时发生错误: {str(e)}")
        # if not prompt_dict:
        #     raise Exception(f"提示词文件格式错误: {prompt_path}")
        # if not prompt_dict.get("prompt"):
        #     raise Exception(f"提示词文件格式错误: {prompt_path}")
        # prompt_dict["schema"] = prompt_dict.get("schema", [])
        # prompt_dict["examples"] = prompt_dict.get("examples", [])
        # 创建kg
        # print(prompt_dict)
        new_kg = KGCreate(
            name=file_dir.split("/")[-1],
            description="",
        )
        kg_result = await kg_service.create_kg(new_kg, db)
        kg_id = kg_result.get("data").get("id")
        # kg_graph_name = kg_result.get("data").get("graph_name")
        # # 迁移图谱
        # neo4j_adapter = Neo4jAdapter()
        # neo4j_adapter.connect()
        # neo4j_adapter.merge_graphs("law_top_rules", kg_graph_name)
        # neo4j_adapter.disconnect()
        files = os.listdir(file_dir)
        file_contents = []
        for file in files:
            file_path = os.path.join(file_dir, file)
            # 只处理md或txt文件
            if file.endswith('.md') or file.endswith('.txt'):
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                file_contents.append({
                    'filename': file,
                    'content': content,
                    'content_type': 'text/markdown' if file.endswith('.md') else 'text/plain'
                })
        task_data = KGTaskCreateByFile(
            dir=file_dir.split("/")[-1],
            prompt=prompt_dict.get("prompt"),
            schema=KGSchema(**prompt_dict.get("schema")),
            examples=prompt_dict.get("examples")
        )
        if type == 1:
            background_tasks.add_task(
                kg_task_manager.run_async_function,
                kg_service.create_kg_task_by_file_with_merge,
                {"kg_id": kg_id, "task_data": task_data, "db": db, "file_contents": file_contents}
            )
        elif type == 2:
            background_tasks.add_task(
                kg_task_manager.run_async_function,
                kg_service.create_kg_task_by_file_with_merge_and_run_batch,
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


@router.post("/kgs/{kg_id}/law_kg_extract_by_dir")
async def kg_extract_by_local_dir(
        background_tasks: BackgroundTasks,  # 后台任务管理器
        data_dir: str,
        prompt: str,
        db: Session = Depends(get_db),  # 数据库会话依赖注入
):
    """
    功能特例化：
    输入目录、提示词文件位置，
    以目录为基础创建一个kg，
    然后遍历目录下的每个文件，
    每个文件创建一个任务，
    目录下的文件对应任务全执行完后合并图谱

    以此，json请求格式为：
    {
        "data_dir": "文件位置",
        "prompt": "提示词位置"
    }

    Args:
        background_tasks (BackgroundTasks): FastAPI后台任务管理器
        data_dir (str): 本体抽取的文件目录位置
        prompt (str): 提示词文件位置
        type (int): 任务类型，默认为1
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
        # 检验参数
        file_dir = data_dir.replace('\\', '/')
        if not os.path.exists(file_dir):
            raise Exception(f"文件目录不存在: {file_dir}")
        if not os.path.isdir(file_dir):
            raise Exception(f"文件目录不是目录: {file_dir}")
        prompt_path = prompt.replace('\\', '/')
        if not os.path.exists(prompt_path):
            raise Exception(f"Python文件不存在: {prompt_path}")
        # 动态导入Python文件
        spec = importlib.util.spec_from_file_location("prompt_module", prompt_path)
        prompt_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(prompt_module)
        # 获取需要的变量（假设文件中有prompt、schema、examples等变量）
        prompt_dict = {
            'prompt': getattr(prompt_module, 'prompt', ''),
            'schema': getattr(prompt_module, 'schema', {}),
            'examples': getattr(prompt_module, 'examples', [])
        }
        background_tasks.add_task(
            kg_task_manager.run_async_function,
            kg_service.kg_extract_by_local_dir,
            {"file_dir": file_dir, "prompt_dict": prompt_dict, "db": db}
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


"""
改进的针对法律知识图谱单门设计抽取流程的接口
"""

@router.post("/kgs/{kg_id}/clause_extract_by_dir")
async def clause_extract_by_dir(
        background_tasks: BackgroundTasks,  # 后台任务管理器
        data_dir: str,
        if_del_task: bool = False,
        db: Session = Depends(get_db),  # 数据库会话依赖注入
):
    """
基于单门的法规文件知识图谱抽取流程设计的接口
功能：对指定目录下的法规文件进行知识图谱抽取，以指定目录为总图谱kg，每个文件生成一个图谱task，if_del_task设置合并图谱时是否删除task，默认为False
1. 抽取流程核心代码：app/infrastructure/information_extraction/law_extract
2. 提示词文件位置：app/infrastructure/information_extraction/law_extract/prompt

以此，json请求格式为：
{
    "data_dir": "文件位置"
}

Args:
    background_tasks (BackgroundTasks): FastAPI后台任务管理器
    data_dir (str): 本体抽取的文件目录位置
    if_del_task (bool): 合并图谱到总图谱时是否删除task，默认为False
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
        background_tasks.add_task(
            kg_task_manager.run_async_function,
            kg_service.clause_extract_by_local_dir,
            {"clause_file_dir": data_dir, "if_del_task": if_del_task, "db": db}
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


