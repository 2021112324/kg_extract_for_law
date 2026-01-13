import os
import re
from difflib import SequenceMatcher
from pathlib import Path
from typing import Optional, List

from dotenv import load_dotenv
from fastapi import HTTPException, UploadFile
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.infrastructure.graph_storage.factory import GraphStorageFactory
from app.infrastructure.response import success_response, not_found_response, error_response
from app.infrastructure.storage.object_storage import StorageFactory
from app.models.kg import KG as KGModel, KGExtractionTask, KGFile
from app.schemas.kg import KGCreate, KGTaskCreate, KGTaskCreateByFile
from app.services.ai.kg_extract_service import kg_extract_service
from app.utils.snowflake_id import generate_snowflake_string_id

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

env_path = os.path.join(project_root, '.env')
load_dotenv(env_path)

MINIO_BUCKET = os.getenv("MINIO_BUCKET", "law-kg")
KG_EXTRACT_METHOD = "langextract"

# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
#     handlers=[
#         logging.FileHandler("kg_service.log"),
#         logging.StreamHandler()
#     ]
# )
#
# # 创建 logger 实例
# logger = logging.getLogger(__name__)

# 图存储配置
GRAPH_TYPE = os.getenv("GRAPH_DB_TYPE", "neo4j")
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://60.205.171.106:7687")
NEO4J_USERNAME = os.getenv("NEO4J_USERNAME", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "hit-wE8sR9wQ3pG1")
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE", "neo4j")


class KGService:
    def __init__(self):
        self.graph_storage = GraphStorageFactory.create(
            GRAPH_TYPE,
            config={
                "uri": NEO4J_URI,
                "username": NEO4J_USERNAME,
                "password": NEO4J_PASSWORD,
                "database": NEO4J_DATABASE
            }
        )
        self.file_storage = StorageFactory.get_default_storage()
        self.file_storage.initialize()
        self.kg_extract_service = kg_extract_service

    @staticmethod
    async def get_kgs(
            db: Session,
            page: int = 1,
            limit: int = 10,
            name: Optional[str] = None,
    ):
        try:
            # 查询数据库中的知识图谱
            query = db.query(KGModel).filter(KGModel.del_flag == 0)
            # 如果有名称过滤条件
            if name:
                query = query.filter(KGModel.name.ilike(f"%{name}%"))
            # 计算总数
            total = query.count()
            # 分页查询
            query = query.offset((page - 1) * limit).limit(limit)
            # 获取结果
            result = [kg.to_dict() for kg in query.all()]
            kgs = []
            for kg in result:
                kgs.append({
                   "id": kg["id"],
                   "name": kg["name"],
                   "description": kg["description"],
                   "status": kg["status"],
                })
            return success_response(
                data={
                    "total": total,
                    "items": result
                },
                msg="获取知识图谱列表成功"
            )
        except Exception as e:
            raise e

    @staticmethod
    async def get_kg_detail_by_id(
            kg_id,
            db: Session,
    ):
        """
        获取知识图谱详情
        """
        try:
            # 从数据库查询指定ID的知识图谱
            kg = db.query(KGModel).filter(KGModel.id == kg_id, KGModel.del_flag == 0).first()

            if not kg:
                return not_found_response(
                    entity="知识图谱"
                )

            # 转换为字典并添加统计信息
            result = kg.to_dict()

            return success_response(
                data=result,
                msg="获取知识图谱详情成功"
            )
        except Exception as e:
            raise e

    @staticmethod
    async def create_kg(
            kg_data: KGCreate,
            db: Session,
    ):
        """
        创建知识图谱
        """
        try:
            # 创建新的知识图谱对象并添加到数据库
            new_kg = KGModel(
                name=kg_data.name,
                description=kg_data.description or "",
                entity_count=0,
                relation_count=0,
                config={},
                status=0,
                graph_status=0,  # 初始状态为pending，等待创建Neo4j子图
            )

            graph_name = generate_unique_name(f"{kg_data.name}_kg")
            new_kg.graph_name = graph_name

            # 添加到数据库
            db.add(new_kg)
            db.commit()
            # 刷新以获取自动生成的属性
            db.refresh(new_kg)

            kg_id = new_kg.id
            print(f"成功创建知识图谱: {kg_id}")
            # 返回创建的知识图谱
            return success_response(
                data=new_kg.to_dict(),
                msg="创建知识图谱成功"
            )
        except Exception as e:
            db.rollback()
            raise e

    @staticmethod
    async def get_kg_task_list(
            kg_id,
            db: Session,
            page: int = 1,
            limit: int = 10,
    ):
        """
        获取知识图谱任务列表

        Args:
            kg_id: 知识图谱ID
            page: 页码
            limit: 每页数量
            db: 数据库会话

        Returns:
            dict: 包含任务总数和任务列表的字典
        """
        try:
            # 验证知识图谱是否存在
            kg = db.query(KGModel).filter(KGModel.id == kg_id, KGModel.del_flag == 0).first()
            if not kg:
                return not_found_response(
                    entity="知识图谱"
                )

            # 查询该知识图谱下的任务总数
            total_query = db.query(KGExtractionTask).filter(KGExtractionTask.kg_id == kg_id, KGExtractionTask.del_flag == 0)
            total = total_query.count()

            # 分页查询任务列表
            tasks = total_query.offset((page - 1) * limit).limit(limit).all()

            # 转换为字典列表
            task_list = [task.to_dict() for task in tasks]

            tasks = []
            for task in task_list:
                tasks.append({
                    "id": task.get("id"),
                    "name": task.get("name"),
                    "description": task.get("description"),
                    "status": task.get("status"),
                })
            return success_response(
                data={
                    "total": total,
                    "items": tasks
                },
                msg="获取任务列表成功"
            )
        except Exception as e:
            raise e

    async def delete_kg(
            self,
            kg_id,
            db: Session,
    ):
        """
        删除知识图谱
        1. 查询数据库中是否有该图谱
        2. 遍历该图谱对应的每个任务，判断任务状态，若有任务状态为creating，则执行终止任务相关操作（该步骤暂时省略）
        3. 再次遍历每个任务，执行delete_kg_task操作
        4. 先后删除数据库中的schema_nodes、schema_edges和kgs中的相关数据
        """
        try:
            # 1. 查询数据库中是否有该图谱
            kg = db.query(KGModel).filter(KGModel.id == kg_id, KGModel.del_flag == 0).first()
            if not kg:
                return not_found_response(
                    entity="知识图谱",
                )
            # 2. 遍历该图谱对应的每个任务，判断任务状态，若有任务状态为creating，则执行终止任务相关操作（该步骤暂时省略）
            tasks = db.query(KGExtractionTask).filter(KGExtractionTask.kg_id == kg_id, KGExtractionTask.del_flag == 0).all()
            for task in tasks:
                # 判断任务状态
                if task.status == 1:     # 图谱状态：0-pending, 1-running, 2-completed, 3-merged, 4-failed, 5-cancelled
                    # TODO: 执行终止任务相关操作
                    print(f"任务 {task.id} 正在创建中，暂不支持终止操作")
                    return error_response(
                        code=409,
                        msg="任务正在创建中，暂不支持终止操作",
                        data=None
                    )
            # 3. 再次遍历每个任务，执行delete_kg_task操作
            for task in tasks:
                try:
                    task_result = await self.delete_kg_task(kg_id, task.id, db)
                    if task_result.get("code", 400) != 200:
                        return task_result
                except Exception as e:
                    return error_response(
                        code=500,
                        msg=f"删除任务 {task.id} 时出错: {str(e)}",
                        data=None
                    )
            # 4. 先后删除数据库中的schema_nodes、schema_edges和kgs中的相关数据
            # 方案二：置删除标志位
            kg.del_flag = 1
            # 提交事务
            db.commit()
            return success_response(
                data=None,
                msg=f"知识图谱 {kg.name} 已删除"
            )
        except HTTPException as e:
            db.rollback()
            raise e
        except Exception as e:
            db.rollback()
            raise Exception(f"删除知识图谱时出错: {str(e)}")

    async def delete_kg_task(
            self,
            kg_id,
            task_id,
            db: Session,
    ):
        """
        删除知识图谱任务任务编排：
        1. 查询数据库中是否有该任务
        2. 判断任务状态
        3. 若任务状态为creating，则执行终止任务相关操作（该步骤暂时省略）
        4. 删除图数据库中的数据，若删除删除失败，则返回
        5. 先后删除数据库中的kg_files和kg_extraction_tasks中的相关数据
        """
        try:
            # 1. 查询数据库中是否有该任务
            task = db.query(KGExtractionTask).filter(
                KGExtractionTask.id == task_id,
                KGExtractionTask.kg_id == kg_id,
                KGExtractionTask.del_flag == 0
            ).first()
            if not task:
                return not_found_response(
                    entity="任务",
                )
            # 2. 判断任务状态
            # 3. 若任务状态为running，则执行终止任务相关操作（该步骤暂时省略）
            if task.status == 1:        # 图谱状态：0-pending, 1-running, 2-completed, 3-merged, 4-failed, 5-cancelled
                # TODO: 执行终止任务相关操作
                return error_response(
                    code=409,
                    msg="任务正在创建中，暂不支持终止操作",
                    data=None
                )
            # 4. 删除图数据库中的数据，若删除失败，则返回
            if task.graph_name:
                # 连接图数据库
                if not self.graph_storage.connect():
                    return error_response(
                        msg="连接图数据库失败",
                        code=500,
                        data=None
                    )
                try:
                    # 删除图数据库中的子图数据
                    delete_result = self.graph_storage.delete_subgraph(task.graph_name)
                    if not delete_result:
                        return error_response(
                            msg="删除图数据库中的数据失败",
                            code=500,
                            data=None
                        )
                finally:
                    # 断开图数据库连接
                    self.graph_storage.disconnect()
            # 5. 先后删除数据库中的kg_files和kg_extraction_tasks中的相关数据
            # 先删除kg_files中的相关数据
            db.query(KGFile).filter(KGFile.task_id == task_id).delete()
            # # 再删除kg_extraction_tasks中的数据
            # db.delete(task)
            # 方案二：置删除标志位
            task.del_flag = 1
            # 提交事务
            db.commit()
            return success_response(
                data=None,
                msg=f"任务 {task_id} 已删除"
            )
        except HTTPException as e:
            db.rollback()
            raise e
        except Exception as e:
            db.rollback()
            raise Exception(f"删除知识图谱任务时出错: {str(e)}")

    async def create_kg_task(
            self,
            kg_id,
            task_data: KGTaskCreate,
            file_contents: List[dict],
            db: Session,
    ):
        """
        创建知识图谱任务
        """
        try:
            # 验证知识图谱是否存在
            kg = db.query(KGModel).filter(KGModel.id == kg_id, KGModel.del_flag == 0).first()
            if not kg:
                return not_found_response(
                    entity="知识图谱"
                )

            # 生成任务的子图名称
            graph_name = generate_unique_name(f"{kg.name}_task")
            parameters = {
                "schema": task_data.schema.model_dump(),
                "examples": task_data.examples,
            }

            # 创建新的任务对象
            new_task = KGExtractionTask(
                kg_id=kg_id,
                name=task_data.name,
                description=task_data.description,
                prompt=task_data.prompt,
                parameters=parameters,
                graph_name=graph_name,
            )

            # 添加到数据库
            db.add(new_task)
            db.flush()  # 刷新以获取任务ID

            # 关联文件
            if file_contents:
                for file_content in file_contents:
                    # 上传文件
                    minio_bucket = MINIO_BUCKET
                    minio_name = generate_unique_name("kg_task_file")
                    content = file_content['content']
                    content_type = file_content.get('content_type', 'application/octet-stream')
                    filename = file_content.get('filename', 'unknown')
                    try:
                        # 修复：确保传入的是字节流而不是字符串
                        if isinstance(content, str):
                            content = content.encode('utf-8')
                        self.file_storage.upload_file_object(
                            file_data=content,
                            bucket_name=minio_bucket,
                            object_name=minio_name,
                            content_type=content_type
                        )
                    except Exception as e:
                        print(f"{filename}文件上传出现问题，请检查！" + e)
                        continue

                    # 创建KGFile关联记录
                    kg_file = KGFile(
                        kg_id=kg_id,
                        task_id=new_task.id,
                        minio_filename=minio_name,
                        filename=filename,
                        minio_bucket=minio_bucket,
                        minio_path=minio_bucket + '/' + minio_name
                    )
                    db.add(kg_file)
            else:
                raise Exception("请上传文件")

            new_task.status = 1

            # 提交所有更改
            db.commit()
            db.refresh(new_task)

            # 执行任务
            result = await self.execute_kg_task(kg_id, new_task.id)

            if result.get("code") == 200:
                return success_response(
                    data=new_task.to_dict(),
                    msg="创建任务成功"
                )
            else:
                return error_response(
                    code=500,
                    msg="任务执行失败",
                    data=None
                )
        except Exception as e:
            db.rollback()
            raise e

    async def create_kg_task_by_file(
            self,
            kg_id,
            task_data: KGTaskCreateByFile,
            file_contents: List[dict],
            db: Session,
    ):
        """
        创建知识图谱任务
        """
        kg = db.query(KGModel).filter(KGModel.id == kg_id, KGModel.del_flag == 0).first()
        if not kg:
            return not_found_response(
                entity="知识图谱"
            )
        error_file_list = []
        for file_content in file_contents:
            try:
                file_name = file_content['filename']
                task_name = file_name
                task_description = f"{task_data.dir}-{task_name}"
                existed_task = db.query(KGExtractionTask).filter(
                    KGExtractionTask.name == task_name,
                    KGExtractionTask.kg_id == kg_id,
                    KGExtractionTask.del_flag == 0
                ).first()
                if existed_task:
                    continue
                one_task_data = KGTaskCreate(
                    name=task_name,
                    description=task_description,
                    prompt=task_data.prompt,
                    schema=task_data.schema,
                    examples=task_data.examples
                )
                result = await self.create_kg_task(kg_id, one_task_data, [file_content], db)
                if result.get("code") != 200:
                    raise Exception(f"{file_name}对应的任务抽取时出错: {str(e)}")
            except Exception as e:
                db.rollback()
                error_file_list.append(file_name)
                print(f"Error: {file_name}文件处理出现问题，请检查！" + str(e))
                continue

        # 使用项目根目录的相对路径
        error_file_path = project_root + "/tests/error_file_list.txt"

        try:
            with open(error_file_path, "w", encoding="utf-8") as f:
                f.write("\n".join(error_file_list))
                print(f"已输出错误文件列表到 {error_file_path}")
        except IOError as e:
            print(f"写入错误文件列表失败: {e}")
        return success_response(
            data=None,
            msg="任务执行完毕"
        )

    async def rerun_kg_task(
            self,
            kg_id,
            task_id,
            db: Session,
    ):
        """
        重新执行知识图谱任务
        """
        try:
            task = db.query(KGExtractionTask).filter(KGExtractionTask.id == task_id, KGExtractionTask.del_flag == 0).first()
            if not task:
                return not_found_response(
                    entity="任务"
                )
            if task.status == 1:
                return error_response(
                    msg="任务状态异常",
                    code=500,
                    data=None
                )
            task.status = 1
            db.commit()
            return await self.execute_kg_task(kg_id, task_id)
        except Exception as e:
            db.rollback()
            raise e

    async def execute_kg_task(
            self,
            kg_id,
            task_id,
            kg_level: str = "DomainLevel"
    ):
        """
        执行知识图谱任务编排
        1. 查询任图谱是否存在
        2. 获取图谱模型id
        3. 查询任务是否存在
        4. 获取任务对应的md文件路径列表
        5. 判断文件列表中各文件是否存在minio中的md文件
        6. 执行抽取任务
        7. 将抽取出的图谱保存到图数据库中
        8. 更新任务数据库数据

        Args:
            kg_id: 知识图谱ID
            task_id: 任务ID
            kg_level: 知识图谱类型：
                        如果是文件级别，则每个文件保存一次图谱；
                        如果是领域级别，则合并后保存图谱

        Returns:
            dict: 操作结果
        """
        # 在函数内部创建新的数据库会话
        db_gen = get_db()
        db = next(db_gen)
        result = {}
        try:
            # 1. 查询任图谱是否存在
            kg = db.query(KGModel).filter(KGModel.id == kg_id, KGModel.del_flag == 0).first()
            if not kg:
                return not_found_response(
                    entity="知识图谱",
                )
            # 3. 查询任务是否存在
            task = db.query(KGExtractionTask).filter(KGExtractionTask.id == task_id, KGExtractionTask.del_flag == 0).first()
            if not task:
                return not_found_response(
                    entity="任务",
                )
            # 获取任务图谱名称
            graph_name = task.graph_name
            # 获取任务提示词
            user_prompt = task.prompt
            prompt_parameters = {}
            # 示例
            # examples = []
            if KG_EXTRACT_METHOD == "langextract":
                # langextract需要提示词
                examples = task.parameters.get("examples", [])
                prompt_parameters["examples"] = examples
                kg_schema = task.parameters.get("schema", [])
                prompt_parameters["schema"] = kg_schema
            # 4. 获取任务对应的md文件路径列表
            task_files = db.query(KGFile).filter(KGFile.kg_id == kg_id, KGFile.task_id == task_id).all()
            # 优化：将抽取结果与文件绑定
            if task_files:
                minio_files = []
                for file in task_files:
                    file_minio_path = file.minio_path
                    filename = file.filename
                    if not file_minio_path or not filename:
                        continue
                    minio_files.append(
                        {
                            "minio_path": file_minio_path,
                            "filename": filename
                        }
                    )
            else:
                minio_files = []
            # 5. 判断文件列表中各文件是否存在minio中的md文件
            for minio_file in minio_files:
                minio_path = minio_file.get("minio_path", "")
                if not await self._is_md_exist_in_minio(minio_path):
                    return not_found_response(
                        entity="文件",
                    )
            if task.status == 4:        # 图谱状态：0-pending, 1-running, 2-completed, 3-merged, 4-failed, 5-cancelled
                task.retry_count = task.retry_count + 1
            try:
                # 6.执行抽取任务
                result = await self.kg_extract_service.extract_kg_from_minio_paths(
                    minio_files=minio_files,
                    user_prompt=user_prompt,
                    prompt_parameters=prompt_parameters,
                    kg_level=kg_level
                )
                # print("debug:" + str(result))
                try:
                    # 7. 将抽取出的图谱保存到图数据库中
                    if result:

                        # TODO: 保留图谱节点文件来源？

                        await self.save_kg_result_to_storage(
                            result=result,
                            graph_name=graph_name,
                            kg_level=kg_level
                        )

                        # 只有在result不为None时才设置计数
                        task.status = 2  # 图谱状态：0-pending, 1-running, 2-completed, 3-merged, 4-failed, 5-cancelled
                        task.entity_count = len(result.get("nodes", []))
                        task.relation_count = len(result.get("edges", []))
                    else:
                        task.status = 2  # 图谱状态：0-pending, 1-running, 2-completed, 3-merged, 4-failed, 5-cancelled
                        task.entity_count = 0
                        task.relation_count = 0
                except Exception as e:
                    print(f"保存图谱数据时出错: {str(e)}")
                    raise e
                # task.message = "任务执行完成"
                # task.progress = 100
            except Exception as e:
                print(f"执行任务时出错: {str(e)}")
                task.status = 4             # 图谱状态：0-pending, 1-running, 2-completed, 3-merged, 4-failed, 5-cancelled
                # task.message = "任务执行失败"
                raise e
            finally:
                # 8. 更新任务数据库数据
                # task.result = ""              #TODO: 保存抽取日志？
                try:
                    db.commit()
                except Exception as e:
                    db.rollback()
                    print(f"提交数据库事务时出错: {str(e)}")
                    raise e
        except Exception as e:
            db.rollback()
            print(f"回滚数据库事务时出错: {str(e)}")
            raise e
        finally:
            # 确保数据库会话被正确关闭
            try:
                db.close()
            except Exception as e:
                print(f"关闭数据库会话时出错: {str(e)}")
            # 关闭生成器
            try:
                next(db_gen)
            except StopIteration:
                pass  # 这是正常情况，生成器已耗尽
        return success_response(
            data=result,
            msg="任务执行完毕"
        )

    @staticmethod
    async def get_kg_task_detail(
            kg_id,
            task_id,
            db: Session,
    ):
        """
        获取知识图谱任务详情
        """
        try:
            task = db.query(KGExtractionTask).filter(
                KGExtractionTask.kg_id == kg_id,
                KGExtractionTask.id == task_id,
                KGExtractionTask.del_flag == 0
            ).first()
            if not task:
                return not_found_response(
                    entity="任务"
                )
            return success_response(
                data=task.to_dict(),
                msg="获取任务详情成功"
            )
        except Exception as e:
            raise e

    @staticmethod
    async def get_kg_file_list(
            kg_id,
            db: Session,
            page: int = 1,
            limit: int = 10,
    ):
        """
        获取知识图谱关联文件列表
        """
        try:
            kg = db.query(KGModel).filter(KGModel.id == kg_id, KGModel.del_flag == 0).first()
            if not kg:
                return not_found_response(
                    entity="知识图谱"
                )
            # 查询关联文件总数
            total_query = db.query(KGFile).filter(KGFile.kg_id == kg_id)
            # 分页查询文件列表
            files = total_query.offset((page - 1) * limit).limit(limit).all()
            total = len(files)
            # 转换为字典列表
            file_list = [file.to_dict() for file in files]
            return success_response(
                data={
                    "total": total,
                    "items": file_list
                },
                msg="获取文件列表成功"
            )
        except Exception as e:
            raise e

    @staticmethod
    async def get_kg_task_file_list(
            kg_id,
            task_id,
            db: Session,
            page: int = 1,
            limit: int = 10,
    ):
        """
        获取知识图谱任务关联文件列表
        """
        try:
            task = db.query(KGExtractionTask).filter(
                KGExtractionTask.kg_id == kg_id,
                KGExtractionTask.id == task_id,
                KGExtractionTask.del_flag == 0
            ).first()
            if not task:
                return not_found_response(
                    entity="任务"
                )
            total_query = db.query(KGFile).filter(KGFile.kg_id == kg_id, KGFile.task_id == task_id)
            files = total_query.offset((page - 1) * limit).limit(limit).all()
            total = len(files)
            file_list = [file.to_dict() for file in files]
            return success_response(
                data={
                    "total": total,
                    "items": file_list
                },
                msg="获取文件列表成功"
            )
        except Exception as e:
            raise e

    @staticmethod
    async def get_task_status(
            kg_id,
            task_id,
            db: Session,
    ):
        """
        获取任务执行状态
        """
        kg = db.query(KGModel).filter(KGModel.id == kg_id, KGModel.del_flag == 0).first()
        if not kg:
            return not_found_response(
                entity="知识图谱"
            )
        task = db.query(KGExtractionTask).filter(
            KGExtractionTask.kg_id == kg_id,
            KGExtractionTask.id == task_id,
            KGExtractionTask.del_flag == 0
        ).first()
        if not task:
            return not_found_response(
                entity="任务"
            )
        return success_response(
            data={
                "status": task.status,
                "message": task.message
            },
            msg="获取任务状态成功"
        )

    async def save_kg_result_to_storage(
            self,
            result: dict | list[dict],
            graph_name: str,
            kg_level: str = "DomainLevel",
    ) -> bool:
        """
        将图谱保存到图数据库中
        :param result:
        :param graph_name:
        :param kg_level:
        :return:
        """
        if kg_level == "DomainLevel":
            if not isinstance(result, dict):
                raise ValueError("参数result必须是列表")
            # TODO：生成文本节点，并建立节点关系
            kg_data = self.process_source_text(result)
            self.graph_storage.connect()
            self.graph_storage.add_subgraph_with_merge(
                kg_data=kg_data,
                graph_tag=graph_name,
                graph_level="DomainLevel"
            )
            self.graph_storage.disconnect()
        elif kg_level == "DocumentLevel":
            if not isinstance(result, list):
                raise ValueError("参数result必须是列表")
            self.graph_storage.connect()
            for kg_data in result:
                filename = kg_data.get("filename")
                if not filename:
                    filename = ""
                # TODO: 处理文本溯源
                self.graph_storage.add_subgraph_with_merge(
                    kg_data=kg_data,
                    graph_tag=graph_name,
                    graph_level="DocumentLevel",
                    filename=filename
                )
            self.graph_storage.disconnect()

    async def merge_kg_task(
            self,
            kg_id,
            task_id,
            merge_flag: bool,
            db: Session,
    ):
        """
        合并知识图谱任务
        """
        try:
            kg = db.query(KGModel).filter(KGModel.id == kg_id, KGModel.del_flag == 0).first()
            if not kg:
                return not_found_response(
                    entity="知识图谱"
                )
            task = db.query(KGExtractionTask).filter(
                KGExtractionTask.kg_id == kg_id,
                KGExtractionTask.id == task_id,
                KGExtractionTask.del_flag == 0
            ).first()
            if not task:
                return not_found_response(
                    entity="任务"
                )
            if task.status != 2:        # 图谱状态：0-pending, 1-running, 2-completed, 3-merged, 4-failed, 5-cancelled
                return error_response(
                    code=400,
                    msg="任务未完成，请等待任务完成"
                )
            if not merge_flag:
                task.status = 5         # 图谱状态：0-pending, 1-running, 2-completed, 3-merged, 4-failed, 5-cancelled
                db.commit()
                return success_response(
                    data={
                        "status": task.status,
                    },
                    msg="取消合并图谱"
                )
            if not task.graph_name:
                return not_found_response(
                    entity="任务图谱"
                )
            if not kg.graph_name:
                return not_found_response(
                    entity="总图谱"
                )
            self.graph_storage.connect()
            result = self.graph_storage.merge_graphs(
                task.graph_name,
                kg.graph_name,
            )
            self.graph_storage.disconnect()
            return success_response(
                data=result.model_dump(),
                msg="合并图谱成功"
            )
        except Exception as e:
            raise e

    async def match_node_with_type(
            self,
            kg_id,
            task_id,
            node_type: str,
            db: Session,
    ):
        """
        合并知识图谱任务
        """
        try:
            kg = db.query(KGModel).filter(KGModel.id == kg_id, KGModel.del_flag == 0).first()
            if not kg:
                return not_found_response(
                    entity="知识图谱"
                )
            task = db.query(KGExtractionTask).filter(
                KGExtractionTask.kg_id == kg_id,
                KGExtractionTask.id == task_id,
                KGExtractionTask.del_flag == 0
            ).first()
            if not task:
                return not_found_response(
                    entity="任务"
                )
            if task.status != 2:        # 图谱状态：0-pending, 1-running, 2-completed, 3-merged, 4-failed, 5-cancelled
                return error_response(
                    code=400,
                    msg="任务未完成，请等待任务完成"
                )
            if not task.graph_name:
                return not_found_response(
                    entity="任务图谱"
                )
            if not kg.graph_name:
                return not_found_response(
                    entity="总图谱"
                )
            self.graph_storage.connect()
            result = self.graph_storage.get_nodes_by_type(
                kg.graph_name,
                node_type
            )
            self.graph_storage.disconnect()
            node_list = [node.name for node in result]
            # 获取task.name中第一个"."前的内容，如果没有"."，则获取全部内容
            source_name = task.name.split('.')[0] if '.' in task.name else task.name
            matched_node = self.find_best_match(self.sanitize_neo4j_property_name(source_name), node_list)
            if matched_node:
                matched_node_id = None
                for node in result:
                    if node.name == matched_node:
                        matched_node_id = node.id
                        break
                if matched_node_id:
                    return success_response(
                        data={
                            "matched_node": matched_node,
                            "matched_node_id": matched_node_id
                        },
                        msg="匹配成功"
                    )
            return error_response(
                code=404,
                msg="未找到匹配的节点"
            )
        except Exception as e:
            raise e

    async def merge_graph_with_match(
            self,
            kg_id,
            task_id,
            db: Session,
    ):
        """
        将单个子图合并到总图谱

        :param kg_id:
        :param task_id:
        :param db:
        :return:
        """
        try:
            kg = db.query(KGModel).filter(KGModel.id == kg_id, KGModel.del_flag == 0).first()
            if not kg:
                return not_found_response(
                    entity="知识图谱"
                )
            task = db.query(KGExtractionTask).filter(
                KGExtractionTask.kg_id == kg_id,
                KGExtractionTask.id == task_id,
                KGExtractionTask.del_flag == 0
            ).first()
            if not task:
                return not_found_response(
                    entity="任务"
                )
            matched_node_result = await self.match_node_with_type(
                kg_id,
                task_id,
                "规章文件",
                db
            )
            matched_node_id = matched_node_result.get("data").get("matched_node_id")
            if not matched_node_id:
                return error_response(
                    code=400,
                    msg="未找到匹配的节点"
                )
            self.graph_storage.connect()
            result = self.graph_storage.merge_graphs_with_match_node(
                task.graph_name,
                kg.graph_name,
                matched_node_id
            )
            self.graph_storage.disconnect()
            if not result:
                return error_response(
                    code=400,
                    msg="合并图谱失败"
                )
            return success_response(
                data=result.model_dump(),
                msg="合并图谱成功"
            )
        except Exception as e:
            raise e

    async def merge_all_graph_with_match(
            self,
            kg_id,
            db: Session,
    ):
        """
        将所有子图合并到总图谱

        :param kg_id:
        :param db:
        :return:
        """
        try:
            kg = db.query(KGModel).filter(KGModel.id == kg_id, KGModel.del_flag == 0).first()
            if not kg:
                return not_found_response(
                    entity="知识图谱"
                )
            task_list = db.query(KGExtractionTask).filter(
                KGExtractionTask.kg_id == kg_id,
                KGExtractionTask.del_flag == 0
            ).all()
            error_list = []
            for task in task_list:
                matched_node_result = await self.match_node_with_type(
                    kg_id,
                    task.id,
                    "规章文件",
                    db
                )
                if matched_node_result.get("code") != 200 or not matched_node_result.get("data"):
                    error_list.append(task.id)
                    continue
                matched_node_id = matched_node_result.get("data").get("matched_node_id")
                if not matched_node_id:
                    return error_response(
                        code=400,
                        msg="未找到匹配的节点"
                    )
                self.graph_storage.connect()
                result = self.graph_storage.merge_graphs_with_match_node(
                    task.graph_name,
                    kg.graph_name,
                    matched_node_id
                )
                self.graph_storage.disconnect()
                if result.error:
                    error_list.append(task.id)
                    continue
            # 使用项目根目录的相对路径
            error_file_path = project_root + "/tests/error_task_list.txt"

            try:
                with open(error_file_path, "w", encoding="utf-8") as f:
                    f.write("\n".join(str(task_id) for task_id in error_list))
                    print(f"已输出错误任务列表到 {error_file_path}")
            except IOError as e:
                print(f"写入错误任务列表失败: {e}")
            self.graph_storage.connect()
            graph_status = self.graph_storage.get_subgraph_stats(kg.graph_name)
            return success_response(
                data={
                    "graph_status": graph_status,
                    "error_list": error_list
                },
                msg="合并图谱成功"
            )
        except Exception as e:
            raise e

    async def merge_all_graph(
            self,
            kg_id,
            db: Session,
    ):
        """
        将所有子图合并到总图谱

        :param kg_id:
        :param db:
        :return:
        """
        try:
            kg = db.query(KGModel).filter(KGModel.id == kg_id, KGModel.del_flag == 0).first()
            if not kg:
                return not_found_response(
                    entity="知识图谱"
                )
            task_list = db.query(KGExtractionTask).filter(
                KGExtractionTask.kg_id == kg_id,
                KGExtractionTask.del_flag == 0
            ).all()
            error_list = []
            for task in task_list:
                self.graph_storage.connect()
                result = self.graph_storage.merge_graphs(
                    task.graph_name,
                    kg.graph_name,
                )
                if not result:
                    return error_response(
                        code=400,
                        msg="合并图谱失败"
                    )
                self.graph_storage.delete_subgraph(task.graph_name)
                self.graph_storage.disconnect()
                if result.error:
                    error_list.append(task.id)
                    continue
            # 使用项目根目录的相对路径
            error_file_path = project_root + "/tests/error_task_list.txt"

            try:
                with open(error_file_path, "w", encoding="utf-8") as f:
                    f.write("\n".join(str(task_id) for task_id in error_list))
                    print(f"已输出错误任务列表到 {error_file_path}")
            except IOError as e:
                print(f"写入错误任务列表失败: {e}")
            self.graph_storage.connect()
            graph_status = self.graph_storage.get_subgraph_stats(kg.graph_name)
            return success_response(
                data={
                    "graph_status": graph_status,
                    "error_list": error_list
                },
                msg="合并图谱成功"
            )
        except Exception as e:
            raise e

    async def create_kg_task_by_file_with_merge(
            self,
            kg_id,
            task_data: KGTaskCreateByFile,
            file_contents: List[dict],
            db: Session,
    ):
        """
        创建知识图谱任务
        """
        kg = db.query(KGModel).filter(KGModel.id == kg_id, KGModel.del_flag == 0).first()
        if not kg:
            return not_found_response(
                entity="知识图谱"
            )
        error_file_list = []
        for file_content in file_contents:
            try:
                file_name = file_content.get("filename")
                if not file_name:
                    continue
                task_name = os.path.splitext(file_name)[0]
                task_description = f"{task_data.dir}-{task_name}"
                existed_task = db.query(KGExtractionTask).filter(
                    KGExtractionTask.name == task_name,
                    KGExtractionTask.kg_id == kg_id,
                    KGExtractionTask.del_flag == 0
                ).first()
                if existed_task:
                    continue
                one_task_data = KGTaskCreate(
                    name=task_name,
                    description=task_description,
                    prompt=task_data.prompt,
                    schema=task_data.schema,
                    examples=task_data.examples
                )
                result = await self.create_kg_task(kg_id, one_task_data, [file_content], db)
                if result.get("code") != 200:
                    raise Exception(f"{file_name}对应的任务抽取时出错: {str(e)}")
            except Exception as e:
                db.rollback()
                error_file_list.append(file_name)
                print(f"Error: {file_name}文件处理出现问题，请检查！" + str(e))
                continue

        # 使用项目根目录的相对路径
        error_file_path = project_root + "/tests/error_file_list.txt"

        try:
            with open(error_file_path, "w", encoding="utf-8") as f:
                f.write("\n".join(str(file_name) for file_name in error_file_list))
                print(f"已输出错误文件列表到 {error_file_path}")
        except IOError as e:
            print(f"写入错误文件列表失败: {e}")

        # merge_result = await self.merge_all_graph_with_match(kg_id, db)
        merge_result = await self.merge_all_graph(kg_id, db)
        if merge_result.get("code") != 200:
            return merge_result
        return success_response(
            data=None,
            msg="任务执行完毕"
        )

    async def _is_md_exist_in_minio(
            self,
            minio_path: str,
    ):
        """
        判断文件是否存在minio中
        """
        try:
            # 解析路径，分离存储桶和文件名
            if '/' in minio_path:
                bucket_name, file_name = minio_path.split('/', 1)
            else:
                # 如果没有'/'分隔符，则使用默认存储桶
                bucket_name = MINIO_BUCKET
                file_name = minio_path
            # 使用file_storage检查文件是否存在
            return self.file_storage.file_exists(bucket_name, file_name)
        except Exception as e:
            print(f"检查文件是否存在时出错: {str(e)}")
            return False

    def process_source_text(
            self,
            kg_data: dict,
    ):
        """
        将kg_data中的溯源文本数据处理成节点和关系
        :param kg_data:
        :return:
        """
        if not kg_data:
            return {}
        text_classes = kg_data.get("text_classes")
        if not text_classes:
            return kg_data
        text_id_set = set()
        new_kg_data = {
            "nodes": [],
            "edges": []
        }
        for text_class in text_classes:
            text_id = text_class.get("text_id")
            if not text_id:
                continue
            new_text_id = f"text_{text_id}"
            text_id_set.add(new_text_id)
            text_filename = text_class.get("filename")
            while isinstance(text_filename, list) and len(text_filename) > 0:
                text_filename = text_filename[0]
                # 如果最终还是列表但长度为0，则设置为空字符串
            if isinstance(text_filename, list):
                text_filename = ""
            temp_text_filename = self.sanitize_neo4j_filename(text_filename)
            new_kg_data["nodes"].append(
                {
                    "node_id": new_text_id,
                    "node_name": temp_text_filename,
                    "node_type": "SourceText",
                    "properties": {
                        "text": text_class.get("text")
                    }
                }
            )
        nodes = kg_data.get("nodes")
        edges = kg_data.get("edges")
        if not isinstance(nodes, list) or not isinstance(edges, list):
            return {}
        for node in nodes:
            node_id = node.get("node_id")
            node_name = node.get("node_name")
            node_type = node.get("node_type")
            if not node_id or not node_type:
                continue
            properties = node.get("properties")
            description = node.get("description")
            new_kg_data["nodes"].append(
                {
                    "node_id": node_id,
                    "node_name": node_name,
                    "node_type": node_type,
                    "properties": properties,
                    "description": description
                }
            )
            # 建立节点到原文本的关系
            source_text_info = node.get("source_text_info")
            if not source_text_info or not isinstance(source_text_info, dict):
                continue
            for text_id, text_id_info in source_text_info.items():
                new_text_id = f"text_{text_id}"
                if new_text_id not in text_id_set:
                    continue
                source_id = node_id
                target_id = new_text_id
                relation_type = "TracedFrom"
                properties = {
                    "定位": text_id_info
                }
                new_kg_data["edges"].append(
                    {
                        "source_id": source_id,
                        "target_id": target_id,
                        "relation_type": relation_type,
                        "properties": properties,
                        "weight": 1.0,
                        "bidirectional": False,
                    }
                )
        for edge in edges:
            source_id = edge.get("source_id")
            target_id = edge.get("target_id")
            relation_type = edge.get("relation_type")
            properties = edge.get("properties")
            weight = edge.get("weight")
            bidirectional = edge.get("bidirectional")
            new_kg_data["edges"].append(
                {
                    "source_id": source_id,
                    "target_id": target_id,
                    "relation_type": relation_type,
                    "properties": properties,
                    "weight": weight,
                    "bidirectional": bidirectional,
                }
            )
            # TODO:是否添加关系溯源
        return new_kg_data

    @staticmethod
    def sanitize_neo4j_property_name(name: str) -> str:
        """
        将字符串中不能作为Neo4j属性名的符号替换为单个下划线，但保留中文符号

        Args:
            name (str): 原始字符串

        Returns:
            str: 处理后的符合Neo4j属性名规范的字符串
        """
        if not name:
            return name

        # 保留字母、数字、下划线、中文字符以及常见的中文符号
        # 包括中文括号（）、书名号《》、引号""、顿号、问号、感叹号、冒号、分号等
        pattern = r'[^a-zA-Z0-9_\u4e00-\u9fff\uFF08\uFF09\u300A\u300B\u201C\u201D\u3001\uFF1F\uFF01\uFF1A\uFF1B\u3002\uFF0C]'
        sanitized = re.sub(pattern, '_', name)

        # 将连续的下划线替换为单个下划线
        sanitized = re.sub(r'_+', '_', sanitized)

        # 移除开头和结尾的下划线
        sanitized = sanitized.strip('_')

        # 如果结果为空，返回默认名称
        if not sanitized:
            return ""

        # 确保不以数字开头（Cypher要求）
        if sanitized[0].isdigit():
            sanitized = '_' + sanitized

        return sanitized

    def sanitize_neo4j_filename(self, name: str) -> str:
        """
        将文件名中后缀去掉，并且字符串中不能作为Neo4j属性名的符号替换为单个下划线，但保留中文符号

        Args:
            name (str): 原始字符串

        Returns:
            str: 处理后的符合Neo4j属性名规范的字符串
        """
        # 防止空字符串输入
        if not name:
            return ""
        return self.sanitize_neo4j_property_name(os.path.splitext(name)[0])


    @staticmethod
    def find_best_match(
            target: str,
            candidates: List[str],
            threshold: float = 0.9
    ) -> Optional[str]:
        """
        在字符串列表中查找与目标字符串相似度最高的结果

        Args:
            target (str): 目标字符串
            candidates (List[str]): 候选字符串列表
            threshold (float): 相似度阈值，默认0.9

        Returns:
            Optional[str]: 匹配度最高的字符串，若低于阈值则返回None
        """
        if not target or not candidates:
            return None

        best_match = None
        highest_similarity = 0.0

        for candidate in candidates:
            similarity = SequenceMatcher(None, target, candidate).ratio()
            if similarity > highest_similarity:
                highest_similarity = similarity
                best_match = candidate

        # 只有当最高相似度超过阈值时才返回结果
        if highest_similarity >= threshold:
            return best_match
        else:
            return None

    # @staticmethod
    # def process_source_text():

# TODO:设计图谱名的生成逻辑
def generate_unique_name(source_name):
    return f"{source_name}_{generate_snowflake_string_id()}"
#
#
kg_service = KGService()
