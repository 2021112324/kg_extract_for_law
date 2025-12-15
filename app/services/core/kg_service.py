import os
from typing import Optional, List

from dotenv import load_dotenv
from fastapi import HTTPException, UploadFile
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.infrastructure.graph_storage.factory import GraphStorageFactory
from app.infrastructure.response import success_response, not_found_response, error_response
from app.infrastructure.storage.object_storage import StorageFactory
from app.models.kg import KG as KGModel, KGExtractionTask, KGFile
from app.schemas.kg import KGCreate, KGTaskCreate
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
            name: Optional[str] =  None,
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
            if task_files:
                md_paths = [file.minio_path for file in task_files if file.minio_path]
            else:
                md_paths = []
            # 5. 判断文件列表中各文件是否存在minio中的md文件
            for md_path in md_paths:
                if not await self._is_md_exist_in_minio(md_path):
                    return not_found_response(
                        entity="文件",
                    )
            if task.status == 4:        # 图谱状态：0-pending, 1-running, 2-completed, 3-merged, 4-failed, 5-cancelled
                task.retry_count = task.retry_count + 1
            try:
                # 6.执行抽取任务
                result = await self.kg_extract_service.extract_kg_from_md_paths(
                    md_paths=md_paths,
                    user_prompt=user_prompt,
                    db=db,
                    prompt_parameters=prompt_parameters,
                )
                print("debug:" + str(result))
                try:
                    # 7. 将抽取出的图谱保存到图数据库中
                    if result:
                        # TODO: 保留图谱节点文件来源？
                        self.graph_storage.connect()
                        self.graph_storage.add_subgraph_with_merge(
                            kg_data=result,
                            graph_tag=graph_name,
                            graph_level="DomainLevel"
                        )
                        self.graph_storage.disconnect()
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

    async def _is_md_exist_in_minio(
            self,
            md_path: str,
    ):
        """
        判断文件是否存在minio中
        """
        try:
            # 解析路径，分离存储桶和文件名
            if '/' in md_path:
                bucket_name, file_name = md_path.split('/', 1)
            else:
                # 如果没有'/'分隔符，则使用默认存储桶
                bucket_name = MINIO_BUCKET
                file_name = md_path
            # 使用file_storage检查文件是否存在
            return self.file_storage.file_exists(bucket_name, file_name)
        except Exception as e:
            print(f"检查文件是否存在时出错: {str(e)}")
            return False


# TODO:设计图谱名的生成逻辑
def generate_unique_name(source_name):
    return f"{source_name}_{generate_snowflake_string_id()}"
#
#
kg_service = KGService()
