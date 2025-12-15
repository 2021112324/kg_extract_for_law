"""
使用任务池线程等方式实现抽取任务的后台运行
"""

import asyncio
import concurrent.futures
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, List

from dotenv import load_dotenv
from sqlalchemy.orm import Session

# 配置日志
logger = logging.getLogger(__name__)

# 项目根目录
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
env_path = os.path.join(project_root, '.env')
load_dotenv(env_path)

THREAD_POOL_MAX_WORKERS = int(os.getenv("THREAD_POOL_MAX_WORKERS", 5))

# 创建全局线程池执行器
# 限制最大工作线程数，避免资源耗尽
thread_pool_executor = ThreadPoolExecutor(max_workers=5)

# 创建临时文件路径
temp_dir = os.path.join(project_root, "tests", "tests", "tempdata")
os.makedirs(temp_dir, exist_ok=True)


class KGExtractionTaskManager:
    """
    知识图谱抽取任务管理器
    使用线程池处理长时间运行的抽取任务
    """

    def __init__(
            self,
            db_session: Session = None
    ):
        """
        初始化任务管理器

        Args:
            db_session: 数据库会话
        """
        self.db = db_session

    async def run_async_tasks(
            self,
            async_func: Callable,
            params_list: List[dict]
    ) -> List[Any]:
        """
        并发执行异步函数

        Args:
            async_func: 要执行的异步函数
            params_list: 参数列表，每个元素是一个字典，包含函数所需参数

        Returns:
            List[Any]: 执行结果列表，按输入参数顺序排列
        """
        if not params_list:
            return []

        results = [None] * len(params_list)  # 保持结果顺序

        try:
            # 使用线程池执行器
            with concurrent.futures.ThreadPoolExecutor(max_workers=THREAD_POOL_MAX_WORKERS) as executor:
                # 提交所有任务
                future_to_index = {}
                for index, params in enumerate(params_list):
                    future = executor.submit(self.run_async_function, async_func, params)
                    future_to_index[future] = index

                # 收集结果
                for future in concurrent.futures.as_completed(future_to_index):
                    index = future_to_index[future]
                    try:
                        result = future.result()
                        results[index] = result
                        logger.info(f"任务 {index} 执行完成")
                    except Exception as e:
                        logger.error(f"任务 {index} 执行失败: {str(e)}")
                        results[index] = {"error": str(e)}

        except Exception as e:
            logger.error(f"执行并发任务时发生异常: {str(e)}")

        return results

    @staticmethod
    def run_async_function(async_func: Callable, params: dict) -> Any:
        """
        在新事件循环中运行异步函数，将异步函数包装成同步函数

        Args:
            async_func: 异步函数
            params: 函数参数字典

        Returns:
            Any: 函数执行结果
        """
        # 为新线程创建事件循环
        if sys.platform == 'win32':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

        # 创建新的事件循环
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            # 运行异步函数
            result = loop.run_until_complete(async_func(**params))
            return result
        except Exception as e:
            logger.error(f"异步函数执行失败: {str(e)}")
            raise e
        finally:
            loop.close()


kg_task_manager = KGExtractionTaskManager()
