"""MinerU File转Markdown转换的API客户端。"""

import asyncio
import os
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
import tempfile

import aiohttp
import requests

from app.core.config import settings
from .mineru_config import logger, ensure_output_dir


def singleton_func(cls):
    instance = {}

    def _singleton(*args, **kwargs):
        if cls not in instance:
            instance[cls] = cls(*args, **kwargs)
        return instance[cls]

    return _singleton


@singleton_func
class MinerUClient:
    """
    用于与 MinerU API 交互以将 PDF 转换为 Markdown 的客户端。
    """

    def __init__(self, api_base: Optional[str] = None, api_key: Optional[str] = None):
        """
        初始化 MinerU API 客户端。

        Args:
            api_base: MinerU API 的基础 URL (默认: 从配置获取)
            api_key: 用于向 MinerU 进行身份验证的 API 密钥 (默认: 从配置获取)
        """
        self.api_base = api_base or settings.MINERU_API_BASE
        self.api_key = api_key or settings.MINERU_API_KEY

        if not self.api_key:
            # 提供更友好的错误消息
            raise ValueError(
                "错误: MinerU API 密钥 (MINERU_API_KEY) 未设置或为空。\\n"
                "请确保已设置 MINERU_API_KEY 环境变量，例如:\\n"
                "  export MINERU_API_KEY='your_actual_api_key'\\n"
                "或者，在项目根目录的 `.env` 文件中定义该变量。"
            )

    async def _request(self, method: str, endpoint: str, **kwargs) -> Dict[str, Any]:
        """
        向 MinerU API 发出请求。

        Args:
            method: HTTP 方法 (GET, POST 等)
            endpoint: API 端点路径 (不含基础 URL)
            **kwargs: 传递给 aiohttp 请求的其他参数

        Returns:
            dict: API 响应 (JSON 格式)
        """
        url = f"{self.api_base}{endpoint}"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json",
        }

        if "headers" in kwargs:
            kwargs["headers"].update(headers)
        else:
            kwargs["headers"] = headers

        # 创建一个不包含授权信息的参数副本，用于日志记录
        log_kwargs = kwargs.copy()
        if "headers" in log_kwargs and "Authorization" in log_kwargs["headers"]:
            log_kwargs["headers"] = log_kwargs["headers"].copy()
            log_kwargs["headers"]["Authorization"] = "Bearer ****"  # 隐藏API密钥

        logger.debug(f"API请求: {method} {url}")
        logger.debug(f"请求参数: {log_kwargs}")

        async with aiohttp.ClientSession() as session:
            async with session.request(method, url, **kwargs) as response:
                response.raise_for_status()
                response_json = await response.json()

                logger.debug(f"API响应: {response_json}")

                return response_json

    async def submit_file_task(
        self,
        files: Union[str, List[Union[str, Dict[str, Any]]], Dict[str, Any]],
        enable_ocr: bool = True,
        language: str = "ch",
        page_ranges: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        提交本地 PDF 文件以转换为 Markdown。支持单个文件路径或多个文件配置。

        Args:
            files: 可以是以下形式之一:
                1. 单个文件路径字符串
                2. 多个文件路径的列表
                3. 包含文件配置的字典列表，每个字典包含:
                   - path/name: 文件路径或文件名
                   - is_ocr: 是否启用OCR (可选)
                   - data_id: 文件数据ID (可选)
                   - page_ranges: 页码范围 (可选)
            enable_ocr: 是否为转换启用 OCR（所有文件的默认值）
            language: 指定文档语言，默认 ch，中文
            page_ranges: 指定页码范围，格式为逗号分隔的字符串。例如："2,4-6"表示选取第2页、第4页至第6页；"2--2"表示从第2页到倒数第2页。

        Returns:
            dict: 任务信息，包括batch_id
        """
        # 统计文件数量
        file_count = 1
        if isinstance(files, list):
            file_count = len(files)
        logger.debug(
            f"调用submit_file_task: {file_count}个文件, "
            + f"ocr={enable_ocr}, "
            + f"language={language}"
        )

        # 处理输入，确保我们有一个文件配置列表
        files_config = []

        # 转换输入为标准格式
        if isinstance(files, str):
            # 单个文件路径
            file_path = Path(files)
            if not file_path.exists():
                raise FileNotFoundError(f"未找到 PDF 文件: {file_path}")

            files_config.append(
                {
                    "path": file_path,
                    "name": file_path.name,
                    "is_ocr": enable_ocr,
                    "page_ranges": page_ranges,
                }
            )

        elif isinstance(files, list):
            # 处理文件路径列表或文件配置列表
            for i, file_item in enumerate(files):
                if isinstance(file_item, str):
                    # 简单的文件路径
                    file_path = Path(file_item)
                    if not file_path.exists():
                        raise FileNotFoundError(f"未找到 PDF 文件: {file_path}")

                    files_config.append(
                        {
                            "path": file_path,
                            "name": file_path.name,
                            "is_ocr": enable_ocr,
                            "page_ranges": page_ranges,
                        }
                    )

                elif isinstance(file_item, dict):
                    # 含有详细配置的文件字典
                    if "path" not in file_item and "name" not in file_item:
                        raise ValueError(
                            f"文件配置必须包含 'path' 或 'name' 字段: {file_item}"
                        )

                    if "path" in file_item:
                        file_path = Path(file_item["path"])
                        if not file_path.exists():
                            raise FileNotFoundError(f"未找到 PDF 文件: {file_path}")

                        file_name = file_path.name
                    else:
                        file_name = file_item["name"]
                        file_path = None

                    file_is_ocr = file_item.get("is_ocr", enable_ocr)
                    file_page_ranges = file_item.get("page_ranges", page_ranges)

                    file_config = {
                        "path": file_path,
                        "name": file_name,
                        "is_ocr": file_is_ocr,
                    }
                    if file_page_ranges is not None:
                        file_config["page_ranges"] = file_page_ranges

                    files_config.append(file_config)
                else:
                    raise TypeError(f"不支持的文件配置类型: {type(file_item)}")
        elif isinstance(files, dict):
            # 单个文件配置字典
            if "path" not in files and "name" not in files:
                raise ValueError(f"文件配置必须包含 'path' 或 'name' 字段: {files}")

            if "path" in files:
                file_path = Path(files["path"])
                if not file_path.exists():
                    raise FileNotFoundError(f"未找到 PDF 文件: {file_path}")

                file_name = file_path.name
            else:
                file_name = files["name"]
                file_path = None

            file_is_ocr = files.get("is_ocr", enable_ocr)
            file_page_ranges = files.get("page_ranges", page_ranges)

            file_config = {
                "path": file_path,
                "name": file_name,
                "is_ocr": file_is_ocr,
            }
            if file_page_ranges is not None:
                file_config["page_ranges"] = file_page_ranges

            files_config.append(file_config)
        else:
            raise TypeError(f"files 必须是字符串、列表或字典，而不是 {type(files)}")

        # 步骤1: 构建API请求payload
        files_payload = []
        for file_config in files_config:
            file_payload = {
                "name": file_config["name"],
                "is_ocr": file_config["is_ocr"],
            }
            if "page_ranges" in file_config and file_config["page_ranges"] is not None:
                file_payload["page_ranges"] = file_config["page_ranges"]
            files_payload.append(file_payload)

        payload = {
            "language": language,
            "files": files_payload,
        }

        # 步骤2: 获取文件上传URL
        response = await self._request("POST", "/api/v4/file-urls/batch", json=payload)

        # 检查响应
        if (
            "data" not in response
            or "batch_id" not in response["data"]
            or "file_urls" not in response["data"]
        ):
            raise ValueError(f"获取上传URL失败: {response}")

        batch_id = response["data"]["batch_id"]
        file_urls = response["data"]["file_urls"]

        if len(file_urls) != len(files_config):
            raise ValueError(
                f"上传URL数量 ({len(file_urls)}) 与文件数量 ({len(files_config)}) 不匹配"
            )

        logger.info(f"开始上传 {len(file_urls)} 个本地文件")
        logger.debug(f"获取上传URL成功，批次ID: {batch_id}")

        # 步骤3: 上传所有文件
        uploaded_files = []

        for i, (file_config, upload_url) in enumerate(zip(files_config, file_urls)):
            file_path = file_config["path"]
            if file_path is None:
                raise ValueError(f"文件 {file_config['name']} 没有有效的路径")

            try:
                with open(file_path, "rb") as f:
                    # 重要：不设置Content-Type，让OSS自动处理
                    response = requests.put(upload_url, data=f)

                    if response.status_code != 200:
                        raise ValueError(
                            f"文件上传失败，状态码: {response.status_code}, 响应: {response.text}"
                        )

                    logger.debug(f"文件 {file_path.name} 上传成功")
                    uploaded_files.append(file_path.name)
            except Exception as e:
                raise ValueError(f"文件 {file_path.name} 上传失败: {str(e)}")

        logger.info(f"文件上传完成，共 {len(uploaded_files)} 个文件")

        # 返回包含batch_id的响应和已上传的文件信息
        result = {"data": {"batch_id": batch_id, "uploaded_files": uploaded_files}}

        # 对于单个文件的情况，保持与原来返回格式的兼容性
        if len(uploaded_files) == 1:
            result["data"]["file_name"] = uploaded_files[0]

        return result

    async def get_batch_task_status(self, batch_id: str) -> Dict[str, Any]:
        """
        获取批量转换任务的状态。

        Args:
            batch_id: 批量任务的ID

        Returns:
            dict: 批量任务状态信息
        """
        response = await self._request(
            "GET", f"/api/v4/extract-results/batch/{batch_id}"
        )

        return response

    async def process_file_to_markdown(
        self,
        file_path: str,
        enable_ocr: bool = True,
        output_dir: Optional[str] = None,
        max_retries: int = 180,
        retry_interval: int = 10,
    ) -> Dict[str, Any]:
        """
        从开始到结束处理 PDF 到 Markdown 的转换。

        Args:
            file_path: PDF 文件路径
            enable_ocr: 是否启用 OCR
            output_dir: 结果的输出目录
            max_retries: 最大状态检查重试次数
            retry_interval: 状态检查之间的时间间隔 (秒)

        Returns:
            Dict[str, Any]: 处理结果，包含：
                - success: 是否成功
                - content: Markdown 内容
                - extract_dir: 提取目录
                - output_files: 输出文件列表
        """
        try:
            # 提交任务
            task_info = await self.submit_file_task(file_path, enable_ocr)
            batch_id = task_info["data"]["batch_id"]
            file_name = task_info["data"]["file_name"]
            
            logger.info(f"PDF任务提交成功，批次ID: {batch_id}")

            # 准备输出路径
            if output_dir:
                output_path = Path(output_dir)
                output_path.mkdir(parents=True, exist_ok=True)
            else:
                output_path = Path(tempfile.mkdtemp())

            # 轮询任务完成情况
            for i in range(max_retries):
                status_info = await self.get_batch_task_status(batch_id)
                
                if (
                    "data" not in status_info
                    or "extract_result" not in status_info["data"]
                ):
                    logger.error(f"获取任务状态失败: {status_info}")
                    await asyncio.sleep(retry_interval)
                    continue

                # 检查文件状态
                for result in status_info["data"]["extract_result"]:
                    if result.get("file_name") == file_name:
                        state = result.get("state")
                        
                        if state == "done":
                            full_zip_url = result.get("full_zip_url")
                            if not full_zip_url:
                                raise ValueError("处理完成但没有下载链接")
                                
                            logger.info(f"文件 {file_name} 处理完成，开始下载结果")
                            
                            # 下载并解压结果
                            extract_dir = output_path / batch_id
                            extract_dir.mkdir(exist_ok=True)
                            
                            # 下载ZIP文件
                            zip_path = output_path / f"{batch_id}.zip"
                            
                            async with aiohttp.ClientSession() as session:
                                async with session.get(
                                    full_zip_url,
                                    headers={"Authorization": f"Bearer {self.api_key}"},
                                ) as response:
                                    response.raise_for_status()
                                    with open(zip_path, "wb") as f:
                                        f.write(await response.read())
                            
                            # 解压到目录
                            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                                zip_ref.extractall(extract_dir)
                            
                            # 解压后删除ZIP文件
                            zip_path.unlink()
                            
                            # 读取Markdown内容
                            markdown_content = ""
                            markdown_files = list(extract_dir.glob("*.md"))
                            if markdown_files:
                                with open(markdown_files[0], "r", encoding="utf-8") as f:
                                    markdown_content = f.read()
                            
                            # 收集输出文件
                            output_files = []
                            for file in extract_dir.rglob("*"):
                                if file.is_file():
                                    output_files.append(str(file))
                            
                            logger.info(f"文件处理完成，结果保存到: {extract_dir}")
                            
                            return {
                                "success": True,
                                "content": markdown_content,
                                "extract_dir": str(extract_dir),
                                "output_files": output_files,
                                "batch_id": batch_id
                            }
                        
                        elif state in ["failed", "error"]:
                            error_msg = result.get("err_msg", "处理失败")
                            raise ValueError(f"文件处理失败: {error_msg}")
                        
                        else:
                            logger.info(f"等待文件处理完成... 状态: {state} ({i+1}/{max_retries})")
                            break
                
                await asyncio.sleep(retry_interval)
            else:
                raise TimeoutError(f"任务 {batch_id} 未在允许的时间内完成")

        except Exception as e:
            logger.error(f"处理 PDF 到 Markdown 失败: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "content": "",
                "extract_dir": "",
                "output_files": []
            }