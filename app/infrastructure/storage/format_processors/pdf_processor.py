import os
import logging
from typing import Dict, Any, List
import tempfile
from pathlib import Path

from app.infrastructure.external_apis.mineru_client import MinerUClient
from .base_processor import BaseProcessor, ProcessResult

logger = logging.getLogger(__name__)

class PDFProcessor(BaseProcessor):
    """
    PDF文件处理器
    
    专门处理PDF文件的解析、文本提取和内容转换
    使用MinerU API进行PDF智能解析
    """
    
    def __init__(self):
        super().__init__()
        self.supported_extensions = ['.pdf']
        self.mineru_client = MinerUClient()
        
    async def process_file(self, file_path: str, output_dir: str, **kwargs) -> ProcessResult:
        """
        处理PDF文件
        
        Args:
            file_path: PDF文件路径
            output_dir: 输出目录
            **kwargs: 额外参数（如filename_uuid, enable_ocr）
            
        Returns:
            ProcessResult: 处理结果
        """
        logger.info(f"开始处理PDF文件: {file_path}，使用MinerU API")
        try:
            filename_uuid = kwargs.get('filename_uuid', 'pdf_output')
            enable_ocr = kwargs.get('enable_ocr', True)
            images_dir = os.path.join(output_dir, 'images')
            
            # 确保目录存在
            os.makedirs(output_dir, exist_ok=True)
            os.makedirs(images_dir, exist_ok=True)
            
            # 使用MinerU API处理文件
            result = await self.mineru_client.process_file_to_markdown(
                file_path=file_path,
                enable_ocr=enable_ocr,
                output_dir=output_dir
            )
            
            if not result['success']:
                error_msg = f"MinerU API处理失败: {result.get('error', '未知错误')}"
                logger.error(error_msg)
                return self.create_error_result(file_path, error_msg)
            
            # 获取处理结果
            md_content = result['content']
            extract_dir = Path(result['extract_dir'])
            api_output_files = result['output_files']
            
            # 创建输出文件路径
            md_file_path = os.path.join(output_dir, f"{filename_uuid}.md")
            
            # 如果API返回了Markdown内容，保存到指定位置
            if md_content:
                with open(md_file_path, 'w', encoding='utf-8') as f:
                    f.write(md_content)
            
            # 收集输出文件
            output_files = [md_file_path] if os.path.exists(md_file_path) else []
            
            # 复制图片文件到images目录
            image_count = 0
            for file_path_str in api_output_files:
                file_path_obj = Path(file_path_str)
                if file_path_obj.suffix.lower() in ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp']:
                    # 复制图片到images目录
                    import shutil
                    dest_path = os.path.join(images_dir, file_path_obj.name)
                    try:
                        shutil.copy2(file_path_str, dest_path)
                        output_files.append(dest_path)
                        image_count += 1
                    except Exception as copy_error:
                        logger.warning(f"复制图片失败: {copy_error}")
                elif file_path_obj.suffix.lower() == '.pdf':
                    # 复制PDF文件（如果有可视化PDF）
                    visual_pdf_path = os.path.join(output_dir, f"{filename_uuid}_visual.pdf")
                    try:
                        import shutil
                        shutil.copy2(file_path_str, visual_pdf_path)
                        output_files.append(visual_pdf_path)
                    except Exception as copy_error:
                        logger.warning(f"复制可视化PDF失败: {copy_error}")
            
            return ProcessResult(
                success=True,
                file_path=file_path,
                file_type='.pdf',
                extracted_text=md_content,
                output_files=output_files,
                metadata={
                    **self.get_metadata(file_path),
                    'processing_mode': 'API_OCR' if enable_ocr else 'API_TEXT',
                    'markdown_path': md_file_path,
                    'images_dir': images_dir,
                    'extracted_images_count': image_count,
                    'batch_id': result.get('batch_id'),
                    'api_extract_dir': result['extract_dir']
                }
            )
            
        except Exception as e:
            error_msg = f"PDF API处理失败: {str(e)}"
            logger.error(error_msg)
            return self.create_error_result(file_path, error_msg)
    
    async def extract_text(self, file_path: str) -> str:
        """
        从 PDF 文件中提取纯文本内容
        
        Args:
            file_path: PDF文件路径
            
        Returns:
            str: 提取的文本内容
        """
        try:
            # 使用MinerU API进行文本提取
            result = await self.mineru_client.process_file_to_markdown(
                file_path=file_path,
                enable_ocr=True,  # 默认启用OCR以获得最佳文本提取效果
                output_dir=None  # 使用临时目录
            )
            
            if result['success']:
                return result['content']
            else:
                error_msg = f"PDF API文本提取失败: {result.get('error', '未知错误')}"
                logger.error(error_msg)
                return error_msg
                    
        except Exception as e:
            error_msg = f"PDF API文本提取失败: {str(e)}"
            logger.error(error_msg)
            return error_msg
    
    async def extract_images(self, file_path: str, output_dir: str) -> List[str]:
        """
        从PDF中提取图片
        
        Args:
            file_path: PDF文件路径
            output_dir: 图片输出目录
            
        Returns:
            List[str]: 提取的图片文件路径列表
        """
        try:
            os.makedirs(output_dir, exist_ok=True)
            
            # 使用MinerU API进行图片提取
            result = await self.mineru_client.process_file_to_markdown(
                file_path=file_path,
                enable_ocr=True,  # 启用OCR以获得更好的图片提取效果
                output_dir=None  # 使用临时目录
            )
            
            if not result['success']:
                logger.error(f"PDF API图片提取失败: {result.get('error', '未知错误')}")
                return []
            
            # 收集并复制图片文件
            image_files = []
            for file_path_str in result['output_files']:
                file_path_obj = Path(file_path_str)
                if file_path_obj.suffix.lower() in ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp']:
                    # 复制图片到指定输出目录
                    dest_path = os.path.join(output_dir, file_path_obj.name)
                    try:
                        import shutil
                        shutil.copy2(file_path_str, dest_path)
                        image_files.append(dest_path)
                    except Exception as copy_error:
                        logger.warning(f"复制图片失败: {copy_error}")
            
            logger.info(f"从PDF提取了 {len(image_files)} 张图片")
            return image_files
            
        except Exception as e:
            logger.error(f"PDF API图片提取失败: {str(e)}")
            return []
    
    def get_pdf_metadata(self, file_path: str) -> Dict[str, Any]:
        """
        获取PDF特定的元数据
        
        Args:
            file_path: PDF文件路径
            
        Returns:
            Dict: PDF元数据
        """
        metadata = self.get_metadata(file_path)
        
        try:
            # 这里可以添加更多 PDF 特定的元数据提取
            # 例如页数、作者、创建日期等
            metadata.update({
                'file_format': 'PDF',
                'supports_ocr': True,
                'supports_text_extraction': True,
                'supports_image_extraction': True
            })
            
        except Exception as e:
            logger.error(f"获取PDF元数据失败: {str(e)}")
        
        return metadata