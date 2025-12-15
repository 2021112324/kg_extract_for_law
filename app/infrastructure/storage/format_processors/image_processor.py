import os
import logging
from typing import Dict, Any, Optional, Tuple
from .base_processor import BaseProcessor, ProcessResult

logger = logging.getLogger(__name__)

class ImageProcessor(BaseProcessor):
    """
    图片文件处理器
    
    处理各种图片格式的基本操作
    不包含AI分析功能，只负责基础的图片处理
    """
    
    def __init__(self):
        super().__init__()
        self.supported_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.tiff', '.svg']
        
    async def process_file(self, file_path: str, output_dir: str, **kwargs) -> ProcessResult:
        """
        处理图片文件
        
        Args:
            file_path: 图片文件路径
            output_dir: 输出目录
            **kwargs: 额外参数
            
        Returns:
            ProcessResult: 处理结果
        """
        try:
            file_ext = os.path.splitext(file_path)[1].lower()
            filename_uuid = kwargs.get('filename_uuid', 'image_output')
            
            # 获取图片信息
            image_info = await self.get_image_info(file_path)
            
            # 复制图片到输出目录（可选）
            os.makedirs(output_dir, exist_ok=True)
            
            import shutil
            output_file_path = os.path.join(output_dir, f"{filename_uuid}{file_ext}")
            shutil.copy2(file_path, output_file_path)
            
            # 图片本身不包含文本，但可以提供基本描述
            description = f"图片文件: {os.path.basename(file_path)}\n"
            description += f"尺寸: {image_info.get('width', '未知')} x {image_info.get('height', '未知')}\n"
            description += f"格式: {image_info.get('format', '未知')}\n"
            description += f"文件大小: {self._format_file_size(image_info.get('file_size', 0))}"
            
            return ProcessResult(
                success=True,
                file_path=file_path,
                file_type=file_ext,
                extracted_text=description,
                output_files=[output_file_path],
                metadata={
                    **self.get_metadata(file_path),
                    **image_info,
                    'copied_to': output_file_path,
                    'requires_ai_analysis': True  # 标记需要AI分析
                }
            )
            
        except Exception as e:
            error_msg = f"图片处理失败: {str(e)}"
            logger.error(error_msg)
            return self.create_error_result(file_path, error_msg)
    
    async def extract_text(self, file_path: str) -> str:
        """
        从图片中提取文本（基础处理器不包含OCR）
        
        Args:
            file_path: 图片文件路径
            
        Returns:
            str: 图片描述信息
        """
        try:
            image_info = await self.get_image_info(file_path)
            
            description = f"图片文件: {os.path.basename(file_path)}\n"
            description += f"尺寸: {image_info.get('width', '未知')} x {image_info.get('height', '未知')}\n"
            description += f"格式: {image_info.get('format', '未知')}\n"
            description += f"颜色模式: {image_info.get('mode', '未知')}\n"
            description += "\n注意: 这是图片文件，需要使用OCR或AI视觉模型来提取文本内容。"
            
            return description
            
        except Exception as e:
            logger.error(f"图片信息提取失败: {str(e)}")
            return f"图片信息提取失败: {str(e)}"
    
    async def get_image_info(self, file_path: str) -> Dict[str, Any]:
        """
        获取图片的详细信息
        
        Args:
            file_path: 图片文件路径
            
        Returns:
            Dict: 图片信息
        """
        try:
            from PIL import Image
            
            with Image.open(file_path) as img:
                return {
                    'width': img.width,
                    'height': img.height,
                    'format': img.format,
                    'mode': img.mode,
                    'file_size': os.path.getsize(file_path),
                    'has_transparency': img.mode in ('RGBA', 'LA') or 'transparency' in img.info
                }
                
        except ImportError:
            logger.warning("Pillow库未安装，无法获取详细图片信息")
            return {
                'file_size': os.path.getsize(file_path),
                'format': os.path.splitext(file_path)[1].upper().lstrip('.'),
                'width': '未知',
                'height': '未知',
                'mode': '未知'
            }
        except Exception as e:
            logger.error(f"获取图片信息失败: {str(e)}")
            return {
                'file_size': os.path.getsize(file_path) if os.path.exists(file_path) else 0,
                'error': str(e)
            }
    
    async def validate_image(self, file_path: str) -> bool:
        """
        验证图片文件是否有效
        
        Args:
            file_path: 图片文件路径
            
        Returns:
            bool: 是否为有效图片
        """
        try:
            from PIL import Image
            
            with Image.open(file_path) as img:
                # 尝试加载图片数据来验证文件完整性
                img.verify()
                return True
                
        except ImportError:
            # 如果没有Pillow，只检查文件扩展名
            return self.supports_file(file_path)
        except Exception as e:
            logger.error(f"图片验证失败: {str(e)}")
            return False
    
    async def resize_image(self, file_path: str, output_path: str, max_size: Tuple[int, int] = (1920, 1080)) -> bool:
        """
        调整图片尺寸（可选功能）
        
        Args:
            file_path: 输入图片路径
            output_path: 输出图片路径
            max_size: 最大尺寸 (width, height)
            
        Returns:
            bool: 是否成功
        """
        try:
            from PIL import Image
            
            with Image.open(file_path) as img:
                # 保持容比缩放
                img.thumbnail(max_size, Image.Resampling.LANCZOS)
                
                # 确保输出目录存在
                os.makedirs(os.path.dirname(output_path), exist_ok=True)
                
                # 保存图片
                img.save(output_path, optimize=True, quality=85)
                
            logger.info(f"图片缩放成功: {file_path} -> {output_path}")
            return True
            
        except ImportError:
            logger.error("Pillow库未安装，无法调整图片尺寸")
            return False
        except Exception as e:
            logger.error(f"图片缩放失败: {str(e)}")
            return False
    
    def _format_file_size(self, size_bytes: int) -> str:
        """格式化文件大小显示"""
        if size_bytes < 1024:
            return f"{size_bytes} B"
        elif size_bytes < 1024 * 1024:
            return f"{size_bytes / 1024:.1f} KB"
        else:
            return f"{size_bytes / (1024 * 1024):.1f} MB"
    
    def get_supported_formats(self) -> Dict[str, str]:
        """获取支持的图片格式列表"""
        return {
            '.jpg': 'JPEG图片',
            '.jpeg': 'JPEG图片',
            '.png': 'PNG图片',
            '.gif': 'GIF图片',
            '.bmp': 'BMP图片',
            '.webp': 'WebP图片',
            '.tiff': 'TIFF图片',
            '.svg': 'SVG矢量图'
        }