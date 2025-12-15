import os
import logging
import json
import csv
from typing import Dict, Any, List
from .base_processor import BaseProcessor, ProcessResult

logger = logging.getLogger(__name__)

class TextProcessor(BaseProcessor):
    """
    文本文件处理器
    
    处理各种文本格式的文件
    支持.txt, .md, .csv, .json, .xml等格式
    """
    
    def __init__(self):
        super().__init__()
        self.supported_extensions = ['.txt', '.md', '.csv', '.json', '.xml', '.yml', '.yaml']
        
    async def process_file(self, file_path: str, output_dir: str, **kwargs) -> ProcessResult:
        """
        处理文本文件
        
        Args:
            file_path: 文本文件路径
            output_dir: 输出目录
            **kwargs: 额外参数
            
        Returns:
            ProcessResult: 处理结果
        """
        try:
            file_ext = os.path.splitext(file_path)[1].lower()
            filename_uuid = kwargs.get('filename_uuid', 'text_output')
            
            # 提取文本内容
            extracted_text = await self.extract_text(file_path)
            
            # 保存处理后的文本
            processed_file_path = os.path.join(output_dir, f"{filename_uuid}_processed.txt")
            os.makedirs(output_dir, exist_ok=True)
            
            with open(processed_file_path, 'w', encoding='utf-8') as f:
                f.write(extracted_text)
            
            # 获取文本统计信息
            text_stats = self._get_text_statistics(extracted_text)
            
            return ProcessResult(
                success=True,
                file_path=file_path,
                file_type=file_ext,
                extracted_text=extracted_text,
                output_files=[processed_file_path],
                metadata={
                    **self.get_metadata(file_path),
                    **text_stats,
                    'text_type': self._get_text_type(file_ext),
                    'processed_file_path': processed_file_path,
                    'encoding': self._detect_encoding(file_path)
                }
            )
            
        except Exception as e:
            error_msg = f"文本文件处理失败: {str(e)}"
            logger.error(error_msg)
            return self.create_error_result(file_path, error_msg)
    
    async def extract_text(self, file_path: str) -> str:
        """
        从文本文件中提取内容
        
        Args:
            file_path: 文本文件路径
            
        Returns:
            str: 提取的文本内容
        """
        file_ext = os.path.splitext(file_path)[1].lower()
        
        try:
            if file_ext in ['.txt', '.md']:
                return await self._extract_plain_text(file_path)
            elif file_ext == '.csv':
                return await self._extract_csv_text(file_path)
            elif file_ext == '.json':
                return await self._extract_json_text(file_path)
            elif file_ext == '.xml':
                return await self._extract_xml_text(file_path)
            elif file_ext in ['.yml', '.yaml']:
                return await self._extract_yaml_text(file_path)
            else:
                # 默认作为纯文本处理
                return await self._extract_plain_text(file_path)
                
        except Exception as e:
            logger.error(f"文本提取失败: {str(e)}")
            return f"文本提取失败: {str(e)}"
    
    async def _extract_plain_text(self, file_path: str) -> str:
        """提取纯文本内容"""
        encoding = self._detect_encoding(file_path)
        
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                return f.read()
        except UnicodeDecodeError:
            # 如果检测的编码失败，尝试其他编码
            for fallback_encoding in ['utf-8', 'gbk', 'latin-1']:
                try:
                    with open(file_path, 'r', encoding=fallback_encoding) as f:
                        logger.warning(f"使用备用编码 {fallback_encoding} 读取文件: {file_path}")
                        return f.read()
                except UnicodeDecodeError:
                    continue
            
            # 所有编码都失败，使用二进制模式
            with open(file_path, 'rb') as f:
                content = f.read()
                return content.decode('utf-8', errors='replace')
    
    async def _extract_csv_text(self, file_path: str) -> str:
        """提取CSV文件内容"""
        encoding = self._detect_encoding(file_path)
        text_content = []
        
        try:
            with open(file_path, 'r', encoding=encoding, newline='') as f:
                # 自动检测分隔符
                sample = f.read(1024)
                f.seek(0)
                
                sniffer = csv.Sniffer()
                delimiter = sniffer.sniff(sample).delimiter
                
                reader = csv.reader(f, delimiter=delimiter)
                
                for i, row in enumerate(reader):
                    if i == 0:  # 标题行
                        text_content.append(f"CSV表头: {' | '.join(row)}")
                        text_content.append('-' * 50)
                    else:
                        text_content.append(' | '.join(row))
                        
                        # 限制显示行数避免内容过长
                        if i > 100:
                            text_content.append(f"... (省略剩余行)")
                            break
            
            return '\n'.join(text_content)
            
        except Exception as e:
            logger.error(f"CSV文件处理失败: {str(e)}")
            # 如果CSV解析失败，作为纯文本处理
            return await self._extract_plain_text(file_path)
    
    async def _extract_json_text(self, file_path: str) -> str:
        """提取JSON文件内容"""
        encoding = self._detect_encoding(file_path)
        
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                data = json.load(f)
            
            # 将JSON数据转换为可读文本
            formatted_json = json.dumps(data, ensure_ascii=False, indent=2)
            
            # 添加结构化描述
            description = f"JSON文件结构分析:\n"
            description += f"数据类型: {type(data).__name__}\n"
            
            if isinstance(data, dict):
                description += f"键数量: {len(data)}\n"
                description += f"主要键名: {list(data.keys())[:10]}\n\n"
            elif isinstance(data, list):
                description += f"元素数量: {len(data)}\n\n"
            
            description += "格式化内容:\n"
            description += formatted_json
            
            return description
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON解析失败: {str(e)}")
            return f"JSON解析失败: {str(e)}\n\n原始内容:\n" + await self._extract_plain_text(file_path)
        except Exception as e:
            logger.error(f"JSON文件处理失败: {str(e)}")
            return await self._extract_plain_text(file_path)
    
    async def _extract_xml_text(self, file_path: str) -> str:
        """提取XML文件内容"""
        try:
            import xml.etree.ElementTree as ET
            
            tree = ET.parse(file_path)
            root = tree.getroot()
            
            # 提取所有文本内容
            text_content = []
            text_content.append(f"XML文件结构分析:")
            text_content.append(f"根元素: {root.tag}")
            text_content.append(f"属性: {root.attrib}")
            text_content.append("\n文本内容:")
            
            def extract_element_text(element, level=0):
                indent = "  " * level
                if element.text and element.text.strip():
                    text_content.append(f"{indent}{element.tag}: {element.text.strip()}")
                
                for child in element:
                    extract_element_text(child, level + 1)
            
            extract_element_text(root)
            
            return '\n'.join(text_content)
            
        except Exception as e:
            logger.error(f"XML文件处理失败: {str(e)}")
            return await self._extract_plain_text(file_path)
    
    async def _extract_yaml_text(self, file_path: str) -> str:
        """提取YAML文件内容"""
        try:
            import yaml
            
            encoding = self._detect_encoding(file_path)
            with open(file_path, 'r', encoding=encoding) as f:
                data = yaml.safe_load(f)
            
            # 将YAML数据转换为可读文本
            formatted_yaml = yaml.dump(data, default_flow_style=False, allow_unicode=True)
            
            description = f"YAML文件结构分析:\n"
            description += f"数据类型: {type(data).__name__}\n\n"
            description += "格式化内容:\n"
            description += formatted_yaml
            
            return description
            
        except ImportError:
            logger.warning("PyYAML库未安装，将YAML文件作为纯文本处理")
            return await self._extract_plain_text(file_path)
        except Exception as e:
            logger.error(f"YAML文件处理失败: {str(e)}")
            return await self._extract_plain_text(file_path)
    
    def _detect_encoding(self, file_path: str) -> str:
        """检测文件编码"""
        try:
            import chardet
            
            with open(file_path, 'rb') as f:
                raw_data = f.read(10000)  # 读取前10KB用于检测
                result = chardet.detect(raw_data)
                
            encoding = result.get('encoding', 'utf-8')
            confidence = result.get('confidence', 0)
            
            if confidence < 0.7:  # 置信度较低，使用默认编码
                encoding = 'utf-8'
            
            return encoding
            
        except ImportError:
            # 如果没有chardet库，使用默认编码
            return 'utf-8'
        except Exception:
            return 'utf-8'
    
    def _get_text_statistics(self, text: str) -> Dict[str, Any]:
        """获取文本统计信息"""
        lines = text.split('\n')
        words = text.split()
        
        return {
            'character_count': len(text),
            'word_count': len(words),
            'line_count': len(lines),
            'non_empty_line_count': len([line for line in lines if line.strip()]),
            'average_line_length': len(text) / len(lines) if lines else 0
        }
    
    def _get_text_type(self, file_ext: str) -> str:
        """获取文本类型描述"""
        type_mapping = {
            '.txt': '纯文本文件',
            '.md': 'Markdown文档',
            '.csv': 'CSV数据文件',
            '.json': 'JSON数据文件',
            '.xml': 'XML数据文件',
            '.yml': 'YAML配置文件',
            '.yaml': 'YAML配置文件'
        }
        return type_mapping.get(file_ext.lower(), f'文本文件 ({file_ext})')