"""
文本编码工具类

提供通用的文本编码检测和安全解码功能
适用于任何文件流（MinIO、本地文件、网络流等）
"""
from typing import Optional, Tuple, BinaryIO


class TextEncodingUtils:
    """
    文本编码工具类
    
    提供文本编码检测和安全解码功能，适用于各种文件流场景：
    - MinIO对象存储流
    - 本地文件流
    - 网络文件流
    - 内存字节流
    - 任何实现了 read()、tell()、seek() 方法的流对象
    """
    
    # 支持的编码列表（按优先级排序）
    DEFAULT_ENCODINGS = ['utf-8', 'gbk']
    
    # 编码检测采样大小（字节）
    ENCODING_DETECT_SAMPLE_SIZE = 4096
    
    # 安全解码时检查的最大字节数（用于处理多字节编码截断）
    MAX_CHECK_BYTES = 20
    
    @staticmethod
    def detect_encoding(file_stream: BinaryIO, sample_size: int = None) -> str:
        """
        检测文件流的编码格式
        
        通过读取文件流的前几个字节来检测编码，支持多种编码格式。
        此方法不会改变文件流的当前位置。
        
        Args:
            file_stream: 文件流对象，需要实现 read()、tell()、seek() 方法
            sample_size: 采样大小（字节），默认使用 ENCODING_DETECT_SAMPLE_SIZE
            
        Returns:
            str: 检测到的编码格式（如 'utf-8', 'gbk'），默认返回 'utf-8'
            
        Examples:
            >>> # 用于MinIO文件流
            >>> response = minio_client.get_file_stream(bucket, file_name)
            >>> encoding = TextEncodingUtils.detect_encoding(response)
            
            >>> # 用于本地文件流
            >>> with open('file.txt', 'rb') as f:
            >>>     encoding = TextEncodingUtils.detect_encoding(f)
            
            >>> # 用于网络文件流
            >>> response = requests.get(url, stream=True)
            >>> encoding = TextEncodingUtils.detect_encoding(response.raw)
        """
        if sample_size is None:
            sample_size = TextEncodingUtils.ENCODING_DETECT_SAMPLE_SIZE
        
        try:
            # 保存当前位置
            original_pos = file_stream.tell() if hasattr(file_stream, 'tell') else None
            
            # 读取采样数据
            sample = file_stream.read(sample_size)
            
            # 恢复位置
            if original_pos is not None and hasattr(file_stream, 'seek'):
                file_stream.seek(original_pos)
            
            # 尝试各种编码
            for encoding in TextEncodingUtils.DEFAULT_ENCODINGS:
                try:
                    sample.decode(encoding)
                    return encoding
                except UnicodeDecodeError:
                    continue
            
            # 默认返回UTF-8
            return 'utf-8'
        except Exception:
            # 发生任何异常时返回默认编码
            return 'utf-8'
    
    @staticmethod
    def decode_chunk_safe(buffer: bytes, encoding: str) -> Tuple[Optional[str], bytes]:
        """
        安全地解码字节块
        
        处理字节块可能在字符中间截断的情况（常见于流式读取）。
        对于多字节编码（如UTF-8、GBK），如果字节块在字符中间截断，
        会导致解码失败。此方法会尝试找到安全的截断点。
        
        Args:
            buffer: 待解码的字节缓冲区
            encoding: 编码格式（如 'utf-8', 'gbk'）
            
        Returns:
            Tuple[Optional[str], bytes]: 
                - decoded_text: 解码后的文本，如果无法解码则返回 None
                - remaining_buffer: 剩余的字节（可能是截断的字符）
                
        Examples:
            >>> # 流式读取文件时使用
            >>> buffer = b''
            >>> while True:
            >>>     chunk = file_stream.read(1024)
            >>>     if not chunk:
            >>>         break
            >>>     buffer += chunk
            >>>     text, remaining = TextEncodingUtils.decode_chunk_safe(buffer, 'utf-8')
            >>>     if text:
            >>>         process_text(text)
            >>>     buffer = remaining
        """
        if not buffer:
            return None, b''
        
        # 尝试从后往前找到可以解码的位置
        # 对于多字节编码（如UTF-8），最多需要检查4个字节；对于GBK，最多需要2个字节
        # 为了安全，检查最后 MAX_CHECK_BYTES 个字节
        max_check_bytes = min(TextEncodingUtils.MAX_CHECK_BYTES, len(buffer))
        
        for i in range(len(buffer), max(0, len(buffer) - max_check_bytes), -1):
            try:
                decoded = buffer[:i].decode(encoding)
                remaining = buffer[i:]
                return decoded, remaining
            except UnicodeDecodeError:
                continue
        
        # 如果都失败，尝试使用错误处理策略
        try:
            decoded = buffer.decode(encoding, errors='ignore')
            return decoded, b''
        except Exception:
            return None, buffer
    
    @staticmethod
    def decode_chunk(buffer: bytes, encoding: str, fallback_encodings: list = None) -> Optional[str]:
        """
        解码字节块（简单版本）
        
        尝试使用指定编码解码，如果失败则尝试备用编码。
        
        Args:
            buffer: 待解码的字节缓冲区
            encoding: 主要编码格式
            fallback_encodings: 备用编码列表，默认使用 DEFAULT_ENCODINGS
            
        Returns:
            Optional[str]: 解码后的文本，如果所有编码都失败则返回 None
        """
        if not buffer:
            return None
        
        if fallback_encodings is None:
            fallback_encodings = TextEncodingUtils.DEFAULT_ENCODINGS
        
        # 尝试主要编码
        encodings_to_try = [encoding] + [e for e in fallback_encodings if e != encoding]
        
        for enc in encodings_to_try:
            try:
                return buffer.decode(enc)
            except UnicodeDecodeError:
                continue
        
        # 所有编码都失败，使用错误处理策略
        try:
            return buffer.decode(encoding, errors='ignore')
        except Exception:
            return None

