"""
Format Processor Module

Provides file format processing capabilities for PDF, Office documents, images and text files.
"""

from .base_processor import BaseProcessor, ProcessResult
from .pdf_processor import PDFProcessor
from .office_processor import OfficeProcessor
from .image_processor import ImageProcessor
from .text_processor import TextProcessor
from .processor_factory import (
    ProcessorFactory,
    get_processor_factory,
    process_file,
    supports_file,
    get_supported_extensions
)

__all__ = [
    # Base classes
    'BaseProcessor',
    'ProcessResult',
    
    # Format processors
    'PDFProcessor',
    'OfficeProcessor', 
    'ImageProcessor',
    'TextProcessor',
    
    # Factory class and utility functions
    'ProcessorFactory',
    'get_processor_factory',
    'process_file',
    'supports_file',
    'get_supported_extensions'
]