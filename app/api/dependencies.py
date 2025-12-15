"""
API Dependencies

Provides dependency injection for services and database sessions.
This centralizes service creation and management for API endpoints.
"""

from functools import lru_cache
from typing import Generator, Optional
from fastapi import Depends
from sqlalchemy.orm import Session

from app.db.session import SessionLocal
from app.services import (
    FileService,
    StorageService, 
    KnowledgeService,
    FileProcessingService
)


def get_db() -> Generator[Session, None, None]:
    """
    Get database session
    
    This is the standard database dependency used throughout the application.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_file_service(db: Session = Depends(get_db)) -> FileService:
    """
    Get File Service instance with database session
    
    Returns:
        FileService: Configured file service
    """
    return FileService(db=db)


def get_storage_service(db: Session = Depends(get_db)) -> StorageService:
    """
    Get Storage Service instance with database session
    
    Returns:
        StorageService: Configured storage service
    """
    return StorageService(db=db)


def get_knowledge_service(db: Session = Depends(get_db)) -> KnowledgeService:
    """
    Get Knowledge Service instance with database session
    
    Returns:
        KnowledgeService: Configured knowledge service
    """
    return KnowledgeService(db=db)


def get_file_processing_service(db: Session = Depends(get_db)) -> FileProcessingService:
    """
    Get File Processing Service instance with database session
    
    Returns:
        FileProcessingService: Configured file processing service
    """
    return FileProcessingService(db=db)


class ServiceContainer:
    """
    Service container for grouping related services
    
    This provides a convenient way to inject multiple services at once
    for endpoints that need multiple service types.
    """
    
    def __init__(
        self,
        file_service: FileService,
        storage_service: StorageService,
        knowledge_service: KnowledgeService,
        processing_service: FileProcessingService
    ):
        self.file = file_service
        self.storage = storage_service
        self.knowledge = knowledge_service
        self.processing = processing_service


def get_services(
    file_service: FileService = Depends(get_file_service),
    storage_service: StorageService = Depends(get_storage_service),
    knowledge_service: KnowledgeService = Depends(get_knowledge_service),
    processing_service: FileProcessingService = Depends(get_file_processing_service)
) -> ServiceContainer:
    """
    Get all services in a container
    
    This is useful for endpoints that need multiple services.
    
    Returns:
        ServiceContainer: Container with all services
    """
    return ServiceContainer(
        file_service=file_service,
        storage_service=storage_service,
        knowledge_service=knowledge_service,
        processing_service=processing_service
    )
