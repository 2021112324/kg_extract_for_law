# Object Storage Infrastructure

对象存储基础设施层，提供统一的对象存储抽象接口，支持多种云存储提供商。

## 概述

这个模块实现了对象存储的抽象层，支持多种对象存储后端。目前支持 MinIO，并为未来扩展其他云存储服务（如 AWS S3、阿里云 OSS、腾讯云 COS 等）预留了接口。

通过抽象接口设计，应用程序可以轻松在不同的对象存储提供商之间切换，而无需修改业务逻辑代码。

## 架构设计

```
app/infrastructure/storage/object_storage/
├── __init__.py          # 模块导出
├── base.py              # 抽象接口和数据结构
├── minio_adapter.py     # MinIO 实现
├── factory.py           # 工厂模式和配置管理
└── README.md            # 本文档
```

### 核心组件

#### 1. ObjectStorageInterface (抽象基类)

定义了所有对象存储必须实现的接口：

```python
class ObjectStorageInterface(ABC):
    def ensure_bucket_exists(bucket_name: str) -> bool
    def upload_file_object(file_data, bucket_name, object_name, content_type) -> bool  
    def upload_file_stream(file_stream, bucket_name, object_name, ...) -> bool
    def upload_file_path(local_path, bucket_name, object_name) -> bool
    def download_file(bucket_name, object_name, local_path) -> bool
    def get_file_stream(bucket_name, object_name) -> Optional[BinaryIO]
    def get_file_url(bucket_name, object_name, expires) -> Optional[str]
    def delete_file(bucket_name, object_name) -> bool
    def list_files(bucket_name, prefix) -> List[FileMetadata]
    def file_exists(bucket_name, object_name) -> bool
    def get_file_metadata(bucket_name, object_name) -> Optional[FileMetadata]
    def initialize() -> bool
```

#### 2. StorageConfig (配置数据类)

标准化的存储配置格式：

```python
@dataclass
class StorageConfig:
    endpoint: str
    access_key: str
    secret_key: str
    secure: bool = True
    region: Optional[str] = None
    
    # 桶配置
    raw_bucket: str = "raw-files"
    processed_bucket: str = "processed-files"  
    image_bucket: str = "image-files"
```

#### 3. FileMetadata (文件元数据)

统一的文件元数据结构：

```python
@dataclass
class FileMetadata:
    object_name: str
    size: int
    content_type: Optional[str] = None
    last_modified: Optional[str] = None
    etag: Optional[str] = None
```

#### 4. MinIOAdapter (MinIO实现)

MinIO 对象存储的具体实现，支持：
- 环境变量配置
- 自动桶管理
- 文件上传/下载/删除
- 预签名URL生成
- 文件流处理

#### 5. StorageFactory (工厂类)

提供工厂模式创建存储实例：
- 根据配置创建相应的存储适配器
- 支持默认配置从环境变量加载
- 为多种存储类型提供统一创建入口

## 使用方法

### 基本使用

```python
from app.infrastructure.storage.object_storage import StorageFactory

# 获取默认存储实例（MinIO）
storage = StorageFactory.get_default_storage()

# 初始化存储（创建必要的桶）
storage.initialize()

# 上传文件
success = storage.upload_file_path(
    local_path="/path/to/file.pdf",
    bucket_name=storage.config.raw_bucket,
    object_name="documents/file.pdf"
)

# 下载文件
success = storage.download_file(
    bucket_name=storage.config.raw_bucket,
    object_name="documents/file.pdf",
    local_path="/path/to/download/file.pdf"
)

# 生成预签名URL
url = storage.get_file_url(
    bucket_name=storage.config.raw_bucket,
    object_name="documents/file.pdf",
    expires=3600  # 1小时过期
)

# 获取文件流
file_stream = storage.get_file_stream(
    bucket_name=storage.config.raw_bucket,
    object_name="documents/file.pdf"
)

# 列出文件
files = storage.list_files(
    bucket_name=storage.config.raw_bucket,
    prefix="documents/"
)

# 删除文件
success = storage.delete_file(
    bucket_name=storage.config.raw_bucket,
    object_name="documents/file.pdf"
)
```

### 在服务层中使用

```python
from app.infrastructure.storage.object_storage import StorageFactory
from app.services.core.storage_service import StorageService

# 通过依赖注入使用自定义存储
custom_storage = StorageFactory.create_storage("minio", custom_config)
storage_service = StorageService(storage=custom_storage)

# 使用默认配置
storage_service = StorageService()  # 自动使用默认MinIO配置
```

### 指定存储类型

```python
from app.infrastructure.storage.object_storage import StorageFactory, StorageConfig

# 创建自定义MinIO配置
config = StorageConfig(
    endpoint="my-minio.company.com:9000",
    access_key="my_access_key",
    secret_key="my_secret_key",
    secure=True,
    raw_bucket="my-raw-files"
)

# 创建MinIO存储实例
storage = StorageFactory.create_storage("minio", config)
```

## 配置

### MinIO 配置

通过环境变量或 `.env` 文件配置：

```env
# MinIO 服务配置
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=your_access_key
MINIO_SECRET_KEY=your_secret_key
MINIO_SECURE=false

# 桶配置
RAW_BUCKET=raw-files
PROCESSED_BUCKET=processed-files
IMAGE_BUCKET=image-files
```

### 动态配置

```python
from app.infrastructure.storage.object_storage import StorageConfig, StorageFactory

# 代码中指定配置
config = StorageConfig(
    endpoint="localhost:9000",
    access_key="minioadmin", 
    secret_key="minioadmin",
    secure=False,
    raw_bucket="documents",
    processed_bucket="processed-docs",
    image_bucket="images"
)

storage = StorageFactory.create_storage("minio", config)
```

## 扩展新的存储提供商

### 1. 创建适配器实现类

```python
# app/infrastructure/storage/object_storage/s3_adapter.py
import boto3
from .base import ObjectStorageInterface, StorageConfig, FileMetadata

class S3Adapter(ObjectStorageInterface):
    def __init__(self, config: StorageConfig):
        super().__init__(config)
        self.client = boto3.client(
            's3',
            aws_access_key_id=config.access_key,
            aws_secret_access_key=config.secret_key,
            region_name=config.region,
            endpoint_url=f"{'https' if config.secure else 'http'}://{config.endpoint}"
        )
    
    def upload_file_path(self, local_path: str, bucket_name: str, object_name: str) -> bool:
        try:
            self.client.upload_file(local_path, bucket_name, object_name)
            return True
        except Exception as e:
            logger.error(f"S3 upload failed: {e}")
            return False
    
    def download_file(self, bucket_name: str, object_name: str, local_path: str) -> bool:
        try:
            self.client.download_file(bucket_name, object_name, local_path)
            return True
        except Exception as e:
            logger.error(f"S3 download failed: {e}")
            return False
    
    # ... 实现其他抽象方法
```

### 2. 注册到工厂类

```python
# app/infrastructure/storage/object_storage/factory.py
from .s3_adapter import S3Adapter

class StorageFactory:
    @staticmethod
    def create_storage(storage_type: str = "minio", config: Optional[StorageConfig] = None):
        if storage_type.lower() == "minio":
            return MinIOAdapter(config)
        elif storage_type.lower() == "s3":
            return S3Adapter(config)
        # elif storage_type.lower() == "oss":
        #     return OSSAdapter(config)
        else:
            raise ValueError(f"Unsupported storage type: {storage_type}")
```

### 3. 更新模块导出

```python
# app/infrastructure/storage/object_storage/__init__.py
from .s3_adapter import S3Adapter

__all__ = [
    'ObjectStorageInterface',
    'StorageConfig',
    'FileMetadata', 
    'MinIOAdapter',
    'S3Adapter',  # 添加新适配器
    'StorageFactory'
]
```

## 与现有代码集成

### 向后兼容

现有使用 `app.core.minio_client` 的代码会继续工作，但会收到弃用警告：

```python
# 旧代码（仍然工作，但有警告）
from app.core.minio_client import upload_file_minIO, RAW_BUCKET
# ⚠️ DeprecationWarning: app.core.minio_client is deprecated

# 新代码（推荐）
from app.infrastructure.storage.object_storage import StorageFactory
storage = StorageFactory.get_default_storage()
storage.upload_file_path(local_path, bucket, object_name)
```

### StorageService 集成

`StorageService` 已经重构为使用新的抽象接口：

```python
from app.services.core.storage_service import StorageService

# 使用默认配置
storage_service = StorageService()

# 或者注入自定义存储
custom_storage = StorageFactory.create_storage("minio", custom_config)
storage_service = StorageService(storage=custom_storage)

# 使用服务层方法
result = await storage_service.upload_to_storage(
    file_path="/path/to/file.pdf",
    object_name="documents/file.pdf"
)
```

## 最佳实践

### 1. 错误处理

```python
from app.infrastructure.storage.object_storage import StorageFactory

def safe_storage_operation():
    try:
        storage = StorageFactory.get_default_storage()
        
        # 测试连接
        if not storage.initialize():
            logger.error("存储服务初始化失败")
            return False
        
        # 执行操作
        success = storage.upload_file_path(local_path, bucket, object_name)
        return success
        
    except Exception as e:
        logger.error(f"存储操作失败: {e}")
        return False
```

### 2. 配置验证

```python
def validate_storage_config():
    """在应用启动时验证存储配置"""
    try:
        storage = StorageFactory.get_default_storage()
        if not storage.initialize():
            raise RuntimeError("无法初始化存储服务")
        logger.info("✅ 存储服务配置验证通过")
    except Exception as e:
        logger.error(f"❌ 存储服务配置验证失败: {e}")
        raise
```

### 3. 资源管理

```python
def upload_with_cleanup(file_path: str, bucket: str, object_name: str):
    """上传文件并清理临时资源"""
    storage = StorageFactory.get_default_storage()
    
    try:
        success = storage.upload_file_path(file_path, bucket, object_name)
        if success:
            logger.info(f"文件上传成功: {object_name}")
        return success
    finally:
        # 清理临时文件
        if os.path.exists(file_path) and file_path.startswith("/tmp/"):
            os.remove(file_path)
```

### 4. 批量操作

```python
def batch_upload_files(files: List[Tuple[str, str, str]]):
    """批量上传文件"""
    storage = StorageFactory.get_default_storage()
    results = []
    
    for local_path, bucket, object_name in files:
        success = storage.upload_file_path(local_path, bucket, object_name)
        results.append({
            'object_name': object_name,
            'success': success
        })
    
    return results
```

## 故障排除

### 常见问题

1. **"Unsupported storage type" 错误**
   - 检查传入的 storage_type 参数是否正确
   - 确认相应的适配器类是否已实现并注册

2. **连接失败**
   - 验证 endpoint、access_key、secret_key 配置
   - 检查网络连接和防火墙设置
   - 确认存储服务是否正常运行

3. **桶操作失败**
   - 检查桶名称是否符合命名规范
   - 验证访问权限配置
   - 确认存储配额是否足够

4. **文件上传/下载失败**
   - 检查文件路径是否存在且可访问
   - 验证文件大小是否超过限制
   - 确认对象名称是否合法

### 调试模式

```python
import logging

# 启用详细日志
logging.getLogger('app.infrastructure.storage.object_storage').setLevel(logging.DEBUG)

# 测试连接
storage = StorageFactory.get_default_storage()
if storage.initialize():
    print("✅ 存储连接正常")
else:
    print("❌ 存储连接失败")
```

### 健康检查

```python
def health_check_storage():
    """存储服务健康检查"""
    try:
        storage = StorageFactory.get_default_storage()
        
        # 测试连接
        if not storage.initialize():
            return {"status": "unhealthy", "error": "初始化失败"}
        
        # 测试基本操作
        test_bucket = storage.config.raw_bucket
        if not storage.ensure_bucket_exists(test_bucket):
            return {"status": "unhealthy", "error": "桶操作失败"}
        
        return {"status": "healthy", "storage_type": type(storage).__name__}
        
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}
```

## 性能优化

### 1. 连接复用

工厂模式自动缓存存储实例，避免重复创建连接：

```python
# 多次调用会返回同一个实例
storage1 = StorageFactory.get_default_storage()
storage2 = StorageFactory.get_default_storage()
assert storage1 is storage2
```

### 2. 批量操作

尽可能使用批量操作提高效率：

```python
# ✅ 推荐：批量操作
file_list = storage.list_files(bucket, prefix="documents/")

# ❌ 避免：逐个检查
for doc_name in doc_names:
    exists = storage.file_exists(bucket, doc_name)
```

### 3. 流式处理

对于大文件，使用流式处理避免内存占用过高：

```python
# 使用文件流上传
with open(large_file_path, 'rb') as f:
    storage.upload_file_stream(f, bucket, object_name)

# 而不是加载到内存
with open(large_file_path, 'rb') as f:
    data = f.read()  # ❌ 可能导致内存不足
    storage.upload_file_object(data, bucket, object_name)
```

## 迁移指南

### 从 minio_client.py 迁移

#### 第一步：更新导入

```python
# 旧版本
from app.core.minio_client import upload_file_minIO, RAW_BUCKET, get_file_url

# 新版本
from app.infrastructure.storage.object_storage import StorageFactory

storage = StorageFactory.get_default_storage()
```

#### 第二步：更新函数调用

```python
# 旧版本
success = upload_file_minIO(local_path, RAW_BUCKET, object_name)
url = get_file_url(RAW_BUCKET, object_name)

# 新版本  
success = storage.upload_file_path(local_path, storage.config.raw_bucket, object_name)
url = storage.get_file_url(storage.config.raw_bucket, object_name)
```

#### 第三步：使用服务层（推荐）

```python
# 更高层次的抽象
from app.services.core.storage_service import StorageService

storage_service = StorageService()
result = await storage_service.upload_to_storage(local_path, object_name)
if result['success']:
    url = result['data']['url']
```

## 版本历史

- **v1.0.0** - 初始版本，重构自 minio_client.py
  - 实现抽象接口 ObjectStorageInterface
  - 添加 MinIOAdapter 实现
  - 创建 StorageFactory 工厂类
  - 提供向后兼容性支持

- **v1.1.0** (计划中)
  - 添加 AWS S3 适配器
  - 添加阿里云 OSS 适配器
  - 支持异步操作

- **v2.0.0** (未来版本)
  - 移除向后兼容代码
  - 添加更多云存储提供商支持
  - 支持存储策略和负载均衡

## 贡献指南

1. 新增存储适配器必须实现 `ObjectStorageInterface` 的所有抽象方法
2. 添加完整的错误处理和日志记录
3. 包含单元测试覆盖所有方法
4. 更新相关文档和示例
5. 确保向后兼容性
6. 遵循现有的代码风格和命名约定

## 许可证

本模块遵循项目整体的许可证协议。