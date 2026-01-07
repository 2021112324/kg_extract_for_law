# TextEncodingUtils 使用示例

`TextEncodingUtils` 是一个通用的文本编码工具类，提供了文本编码检测和安全解码功能。它**不依赖具体的存储方式**，可以用于任何实现了流接口的对象。

## 适用场景

### ✅ 1. MinIO 对象存储流

```python
from app.utils.text_encoding_utils import TextEncodingUtils
from app.infrastructure.storage.object_storage import StorageFactory

# 获取 MinIO 文件流
storage = StorageFactory.get_default_storage()
response = storage.get_file_stream(bucket_name, file_name)

# 检测编码
encoding = TextEncodingUtils.detect_encoding(response)

# 流式读取并安全解码
buffer = b''
while True:
    chunk = response.read(1024 * 1024)  # 每次读取1MB
    if not chunk:
        break
    buffer += chunk
    text, remaining = TextEncodingUtils.decode_chunk_safe(buffer, encoding)
    if text:
        # 处理文本
        process_text(text)
    buffer = remaining
```

### ✅ 2. 本地文件流

```python
from app.utils.text_encoding_utils import TextEncodingUtils

# 打开本地文件
with open('large_file.txt', 'rb') as f:
    # 检测编码
    encoding = TextEncodingUtils.detect_encoding(f)

    # 流式读取并安全解码
    buffer = b''
    while True:
        chunk = f.read(1024 * 1024)  # 每次读取1MB
        if not chunk:
            break
        buffer += chunk
        text, remaining = TextEncodingUtils.decode_chunk_safe(buffer, encoding)
        if text:
            # 处理文本
            process_text(text)
        buffer = remaining
```

### ✅ 3. 网络文件流（HTTP/HTTPS）

```python
import requests
from app.utils.text_encoding_utils import TextEncodingUtils

# 下载大文件
response = requests.get('https://example.com/large_file.txt', stream=True)

# 检测编码
encoding = TextEncodingUtils.detect_encoding(response.raw)

# 流式读取并安全解码
buffer = b''
for chunk in response.iter_content(chunk_size=1024 * 1024):  # 每次1MB
    if chunk:
        buffer += chunk
        text, remaining = TextEncodingUtils.decode_chunk_safe(buffer, encoding)
        if text:
            # 处理文本
            process_text(text)
        buffer = remaining
```

### ✅ 4. 内存字节流

```python
from io import BytesIO
from app.utils.text_encoding_utils import TextEncodingUtils

# 从内存中的字节数据创建流
byte_data = b'\xe4\xb8\xad\xe6\x96\x87...'  # UTF-8编码的中文
stream = BytesIO(byte_data)

# 检测编码
encoding = TextEncodingUtils.detect_encoding(stream)

# 解码
text = TextEncodingUtils.decode_chunk(byte_data, encoding)
```

### ✅ 5. 其他实现了流接口的对象

只要对象实现了 `read()`, `tell()`, `seek()` 方法，就可以使用：

```python
class CustomStream:
    def __init__(self, data):
        self.data = data
        self.pos = 0
    
    def read(self, size=-1):
        if size == -1:
            result = self.data[self.pos:]
            self.pos = len(self.data)
        else:
            result = self.data[self.pos:self.pos + size]
            self.pos += len(result)
        return result
    
    def tell(self):
        return self.pos
    
    def seek(self, pos):
        self.pos = pos

# 使用自定义流
custom_stream = CustomStream(b'...')
encoding = TextEncodingUtils.detect_encoding(custom_stream)
```

## 方法说明

### `detect_encoding(file_stream, sample_size=None)`

检测文件流的编码格式。

**参数：**
- `file_stream`: 文件流对象（需要实现 `read()`, `tell()`, `seek()` 方法）
- `sample_size`: 采样大小（字节），默认 4096

**返回：**
- `str`: 检测到的编码格式（'utf-8', 'gbk' 等），默认返回 'utf-8'

**特点：**
- 不会改变文件流的当前位置
- 支持多种编码格式检测
- 异常安全，发生错误时返回默认编码

### `decode_chunk_safe(buffer, encoding)`

安全地解码字节块，处理字符中间截断的情况。

**参数：**
- `buffer`: 待解码的字节缓冲区
- `encoding`: 编码格式

**返回：**
- `Tuple[Optional[str], bytes]`: (解码后的文本, 剩余的字节)

**特点：**
- 处理多字节编码截断问题
- 适用于流式读取场景
- 自动找到安全的截断点

### `decode_chunk(buffer, encoding, fallback_encodings=None)`

简单版本的解码方法。

**参数：**
- `buffer`: 待解码的字节缓冲区
- `encoding`: 主要编码格式
- `fallback_encodings`: 备用编码列表

**返回：**
- `Optional[str]`: 解码后的文本，失败返回 None

## 设计优势

1. **通用性**：不依赖具体的存储方式，适用于任何流对象
2. **安全性**：处理多字节编码截断问题，避免解码错误
3. **易用性**：简单的静态方法，无需实例化
4. **可扩展性**：可以轻松添加新的编码支持

## 注意事项

1. `detect_encoding` 方法需要流对象支持 `seek()` 方法，如果不支持，会尝试重新获取流
2. `decode_chunk_safe` 最多检查最后 20 个字节，对于非常大的字符可能需要调整
3. 默认支持的编码是 UTF-8 和 GBK，可以通过修改 `DEFAULT_ENCODINGS` 添加更多编码

