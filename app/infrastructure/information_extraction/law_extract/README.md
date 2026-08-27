# 中文法规知识图谱抽取

中文法规目录抽取提供两种运行方式。两种方式均使用现有 `ClauseExtractor` 完成大模型抽取，并将结果写入 Neo4j；区别只在于是否使用 MySQL 保存知识库元数据和任务状态。

## 独立模式

仅需要最终 Neo4j 图谱时，使用以下接口：

```text
POST /api/kg/kgs/clause_extract_by_dir/standalone
```

请求参数：

- `data_dir`：本地中文法规文件目录，仅处理该目录中的 `.txt` 和 `.md` 文件。
- `if_del_task`：合并成功后是否删除文件级临时子图，默认为 `false`。

接口受理任务后立即返回：

```json
{
  "code": 200,
  "data": {
    "kg_graph_name": "目标图谱名称",
    "use_mysql": false
  },
  "msg": "无 MySQL 中文法规抽取任务开始执行"
}
```

`kg_graph_name` 是最终图谱在 Neo4j 中使用的标签，也是后续定位和导出图谱的依据。任务完成后，日志会输出文件总数、成功数、失败数、错误清单和抽取器警告统计。

独立模式具有以下边界：

- 不创建 MySQL 知识库记录或文件级任务记录。
- 不读取或写入 MinIO。
- 仅将大模型返回的有效图谱且成功写入 Neo4j 的文件计为成功。
- 没有任何文件成功入库时，任务明确失败，不生成虚假的成功结果。
- 大模型服务和 Neo4j 仍是必需依赖。

独立部署建议使用以下配置：

```dotenv
MYSQL_SKIP_INIT=true
MINIO_SKIP_INIT=true
```

`MYSQL_SKIP_INIT` 只控制应用启动时是否初始化 MySQL，不会让原有管理接口自动切换为独立模式。`MINIO_SKIP_INIT` 控制应用启动和服务初始化时是否初始化对象存储。

## 管理模式

需要在 MySQL 中保留知识库元数据和任务状态时，继续使用原接口：

```text
POST /api/kg/kgs/{kg_id}/clause_extract_by_dir
```

该接口保持原有行为，需要可用的 MySQL 会话，并在完成文件级抽取后从任务记录中汇总子图。即使部署启用了 `MYSQL_SKIP_INIT=true`，调用该接口时仍必须保证 MySQL 可用。

## 运行环境

当前项目使用的 Conda 环境 Python 路径为：

```text
E:\Anaconda3\envs\cogmait312\python.exe
```

启动服务后，可在日志中确认出现以下信息：

```text
MYSQL_SKIP_INIT=true，跳过MySQL初始化
MINIO_SKIP_INIT=true，跳过MinIO初始化
```
