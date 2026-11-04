# 知识图谱自然语言转换模块

本目录按职责拆分代码与资源：

```text
kg_to_natural_language/
├── converters/  # 各类图谱的自然语言转换器及统一分发
├── graph/       # Neo4j 图谱导出
├── batch/       # 分类配置与批量转换编排
├── risk/        # 三级风险指标解析、图谱查询、知识生成、排序与统计
│   └── tree/    # 风险指标映射树及同步工具
├── data/        # 正式输出数据
├── temp/        # 转换中间结果
├── docs/
│   ├── design/  # 转换规则和使用说明
│   └── tasks/   # 任务及实施报告
└── __init__.py  # 对外功能入口和旧模块路径兼容层
```

## 使用约定

- 新代码应从 `converters`、`graph`、`batch` 或 `risk` 对应子包导入实现。
- 外部调用优先使用包根目录 `__init__.py` 暴露的公共函数。
- 原有模块路径已通过兼容别名保留，现有调用方可以逐步迁移，不需要一次性修改。
- 运行数据只写入 `data` 或 `temp`，不要与实现代码和设计文档混放。

## 全量风险指标知识处理

`process_all_risk_indicator_knowledge()` 会遍历指标树中的全部三级指标，依次执行映射文件知识生成、关联度评分和排序：

```python
import asyncio

from app.infrastructure.kg_to_natural_language import (
    process_all_risk_indicator_knowledge,
)

summary = asyncio.run(process_all_risk_indicator_knowledge())
```

单指标知识写入 `data/risk/<指标序号>/all.txt`，全量进度和结果汇总写入 `data/risk/处理汇总.json`。指标之间顺序处理，每个指标内部按模型配置并发评分；单个指标失败时默认继续处理后续指标。

全量处理默认支持断点续跑：若 `data/risk/<指标序号>/all.txt` 已存在且包含知识条目，则认为该指标已经生成最终成果并跳过；如果最终目录中没有有效的 `all.txt`，即使 `temp/risk/<指标序号>` 中已有临时数据，也会重新执行该指标。调用函数时传入 `force=True`，或执行脚本时使用 `--force`，可以忽略已有最终成果并重跑全部指标。

## 风险指标知识条目数统计

`build_risk_indicator_knowledge_statistics()` 根据风险指标树和 `temp/risk` 中的阶段一成果，统计三级以及风险根、一级、二级层次的知识数量：

```python
from app.infrastructure.kg_to_natural_language import (
    build_risk_indicator_knowledge_statistics,
)

result = build_risk_indicator_knowledge_statistics()
```

默认生成 `data/stats/知识条目数统计.json`。三级指标按实际唯一知识计数，上级节点按规范化来源文件去重，避免同一文件映射到多个后代指标时重复累计。该功能仅读取本地 JSON，不访问 Neo4j、模型、MySQL 或 MinIO。

## 可选：知识条目单表存储

新增入口 `app/infrastructure/kg_to_natural_language/run_storage.py`，修改文件内配置后直接运行。默认只生成结构化预览，不写数据库；原有文件导出和风险知识接口不强制依赖 MySQL。

显式执行入库时，只新增 `t_natural_knowledge_entry` 一张 MySQL 知识表。来源、风险、行业和两类指标作为同一行的 JSON 属性保存，支持版本化查询；`validity_status` 可空，来源文件指标仅在显式启用的查询中作为保底，不覆盖正式指标。

详见[单表知识存储使用指南](tasks/task/实现/任务1103_单表知识存储使用指南.md)。

若要一次读取“法律法规条款、国家标准、合规指引、行政监管规则”并直接入库，可执行 `app/infrastructure/kg_to_natural_language/run_four_graphs_to_mysql.py`。该专用入口无需手动切换 prepare/import，会自动保留冻结运行并在再次执行时核对续跑；先在文件开头填写实际 MySQL 8 地址、端口和账号，密码由环境变量或终端隐藏输入提供。脚本不会安装或启动 MySQL，也不会改写源图谱。
