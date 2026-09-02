# `kg_to_natural_language.__init__.py` 接口功能详细说明

本文档依据当前代码实现编写，说明
`app.infrastructure.kg_to_natural_language` 包对外暴露的接口、调用关系、输入输出、产物位置及运行注意事项。

## 第一章 接口功能简述与产物位置

### 1.1 模块作用

`kg_to_natural_language` 是知识图谱向自然语言知识条目转换的统一入口。调用方只需从包级入口导入函数，无需了解导出器、分类转换器、批处理器和风险指标处理器的内部目录结构。

该模块提供以下六类能力：

1. 从 Neo4j 按标签导出节点和关系，生成 `node.json`、`edge.json`。
2. 将已有图谱 JSON 转换为自然语言知识条目，并生成统计文件。
3. 自动识别中文法规、国家标准、英文法规和合规指引图谱，分发给对应转换器。
4. 一次性处理“法律法规条款、国家标准、行政监管规则、英文法规、合规指引”五类图谱，发布分类知识条目。
5. 根据三级风险指标映射文件生成知识条目，调用大模型计算关联度，并发布按关联度排序的最终知识文本。
6. 根据指标树和阶段一成果，统计三级及上级风险指标的知识条目数量，并在父级范围内对重复映射文件去重。

模块主要处理 Neo4j 和本地文件，不依赖 MySQL、MinIO。只有风险知识关联度评分阶段需要访问大模型服务。

### 1.2 对外接口概览

| 功能组 | 对外接口 | 主要作用 |
| --- | --- | --- |
| 图谱导出 | `clear_temp_dir` | 清理通用临时目录中的 JSON、TXT 和 CSV 文件 |
| 图谱导出 | `export_graph` | 将任意指定 Neo4j 标签导出到默认临时目录 |
| 图谱导出 | `export_graph_to_directory` | 将五类固定标签之一导出到指定目录 |
| 通用转换 | `convert_json_to_text` | 使用旧版通用法规规则转换图谱 JSON |
| 中文法规转换 | `convert_json_to_text_v2` | 使用中文法规“过滤与组装”规则生成知识条目 |
| 国家标准转换 | `convert_national_standard_json_to_text` | 生成国家标准知识条目及结构化元数据 |
| 英文法规转换 | `convert_english_law_json_to_text` | 生成英文法规的英文知识条目及元数据 |
| 合规指引转换 | `convert_compliance_guide_json_to_text` | 生成合规指引知识条目及元数据 |
| 自动路由 | `detect_graph_type` | 根据节点属性中的类型标识判断图谱类别 |
| 自动路由 | `convert_graph_to_text` | 自动识别图谱类别并调用专用转换器 |
| 单图完整处理 | `process_graph` | 从 Neo4j 导出指定标签，并自动路由完成转换 |
| 中文法规兼容处理 | `process_graph_v2` | 从 Neo4j 导出指定标签，并固定使用中文法规 v2 转换器 |
| 五类批处理 | `process_graphs_by_type` | 导出五类固定图谱，分别转换并统一发布 |
| 风险知识阶段一 | `generate_risk_indicator_knowledge` | 生成一个三级指标所映射文件的知识 JSON |
| 风险知识阶段二 | `rank_risk_indicator_knowledge` | 对阶段一知识评分、排序并发布最终 TXT |
| 单指标完整处理 | `process_risk_indicator_knowledge` | 顺序执行单指标的知识生成和评分排序 |
| 全指标完整处理 | `process_all_risk_indicator_knowledge` | 遍历全部三级指标，断点续跑并生成处理汇总 |
| 风险知识统计 | `build_risk_indicator_knowledge_statistics` | 生成三级及上级风险指标的文件去重知识条目数统计 |

### 1.3 产物位置

以下路径均位于：

```text
F:\企业大脑知识库系统\8.1项目\抽取代码\kg_extract_for_law\
app\infrastructure\kg_to_natural_language
```

| 产物类别 | 默认位置 | 内容 |
| --- | --- | --- |
| 通用图谱临时数据 | `temp/json/node.json` | 从 Neo4j 导出的节点数组 |
| 通用图谱临时数据 | `temp/json/edge.json` | 从 Neo4j 导出的关系数组 |
| 通用自然语言知识 | `temp/txt/<图谱类型>.txt` | 每行一条自然语言知识 |
| 转换元数据 | `temp/jsonl/<图谱类型>.jsonl` | 国家标准、英文法规和合规指引的逐条知识元数据 |
| 转换统计 | `temp/csv/` | 知识类型、来源文件、跳过原因和关系问题等统计 |
| 五类正式知识 | `data/type/` | 五个分类 TXT 及 `知识条目统计.json` |
| 单指标阶段一成果 | `temp/risk/<三级指标序号>/` | 每个映射文件的 JSON 和汇总 `all.json` |
| 单指标最终成果 | `data/risk/<三级指标序号>/all.txt` | 按关联度排序的知识条目，仅保留知识正文 |
| 全指标处理汇总 | `data/risk/处理汇总.json` | 全部三级指标的进度、成功、失败和跳过统计 |
| 风险层级知识统计 | `data/stats/知识条目数统计.json` | 三级及上级风险指标的知识数、文件数、覆盖状态和数据诊断 |

当接口传入自定义 `input_dir`、`output_dir`、`temp_output_root` 或 `data_output_root` 时，产物写入调用方指定的位置，不再使用相应默认目录。

## 第二章 模块结构与调用关系

### 2.1 内部模块分工

```text
kg_to_natural_language/
├── __init__.py             对外接口和兼容导入入口
├── graph/                  Neo4j 图谱导出
├── converters/             通用及分类型知识转换器
├── batch/                  五类图谱批量转换与成果发布
├── risk/                   指标树解析、图谱查询、知识生成与评分排序
├── temp/                   临时图谱、转换结果及阶段一风险知识
├── data/                   正式分类知识和最终风险知识
└── docs/                   设计、任务和核心说明文档
```

### 2.2 主要调用链

```text
process_graph
  ├── clear_temp_dir
  ├── export_graph
  └── convert_graph_to_text
        ├── detect_graph_type
        └── 对应类别转换器

process_graphs_by_type
  └── process_typed_knowledge_entries
        ├── export_graph_to_directory
        ├── 对应类别转换器
        ├── 结果一致性校验
        └── data/type 原子发布

process_risk_indicator_knowledge
  ├── generate_risk_indicator_knowledge
  │     ├── 读取风险指标树
  │     ├── 查询五类 Neo4j 文件子图
  │     └── 调用对应类别转换器
  └── rank_risk_indicator_knowledge
        ├── 并发调用大模型评分
        ├── 回写 all.json
        └── 发布 data/risk/<指标>/all.txt

process_all_risk_indicator_knowledge
  └── 对全部三级指标调用 process_risk_indicator_knowledge

build_risk_indicator_knowledge_statistics
  ├── 一次读取 risk/tree/risk2file_tree.json
  ├── 校验 temp/risk/<指标>/all.json
  ├── 按来源文件去重并完成层级聚合
  └── 原子发布 data/stats/知识条目数统计.json
```

### 2.3 包级兼容机制

`__init__.py` 除了暴露函数，还把旧模块名注册为兼容别名。已有代码仍可通过旧路径导入转换器、导出器和风险处理模块；新代码建议直接从 `app.infrastructure.kg_to_natural_language` 导入公开函数，减少对内部目录的依赖。

## 第三章 图谱导出接口

### 3.1 `clear_temp_dir()`

**作用：** 清空并重建通用临时目录。

```python
from app.infrastructure.kg_to_natural_language import clear_temp_dir

clear_temp_dir()
```

该接口删除 `temp/json`、`temp/txt`、`temp/csv` 目录下的普通文件，但保留目录本身。它不会清理 `temp/risk`、`temp/jsonl`、`data/type` 和 `data/risk`。

**返回值：** 无。

**注意：** 这是有清理副作用的接口。若临时目录中存在仍需保留的转换结果，应在调用前转移或改用自定义输出目录。

### 3.2 `export_graph(tag)`

**作用：** 从 Neo4j 导出指定标签的全部节点，以及这些节点发出的关系。

```python
stats = export_graph("某个Neo4j标签")
```

**输入：**

- `tag`：Neo4j 节点标签字符串，可以是具体图谱批次标签。

**处理流程：**

1. 调用 `clear_temp_dir()` 清理通用临时产物。
2. 从项目配置读取 Neo4j 地址、用户名、密码和数据库名。
3. 校验连接。
4. 查询指定标签的节点。
5. 查询以指定标签节点为起点的关系。
6. 将结果写入默认临时目录。

**产物：**

```text
temp/json/node.json
temp/json/edge.json
```

**返回值：**

```python
{
    "node_count": 100,
    "edge_count": 120,
    "node_path": ".../temp/json/node.json",
    "edge_path": ".../temp/json/edge.json",
}
```

Neo4j 不可达时抛出 `ConnectionError`；查询或写出失败时抛出 `RuntimeError`。

### 3.3 `export_graph_to_directory(tag, output_dir)`

**作用：** 把固定类别标签的图谱导出到指定目录，主要供五类批处理和风险指标知识生成使用。

**允许的标签：**

- `法律法规条款`
- `国家标准`
- `行政监管规则`
- `英文法规`
- `合规指引`

与 `export_graph` 相比，该接口存在两个重要差异：

1. 不清理通用临时目录，直接写入调用方指定目录。
2. 只保留起止节点都带当前类别标签的内部关系。

若传入固定列表之外的标签，接口抛出 `ValueError`。返回值字段与 `export_graph` 一致。

## 第四章 图谱类型识别与转换接口

### 4.1 统一输入约定

所有转换接口默认从 `temp/json` 读取：

```text
node.json
edge.json
```

也可以通过 `input_dir` 指向其他目录。两个文件均应为 JSON 数组，并采用 Neo4j 导出包装结构。`graph_type` 主要用于输出文件命名和统计展示，不等同于 Neo4j 标签。

### 4.2 `detect_graph_type(node_records)`

**作用：** 从节点记录中识别图谱类型。

接口最多抽样前 200 个节点，读取节点 `properties.label`，按以下优先级判断：

1. 英文法规；
2. 合规指引；
3. 国家标准；
4. 中文法规；
5. 无法识别时返回 `unknown`。

**返回值：**

```text
english_law
compliance_guide
national_standard
chinese_law
unknown
```

该接口只判断类型，不读取 Neo4j，也不生成文件。

### 4.3 `convert_graph_to_text(graph_type="知识库", *, input_dir=None, output_dir=None)`

**作用：** 读取图谱 JSON，自动识别类型，并调用对应专用转换器。

路由关系如下：

| 识别结果 | 转换器 |
| --- | --- |
| `chinese_law` | `convert_json_to_text_v2` |
| `national_standard` | `convert_national_standard_json_to_text` |
| `english_law` | `convert_english_law_json_to_text` |
| `compliance_guide` | `convert_compliance_guide_json_to_text` |

无法识别时抛出 `ValueError`。返回值直接沿用被调用转换器的返回字典。

### 4.4 `convert_json_to_text(...)`

**定位：** 旧版通用法规转换接口，主要用于兼容既有调用。

它基于节点属性和关系生成自然语言句子，输出：

```text
<output_dir>/txt/<graph_type>.txt
<output_dir>/csv/知识条目统计.csv
```

返回 `total_entries`、`source_files`、`txt_path` 和 `csv_path`。新中文法规和行政监管规则转换优先使用 `convert_json_to_text_v2`。

### 4.5 `convert_json_to_text_v2(...)`

**定位：** 中文法规和行政监管规则专用转换接口。

该接口按照法规文件、条款单元、引用关系和法条等对象组织知识，执行内容过滤、来源前缀补充、条款序号保留和去重组装，并将法条补充性信息放在核心知识之后输出。

**产物：**

```text
<output_dir>/txt/<graph_type>.txt
<output_dir>/csv/知识条目统计.csv
```

**主要返回字段：**

- `total_entries`：知识条目总数；
- `source_files`：来源文件数；
- `txt_path`、`csv_path`：产物路径；
- `source_context_issue_counts`：来源语境问题统计。

### 4.6 `convert_national_standard_json_to_text(...)`

**定位：** 国家标准专用转换接口。

该接口围绕标准文件信息、术语、要求、指标、检测方法、有效文字描述和引用关系生成知识，执行语义角色校验、图谱规范化、去重及问题统计。表格和流程图只在存在可用文字描述时形成知识，不输出原始表格内容、HTML、流程图代码或图片文件本身。

**产物：**

```text
<output_dir>/txt/<graph_type>.txt
<output_dir>/jsonl/<graph_type>.jsonl
<output_dir>/csv/<graph_type>_知识条目统计.csv
```

TXT 用于检索和后续汇总；JSONL 保留知识编号、类型、来源和位置等元数据；CSV 保存知识类型、来源文件、跳过原因和关系问题统计。

### 4.7 `convert_english_law_json_to_text(...)`

**定位：** 英文法规专用转换接口，最终知识正文为英文。

接口从英文法规文件、法律依据、法条、条款、最小正文单元和外部引用等对象生成英文知识，执行 Schema 属性过滤、来源层级选择、去重和校验统计。

**产物：**

```text
<output_dir>/txt/<graph_type>.txt
<output_dir>/jsonl/<graph_type>.jsonl
<output_dir>/csv/<graph_type>_knowledge_statistics.csv
```

返回值除通用统计和路径外，还包括法律文本候选层级、最终输出层级、过滤属性和校验问题统计。

### 4.8 `convert_compliance_guide_json_to_text(...)`

**定位：** 合规指引专用转换接口。

接口从指引文件、知识单元、责任主体、量化目标、结构文字和引用关系等内容生成知识，执行语义角色校验、图谱规范化、来源语境补充和去重。

**产物：**

```text
<output_dir>/txt/<graph_type>.txt
<output_dir>/jsonl/<graph_type>.jsonl
<output_dir>/csv/<graph_type>_知识条目统计.csv
```

返回知识类型、来源文件、跳过原因、关系问题和来源语境问题等统计。

## 第五章 图谱导出与转换编排接口

### 5.1 `process_graph(tag, graph_type)`

**作用：** 完成“清理临时目录、从 Neo4j 导出、自动识别类别、转换知识”的完整流程。

```python
from app.infrastructure.kg_to_natural_language import process_graph

result = process_graph(
    tag="Neo4j图谱标签",
    graph_type="输出类型名称",
)
```

`tag` 决定查询哪个 Neo4j 图谱；`graph_type` 决定输出文件名和统计名称。转换器不是根据 `graph_type` 参数选择，而是根据导出节点的 `properties.label` 自动识别。

**默认产物：**

- `temp/json/node.json`；
- `temp/json/edge.json`；
- `temp/txt/<graph_type>.txt`；
- 对应转换器支持的 CSV、JSONL。

**返回值：**

```python
{
    "tag": "Neo4j图谱标签",
    "graph_type": "输出类型名称",
    "export": {...导出统计...},
    "convert": {...转换统计...},
}
```

### 5.2 `process_graph_v2(tag, graph_type)`

**作用：** 完成 Neo4j 导出后，固定使用中文法规 v2 转换器。

该接口不进行图谱类型识别，适用于已确认符合中文法规或行政监管规则 Schema 的图谱。若输入实际为国家标准、英文法规或合规指引，不应使用该接口。

产物位置和返回结构与 `process_graph` 相似，但 `convert` 固定来自 `convert_json_to_text_v2`。

### 5.3 `process_graphs_by_type()`

**作用：** 一次性处理五个固定 Neo4j 类别，并发布可直接使用的分类知识库文件。

```python
from app.infrastructure.kg_to_natural_language import process_graphs_by_type

summary = process_graphs_by_type()
```

**固定类别与转换器：**

| Neo4j 标签 | 转换方式 | 正式文件 |
| --- | --- | --- |
| 法律法规条款 | 中文法规 v2 | `法律法规条款.txt` |
| 国家标准 | 国家标准专用转换器 | `国家标准.txt` |
| 行政监管规则 | 中文法规 v2 | `行政监管规则.txt` |
| 英文法规 | 英文法规专用转换器 | `英文法规.txt` |
| 合规指引 | 合规指引专用转换器 | `合规指引.txt` |

**默认正式产物：**

```text
data/type/法律法规条款.txt
data/type/国家标准.txt
data/type/行政监管规则.txt
data/type/英文法规.txt
data/type/合规指引.txt
data/type/知识条目统计.json
```

批处理先在暂存目录完成五类导出、转换和结果一致性校验，全部成功后才替换正式成果。发布失败时尝试恢复上一次完整文件，避免出现只更新部分分类的状态。

若某类别没有节点，会生成空 TXT 并记为零；若图谱存在节点但转换结果为零条知识，则视为结果异常。

## 第六章 风险指标知识接口

### 6.1 风险知识处理的数据基础

默认指标树位于：

```text
risk/tree/risk2file_tree.json
```

接口接收四段点分数字形式的三级指标序号。阶段一以指标树叶节点中非空的“已找到”文件名为准，在以下五类 Neo4j 图谱中查询对应文件：

- 法律法规条款；
- 国家标准；
- 行政监管规则；
- 英文法规；
- 合规指引。

文件子图会按照类别调用对应转换器。风险树、Neo4j 图谱和本地文件是阶段一的全部依赖；阶段一不调用大模型。

### 6.2 `generate_risk_indicator_knowledge(indicator_number, ...)`

**作用：** 执行单个三级指标的阶段一处理。

```python
result = generate_risk_indicator_knowledge("1.1.1.1")
```

**主要参数：**

- `indicator_number`：三级指标序号；
- `risk_tree_path`：可选的指标树路径；
- `output_dir`：可选的阶段一输出目录。

**处理流程：**

1. 校验指标序号并读取指标名称、描述、风险类型和映射文件。
2. 使用文件名及其规范化变体查询五类 Neo4j 图谱。
3. 按文件和图谱类别构建子图。
4. 调用类别转换器生成自然语言知识。
5. 为知识补充来源文件、图谱类别、映射来源和待评分字段。
6. 对重复知识进行合并。
7. 原子发布每个文件的 JSON 和总汇 `all.json`。

**默认产物：**

```text
temp/risk/<三级指标序号>/<映射文件名_短哈希>.json
temp/risk/<三级指标序号>/all.json
```

单文件 JSON 保存该映射文件的图谱及知识统计；`all.json` 汇总全部映射文件知识、未命中文件、跨类别命中、分类统计和警告信息。

### 6.3 `rank_risk_indicator_knowledge(indicator_number, ...)`

**作用：** 执行单个三级指标的阶段二评分和排序。

这是异步接口，需要使用 `await` 或 `asyncio.run()` 调用。

```python
result = asyncio.run(rank_risk_indicator_knowledge("1.1.1.1"))
```

**主要参数：**

- `input_dir`：阶段一 `all.json` 所在目录；
- `output_dir`：最终 `all.txt` 的发布目录；
- `max_concurrency`：单指标内同时评分的最大知识条目数；
- `force`：是否忽略 `all.json` 中已有的成功评分并全部重评；
- `model_config`：可选的显式模型配置对象。

**处理流程：**

1. 读取并校验阶段一 `all.json`。
2. 每次向大模型发送一个指标描述和一条知识。
3. 校验模型返回的 `relevance_score` 和原因。
4. 定期把评分进度回写到 `all.json`。
5. 将成功评分知识排在失败知识之前。
6. 成功知识按关联度从高到低排序；同分时中文知识优先，再保留稳定原始顺序。
7. 将知识正文写入最终 `all.txt`。

**产物：**

```text
temp/risk/<三级指标序号>/all.json
data/risk/<三级指标序号>/all.txt
```

评分和原因保存在 `all.json`；最终 TXT 只保留排序后的知识正文，不附加分数、原因或其他元数据。

### 6.4 `process_risk_indicator_knowledge(indicator_number, ...)`

**作用：** 对一个三级指标顺序执行阶段一和阶段二。

```python
result = asyncio.run(process_risk_indicator_knowledge("1.1.1.1"))
```

阶段一在线程中执行 Neo4j 查询和文件转换，阶段二在异步任务中并发评分。返回值同时包含：

- 指标名称、描述和风险类型；
- 映射文件数、命中文件数和未命中文件数；
- 知识数量、评分成功数和失败数；
- `all.json`、`all.txt` 路径；
- `stage_one`、`stage_two` 两阶段的完整结果。

### 6.5 `process_all_risk_indicator_knowledge(...)`

**作用：** 遍历指标树中的全部三级指标，完成知识生成、评分、排序和汇总。

```python
summary = asyncio.run(process_all_risk_indicator_knowledge())
```

**主要参数：**

- `risk_tree_path`：自定义指标树；
- `temp_output_root`：阶段一根目录；
- `data_output_root`：最终成果根目录；
- `max_concurrency`：每个指标内部的模型评分并发数；
- `force`：强制重新生成并评分全部指标；
- `continue_on_error`：单个指标失败后是否继续；
- `model_config`：可选的显式模型配置。

**执行策略：**

- 指标之间顺序执行，避免多个指标同时大规模读取图谱和写文件。
- 单指标内部的知识评分并发执行。
- 同一临时输出根目录通过进程锁限制为一个全量任务。
- 默认隔离单指标失败并继续处理后续指标。
- 处理期间持续更新 `data/risk/处理汇总.json`。

**断点续跑规则：**

1. 若 `data/risk/<指标序号>/all.txt` 存在且至少包含一条非空知识，则该指标视为已完成并跳过。
2. 若最终目录没有有效 `all.txt`，即使 `temp/risk/<指标序号>` 已有 `all.json` 或其他临时数据，也会重新执行该指标。
3. `force=True` 时忽略已有最终成果，重新处理全部指标。

处理汇总中包含指标总数、实际处理数、跳过已有数、成功数、部分成功数、失败数、新增知识数、已有知识数、评分统计、逐指标结果和失败原因。

### 6.6 `build_risk_indicator_knowledge_statistics(...)`

**作用：** 根据风险指标树和阶段一 `all.json`，生成风险根节点、一级、二级和三级指标的知识条目数统计。

```python
from app.infrastructure.kg_to_natural_language import (
    build_risk_indicator_knowledge_statistics,
)

statistics = build_risk_indicator_knowledge_statistics()
```

**主要参数：**

- `risk_tree_path`：可选的指标树 JSON 路径；
- `temp_risk_root`：可选的阶段一成果根目录；
- `output_path`：可选的统计 JSON 文件路径。

无参数调用时读取 `risk/tree/risk2file_tree.json` 和 `temp/risk`，默认产物为：

```text
data/stats/知识条目数统计.json
```

三级指标以对应 `all.json` 中实际存在的唯一知识标识计数，不直接信任声明数量。二级、一级和风险根节点不能简单累加后代指标数量，而是在各自范围内按规范化来源文件合并知识集合；同一文件被多个后代指标复用时只统计一次。六类风险根节点分别统计。由于同一来源文件及其知识可能同时映射到多个风险类型，统计结果不生成跨六类风险的知识条目数或文件数总计，也不能通过直接相加六类风险统计值得到全局数量。

统计结果保留全部三级指标，并以 `available`、`missing`、`invalid` 区分阶段一成果状态。JSON 同时包含层级知识数、唯一文件数、指标覆盖情况、缺失或无效成果、文件知识集合冲突和校验警告。存在未生成指标或数据一致性问题时，状态为 `partial_success`，不将缺失成果解释为完整的零知识结果。

该接口是同步的纯本地统计功能，不查询 Neo4j、不调用大模型，也不初始化 MySQL 或 MinIO。结果在内存中完整构建后原子写入目标 JSON，函数返回值与写入内容一致。

## 第七章 返回值和状态说明

### 7.1 常见状态

| 状态 | 含义 |
| --- | --- |
| `success` | 本次要求处理的内容全部成功 |
| `partial_success` | 已形成部分成果，但仍有评分或单指标失败 |
| `failed` | 处理未形成可接受的完整结果 |
| `running` | 全量汇总或阶段二检查点记录中的处理中状态 |
| `skipped_existing` | 全量断点续跑时发现已有有效最终成果 |

### 7.2 路径字段

接口返回的路径通常转换为绝对路径。调用方应优先使用返回字典中的 `node_path`、`edge_path`、`txt_path`、`jsonl_path`、`csv_path`、`all_json_path`、`all_txt_path` 或 `statistics_json_path`，不要自行拼接文件名。

### 7.3 统计口径

- `node_count`、`edge_count` 或 `relationship_count`：本次导出的图谱规模；
- `total_entries` 或 `knowledge_count`：转换后的知识条目数量；
- `source_files` 或 `source_file_count`：知识来源文件数量；
- `ranking_success_count`：存在合法分数和原因的知识数量；
- `ranking_failure_count`：评分失败或评分结果不合法的知识数量；
- `skipped_existing_indicator_count`：全量处理中复用已有最终成果的指标数量。
- `unique_file_count`：当前风险层级范围内去重后的来源文件数量；
- `available_third_level_count`：已有合法阶段一成果的后代三级指标数量；
- `is_complete`：当前节点的全部后代三级指标是否均有合法阶段一成果。

## 第八章 配置、异常和运行注意事项

### 8.1 Neo4j 配置

图谱导出和风险映射文件查询使用 `app.core.config.settings` 中的 Neo4j 配置，包括连接地址、用户名、密码和数据库名。项目运行前应确保 `.env` 能被项目配置模块正常加载，并确认 Neo4j 可访问。

### 8.2 大模型配置

只有 `rank_risk_indicator_knowledge`、`process_risk_indicator_knowledge` 和 `process_all_risk_indicator_knowledge` 涉及大模型。模型名称、接口地址、密钥、并发数、总时限、重试次数和检查点间隔由风险配置模块读取；可通过环境变量或 `RiskRelevanceModelConfig` 覆盖。

接口日志只记录脱敏后的模型地址，异常信息会尝试移除密钥。业务文档和调用日志中不应直接输出模型密钥。

### 8.3 同步与异步接口

下列接口为异步接口：

- `rank_risk_indicator_knowledge`；
- `process_risk_indicator_knowledge`；
- `process_all_risk_indicator_knowledge`。

普通同步程序可使用 `asyncio.run()`；已经处于异步框架中的调用方应直接使用 `await`，不能在活动事件循环中再次调用 `asyncio.run()`。

### 8.4 异常处理建议

- Neo4j 连接失败：检查连接配置、数据库状态和网络。
- 图谱类型无法识别：检查 `node.json` 是否为空，以及节点 `properties.label` 是否符合 Schema。
- 转换结果为零：检查图谱节点类型、必需属性、关系方向和转换器过滤规则。
- 阶段一映射文件未命中：检查指标树“已找到”文件名和 Neo4j 节点 `filename`。
- 模型评分失败：检查模型服务、认证配置、并发数、超时和返回 JSON 格式。
- Windows 文件替换失败：确认目标文件未被编辑器占用，并避免同时启动多个相同输出目录的任务。

### 8.5 临时成果与正式成果

`temp` 下内容用于图谱中转、转换元数据、评分检查点和问题排查，可以被后续任务覆盖。`data/type` 和 `data/risk` 下内容属于正式发布结果。业务系统使用知识条目时，应优先读取 `data` 下产物。

## 第九章 推荐调用方式

### 9.1 已知 Neo4j 标签，自动识别图谱类别

```python
from app.infrastructure.kg_to_natural_language import process_graph

result = process_graph(
    tag="待处理图谱标签",
    graph_type="成果名称",
)
print(result["convert"]["txt_path"])
```

### 9.2 已有本地图谱 JSON，自动选择转换器

```python
from app.infrastructure.kg_to_natural_language import convert_graph_to_text

result = convert_graph_to_text(
    "成果名称",
    input_dir=r"D:\graph_json",
    output_dir=r"D:\knowledge_output",
)
print(result["txt_path"])
```

### 9.3 更新五类正式知识条目

```python
from app.infrastructure.kg_to_natural_language import process_graphs_by_type

summary = process_graphs_by_type()
print(summary["statistics_json_path"])
```

### 9.4 处理一个三级风险指标

```python
import asyncio

from app.infrastructure.kg_to_natural_language import (
    process_risk_indicator_knowledge,
)

result = asyncio.run(process_risk_indicator_knowledge("1.1.1.1"))
print(result["all_txt_path"])
```

### 9.5 断点续跑全部三级风险指标

```python
import asyncio

from app.infrastructure.kg_to_natural_language import (
    process_all_risk_indicator_knowledge,
)

summary = asyncio.run(
    process_all_risk_indicator_knowledge(
        continue_on_error=True,
        force=False,
    )
)
print(summary["summary_path"])
```

默认调用会跳过已有非空最终 `all.txt` 的指标。只有明确需要重新生成全部成果时，才应设置 `force=True`。
