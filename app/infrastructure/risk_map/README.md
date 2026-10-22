# 三级指标映射

独立扩展功能，不修改中文法规抽取。唯一流程为：**先完整复制Neo4j图谱，再定位全部候选，最后剔除不合理指标**。

编辑`run_mapping.py`配置区后直接运行该文件。详细步骤见[使用指南](task/三级指标映射使用步骤指南.md)。

- 图谱输入：Neo4j源业务标签、未占用的目标业务标签。指标依据：原有JSON映射树。
- 每轮按固定清单每批20条并发，单个模型任务最多尝试5次。
- 不评分、不排序、不限制Top5。候选和最终结果分别保存为Neo4j字符串数组，每项编码一个正式指标对象。
- 模型局部失败不会撤销基础图谱；数据库提交不明则停止，禁止盲目重放。
- 本地日志、回执、检查点：`output/copy_first`。历史`output/mapped`保留，不自动兼容旧清单。
- 不创建数据库管理节点、技术标签、锁属性或专用约束/索引。
- 映射状态只保存在本地处理记录和日志中，不再向Neo4j写入`三级指标映射状态`；图谱业务输出仅保留候选列表和最终列表。
- 本地算法协议为`copy-first-many-to-many-v3-audit-fixes`，旧v1/v2运行不能直接续跑；已存在的业务图谱不自动清理。重新映射必须使用未占用的新目标标签。

## 二次审查：序号与理由

审查协议为 `removal-reasons-v1`：模型返回一个对象，其中 `removed_indicator_numbers` 是剔除序号字符串数组，`reasons` 保存每项理由。法条支持指标的全部或部分事项即可保留；没有剔除时返回空数组和空对象。

- `MODEL_OPTIONS["review_max_group_candidates"]` 默认8，只控制每次请求的候选数，不限制最终映射数量。输出截断、理由缺失等按最多5次总尝试处理。
- 终端的“审查判定”表示模型建议；“审查确认”才表示批次提交及本地确认完成。
- 理由在 `runs/<run_id>/review/events.jsonl` 和 `checkpoints/*_review_intent.json` 的 `results[].groups[].reasons`，不写入图谱属性。是否已应用须核对配套 ack。
- `summary.json` 的 `review_audit` 按已确认批次统计，不按日志行数计数。
- 每条法条的状态在`checkpoints/*_intent.json`的`results[].status`及阶段`events.jsonl`的`article_result`事件中；汇总在`summary.json.article_statuses`。
- 旧裸数组运行不能跨协议续跑或被补造理由；请使用新运行和未占用目标。模型语义正确性仍需抽查，格式通过不等于判断正确。

目录：`preparation`为树索引，`matching`为依据定位，`review`为指标剔除，`pipeline.py`为编排，`neo4j_bridge.py`复用项目图存储，`local_records.py`保存本地证据，`test`为隔离测试。

本地数据库验证：设置`RISK_MAP_TEST_NEO4J_URI/USER/PASSWORD/DATABASE`后执行`python app/infrastructure/risk_map/test/run_validation.py --local`。测试拒绝远程URI，只操作随机专用标签。

## 审查问题修复与容错

- 两阶段先严格解析；可确认不改变值的JSON语法错误尝试一次`json-repair`，之后仍做完整校验。重复键、序号与理由不一致、缺业务值、多对象或截断不会被修复成成功。
- 校验失败时，把具体错误加入下一次提示；最多5次总请求，不因本地修复增加次数。修复前后响应和`json_repair`事件仅存本地。
- 文件名按直接规范名、修订族、中国常用前缀变体的优先级查找。保留原名和修订说明，不删除试行/暂行等限定词，不声称确定最新版本。匹配方法在本地`file_match`中。
- 空知识依据正常跳过，不计失败；全部为空时本地状态为`无可用依据`，候选和最终均为空数组，不代表模型已判断全部不相关。`summary.json.basis_skips`汇总唯一挂接和影响范围。
- `runs/<run_id>/inputs/`保存实际指标树字节、提示词、脱敏配置、代码身份、空依据明细和校验清单；不保存认证密钥或数据库密码。
- 精确历史样例及可选真实模型测试入口为`test/run_audit_fixes_validation.py`，使用`--output`指定新的证据目录；`--real-model`调用配置模型，`--local`仅使用显式本地测试连接。语义测试不等于全量准确率。
