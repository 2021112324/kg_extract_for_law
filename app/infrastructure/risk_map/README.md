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
- 本地算法协议为`copy-first-many-to-many-v3-audit-fixes`；优化五名称策略为`scoped-reference-alias-v2`，旧名称策略运行（包括旧v3）不能直接续跑；已存在的业务图谱不自动清理。重新映射必须使用未占用的新目标标签。

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
- 每个来源文件名独立按规范名、修订族、中国前缀变体查找，再核对身份；明确别名可参与定位，但不是全局同义词。保留原名和修订说明，不删除试行/暂行等限定词，不声称确定最新版本。多个无法消解的版本身份保持歧义，精确命中不扩大到其他版。
- 空知识依据正常跳过，不计失败；全部为空时本地状态为`无可用依据`，候选和最终均为空数组，不代表模型已判断全部不相关。`summary.json.basis_skips`汇总唯一挂接和影响范围。
- `runs/<run_id>/inputs/`保存实际指标树字节、提示词、脱敏配置、代码身份、空依据明细和校验清单；不保存认证密钥或数据库密码。
- 精确历史样例及可选真实模型测试入口为`test/run_audit_fixes_validation.py`，使用`--output`指定新的证据目录；`--real-model`调用配置模型，`--local`仅使用显式本地测试连接。语义测试不等于全量准确率。

## 优化五：引用级别名与可选来源核验

- 兼容旧四字段文件叶节点，新增`别名`必须为字符串数组，缺失时按`[]`处理。空白、重复名称在索引中清理，原始树不变。
- 别名仅属于声明它的引用。相同别名指向不同文件保持歧义，不沿别名链合并身份；中英文名称明确属于同一文件时不再仅因字符串不同报错。
- `file_match.name_hits`在本地保存每个名称的命中角色、层级、文件身份和挂接证据；一个名称精确命中不能遮蔽其他来源名的冲突。
- `run_mapping.py`中的`PROVENANCE_PATH`默认`None`。需要来源原因说明时可显式设置为`Path(TREE_PATH).with_name("条款知识来源明细.json")`，不自动读取同目录文件，更不读取历史审查明细。
- 来源明细仅用于核验解释，不覆盖主树知识。空依据按核验结果区分`risk_column_empty`、`source_not_found`、`source_ambiguity`；未提供或核验不了时为`empty_reason_unverified`。`已找到`为空不阻止使用主树非空知识。
- 显式来源文件缺失或结构错误时在写库和模型请求前停止；逐挂接不一致时保留强警告并降级解释，不补造正文。上游来源歧义即使原因核验通过，也仍未解决。
- 输入快照增加`source_verification.json`；启用附件时还保存`provenance.json`原字节。来源启用状态、哈希和策略参与恢复及缓存隔离；附件变化不能接续旧运行。拒绝不兼容恢复时不覆盖旧摘要。
- `summary.basis_skips.by_reason`只统计实际命中的空挂接及影响法条，法条可出现在多个原因中，不可直接相加；全输入空挂接另见`inputs/empty_attachments.json`。这些信息不写入Neo4j。

离线与当前输入验证：`python app/infrastructure/risk_map/test/run_scoped_aliases_validation.py --output <新的证据目录>`。此入口不连接真实数据库或模型；本地数据库联调继续使用前述专用入口。
