# English Legal Extraction Schema Mapping

本文档整理 `law_en_extract/prompt` 目录下英文法规抽取 prompt/schema/example 与中文法规抽取 schema 的对应关系，用于维护英文本体、示例和后续知识图谱入库规则。

## 1. 总体设计

英文法规抽取 schema 延续中文法规抽取 schema 的两层结构：

- 文件信息抽取：抽取法规文件本身、法规依据、编纂位置及其关系。
- 法条信息抽取：抽取法条、条款单元、引用依据及其关系。

英文 schema 的实体、属性、关系名均要求使用英文。关系记录为了兼容当前 Langextract 解析器，仍使用中文外壳字段：

| 兼容字段 | 中文含义 | 用法 |
|---|---|---|
| `关系` | 关系抽取类 | relation extraction class |
| `主体` | 主体 | relation source |
| `谓词` | 谓词 | relation predicate |
| `客体` | 客体 | relation target |

注意：虽然关系外壳字段是中文，但 `谓词` 的值必须是英文关系名，如 `BASED_ON`、`CODIFIED_IN`、`CONTAINS`、`CITES`。

## 2. 文件信息实体对应

| 中文 schema | 英文 schema | 变化说明 |
|---|---|---|
| 法规文件 | `Legal Document` | 对应。英文覆盖 Act、Code、Regulation、CFR Part、Rule、Order、Directive、Treaty 等。 |
| 法规依据 | `Legal Basis` | 对应。仅在文本明确表达 authority、pursuant to、under、in accordance with 等依据/授权语义时抽取。 |
| 无 | `Codification Location` | 英文新增。用于记录 U.S.C./CFR 编纂位置，避免把“所在位置”误当作“法规依据”。 |

## 3. 法规文件属性对应

| 中文属性 | 英文属性 | 变化说明 |
|---|---|---|
| 文件全称 | `official_title` | 对应，法规文件正式全称。 |
| 文号 | `legal_citation` | 近似对应。英文法规常以 citation 表达，如 `50 U.S.C. 1701 et seq.`、`22 CFR Part 123`。 |
| 文件别名 | `short_title` | 对应短标题，如文本明确给出。 |
| 文件别名 | `abbreviation` | 英文拆分出的缩写字段，如 `IEEPA`、`FIRRMA`。 |
| 文件性质 | `document_type` | 对应，如 Act、Code、Regulation、CFR Part、Rule。 |
| 发布生效信息 | `publication_effective_info` | 对应，记录发布、制定、生效、实施等综合信息。 |
| 制定目的 | `purpose` | 对应，明示的立法或监管目的。 |
| 应用领域 | `domain` | 对应，法律或政策领域。 |
| 适用行业 | `applicable_industry` | 对应，适用行业或领域。 |
| 应用范围 | `scope_of_application` | 对应，地域、时间、主体、事项等适用范围。 |
| 发布单位 | `issuing_authority` | 对应，也可表示 enacting/administering authority。 |
| 发布日期 | `publication_date` | 对应，发布日期、制定日期或颁布日期。 |
| 生效日期 | `effective_date` | 对应，生效日期或实施日期。 |
| 时效性 | `status` | 对应，如 current、effective、amended、repealed。 |
| 无 | `codification_text` | 英文新增，记录 U.S.C./CFR 的 title/chapter/part 编纂路径。 |
| 无 | `authority_text` | 英文新增，保留 Authority 段落或授权依据原文。 |
| 无 | `source_text` | 英文新增，保留 Source 或 Federal Register 来源说明。 |

## 4. 法规依据属性对应

| 中文属性 | 英文属性 | 变化说明 |
|---|---|---|
| 文件全称 | `official_title` | 对应，依据文件正式名称。 |
| 文件别名 | `abbreviation` | 对应英文缩写或简称。 |
| 无 | `legal_citation` | 英文新增，记录依据文件或授权条款的引用。 |
| 无 | `basis_role` | 英文新增，标注 authority、authorization、delegation、amendment basis 等依据角色。 |
| 无 | `source_text` | 英文新增，保留支持该依据判断的原文片段。 |

## 5. 编纂位置属性

`Codification Location` 是英文 schema 新增实体，中文 schema 没有直接对应项。

| 英文属性 | 中文含义 | 说明 |
|---|---|---|
| `codification_level` | 编纂层级 | 如 title、chapter、subchapter、part、section、code path。 |
| `codification_text` | 编纂路径文本 | 完整 U.S.C./CFR 路径原文。 |
| `citation` | 编纂引用 | 如 `22 CFR Part 123`、`50 U.S.C. Chapter 35`。 |

## 6. 文件信息关系对应

| 中文关系 | 英文关系 | 变化说明 |
|---|---|---|
| 法规文件 - 依据 - 法规依据 | `Legal Document - BASED_ON - Legal Basis` | 对应。表示法规文件依据其他法律文件或授权条款制定。 |
| 无 | `Legal Document - CODIFIED_IN - Codification Location` | 英文新增。表示法规文件被编纂或位于某 U.S.C./CFR 路径下。 |

## 7. 法条信息实体对应

| 中文 schema | 英文 schema | 变化说明 |
|---|---|---|
| 法条 | `Legal Provision` | 对应。英文覆盖 section、article、provision 等。每个 clause 抽取任务应只有一个当前 `Legal Provision`。 |
| 条款单元 | `Provision Unit` | 对应。英文覆盖 subsection、paragraph、subparagraph、clause、item、note、reserved unit 等。 |
| 引用依据 | `Citation` | 对应。英文更强调 Document/Provision 两类引用。 |

## 8. 法条属性对应

| 中文属性 | 英文属性 | 变化说明 |
|---|---|---|
| 条 / 条款编号 | `provision_number` | 对应，如 `Section 1701`、`§ 123.1`。 |
| 条款标题 | `provision_heading` | 英文新增/强化，记录 section heading。 |
| 核心主题 | `core_topic` | 对应，中心法律主题或监管议题。 |
| 效力范围 | `scope_of_effect` | 对应，按地域、时间、主体或对象描述适用范围。 |
| 章、节 | `classification_context` | 英文合并为 title/chapter/subchapter/part/subpart 上下文。 |
| 适用行业 | `applicable_industry` | 对应。 |
| 法条全文 | 无直接 schema 字段 | 英文保留在外层 `input_clause.clause_content`，不要求作为 `Legal Provision` 属性重复抽取。 |

## 9. 条款单元属性对应

| 中文属性 | 英文属性 | 变化说明 |
|---|---|---|
| 条款单元内容 | `unit_content` | 对应，必须是完整语义单元内容。 |
| 条款主旨 | `unit_heading` | 近似对应，记录单元附着标题或小标题。 |
| 单元层级 | `unit_level` | 对应，如 section、subsection、paragraph、subparagraph、clause、item、note、reserved。 |
| 单元编号 | `unit_number` | 对应，如 `Section 1701 subsection (a)`。 |
| 无 | `hierarchy_path` | 英文新增，记录完整层级路径，如 `§ 123.1 > paragraph (a)`。 |
| 适用行业 | `applicable_industry` | 对应。 |
| 功能类型 | `function_type` | 对应，如 obligation、prohibition、authorization、procedure、liability、definition、exception、limitation、rule。 |
| 适用主体 | `applicable_subject` | 对应。 |
| 责任角色 | `responsible_role` | 对应。 |
| 行为描述 | `conduct_description` | 对应。 |
| 适用前提 | `condition` | 对应。 |
| 法律后果 | `legal_consequence` | 对应。 |
| 例外情形 | `exception` | 对应。 |
| 时间要素 | `time_element` | 对应。 |
| 量化标准 | `quantitative_standard` | 对应。 |
| 其他信息 | `other_information` | 对应。 |

## 10. 引用依据属性对应

| 中文属性 | 英文属性 | 变化说明 |
|---|---|---|
| 引用类型 | `citation_type` | 对应，英文限定为 `Document` 或 `Provision`。 |
| 文件全称 | `official_title` | 对应。 |
| 文件别名 | `abbreviation` | 对应英文缩写或简称。 |
| 文件类型 | `document_type` | 对应，如 Act、Code、Regulation、CFR Part、Rule、Order、Directive、Treaty。 |
| 条款编号 | `provision_number` | 对应，被引用条款编号。 |
| 是否本文件内引用 | `is_internal_reference` | 对应，英文使用 `Yes` / `No`。 |
| 引用关系 | `citation_relation` | 对应，如 pursuant to、under、subject to、except as provided in、defined in、reference。 |
| 引用目的 | `citation_purpose` | 对应，如 authority、exception、definition、procedure、penalty、supplement。 |

## 11. 法条关系对应

| 中文关系 | 英文关系 | 变化说明 |
|---|---|---|
| 法条 - 包含 - 条款单元 | `Legal Provision - CONTAINS - Provision Unit` | 对应。 |
| 条款单元 - 引用 - 引用依据 | `Provision Unit - CITES - Citation` | 对应。 |

## 12. Prompt 规则对应

### 文件信息抽取规则

| 中文规则含义 | 英文 prompt 规则 | 说明 |
|---|---|---|
| 单次任务只有一个法规文件实体 | Extract exactly one `Legal Document` entity | 防止一个 file_info 抽出多个主文件。 |
| 只抽明示依据 | Extract `Legal Basis` only when authority/legal basis is explicit | 防止模型凭常识补依据。 |
| 不混淆依据和位置 | Do not confuse codification/source location with legal basis | 英文法规关键规则。 |
| 不推断未明示信息 | Do not infer unstated dates, authorities, status, purpose or basis | 控制幻觉。 |

### 法条抽取规则

| 中文规则含义 | 英文 prompt 规则 | 说明 |
|---|---|---|
| 单次任务整体是一条法条 | Input text describes exactly one `Legal Provision` | 防止被引用条款被抽成当前法条。 |
| 抽完整语义单元 | Extract complete `Provision Unit` entities | 防止把不完整列项拆成孤立节点。 |
| 下级列项需保留父上下文 | Keep parent paragraph/subsection when lower-level item loses context | 英文长嵌套条款必要规则。 |
| 保留层级路径 | Preserve parent context in `hierarchy_path` and `unit_number` | 支撑后续入图定位。 |
| 只抽明示引用 | Extract `Citation` entities only for explicit citations | 控制幻觉。 |
| 本条/本章/本法识别内部引用 | Treat this section/subchapter/title/Act as internal references when appropriate | 对应 `is_internal_reference`。 |
| 不推断法律后果、日期、主体、依据 | Do not invent legal consequences, dates, subjects, citations or legal bases | 控制幻觉。 |

## 13. Example 文件中的英文结构

`example.py` 中示例保持如下结构：

```python
example_for_file_info = [
    {
        "text": "...",
        "extractions": [
            {"name": "...", "type": "Legal Document", "attributes": {...}},
            {"name": "...", "type": "Legal Basis", "attributes": {...}},
            {"name": "...", "type": "Codification Location", "attributes": {...}},
            {
                "name": "",
                "type": RELATION_CLASS,
                "attributes": {
                    SUBJECT_KEY: "Legal Document_...",
                    PREDICATE_KEY: "BASED_ON",
                    OBJECT_KEY: "Legal Basis_...",
                },
            },
        ],
    },
]
```

```python
example_for_clause = [
    {
        "text": "...",
        "extractions": [
            {"name": "...", "type": "Legal Provision", "attributes": {...}},
            {"name": "...", "type": "Provision Unit", "attributes": {...}},
            {"name": "...", "type": "Citation", "attributes": {...}},
            {
                "name": "",
                "type": RELATION_CLASS,
                "attributes": {
                    SUBJECT_KEY: "Legal Provision_...",
                    PREDICATE_KEY: "CONTAINS",
                    OBJECT_KEY: "Provision Unit_...",
                },
            },
        ],
    },
]
```

## 14. 主要变化总结

1. 英文新增 `Codification Location`，专门处理 U.S.C./CFR 编纂路径。
2. 英文把中文“文件别名”拆成 `short_title` 和 `abbreviation`。
3. 英文强化 citation 表达，增加 `legal_citation`、`authority_text`、`source_text`、`codification_text`。
4. 英文条款层级比中文“条/款/项”更细，覆盖 section、subsection、paragraph、subparagraph、clause、item、note。
5. 英文新增 `hierarchy_path`，用于保留复杂层级路径。
6. 英文将制定依据、编纂位置、条款内引用拆分为三类语义：
   - `Legal Basis` + `BASED_ON`
   - `Codification Location` + `CODIFIED_IN`
   - `Citation` + `CITES`

