"""格式一英文法规图谱装配器。

本文件负责把两类输入合并成最终知识图谱：
1. `splitter.py` 产生的确定性结构切分结果。
2. LLM 抽取返回的实体、关系和原始输出。

核心职责：
- 统一 Pydantic / Langextract / dict 等不同结果形态。
- 创建 Neo4j 友好的节点 ID。
- 构建 LegalDocument、LegalProvision、ProvisionUnit、Citation 等节点。
- 构建 CONTAINS、BASED_ON、CITES 等关系。
- Article 节点由一阶段规则稳定生成，LLM 仅补充语义属性和关系。
- 尽量把同文件内部 Citation 解析到已有 Article 或 ProvisionUnit 节点。
"""

from __future__ import annotations

# re 用于清洗 ID、关系类型和解析 Article 引用文本。
import re
# Any 用于描述 LLM 输出和图谱属性这类结构可变的数据。
from typing import Any

# 项目公共 UUID 工具，用于生成唯一节点 ID。
from app.infrastructure.string_utils.id_tool import generate_hex_uuid
# 项目公共 Neo4j 清洗工具，用于避免节点 ID 中出现特殊控制字符。
from app.infrastructure.string_utils.str_clean import clean_string_for_neo4j_extended
# 合规风险类型枚举，用于把文首中文风险类型拆成稳定数组。
from app.infrastructure.information_extraction.en_law_v1.config import RISK_TYPE_VALUES


# Langextract 兼容关系实体类名；schema/prompt 中也使用同一个值。
RELATION_CLASS = "关系"
# 关系主体字段名。
SUBJECT_KEY = "主体"
# 关系谓词字段名。
PREDICATE_KEY = "谓词"
# 关系客体字段名。
OBJECT_KEY = "客体"

# Neo4j 友好的规范实体类型。schema、prompt、graph_builder 均以这些值为准。
LEGAL_DOCUMENT = "LegalDocument"
LEGAL_BASIS = "LegalBasis"
LEGAL_PROVISION = "LegalProvision"
PROVISION_UNIT = "ProvisionUnit"
PROVISION_CLAUSE = "ProvisionClause"
PROVISION_TEXT_PARAGRAPH = "ProvisionTextParagraph"
CITATION = "Citation"

# Article 级确定性结构字段只能来自 splitter，不允许 LLM 覆盖。
SPLITTER_LOCKED_PROVISION_FIELDS = {
    "provision_number",
    "provision_heading",
    "provision_content",
    "classification_context",
    "classification_title",
    "classification_chapter",
    "classification_section",
    "classification_part",
    "classification_context_text",
    "title",
    "chapter",
    "section",
    "part",
    "line_start",
    "line_end",
    "content_line_start",
    "content_line_end",
    "is_amendment_article",
    "extraction_status",
    "llm_extraction_status",
}

# ProvisionUnit.function_type / ProvisionClause.legal_function 的 LLM 允许枚举。LLM 空值或越界值保持为空。
LLM_FUNCTION_TYPES = {"prohibition", "mandatory", "optional"}

ARTICLE_PREFIX_RE = re.compile(r"^Article\s+\d+[A-Za-z]?", re.IGNORECASE)
LOCAL_UNIT_SEQUENCE_RE = re.compile(r"^(?:\(?[0-9]+[A-Za-z]?\)?|\(?[a-z]{1,3}\)?|\(?[ivxlcdm]+\)?)+$", re.IGNORECASE)
FUNCTION_TYPE_FALLBACK = ""
QUANTITATIVE_VALUE_TYPE_ALIASES = {
    "amount": "amount",
    "monetary_amount": "amount",
    "monetary_value": "amount",
    "money": "amount",
    "fee": "amount",
    "ratio": "ratio",
    "percentage": "ratio",
    "percent": "ratio",
    "rate": "ratio",
    "time_limit": "time_limit",
    "deadline": "time_limit",
    "date": "time_limit",
    "time_period": "time_limit",
    "period": "time_limit",
    "duration": "time_limit",
    "count": "count",
    "frequency": "count",
    "number": "count",
    "multiple": "multiple",
    "multiplier": "multiple",
    "other": "other",
}
QUANTITATIVE_RELATION_ALIASES = {
    "range": "range",
    "between": "range",
    "lower_bound": "lower_bound",
    "minimum": "lower_bound",
    "min": "lower_bound",
    "at_least": "lower_bound",
    "upper_bound": "upper_bound",
    "maximum": "upper_bound",
    "max": "upper_bound",
    "no_more_than": "upper_bound",
    "equal": "equal",
    "equals": "equal",
    "exact": "equal",
    "exact_value": "equal",
    "other": "other",
}

# 兼容历史结果和少量 LLM 可能输出的带空格类型。
ENTITY_TYPE_ALIASES = {
    "Legal Document": LEGAL_DOCUMENT,
    "LegalDocument": LEGAL_DOCUMENT,
    "Legal Basis": LEGAL_BASIS,
    "LegalBasis": LEGAL_BASIS,
    "Legal Provision": LEGAL_PROVISION,
    "LegalProvision": LEGAL_PROVISION,
    "Provision Unit": PROVISION_UNIT,
    "ProvisionUnit": PROVISION_UNIT,
    "Provision Clause": PROVISION_CLAUSE,
    "ProvisionClause": PROVISION_CLAUSE,
    "Provision Text Paragraph": PROVISION_TEXT_PARAGRAPH,
    "ProvisionTextParagraph": PROVISION_TEXT_PARAGRAPH,
    "Citation": CITATION,
}


def normalize_entity_type(value: Any) -> str:
    """把 LLM 输出实体类型规范化为 Neo4j 友好的 schema 标签。"""
    text = str(value or "").strip()
    return ENTITY_TYPE_ALIASES.get(text, text)


def to_plain(value: Any) -> Any:
    """将 Pydantic、Langextract 或普通对象转换为 JSON 安全的 Python 值。"""
    # Pydantic v2 对象通常提供 model_dump。
    if hasattr(value, "model_dump"):
        return value.model_dump()
    # Pydantic v1 对象通常提供 dict。
    if hasattr(value, "dict"):
        return value.dict()
    # 列表逐项递归转换。
    if isinstance(value, list):
        return [to_plain(item) for item in value]
    # 元组转换成列表，保证 JSON 可序列化。
    if isinstance(value, tuple):
        return [to_plain(item) for item in value]
    # 字典对每个 value 递归转换。
    if isinstance(value, dict):
        return {key: to_plain(item) for key, item in value.items()}
    # 基础类型直接返回。
    return value


def normalize_extraction_result(value: Any) -> dict[str, list[dict[str, Any]]]:
    """把不同形态的 LLM 抽取结果统一成 entities / relations 两个列表。"""
    # 先把对象转成普通 dict/list，后续逻辑只处理标准 Python 数据。
    plain = to_plain(value or {})
    # 有些调用层会把真实结果包在 extraction 字段下，这里向内解包一层。
    if isinstance(plain, dict) and "extraction" in plain:
        plain = plain.get("extraction") or {}
    # 标准化后的实体列表。
    entities = []
    # 标准化后的关系列表。
    relations = []

    # 遍历实体输出；如果某个实体其实是“关系实体”，则转入 relations。
    for entity in plain.get("entities", []) if isinstance(plain, dict) else []:
        if not isinstance(entity, dict):
            continue
        # 兼容 entity_type / class / type 三种字段命名。
        entity_type = entity.get("entity_type") or entity.get("class") or entity.get("type") or ""
        if entity_type in {RELATION_CLASS, "Relation"}:
            # Langextract 可能把关系三元组放在 properties 或 attributes 中。
            props = entity.get("properties") or entity.get("attributes") or {}
            relations.append(
                {
                    "source": props.get(SUBJECT_KEY) or props.get("source") or "",
                    "target": props.get(OBJECT_KEY) or props.get("target") or "",
                    "type": props.get(PREDICATE_KEY) or props.get("type") or "",
                    "properties": props,
                }
            )
        else:
            # 普通实体统一成 name、entity_type、properties 三个字段。
            entities.append(
                {
                    "name": entity.get("name") or entity.get("text") or "",
                    "entity_type": normalize_entity_type(entity_type),
                    "properties": entity.get("properties") or entity.get("attributes") or {},
                }
            )

    # 遍历显式 relations 输出；部分适配器会直接返回 relations 列表。
    for relation in plain.get("relations", []) if isinstance(plain, dict) else []:
        if not isinstance(relation, dict):
            continue
        # properties 中也可能包含主体/客体/谓词字段，因此需要兜底读取。
        props = relation.get("properties") or {}
        relations.append(
            {
                "source": relation.get("source") or props.get(SUBJECT_KEY) or "",
                "target": relation.get("target") or props.get(OBJECT_KEY) or "",
                "type": relation.get("type") or props.get(PREDICATE_KEY) or "",
                "properties": props,
            }
        )
    return {"entities": entities, "relations": relations}


def _safe_id_part(value: str) -> str:
    """把节点类型清洗成可用于 node_id 前缀的安全字符串。"""
    # 先使用项目公共 Neo4j 清洗函数移除控制字符和特殊空白。
    cleaned = clean_string_for_neo4j_extended(str(value or ""))
    # 再把非字母、数字、下划线和中文的字符替换成下划线。
    safe = re.sub(r"[^0-9A-Za-z_\u4e00-\u9fff]+", "_", cleaned).strip("_")
    # 如果清洗后为空，使用兜底前缀 node。
    return safe or "node"


def make_node_id(node_type: str) -> str:
    """根据节点类型和 UUID 生成唯一节点 ID。"""
    return clean_string_for_neo4j_extended(f"{_safe_id_part(node_type)}_{generate_hex_uuid()}")


def make_node(name: str, node_type: str, properties: dict[str, Any] | None, filename: str) -> dict[str, Any]:
    """创建项目统一格式的图谱节点。"""
    return {
        # node_id 是 Neo4j 入库时的唯一标识。
        "node_id": make_node_id(node_type),
        # node_name 是面向人看的名称；如果模型没给名称，则用节点类型兜底。
        "node_name": str(name or node_type),
        # node_type 是图谱实体类型。
        "node_type": node_type,
        # properties 保存节点业务属性。
        "properties": dict(properties or {}),
        # filename 记录节点来自哪个原始文件。
        "filename": filename,
    }


def _normalize_bool(value: Any) -> bool:
    """把 Yes/No、true/false 等模型输出统一为 bool。"""
    return str(value or "").strip().lower() in {"yes", "true", "1", "internal", "y"}


def _split_risk_types(value: Any) -> list[str]:
    """按六类风险枚举拆分文首合规风险类型，避免复合字符串直接入下游字段。"""
    text = str(value or "").strip()
    if not text:
        return []
    matched = [risk_type for risk_type in RISK_TYPE_VALUES if risk_type in text]
    if matched:
        return matched
    return [text]


def _add_classification_flat_fields(props: dict[str, Any], article: dict[str, Any]) -> None:
    """为 Article 分类上下文补充 Neo4j 友好的扁平字段。"""
    context = article.get("classification_context") or {}
    if not isinstance(context, dict):
        context = {}
    props["title"] = article.get("title", context.get("title", ""))
    props["chapter"] = article.get("chapter", context.get("chapter", ""))
    props["section"] = article.get("section", context.get("section", ""))
    props["part"] = article.get("part", context.get("part", ""))


def _normalize_function_type(value: Any) -> str:
    """把 function_type / legal_function 收敛到 KG 枚举。"""
    if value in (None, ""):
        return ""
    normalized = str(value).strip().lower().replace(" ", "_")
    if not normalized:
        return ""
    if normalized in LLM_FUNCTION_TYPES:
        return normalized
    return FUNCTION_TYPE_FALLBACK


def _normalize_legal_function(value: Any) -> str:
    """规范化 ProvisionClause.legal_function，无法判断时保持为空。"""
    return _normalize_function_type(value)


def _normalize_quantitative_feature(value: Any) -> str:
    """统一 quantitative_feature 大小写。"""
    normalized = str(value or "").strip().lower()
    if normalized == "quantitative":
        return "Quantitative"
    return "Qualitative"


def _normalize_quantitative_enum(value: Any, aliases: dict[str, str]) -> str:
    """把量化枚举值规范为小写下划线枚举。"""
    normalized = str(value or "").strip().lower()
    normalized = re.sub(r"[^a-z0-9]+", "_", normalized).strip("_")
    return aliases.get(normalized, "other")


def _has_concrete_quantity(raw_text: Any, indicator: dict[str, Any]) -> bool:
    """判断量化描述是否包含可校验数值、范围或单位。"""
    text = str(raw_text or "").strip().lower()
    if re.search(r"\d", text):
        return True
    if re.search(
        r"\b(one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|"
        r"twenty|thirty|forty|fifty|hundred|thousand|million|billion)\b",
        text,
    ):
        return True
    return any(indicator.get(key) not in (None, "") for key in ("min", "minimum_value", "max", "maximum_value"))


def _is_reference_like_quantity(raw_text: Any, value_type: str) -> bool:
    """识别不应作为 quantitative_indicator 的引用型或模糊型表达。"""
    text = str(raw_text or "").strip().lower()
    if value_type in {"reference", "reference_to_annex", "reference_to_provisions"}:
        return True
    if re.search(r"\b(in accordance with|pursuant to|referred to in|within the meaning of)\b", text):
        return True
    if re.search(r"\barticles?\s+\d", text) or re.search(r"\b(annex|part|chapter|section)\s+[ivxlcdm0-9]", text):
        return True
    if re.fullmatch(r"(the|that|such)?\s*(period|duration|time limit)", text):
        return True
    if "reasonable duration" in text:
        return True
    return False


def _normalize_quantitative_indicator(props: dict[str, Any]) -> dict[str, Any] | None:
    """把新旧量化字段统一为 quantitative_indicator 结构。"""
    indicator = props.get("quantitative_indicator")
    if indicator in (None, "", {}):
        indicator = props.get("quantitative_condition")
    if not isinstance(indicator, dict) or not indicator:
        return None

    raw_value_type = indicator.get("value_type") or indicator.get("quantitative_value_type")
    raw_relation = indicator.get("relation") or indicator.get("constraint_relation")
    raw_text = indicator.get("raw_text")
    if _is_reference_like_quantity(raw_text, str(raw_value_type or "")):
        return None
    if not _has_concrete_quantity(raw_text, indicator):
        return None

    return {
        "raw_text": raw_text,
        "value_type": _normalize_quantitative_enum(raw_value_type, QUANTITATIVE_VALUE_TYPE_ALIASES),
        "min": indicator.get("min", indicator.get("minimum_value")),
        "max": indicator.get("max", indicator.get("maximum_value")),
        "unit": indicator.get("unit"),
        "relation": _normalize_quantitative_enum(raw_relation, QUANTITATIVE_RELATION_ALIASES),
    }


def _normalize_provision_unit_props(props: dict[str, Any]) -> dict[str, Any]:
    """规范化 ProvisionUnit 属性，减少 KG 枚举和 Neo4j 入库问题。"""
    result = dict(props)
    if "function_type" in result:
        result["function_type"] = _normalize_function_type(result.get("function_type"))
    if "legal_function" in result:
        result["legal_function"] = _normalize_legal_function(result.get("legal_function"))
    result["quantitative_feature"] = _normalize_quantitative_feature(result.get("quantitative_feature"))
    quantitative_indicator = _normalize_quantitative_indicator(result)
    result.pop("quantitative_condition", None)
    if quantitative_indicator:
        result["quantitative_indicator"] = quantitative_indicator
        result["quantitative_feature"] = "Quantitative"
    else:
        result["quantitative_indicator"] = None
        result["quantitative_feature"] = "Qualitative"
    return result


def _local_unit_suffix(value: str) -> str | None:
    """把局部单元序号规范成 Article 后缀，如 22(a) -> (22)(a)。"""
    text = str(value or "").strip().rstrip(".")
    if not text or not LOCAL_UNIT_SEQUENCE_RE.match(text):
        return None
    tokens = re.findall(r"\(?([0-9]+[A-Za-z]?|[a-z]{1,3}|[ivxlcdm]+)\)?", text, flags=re.IGNORECASE)
    if not tokens:
        return None
    rebuilt = "".join(f"({token})" for token in tokens)
    return rebuilt if rebuilt else None


def _normalize_unit_number_for_parent(props: dict[str, Any], article_number: str) -> tuple[dict[str, Any], str | None]:
    """把 ProvisionUnit.unit_number 锚定到当前父级 Article。"""
    result = dict(props)
    unit_number = str(result.get("unit_number") or "").strip()
    if not article_number or not unit_number:
        return result, None

    article_match = ARTICLE_PREFIX_RE.match(unit_number)
    if article_match:
        old_prefix = article_match.group(0)
        if old_prefix == article_number:
            return result, None
        result["unit_number"] = article_number + unit_number[article_match.end():]
        return result, f"{unit_number} -> {result['unit_number']}"

    local_suffix = _local_unit_suffix(unit_number)
    if local_suffix:
        result["unit_number"] = f"{article_number}{local_suffix}"
        return result, f"{unit_number} -> {result['unit_number']}"

    return result, None


def _unit_number_belongs_to_article(unit_number: Any, article_number: str) -> bool:
    """校验单元编号是否为当前 Article 编号加单元序号。"""
    text = str(unit_number or "").strip()
    if not text or not article_number:
        return False
    if text == article_number:
        return True
    if not text.startswith(article_number):
        return False
    suffix = text[len(article_number):].strip()
    return bool(suffix and _local_unit_suffix(suffix))


def _unit_content_supported_by_article(unit_content: Any, article_content: str) -> bool:
    """校验 ProvisionUnit 内容是否基本来自当前 Article 原文。"""
    if not isinstance(unit_content, str):
        return True
    unit_text = unit_content.strip()
    if not unit_text:
        return True

    normalized_unit = re.sub(r"\s+", " ", unit_text)
    normalized_article = re.sub(r"\s+", " ", article_content or "")
    if normalized_unit in normalized_article:
        return True

    unit_tokens = set(re.findall(r"[A-Za-z0-9]+", normalized_unit.lower()))
    if len(unit_tokens) < 6:
        return True
    article_tokens = set(re.findall(r"[A-Za-z0-9]+", normalized_article.lower()))
    if not article_tokens:
        return False
    return len(unit_tokens & article_tokens) / len(unit_tokens) >= 0.65


def _final_text_paragraph_unit_number(base_unit_number: str, duplicate_count: int, index: int) -> str:
    """生成 ProvisionTextParagraph 最终唯一编号。"""
    base = str(base_unit_number or "").strip()
    if duplicate_count <= 1:
        return base
    if re.search(r"\s+paragraph\s+\d+\s*$", base, re.IGNORECASE):
        return base
    return f"{base} paragraph {index}"


def _normalize_citation_props(props: dict[str, Any]) -> dict[str, Any]:
    """规范化 Citation 属性，尤其是内部引用布尔值。"""
    result = dict(props)
    if any(key in result for key in ("is_internal_reference", "internal_reference", "is_internal")):
        result["is_internal_reference"] = _normalize_bool(
            result.get("is_internal_reference") or result.get("internal_reference") or result.get("is_internal")
        )
    return result


def _normalize_relation_type(value: str) -> str:
    """把关系类型统一成大写下划线形式。"""
    relation_type = re.sub(r"[^0-9A-Za-z]+", "_", str(value or "")).strip("_").upper()
    return relation_type or "RELATED_TO"


def make_edge(
    source_id: str,
    target_id: str,
    relation_type: str,
    filename: str,
    properties: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """创建项目统一格式的图谱关系。"""
    return {
        # source_id 指向起点节点。
        "source_id": source_id,
        # target_id 指向终点节点。
        "target_id": target_id,
        # relation_type 使用英文大写关系名。
        "relation_type": _normalize_relation_type(relation_type),
        # 当前图谱关系均按单向关系处理。
        "directionality": "single",
        # properties 保存关系附加属性。
        "properties": dict(properties or {}),
        # filename 记录关系来自哪个原始文件。
        "filename": filename,
    }


def _entity_key(name: str) -> str:
    """把实体名称归一化成查找键。"""
    return re.sub(r"\s+", " ", str(name or "").strip()).lower()


def _register_lookup(lookup: dict[str, str], name: Any, node_id: str) -> None:
    """向名称索引中注册一个别名到 node_id 的映射。"""
    key = _entity_key(str(name or ""))
    if key:
        lookup.setdefault(key, node_id)


def _register_many(lookup: dict[str, str], node_id: str, values: list[Any]) -> None:
    """批量注册多个别名到同一个 node_id。"""
    for value in values:
        _register_lookup(lookup, value, node_id)


def _edge_key(edge: dict[str, Any]) -> tuple[str, str, str]:
    """提取关系去重键。"""
    return (
        str(edge.get("source_id") or ""),
        str(edge.get("target_id") or ""),
        str(edge.get("relation_type") or ""),
    )


def _append_edge(edges: list[dict[str, Any]], seen: set[tuple[str, str, str]], edge: dict[str, Any]) -> None:
    """追加关系，并根据 source/target/type 去重。"""
    key = _edge_key(edge)
    if not all(key) or key in seen:
        return
    edges.append(edge)
    seen.add(key)


def _dedupe_edges(edges: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """对已有关系列表做一次整体去重。"""
    seen: set[tuple[str, str, str]] = set()
    result = []
    for edge in edges:
        _append_edge(result, seen, edge)
    return result


def _truthy(value: Any) -> bool:
    """把模型输出的 Yes/true/1/internal 等值判断为真。"""
    return str(value or "").strip().lower() in {"yes", "true", "1", "internal", "y"}


def _is_internal_citation(props: dict[str, Any]) -> bool:
    """判断 Citation 是否指向当前文件内部条款。"""
    return _truthy(
        props.get("is_internal_reference")
        or props.get("internal_reference")
        or props.get("is_internal")
    )


def _citation_aliases(name: Any, props: dict[str, Any]) -> list[str]:
    """从 Citation 节点名称和属性中提取可用于匹配的别名。"""
    aliases = [
        name,
        props.get("citation_text"),
        props.get("provision_number"),
        props.get("official_title"),
    ]
    return [str(alias).strip() for alias in aliases if str(alias or "").strip()]


def _citation_key(name: Any, props: dict[str, Any]) -> str:
    """生成外部 Citation 去重键。"""
    text = props.get("citation_text") or props.get("official_title") or name or "Citation"
    provision = props.get("provision_number") or ""
    return _entity_key(f"{text}|{provision}")


def _article_aliases_from_text(text: str) -> list[str]:
    """从文本中解析 Article 形式的引用别名。"""
    aliases = []
    # 支持 Article 2、Article 2(1)、Article 2(1)(a) 等常见引用。
    for match in re.finditer(r"\bArticle\s+\d+[A-Za-z]?(?:\([^)]+\))*", str(text or ""), re.IGNORECASE):
        aliases.append(match.group(0))
        # 同时注册基础 Article 编号，便于 Article 2(1) 回退匹配 Article 2。
        base = re.match(r"\bArticle\s+\d+[A-Za-z]?", match.group(0), re.IGNORECASE)
        if base:
            aliases.append(base.group(0))
    return aliases


def _resolve_internal_citation(name: Any, props: dict[str, Any], lookup: dict[str, str]) -> str | None:
    """尝试把内部 Citation 解析到已经存在的 Article 或 ProvisionUnit 节点。"""
    # 先从 Citation 名称和属性中提取候选别名。
    aliases = _citation_aliases(name, props)
    # 再从候选别名中提取 Article 编号形式。
    for alias in list(aliases):
        aliases.extend(_article_aliases_from_text(alias))
    # dict.fromkeys 用于保持顺序并去重。
    for alias in dict.fromkeys(aliases):
        target_id = lookup.get(_entity_key(alias))
        if target_id:
            return target_id
    return None


class FormatOneGraphBuilder:
    """格式一英文法规图谱装配类。"""

    def __init__(self, lenient_mode: bool = True):
        """初始化图谱装配器。"""
        # lenient_mode 保留为兼容配置；Article 节点始终由一阶段生成，不再因 LLM 失败缺失。
        self.lenient_mode = lenient_mode
        # warnings 用于收集装配过程中无法解析但不致命的问题。
        self.warnings: list[str] = []
        # errors 用于收集 Article 抽取失败等不应入库为正常节点的问题。
        self.errors: list[str] = []

    def _build_document_node(
        self,
        filename: str,
        split_result: dict[str, Any],
        file_info_result: Any | None,
    ) -> tuple[dict[str, Any], dict[str, str], list[dict[str, Any]], list[dict[str, Any]]]:
        """构建法规文件节点和文件级法律依据节点。"""
        # 先把文件级 LLM 结果统一成 entities / relations。
        normalized = normalize_extraction_result(file_info_result)
        # fallback_metadata 来自规则切分器，可在 LLM 缺失字段时兜底。
        fallback = dict(split_result.get("fallback_metadata") or {})
        # 合规风险类型来自文首中文业务字段。
        risk_type = split_result.get("compliance_risk_type") or ""
        # 文件级 LLM 应抽取 exactly one LegalDocument，这里按类型筛选。
        document_entities = [
            entity for entity in normalized["entities"]
            if entity.get("entity_type") == LEGAL_DOCUMENT
        ]
        # LegalBasis 是法规文件的上位法律依据，例如 TFEU Article。
        basis_entities = [
            entity for entity in normalized["entities"]
            if entity.get("entity_type") == LEGAL_BASIS
        ]

        if document_entities:
            # 如果 LLM 给出了 LegalDocument，则用 LLM 属性覆盖规则兜底属性。
            doc_entity = document_entities[0]
            props = dict(fallback)
            props.update(doc_entity.get("properties") or {})
            name = doc_entity.get("name") or props.get("document_name") or fallback.get("document_name") or filename
        else:
            # 如果文件级 LLM 未输出 LegalDocument，则直接用规则元数据兜底。
            props = dict(fallback)
            name = props.get("document_name") or filename

        # 按业务要求，最终图谱中保留中文属性名“合规风险类型”。
        props["合规风险类型"] = risk_type
        # 同时提供数组化英文属性，供后续导出 regulation_knowledge.risk_types 使用。
        props["risk_types"] = _split_risk_types(risk_type)
        # 记录源文件名，便于 Neo4j 里回溯来源。
        props.setdefault("source_filename", filename)
        # 记录文件格式，后续多格式合并时可区分来源。
        props.setdefault("document_format", split_result.get("document_format"))
        # 创建法规文件主节点。
        doc_node = make_node(name, LEGAL_DOCUMENT, props, filename)

        # node_lookup 是文件级节点索引，用于解析 BASED_ON 等关系目标。
        node_lookup: dict[str, str] = {}
        _register_many(node_lookup, doc_node["node_id"], [doc_node["node_name"], LEGAL_DOCUMENT, "Legal Document"])
        # nodes 收集文件级节点，初始只有法规文件节点。
        nodes = [doc_node]
        # edges 收集文件级关系。
        edges = []
        # edge_seen 用于在文件级关系内部去重。
        edge_seen: set[tuple[str, str, str]] = set()

        # 遍历文件级法律依据实体，生成 LegalBasis 节点和 BASED_ON 边。
        for basis in basis_entities:
            basis_props = basis.get("properties") or {}
            basis_node = make_node(
                basis.get("name") or basis_props.get("official_title") or basis_props.get("citation_text") or LEGAL_BASIS,
                LEGAL_BASIS,
                basis_props,
                filename,
            )
            nodes.append(basis_node)
            # 同时注册名称、official_title、citation_text 三类可匹配别名。
            _register_many(
                node_lookup,
                basis_node["node_id"],
                [basis_node["node_name"], basis_props.get("official_title"), basis_props.get("citation_text")],
            )
            # 法规文件基于该法律依据。
            _append_edge(edges, edge_seen, make_edge(doc_node["node_id"], basis_node["node_id"], "BASED_ON", filename))

        # 如果 LLM 显式输出 BASED_ON 关系，则尝试解析并保留。
        for relation in normalized["relations"]:
            if _normalize_relation_type(relation.get("type")) != "BASED_ON":
                continue
            target = node_lookup.get(_entity_key(relation.get("target")))
            if target:
                _append_edge(
                    edges,
                    edge_seen,
                    make_edge(doc_node["node_id"], target, "BASED_ON", filename, relation.get("properties")),
                )
            else:
                # 找不到目标节点时只记录 warning，不中断整份文件。
                self.warnings.append(f"Unresolved file-info relation target: {relation.get('target')}")

        return doc_node, node_lookup, nodes, edges

    def _build_article_contexts(
        self,
        filename: str,
        split_result: dict[str, Any],
        article_results: list[dict[str, Any]],
        failed_articles: list[dict[str, Any]],
        global_lookup: dict[str, str],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """第一阶段：先构建全部 Article 节点和全局引用索引。"""
        # nodes 只收集 Article 级 LegalProvision 节点。
        nodes = []
        # contexts 保存每个 Article 后续处理 ProvisionUnit / Citation 所需的局部上下文。
        contexts = []
        # 将 LLM Article 结果按 Article 编号映射，便于和切分结果对齐。
        article_result_map = {
            (result.get("article_number") or result.get("section_number") or ""): result
            for result in article_results
        }
        # 收集失败 Article，后续只记录错误，不再生成 fallback 节点。
        failed_map = {
            failed.get("article_number") or failed.get("section_number") or "": failed
            for failed in failed_articles
        }

        # 按切分器输出顺序遍历每个 Article，保证最终图谱顺序稳定。
        for article in split_result.get("clauses", []):
            article_number = article.get("article_number") or ""
            # 根据 Article 编号查找对应 LLM 结果。
            result = article_result_map.get(article_number)
            # 标准化该 Article 的 LLM 输出。
            normalized = normalize_extraction_result(result)
            # 一个 Article 理论上应有一个 LegalProvision 实体。
            provision_entities = [
                entity for entity in normalized["entities"]
                if entity.get("entity_type") == LEGAL_PROVISION
            ]
            # LegalProvision 基础节点始终由一阶段 Article 生成；LLM 只补充语义属性。
            props: dict[str, Any] = {}
            provision_name = article_number
            if provision_entities:
                provision = provision_entities[0]
                props.update(provision.get("properties") or {})
                provision_name = provision.get("name") or article_number
                for key in SPLITTER_LOCKED_PROVISION_FIELDS:
                    props.pop(key, None)
            elif article_number in failed_map:
                # LLM 失败只记录错误，不影响一阶段 LegalProvision 节点生成。
                failed = failed_map.get(article_number) or {}
                self.errors.append(
                    f"Article extraction failed; LegalProvision kept from splitter: "
                    f"{filename} {article_number}: {failed.get('error') or 'unknown error'}"
                )
            elif not result:
                # 没有 LLM 结果只记录错误，不影响一阶段 LegalProvision 节点生成。
                self.errors.append(
                    f"Article extraction missing; LegalProvision kept from splitter: {filename} {article_number}"
                )
            else:
                # 有 LLM 结果但没有 LegalProvision，视为抽取结构错误；节点仍由一阶段保留。
                self.errors.append(
                    f"Article extraction has no LegalProvision; LegalProvision kept from splitter: "
                    f"{filename} {article_number}"
                )

            props.update({
                "provision_number": article_number,
                "provision_heading": article.get("article_heading"),
                "provision_content": article.get("content", ""),
                "is_amendment_article": article.get("is_amendment_article", False),
            })
            _add_classification_flat_fields(props, article)
            article_node = make_node(provision_name, LEGAL_PROVISION, props, filename)

            nodes.append(article_node)
            # local_lookup 只服务于当前 Article 内部的实体关系解析。
            local_lookup: dict[str, str] = {}
            # 同一 Article 可能被 Article 编号、节点名、provision_number 等多种方式引用。
            aliases = [
                article_node["node_name"],
                article_number,
                article_node["properties"].get("provision_number"),
                LEGAL_PROVISION,
                "Legal Provision",
            ]
            # 注册局部别名，供当前 Article 的关系解析使用。
            _register_many(local_lookup, article_node["node_id"], aliases)
            # 注册全局别名，供跨 Article 内部 Citation 解析使用；不注册泛化的 LegalProvision。
            _register_many(global_lookup, article_node["node_id"], aliases[:-1])
            # 保存后续第二阶段处理所需上下文。
            contexts.append(
                {
                    "article": article,
                    "article_node": article_node,
                    "normalized": normalized,
                    "local_lookup": local_lookup,
                }
            )
        return nodes, contexts

    def _build_article_nodes(
        self,
        filename: str,
        doc_node: dict[str, Any],
        split_result: dict[str, Any],
        article_results: list[dict[str, Any]],
        failed_articles: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """第二阶段：处理 Article 内部节点、Citation 和关系。"""
        # global_lookup 用于解析跨 Article 的内部引用。
        global_lookup: dict[str, str] = {}
        # 先注册法规文件节点，方便后续如有需要解析到 LegalDocument。
        _register_many(global_lookup, doc_node["node_id"], [doc_node["node_name"], LEGAL_DOCUMENT, "Legal Document"])
        # external_citation_lookup 用于复用外部引用节点，减少重复 Citation。
        external_citation_lookup: dict[str, dict[str, Any]] = {}
        # edge_seen 用于 Article 阶段关系去重。
        edge_seen: set[tuple[str, str, str]] = set()
        # edges 收集 Article 阶段关系。
        edges: list[dict[str, Any]] = []

        # 第一阶段先创建所有 Article 节点，确保后向引用也能被解析。
        article_nodes, contexts = self._build_article_contexts(
            filename,
            split_result,
            article_results,
            failed_articles,
            global_lookup,
        )
        # nodes 初始为全部 Article 节点，后续会追加 ProvisionUnit 和 Citation 节点。
        nodes = list(article_nodes)

        # 遍历每个 Article 上下文，处理该 Article 的条文单元和引用。
        for context in contexts:
            article = context["article"]
            article_node = context["article_node"]
            normalized = context["normalized"]
            node_lookup = context["local_lookup"]
            article_number = article.get("article_number") or ""
            article_content = article.get("content") or ""
            # 法规文件包含每个 Article。
            _append_edge(edges, edge_seen, make_edge(doc_node["node_id"], article_node["node_id"], "CONTAINS", filename))

            # 第一轮处理实体：生成 ProvisionUnit，解析或生成 Citation。
            for entity in normalized["entities"]:
                entity_type = entity.get("entity_type")
                # LegalProvision 已在第一阶段生成，这里跳过避免重复节点。
                if entity_type == LEGAL_PROVISION:
                    continue
                # 当前 Article 阶段只接受 ProvisionUnit 和 Citation。
                if entity_type not in {PROVISION_UNIT, CITATION}:
                    self.warnings.append(f"Unsupported entity type in {article_number}: {entity_type}")
                    continue

                # 提取实体属性和名称；名称为空时按属性字段兜底。
                props = entity.get("properties") or {}
                name = entity.get("name") or props.get("unit_number") or props.get("citation_text") or entity_type

                if entity_type == PROVISION_UNIT:
                    # ProvisionUnit 是 Article 内部可引用的稳定法律规则单元。
                    props = _normalize_provision_unit_props(props)
                    if not props.get("unit_number") and name:
                        props["unit_number"] = name
                    old_unit_number = props.get("unit_number")
                    props, unit_number_change = _normalize_unit_number_for_parent(props, article_number)
                    if unit_number_change:
                        self.warnings.append(
                            f"Normalized ProvisionUnit.unit_number to parent Article: "
                            f"{filename} {article_number} {unit_number_change}"
                        )
                    if not _unit_number_belongs_to_article(props.get("unit_number"), article_number):
                        raise ValueError(
                            f"ProvisionUnit unit_number is not anchored to current Article: "
                            f"{filename} {article_number} {props.get('unit_number') or name}"
                        )
                    if not _unit_content_supported_by_article(props.get("unit_content"), article_content):
                        raise ValueError(
                            f"ProvisionUnit content is not supported by current Article text: "
                            f"{filename} {article_number} {props.get('unit_number') or name}"
                        )
                    name = props.get("unit_number")
                    node = make_node(name, PROVISION_UNIT, props, filename)
                    nodes.append(node)
                    # ProvisionUnit 可通过节点名或 unit_number 被关系引用。
                    aliases = [node["node_name"], props.get("unit_number")]
                    _register_many(node_lookup, node["node_id"], aliases)
                    _register_many(global_lookup, node["node_id"], aliases)
                    # Article 包含 ProvisionUnit。
                    _append_edge(edges, edge_seen, make_edge(article_node["node_id"], node["node_id"], "CONTAINS", filename))
                    continue

                # Citation 先尝试解析为当前文件内部节点。
                target_id = None
                if _is_internal_citation(props):
                    target_id = _resolve_internal_citation(name, props, global_lookup)
                    if not target_id:
                        # 内部引用未解析成功时记录 warning，后续会生成 Citation 占位节点。
                        self.warnings.append(f"Unresolved internal citation in {article_number}: {name}")
                if target_id:
                    # 内部引用解析成功后，不再额外创建 Citation 节点，只把别名注册到局部索引。
                    _register_many(node_lookup, target_id, _citation_aliases(name, props))
                    _append_edge(edges, edge_seen, make_edge(article_node["node_id"], target_id, "CITES", filename, {"source": "citation_entity_fallback"}))
                    continue

                # 外部 Citation 或未解析内部 Citation 进入节点归并流程。
                citation_key = _citation_key(name, props)
                node = external_citation_lookup.get(citation_key)
                if node is None:
                    # 第一次遇到该 Citation 时创建节点。
                    citation_props = _normalize_citation_props(props)
                    if _is_internal_citation(props):
                        # 未解析成功的内部引用保留状态，便于后续人工修复或二次匹配。
                        citation_props["resolution_status"] = "unresolved_internal_reference"
                    node = make_node(name, CITATION, citation_props, filename)
                    external_citation_lookup[citation_key] = node
                    nodes.append(node)
                # 把 Citation 的各种名称注册到当前 Article 局部索引，供关系解析。
                _register_many(node_lookup, node["node_id"], _citation_aliases(name, props))
                # 兜底接入主图，避免 Citation 节点在 Neo4j 中成为孤立节点。
                _append_edge(edges, edge_seen, make_edge(article_node["node_id"], node["node_id"], "CITES", filename, {"source": "citation_entity_fallback"}))

            # 第二轮处理关系：此时当前 Article 的节点索引已经尽量完整。
            for relation in normalized["relations"]:
                relation_type = _normalize_relation_type(relation.get("type"))
                # source 优先在当前 Article 内找，找不到再尝试全局索引。
                source_id = node_lookup.get(_entity_key(relation.get("source"))) or global_lookup.get(_entity_key(relation.get("source")))
                # target 同样先局部后全局。
                target_id = node_lookup.get(_entity_key(relation.get("target"))) or global_lookup.get(_entity_key(relation.get("target")))
                if not target_id:
                    # 如果 target 是类似 Article 2 的文本，再尝试内部引用解析。
                    target_id = _resolve_internal_citation(relation.get("target"), relation.get("properties") or {}, global_lookup)
                if not source_id or not target_id:
                    # 无法解析端点时只记录 warning，不让整份图谱构建失败。
                    self.warnings.append(
                        f"Unresolved relation in {article_number}: "
                        f"{relation.get('source')} - {relation_type} - {relation.get('target')}"
                    )
                    continue
                # 解析成功后追加关系，并进行去重。
                _append_edge(
                    edges,
                    edge_seen,
                    make_edge(source_id, target_id, relation_type, filename, relation.get("properties")),
                )

        return nodes, edges

    def _article_node_name(self, article: dict[str, Any], provision_entity: dict[str, Any] | None = None) -> str:
        """生成 v1 LegalProvision 节点名。"""
        article_number = str(article.get("article_number") or "").strip()
        heading = str(article.get("article_heading") or "").strip()
        if provision_entity and provision_entity.get("name"):
            return str(provision_entity.get("name"))
        return f"{article_number} - {heading}" if heading else article_number

    def _build_v1_article_contexts(
        self,
        filename: str,
        split_result: dict[str, Any],
        article_results: list[dict[str, Any]],
        failed_articles: list[dict[str, Any]],
        global_lookup: dict[str, str],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """构建 v1 Article/LegalProvision 节点，Article 阶段不处理 Clause。"""
        nodes: list[dict[str, Any]] = []
        contexts: list[dict[str, Any]] = []
        article_result_map = {
            (result.get("article_number") or result.get("section_number") or ""): result
            for result in article_results
        }
        failed_map = {
            failed.get("article_number") or failed.get("section_number") or "": failed
            for failed in failed_articles
        }

        for article in split_result.get("clauses", []):
            article_number = article.get("article_number") or ""
            result = article_result_map.get(article_number)
            normalized = normalize_extraction_result(result)
            provision_entities = [
                entity for entity in normalized["entities"]
                if entity.get("entity_type") == LEGAL_PROVISION
            ]
            provision_entity = provision_entities[0] if provision_entities else None
            props: dict[str, Any] = {}
            if provision_entity:
                props.update(provision_entity.get("properties") or {})
                for key in SPLITTER_LOCKED_PROVISION_FIELDS:
                    props.pop(key, None)
            elif article_number in failed_map:
                failed = failed_map.get(article_number) or {}
                self.errors.append(
                    f"Article extraction failed; LegalProvision kept from splitter: "
                    f"{filename} {article_number}: {failed.get('error') or 'unknown error'}"
                )
            elif not result:
                self.errors.append(
                    f"Article extraction missing; LegalProvision kept from splitter: {filename} {article_number}"
                )
            elif normalized["entities"]:
                self.errors.append(
                    f"Article extraction has no LegalProvision; LegalProvision kept from splitter: "
                    f"{filename} {article_number}"
                )

            props.update({
                "provision_number": article_number,
                "provision_heading": article.get("article_heading"),
                "provision_content": article.get("content", ""),
                "is_amendment_article": article.get("is_amendment_article", False),
            })
            _add_classification_flat_fields(props, article)
            article_node = make_node(self._article_node_name(article, provision_entity), LEGAL_PROVISION, props, filename)
            nodes.append(article_node)

            local_lookup: dict[str, str] = {}
            aliases = [
                article_node["node_name"],
                article_number,
                article_node["properties"].get("provision_number"),
                LEGAL_PROVISION,
                "Legal Provision",
            ]
            _register_many(local_lookup, article_node["node_id"], aliases)
            _register_many(global_lookup, article_node["node_id"], aliases[:-1])
            contexts.append({
                "article": article,
                "article_node": article_node,
                "local_lookup": local_lookup,
            })
        return nodes, contexts

    def _build_v1_article_nodes(
        self,
        filename: str,
        doc_node: dict[str, Any],
        split_result: dict[str, Any],
        raw: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """构建 v1 Article、ProvisionClause、ProvisionTextParagraph 和 Citation。"""
        global_lookup: dict[str, str] = {}
        _register_many(global_lookup, doc_node["node_id"], [doc_node["node_name"], LEGAL_DOCUMENT, "Legal Document"])
        external_citation_lookup: dict[str, dict[str, Any]] = {}
        edge_seen: set[tuple[str, str, str]] = set()
        edges: list[dict[str, Any]] = []

        article_nodes, contexts = self._build_v1_article_contexts(
            filename,
            split_result,
            raw.get("article_extractions") or [],
            raw.get("failed_article_extractions") or [],
            global_lookup,
        )
        nodes = list(article_nodes)
        clause_result_map = {
            str(result.get("unit_number") or ""): result
            for result in raw.get("provision_clause_extractions") or []
        }
        failed_clause_map = {
            str(result.get("unit_number") or ""): result
            for result in raw.get("failed_provision_clause_extractions") or []
        }

        for context in contexts:
            article = context["article"]
            article_node = context["article_node"]
            article_number = article.get("article_number") or ""
            node_lookup = dict(context["local_lookup"])
            _append_edge(edges, edge_seen, make_edge(doc_node["node_id"], article_node["node_id"], "CONTAINS", filename))

            for provision_clause in article.get("provision_clauses") or []:
                clause_number = str(provision_clause.get("unit_number") or "").strip()
                clause_content = provision_clause.get("unit_content") or ""
                result = clause_result_map.get(clause_number)
                normalized = normalize_extraction_result(result)
                clause_entities = [
                    entity for entity in normalized["entities"]
                    if entity.get("entity_type") == PROVISION_CLAUSE
                ]
                clause_props: dict[str, Any] = {}
                if clause_entities:
                    clause_props.update(clause_entities[0].get("properties") or {})
                elif clause_number in failed_clause_map:
                    failed = failed_clause_map.get(clause_number) or {}
                    self.errors.append(
                        f"ProvisionClause extraction failed; code clause kept: "
                        f"{filename} {clause_number}: {failed.get('error') or 'unknown error'}"
                    )
                elif not result:
                    self.errors.append(
                        f"ProvisionClause extraction missing; code clause kept: {filename} {clause_number}"
                    )

                if "legal_function" in clause_props:
                    clause_props["legal_function"] = _normalize_legal_function(clause_props.get("legal_function"))
                # 代码锁定字段不得由 LLM 覆盖。
                clause_props.update({
                    "unit_number": clause_number,
                    "unit_level": provision_clause.get("unit_level") or "paragraph",
                    "unit_content": clause_content,
                    "source_article_number": article_number,
                    "clause_index": provision_clause.get("clause_index"),
                    "explicit_boundary": provision_clause.get("explicit_boundary"),
                    "line_start": provision_clause.get("line_start"),
                    "line_end": provision_clause.get("line_end"),
                })
                clause_node = make_node(clause_number, PROVISION_CLAUSE, clause_props, filename)
                nodes.append(clause_node)
                _register_many(node_lookup, clause_node["node_id"], [clause_number, clause_node["node_name"], PROVISION_CLAUSE])
                _register_many(global_lookup, clause_node["node_id"], [clause_number, clause_node["node_name"]])
                _append_edge(edges, edge_seen, make_edge(article_node["node_id"], clause_node["node_id"], "CONTAINS", filename))

                text_paragraph_nodes: list[dict[str, Any]] = []
                text_paragraph_candidates: list[tuple[dict[str, Any], str]] = []
                text_paragraph_base_counts: dict[str, int] = {}
                citation_candidates: list[tuple[Any, dict[str, Any]]] = []
                for entity in normalized["entities"]:
                    entity_type = entity.get("entity_type")
                    if entity_type in {LEGAL_PROVISION, PROVISION_CLAUSE}:
                        continue
                    props = entity.get("properties") or {}
                    name = entity.get("name") or props.get("unit_number") or props.get("citation_text") or entity_type

                    if entity_type == PROVISION_TEXT_PARAGRAPH:
                        props = _normalize_provision_unit_props(props)
                        if not props.get("unit_number") and name:
                            props["unit_number"] = name
                        unit_number = str(props.get("unit_number") or "").strip()
                        if not unit_number.startswith(clause_number):
                            self.warnings.append(
                                f"Skipped ProvisionTextParagraph outside parent clause: "
                                f"{filename} {clause_number} {unit_number or name}"
                            )
                            continue
                        if not _unit_content_supported_by_article(props.get("unit_content"), clause_content):
                            self.warnings.append(
                                f"Skipped ProvisionTextParagraph not supported by clause text: "
                                f"{filename} {clause_number} {unit_number or name}"
                            )
                            continue
                        text_paragraph_candidates.append((props, unit_number))
                        text_paragraph_base_counts[unit_number] = text_paragraph_base_counts.get(unit_number, 0) + 1
                        continue

                    if entity_type == CITATION:
                        citation_candidates.append((name, props))
                        continue

                    if entity_type:
                        self.warnings.append(f"Unsupported entity type in {clause_number}: {entity_type}")

                text_paragraph_seen: dict[str, int] = {}
                for props, base_unit_number in text_paragraph_candidates:
                    text_paragraph_seen[base_unit_number] = text_paragraph_seen.get(base_unit_number, 0) + 1
                    final_unit_number = _final_text_paragraph_unit_number(
                        base_unit_number,
                        text_paragraph_base_counts.get(base_unit_number, 1),
                        text_paragraph_seen[base_unit_number],
                    )
                    props["unit_number"] = final_unit_number
                    text_node = make_node(final_unit_number, PROVISION_TEXT_PARAGRAPH, props, filename)
                    nodes.append(text_node)
                    text_paragraph_nodes.append(text_node)
                    aliases = [text_node["node_name"], final_unit_number]
                    if text_paragraph_base_counts.get(base_unit_number, 1) == 1:
                        aliases.append(base_unit_number)
                    _register_many(node_lookup, text_node["node_id"], aliases)
                    _register_many(global_lookup, text_node["node_id"], aliases)
                    _append_edge(edges, edge_seen, make_edge(clause_node["node_id"], text_node["node_id"], "CONTAINS", filename))

                for name, props in citation_candidates:
                    target_id = None
                    if _is_internal_citation(props):
                        target_id = _resolve_internal_citation(name, props, global_lookup)
                        if not target_id:
                            self.warnings.append(f"Unresolved internal citation in {clause_number}: {name}")
                    if target_id:
                        _register_many(node_lookup, target_id, _citation_aliases(name, props))
                        continue
                    citation_key = _citation_key(name, props)
                    node = external_citation_lookup.get(citation_key)
                    if node is None:
                        citation_props = _normalize_citation_props(props)
                        if _is_internal_citation(props):
                            citation_props["resolution_status"] = "unresolved_internal_reference"
                        node = make_node(name, CITATION, citation_props, filename)
                        external_citation_lookup[citation_key] = node
                        nodes.append(node)
                    _register_many(node_lookup, node["node_id"], _citation_aliases(name, props))
                    if text_paragraph_nodes:
                        _append_edge(
                            edges,
                            edge_seen,
                            make_edge(text_paragraph_nodes[0]["node_id"], node["node_id"], "CITES", filename, {"source": "citation_entity_fallback"}),
                        )

                for relation in normalized["relations"]:
                    relation_type = _normalize_relation_type(relation.get("type"))
                    source_id = node_lookup.get(_entity_key(relation.get("source"))) or global_lookup.get(_entity_key(relation.get("source")))
                    target_id = node_lookup.get(_entity_key(relation.get("target"))) or global_lookup.get(_entity_key(relation.get("target")))
                    if not target_id:
                        target_id = _resolve_internal_citation(relation.get("target"), relation.get("properties") or {}, global_lookup)
                    if not source_id or not target_id:
                        self.warnings.append(
                            f"Unresolved relation in {clause_number}: "
                            f"{relation.get('source')} - {relation_type} - {relation.get('target')}"
                        )
                        continue
                    _append_edge(edges, edge_seen, make_edge(source_id, target_id, relation_type, filename, relation.get("properties")))

        return nodes, edges

    def build(
        self,
        filename: str,
        split_result: dict[str, Any],
        raw_llm_result: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """构建最终图谱结果。"""
        # 每次 build 都从切分器 warnings 开始，避免沿用上次构建状态。
        self.warnings = list(split_result.get("warnings") or [])
        # 每次 build 重置 errors，避免沿用上次构建状态。
        self.errors = []
        # raw 为空时无法构建 Article 级 LLM 知识，只保留文件级节点并记录错误。
        raw = raw_llm_result or {}
        # 构建法规文件节点和文件级依据节点。
        doc_node, _lookup, file_nodes, file_edges = self._build_document_node(
            filename,
            split_result,
            raw.get("file_info_extraction"),
        )
        # v1 构建 Article、ProvisionClause、ProvisionTextParagraph、Citation。
        article_nodes, article_edges = self._build_v1_article_nodes(filename, doc_node, split_result, raw)
        # 最终节点列表由文件级节点和 Article 级节点拼接得到。
        nodes = file_nodes + article_nodes
        # 最终关系列表再做一次整体去重。
        edges = _dedupe_edges(file_edges + article_edges)
        # metadata 汇总本次图谱构建的关键统计信息。
        metadata = {
            "filename": filename,
            "document_format": split_result.get("document_format"),
            "compliance_risk_type": split_result.get("compliance_risk_type", ""),
            "article_count": len(split_result.get("clauses", [])),
            "annex_count": len(split_result.get("annexes_metadata", [])),
            "skipped_annex_count": len(split_result.get("annexes_metadata", [])),
            "recital_count": len(split_result.get("recitals", [])),
            "success_article_count": len(raw.get("article_extractions") or []),
            "failed_article_count": len(raw.get("failed_article_extractions") or []),
            "provision_clause_count": sum(len(article.get("provision_clauses") or []) for article in split_result.get("clauses", [])),
            "success_clause_count": len(raw.get("provision_clause_extractions") or []),
            "failed_clause_count": len(raw.get("failed_provision_clause_extractions") or []),
            "nodes": len(nodes),
            "edges": len(edges),
            "warnings": self.warnings,
            "errors": list(raw.get("errors", []) or []) + self.errors,
        }
        # 返回完整图谱、切分结果和 raw LLM 结果，方便审查全链路。
        return {
            "nodes": nodes,
            "edges": edges,
            "metadata": metadata,
            "split_result": {
                **split_result,
                "annexes": split_result.get("annexes_metadata", []),
            },
            "raw_llm_result": raw,
        }

