"""Schema-derived validation and extraction normalization for guide_p1."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from app.infrastructure.information_extraction.base import Entity, Relationship

from .prompt.schema import (
    CONSTRAINT_STRENGTH_VALUES,
    KNOWLEDGE_TYPE_VALUES,
    OBJECT_KEY,
    PREDICATE_KEY,
    QUANTITATIVE_RELATION_VALUES,
    QUANTITATIVE_VALUE_TYPE_VALUES,
    RELATION_CLASS,
    RESPONSIBILITY_ROLE_VALUES,
    RISK_TYPE_VALUES,
    SUBJECT_KEY,
    schema_for_content_block,
    schema_for_file_info,
)


@dataclass(frozen=True)
class SchemaContract:
    allowed_properties: dict[str, frozenset[str]]
    legal_triples: frozenset[tuple[str, str, str]]
    relationship_types: frozenset[str]


def _parse_entity_properties(schema_text: str) -> dict[str, set[str]]:
    entity_part = schema_text.split("# 关系", 1)[0]
    allowed: dict[str, set[str]] = {}
    for match in re.finditer(r"^##\s+(.+?)\s*\n(.*?)(?=^##\s+|\Z)", entity_part, flags=re.M | re.S):
        entity_type = match.group(1).strip()
        attr_match = re.search(r"^###\s+属性\s*\n(.*?)(?=^###\s+|\Z)", match.group(2), flags=re.M | re.S)
        if not attr_match:
            continue
        props = set()
        for line in attr_match.group(1).splitlines():
            prop_match = re.match(r"\s*-\s*([^：:\n]+?)\s*[：:]", line)
            if prop_match:
                props.add(prop_match.group(1).strip())
        if props:
            allowed.setdefault(entity_type, set()).update(props)
    return allowed


def _parse_relation_triples(schema_text: str) -> set[tuple[str, str, str]]:
    relation_part = schema_text.split("# 关系", 1)[1] if "# 关系" in schema_text else ""
    triples: set[tuple[str, str, str]] = set()
    for line in relation_part.splitlines():
        value = line.strip()
        if not value.startswith("- "):
            continue
        parts = [part.strip() for part in value[2:].split("-", 2)]
        if len(parts) == 3 and all(parts):
            triples.add((parts[0], parts[1], parts[2]))
    return triples


def build_schema_contract(*schema_texts: str) -> SchemaContract:
    allowed: dict[str, set[str]] = {}
    triples: set[tuple[str, str, str]] = set()
    for schema_text in schema_texts:
        if not schema_text or "# 实体" not in schema_text or "# 关系" not in schema_text:
            raise ValueError("guide_p1 schema 缺少实体或关系定义")
        for entity_type, props in _parse_entity_properties(schema_text).items():
            allowed.setdefault(entity_type, set()).update(props)
        triples.update(_parse_relation_triples(schema_text))
    if not allowed:
        raise ValueError("guide_p1 schema 未解析出实体属性白名单")
    if not triples:
        raise ValueError("guide_p1 schema 未解析出合法关系三元组")
    return SchemaContract(
        allowed_properties={key: frozenset(value) for key, value in allowed.items()},
        legal_triples=frozenset(triples),
        relationship_types=frozenset(item[1] for item in triples),
    )


SCHEMA_CONTRACT = build_schema_contract(schema_for_file_info, schema_for_content_block)


def to_plain(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return value.model_dump()
    if hasattr(value, "dict"):
        return value.dict()
    if isinstance(value, dict):
        return {key: to_plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_plain(item) for item in value]
    return value


def normalize_extraction_result(value: Any) -> dict[str, list[dict[str, Any]]]:
    plain = to_plain(value or {})
    if isinstance(plain, dict) and "extraction" in plain:
        plain = plain.get("extraction") or {}
    entities: list[dict[str, Any]] = []
    relations: list[dict[str, Any]] = []
    raw_entities = plain.get("entities", []) if isinstance(plain, dict) else []
    for entity in raw_entities or []:
        if isinstance(entity, Entity):
            entity = to_plain(entity)
        if not isinstance(entity, dict):
            continue
        entity_type = str(entity.get("entity_type") or entity.get("type") or entity.get("class") or "").strip()
        props = entity.get("properties") or entity.get("attributes") or {}
        if entity_type in {RELATION_CLASS, "Relation"}:
            relations.append(
                {
                    "source": props.get(SUBJECT_KEY) or props.get("source") or "",
                    "type": props.get(PREDICATE_KEY) or props.get("type") or "",
                    "target": props.get(OBJECT_KEY) or props.get("target") or "",
                    "properties": props,
                }
            )
            continue
        entities.append(
            {
                "name": str(entity.get("name") or entity.get("text") or "").strip(),
                "entity_type": entity_type,
                "properties": dict(props) if isinstance(props, dict) else {},
            }
        )
    raw_relations = plain.get("relations", []) if isinstance(plain, dict) else []
    for relation in raw_relations or []:
        if isinstance(relation, Relationship):
            relation = to_plain(relation)
        if not isinstance(relation, dict):
            continue
        props = relation.get("properties") or {}
        relations.append(
            {
                "source": relation.get("source") or props.get(SUBJECT_KEY) or "",
                "type": relation.get("type") or props.get(PREDICATE_KEY) or "",
                "target": relation.get("target") or props.get(OBJECT_KEY) or "",
                "properties": props,
            }
        )
    return {"entities": entities, "relations": relations}


def normalize_risk_types(value: Any) -> list[str]:
    if isinstance(value, (list, tuple, set)):
        text = " ".join(str(item) for item in value)
    else:
        text = str(value or "")
    return [risk for risk in RISK_TYPE_VALUES if risk in text]


def _normalize_enum(value: Any, allowed: tuple[str, ...]) -> str:
    text = str(value or "").strip()
    return text if text in allowed else ""


def _normalize_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "是"}


def _normalize_numeric(value: Any) -> int | float | None:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value
    text = str(value).replace(",", "").strip()
    try:
        number = float(text)
        return int(number) if number.is_integer() else number
    except ValueError:
        return None


def _normalize_properties(entity_type: str, properties: dict[str, Any], warnings: list[str]) -> dict[str, Any]:
    allowed = SCHEMA_CONTRACT.allowed_properties.get(entity_type, frozenset())
    cleaned: dict[str, Any] = {}
    for key, value in properties.items():
        if key not in allowed:
            warnings.append(f"删除 schema 外属性: {entity_type}.{key}")
            continue
        if key == "合规风险类型":
            cleaned[key] = normalize_risk_types(value)
        elif key == "知识类型":
            normalized = _normalize_enum(value, KNOWLEDGE_TYPE_VALUES)
            if value and not normalized:
                warnings.append(f"不受控知识类型已置空: {value}")
            cleaned[key] = normalized
        elif key == "约束强度":
            normalized = _normalize_enum(value, CONSTRAINT_STRENGTH_VALUES)
            if value and not normalized:
                warnings.append(f"不受控约束强度已置空: {value}")
            cleaned[key] = normalized
        elif key == "责任角色":
            normalized = _normalize_enum(value, RESPONSIBILITY_ROLE_VALUES)
            if value and not normalized:
                warnings.append(f"不受控责任角色已置空: {value}")
            cleaned[key] = normalized
        elif key == "量化值类型":
            cleaned[key] = _normalize_enum(value, QUANTITATIVE_VALUE_TYPE_VALUES)
        elif key == "约束关系":
            cleaned[key] = _normalize_enum(value, QUANTITATIVE_RELATION_VALUES)
        elif key == "是否知识条目候选":
            cleaned[key] = _normalize_bool(value)
        elif key in {"最小值", "最大值"}:
            cleaned[key] = _normalize_numeric(value)
        else:
            cleaned[key] = value
    return cleaned


def validate_extraction_result(
    value: Any,
    context_entity_types: dict[str, str] | None = None,
    source_text: str = "",
) -> dict[str, Any]:
    normalized = normalize_extraction_result(value)
    warnings: list[str] = []
    errors: list[str] = []
    entities: list[dict[str, Any]] = []
    entity_types = dict(context_entity_types or {})
    entity_aliases: dict[str, tuple[str, str]] = {
        name: (name, entity_type) for name, entity_type in entity_types.items()
    }
    evidence_fields = {"证据原文", "原文依据", "责任原文", "原始文本", "引用原文"}

    for entity in normalized["entities"]:
        entity_type = entity.get("entity_type", "")
        name = entity.get("name", "")
        if entity_type not in SCHEMA_CONTRACT.allowed_properties:
            warnings.append(f"删除未知实体类型: {entity_type or '<empty>'}")
            continue
        if not name:
            warnings.append(f"删除名称为空的实体: {entity_type}")
            continue
        props = _normalize_properties(entity_type, entity.get("properties") or {}, warnings)
        for field in evidence_fields.intersection(props):
            evidence = str(props.get(field) or "").strip()
            if evidence and source_text and evidence not in source_text:
                warnings.append(f"删除无法回链当前输入的证据字段: {entity_type}.{field}")
                props[field] = ""
        cleaned = {"name": name, "entity_type": entity_type, "properties": props}
        entities.append(cleaned)
        entity_types.setdefault(name, entity_type)
        for alias in (name, f"{entity_type}_{name}", f"{entity_type} {name}"):
            entity_aliases.setdefault(alias, (name, entity_type))

    relations = []
    for relation in normalized["relations"]:
        source = str(relation.get("source") or "").strip()
        target = str(relation.get("target") or "").strip()
        relation_type = str(relation.get("type") or "").strip()
        source_name, source_type = entity_aliases.get(source, (source, entity_types.get(source, "")))
        target_name, target_type = entity_aliases.get(target, (target, entity_types.get(target, "")))
        if not source_type or not target_type:
            warnings.append(f"删除端点无法解析的关系: {source}-{relation_type}-{target}")
            continue
        triple = (source_type, relation_type, target_type)
        if triple not in SCHEMA_CONTRACT.legal_triples:
            warnings.append(f"删除非法关系三元组: {source_type}-{relation_type}-{target_type}")
            continue
        relations.append(
            {
                "source": source_name,
                "target": target_name,
                "type": relation_type,
                "properties": {},
            }
        )

    return {
        "entities": entities,
        "relations": relations,
        "warnings": warnings,
        "errors": errors,
        "valid": not errors,
    }
