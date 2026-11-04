"""Database-independent identities and validation for persisted knowledge."""

import hashlib
import json
import re
from datetime import date, datetime, timedelta, timezone

CST = timezone(timedelta(hours=8))
RULE_VERSION = "knowledge-record-v1"
RISK_TYPES = frozenset(("产品法律风险", "供应链合规风险", "劳动用工法律合规风险",
                        "企业关联方合规风险", "企业国际化经营合规风险", "企业信用风险"))
INDUSTRIES = frozenset(("通用", "制造业", "电子信息业", "其他"))
STATES = frozenset(("present", "empty", "missing", "conflict", "invalid", "not_applicable"))
BUSINESS_FIELDS = ("knowledge_id", "dataset_version", "content", "knowledge_category", "knowledge_type",
                   "validity_status", "semantic_types", "risk_types", "economic_industries", "tags",
                   "indicators", "document_indicators", "sources", "attribute_provenance", "field_states",
                   "metadata", "content_hash", "conversion_rule_version", "classification_config_version")
JSON_FIELDS = ("semantic_types", "risk_types", "economic_industries", "tags", "indicators",
               "document_indicators", "sources", "attribute_provenance", "field_states", "metadata")


class KnowledgeStorageError(ValueError):
    """Invalid input, incompatible versions or unverifiable storage state."""


def json_default(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if type(value).__module__.startswith("neo4j.time") and hasattr(value, "iso_format"):
        return value.iso_format()
    raise TypeError("Unsupported JSON value type: " + type(value).__name__)


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
                      allow_nan=False, default=json_default)


def digest(value):
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def record_digest(record):
    return digest({key: record.get(key) for key in BUSINESS_FIELDS})


def now_cn():
    return datetime.now(CST).replace(tzinfo=None)


def normalized_text(value):
    return re.sub(r"\s+", " ", str(value or "")).strip()


def validate_indicator(item):
    if not isinstance(item, dict):
        raise KnowledgeStorageError("Indicator must be an object")
    number = item.get("indicator_number")
    if not isinstance(number, str) or not re.fullmatch(r"\d+(?:\.\d+)+", number):
        raise KnowledgeStorageError("Indicator number must be a dotted string")
    for key in ("indicator_name", "indicator_description", "indicator_system_version"):
        if item.get(key) is not None and not isinstance(item[key], str):
            raise KnowledgeStorageError("Invalid indicator text: " + key)


def validate_record(record):
    for key, limit in (("knowledge_id", 80), ("dataset_version", 96), ("knowledge_category", 96),
                       ("knowledge_type", 96), ("conversion_rule_version", 96), ("run_id", 64)):
        value = record.get(key)
        if not isinstance(value, str) or not value.strip() or len(value) > limit:
            raise KnowledgeStorageError("Invalid required field: " + key)
    if not isinstance(record.get("content"), str) or not record["content"].strip():
        raise KnowledgeStorageError("Empty knowledge content")
    if record.get("validity_status") is not None and (
            not isinstance(record["validity_status"], str) or len(record["validity_status"]) > 128):
        raise KnowledgeStorageError("Invalid nullable validity_status")
    if record.get("publication_status") not in {"staging", "published"}:
        raise KnowledgeStorageError("Invalid publication_status")
    for field in ("semantic_types", "tags"):
        if not isinstance(record.get(field), list) or any(not isinstance(x, str) for x in record[field]):
            raise KnowledgeStorageError("Expected string array: " + field)
    for field, allowed in (("risk_types", RISK_TYPES), ("economic_industries", INDUSTRIES)):
        values = record.get(field)
        if values is not None and (not isinstance(values, list) or any(x not in allowed for x in values)):
            raise KnowledgeStorageError("Invalid controlled array: " + field)
    values = record.get("economic_industries") or []
    if "通用" in values and len(set(values)) > 1:
        raise KnowledgeStorageError("General industry must not coexist with specific industries")
    for field in ("indicators", "document_indicators", "sources", "attribute_provenance"):
        values = record.get(field)
        if values is None and field == "indicators":
            continue
        if not isinstance(values, list) or any(not isinstance(x, dict) for x in values):
            raise KnowledgeStorageError("Expected JSON object array: " + field)
    if not record["sources"]:
        raise KnowledgeStorageError("Knowledge must have an auditable source")
    for item in record.get("indicators") or []:
        validate_indicator(item)
    for group in record["document_indicators"]:
        if not group.get("document_key") or group.get("mapping_state") not in STATES:
            raise KnowledgeStorageError("Invalid source document indicator group")
        if group.get("indicators") is not None:
            if not isinstance(group["indicators"], list):
                raise KnowledgeStorageError("Invalid file indicator array")
            for item in group["indicators"]:
                validate_indicator(item)
    for field in ("field_states", "metadata"):
        if not isinstance(record.get(field), dict):
            raise KnowledgeStorageError("Expected JSON object: " + field)
    for key, value in record["field_states"].items():
        if not isinstance(value, dict) or value.get("state") not in STATES:
            raise KnowledgeStorageError("Invalid field state: " + key)
    for field in ("generated_at", "created_at", "updated_at"):
        value = record.get(field)
        if not isinstance(value, datetime) or value.tzinfo is not None:
            raise KnowledgeStorageError("Expected naive Asia/Shanghai timestamp: " + field)
    for field in ("content_hash", "record_hash"):
        if not isinstance(record.get(field), str) or not re.fullmatch("[a-f0-9]{64}", record[field]):
            raise KnowledgeStorageError("Invalid hash: " + field)
    if digest(record["content"]) != record["content_hash"] or record_digest(record) != record["record_hash"]:
        raise KnowledgeStorageError("Knowledge record digest mismatch")
    for field in JSON_FIELDS:
        canonical(record[field])


def from_json_record(record):
    result = dict(record)
    for key in ("generated_at", "created_at", "updated_at"):
        if isinstance(result.get(key), str):
            value = datetime.fromisoformat(result[key])
            result[key] = value.astimezone(CST).replace(tzinfo=None) if value.tzinfo else value
    validate_record(result)
    return result
