"""Read-only MySQL 8 queries; importing/building a query never loads a DB engine.

``build_query`` returns SQL strings and bind values (not interpolated SQL).
Source dates are inclusive ISO dates; row timestamp bounds are inclusive local
Asia/Shanghai timestamps. Array modes apply independently to each dimension.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from importlib import import_module
from typing import Any, Mapping, Optional, Tuple


_TABLE = "t_natural_knowledge_entry"
_ARRAYS = ("risk_types", "economic_industries", "tags", "semantic_types")
_JSON_FIELDS = _ARRAYS + (
    "sources", "indicators", "document_indicators", "field_states",
    "attribute_provenance", "metadata",
)
_SOURCE_DATES = ("publication_date", "effective_date")
_ROW_DATES = ("generated_at", "created_at", "updated_at")
_SORTS = ("knowledge_id", "knowledge_category", "knowledge_type") + _ROW_DATES
_FLAGS = ("error", "errors", "ambiguous", "ambiguity", "parse_error", "processing_error")
_CST = timezone(timedelta(hours=8))


@dataclass(frozen=True)
class QuerySpec:
    """Exact scalar/source filters and independent any/all array filters.

    ``indicator_numbers`` share the optional system version; all-mode fallback
    may cover several matching source files, but never mixes formal and file
    indicators. ``date_field`` is one of publication_date/effective_date (source
    ISO dates) or generated_at/created_at/updated_at (local timestamps).
    """

    dataset_version: str
    publication_status: str = "published"
    diagnostic: bool = False
    keyword: Optional[str] = None
    knowledge_category: Optional[str] = None
    knowledge_type: Optional[str] = None
    validity_status: Optional[str] = None
    graph_label: Optional[str] = None
    filename: Optional[str] = None
    risk_types: Tuple[str, ...] = ()
    risk_types_mode: str = "any"
    economic_industries: Tuple[str, ...] = ()
    economic_industries_mode: str = "any"
    tags: Tuple[str, ...] = ()
    tags_mode: str = "any"
    semantic_types: Tuple[str, ...] = ()
    semantic_types_mode: str = "any"
    indicator_number: Optional[str] = None
    indicator_numbers: Tuple[str, ...] = ()
    indicator_system_version: Optional[str] = None
    indicators_mode: str = "any"
    allow_document_fallback: bool = False
    date_field: Optional[str] = None
    date_from: Optional[Any] = None
    date_to: Optional[Any] = None
    page: int = 1
    page_size: int = 50
    sort_by: str = "knowledge_id"
    sort_direction: str = "asc"

    def validate(self) -> QuerySpec:
        return validate(self)


def _bound(value: Any, source_date: bool) -> Any:
    if source_date:
        if isinstance(value, datetime):
            raise ValueError("Source date bounds must be ISO dates, not timestamps")
        if isinstance(value, str):
            parsed = date.fromisoformat(value)
            if parsed.isoformat() != value:
                raise ValueError("Source date bounds must be YYYY-MM-DD")
            return value
        if isinstance(value, date):
            return value.isoformat()
        raise ValueError("Invalid source date bound")
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    elif isinstance(value, date) and not isinstance(value, datetime):
        value = datetime.combine(value, datetime.min.time())
    if not isinstance(value, datetime):
        raise ValueError("Invalid timestamp bound")
    return value.astimezone(_CST).replace(tzinfo=None) if value.tzinfo else value


def validate(spec: QuerySpec) -> QuerySpec:
    """Validate before importing SQLAlchemy, repository code or DB configuration."""
    if not isinstance(spec, QuerySpec):
        raise TypeError("spec must be a QuerySpec")
    if not isinstance(spec.dataset_version, str) or not spec.dataset_version.strip():
        raise ValueError("dataset_version is required")
    if len(spec.dataset_version) > 96:
        raise ValueError("dataset_version exceeds 96 characters")
    for name in ("diagnostic", "allow_document_fallback"):
        if type(getattr(spec, name)) is not bool:
            raise ValueError(name + " must be a bool")
    if spec.publication_status not in ("published", "staging"):
        raise ValueError("Invalid publication_status")
    if spec.publication_status == "staging" and not spec.diagnostic:
        raise ValueError("staging requires diagnostic=True")
    for name in ("keyword", "knowledge_category", "knowledge_type", "validity_status",
                 "graph_label", "filename", "indicator_number", "indicator_system_version"):
        value = getattr(spec, name)
        if value is not None and (not isinstance(value, str) or not value.strip()):
            raise ValueError(name + " must be a nonempty string")
    for name in _ARRAYS + ("indicator_numbers",):
        value = getattr(spec, name)
        if not isinstance(value, (tuple, list)) or any(
                not isinstance(item, str) or not item.strip() for item in value):
            raise ValueError(name + " must be a string array")
    for name in _ARRAYS + ("indicators",):
        if getattr(spec, name + "_mode") not in ("any", "all"):
            raise ValueError(name + "_mode must be any or all")
    if spec.indicator_number is not None and spec.indicator_numbers:
        raise ValueError("Use indicator_number or indicator_numbers, not both")
    for name in ("page", "page_size"):
        if type(getattr(spec, name)) is not int or getattr(spec, name) < 1:
            raise ValueError(name + " must be a positive integer")
    if spec.page_size > 200:
        raise ValueError("page_size must not exceed 200")
    if spec.sort_by not in _SORTS or spec.sort_direction not in ("asc", "desc"):
        raise ValueError("Invalid sort field or direction")
    if spec.date_field is not None and spec.date_field not in _SOURCE_DATES + _ROW_DATES:
        raise ValueError("Invalid date_field")
    if (spec.date_from is not None or spec.date_to is not None) and spec.date_field is None:
        raise ValueError("date_field is required for date bounds")
    if spec.date_field is not None:
        lo = _bound(spec.date_from, spec.date_field in _SOURCE_DATES) if spec.date_from is not None else None
        hi = _bound(spec.date_to, spec.date_field in _SOURCE_DATES) if spec.date_to is not None else None
        if lo is not None and hi is not None and lo > hi:
            raise ValueError("date_from must not exceed date_to")
    return spec


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _array_expr(expr: str) -> str:
    return "CASE WHEN JSON_TYPE({0}) = 'ARRAY' THEN {0} ELSE JSON_ARRAY() END".format(expr)


def _element(column: str, alias: str) -> str:
    return "JSON_EXTRACT({}, CONCAT('$[', {}.pos - 1, ']'))".format(column, alias)


def _exists(column: str, alias: str, conditions: str) -> str:
    # MySQL 8.3 can decorrelate JSON_TABLE EXISTS into a semijoin that evaluates
    # the table function before its owning row. HAVING keeps it dependent;
    # LIMIT 1 alone does not prevent that rewrite.
    return ("EXISTS (SELECT 1 FROM JSON_TABLE({}, '$[*]' COLUMNS "
            "(pos FOR ORDINALITY)) AS {} WHERE {} HAVING COUNT(*) > 0)").format(
                _array_expr(column), alias, conditions)


def _state_sql(expr: str, allowed: Tuple[str, ...], absent: bool = False) -> str:
    values = ", ".join("'" + value + "'" for value in allowed)
    state = ("CASE JSON_TYPE({0}) WHEN 'STRING' THEN JSON_UNQUOTE({0}) "
             "WHEN 'OBJECT' THEN JSON_UNQUOTE(JSON_EXTRACT({0}, '$.state')) END").format(expr)
    allowed_sql = "{} IN ({})".format(state, values)
    if absent:
        allowed_sql = "({0} IS NULL OR JSON_TYPE({0}) = 'NULL' OR {1})".format(expr, allowed_sql)
    checks = [allowed_sql]
    for flag in _FLAGS:
        checks.append("COALESCE(CAST(JSON_EXTRACT({}, '$.{}') AS CHAR), 'null') "
                      "IN ('null', 'false', '0', '\"\"', '[]', '{{}}')".format(expr, flag))
    return "(" + " AND ".join(checks) + ")"


def _fallback_sql() -> str:
    state = "JSON_EXTRACT(k.field_states, '$.indicators')"
    empty = "JSON_TYPE(k.indicators) = 'ARRAY' AND JSON_LENGTH(k.indicators) = 0"
    missing = "(k.indicators IS NULL OR JSON_TYPE(k.indicators) = 'NULL')"
    safe_empty = _state_sql(state, ("empty", "missing"), absent=True)
    safe_missing = _state_sql(state, ("missing",))
    safe_sources = _state_sql("JSON_EXTRACT(k.field_states, '$.sources')",
                              ("present", "empty", "missing", "not_applicable"), absent=True)
    safe_documents = _state_sql("JSON_EXTRACT(k.field_states, '$.document_indicators')",
                                ("present", "empty", "missing", "not_applicable"), absent=True)
    return "JSON_TYPE(k.field_states) = 'OBJECT' AND ((({0}) AND {1}) OR ({2} AND {3})) AND {4} AND {5}".format(
        empty, safe_empty, missing, safe_missing, safe_sources, safe_documents)


def _indicator_targets(spec: QuerySpec) -> list:
    numbers = (spec.indicator_number,) if spec.indicator_number is not None else spec.indicator_numbers
    targets = []
    for number in dict.fromkeys(numbers):
        item = {"indicator_number": number}
        if spec.indicator_system_version is not None:
            item["indicator_system_version"] = spec.indicator_system_version
        targets.append(item)
    if not targets and spec.indicator_system_version is not None:
        targets.append({"indicator_system_version": spec.indicator_system_version})
    return targets


def _source_conditions(spec: QuerySpec, source: str, params: dict) -> list:
    conditions = ["JSON_TYPE({}) = 'OBJECT'".format(source)]
    for name in ("filename", "graph_label"):
        value = getattr(spec, name)
        if value is not None:
            params["source_" + name] = _json({name: value})
            match = "JSON_CONTAINS({}, CAST(:source_{} AS JSON)) = 1".format(source, name)
            if name == "filename":
                params["source_filename_member"] = _json(value)
                filenames = "JSON_EXTRACT({}, '$.filenames')".format(source)
                member = _element(filenames, "fn")
                # Match only direct string members, never nested arrays or JSON text.
                aliases = _exists(filenames, "fn",
                    "JSON_TYPE({0}) = 'STRING' AND {0} = CAST(:source_filename_member AS JSON)".format(member))
                match = "((JSON_TYPE(JSON_EXTRACT({}, '$.filename')) = 'STRING' AND {}) OR {})".format(
                    source, match, aliases)
            conditions.append(match)
    if spec.date_field in _SOURCE_DATES:
        expr = "JSON_EXTRACT({}, '$.{}')".format(source, spec.date_field)
        conditions.append("JSON_TYPE({}) = 'STRING'".format(expr))
        for name, op in (("date_from", ">="), ("date_to", "<=")):
            if getattr(spec, name) is not None:
                params[name] = _bound(getattr(spec, name), True)
                conditions.append("BINARY JSON_UNQUOTE({}) {} BINARY :{}".format(expr, op, name))
    return conditions


def build_query(spec: QuerySpec) -> tuple:
    """Return ``(count_sql, select_sql, params)`` for the single physical table."""
    validate(spec)
    params = {"dataset_version": spec.dataset_version, "publication_status": spec.publication_status,
              "limit": spec.page_size, "offset": (spec.page - 1) * spec.page_size}
    where = ["BINARY k.dataset_version = BINARY :dataset_version",
             "BINARY k.publication_status = BINARY :publication_status"]
    for name in ("knowledge_category", "knowledge_type", "validity_status"):
        if getattr(spec, name) is not None:
            params[name] = getattr(spec, name)
            where.append("BINARY k.{} = BINARY :{}".format(name, name))
    if spec.keyword is not None:
        params["keyword"] = spec.keyword
        where.append("LOCATE(BINARY :keyword, BINARY k.content) > 0")
    for name in _ARRAYS:
        checks = []
        for index, value in enumerate(dict.fromkeys(getattr(spec, name))):
            key = "{}_{}".format(name, index)
            params[key] = _json(value)
            checks.append("JSON_CONTAINS(k.{}, CAST(:{} AS JSON)) = 1".format(name, key))
        if checks:
            joiner = " OR " if getattr(spec, name + "_mode") == "any" else " AND "
            where.append("(JSON_TYPE(k.{}) = 'ARRAY' AND ({}))".format(name, joiner.join(checks)))
    source = _element("k.sources", "src")
    source_checks = _source_conditions(spec, source, params)
    if spec.filename is not None or spec.graph_label is not None or spec.date_field in _SOURCE_DATES:
        where.append(_exists("k.sources", "src", " AND ".join(source_checks)))
    if spec.date_field in _ROW_DATES:
        for name, op in (("date_from", ">="), ("date_to", "<=")):
            if getattr(spec, name) is not None:
                params[name] = _bound(getattr(spec, name), False)
                where.append("k.{} {} :{}".format(spec.date_field, op, name))
    targets = _indicator_targets(spec)
    if targets:
        formal, document = [], []
        group = _element("k.document_indicators", "dg")
        nested = "JSON_EXTRACT({}, '$.indicators')".format(group)
        group_snapshot = "JSON_EXTRACT({}, '$.source_file_snapshot_key')".format(group)
        source_snapshot = "JSON_EXTRACT({}, '$.source_file_snapshot_key')".format(source)
        # New groups bind a physical file in the frozen graph, not just a logical
        # document. Only absent keys use legacy matching; invalid/null keys fail closed.
        link = source_checks + [
            "JSON_TYPE(JSON_EXTRACT({}, '$.document_key')) = 'STRING'".format(group),
            "JSON_EXTRACT({}, '$.document_key') = JSON_EXTRACT({}, '$.document_key')".format(group, source),
            ("(JSON_CONTAINS_PATH({0}, 'one', '$.source_file_snapshot_key') = 0 OR "
             "(JSON_TYPE({1}) = 'STRING' AND CHAR_LENGTH(JSON_UNQUOTE({1})) > 0 "
             "AND JSON_TYPE({2}) = 'STRING' AND {1} = {2}))").format(
                 group, group_snapshot, source_snapshot),
        ]
        for index, target in enumerate(targets):
            key = "indicator_{}".format(index)
            params[key] = _json(target)
            formal.append("JSON_CONTAINS(k.indicators, CAST(:{} AS JSON)) = 1".format(key))
            document.append(_exists("k.document_indicators", "dg", " AND ".join([
                "JSON_TYPE({}) = 'ARRAY'".format(nested),
                "JSON_CONTAINS({}, CAST(:{} AS JSON)) = 1".format(nested, key),
                "JSON_CONTAINS({}, CAST(:document_present AS JSON)) = 1".format(group),
                _exists("k.sources", "src", " AND ".join(link)),
            ])))
        joiner = " OR " if spec.indicators_mode == "any" else " AND "
        formal_sql = "(JSON_TYPE(k.indicators) = 'ARRAY' AND ({}))".format(joiner.join(formal))
        if spec.allow_document_fallback:
            params["document_present"] = _json({"mapping_state": "present"})
            where.append("({} OR (({}) AND ({})))".format(
                formal_sql, _fallback_sql(), joiner.join(document)))
        else:
            where.append(formal_sql)
    predicate = " AND ".join("(" + condition + ")" for condition in where)
    base = " FROM {} AS k WHERE {}".format(_TABLE, predicate)
    order = "k.{} {}".format(spec.sort_by, spec.sort_direction.upper())
    if spec.sort_by != "knowledge_id":
        order += ", k.knowledge_id ASC"
    return ("SELECT COUNT(*)" + base,
            "SELECT k.*" + base + " ORDER BY " + order + " LIMIT :limit OFFSET :offset", params)


def _state_safe(value: Any, allowed: tuple, absent: bool = False) -> bool:
    if value is None:
        return absent
    if isinstance(value, str):
        return value in allowed
    return (isinstance(value, dict) and value.get("state") in allowed
            and all(_json(value.get(flag)) in ("null", "false", "0", '\"\"', "[]", "{}")
                    for flag in _FLAGS))


def _fallback_allowed(item: dict) -> bool:
    states = item.get("field_states")
    if not isinstance(states, dict):
        return False
    formal = item.get("indicators")
    state = states.get("indicators")
    allowed = ((formal == [] and _state_safe(state, ("empty", "missing"), True))
               or (formal is None and _state_safe(state, ("missing",))))
    return allowed and all(_state_safe(states.get(name),
        ("present", "empty", "missing", "not_applicable"), True)
        for name in ("sources", "document_indicators"))


def _source_matches(source: dict, spec: QuerySpec) -> bool:
    if not isinstance(source, dict):
        return False
    if spec.filename is not None:
        filenames = source.get("filenames")
        if source.get("filename") != spec.filename and not (
                isinstance(filenames, list) and spec.filename in filenames):
            return False
    if spec.graph_label is not None and source.get("graph_label") != spec.graph_label:
        return False
    if spec.date_field in _SOURCE_DATES:
        value = source.get(spec.date_field)
        if not isinstance(value, str):
            return False
        if spec.date_from is not None and value < _bound(spec.date_from, True):
            return False
        if spec.date_to is not None and value > _bound(spec.date_to, True):
            return False
    return True


def _document_source_matches(group: dict, source: dict) -> bool:
    key = group.get("document_key")
    if not isinstance(key, str) or source.get("document_key") != key:
        return False
    # Keep this optional physical-source constraint identical to the SQL branch.
    if "source_file_snapshot_key" not in group:
        return True
    snapshot = group["source_file_snapshot_key"]
    return (isinstance(snapshot, str) and bool(snapshot)
            and source.get("source_file_snapshot_key") == snapshot)


def _matches(indicators: Any, targets: list, mode: str) -> list:
    if not isinstance(indicators, list):
        return []
    matched = [item for item in indicators if isinstance(item, dict) and any(
        all(type(item.get(key)) is type(value) and item.get(key) == value
            for key, value in target.items()) for target in targets)]
    if mode == "all" and not all(any(all(item.get(k) == v for k, v in target.items())
                                   for item in matched) for target in targets):
        return []
    return matched


def _annotate(row: Mapping[str, Any], spec: QuerySpec) -> dict:
    item = dict(row)
    for key in _JSON_FIELDS:
        if isinstance(item.get(key), (str, bytes, bytearray)):
            item[key] = json.loads(item[key])
    sources = item.get("sources") if isinstance(item.get("sources"), list) else []
    eligible = [source for source in sources if _source_matches(source, spec)]
    targets = _indicator_targets(spec)
    item.update(match_mode="unfiltered", matched_indicators=[], source_files=list(eligible))
    if not targets:
        return item
    matched = _matches(item.get("indicators"), targets, spec.indicators_mode)
    if matched:
        item.update(match_mode="formal", matched_indicators=matched)
        return item
    item.update(match_mode="none", source_files=[])
    if not spec.allow_document_fallback or not _fallback_allowed(item):
        return item
    groups = item.get("document_indicators")
    if not isinstance(groups, list):
        return item
    matched, files = [], []
    for group in groups:
        if not isinstance(group, dict) or group.get("mapping_state") != "present":
            continue
        if not any(_document_source_matches(group, source) for source in eligible):
            continue
        hits = _matches(group.get("indicators"), targets, "any")
        if hits:
            matched.extend(hits)
            files.append(group)
    if _matches(matched, targets, spec.indicators_mode):
        item.update(match_mode="document_fallback", matched_indicators=matched, source_files=files)
    return item


def _resolve_engine(engine: Any) -> Any:
    try:
        repository = import_module(__package__ + ".repository")
    except ModuleNotFoundError as exc:
        if exc.name != __package__ + ".repository":
            raise
        if engine is not None:
            return engine
        return import_module("app.db.base").engine
    return repository.KnowledgeRepository(engine).engine


def query_knowledge(spec: QuerySpec, engine: Any = None) -> dict:
    """Run two SELECTs; injected engines never require project DB configuration.

    Match modes are ``unfiltered``, ``formal`` and ``document_fallback``.
    Fallback ``source_files`` contains the actual matching document groups;
    otherwise it contains source objects satisfying the source filters.
    """
    count_sql, select_sql, params = build_query(spec)
    from sqlalchemy import text

    engine = _resolve_engine(engine)
    with engine.connect() as connection:
        with connection.begin():
            total = connection.execute(text(count_sql), params).scalar()
            rows = connection.execute(text(select_sql), params).mappings().all()
    return {"total": int(total), "items": [_annotate(row, spec) for row in rows]}


__all__ = ["QuerySpec", "validate", "build_query", "query_knowledge"]
