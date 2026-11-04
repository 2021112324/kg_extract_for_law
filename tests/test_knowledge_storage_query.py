"""Offline semantics/compilation plus opt-in isolated MySQL regressions."""

from __future__ import annotations

import copy
import json
import os
import subprocess
import sys
import uuid
from dataclasses import replace
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.infrastructure.kg_to_natural_language.storage import query
from app.infrastructure.kg_to_natural_language.storage.query import (
    QuerySpec, build_query, query_knowledge, validate,
)


def _indicator(number="1.2.3", version="v1"):
    return {"indicator_number": number, "indicator_system_version": version,
            "indicator_name": "example"}


def _source(key="a", **values):
    return {"document_key": key, "filename": key + ".txt", "graph_label": "law",
            "publication_date": "2026-01-02", "effective_date": "2026-02-03", **values}


def _group(key="a", indicators=None, **values):
    return {"document_key": key, "filename": key + ".txt", "mapping_state": "present",
            "indicators": [_indicator()] if indicators is None else indicators, **values}


def _row(**values):
    return {"knowledge_id": "k1", "dataset_version": "release-1", "indicators": [],
            "document_indicators": [_group()], "sources": [_source()],
            "field_states": {"indicators": {"state": "empty"}, "sources": {"state": "present"}},
            **values}


def _spec(**values):
    return QuerySpec(dataset_version="release-1", **values)


@pytest.mark.parametrize("values", [
    {"dataset_version": ""}, {"dataset_version": " "}, {"dataset_version": None},
    {"dataset_version": "x" * 97}, {"page": 0}, {"page": True}, {"page_size": 201},
    {"page_size": 0}, {"page_size": 1.5}, {"publication_status": "all"},
    {"publication_status": "staging"}, {"diagnostic": "true"},
    {"allow_document_fallback": 1}, {"sort_by": "content; DROP TABLE x"},
    {"sort_direction": "DESC; --"}, {"date_field": "sources.$[*]"},
    {"date_from": "2026-01-01"}, {"date_field": "publication_date", "date_from": "bad"},
    {"date_field": "publication_date", "date_from": "2026-02-30"},
    {"date_field": "publication_date", "date_from": datetime(2026, 1, 1)},
    {"date_field": "created_at", "date_from": "2026-02-01", "date_to": "2026-01-01"},
    {"risk_types": "risk"}, {"tags": [1]}, {"tags_mode": "ANY"},
    {"indicators_mode": "some"}, {"keyword": ""}, {"filename": {}},
    {"indicator_number": "1.2", "indicator_numbers": ("1.3",)},
])
def test_invalid_input_is_rejected_before_any_engine_resolution(values, monkeypatch):
    monkeypatch.setattr(query, "_resolve_engine", lambda engine: pytest.fail("engine was resolved"))
    with pytest.raises((ValueError, TypeError)):
        query_knowledge(QuerySpec(**{"dataset_version": "v", **values}))


def test_version_is_required_and_validate_returns_spec():
    with pytest.raises(TypeError):
        QuerySpec()
    spec = _spec()
    assert validate(spec) is spec
    assert spec.validate() is spec
    with pytest.raises(TypeError):
        validate({"dataset_version": "v"})


def test_default_query_is_single_table_published_and_stable():
    count, select, params = build_query(_spec(page=3, page_size=200))
    assert count.startswith("SELECT COUNT(*) FROM t_natural_knowledge_entry AS k")
    assert "BINARY k.dataset_version = BINARY :dataset_version" in count
    assert "BINARY k.publication_status = BINARY :publication_status" in count
    assert "JOIN" not in select
    assert "ORDER BY k.knowledge_id ASC LIMIT :limit OFFSET :offset" in select
    assert params == {"dataset_version": "release-1", "publication_status": "published",
                      "limit": 200, "offset": 400}
    _, select, _ = build_query(_spec(sort_by="created_at", sort_direction="desc"))
    assert "ORDER BY k.created_at DESC, k.knowledge_id ASC" in select


def test_staging_is_explicit_even_in_diagnostic_mode():
    assert build_query(_spec(diagnostic=True))[2]["publication_status"] == "published"
    assert build_query(_spec(publication_status="staging", diagnostic=True))[2]["publication_status"] == "staging"


def test_all_user_values_are_bound_and_keywords_have_literal_contains_semantics():
    payload = "%' OR 1=1; --_\\"
    spec = _spec(keyword=payload, filename=payload, graph_label=payload,
                 knowledge_category=payload, knowledge_type=payload, validity_status=payload,
                 tags=(payload,), indicator_number=payload, indicator_system_version=payload,
                 allow_document_fallback=True)
    count, select, params = build_query(spec)
    assert payload not in count and payload not in select
    assert "LOCATE(BINARY :keyword, BINARY k.content) > 0" in select
    assert "LIKE" not in select
    assert params["keyword"] == payload
    assert json.loads(params["source_filename"]) == {"filename": payload}
    assert json.loads(params["source_filename_member"]) == payload
    assert json.loads(params["indicator_0"]) == {
        "indicator_number": payload, "indicator_system_version": payload}


@pytest.mark.parametrize("field", ["risk_types", "economic_industries", "tags", "semantic_types"])
@pytest.mark.parametrize("mode,joiner", [("any", " OR "), ("all", " AND ")])
def test_array_modes_use_structural_json_and_deduplicate_parameters(field, mode, joiner):
    count, _, params = build_query(_spec(**{field: ("a", "b", "a"), field + "_mode": mode}))
    expression = ("JSON_CONTAINS(k.{0}, CAST(:{0}_0 AS JSON)) = 1{1}"
                  "JSON_CONTAINS(k.{0}, CAST(:{0}_1 AS JSON)) = 1").format(field, joiner)
    assert expression in count
    assert json.loads(params[field + "_0"]) == "a"
    assert field + "_2" not in params


def test_filename_graph_and_date_share_one_source_exists():
    count, _, params = build_query(_spec(filename="a.txt", graph_label="law",
        date_field="publication_date", date_from=date(2026, 1, 1), date_to="2026-02-01"))
    assert count.count("JSON_TABLE(") == 2
    assert "$.publication_date" in count
    assert ":source_filename" in count and ":source_graph_label" in count
    assert ":date_from" in count and ":date_to" in count
    assert params["date_from"] == "2026-01-01"
    assert not query._source_matches(_source("a", publication_date="2025-01-01"),
        _spec(filename="a.txt", date_field="publication_date", date_from="2026-01-01"))
    assert not query._source_matches(_source("b"), _spec(filename="a.txt"))


@pytest.mark.parametrize("field", ["publication_date", "effective_date"])
@pytest.mark.parametrize("fallback", [False, True])
def test_json_table_correlation_is_protected_from_mysql83_semijoin(field, fallback):
    count, select, params = build_query(_spec(filename="b.txt", graph_label="law",
        date_field=field, date_from="2024-01-01", date_to="2025-12-31",
        indicator_number="1.2.3" if fallback else None, allow_document_fallback=fallback))
    # An ordinary EXISTS, including LIMIT 1, is still semijoin-eligible on 8.3.
    # The aggregate HAVING is a semantic barrier, not an ignorable optimizer hint.
    assert count.count("JSON_TABLE(") == count.count("HAVING COUNT(*) > 0")
    assert count.count("JSON_TABLE(") == (5 if fallback else 2)
    assert params["date_from"] == "2024-01-01"
    assert params["date_to"] == "2025-12-31"
    assert count.split(" WHERE ", 1)[1] == select.split(" WHERE ", 1)[1].split(" ORDER BY ")[0]
    spec = _spec(filename="b.txt", graph_label="law", date_field=field,
                 date_from="2024-01-01", date_to="2025-12-31")
    assert not query._source_matches(_source("a", **{field: "2025-01-01"}), spec)
    assert not query._source_matches(_source("b", **{field: "2020-01-01"}), spec)
    assert query._source_matches(_source("b", **{field: "2025-01-01"}), spec)


def test_timestamp_bounds_are_local_and_source_dates_are_not_used():
    count, _, params = build_query(_spec(date_field="generated_at",
        date_from=datetime(2026, 1, 1, tzinfo=timezone.utc), date_to="2026-01-02T00:00:00"))
    assert params["date_from"] == datetime(2026, 1, 1, 8)
    assert "k.generated_at >= :date_from" in count
    assert "JSON_TABLE" not in count


def test_indicator_number_and_version_are_one_json_object_not_independent_exists():
    count, _, params = build_query(_spec(indicator_number="1.2.3", indicator_system_version="v1"))
    assert "JSON_CONTAINS(k.indicators, CAST(:indicator_0 AS JSON)) = 1" in count
    assert json.loads(params["indicator_0"]) == {
        "indicator_number": "1.2.3", "indicator_system_version": "v1"}
    assert "document_indicators" not in count
    assert not query._matches([_indicator("1.2.3", "v2"), _indicator("4.5.6", "v1")],
                             query._indicator_targets(_spec(indicator_number="1.2.3",
                                 indicator_system_version="v1")), "any")
    assert not query._matches([_indicator(version=None)],
        [{"indicator_number": "1.2.3", "indicator_system_version": "v1"}], "any")


def test_unknown_version_can_match_number_only_and_version_only_is_supported():
    assert query._matches([_indicator(version=None)], [{"indicator_number": "1.2.3"}], "any")
    _, _, params = build_query(_spec(indicator_system_version="v1"))
    assert json.loads(params["indicator_0"]) == {"indicator_system_version": "v1"}


def test_fallback_sql_is_explicit_guarded_and_correlated_to_matching_sources():
    count, select, params = build_query(_spec(indicator_number="1.2.3", allow_document_fallback=True,
        filename="a.txt", graph_label="law", date_field="effective_date", date_from="2026-01-01"))
    assert "JSON_TYPE(k.field_states) = 'OBJECT'" in count
    assert "JSON_LENGTH(k.indicators) = 0" in count
    assert "$.indicators" in count and "$.sources" in count
    assert "$.state" in count
    assert "JSON_EXTRACT(k.document_indicators, CONCAT('$[', dg.pos - 1, ']'))" in count
    assert "$.document_key') = JSON_EXTRACT(" in count
    assert json.loads(params["document_present"]) == {"mapping_state": "present"}
    assert count.count(":source_filename AS JSON") == 2
    assert count.count(":source_filename_member AS JSON") == 2
    assert count.count(":date_from") == 2
    assert " JOIN " not in count and "DISTINCT" not in count
    assert count.split(" WHERE ", 1)[1] == select.split(" WHERE ", 1)[1].split(" ORDER BY ")[0]


@pytest.mark.parametrize("formal,state,allowed", [
    ([], {"state": "empty"}, True), ([], None, True), ([], "empty", True),
    (None, {"state": "missing"}, True), (None, "missing", True),
    (None, None, False), (None, {}, False), (None, {"state": "empty"}, False),
    (None, {"state": "invalid"}, False), (None, {"state": "conflict"}, False),
    (None, {"state": "not_applicable"}, False), (None, "unknown", False),
    ([], {"state": "invalid"}, False), ([], {"state": "present"}, False),
    ([], {"state": "empty", "errors": ["bad"]}, False),
    (None, {"state": "missing", "ambiguous": True}, False),
    ([_indicator("9.9.9")], {"state": "present", "coverage": "partial"}, False),
    ("[]", {"state": "empty"}, False), ({}, {"state": "missing"}, False),
])
def test_fallback_eligibility_never_defaults_unknown_or_invalid_to_missing(formal, state, allowed):
    assert query._fallback_allowed(_row(indicators=formal,
        field_states={"indicators": state, "sources": {"state": "present"}})) is allowed


@pytest.mark.parametrize("states", [None, "missing", [],
    {"indicators": {"state": "missing"}, "sources": {"state": "conflict"}},
    {"indicators": {"state": "missing"}, "document_indicators": {"state": "invalid"}},
    {"indicators": {"state": "missing"}, "sources": {"state": "present", "errors": ["x"]}},
])
def test_malformed_states_and_source_ambiguity_block_fallback(states):
    assert not query._fallback_allowed(_row(indicators=None, field_states=states))


def test_formal_hit_wins_and_annotation_does_not_change_inputs():
    row = _row(indicators=[_indicator()], field_states={"indicators": {"state": "present"}})
    original = copy.deepcopy(row)
    result = query._annotate(row, _spec(indicator_number="1.2.3", allow_document_fallback=True))
    assert result["match_mode"] == "formal"
    assert result["matched_indicators"] == [_indicator()]
    assert result["source_files"] == row["sources"]
    assert row == original


def test_nonmatching_formal_partial_coverage_cannot_fall_back():
    result = query._annotate(_row(indicators=[_indicator("9.9.9")],
        field_states={"indicators": {"state": "present", "coverage": "partial"}}),
        _spec(indicator_number="1.2.3", allow_document_fallback=True))
    assert result["match_mode"] == "none"
    assert result["matched_indicators"] == []


def test_fallback_returns_exact_matching_file_and_nested_indicators():
    row = _row(sources=[_source("a"), _source("b")], document_indicators=[
        _group("a", [_indicator(), _indicator("9.9.9")]), _group("b")])
    spec = _spec(indicator_number="1.2.3", indicator_system_version="v1", filename="a.txt",
                 allow_document_fallback=True)
    original = copy.deepcopy(row)
    result = query._annotate(row, spec)
    assert result["match_mode"] == "document_fallback"
    assert result["matched_indicators"] == [_indicator()]
    assert [group["document_key"] for group in result["source_files"]] == ["a"]
    assert result["indicators"] == []
    assert row == original
    assert query._annotate(row, replace(spec, allow_document_fallback=False))["match_mode"] == "none"


@pytest.mark.parametrize("group", [
    _group(mapping_state="invalid"), _group(mapping_state="missing"),
    _group(mapping_state="empty"), _group("other"), _group(indicators=[]),
    _group(indicators=[_indicator(version=None)]),
    _group(indicators=[_indicator(version="v2"), _indicator("9.9.9", "v1")]),
    {"document_key": "a", "indicator_number": "1.2.3", "mapping_state": "present"},
])
def test_invalid_unlinked_or_cross_version_document_groups_do_not_match(group):
    result = query._annotate(_row(document_indicators=[group]), _spec(
        indicator_number="1.2.3", indicator_system_version="v1", allow_document_fallback=True))
    assert result["match_mode"] == "none"


def test_file_b_indicator_cannot_satisfy_file_a_filter_or_cross_source_dates():
    row = _row(sources=[_source("a", publication_date="2025-01-01"), _source("b")],
               document_indicators=[_group("a", []), _group("b")])
    for spec in [
        _spec(filename="a.txt", indicator_number="1.2.3", allow_document_fallback=True),
        _spec(filename="a.txt", date_field="publication_date", date_from="2026-01-01",
              indicator_number="1.2.3", allow_document_fallback=True),
    ]:
        assert query._annotate(row, spec)["match_mode"] == "none"


@pytest.mark.parametrize("source_values,filename,expected", [
    ({"filename": "main.txt", "filenames": ["main.txt", "alias.txt"]}, "main.txt", True),
    ({"filename": "main.txt", "filenames": ["main.txt", "alias.txt"]}, "alias.txt", True),
    ({"filename": "main.txt"}, "main.txt", True),
    ({"filename": "main.txt", "filenames": None}, "main.txt", True),
    ({"filename": None, "filenames": ["alias.txt"]}, "alias.txt", True),
    ({"filename": "main.txt", "filenames": ["alias.txt"]}, "ALIAS.txt", False),
    ({"filename": "main.txt", "filenames": ["alias.txt"]}, "alias", False),
    ({"filename": "main.txt", "filenames": ["alias.txt"]}, "alias.txt ", False),
    ({"filename": "main.txt", "filenames": "alias.txt"}, "alias.txt", False),
    ({"filename": "main.txt", "filenames": '["alias.txt"]'}, "alias.txt", False),
    ({"filename": "main.txt", "filenames": {"name": "alias.txt"}}, "alias.txt", False),
    ({"filename": "main.txt", "filenames": [["alias.txt"]]}, "alias.txt", False),
    ({"filename": ["alias.txt"], "filenames": []}, "alias.txt", False),
    ({"filename": "main.txt", "filenames": [1, False, None]}, "1", False),
    ({"filename": "main.txt", "filenames": ["%' OR 1=1 --_\\"]}, "%' OR 1=1 --_\\", True),
])
def test_filename_matches_primary_or_exact_direct_alias(source_values, filename, expected):
    assert query._source_matches(_source(**source_values), _spec(filename=filename)) is expected


def test_filename_alias_sql_stays_in_same_source_and_uses_bound_json_strings():
    spec = _spec(filename="alias.txt", graph_label="law", date_field="publication_date",
                 date_from="2024-01-01", indicator_number="1.2.3", allow_document_fallback=True)
    count, select, params = build_query(spec)
    filenames = "JSON_EXTRACT({}, '$.filenames')".format(query._element("k.sources", "src"))
    member = query._element(filenames, "fn")
    assert "JSON_TYPE({}) = 'STRING'".format(member) in count
    assert "{} = CAST(:source_filename_member AS JSON)".format(member) in count
    assert "CASE WHEN JSON_TYPE({0}) = 'ARRAY' THEN {0} ELSE JSON_ARRAY() END".format(filenames) in count
    assert " OR EXISTS (SELECT 1 FROM JSON_TABLE(" in count
    assert params["source_filename_member"] == '"alias.txt"'
    assert "alias.txt" not in count
    assert "LIKE" not in count
    assert count.count("JSON_TABLE(") == count.count("HAVING COUNT(*) > 0")
    assert count.split(" WHERE ", 1)[1] == select.split(" WHERE ", 1)[1].split(" ORDER BY ")[0]


def test_filename_alias_annotations_keep_dates_graph_and_physical_source_binding():
    early = _source("a", filenames=["a.txt", "alias.txt"], publication_date="2020-01-01",
                    source_file_snapshot_key="file-a")
    late = _source("a", filenames=["a.txt", "alias.txt"], publication_date="2025-01-01",
                   source_file_snapshot_key="file-b")
    groups = [_group("a", source_file_snapshot_key="file-a"),
              _group("a", source_file_snapshot_key="file-b")]
    row = _row(sources=[early, late], document_indicators=groups)
    spec = _spec(filename="alias.txt", graph_label="law", date_field="publication_date",
                 date_from="2024-01-01", indicator_number="1.2.3", allow_document_fallback=True)
    result = query._annotate(row, spec)
    assert result["match_mode"] == "document_fallback"
    assert result["source_files"] == [groups[1]]
    assert result["matched_indicators"] == [_indicator()]
    assert query._annotate(row, replace(spec, indicator_number=None))["source_files"] == [late]
    row["document_indicators"] = [groups[0]]
    assert query._annotate(row, spec)["match_mode"] == "none"
    late["filenames"] = ["a.txt"]
    assert query._annotate(row, replace(spec, indicator_number=None))["source_files"] == []
    late["filenames"] = ["a.txt", "alias.txt"]
    late["graph_label"] = "other"
    assert query._annotate(row, replace(spec, indicator_number=None))["source_files"] == []


def test_same_document_snapshot_groups_do_not_borrow_other_source_dates():
    early = _source(source_file_snapshot_key="file-a", publication_date="2020-01-01")
    late = _source(source_file_snapshot_key="file-b", publication_date="2025-01-01")
    groups = [_group(source_file_snapshot_key="file-a"), _group(source_file_snapshot_key="file-b")]
    spec = _spec(filename="a.txt", date_field="publication_date", date_from="2024-01-01",
                 indicator_number="1.2.3", allow_document_fallback=True)
    row = _row(sources=[early, late], document_indicators=groups)
    result = query._annotate(row, spec)
    assert result["match_mode"] == "document_fallback"
    assert result["source_files"] == [groups[1]]
    assert result["matched_indicators"] == [_indicator()]
    row["document_indicators"] = [groups[0]]
    assert query._annotate(row, spec)["match_mode"] == "none"
    # Legacy groups have only the logical key and retain their original semantics.
    row["document_indicators"] = [_group()]
    assert query._annotate(row, spec)["match_mode"] == "document_fallback"


@pytest.mark.parametrize("group_value,source_values,expected", [
    ("file-a", {"source_file_snapshot_key": "file-a"}, "document_fallback"),
    ("file-a", {"source_file_snapshot_key": "FILE-A"}, "none"),
    ("file-a", {"source_file_snapshot_key": "file-a "}, "none"),
    ("file-a", {"source_file_snapshot_key": "file-b"}, "none"),
    ("file-a", {}, "none"), ("file-a", {"source_file_snapshot_key": None}, "none"),
    (None, {}, "none"), ("", {"source_file_snapshot_key": ""}, "none"),
    (7, {"source_file_snapshot_key": 7}, "none"),
    (["file-a"], {"source_file_snapshot_key": ["file-a"]}, "none"),
])
def test_snapshot_link_is_exact_and_provided_invalid_keys_fail_closed(group_value, source_values, expected):
    row = _row(sources=[_source(**source_values)],
               document_indicators=[_group(source_file_snapshot_key=group_value)])
    result = query._annotate(row, _spec(indicator_number="1.2.3", allow_document_fallback=True))
    assert result["match_mode"] == expected


def test_snapshot_link_is_in_sql_with_fixed_json_paths():
    count, _, _ = build_query(_spec(indicator_number="1.2.3", allow_document_fallback=True))
    group = query._element("k.document_indicators", "dg")
    source = query._element("k.sources", "src")
    assert "JSON_CONTAINS_PATH({}, 'one', '$.source_file_snapshot_key') = 0".format(group) in count
    assert "JSON_EXTRACT({}, '$.source_file_snapshot_key') = JSON_EXTRACT({}, '$.source_file_snapshot_key')".format(
        group, source) in count


@pytest.fixture(scope="module")
def isolated_query_mysql_engine():
    path = os.environ.get("KNOWLEDGE_TEST_CREDENTIALS")
    if not path:
        pytest.skip("Set KNOWLEDGE_TEST_CREDENTIALS for isolated local MySQL regressions")
    config = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    if config.get("host") not in {"localhost", "127.0.0.1", "::1"}:
        pytest.fail("Remote MySQL integration tests are forbidden")
    from sqlalchemy import create_engine, text
    from sqlalchemy.engine import URL

    name = "risk_knowledge_test_" + uuid.uuid4().hex
    url = URL.create("mysql+pymysql", username=config["username"], password=config["password"],
                     host=config["host"], port=int(config["port"]), query={"charset": "utf8mb4"})
    admin = create_engine(url, hide_parameters=True, pool_pre_ping=True)
    engine = None
    try:
        with admin.begin() as connection:
            connection.execute(text("CREATE DATABASE `" + name + "` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin"))
        engine = create_engine(url.set(database=name), hide_parameters=True, pool_pre_ping=True)
        yield engine
    finally:
        if engine is not None:
            engine.dispose()
        with admin.begin() as connection:
            connection.execute(text("DROP DATABASE IF EXISTS `" + name + "`"))
        admin.dispose()


@pytest.mark.parametrize("date_field", ["publication_date", "effective_date"])
def test_real_same_document_snapshot_date_groups(isolated_query_mysql_engine, tmp_path, date_field):
    from app.infrastructure.kg_to_natural_language.storage.records import record_digest
    from app.infrastructure.kg_to_natural_language.storage.repository import KnowledgeRepository
    from tests.test_knowledge_storage_records import enriched

    engine = isolated_query_mysql_engine
    repo = KnowledgeRepository(engine)
    repo.initialize()
    base = enriched(tmp_path, "empty")
    first = base["sources"][0]
    first.update(source_file_snapshot_key="file-a", **{date_field: "2020-01-01"})
    second = copy.deepcopy(first)
    second.update(source_file_snapshot_key="file-b", **{date_field: "2025-01-01"})
    base["sources"].append(second)
    group_a = base["document_indicators"][0]
    group_a["source_file_snapshot_key"] = "file-a"
    group_b = copy.deepcopy(group_a)
    group_b["source_file_snapshot_key"] = "file-b"
    for mode, groups, expected_total in [
        ("both", [group_a, group_b], 1), ("early_only", [group_a], 0),
        ("legacy", [{k: v for k, v in group_a.items() if k != "source_file_snapshot_key"}], 1),
    ]:
        row = copy.deepcopy(base)
        row["dataset_version"] = "dategroups_" + uuid.uuid4().hex
        row["document_indicators"] = copy.deepcopy(groups)
        row["record_hash"] = record_digest(row)
        with repo.lock(row["dataset_version"]):
            repo.write_batch([row])
            repo.publish(row["dataset_version"], [row])
        spec = QuerySpec(row["dataset_version"], filename=first["filename"], date_field=date_field,
                         date_from="2024-01-01", date_to="2025-12-31", indicator_number="3.1.3",
                         allow_document_fallback=True, page_size=1)
        result = query_knowledge(spec, engine)
        assert result["total"] == expected_total, mode
        assert len(result["items"]) == expected_total, mode
        if expected_total:
            item = result["items"][0]
            assert item["match_mode"] == "document_fallback"
            assert len(item["matched_indicators"]) == 1
            assert len(item["source_files"]) == 1
            if mode == "both":
                assert item["source_files"][0]["source_file_snapshot_key"] == "file-b"
        assert query_knowledge(replace(spec, page=2), engine)["items"] == []


@pytest.mark.parametrize("mode,expected", [("any", "document_fallback"), ("all", "none")])
def test_multiple_indicator_any_all(mode, expected):
    spec = _spec(indicator_numbers=("1.2.3", "4.5.6"), indicators_mode=mode,
                 allow_document_fallback=True)
    assert query._annotate(_row(), spec)["match_mode"] == expected
    count, _, params = build_query(spec)
    assert len([key for key in params if key.startswith("indicator_")]) == 2
    assert (" OR " if mode == "any" else " AND ") in count


def test_all_document_indicators_can_span_files_without_duplicating_knowledge():
    row = _row(sources=[_source("a"), _source("b")],
               document_indicators=[_group("a"), _group("b", [_indicator("4.5.6")])])
    result = query._annotate(row, _spec(indicator_numbers=("1.2.3", "4.5.6"),
        indicators_mode="all", allow_document_fallback=True))
    assert result["match_mode"] == "document_fallback"
    assert len(result["source_files"]) == 2


def test_mysql_dialect_compilation_has_exact_bind_sets_without_literal_values():
    from sqlalchemy import text
    from sqlalchemy.dialects.mysql import dialect

    specs = [_spec(), _spec(indicator_number="1.2.3"),
        _spec(filename="secret-filename", indicator_numbers=("1.2.3", "4.5.6"),
              indicators_mode="all", allow_document_fallback=True, tags=("private-tag",),
              date_field="effective_date", date_from="2026-01-01")]
    for spec in specs:
        count, select, params = build_query(spec)
        for sql in (count, select):
            compiled = text(sql).compile(dialect=dialect())
            assert set(compiled.params) <= set(params)
            assert "secret-filename" not in str(compiled)
            assert "private-tag" not in str(compiled)
            assert ":indicator_0" not in str(compiled)


class _Connection:
    def __init__(self, total, rows):
        self.calls = []
        self.total = total
        self.rows = rows

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def begin(self):
        return self

    def execute(self, statement, params):
        self.calls.append((str(statement), params.copy()))
        if str(statement).startswith("SELECT COUNT(*)"):
            return SimpleNamespace(scalar=lambda: self.total)
        return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: self.rows))


def test_query_executes_only_two_selects_and_preserves_sql_total(monkeypatch):
    row = _row()
    encoded = {key: json.dumps(value) if key in query._JSON_FIELDS else value
               for key, value in row.items()}
    connection = _Connection(99, [encoded])
    engine = SimpleNamespace(connect=lambda: connection)
    resolutions = []
    monkeypatch.setattr(query, "_resolve_engine", lambda value: resolutions.append(value) or value)
    result = query_knowledge(_spec(indicator_number="1.2.3", allow_document_fallback=True), engine)
    assert resolutions == [engine]
    assert result["total"] == 99 and len(result["items"]) == 1
    assert result["items"][0]["match_mode"] == "document_fallback"
    assert len(connection.calls) == 2
    assert all(sql.startswith("SELECT ") for sql, _ in connection.calls)


def test_repository_engine_is_reused_without_importing_default_db(monkeypatch):
    supplied, resolved = object(), object()
    calls = []

    class Repository:
        def __init__(self, engine):
            calls.append(engine)
            self.engine = resolved

    def import_fake(name):
        assert name == query.__package__ + ".repository"
        return SimpleNamespace(KnowledgeRepository=Repository)

    monkeypatch.setattr(query, "import_module", import_fake)
    assert query._resolve_engine(supplied) is resolved
    assert calls == [supplied]


def test_default_engine_is_only_resolved_when_needed_without_repository(monkeypatch):
    resolved, calls = object(), []

    def missing_repository(name):
        calls.append(name)
        if name.endswith(".repository"):
            raise ModuleNotFoundError(name, name=name)
        assert name == "app.db.base"
        return SimpleNamespace(engine=resolved)

    monkeypatch.setattr(query, "import_module", missing_repository)
    assert query._resolve_engine(None) is resolved
    assert calls == [query.__package__ + ".repository", "app.db.base"]


@pytest.mark.parametrize("bad", ["missing", {"state": "missing", "errors": "false"},
                                  {"state": "missing", "error": "0"}])
def test_double_encoded_or_error_states_are_not_treated_as_safe_missing(bad):
    state = json.dumps({"state": bad}) if isinstance(bad, str) else bad
    row = _row(indicators=None, field_states={"indicators": state})
    assert query._annotate(row, _spec(indicator_number="1.2.3",
        allow_document_fallback=True))["match_mode"] == "none"


def test_wrong_json_shapes_cannot_create_file_matches():
    spec = _spec(indicator_number="1.2.3", allow_document_fallback=True)
    for row in [
        _row(sources=[json.dumps(_source())]),
        _row(document_indicators=[json.dumps(_group())]),
        _row(document_indicators=[_group(indicators=[json.dumps(_indicator())])]),
        _row(indicators={}, field_states={"indicators": {"state": "missing"}}),
    ]:
        assert query._annotate(row, spec)["match_mode"] == "none"
    count, _, _ = build_query(spec)
    assert "CASE WHEN JSON_TYPE(k.sources) = 'ARRAY'" in count
    assert "CASE WHEN JSON_TYPE(k.document_indicators) = 'ARRAY'" in count


def test_all_mode_requires_all_formal_targets_without_mixing_document_hits():
    row = _row(indicators=[_indicator()],
               field_states={"indicators": {"state": "present"}},
               document_indicators=[_group(indicators=[_indicator("4.5.6")])])
    spec = _spec(indicator_numbers=("1.2.3", "4.5.6"), indicators_mode="all",
                 allow_document_fallback=True)
    assert query._annotate(row, spec)["match_mode"] == "none"
    count, _, _ = build_query(spec)
    assert ("JSON_CONTAINS(k.indicators, CAST(:indicator_0 AS JSON)) = 1 AND "
            "JSON_CONTAINS(k.indicators, CAST(:indicator_1 AS JSON)) = 1") in count


def test_injected_engine_works_before_repository_exists(monkeypatch):
    engine = object()

    def missing(name):
        raise ModuleNotFoundError(name, name=name)

    monkeypatch.setattr(query, "import_module", missing)
    assert query._resolve_engine(engine) is engine


def test_repository_internal_import_errors_are_not_hidden(monkeypatch):
    def missing(name):
        raise ModuleNotFoundError("missing dependency", name="internal_dependency")

    monkeypatch.setattr(query, "import_module", missing)
    with pytest.raises(ModuleNotFoundError, match="missing dependency"):
        query._resolve_engine(object())


def test_import_and_build_work_with_sqlalchemy_and_db_imports_forbidden():
    path = Path(query.__file__).resolve()
    script = r'''
import builtins, importlib.util, sys
original = builtins.__import__
def guarded(name, *args, **kwargs):
    if name.startswith('sqlalchemy') or name.startswith('app.db') or name.endswith('repository'):
        raise AssertionError('eager database import: ' + name)
    return original(name, *args, **kwargs)
builtins.__import__ = guarded
spec = importlib.util.spec_from_file_location('isolated_query', sys.argv[1])
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)
module.build_query(module.QuerySpec(dataset_version='v', indicator_number='1.2.3',
                                   allow_document_fallback=True))
'''
    result = subprocess.run([sys.executable, "-c", script, str(path)],
                            capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
