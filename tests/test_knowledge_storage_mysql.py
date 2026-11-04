"""Real MySQL JSON tests, opt-in and restricted to a freshly created local DB."""

import copy
import json
import os
import uuid
from pathlib import Path

import pytest

from app.infrastructure.kg_to_natural_language.storage.pipeline import persist_knowledge
from app.infrastructure.kg_to_natural_language.storage.query import QuerySpec, query_knowledge
from app.infrastructure.kg_to_natural_language.storage.records import KnowledgeStorageError, record_digest
from tests.test_knowledge_storage_records import enriched, prepared


@pytest.fixture(scope="module")
def mysql_engine():
    path = os.environ.get("KNOWLEDGE_TEST_CREDENTIALS")
    if not path:
        pytest.skip("Set KNOWLEDGE_TEST_CREDENTIALS to explicit isolated local test credentials")
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


@pytest.fixture
def repo(mysql_engine):
    from app.infrastructure.kg_to_natural_language.storage.repository import KnowledgeRepository
    repository = KnowledgeRepository(mysql_engine)
    repository.initialize()
    return repository


def set_version(row, version):
    row = copy.deepcopy(row)
    row["dataset_version"] = version
    row["record_hash"] = record_digest(row)
    return row


def test_real_single_table_and_native_json(repo, mysql_engine, tmp_path):
    from sqlalchemy import inspect, text
    assert inspect(mysql_engine).get_table_names() == ["t_natural_knowledge_entry"]
    version = "json_" + uuid.uuid4().hex
    row = set_version(enriched(tmp_path), version)
    with repo.lock(version):
        repo.write_batch([row])
        with mysql_engine.connect() as connection:
            types = connection.execute(text("SELECT JSON_TYPE(indicators),JSON_TYPE(document_indicators),JSON_TYPE(sources) "
                                            "FROM t_natural_knowledge_entry WHERE dataset_version=:v"), {"v": version}).one()
            assert tuple(types) == ("ARRAY", "ARRAY", "ARRAY")
        assert query_knowledge(QuerySpec(version), mysql_engine)["total"] == 0
        repo.publish(version, [row])
    got = query_knowledge(QuerySpec(version), mysql_engine)
    assert got["total"] == 1
    assert isinstance(got["items"][0]["indicators"][0], dict)


def test_real_query_formal_and_fallback(repo, mysql_engine, tmp_path):
    version = "query_" + uuid.uuid4().hex
    rows = [set_version(enriched(tmp_path, state), version) for state in ("present", "empty", "invalid", "missing")]
    with repo.lock(version):
        repo.write_batch(rows)
        repo.publish(version, rows)
    assert query_knowledge(QuerySpec(version, indicator_number="3.1.2"), mysql_engine)["total"] == 1
    assert query_knowledge(QuerySpec(version, indicator_number="3.1.3"), mysql_engine)["total"] == 0
    fallback = query_knowledge(QuerySpec(version, indicator_number="3.1.3", allow_document_fallback=True), mysql_engine)
    assert fallback["total"] == 2  # Valid empty and legitimately missing, never invalid or existing formal.
    assert all(r["match_mode"] == "document_fallback" for r in fallback["items"])
    assert all(r["source_files"][0]["filename"] == "示例法规.txt" for r in fallback["items"])
    assert query_knowledge(QuerySpec(version, risk_types=["产品法律风险"], economic_industries=["制造业"]), mysql_engine)["total"] == 4
    assert query_knowledge(QuerySpec(version, keyword="' OR 1=1 --"), mysql_engine)["total"] == 0
    assert query_knowledge(QuerySpec(version, indicator_number="3.1.2", indicator_system_version="unknown-version"), mysql_engine)["total"] == 0


def test_real_same_source_conditions_and_pagination(repo, mysql_engine, tmp_path):
    version = "source_" + uuid.uuid4().hex
    row = set_version(enriched(tmp_path), version)
    row["sources"][0]["publication_date"] = "2020-01-01"
    other = copy.deepcopy(row["sources"][0])
    other.update(filename="另一法规.txt", filenames=["另一法规.txt"], document_key="other-document", publication_date="2025-01-01")
    row["sources"].append(other)
    row["record_hash"] = record_digest(row)
    with repo.lock(version):
        repo.write_batch([row])
        repo.publish(version, [row])
    crossed = QuerySpec(version, filename="示例法规.txt", date_field="publication_date", date_from="2024-01-01")
    assert query_knowledge(crossed, mysql_engine)["total"] == 0
    matched = QuerySpec(version, filename="另一法规.txt", date_field="publication_date", date_from="2024-01-01", page_size=1)
    assert query_knowledge(matched, mysql_engine)["total"] == 1
    assert len(query_knowledge(QuerySpec(version, page_size=1, page=2), mysql_engine)["items"]) == 0


def test_real_idempotence_and_database_metadata_tampering(repo, mysql_engine, tmp_path):
    from sqlalchemy import update
    version = "identity_" + uuid.uuid4().hex
    row = set_version(enriched(tmp_path), version)
    with repo.lock(version):
        repo.write_batch([row])
        repo.write_batch([row])
        assert len(repo.read_version(version)) == 1
        with mysql_engine.begin() as connection:
            connection.execute(update(repo.table).where(repo.table.c.dataset_version == version)
                               .values(tags=["tampered"]))
        with pytest.raises(KnowledgeStorageError):
            repo.verify(version, [row])
        with pytest.raises(KnowledgeStorageError):
            repo.write_batch([row])


def test_real_published_version_cannot_change(repo, tmp_path):
    version = "immutable_" + uuid.uuid4().hex
    row = set_version(enriched(tmp_path), version)
    with repo.lock(version):
        repo.write_batch([row])
        repo.publish(version, [row])
        repo.write_batch([row])
        changed = copy.deepcopy(row)
        changed["tags"] = ["new"]
        changed["record_hash"] = record_digest(changed)
        with pytest.raises(KnowledgeStorageError):
            repo.write_batch([changed])


def test_real_competing_lock_and_lost_lock(repo, mysql_engine):
    from app.infrastructure.kg_to_natural_language.storage.repository import KnowledgeRepository
    competing = KnowledgeRepository(mysql_engine)
    version = "lock_" + uuid.uuid4().hex
    with repo.lock(version):
        with pytest.raises(KnowledgeStorageError):
            with competing.lock(version):
                pass
    with pytest.raises(KnowledgeStorageError):
        with repo.lock(version):
            repo._lock_token.connection.close()
            repo.assert_lock()


def test_real_fallback_does_not_borrow_another_physical_file_date(repo, mysql_engine, tmp_path):
    version = "physical_" + uuid.uuid4().hex
    row = set_version(enriched(tmp_path, "empty"), version)
    row["sources"][0]["publication_date"] = "2020-01-01"
    other = copy.deepcopy(row["sources"][0])
    other.update(source_file_snapshot_key="another-physical-node", publication_date="2025-01-01")
    row["sources"].append(other)
    row["record_hash"] = record_digest(row)
    with repo.lock(version):
        repo.write_batch([row])
        repo.publish(version, [row])
    wrong_file = QuerySpec(version, indicator_number="3.1.3", allow_document_fallback=True,
                           date_field="publication_date", date_from="2024-01-01")
    assert query_knowledge(wrong_file, mysql_engine)["total"] == 0
    right_file = QuerySpec(version, indicator_number="3.1.3", allow_document_fallback=True,
                           date_field="publication_date", date_to="2021-01-01")
    assert query_knowledge(right_file, mysql_engine)["total"] == 1


def test_real_indicator_all_mode_matches_whole_objects(repo, mysql_engine, tmp_path):
    version = "all_" + uuid.uuid4().hex
    row = set_version(enriched(tmp_path), version)
    extra = {**row["indicators"][0], "indicator_number": "4.5.6", "indicator_name": "另一个指标"}
    row["indicators"].append(extra)
    row["record_hash"] = record_digest(row)
    with repo.lock(version):
        repo.write_batch([row])
        repo.publish(version, [row])
    assert query_knowledge(QuerySpec(version, indicator_numbers=["3.1.2", "4.5.6"], indicators_mode="all"), mysql_engine)["total"] == 1
    assert query_knowledge(QuerySpec(version, indicator_numbers=["3.1.2", "9.9.9"], indicators_mode="all"), mysql_engine)["total"] == 0


def test_real_pipeline_and_resume(mysql_engine, tmp_path):
    from app.infrastructure.kg_to_natural_language.storage.repository import KnowledgeRepository
    directory = prepared(tmp_path)
    class AckLoss(KnowledgeRepository):
        once = True
        def write_batch(self, rows):
            result = super().write_batch(rows)
            if self.once:
                self.once = False
                raise OSError("simulated acknowledgement loss after real COMMIT")
            return result
    result = persist_knowledge(directory, repository=AckLoss(mysql_engine))
    assert result["status"] == "published", result
    again = persist_knowledge(directory, repository=KnowledgeRepository(mysql_engine))
    assert again["status"] == "already_published", again
