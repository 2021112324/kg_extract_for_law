"""Offline repository contract tests, not a substitute for MySQL integration tests.

Load only the storage package so unrelated legacy package imports cannot connect
to any configured service. All SQL execution uses the explicit in-memory fake.
"""

import builtins
import importlib
import importlib.util
import socket
import sys
import threading
from contextlib import contextmanager
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy.dialects import mysql
from sqlalchemy.schema import CreateTable
from sqlalchemy.sql import operators
from sqlalchemy.sql.elements import BindParameter, BinaryExpression, BooleanClauseList, TextClause

_STORAGE = (Path(__file__).resolve().parents[1] / "app" / "infrastructure"
            / "kg_to_natural_language" / "storage")
_PACKAGE = "_offline_knowledge_repository_test"
_SPEC = importlib.util.spec_from_file_location(
    _PACKAGE, _STORAGE / "__init__.py", submodule_search_locations=[str(_STORAGE)])
_MODULE = importlib.util.module_from_spec(_SPEC)
sys.modules[_PACKAGE] = _MODULE
_SPEC.loader.exec_module(_MODULE)
model = importlib.import_module(_PACKAGE + ".model")
records_module = importlib.import_module(_PACKAGE + ".records")
repository = importlib.import_module(_PACKAGE + ".repository")
KnowledgeRepository = repository.KnowledgeRepository
StorageError = records_module.KnowledgeStorageError


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def blocked(*args, **kwargs):
        raise AssertionError("Offline repository tests must not open network connections")
    monkeypatch.setattr(socket.socket, "connect", blocked)
    monkeypatch.setattr(socket, "create_connection", blocked)


def record(key="id-1", version="v1", **changes):
    timestamp = datetime(2026, 11, 3, 9, 30, 1, 123456)
    item = dict(
        knowledge_id=key, dataset_version=version, content="A person shall comply.",
        knowledge_category="law", knowledge_type="obligation", validity_status=None,
        semantic_types=["obligation"], risk_types=None, economic_industries=None,
        tags=[], indicators=[], document_indicators=[],
        sources=[{"document_key": "doc-1", "filename": "Example Act", "nodes": ["node-1"]}],
        attribute_provenance=[], field_states={"indicators": {"state": "empty"}},
        metadata={"input_binding": {"snapshot": "frozen-input"}}, content_hash="",
        record_hash="", run_id="run-1", conversion_rule_version="rule-v1",
        classification_config_version=None, publication_status="staging",
        generated_at=timestamp, created_at=timestamp, updated_at=timestamp, log_reference=None,
    )
    item.update(changes)
    item["content_hash"] = records_module.digest(item["content"])
    item["record_hash"] = records_module.record_digest(item)
    return item


class Result:
    def __init__(self, rows=(), scalar=None):
        self.rows = list(rows)
        self.scalar = scalar

    def mappings(self):
        return self

    def all(self):
        return self.rows

    def first(self):
        return self.rows[0] if self.rows else None

    def one(self):
        assert len(self.rows) == 1
        return self.rows[0]

    def scalar_one(self):
        return self.scalar


def evaluate(expression, row):
    if expression is None:
        return True
    if isinstance(expression, BindParameter):
        return expression.value
    if isinstance(expression, BooleanClauseList):
        assert expression.operator == operators.and_
        return all(evaluate(clause, row) for clause in expression.clauses)
    if isinstance(expression, BinaryExpression):
        left, right = evaluate(expression.left, row), evaluate(expression.right, row)
        if expression.operator == operators.eq:
            return left == right
        if expression.operator == operators.in_op:
            return left in right
        raise AssertionError("Unsupported predicate in test fake")
    return row[expression.name]


class Backend:
    def __init__(self):
        self.rows = []
        self.locks = {}
        self.next_id = 100
        self.mutex = threading.Lock()


class FakeEngine:
    def __init__(self, backend=None):
        self.backend = backend or Backend()
        self.dialect = SimpleNamespace(name="mysql", is_mariadb=False)
        self.version = "8.0.36"
        self.comment = "MySQL Community Server - GPL"
        self.server_uuid = "ordinary-server-uuid"
        self.database = "isolated_fake_database"
        self.variables = []
        self.channels = 0
        self.json_value = 7
        self.collation_count = 1
        self.connections = []
        self.events = []
        self.fail_insert = False
        self.fail_commit = None
        self.on_insert = None
        self.on_update = None

    def connect(self):
        with self.backend.mutex:
            self.backend.next_id += 1
            connection = FakeConnection(self, self.backend.next_id)
            self.connections.append(connection)
            return connection

    @contextmanager
    def begin(self):
        with self.connect() as connection:
            connection.transaction_rows = deepcopy(self.backend.rows)
            self.events.append("begin")
            try:
                yield connection
            except BaseException:
                self.events.append("rollback")
                raise
            else:
                if self.fail_commit == "before":
                    self.events.append("commit_unknown_before")
                    raise RuntimeError("commit acknowledgement unavailable")
                self.backend.rows = connection.transaction_rows
                self.events.append("commit")
                if self.fail_commit == "after":
                    raise RuntimeError("commit acknowledgement unavailable")


class FakeConnection:
    def __init__(self, engine, identity):
        self.engine = engine
        self.identity = identity
        self.server_uuid = engine.server_uuid
        self.database = engine.database
        self.closed = False
        self.invalidated = False
        self.transaction_rows = None
        self.fail_assert = False
        self.detached = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        if self.detached:
            for name, owner in list(self.engine.backend.locks.items()):
                if owner == self.identity:
                    del self.engine.backend.locks[name]
        self.closed = True

    def detach(self):
        self.detached = True

    def invalidate(self):
        self.invalidated = True
        for name, owner in list(self.engine.backend.locks.items()):
            if owner == self.identity:
                del self.engine.backend.locks[name]

    def execute(self, statement, parameters=None):
        assert not self.closed and not self.invalidated
        engine = self.engine
        if isinstance(statement, TextClause):
            sql = statement.text
            parameters = parameters or {}
            engine.events.append((self.identity, sql, deepcopy(parameters)))
            if "VERSION()" in sql:
                return Result([dict(server_version=engine.version, version_comment=engine.comment,
                                    server_uuid=self.server_uuid, database_name=self.database)])
            if sql.startswith("SHOW GLOBAL VARIABLES"):
                return Result(engine.variables)
            if "replication_connection_configuration" in sql:
                return Result(scalar=engine.channels)
            if "information_schema.COLLATIONS" in sql:
                assert parameters == {"collation": "utf8mb4_0900_bin"}
                return Result(scalar=engine.collation_count)
            if "JSON_TABLE" in sql:
                assert parameters["document"] == '[{"value":7}]'
                return Result(scalar=engine.json_value)
            if sql == "SELECT CONNECTION_ID()":
                return Result(scalar=self.identity)
            if "GET_LOCK(" in sql:
                with engine.backend.mutex:
                    name = parameters["name"]
                    if name in engine.backend.locks:
                        return Result(scalar=0)
                    engine.backend.locks[name] = self.identity
                    return Result(scalar=1)
            if "RELEASE_LOCK(" in sql:
                if getattr(engine, "fail_release", False):
                    raise RuntimeError("release acknowledgement unavailable")
                name = parameters["name"]
                if engine.backend.locks.get(name) != self.identity:
                    return Result(scalar=0)
                del engine.backend.locks[name]
                return Result(scalar=1)
            if "IS_USED_LOCK(" in sql:
                if self.fail_assert:
                    raise RuntimeError("connection transport unavailable")
                return Result([dict(connection_id=self.identity, server_uuid=self.server_uuid,
                                    database_name=self.database,
                                    lock_owner=engine.backend.locks.get(parameters["name"]))])
            raise AssertionError("Unexpected SQL: " + sql)
        rows = self.transaction_rows if self.transaction_rows is not None else engine.backend.rows
        if statement.is_select:
            selected = [deepcopy(row) for row in rows if evaluate(statement.whereclause, row)]
            for column in reversed(list(statement._order_by_clauses)):
                selected.sort(key=lambda row: row[column.name])
            if statement._limit_clause is not None:
                selected = selected[:statement._limit_clause.value]
            return Result(selected)
        assert self.transaction_rows is not None, "All writes require a short transaction"
        if statement.is_insert:
            rows.extend(deepcopy(parameters))
            engine.events.append("insert")
            if engine.on_insert:
                engine.on_insert()
            if engine.fail_insert:
                raise RuntimeError("insert failed")
            return Result()
        if statement.is_update:
            for row in rows:
                if evaluate(statement.whereclause, row):
                    row.update({key: value.value for key, value in statement._values.items()})
            engine.events.append("update")
            if engine.on_update:
                engine.on_update()
            return Result()
        raise AssertionError("Unexpected Core statement")


@pytest.fixture
def store():
    engine = FakeEngine()
    return KnowledgeRepository(engine), engine


def test_model_is_exact_single_table_with_mysql_types():
    table = model.knowledge_entry_table
    assert set(model.metadata.tables) == {model.TABLE_NAME}
    assert len(table.c) == 26
    assert [c.name for c in table.primary_key] == ["dataset_version", "knowledge_id"]
    assert table.c.dataset_version.type.length == 96
    assert table.c.knowledge_id.type.length == 80
    ddl = str(CreateTable(table).compile(dialect=mysql.dialect()))
    assert "LONGTEXT NOT NULL" in ddl
    assert ddl.count("DATETIME(6)") == 3
    assert "PRIMARY KEY (dataset_version, knowledge_id)" in ddl
    assert "ENGINE=InnoDB" in ddl and "CHARSET=utf8mb4" in ddl
    assert "utf8mb4_0900_bin" in ddl
    for field in repository._JSON_FIELDS:
        kind = table.c[field].type
        assert kind.none_as_null
        process = kind.bind_processor(mysql.dialect())
        assert process(None) is None
        assert process([]) == "[]"
        assert process({"key": ["value"]}) == '{"key": ["value"]}'


def test_import_and_default_constructor_do_not_import_project_db(monkeypatch):
    original = builtins.__import__

    def guarded(name, *args, **kwargs):
        if name.startswith("app.db"):
            raise AssertionError("Project database imported too early")
        return original(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded)
    importlib.reload(model)
    importlib.reload(repository)
    assert repository.KnowledgeRepository()._engine is None


def test_default_engine_is_lazily_reused(monkeypatch):
    engine = FakeEngine()
    monkeypatch.setitem(sys.modules, "app.db.base", SimpleNamespace(engine=engine))
    repo = KnowledgeRepository()
    assert repo._engine is None
    assert repo.engine is engine


def test_preflight_is_read_only_and_reports_target(store):
    repo, engine = store
    info = repo.preflight()
    assert info["database_name"] == engine.database
    assert info["server_uuid"] == engine.server_uuid
    assert not engine.backend.rows and not engine.backend.locks
    assert all(not isinstance(event, str) for event in engine.events)


@pytest.mark.parametrize("attribute,value", [
    ("version", "5.7.44"), ("version", "8.0.3"), ("version", "10.11.7-MariaDB"),
    ("comment", "MariaDB Server"), ("comment", "TiDB"), ("comment", "Aurora MySQL"),
    ("database", None), ("server_uuid", ""), ("channels", 1), ("json_value", 0),
    ("collation_count", 0),
    ("variables", [("group_replication_group_name", "group-uuid")]),
    ("variables", [("wsrep_on", "ON")]), ("variables", [("ndbcluster", "ON")]),
])
def test_preflight_rejects_incompatible_server(store, attribute, value):
    repo, engine = store
    setattr(engine, attribute, value)
    with pytest.raises(StorageError):
        repo.preflight()


@pytest.mark.parametrize("dialect", [SimpleNamespace(name="sqlite", is_mariadb=False),
                                     SimpleNamespace(name="mysql", is_mariadb=True)])
def test_preflight_rejects_non_mysql_dialects(store, dialect):
    repo, engine = store
    engine.dialect = dialect
    with pytest.raises(StorageError):
        repo.preflight()


def schema_inspector(table):
    columns = [dict(name=c.name, type=deepcopy(c.type), nullable=c.nullable) for c in table.c]
    return SimpleNamespace(
        get_columns=lambda name: columns,
        get_pk_constraint=lambda name: {"constrained_columns": ["dataset_version", "knowledge_id"]},
        get_table_options=lambda name: {"mysql_engine": "InnoDB", "mysql_default charset": "utf8mb4",
                                        "mysql_collate": "utf8mb4_0900_bin"},
    )


def test_initialize_creates_only_selected_table_and_checks_schema(store, monkeypatch):
    repo, engine = store
    created = []
    monkeypatch.setattr(type(repo.table), "create", lambda self, **kwargs: created.append((self, kwargs)))
    monkeypatch.setattr(repository, "inspect", lambda connection: schema_inspector(repo.table))
    assert repo.initialize() is None
    assert len(created) == 1 and created[0][0] is repo.table
    assert created[0][1]["checkfirst"] is True
    assert engine.events[-1] == "commit"


@pytest.mark.parametrize("defect", ["columns", "pk", "type", "length", "nullable", "fsp", "engine", "collation"])
def test_initialize_never_drops_incompatible_table(store, monkeypatch, defect):
    repo, engine = store
    inspector = schema_inspector(repo.table)
    columns = inspector.get_columns(model.TABLE_NAME)
    if defect == "columns":
        columns.pop()
    elif defect == "pk":
        inspector.get_pk_constraint = lambda name: {"constrained_columns": ["knowledge_id"]}
    elif defect == "type":
        columns[2]["type"] = mysql.TEXT()
    elif defect == "length":
        columns[0]["type"] = mysql.VARCHAR(40, collation="utf8mb4_0900_bin")
    elif defect == "nullable":
        columns[2]["nullable"] = True
    elif defect == "fsp":
        next(c for c in columns if c["name"] == "created_at")["type"] = mysql.DATETIME()
    elif defect == "engine":
        inspector.get_table_options = lambda name: {"mysql_engine": "MyISAM"}
    elif defect == "collation":
        columns[0]["type"].collation = "utf8mb4_bin"
    monkeypatch.setattr(repository, "inspect", lambda connection: inspector)
    monkeypatch.setattr(type(repo.table), "create", lambda *args, **kwargs: None)

    def forbidden(*args, **kwargs):
        pytest.fail("Incompatible tables must never be dropped")

    monkeypatch.setattr(type(repo.table), "drop", forbidden)
    with pytest.raises(StorageError):
        repo.initialize()
    assert engine.events[-1] == "rollback"


def test_create_failure_never_attempts_drop(store, monkeypatch):
    repo, _ = store

    def failed(*args, **kwargs):
        raise RuntimeError("DDL failed")

    monkeypatch.setattr(type(repo.table), "create", failed)
    monkeypatch.setattr(type(repo.table), "drop", lambda *args, **kwargs: pytest.fail("Unexpected DROP"))
    with pytest.raises(RuntimeError, match="DDL failed"):
        repo.initialize()


def test_read_before_lock_and_short_transaction_idempotency(store):
    repo, engine = store
    source = record()
    assert repo.read_version("v1") == []
    with repo.lock("v1") as held:
        assert held is repo
        lock_connection = repo._lock_token.connection
        assert repo.write_batch([source]) == 1
        persisted = repo.read_version("v1")
        replay = record(run_id="new-run", log_reference="new-log",
                        generated_at=datetime(2027, 1, 1), created_at=datetime(2027, 1, 1),
                        updated_at=datetime(2027, 1, 1))
        assert repo.write_batch([replay]) == 0
        assert repo.read_version("v1") == persisted
        assert engine.connections.count(lock_connection) == 1
        assert all(connection.closed for connection in engine.connections if connection is not lock_connection)
        assert lock_connection.detached
        assert not lock_connection.closed
    assert lock_connection.closed
    assert not engine.backend.locks
    assert source == record()
    assert engine.events.count("commit") == 2


def test_write_and_publish_always_require_lock_even_if_empty(store):
    repo, _ = store
    for action in (lambda: repo.assert_lock(), lambda: repo.write_batch([]),
                   lambda: repo.publish("v1", [record()])):
        with pytest.raises(StorageError, match="lock context"):
            action()


def test_partial_staging_batch_resume_skips_existing_without_updating(store):
    repo, engine = store
    with repo.lock("v1"):
        repo.write_batch([record("a")])
        original = deepcopy(engine.backend.rows[0])
        resumed = [record("a", run_id="run-resumed", log_reference="resumed-log"), record("b")]
        assert repo.write_batch(resumed) == 1
        assert engine.backend.rows[0] == original
        assert repo.verify("v1", resumed) is None
        assert repo.publish("v1", resumed) == 2


@pytest.mark.parametrize("defect", ["missing_column", "unknown_column", "stale_hash", "double_json",
                                   "config_length", "bad_log_reference"])
def test_invalid_records_do_not_start_write_transaction(store, defect):
    repo, engine = store
    source = record()
    if defect == "missing_column":
        del source["log_reference"]
    elif defect == "unknown_column":
        source["business_digest"] = source["record_hash"]
    elif defect == "stale_hash":
        source["metadata"]["input_binding"] = "tampered"
    elif defect == "double_json":
        source["sources"] = records_module.canonical(source["sources"])
        source["record_hash"] = records_module.record_digest(source)
    elif defect == "config_length":
        source = record(classification_config_version="x" * 97)
    else:
        source["log_reference"] = {"unexpected": "object"}
    with repo.lock("v1"):
        with pytest.raises(StorageError):
            repo.write_batch([source])
        assert "begin" not in engine.events
    assert not engine.backend.rows


def test_wrong_version_and_duplicate_identity_rejected(store):
    repo, engine = store
    with repo.lock("v1"):
        for batch in ([record(version="v2")], [record(), record()]):
            with pytest.raises(StorageError):
                repo.write_batch(batch)
        with pytest.raises(StorageError):
            repo.publish("v2", [record(version="v2")])
    assert not engine.backend.rows


@pytest.mark.parametrize("field,value", [
    ("content", "Changed content"), ("sources", [{"document_key": "different-document"}]),
    ("metadata", {"input_binding": {"snapshot": "changed-input"}}),
    ("tags", ["different-tag"]), ("validity_status", "expired"),
    ("classification_config_version", "different-config"),
])
def test_complete_business_changes_are_conflicts(store, field, value):
    repo, engine = store
    with repo.lock("v1"):
        repo.write_batch([record()])
        before = deepcopy(engine.backend.rows)
        with pytest.raises(StorageError):
            repo.write_batch([record(**{field: value})])
        with pytest.raises(StorageError):
            repo.verify("v1", [record(**{field: value})])
        assert engine.backend.rows == before


@pytest.mark.parametrize("field,value", [("sources", [{"document_key": "tampered"}]),
                                        ("metadata", {"input_binding": "tampered"}),
                                        ("record_hash", "0" * 64)])
def test_stored_hash_is_not_trusted(store, field, value):
    repo, engine = store
    with repo.lock("v1"):
        repo.write_batch([record()])
        engine.backend.rows[0][field] = value
        for action in (lambda: repo.write_batch([record()]), lambda: repo.verify("v1", [record()]),
                       lambda: repo.publish("v1", [record()])):
            with pytest.raises(StorageError):
                action()
    assert engine.backend.rows[0]["publication_status"] == "staging"


def test_full_fields_checked_even_if_digest_function_collides(store, monkeypatch):
    repo, engine = store
    monkeypatch.setattr(records_module, "record_digest", lambda value: "f" * 64)
    monkeypatch.setattr(repository, "record_digest", lambda value: "f" * 64)
    with repo.lock("v1"):
        repo.write_batch([record()])
        with pytest.raises(StorageError, match="business record conflict"):
            repo.write_batch([record(metadata={"input_binding": "different"})])
    assert engine.backend.rows[0]["metadata"]["input_binding"] == {"snapshot": "frozen-input"}


def test_verify_exact_keys_and_atomic_publish_replay(store):
    repo, engine = store
    manifest = [record("b"), record("a")]
    with repo.lock("v1"):
        repo.write_batch(manifest)
        assert [row["knowledge_id"] for row in repo.read_version("v1")] == ["a", "b"]
        for wrong in (manifest[:1], manifest + [record("c")], [record("x"), record("y")]):
            with pytest.raises(StorageError):
                repo.verify("v1", wrong)
            with pytest.raises(StorageError):
                repo.publish("v1", wrong)
        assert repo.verify("v1", manifest) is None
        assert repo.publish("v1", manifest) == 2
        published = deepcopy(engine.backend.rows)
        assert repo.publish("v1", manifest) == 0
        assert repo.write_batch(manifest) == 0
        assert engine.backend.rows == published
        with pytest.raises(StorageError, match="immutable"):
            repo.write_batch([record("new")])
    assert {row["publication_status"] for row in engine.backend.rows} == {"published"}


def test_empty_version_can_be_verified_but_never_published(store):
    repo, _ = store
    assert repo.verify("v1", []) is None
    with repo.lock("v1"):
        assert repo.write_batch([]) == 0
        with pytest.raises(StorageError, match="empty manifest"):
            repo.publish("v1", [])


def test_new_rows_cannot_bypass_staging_with_input_status(store):
    repo, _ = store
    with repo.lock("v1"):
        repo.write_batch([record(publication_status="published")])
    assert repo.read_version("v1")[0]["publication_status"] == "staging"


def test_multiple_versions_and_case_sensitive_identity(store):
    repo, _ = store
    with repo.lock("v1"):
        repo.write_batch([record("Id"), record("id")])
    with repo.lock("V1"):
        repo.write_batch([record("Id", "V1")])
    assert len(repo.read_version("v1")) == 2
    assert len(repo.read_version("V1")) == 1


def test_json_dates_and_aware_timestamps_are_normalized_without_mutation(store):
    repo, _ = store
    aware = datetime(2026, 11, 3, 1, 30, tzinfo=timezone.utc)
    source = record(generated_at=aware, metadata={"date": date(2026, 1, 2)})
    with repo.lock("v1"):
        repo.write_batch([source])
        repo.verify("v1", [source])
    row = repo.read_version("v1")[0]
    assert row["generated_at"] == datetime(2026, 11, 3, 9, 30)
    assert row["metadata"] == {"date": "2026-01-02"}
    assert source["generated_at"] == aware
    assert source["metadata"]["date"] == date(2026, 1, 2)


@pytest.mark.parametrize("defect", ["owner", "closed", "invalidated", "reconnected", "server", "database", "transport"])
def test_lock_loss_stops_all_subsequent_writes(store, defect):
    repo, engine = store
    with pytest.raises(StorageError):
        with repo.lock("v1"):
            token = repo._lock_token
            if defect == "owner":
                engine.backend.locks[token.name] = 999
            elif defect == "closed":
                token.connection.closed = True
            elif defect == "invalidated":
                token.connection.invalidated = True
            elif defect == "reconnected":
                token.connection.identity += 1
            elif defect == "server":
                token.connection.server_uuid = "different-server"
            elif defect == "database":
                token.connection.database = "different-database"
            else:
                token.connection.fail_assert = True
            for action in (repo.assert_lock, lambda: repo.write_batch([record()]),
                           lambda: repo.publish("v1", [record()])):
                with pytest.raises(StorageError):
                    action()
            assert token.lost
    assert not engine.backend.rows


@pytest.mark.parametrize("action", ["write", "publish"])
def test_lock_lost_during_mutation_rolls_back_transaction(store, action):
    repo, engine = store
    with pytest.raises(StorageError):
        with repo.lock("v1"):
            if action == "publish":
                repo.write_batch([record()])
            before = deepcopy(engine.backend.rows)
            lose_lock = lambda: engine.backend.locks.clear()
            if action == "write":
                engine.on_insert = lose_lock
                repo.write_batch([record()])
            else:
                engine.on_update = lose_lock
                repo.publish("v1", [record()])
    assert engine.backend.rows == before
    assert "rollback" in engine.events


def test_write_connection_target_drift_is_rejected(store):
    repo, engine = store
    with pytest.raises(StorageError):
        with repo.lock("v1"):
            engine.server_uuid = "another-server"
            with pytest.raises(StorageError, match="locked database/server"):
                repo.write_batch([record()])
    assert not engine.backend.rows


def test_same_server_named_lock_excludes_other_repository_instances(store):
    repo, engine = store
    other = KnowledgeRepository(FakeEngine(engine.backend))
    errors = []

    def contender():
        try:
            with other.lock("v1"):
                pytest.fail("Concurrent same-version writer entered critical section")
        except StorageError as exc:
            errors.append(exc)

    with repo.lock("v1", timeout=2):
        thread = threading.Thread(target=contender)
        thread.start()
        thread.join(timeout=5)
        assert not thread.is_alive() and len(errors) == 1
        repo.assert_lock()
        with other.lock("v2"):
            other.assert_lock()
        with pytest.raises(StorageError, match="active lock"):
            with repo.lock("v1"):
                pass
    with other.lock("v1"):
        other.assert_lock()
    assert not engine.backend.locks


def test_assert_lock_rejects_forked_process_and_foreign_thread(store, monkeypatch):
    repo, _ = store
    with repo.lock("v1"):
        pid = repo._lock_token.process_id
        with monkeypatch.context() as patch:
            patch.setattr(repository.os, "getpid", lambda: pid + 1)
            with pytest.raises(StorageError, match="another thread or process"):
                repo.assert_lock()
        errors = []

        def foreign_thread():
            try:
                repo.assert_lock()
            except StorageError as exc:
                errors.append(exc)

        thread = threading.Thread(target=foreign_thread)
        thread.start()
        thread.join(timeout=5)
        assert len(errors) == 1


def test_batch_insert_failure_rolls_back_and_releases_lock(store):
    repo, engine = store
    engine.fail_insert = True
    with pytest.raises(RuntimeError, match="insert failed"):
        with repo.lock("v1"):
            repo.write_batch([record("a"), record("b")])
    assert not engine.backend.rows and not engine.backend.locks
    assert engine.events[-2] != "commit"
    with repo.lock("v1"):
        repo.assert_lock()


@pytest.mark.parametrize("primary_error", [False, True])
def test_release_failure_invalidates_connection_without_masking_body_error(store, primary_error):
    repo, engine = store
    engine.fail_release = True
    expected = RuntimeError if primary_error else StorageError
    message = "primary transaction error" if primary_error else "cleanup failed"
    with pytest.raises(expected, match=message):
        with repo.lock("v1"):
            if primary_error:
                raise RuntimeError("primary transaction error")
    assert not engine.backend.locks
    assert engine.connections[0].closed and engine.connections[0].invalidated
    assert repo._lock_token is None
    engine.fail_release = False
    with repo.lock("v1"):
        repo.assert_lock()


def test_preflight_permission_failure_fails_closed_and_lock_guard_is_reusable(store, monkeypatch):
    repo, engine = store
    execute = FakeConnection.execute

    def denied(self, statement, parameters=None):
        if isinstance(statement, TextClause) and "replication_connection_configuration" in statement.text:
            raise RuntimeError("SELECT privilege unavailable")
        return execute(self, statement, parameters)

    with monkeypatch.context() as patch:
        patch.setattr(FakeConnection, "execute", denied)
        with pytest.raises(StorageError, match="preflight failed"):
            repo.preflight()
        with pytest.raises(StorageError, match="preflight failed"):
            with repo.lock("v1"):
                pass
    assert not engine.backend.locks and not engine.backend.rows
    with repo.lock("v1"):
        repo.assert_lock()


@pytest.mark.parametrize("failure,visible", [("before", False), ("after", True)])
@pytest.mark.parametrize("action", ["write", "publish"])
def test_unknown_commit_propagates_without_retry_and_is_readable(store, failure, visible, action):
    repo, engine = store
    with repo.lock("v1"):
        if action == "publish":
            repo.write_batch([record()])
        engine.fail_commit = failure
        with pytest.raises(RuntimeError, match="acknowledgement"):
            if action == "write":
                repo.write_batch([record()])
            else:
                repo.publish("v1", [record()])
        engine.fail_commit = None
    rows = repo.read_version("v1")
    if action == "write":
        assert bool(rows) == visible
        assert engine.events.count("insert") == 1
    else:
        assert rows[0]["publication_status"] == ("published" if visible else "staging")
        assert engine.events.count("update") == 1


@pytest.mark.parametrize("version", [None, "", " ", "x" * 97])
def test_invalid_versions_never_connect(store, version):
    repo, engine = store
    with pytest.raises(StorageError):
        with repo.lock(version):
            pass
    with pytest.raises(StorageError):
        repo.read_version(version)
    assert not engine.connections


@pytest.mark.parametrize("timeout", [-1, 0.5, True, "0"])
def test_invalid_lock_timeout_never_connects(store, timeout):
    repo, engine = store
    with pytest.raises(StorageError):
        with repo.lock("v1", timeout=timeout):
            pass
    assert not engine.connections
