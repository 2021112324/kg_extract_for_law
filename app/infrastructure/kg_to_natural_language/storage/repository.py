"""Core-only, single-server MySQL persistence with fail-closed named locks.

No engine is constructed here. An omitted engine is resolved lazily from
app.db.base; Core transactions use that engine's existing connection pool.
"""

import hashlib
import json
import os
import re
import sys
import threading
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

from sqlalchemy import inspect, select, text

from .model import TABLE_NAME, knowledge_entry_table
from .records import BUSINESS_FIELDS, KnowledgeStorageError, record_digest, validate_record

_CST = timezone(timedelta(hours=8))
_TIMES = ("generated_at", "created_at", "updated_at")
_JSON_FIELDS = tuple(c.name for c in knowledge_entry_table.c
                     if c.type.__visit_name__.upper() == "JSON")
_TARGET_SQL = "@@server_uuid AS server_uuid, DATABASE() AS database_name"


def _json_default(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError("Unsupported JSON value type: " + type(value).__name__)


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
                      allow_nan=False, default=_json_default)


def _version(version):
    if not isinstance(version, str) or not version.strip() or len(version) > 96:
        raise KnowledgeStorageError("A nonempty dataset_version of at most 96 characters is required")
    return version


@dataclass
class _LockToken:
    connection: object
    name: str
    version: str
    connection_id: int
    server_uuid: str
    database_name: str
    thread_id: int
    process_id: int
    lost: bool = False


class KnowledgeRepository:
    """One instance may hold one lock context in one thread at a time."""

    def __init__(self, engine=None):
        self._engine = engine
        self.table = knowledge_entry_table
        self._lock_token = None
        self._lock_guard = threading.Lock()

    @property
    def engine(self):
        if self._engine is None:
            from app.db.base import engine
            self._engine = engine
        return self._engine

    def _preflight_connection(self, connection):
        dialect = self.engine.dialect
        if dialect.name != "mysql" or getattr(dialect, "is_mariadb", False):
            raise KnowledgeStorageError("Only ordinary single-server MySQL 8.0+ is supported")
        try:
            info = dict(connection.execute(text(
                "SELECT VERSION() AS server_version, @@version_comment AS version_comment, "
                + _TARGET_SQL
            )).mappings().one())
            description = (info["server_version"] + " " + info["version_comment"]).lower()
            match = re.match(r"(\d+)\.(\d+)\.(\d+)", info["server_version"])
            if (not match or tuple(map(int, match.groups())) < (8, 0, 4)
                    or any(name in description for name in ("mariadb", "tidb", "vitess", "aurora", "ndb"))):
                raise KnowledgeStorageError("MySQL 8.0+ with native JSON_TABLE is required")
            if not info["server_uuid"] or not info["database_name"]:
                raise KnowledgeStorageError("A concrete MySQL server UUID and selected database are required")
            collation = connection.execute(text(
                "SELECT COUNT(*) FROM information_schema.COLLATIONS "
                "WHERE COLLATION_NAME = :collation AND PAD_ATTRIBUTE = 'NO PAD'"
            ), {"collation": "utf8mb4_0900_bin"}).scalar_one()
            if collation != 1:
                raise KnowledgeStorageError("Identity collation utf8mb4_0900_bin is required (MySQL 8.0.17+)")
            variables = connection.execute(text(
                "SHOW GLOBAL VARIABLES WHERE Variable_name IN "
                "('group_replication_group_name', 'wsrep_on', 'ndbcluster')"
            )).all()
            for name, value in variables:
                if (name.lower() == "group_replication_group_name" and value
                        or name.lower() != "group_replication_group_name"
                        and str(value).upper() not in ("", "OFF", "0", "DISABLED")):
                    raise KnowledgeStorageError("Clustered MySQL is outside the named-lock contract")
            # Fail closed when replication topology cannot be inspected.
            channels = connection.execute(text(
                "SELECT COUNT(*) FROM performance_schema.replication_connection_configuration"
            )).scalar_one()
            if channels:
                raise KnowledgeStorageError("Replication channels are outside the single-server contract")
            capability = connection.execute(text(
                "SELECT jt.value FROM JSON_TABLE(CAST(:document AS JSON), "
                "'$[*]' COLUMNS(value INT PATH '$.value')) AS jt"
            ), {"document": '[{"value":7}]'}).scalar_one()
            if capability != 7:
                raise KnowledgeStorageError("JSON_TABLE capability check failed")
            return info
        except KnowledgeStorageError:
            raise
        except Exception as exc:
            raise KnowledgeStorageError("MySQL capability/topology preflight failed") from exc

    def preflight(self):
        """Read-only capability/topology checks; return non-secret target information."""
        with self.engine.connect() as connection:
            return self._preflight_connection(connection)

    def initialize(self):
        """Preflight, create only this table if absent, and reject incompatible schema."""
        with self.engine.begin() as connection:
            self._preflight_connection(connection)
            self.table.create(bind=connection, checkfirst=True)
            self._check_schema(connection)

    def _check_schema(self, connection):
        inspector = inspect(connection)
        columns = {c["name"]: c for c in inspector.get_columns(TABLE_NAME)}
        if set(columns) != set(self.table.c.keys()):
            raise KnowledgeStorageError("Existing knowledge table has incompatible columns")
        pk = inspector.get_pk_constraint(TABLE_NAME)["constrained_columns"]
        if pk != ["dataset_version", "knowledge_id"]:
            raise KnowledgeStorageError("Existing knowledge table has an incompatible primary key")
        options = inspector.get_table_options(TABLE_NAME)
        charset = options.get("mysql_charset", options.get("mysql_default charset", ""))
        if (options.get("mysql_engine", "").lower() != "innodb"
                or charset.lower() != "utf8mb4"):
            raise KnowledgeStorageError("Knowledge table must use InnoDB and utf8mb4")
        for expected in self.table.c:
            actual = columns[expected.name]
            kind = actual["type"]
            if (kind.__visit_name__.upper() != expected.type.__visit_name__.upper()
                    or getattr(kind, "length", None) != getattr(expected.type, "length", None)
                    or bool(actual["nullable"]) != expected.nullable
                    or getattr(kind, "fsp", None) != getattr(expected.type, "fsp", None)):
                raise KnowledgeStorageError("Incompatible knowledge column: " + expected.name)
            if expected.name in ("dataset_version", "knowledge_id"):
                collation = getattr(kind, "collation", None) or options.get("mysql_collate", "")
                if collation.lower() != "utf8mb4_0900_bin":
                    raise KnowledgeStorageError("Identity columns require case-sensitive NO PAD collation")

    @contextmanager
    def lock(self, version, timeout=0):
        """Hold a dedicated GET_LOCK connection until context exit; yield this repo."""
        version = _version(version)
        if isinstance(timeout, bool) or not isinstance(timeout, int) or timeout < 0:
            raise KnowledgeStorageError("Lock timeout must be a nonnegative integer number of seconds")
        if not self._lock_guard.acquire(blocking=False):
            raise KnowledgeStorageError("This repository already has an active lock context")
        connection = None
        token = None
        try:
            connection = self.engine.connect()
            # Never return a physical session that might still own GET_LOCK to a pool.
            connection.detach()
            info = self._preflight_connection(connection)
            name = hashlib.sha256(_canonical(
                [info["database_name"], TABLE_NAME, version]
            ).encode("utf-8")).hexdigest()
            owner = connection.execute(text("SELECT CONNECTION_ID()")).scalar_one()
            acquired = connection.execute(text("SELECT GET_LOCK(:name, :timeout)"),
                                          {"name": name, "timeout": timeout}).scalar_one()
            if acquired != 1:
                raise KnowledgeStorageError("Knowledge version lock unavailable")
            token = _LockToken(connection, name, version, owner, info["server_uuid"],
                               info["database_name"], threading.get_ident(), os.getpid())
            self._lock_token = token
            self.assert_lock()
            yield self
            self.assert_lock()
        finally:
            active_error = sys.exc_info()[0] is not None
            cleanup_error = None
            try:
                if connection is not None and not connection.closed:
                    try:
                        if token is None or token.lost or connection.invalidated:
                            connection.invalidate()
                        else:
                            self.assert_lock()
                            released = connection.execute(text("SELECT RELEASE_LOCK(:name)"),
                                                          {"name": token.name}).scalar_one()
                            if released != 1:
                                raise KnowledgeStorageError("Named lock release was not confirmed")
                    except Exception as exc:
                        cleanup_error = exc
                        try:
                            if not connection.closed:
                                connection.invalidate()
                        except Exception:
                            pass  # Preserve the ownership/release failure as the cause.
            finally:
                self._lock_token = None
                try:
                    if connection is not None:
                        connection.close()
                except Exception as exc:
                    cleanup_error = cleanup_error or exc
                finally:
                    self._lock_guard.release()
            if cleanup_error is not None and not active_error:
                raise KnowledgeStorageError("Named lock cleanup failed") from cleanup_error

    def assert_lock(self):
        """Fail permanently for this context if its connection/owner/target changed."""
        token = self._lock_token
        if token is None:
            raise KnowledgeStorageError("A version lock context is required")
        if token.thread_id != threading.get_ident() or token.process_id != os.getpid():
            raise KnowledgeStorageError("The lock belongs to another thread or process")
        if token.lost:
            raise KnowledgeStorageError("The version lock was lost; recovery verification is required")
        try:
            if token.connection.closed or token.connection.invalidated:
                raise KnowledgeStorageError("The dedicated lock connection is unavailable")
            row = token.connection.execute(text(
                "SELECT CONNECTION_ID() AS connection_id, IS_USED_LOCK(:name) AS lock_owner, "
                + _TARGET_SQL
            ), {"name": token.name}).mappings().one()
            if (row["connection_id"] != token.connection_id or row["lock_owner"] != token.connection_id
                    or row["server_uuid"] != token.server_uuid
                    or row["database_name"] != token.database_name):
                raise KnowledgeStorageError("The dedicated connection no longer owns this version lock")
        except Exception as exc:
            token.lost = True
            raise KnowledgeStorageError("Version lock ownership cannot be confirmed") from exc

    def _assert_write_connection(self, connection, version):
        self.assert_lock()
        token = self._lock_token
        if version != token.version:
            raise KnowledgeStorageError("The held lock does not cover this dataset_version")
        row = connection.execute(text(
            "SELECT " + _TARGET_SQL + ", IS_USED_LOCK(:name) AS lock_owner"
        ), {"name": token.name}).mappings().one()
        if (row["server_uuid"] != token.server_uuid or row["database_name"] != token.database_name
                or row["lock_owner"] != token.connection_id):
            token.lost = True
            raise KnowledgeStorageError("Write connection does not share the locked database/server")

    def _prepare(self, records, version=None):
        prepared = []
        seen = set()
        for record in records:
            item = deepcopy(dict(record))
            if set(item) != set(self.table.c.keys()):
                raise KnowledgeStorageError("Records must contain exactly the 26 knowledge columns")
            for field in _TIMES:
                value = item[field]
                if isinstance(value, datetime) and value.tzinfo is not None:
                    item[field] = value.astimezone(_CST).replace(tzinfo=None)
            for field in _JSON_FIELDS:
                item[field] = json.loads(_canonical(item[field]))
            validate_record(item)
            if version is not None and item["dataset_version"] != version:
                raise KnowledgeStorageError("Record belongs to a different dataset_version")
            config_version = item["classification_config_version"]
            if config_version is not None and (not isinstance(config_version, str) or len(config_version) > 96):
                raise KnowledgeStorageError("Invalid classification_config_version")
            if item["log_reference"] is not None and not isinstance(item["log_reference"], str):
                raise KnowledgeStorageError("Invalid log_reference")
            key = (item["dataset_version"], item["knowledge_id"])
            if key in seen:
                raise KnowledgeStorageError("Duplicate knowledge identity in input records")
            seen.add(key)
            prepared.append(item)
        return prepared

    def _read_rows(self, connection, version, for_update=False):
        statement = select(self.table).where(self.table.c.dataset_version == version).order_by(
            self.table.c.knowledge_id)
        if for_update:
            statement = statement.with_for_update()
        return [dict(row) for row in connection.execute(statement).mappings().all()]

    def read_version(self, version):
        """Read all staging/published rows, ordered by knowledge_id; no lock needed."""
        version = _version(version)
        with self.engine.connect() as connection:
            return self._read_rows(connection, version)

    @staticmethod
    def _same_record(stored, expected):
        validate_record(stored)
        if (stored["record_hash"] != expected["record_hash"]
                or record_digest(stored) != stored["record_hash"]
                or record_digest(expected) != expected["record_hash"]
                or _canonical({k: stored[k] for k in BUSINESS_FIELDS})
                != _canonical({k: expected[k] for k in BUSINESS_FIELDS})):
            raise KnowledgeStorageError("Knowledge business record conflict; use a new dataset_version")

    def write_batch(self, records):
        """Insert new rows as staging in one transaction; return inserted row count.

        Identical existing records are untouched, including audit fields. A version
        containing published rows cannot acquire new identities.
        """
        self.assert_lock()
        version = self._lock_token.version
        prepared = self._prepare(records, version)
        with self.engine.begin() as connection:
            self._assert_write_connection(connection, version)
            published = connection.execute(select(self.table.c.knowledge_id).where(
                self.table.c.dataset_version == version,
                self.table.c.publication_status == "published").limit(1)).first() is not None
            keys = [item["knowledge_id"] for item in prepared]
            existing = connection.execute(select(self.table).where(
                self.table.c.dataset_version == version,
                self.table.c.knowledge_id.in_(keys)).with_for_update()).mappings().all() if keys else []
            by_id = {row["knowledge_id"]: dict(row) for row in existing}
            inserts = []
            for item in prepared:
                if item["knowledge_id"] in by_id:
                    self._same_record(by_id[item["knowledge_id"]], item)
                elif published:
                    raise KnowledgeStorageError("Published versions are immutable; use a new dataset_version")
                else:
                    item["publication_status"] = "staging"
                    inserts.append(item)
            if inserts:
                self._assert_write_connection(connection, version)
                connection.execute(self.table.insert(), inserts)
            self._assert_write_connection(connection, version)
        return len(inserts)

    def _verify_rows(self, rows, prepared):
        expected = {row["knowledge_id"]: row for row in prepared}
        actual = {row["knowledge_id"]: row for row in rows}
        if len(actual) != len(rows) or set(actual) != set(expected):
            raise KnowledgeStorageError("Version knowledge identity set differs from the complete manifest")
        for key, row in actual.items():
            self._same_record(row, expected[key])

    def verify(self, version, records):
        """Verify exact key set, hashes and every business field; return None."""
        version = _version(version)
        prepared = self._prepare(records, version)
        with self.engine.connect() as connection:
            self._verify_rows(self._read_rows(connection, version), prepared)

    def publish(self, version, records):
        """Verify the complete nonempty manifest and publish atomically.

        Return the number of staging rows changed (zero for an identical replay).
        """
        self.assert_lock()
        version = _version(version)
        prepared = self._prepare(records, version)
        if not prepared:
            raise KnowledgeStorageError("An empty manifest cannot publish a complete version")
        with self.engine.begin() as connection:
            self._assert_write_connection(connection, version)
            rows = self._read_rows(connection, version, for_update=True)
            self._verify_rows(rows, prepared)
            self._assert_write_connection(connection, version)
            changed = sum(row["publication_status"] == "staging" for row in rows)
            if changed:
                connection.execute(self.table.update().where(
                    self.table.c.dataset_version == version,
                    self.table.c.publication_status == "staging"
                ).values(publication_status="published", updated_at=datetime.now(_CST).replace(tzinfo=None)))
            self._assert_write_connection(connection, version)
        return changed
