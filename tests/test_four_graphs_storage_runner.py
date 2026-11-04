"""Offline tests: this runner must never use a default database connection."""

import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from app.infrastructure.kg_to_natural_language import run_four_graphs_to_mysql as runner
from app.infrastructure.kg_to_natural_language.storage import pipeline
from app.infrastructure.kg_to_natural_language.storage.local_records import RunRecords
from app.infrastructure.kg_to_natural_language.storage.records import digest


@pytest.fixture
def config(tmp_path, monkeypatch):
    monkeypatch.setattr(runner, "OUTPUT_ROOT", tmp_path)
    monkeypatch.setattr(runner, "DATASET_VERSION", "offline_four_graphs_v1")
    return runner.build_config()


def test_four_sources_have_explicit_matching_converters(config):
    assert [(s.graph_label, s.converter) for s in config.sources] == [
        ("法律法规条款", "chinese_law"), ("国家标准", "national_standard"),
        ("合规指引", "guide"), ("行政监管规则", "chinese_law"),
    ]
    assert all(s.source_type == "neo4j" and s.graph_label == s.category for s in config.sources)
    assert runner.PROJECT_ROOT.joinpath("app").is_dir()


@pytest.mark.parametrize("version", ["5.6.51", "8.0.16", "10.11.6-MariaDB", "invalid"])
def test_reject_unsupported_mysql(version):
    with pytest.raises(ValueError, match="Requires MySQL"):
        runner.require_mysql8(version)


@pytest.mark.parametrize("version", ["8.0.17", "8.3.0", "8.4.0"])
def test_supported_mysql(version):
    runner.require_mysql8(version)


def test_url_handles_password_without_manual_url_encoding(monkeypatch):
    password = "example@:/#% not-a-real-secret"
    monkeypatch.setattr(runner, "MYSQL_DATABASE", "compliance_knowledge")
    url = runner.database_url(password)
    assert url.password == password
    assert url.database == "compliance_knowledge"
    assert url.port == runner.MYSQL_PORT
    assert runner.database_url(password, with_database=False).database is None


@pytest.mark.parametrize("database", ["x`;DROP DATABASE x", "../data", "", "x" * 65])
def test_database_identifiers_rejected_before_sql(monkeypatch, database):
    monkeypatch.setattr(runner, "MYSQL_DATABASE", database)
    with pytest.raises(ValueError, match="MYSQL_DATABASE"):
        runner.database_url("example")


def prepared_fixture(config, *, empty_label=None):
    records = RunRecords(config.output_root)
    records.write("manifest.json", {
        "dataset_version": config.dataset_version,
        "config_hash": digest(config.semantic()), "runtime_hash": pipeline.runtime_hash(),
    })
    records.write("inputs.json", [
        {"source": {"graph_label": s.graph_label},
         "nodes": [] if s.graph_label == empty_label else [{"id": "example"}]}
        for s in config.sources
    ])
    return records


def test_prepare_then_reuse_frozen_run(config, monkeypatch):
    control = RunRecords(config.output_root, "launcher")
    prepared = prepared_fixture(config)
    prepare = MagicMock(return_value={"status": "prepared", "record_directory": str(prepared.directory)})
    monkeypatch.setattr(pipeline, "prepare_knowledge", prepare)
    assert runner.prepared_directory(config, control) == prepared.directory
    assert runner.prepared_directory(config, control) == prepared.directory
    prepare.assert_called_once_with(config)


def test_changed_configuration_does_not_reuse_old_version(config, monkeypatch):
    control = RunRecords(config.output_root, "launcher")
    prepared = prepared_fixture(config)
    control.write("prepared_run.json", {"record_directory": str(prepared.directory)})
    config.industry_mapping = {"原行业": "制造业"}
    prepare = MagicMock()
    monkeypatch.setattr(pipeline, "prepare_knowledge", prepare)
    with pytest.raises(ValueError, match="DATASET_VERSION"):
        runner.prepared_directory(config, control)
    prepare.assert_not_called()


def test_pointer_outside_root_is_rejected(config, tmp_path):
    control = RunRecords(config.output_root, "launcher")
    control.write("prepared_run.json", {"record_directory": str(tmp_path / "elsewhere" / "run")})
    with pytest.raises(ValueError, match="not within"):
        runner.prepared_directory(config, control)


def test_missing_label_stops_import_and_does_not_save_pointer(config, monkeypatch):
    control = RunRecords(config.output_root, "launcher")
    prepared = prepared_fixture(config, empty_label="合规指引")
    monkeypatch.setattr(pipeline, "prepare_knowledge", MagicMock(return_value={
        "status": "prepared", "record_directory": str(prepared.directory),
    }))
    with pytest.raises(ValueError, match="合规指引"):
        runner.prepared_directory(config, control)
    assert not (control.directory / "prepared_run.json").exists()


def test_whole_runner_prepares_and_imports_without_manual_action_switch(config, monkeypatch):
    engine = MagicMock()
    repository = SimpleNamespace(engine=engine)
    monkeypatch.setattr(runner, "make_repository", MagicMock(return_value=repository))
    directory = RunRecords(config.output_root).directory
    monkeypatch.setattr(runner, "prepared_directory", MagicMock(return_value=directory))
    persist = MagicMock(return_value={"status": "published", "records": 4})
    monkeypatch.setattr(pipeline, "persist_knowledge", persist)
    assert runner.run("example")["status"] == "published"
    persist.assert_called_once_with(directory, repository=repository,
                                    initialize_schema=runner.INITIALIZE_TABLE)
    engine.dispose.assert_called_once()


def test_connection_failure_prevents_neo4j_read(config, monkeypatch):
    monkeypatch.setattr(runner, "make_repository", MagicMock(side_effect=ConnectionError("unavailable")))
    prepare = MagicMock()
    monkeypatch.setattr(runner, "prepared_directory", prepare)
    with pytest.raises(ConnectionError):
        runner.run("example")
    prepare.assert_not_called()


def test_old_mysql_is_rejected_before_create_database(monkeypatch):
    import sqlalchemy

    connection = MagicMock()
    connection.execute.return_value.scalar_one.return_value = "5.6.51"
    server = MagicMock()
    server.connect.return_value.__enter__.return_value = connection
    factory = MagicMock(return_value=server)
    monkeypatch.setattr(sqlalchemy, "create_engine", factory)
    monkeypatch.setattr(runner, "CREATE_DATABASE_IF_MISSING", True)
    with pytest.raises(ValueError, match="Requires MySQL"):
        runner.make_repository("example")
    assert connection.execute.call_count == 1
    assert str(connection.execute.call_args.args[0]) == "SELECT VERSION()"
    server.dispose.assert_called_once()


def test_failures_dispose_connection(config, monkeypatch):
    engine = MagicMock()
    monkeypatch.setattr(runner, "make_repository", MagicMock(return_value=SimpleNamespace(engine=engine)))
    monkeypatch.setattr(runner, "prepared_directory", MagicMock(side_effect=ValueError("bad source")))
    with pytest.raises(ValueError):
        runner.run("example")
    engine.dispose.assert_called_once()


def test_main_returns_failure_and_redacts_password(config, monkeypatch, caplog):
    password = "fake-secret-unique"
    monkeypatch.setenv(runner.MYSQL_PASSWORD_ENV, password)
    monkeypatch.setattr(runner, "run", MagicMock(side_effect=ValueError("password=" + password)))
    assert runner.main() == 2
    assert password not in caplog.text
    assert "请检查 MySQL" not in caplog.text


def test_cyclic_source_error_does_not_suggest_mysql_failure(config, monkeypatch, caplog):
    from app.infrastructure.kg_to_natural_language.storage.records import KnowledgeStorageError
    monkeypatch.setenv(runner.MYSQL_PASSWORD_ENV, "example")
    monkeypatch.setattr(runner, "run", MagicMock(side_effect=KnowledgeStorageError("Cyclic owning relationship")))
    assert runner.main() == 2
    assert "Cyclic owning" in caplog.text
    assert "请检查 MySQL" not in caplog.text


def test_actual_mysql_connection_error_has_relevant_hint(config, monkeypatch, caplog):
    from pymysql.err import OperationalError
    from sqlalchemy.exc import OperationalError as SQLAlchemyOperationalError
    monkeypatch.setenv(runner.MYSQL_PASSWORD_ENV, "example")
    error = SQLAlchemyOperationalError(None, None, OperationalError(2003, "connection refused"))
    monkeypatch.setattr(runner, "run", MagicMock(side_effect=error))
    assert runner.main() == 2
    assert "请检查 MySQL" in caplog.text


@pytest.mark.parametrize("status,expected", [("published", 0), ("already_published", 0), ("failed_or_unconfirmed", 2)])
def test_exit_code_reflects_publication(config, monkeypatch, status, expected, capsys):
    monkeypatch.setenv(runner.MYSQL_PASSWORD_ENV, "example")
    monkeypatch.setattr(runner, "run", MagicMock(return_value={"status": status}))
    assert runner.main() == expected
    assert json.loads(capsys.readouterr().out)["status"] == status
