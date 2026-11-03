"""Failure diagnostics with isolated adapters, transactions and temporary output only."""

from contextlib import redirect_stderr
from dataclasses import replace
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from app.infrastructure.graph_storage.diagnostics import exception_details, redact_diagnostics
from app.infrastructure.graph_storage.neo4j_adapter import Neo4jAdapter
from app.infrastructure.risk_map import run_mapping as runner
from app.infrastructure.risk_map.common.io import atomic_json, read_json
from app.infrastructure.risk_map.config import FINAL, MappingConfig, ModelConfig
from app.infrastructure.risk_map.local_records import RunRecords
from app.infrastructure.risk_map.test.test_copy_first import Client, FakeAdapter, tree


DB_SECRET = "fixture-db-password"
MODEL_SECRET = "fixture-model-key"


class DatabaseFailure(RuntimeError):
    code = "Neo.TransientError.Transaction.Terminated"
    gql_status = "50N42"


class StorageDiagnosticTests(unittest.TestCase):
    def setUp(self):
        self.adapter = object.__new__(Neo4jAdapter)
        self.adapter.database = "fixture_database"
        self.adapter.password = DB_SECRET
        self.adapter.driver = MagicMock()
        self.session, self.tx = MagicMock(), MagicMock()
        self.session.__enter__.return_value = self.session
        self.tx.__enter__.return_value = self.tx
        self.adapter.driver.session.return_value = self.session
        self.session.begin_transaction.return_value = self.tx
        self.tx.run.side_effect = [[{"id": "article-1"}], [{"id": "article-1"}]]
        self.rows = [{"id": "article-1", "properties": {FINAL: []}}]

    def failure(self):
        return DatabaseFailure("controlled password=" + DB_SECRET + " Bearer private-bearer-value")

    def check_failure(self, stage, expected_status="failed"):
        with self.assertLogs("app.infrastructure.graph_storage", level="INFO") as capture:
            result = self.adapter.update_subgraph_article_properties("fixture_target", self.rows, timeout=12)
        self.assertEqual(result["status"], expected_status)
        self.assertFalse(result["commit_confirmed"])
        self.assertEqual(result["failure_stage"], stage)
        self.assertEqual(result["row_count"], 1)
        self.assertEqual(result["timeout_seconds"], 12)
        self.assertIn("traceback", result)
        self.assertIn("[法条属性写入失败]", "\n".join(capture.output))
        serialized = json.dumps(result) + "\n".join(capture.output)
        self.assertNotIn(DB_SECRET, serialized)
        self.assertNotIn("private-bearer-value", serialized)
        self.assertLessEqual(self.tx.commit.call_count, 1)
        return result

    def test_session_creation_failure(self):
        self.adapter.driver.session.side_effect = self.failure()
        self.check_failure("open_session")
        self.tx.commit.assert_not_called()

    def test_transaction_creation_failure(self):
        self.session.begin_transaction.side_effect = self.failure()
        self.check_failure("begin_transaction")
        self.tx.commit.assert_not_called()

    def test_member_query_failure_preserves_neo4j_metadata(self):
        self.tx.run.side_effect = self.failure()
        result = self.check_failure("verify_members")
        self.assertEqual(result["error_type"], "DatabaseFailure")
        self.assertEqual(result["neo4j_code"], DatabaseFailure.code)
        self.assertEqual(result["neo4j_gql_status"], DatabaseFailure.gql_status)
        self.assertIn("DatabaseFailure", result["traceback"])
        self.assertFalse(result["commit_started"])

    def test_member_count_mismatch(self):
        self.tx.run.side_effect = [[]]
        result = self.check_failure("verify_members")
        self.assertEqual(result["error"], "mapping_member_mismatch")
        self.assertEqual(result["matched_count"], 0)
        self.assertEqual(result["missing_member_ids"], ["article-1"])
        self.tx.commit.assert_not_called()

    def test_property_update_count_mismatch_identifies_missing_node(self):
        self.tx.run.side_effect = [[{"id": "article-1"}], []]
        result = self.check_failure("update_properties")
        self.assertEqual(result["error"], "mapping_update_incomplete")
        self.assertEqual(result["missing_updated_ids"], ["article-1"])
        self.tx.commit.assert_not_called()

    def test_property_query_failure(self):
        self.tx.run.side_effect = [[{"id": "article-1"}], self.failure()]
        result = self.check_failure("update_properties")
        self.assertEqual(result["matched_count"], 1)
        self.tx.commit.assert_not_called()

    def test_commit_failure_is_not_retried(self):
        self.tx.commit.side_effect = self.failure()
        result = self.check_failure("commit", "commit_unknown")
        self.assertTrue(result["commit_started"])
        self.assertEqual(result["updated_count"], 1)
        self.tx.commit.assert_called_once()

    def test_cleanup_failure_does_not_invalidate_acknowledged_commit(self):
        self.session.__exit__.side_effect = self.failure()
        with self.assertLogs("app.infrastructure.graph_storage", level="INFO"):
            result = self.adapter.update_subgraph_article_properties("fixture_target", self.rows)
        self.assertEqual(result["status"], "committed")
        self.assertTrue(result["commit_confirmed"])
        self.assertTrue(result["cleanup_error"])
        self.assertEqual(result["cleanup_diagnostic"]["failure_stage"], "cleanup")
        self.assertNotIn(DB_SECRET, json.dumps(result))
        self.tx.commit.assert_called_once()

    def test_success_keeps_original_receipt_contract(self):
        with self.assertLogs("app.infrastructure.graph_storage", level="INFO") as capture:
            result = self.adapter.update_subgraph_article_properties("fixture_target", self.rows)
        self.assertEqual(result, {"status": "committed", "commit_confirmed": True, "ids": ["article-1"]})
        text = "\n".join(capture.output)
        self.assertIn("[法条属性准备提交]", text)
        self.assertIn("[法条属性提交已确认]", text)

    def test_exception_chain_and_redaction(self):
        try:
            try:
                raise OSError("password=" + DB_SECRET)
            except OSError as exc:
                raise DatabaseFailure("api_key=" + MODEL_SECRET) from exc
        except DatabaseFailure as exc:
            result = exception_details(exc, (DB_SECRET, MODEL_SECRET))
        self.assertIn("OSError", result["traceback"])
        self.assertIn("DatabaseFailure", result["traceback"])
        self.assertNotIn(DB_SECRET, json.dumps(result))
        self.assertNotIn(MODEL_SECRET, json.dumps(result))
        self.assertEqual(redact_diagnostics({"password": "hidden", "nested": ["bolt://user:secret@localhost"]}),
                         {"password": "[REDACTED]", "nested": ["bolt://[REDACTED]@localhost"]})


class PipelineDiagnosticTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.root = Path(self.folder.name)
        self.tree_path = self.root / "tree.json"
        atomic_json(tree(), self.tree_path)
        self.config = MappingConfig("source", "target", tree_path=self.tree_path, output_root=self.root / "out",
            write_enabled=True, exclusive_target=True,
            model=ModelConfig(model="fixture", api_key=MODEL_SECRET, retry_delay=0))
        self.adapter = FakeAdapter(1)
        self.adapter.password = DB_SECRET
        self.client = Client(self.adapter)

    def tearDown(self):
        self.folder.cleanup()

    async def run_case(self):
        terminal = io.StringIO()
        with redirect_stderr(terminal):
            summary, code = await runner.execute(self.config, self.adapter, client=self.client)
        path = next((self.config.output_root / "logs").glob("*.log"))
        return summary, code, path.read_text(encoding="utf-8"), terminal.getvalue()

    async def test_failure_receipt_persisted_and_no_write_replay(self):
        receipt = {"status": "commit_unknown", "commit_confirmed": False,
            "failure_stage": "commit", "commit_started": True, "error_type": "ServiceUnavailable",
            "error": "connection failed " + DB_SECRET + " " + MODEL_SECRET,
            "traceback": "controlled diagnostic trace " + DB_SECRET}
        with patch.object(self.adapter, "update_subgraph_article_properties", return_value=receipt) as update:
            summary, code, log, terminal = await self.run_case()
            update.assert_called_once()
        self.assertEqual(code, 2)
        self.assertEqual(summary["mapping_status"], "write_unknown")
        self.assertEqual(summary["diagnostics"]["run_errors"], 1)
        self.assertEqual(summary["pending_batches"], 1)
        detail = summary["write_failure"]
        self.assertEqual(detail["batch"], "00000000_initialization")
        self.assertEqual(detail["receipt"]["failure_stage"], "commit")
        folder = Path(summary["record_directory"])
        saved = read_json(folder / detail["diagnostic_file"])
        self.assertEqual(saved, detail)
        self.assertFalse((folder / "checkpoints/00000000_initialization_ack.json").exists())
        events = (folder / "failures/events.jsonl").read_text(encoding="utf-8")
        for text in (log, terminal, events, json.dumps(summary), json.dumps(saved)):
            self.assertNotIn(DB_SECRET, text)
            self.assertNotIn(MODEL_SECRET, text)
        for marker in ("ServiceUnavailable", "stage=commit", summary["run_id"], "[映射批次写入开始]", "[故障详情]"):
            self.assertIn(marker, log)
        self.assertEqual(self.client.calls, 0)

    async def test_adapter_exception_has_stack_and_is_retained(self):
        with patch.object(self.adapter, "update_subgraph_article_properties", side_effect=OSError("controlled socket error")):
            summary, code, log, _ = await self.run_case()
        self.assertEqual(code, 2)
        receipt = summary["write_failure"]["receipt"]
        self.assertEqual(receipt["status"], "exception")
        self.assertEqual(receipt["failure_stage"], "adapter_call")
        self.assertIn("OSError", receipt["traceback"])
        self.assertIn("controlled socket error", log)

    async def test_invalid_adapter_receipt_is_recorded_without_replay(self):
        with patch.object(self.adapter, "update_subgraph_article_properties", return_value=None) as update:
            summary, code, _, _ = await self.run_case()
        self.assertEqual(code, 2)
        self.assertEqual(summary["write_failure"]["receipt"]["status"], "invalid_receipt")
        self.assertEqual(summary["write_failure"]["receipt"]["error_type"], "NoneType")
        update.assert_called_once()

    async def test_failed_before_commit_not_misreported_as_success(self):
        receipt = {"status": "failed", "commit_confirmed": False, "commit_started": False,
                   "failure_stage": "verify_members", "error_type": "SubgraphError", "error": "mapping_member_mismatch"}
        with patch.object(self.adapter, "update_subgraph_article_properties", return_value=receipt):
            summary, code, log, _ = await self.run_case()
        self.assertEqual(code, 2)
        self.assertFalse(summary["write_failure"]["receipt"]["commit_started"])
        self.assertIn("receipt_status=failed", summary["error"])
        self.assertIn("mapping_member_mismatch", log)

    async def test_successful_commit_and_failed_local_ack_are_distinguished(self):
        with patch.object(RunRecords, "acknowledge", side_effect=OSError("controlled disk error")):
            summary, code, log, _ = await self.run_case()
        self.assertEqual(code, 2)
        detail = summary["write_failure"]
        self.assertEqual(detail["event_type"], "acknowledgement_persistence_failure")
        self.assertTrue(detail["receipt"]["commit_confirmed"])
        self.assertEqual(detail["failure_stage"], "save_acknowledgement")
        self.assertIn("controlled disk error", log)
        self.assertEqual(len([c for c in self.adapter.calls if isinstance(c, tuple) and c[0] == "update"]), 1)

    async def test_failure_file_disk_error_does_not_hide_original_database_error(self):
        original = RunRecords.save

        def save(records, name, value):
            if name.endswith("_failure.json"):
                raise OSError("controlled diagnostic disk error")
            return original(records, name, value)

        receipt = {"status": "failed", "commit_confirmed": False, "error": "original database error"}
        with patch.object(RunRecords, "save", save), patch.object(self.adapter, "update_subgraph_article_properties", return_value=receipt):
            summary, code, log, _ = await self.run_case()
        self.assertEqual(code, 2)
        self.assertIn("original database error", log)
        self.assertIn("controlled diagnostic disk error", log)
        self.assertEqual(summary["write_failure"]["receipt"]["error"], "original database error")
        self.assertEqual(summary["diagnostic_record_errors"][0]["destination"], "file")
        self.assertIn("original database error", (Path(summary["record_directory"]) / "failures/events.jsonl").read_text(encoding="utf-8"))

    async def test_unexpected_read_failure_saved_separately(self):
        with patch.object(self.adapter, "read_subgraph_records", side_effect=OSError("controlled read failure")):
            summary, code, log, _ = await self.run_case()
        self.assertNotEqual(code, 0)
        self.assertEqual(summary["error_type"], "OSError")
        saved = read_json(Path(summary["record_directory"]) / "run_failure.json")
        self.assertIn("controlled read failure", saved["traceback"])
        self.assertIn("[运行异常堆栈]", log)

    async def test_success_logs_and_local_ack_cleanup_diagnostics_are_redacted(self):
        original = self.adapter.update_subgraph_article_properties

        def update(*args, **kwargs):
            return {**original(*args, **kwargs), "cleanup_error": True,
                    "cleanup_diagnostic": {"error": DB_SECRET + " " + MODEL_SECRET}}

        with patch.object(self.adapter, "update_subgraph_article_properties", update):
            summary, code, log, _ = await self.run_case()
        self.assertEqual(code, 0)
        self.assertIn("[映射批次写入已确认]", log)
        self.assertIn("[运行日志]", log)
        for path in (Path(summary["record_directory"]) / "checkpoints").glob("*_ack.json"):
            text = path.read_text(encoding="utf-8")
            self.assertNotIn(DB_SECRET, text)
            self.assertNotIn(MODEL_SECRET, text)

    async def test_wrapper_exception_is_logged_and_handlers_restored(self):
        config = replace(self.config, output_root=self.root / "wrapper")
        terminal = io.StringIO()
        with patch.object(runner, "execute_mapping", side_effect=RuntimeError(MODEL_SECRET)), redirect_stderr(terminal):
            with self.assertRaises(RuntimeError):
                await runner.execute(config, self.adapter)
        text = next((config.output_root / "logs").glob("*.log")).read_text(encoding="utf-8")
        self.assertIn("[执行入口异常]", text)
        self.assertIn("RuntimeError", text)
        self.assertNotIn(MODEL_SECRET, text)


if __name__ == "__main__":
    unittest.main()
