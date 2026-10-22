"""Heartbeat, runner logging and credential redaction without external connections."""

import asyncio
from contextlib import redirect_stderr
import io
import logging
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from app.infrastructure.risk_map import run_mapping as runner
from app.infrastructure.risk_map.common.progress import ExecutionMonitor, set_step, update_progress
from app.infrastructure.risk_map.test.test_copy_first import tree, FakeAdapter, Client
from app.infrastructure.risk_map.common.io import atomic_json
from app.infrastructure.risk_map.config import MappingConfig, ModelConfig


class HeartbeatTests(unittest.TestCase):
    def test_default_interval_and_finish(self):
        monitor = ExecutionMonitor(action="map", source_mode="neo4j")
        self.assertEqual(monitor.interval, 60)
        with self.assertLogs("app.infrastructure.risk_map", level="INFO") as capture:
            monitor.start()
            try:
                set_step("测试步骤", 总数=5)
                update_progress(emit=True, 已完成=2)
                monitor.result("completed_with_errors", 2)
            finally:
                monitor.close()
        text = "\n".join(capture.output)
        for marker in ("[执行开始]", "[步骤切换]", "总数=5 已完成=2", "completed_with_errors exit_code=2"):
            self.assertIn(marker, text)
        self.assertFalse(monitor._thread.is_alive())

    def test_heartbeat_runs_during_blocking_and_stops(self):
        emitted = threading.Event()
        class SignalHandler(logging.Handler):
            def emit(self, record):
                if "[心跳]" in record.getMessage():
                    emitted.set()
        log = logging.getLogger("app.infrastructure.risk_map.progress")
        handler = SignalHandler()
        log.addHandler(handler)
        try:
            with self.assertLogs("app.infrastructure.risk_map", level="INFO") as capture:
                monitor = ExecutionMonitor(action="map", source_mode="neo4j", interval=0.01).start()
                try:
                    set_step("等待数据库返回")
                    self.assertTrue(emitted.wait(2))
                finally:
                    monitor.close("CancelledError")
                count = len(capture.output)
                time.sleep(0.03)
                self.assertEqual(count, len(capture.output))
            self.assertIn("[心跳]", "\n".join(capture.output))
            self.assertFalse(monitor._thread.is_alive())
        finally:
            log.removeHandler(handler)

    def test_progress_without_monitor_no_thread(self):
        before = set(threading.enumerate())
        set_step("无监控")
        update_progress(emit=True, 已完成=1)
        self.assertEqual(before, set(threading.enumerate()))

    def test_formatter_redacts_password_key_and_bearer(self):
        formatter = runner.RedactedFormatter(["fixture-password", "fixture-api"])
        record = logging.LogRecord("test", logging.INFO, "", 0,
                                   "fixture-password fixture-api Bearer secret-value", (), None)
        text = formatter.format(record)
        self.assertNotIn("fixture-password", text)
        self.assertNotIn("fixture-api", text)
        self.assertNotIn("secret-value", text)


class RunnerLoggingTests(unittest.IsolatedAsyncioTestCase):
    async def test_cancel_stops_heartbeat_restores_logger(self):
        with tempfile.TemporaryDirectory() as folder:
            config = MappingConfig("source", "target", output_root=Path(folder))
            log = logging.getLogger("app.infrastructure.risk_map")
            before = list(log.handlers), log.level, log.propagate
            terminal = io.StringIO()
            with patch.object(runner, "execute_mapping", side_effect=asyncio.CancelledError()), redirect_stderr(terminal):
                with self.assertRaises(asyncio.CancelledError):
                    await runner.execute(config, FakeAdapter(0))
            self.assertIn("异常或中断", terminal.getvalue())
            self.assertEqual((list(log.handlers), log.level, log.propagate), before)
            self.assertFalse(any(t.name == "risk-map-heartbeat" for t in threading.enumerate()))

    async def test_terminal_and_file_mapping_steps(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            path = root / "tree.json"
            atomic_json(tree(), path)
            config = MappingConfig("source", "target", tree_path=path, output_root=root / "out",
                      write_enabled=True, exclusive_target=True, model=ModelConfig(retry_delay=0))
            adapter = FakeAdapter(1)
            terminal = io.StringIO()
            with redirect_stderr(terminal):
                result, code = await runner.execute(config, adapter, client=Client(adapter))
            self.assertEqual(code, 0)
            content = next((root / "out" / "logs").glob("*.log")).read_text(encoding="utf-8")
            for marker in ("[执行开始]", "[步骤切换]", "[当前进度]", "[基础图谱已保存]", "[结束摘要]", "[执行结束]"):
                self.assertIn(marker, content)
                self.assertIn(marker, terminal.getvalue())


if __name__ == "__main__":
    unittest.main()
