"""Run-scoped progress and heartbeat logging, independent of the async loop."""

from contextvars import ContextVar
import logging
import threading
import time


logger = logging.getLogger("app.infrastructure.risk_map.progress")
_current = ContextVar("risk_map_progress", default=None)


class ExecutionMonitor:
    def __init__(self, *, action, source_mode, interval=60.0):
        self.action = action
        self.source_mode = source_mode
        self.interval = interval
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = None
        self._token = None
        self._started = time.monotonic()
        self._step_started = self._started
        self._step = "初始化"
        self._values = {}
        self._status = "运行中"

    def start(self):
        self._token = _current.set(self)
        self._thread = threading.Thread(target=self._heartbeat, name="risk-map-heartbeat", daemon=True)
        self._thread.start()
        logger.info("[执行开始] action=%s source_mode=%s 心跳间隔=%ss", self.action, self.source_mode, self.interval)
        return self

    def step(self, name, **values):
        now = time.monotonic()
        with self._lock:
            previous, elapsed = self._step, now - self._step_started
            self._step, self._step_started, self._values = name, now, dict(values)
        logger.info("[步骤切换] 上一步=%s 耗时=%.1fs 当前步骤=%s %s", previous, elapsed, name, self._details(values))

    def update(self, *, emit=False, **values):
        with self._lock:
            self._values.update(values)
            step, current = self._step, dict(self._values)
        if emit:
            logger.info("[当前进度] 步骤=%s %s", step, self._details(current))

    @staticmethod
    def _details(values):
        return " ".join(f"{key}={value}" for key, value in values.items()) or "进度=等待当前操作返回"

    def _heartbeat(self):
        while not self._stop.wait(self.interval):
            with self._lock:
                step, started, values = self._step, self._step_started, dict(self._values)
            now = time.monotonic()
            logger.info("[心跳] 状态=运行中 当前步骤=%s 总耗时=%.1fs 步骤耗时=%.1fs %s",
                        step, now - self._started, now - started, self._details(values))

    def result(self, status, code):
        self._status = f"{status} exit_code={code}"

    def close(self, error_type=None):
        self._stop.set()
        if self._thread is not None:
            self._thread.join()
        if self._token is not None:
            _current.reset(self._token)
            self._token = None
        status = f"异常或中断({error_type})" if error_type else self._status
        logger.info("[执行结束] 状态=%s 总耗时=%.1fs 最后步骤=%s", status, time.monotonic() - self._started, self._step)


def set_step(name, **values):
    monitor = _current.get()
    if monitor is not None:
        monitor.step(name, **values)


def update_progress(*, emit=False, **values):
    monitor = _current.get()
    if monitor is not None:
        monitor.update(emit=emit, **values)
