"""Edit the configuration below, then run this file directly. Neo4j graphs only."""

import asyncio
from datetime import datetime
import json
import logging
import os
from pathlib import Path
import sys
from urllib.parse import urlsplit
import uuid

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.infrastructure.graph_storage.factory import GraphStorageFactory
from app.infrastructure.graph_storage.diagnostics import exception_details, redact_diagnostics
from app.infrastructure.risk_map import MappingConfig, ModelConfig, execute_mapping
from app.infrastructure.risk_map.common.model_client import redact
from app.infrastructure.risk_map.common.progress import ExecutionMonitor
from app.infrastructure.risk_map.preparation.config import DEFAULT_TREE_PATH


def local_credential(name):
    path = SCRIPT_DIR / "credentials.local.json"
    return json.loads(path.read_text(encoding="utf-8")).get(name, "") if path.is_file() else ""


# ===================== EDIT CONFIGURATION HERE =====================
# map：新建完整副本后映射；resume：恢复本次新协议运行；inspect：只读核验；prepare：只读预检。
ACTION = "map"
NEO4J_URI = "bolt://39.96.198.93:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = os.getenv("RISK_MAP_NEO4J_PASSWORD") or local_credential("NEO4J_PASSWORD")
NEO4J_DATABASE = "neo4j"
SOURCE_GRAPH_LABEL = "行政监管规则"
TARGET_GRAPH_LABEL = "行政监管规则_指标映射"
NEO4J_WRITE_ENABLED = True
TARGET_EXCLUSIVE_WRITE_CONFIRMED = True  # 操作确认，不是分布式锁；运行期间不要修改源或目标。
TREE_PATH = DEFAULT_TREE_PATH
PROVENANCE_PATH = None  # 可选：主树同目录的“条款知识来源明细.json”；None不自动读取附件。
REGION = "中国"
OUTPUT_ROOT = SCRIPT_DIR / "output" / "copy_first"
RUN_ID = None  # map时None自动生成；resume/inspect填写本次copy_first摘要中的run_id。
MODEL_NAME = "glm-5.3-flash"
MODEL_API_URL = "http://1.62.254.162:20001/v1/chat/completions"
API_KEY = os.getenv("RISK_MAP_API_KEY") or local_credential("API_KEY")
MODEL_OPTIONS = {"request_timeout": 300, "max_attempts": 5, "retry_delay": 1,
                 "max_input_chars": 60000, "max_tokens": 4096}
NEO4J_OPTIONS = {"fetch_size": 1000, "max_nodes": 50000, "max_relationships": 200000,
                 "max_bytes": 536870912, "timeout": 600}
DRIVER_OPTIONS = {"connection_timeout": 30, "max_connection_pool_size": 20,
                  "max_transaction_retry_time": 30}
# 法条每批固定20条并发，定位后直接保存全部专家关联，不进行第二轮语义审查。
# 旧审查run不能resume；重新映射须使用未占用目标标签，新协议自身支持断点恢复。
# ==================== END OF CONFIGURATION =========================


class RedactedFormatter(logging.Formatter):
    def __init__(self, secrets):
        super().__init__("%(asctime)s %(levelname)s %(name)s %(message)s")
        self.secrets = tuple(value for value in secrets if value)

    def format(self, record):
        value = super().format(record)
        return redact_diagnostics(redact(value), self.secrets)


def build_config():
    uri = urlsplit(NEO4J_URI)
    if uri.scheme not in {"bolt", "neo4j", "bolt+s", "neo4j+s", "bolt+ssc", "neo4j+ssc"} or not uri.hostname or uri.username or uri.password:
        raise ValueError("Neo4j地址不合法，认证信息应单独配置")
    if not (NEO4J_USER and NEO4J_PASSWORD and NEO4J_DATABASE):
        raise ValueError("Neo4j连接配置不完整")
    model = ModelConfig(model=MODEL_NAME, api_url=MODEL_API_URL, api_key=API_KEY, **MODEL_OPTIONS)
    if ACTION in {"map", "resume"}:
        url = urlsplit(MODEL_API_URL)
        if (not MODEL_NAME or not API_KEY or url.scheme not in {"http", "https"}
                or not url.hostname or url.username or url.password or url.query or url.fragment):
            raise ValueError("模型名称、接口或密钥配置不完整/不合法")
    config = MappingConfig(SOURCE_GRAPH_LABEL, TARGET_GRAPH_LABEL, tree_path=Path(TREE_PATH),
        provenance_path=Path(PROVENANCE_PATH) if PROVENANCE_PATH is not None else None,
        output_root=Path(OUTPUT_ROOT), region=REGION, model=model,
        write_enabled=NEO4J_WRITE_ENABLED, exclusive_target=TARGET_EXCLUSIVE_WRITE_CONFIRMED,
        read_options=NEO4J_OPTIONS)
    config.validate(writing=ACTION in {"map", "resume"})
    return config


async def execute(config, adapter, *, action="map", run_id=None, client=None):
    """Logging wrapper also usable by isolated/local tests without global connection settings."""
    root = Path(config.output_root).resolve()
    folder = root / "logs"
    folder.mkdir(parents=True, exist_ok=True)
    log = logging.getLogger("app.infrastructure.risk_map")
    storage_log = logging.getLogger("app.infrastructure.graph_storage")
    log_path = folder / (datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:8] + ".log")
    handlers = [logging.StreamHandler(), logging.FileHandler(log_path, encoding="utf-8")]
    previous = [(item, item.level, item.propagate) for item in (log, storage_log)]
    for handler in handlers:
        handler.setFormatter(RedactedFormatter([config.model.api_key, getattr(adapter, "password", "")]))
    for item, _, _ in previous:
        item.setLevel(logging.INFO)
        item.propagate = False
        for handler in handlers:
            item.addHandler(handler)
    monitor = ExecutionMonitor(action=action, source_mode="neo4j", interval=60).start()
    try:
        log.info("[运行日志] path=%s pid=%s action=%s source=%s target=%s database=%s run_id=%s",
                 log_path, os.getpid(), action, config.source_label, config.target_label, adapter.database,
                 run_id or "new")
        summary, code = await execute_mapping(config, adapter, action=action, run_id=run_id, client=client)
        monitor.result(summary.get("status", "unknown"), code)
        return summary, code
    except (Exception, asyncio.CancelledError) as exc:
        log.error("[执行入口异常] %s", json.dumps(exception_details(exc,
            (config.model.api_key, getattr(adapter, "password", ""))), ensure_ascii=False))
        raise
    finally:
        monitor.close(sys.exc_info()[0].__name__ if sys.exc_info()[0] else None)
        for item, level, propagate in previous:
            for handler in handlers:
                item.removeHandler(handler)
            item.setLevel(level)
            item.propagate = propagate
        for handler in handlers:
            handler.close()


def main():
    adapter = None
    try:
        config = build_config()
        adapter = GraphStorageFactory.create("neo4j", {"uri": NEO4J_URI, "username": NEO4J_USER,
            "password": NEO4J_PASSWORD, "database": NEO4J_DATABASE, "driver_options": DRIVER_OPTIONS})
        if not adapter.connect():
            raise ConnectionError("Neo4j连接失败")
        adapter.verify_subgraph_support()
        summary, code = asyncio.run(execute(config, adapter, action=ACTION, run_id=RUN_ID))
        print(json.dumps(redact(summary, API_KEY), ensure_ascii=False, indent=2))
        return code
    except (ValueError, TypeError, OSError) as exc:
        message = redact(str(exc), API_KEY).replace(NEO4J_PASSWORD, "[REDACTED]") if NEO4J_PASSWORD else redact(str(exc), API_KEY)
        print(json.dumps({"status": "input_error", "error": message}, ensure_ascii=False))
        return 64
    except KeyboardInterrupt:
        print("执行已中断。已提交的基础图谱不会删除，请查看copy_first运行摘要及未决写入。")
        return 130
    except Exception as exc:
        print(json.dumps({"status": "failed", **exception_details(exc, (API_KEY, NEO4J_PASSWORD))}, ensure_ascii=False))
        return 1
    finally:
        if adapter:
            adapter.disconnect()


if __name__ == "__main__":
    raise SystemExit(main())
