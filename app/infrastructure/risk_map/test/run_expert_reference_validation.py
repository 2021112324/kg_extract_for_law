"""Run isolated regression with live database and HTTP transports blocked."""

import argparse
from datetime import datetime
import os
from pathlib import Path
import platform
import sys
from unittest.mock import patch
import xml.etree.ElementTree as ET

REPO = next(p for p in Path(__file__).resolve().parents if (p / "app/infrastructure").is_dir())
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from app.infrastructure.risk_map.common.io import atomic_json
from app.infrastructure.risk_map.config import PROTOCOL


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    output = args.output or REPO / "app/infrastructure/risk_map/task/功能流程" / (
        "优化1102_测试证据_" + datetime.now().strftime("%Y%m%d_%H%M%S"))
    output = output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    os.chdir(REPO)
    import pytest

    junit = output / "pytest_results.xml"
    guards = {}
    blocked = ["app.infrastructure.graph_storage.neo4j_adapter.Neo4jAdapter.connect",
               "httpx.AsyncHTTPTransport.handle_async_request", "httpx.HTTPTransport.handle_request"]
    from contextlib import ExitStack
    with ExitStack() as stack:
        stack.enter_context(patch.dict(os.environ, {"RISK_MAP_LOCAL_NEO4J_TEST": "0"}))
        for name in blocked:
            guards[name] = stack.enter_context(patch(name, side_effect=AssertionError("Live I/O forbidden in isolated regression")))
        code = int(pytest.main(["app/infrastructure/risk_map/test", "-q", "--tb=short", "--show-capture=no",
                                "-p", "no:cacheprovider", "--junitxml=" + str(junit)]))
    suites = ET.parse(junit).getroot().findall("testsuite") if junit.exists() else []
    totals = {key: sum(int(s.get(key, "0")) for s in suites) for key in ("tests", "failures", "errors", "skipped")}
    totals["passed"] = totals["tests"] - totals["failures"] - totals["errors"] - totals["skipped"]
    attempts = {name: guard.call_count for name, guard in guards.items()}
    report = {"time": datetime.now().astimezone().isoformat(), "protocol": PROTOCOL,
              "python": platform.python_version(), "python_executable": sys.executable,
              "platform": platform.platform(), "exit_code": code, "results": totals,
              "live_io_guard_attempts": attempts, "live_database_used": False, "live_model_used": False,
              "local_neo4j_tests_enabled": False, "junit_report": str(junit),
              "scope": "injected adapters/models and temporary data; not real model accuracy or database integration"}
    atomic_json(report, output / "validation_summary.json", durable=True)
    print(output / "validation_summary.json")
    return code or int(any(attempts.values()))


if __name__ == "__main__":
    raise SystemExit(main())
