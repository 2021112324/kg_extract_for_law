"""Read-only comparison against the committed pre-fix ledger reader."""
import ast
import gc
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import tracemalloc

REPO = next(p for p in Path(__file__).resolve().parents if (p/"app/infrastructure").is_dir())
sys.path.insert(0, str(REPO))
from app.infrastructure.risk_map.common.io import atomic_json, content_hash, read_json
from app.infrastructure.risk_map.config import PROTOCOL
from app.infrastructure.risk_map.local_records import RunRecords

OUTPUT = REPO/"app/infrastructure/risk_map/task/测试报告/MemoryError修复_20261028"


def measure(function):
    gc.collect()
    tracemalloc.start()
    start = time.perf_counter()
    confirmed, pending, patches = function()
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return {"elapsed_seconds": round(time.perf_counter()-start, 3), "retained_mib": round(current/2**20, 3),
            "peak_mib": round(peak/2**20, 3), "confirmed_counts": {k: len(v) for k, v in confirmed.items()},
            "pending_batches": len(pending), "result_hash": content_hash(confirmed)}


def main():
    path = "app/infrastructure/risk_map/local_records.py"
    old = subprocess.run(["git", "show", "HEAD:"+path], cwd=REPO, capture_output=True, check=True).stdout.decode("utf-8")
    klass = next(n for n in ast.parse(old).body if isinstance(n, ast.ClassDef) and n.name == "RunRecords")
    method = next(n for n in klass.body if isinstance(n, ast.FunctionDef) and n.name == "batches")
    namespace = {"read_json": read_json, "content_hash": content_hash, "PROTOCOL": PROTOCOL}
    exec(compile(ast.Module(body=[method], type_ignores=[]), "<committed batches>", "exec"), namespace)
    root = REPO/"app/infrastructure/risk_map/output/test/output/copy_first"
    rows, skipped = [], []
    for folder in sorted((root/"runs").iterdir()):
        if not (folder/"run.json").is_file():
            continue
        records = RunRecords(root, folder.name, resume=True)
        try:
            before = measure(lambda: namespace["batches"](records))
        except ValueError as exc:
            tracemalloc.stop()
            skipped.append({"run_id": folder.name, "reason": str(exc)})
            continue
        after = measure(lambda: records.batches(include_patches=False))
        assert before["result_hash"] == after["result_hash"] and before["pending_batches"] == after["pending_batches"]
        rows.append({"run_id": folder.name, "committed_reader": before, "lean_reader": after,
                     "retained_reduction_percent": round(100*(1-after["retained_mib"]/before["retained_mib"]), 2) if before["retained_mib"] else 0})
        print(folder.name, "retained MiB", before["retained_mib"], "->", after["retained_mib"])
    OUTPUT.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="memory_fixture_", dir=OUTPUT) as directory:
        folder = Path(directory).resolve()
        assert folder.is_relative_to(OUTPUT.resolve())
        records = RunRecords(folder)
        for phase in ("initialization", "matching", "review"):
            for start in range(0, 200, 20):
                batch_rows, results = [], []
                for i in range(start, start+20):
                    text = "indicator_"+str(i)+"_"+"x"*16384
                    properties = {"三级指标映射候选列表": None} if phase == "initialization" else {
                        "三级指标映射候选列表" if phase == "matching" else "三级指标映射列表": [text]}
                    batch_rows.append({"id": str(i), "properties": properties})
                    results.append({"id": str(i), "properties": properties, "status": phase})
                key, digest = records.intent(phase, batch_rows, results)
                records.acknowledge(key, digest, {"status": "committed", "ids": [r["id"] for r in batch_rows]})
        before = measure(lambda: namespace["batches"](records))
        after = measure(lambda: records.batches(include_patches=False))
        assert before["result_hash"] == after["result_hash"]
        synthetic = {"articles": 200, "phases": 3, "batches": 30, "committed_reader": before, "lean_reader": after,
                     "retained_reduction_percent": round(100*(1-after["retained_mib"]/before["retained_mib"]), 2)}
        print("synthetic retained MiB", before["retained_mib"], "->", after["retained_mib"])
    result = {"records_unchanged": True, "database_used": False, "results_equal": True, "runs": rows,
              "skipped_historical_runs": skipped, "synthetic": synthetic,
              "failed_run_included": any(r["run_id"] == "9ce9ad324a6c4a3aa833a6fc6095ff79" for r in rows),
              "measurement_scope": "Python traced ledger allocations only; not whole-process RSS or the other computer"}
    atomic_json(result, OUTPUT/"历史记录内存对照.json")


if __name__ == "__main__":
    main()
