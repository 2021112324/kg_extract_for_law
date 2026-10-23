"""Offline regression and current-input diagnostics, never production writes."""
# ruff: noqa: E402

import argparse
import asyncio
from collections import Counter
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime
import io
import json
import logging
import os
from pathlib import Path
import sys
import unittest
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT))
from app.infrastructure.risk_map.common.io import atomic_json, file_hash
from app.infrastructure.risk_map.common.stats import Diagnostics
from app.infrastructure.risk_map.preparation.config import DEFAULT_TREE_PATH
from app.infrastructure.risk_map.preparation.tree_index import build_tree_index, attach_provenance
from app.infrastructure.risk_map.matching.location import resolve_attachments, DocumentAmbiguity
from app.infrastructure.risk_map.preparation.document_identity import normalize_document_name

PACKAGE = Path(__file__).resolve().parents[1]


def inspect_inputs(output, *, baseline=False):
    paths = [DEFAULT_TREE_PATH, DEFAULT_TREE_PATH.with_name("条款知识来源明细.json"), DEFAULT_TREE_PATH.with_name("构建统计.json")]
    before = {p.name: file_hash(p) for p in paths}
    raw, provenance, _ = [json.loads(p.read_bytes().decode("utf-8-sig")) for p in paths]
    diagnostics = Diagnostics()
    index = build_tree_index(raw, diagnostics)
    leaves = []
    def visit(value):
        if not isinstance(value, dict):
            return
        if all(k in value for k in ("原始文本", "已找到", "知识依据", "依据文件名")):
            leaves.append(value)
        else:
            for child in value.values():
                visit(child)
    visit(raw)
    result = {"checked_at": datetime.now(ZoneInfo("Asia/Shanghai")).isoformat(), "baseline": baseline,
        "file_hashes": before, "indicators": len(index.indicators), "attachments": len(index.attachments),
        "alias_attachments": sum(bool(leaf.get("别名")) for leaf in leaves),
        "source_cells": len(provenance["来源单元格"]),
        "upstream_statuses": dict(Counter(r["状态"] for r in provenance["指标文件挂接"])),
        "database_writes": 0, "model_calls": 0}
    if not baseline:
        attach_provenance(index, provenance, diagnostics)
        result["verified_statuses"] = dict(Counter(a.source_verification.get("status", "unverified")
                                                   for a in index.attachments))
        result["unverified"] = [{"attachment_id": i, "path": a.tree_path, "detail": a.source_verification}
                                 for i, a in enumerate(index.attachments) if not a.source_verification["verified"]]
        result["alias_only_query_occurrences"] = Counter()
        queries = {}
        for a in index.attachments:
            for alias in a.aliases:
                query = (a.region, normalize_document_name(alias))
                if query not in queries:
                    try:
                        ids, _ = resolve_attachments(index, [alias], a.region)
                        queries[query] = "matched" if ids else "unmatched"
                    except DocumentAmbiguity:
                        queries[query] = "ambiguous"
                result["alias_only_query_occurrences"][queries[query]] += 1
        result["unique_region_alias_queries"] = dict(Counter(queries.values()))
        result["unique_query_key"] = "region + normalize_document_name(alias)"
        result["verified_attachments"] = sum(a.source_verification["verified"] for a in index.attachments)
        result["diagnostics"] = diagnostics.to_dict()
    result["inputs_unchanged"] = before == {p.name: file_hash(p) for p in paths}
    assert result["inputs_unchanged"]
    atomic_json(result, output / ("input_baseline.json" if baseline else "current_input_validation.json"))
    print(json.dumps({k: v for k, v in result.items() if k not in {"unverified", "diagnostics"}}, ensure_ascii=False))
    return result


def injected_flow(output):
    from app.infrastructure.graph_storage.subgraph_records import snapshot_hash
    from app.infrastructure.risk_map import MappingConfig, ModelConfig
    from app.infrastructure.risk_map.config import CANDIDATES, FINAL
    from app.infrastructure.risk_map.run_mapping import execute
    from app.infrastructure.risk_map.test.test_copy_first import Client, FakeAdapter
    from app.infrastructure.risk_map.test.test_scoped_aliases import named_tree, provenance_for

    data = named_tree(7, ["Example Act"], "企业应当实施专项风险识别。")
    tree_path, provenance_path = output / "synthetic_tree.json", output / "synthetic_provenance.json"
    atomic_json(data, tree_path)
    atomic_json(provenance_for(data), provenance_path)
    adapter = FakeAdapter(1)
    adapter.source["nodes"][0]["n"]["properties"]["filename"] = ["测试法.txt", "Example Act"]
    before = snapshot_hash(adapter.source)
    client = Client(adapter)
    config = MappingConfig("source", "alias_demo", tree_path=tree_path, provenance_path=provenance_path,
        output_root=output / "injected_flow", write_enabled=True, exclusive_target=True,
        model=ModelConfig(model="controlled-no-network", retry_delay=0))
    terminal = io.StringIO()
    with redirect_stderr(terminal):
        summary, code = asyncio.run(execute(config, adapter, client=client))
    props = adapter.target["nodes"][0]["n"]["properties"]
    assert code == 0 and len(props[CANDIDATES]) == len(props[FINAL]) == 7
    assert "三级指标映射状态" not in props and snapshot_hash(adapter.source) == before
    atomic_json(adapter.target, output / "injected_target_graph.json")
    (output / "injected_terminal.txt").write_text(terminal.getvalue(), encoding="utf-8")
    result = {"summary": summary, "code": code, "source_unchanged": True,
        "candidate_count": len(props[CANDIDATES]), "retained_count": len(props[FINAL]),
        "controlled_model_requests": client.calls, "real_model_requests": 0, "real_database_writes": 0,
        "semantic_accuracy_tested": False}
    atomic_json(result, output / "injected_flow_result.json")
    print(json.dumps({k: v for k, v in result.items() if k != "summary"}, ensure_ascii=False))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--baseline-only", action="store_true")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    previous = logging.root.manager.disable
    try:
        logging.disable(logging.CRITICAL)
        inspect_inputs(args.output, baseline=args.baseline_only)
    finally:
        logging.disable(previous)
    if args.baseline_only:
        return
    if os.getenv("RISK_MAP_LOCAL_NEO4J_TEST") == "1":
        raise ValueError("本入口仅运行离线回归；真实本地数据库测试请使用已有专用入口")
    suite = unittest.defaultTestLoader.discover(str(PACKAGE / "test"), "test_*.py", top_level_dir=str(ROOT))
    stream = io.StringIO()
    with redirect_stderr(stream), redirect_stdout(stream):
        result = unittest.TextTestRunner(stream=stream, verbosity=2).run(suite)
    (args.output / "offline_tests.txt").write_text(stream.getvalue(), encoding="utf-8")
    summary = {"tests": result.testsRun, "failures": len(result.failures), "errors": len(result.errors),
               "skipped": len(result.skipped), "success": result.wasSuccessful(),
               "real_model_calls": 0, "database_writes": 0}
    atomic_json(summary, args.output / "offline_test_summary.json")
    print(json.dumps(summary))
    if not result.wasSuccessful():
        print(stream.getvalue()[-12000:])
        raise SystemExit(1)
    injected_flow(args.output.resolve())


if __name__ == "__main__":
    main()
