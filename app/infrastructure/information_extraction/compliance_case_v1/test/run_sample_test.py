"""Random sample test for compliance_case_v1 extraction.

默认调用 LLM，验证真实单阶段知识图谱抽取：
- 随机抽取“合规案例库”5 个文件；
- 随机抽取“合规风险案例”5 个文件；
- 执行单案例整体 LLM 图谱抽取；
- 将结果保存到 compliance_case_v1/result。

如只想快速验证解析和落盘：
1. 命令行增加 --no-llm；或
2. 将 DEFAULT_ENABLE_LLM 改为 False。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import sys
import time
from pathlib import Path
from typing import Any

# 允许直接运行本文件：
# python app/infrastructure/information_extraction/compliance_case_v1/test/run_sample_test.py
#
# 直接运行脚本时，Python 默认只把 test 目录加入 sys.path，无法找到顶层 app 包。
# 这里向上定位到项目根目录 kg_extract_for_law，并加入 sys.path。
PROJECT_ROOT = Path(__file__).resolve().parents[5]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.infrastructure.information_extraction.compliance_case_v1.graph_extract import (
    ComplianceCaseGraphExtractor,
)


BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
RESULT_DIR = BASE_DIR / "result"
CASE_LIBRARY_DIR = DATA_DIR / "合规案例库"
RISK_CASE_DIR = DATA_DIR / "合规风险案例"

SAMPLE_PER_CATEGORY = 20
DEFAULT_ENABLE_LLM = True


async def run_sample_test(
    sample_per_category: int = SAMPLE_PER_CATEGORY,
    output_dir: Path = RESULT_DIR,
    enable_llm: bool = DEFAULT_ENABLE_LLM,
    seed: int | None = None,
) -> dict[str, Any]:
    """执行随机抽样测试并落盘结果。"""

    output_dir.mkdir(parents=True, exist_ok=True)
    seed = int(seed if seed is not None else time.time())
    rng = random.Random(seed)

    case_library_samples = _sample_files(CASE_LIBRARY_DIR, "*.md", sample_per_category, rng)
    risk_case_samples = _sample_files(RISK_CASE_DIR, "*.txt", sample_per_category, rng)
    samples = [("合规案例库", p) for p in case_library_samples] + [("合规风险案例", p) for p in risk_case_samples]

    extractor = ComplianceCaseGraphExtractor(enable_llm=enable_llm)
    all_nodes: list[dict[str, Any]] = []
    all_edges: list[dict[str, Any]] = []
    all_case_knowledge: list[dict[str, Any]] = []
    items: list[dict[str, Any]] = []

    for category, file_path in samples:
        started = time.time()
        item_output = output_dir / f"{category}_{_safe_filename(file_path.stem)}.json"
        try:
            result = await extractor.extract(file_path)
            item_output.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
            graph = result.get("graph", {}) or {}
            all_nodes.extend(graph.get("nodes", []) or [])
            all_edges.extend(graph.get("edges", []) or [])
            if result.get("case_knowledge"):
                all_case_knowledge.append(result["case_knowledge"])
            items.append(
                {
                    "category": category,
                    "file": file_path.name,
                    "input_path": str(file_path),
                    "output_path": str(item_output),
                    "status": result.get("status"),
                    "case_id": result.get("case_id"),
                    "node_count": result.get("stats", {}).get("node_count", 0),
                    "edge_count": result.get("stats", {}).get("edge_count", 0),
                    "llm_call_count": result.get("stats", {}).get("llm_call_count", 0),
                    "elapsed_seconds": round(time.time() - started, 3),
                }
            )
        except Exception as exc:
            items.append(
                {
                    "category": category,
                    "file": file_path.name,
                    "input_path": str(file_path),
                    "status": "failed",
                    "error": str(exc),
                    "elapsed_seconds": round(time.time() - started, 3),
                }
            )

    summary = {
        "test_mode": "llm" if enable_llm else "parser_only",
        "seed": seed,
        "sample_per_category": sample_per_category,
        "total": len(samples),
        "success": sum(1 for item in items if item.get("status") in {"success", "parser_only"}),
        "failed": sum(1 for item in items if item.get("status") not in {"success", "parser_only"}),
        "node_count": len(all_nodes),
        "edge_count": len(all_edges),
        "case_knowledge_count": len(all_case_knowledge),
        "items": items,
    }

    (output_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    (output_dir / "node.json").write_text(json.dumps(all_nodes, ensure_ascii=False, indent=2), encoding="utf-8")
    (output_dir / "edge.json").write_text(json.dumps(all_edges, ensure_ascii=False, indent=2), encoding="utf-8")
    (output_dir / "case_knowledge.json").write_text(
        json.dumps(all_case_knowledge, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (output_dir / "sample_test_report.md").write_text(_build_markdown_report(summary), encoding="utf-8")
    return summary


def _sample_files(directory: Path, pattern: str, count: int, rng: random.Random) -> list[Path]:
    files = sorted(directory.glob(pattern))
    if len(files) <= count:
        return files
    return sorted(rng.sample(files, count))


def _build_markdown_report(summary: dict[str, Any]) -> str:
    lines = [
        "# compliance_case_v1 随机抽样测试报告",
        "",
        f"- 测试模式：{summary['test_mode']}",
        f"- 随机种子：{summary['seed']}",
        f"- 每类抽样数量：{summary['sample_per_category']}",
        f"- 总样本数：{summary['total']}",
        f"- 成功数：{summary['success']}",
        f"- 失败数：{summary['failed']}",
        f"- 汇总节点数：{summary['node_count']}",
        f"- 汇总关系数：{summary['edge_count']}",
        f"- case_knowledge 数量：{summary['case_knowledge_count']}",
        "",
        "## 样本明细",
        "",
        "| 类别 | 文件 | 状态 | 案例ID | 节点数 | 关系数 | LLM调用 | 耗时秒 |",
        "| --- | --- | --- | --- | ---: | ---: | ---: | ---: |",
    ]
    for item in summary["items"]:
        lines.append(
            "| {category} | {file} | {status} | {case_id} | {node_count} | {edge_count} | {llm_call_count} | {elapsed_seconds} |".format(
                category=item.get("category", ""),
                file=item.get("file", ""),
                status=item.get("status", ""),
                case_id=item.get("case_id", ""),
                node_count=item.get("node_count", 0),
                edge_count=item.get("edge_count", 0),
                llm_call_count=item.get("llm_call_count", 0),
                elapsed_seconds=item.get("elapsed_seconds", 0),
            )
        )
    lines.extend(
        [
            "",
            "## 说明",
            "",
            "- parser_only 模式只验证文件读取、结构整理、案例主节点构建、结果落盘和 case_knowledge 视图导出。",
            "- 默认模式会调用 LLM 验证真实知识图谱实体和关系抽取；如只想快速联调，请使用 `--no-llm`。",
        ]
    )
    return "\n".join(lines)


def _safe_filename(value: str) -> str:
    return "".join("_" if ch in '<>:"/\\|?*' else ch for ch in str(value)).strip() or "unnamed"


def main() -> None:
    parser = argparse.ArgumentParser(description="Run random sample test for compliance_case_v1.")
    parser.add_argument("--sample-per-category", type=int, default=SAMPLE_PER_CATEGORY)
    parser.add_argument("--output-dir", type=Path, default=RESULT_DIR)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--no-llm", action="store_true", help="Only run parser/context graph without calling LLM.")
    args = parser.parse_args()
    summary = asyncio.run(
        run_sample_test(
            sample_per_category=args.sample_per_category,
            output_dir=args.output_dir,
            enable_llm=not args.no_llm,
            seed=args.seed,
        )
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
