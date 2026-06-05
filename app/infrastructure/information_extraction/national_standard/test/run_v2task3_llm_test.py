r"""国家标准二阶段 LLM 图谱抽取测试脚本。

默认测试对象：
    D:\CogmAIT\8.1项目\8.1数据\爬取的数据\风险法规\第一版\分类(3)(1)\中文解析后
    \国家标准\产品法律风险 目录下前 10 份完整标准数据

默认行为：
    - 抽取产品法律风险目录下前 10 份完整标准数据。
    - 不限制进入 LLM 的树节点数量。
    - 结果保存到 national_standard/result/graph_llm_test/。

常用运行方式：
    E:\Anaconda3\envs\cogmait312\python.exe app\infrastructure\information_extraction\national_standard\test\run_v2task3_llm_test.py
    E:\Anaconda3\envs\cogmait312\python.exe app\infrastructure\information_extraction\national_standard\test\run_v2task3_llm_test.py --max-tree-nodes 1
    E:\Anaconda3\envs\cogmait312\python.exe app\infrastructure\information_extraction\national_standard\test\run_v2task3_llm_test.py --limit 3
    E:\Anaconda3\envs\cogmait312\python.exe app\infrastructure\information_extraction\national_standard\test\run_v2task3_llm_test.py --timeout 600 --max-concurrent 1 --max-workers 1 --batch-length 1 --max-char-buffer 4000
    E:\Anaconda3\envs\cogmait312\python.exe app\infrastructure\information_extraction\national_standard\test\run_v2task3_llm_test.py --input "D:\...\某个风险分类目录"
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import traceback
from pathlib import Path
from typing import Any


PRODUCT_LEGAL_RISK_ROOT_DIR = Path(
    r"D:\CogmAIT\8.1项目\8.1数据\爬取的数据\风险法规\第一版\分类(3)(1)\中文解析后"
    r"\国家标准\产品法律风险"
)
DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parents[1] / "result" / "graph_llm_test"
DEFAULT_SUMMARY_FILE = "product_legal_risk_10_summary.json"


def _setup_runtime() -> None:
    """设置脚本运行环境，确保从任意工作目录执行都能导入项目模块。"""
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")
    os.environ.setdefault("NATIONAL_STANDARD_GRAPH_TIMEOUT", "600")

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8")

    current = Path(__file__).resolve()
    repo_root = next(
        (parent for parent in current.parents if (parent / "app").exists()),
        current.parents[5],
    )
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="以产品法律风险目录下的 10 份完整国家标准数据测试二阶段 LLM 图谱抽取。"
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=PRODUCT_LEGAL_RISK_ROOT_DIR,
        help="国家标准风险分类目录或单份标准目录。默认使用产品法律风险目录。",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="测试结果 JSON 输出目录。",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="最多测试多少份标准数据。默认 10。",
    )
    parser.add_argument(
        "--max-tree-nodes",
        type=int,
        default=None,
        help=(
            "限制进入 LLM 的正文/附录树节点数量。默认不限制，即完整抽取。"
            "如果只想快速验证链路，可设置为 1。"
        ),
    )
    parser.add_argument(
        "--timeout",
        type=str,
        default=os.environ.get("NATIONAL_STANDARD_GRAPH_TIMEOUT", "600"),
        help="单次 LLM 请求超时时间，单位秒。默认 600。",
    )
    parser.add_argument(
        "--max-concurrent",
        type=int,
        default=1,
        help="二阶段节点级 LLM 并发数。长文本批量测试建议为 1。",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=1,
        help="LangExtract 内部最大工作线程数。长文本批量测试建议为 1。",
    )
    parser.add_argument(
        "--batch-length",
        type=int,
        default=1,
        help="LangExtract 批处理长度。长文本批量测试建议为 1。",
    )
    parser.add_argument(
        "--max-char-buffer",
        type=int,
        default=4000,
        help="LangExtract 单块最大字符数。长文本超时时可调小，例如 3000 或 4000。",
    )
    return parser.parse_args()


def _print_summary(result: dict[str, Any], output_path: Path) -> None:
    print("input_path:", result.get("input_path", ""))
    print("output_path:", output_path)
    print("status:", result.get("status"))
    print("extraction_mode:", result.get("extraction_mode"))
    print("stats:", json.dumps(result.get("stats", {}), ensure_ascii=False))
    if result.get("status") != "success":
        print("error:", result.get("error") or result.get("message") or "")


def _validate_paths(input_path: Path, output_dir: Path) -> None:
    if not input_path.exists():
        raise FileNotFoundError(f"测试输入路径不存在: {input_path}")
    if not input_path.is_dir():
        raise NotADirectoryError(f"测试输入必须是目录: {input_path}")
    output_dir.mkdir(parents=True, exist_ok=True)


def _discover_standard_dirs(input_path: Path, limit: int) -> list[Path]:
    """发现待测试的标准目录。若 input_path 本身含 full.md，则只测试该目录。"""
    if (input_path / "full.md").exists():
        return [input_path]
    dirs = [
        path
        for path in sorted(input_path.iterdir(), key=lambda item: item.name)
        if path.is_dir() and (path / "full.md").exists()
    ]
    return dirs[: max(limit, 0)]


def _safe_output_name(name: str) -> str:
    safe = "".join("_" if char in r'\/:*?"<>|' else char for char in name).strip()
    return safe or "未命名标准"


async def _run() -> int:
    _setup_runtime()
    args = _parse_args()
    os.environ["NATIONAL_STANDARD_GRAPH_TIMEOUT"] = str(args.timeout)
    os.environ["NATIONAL_STANDARD_GRAPH_MAX_WORKERS"] = str(args.max_workers)
    os.environ["NATIONAL_STANDARD_GRAPH_BATCH_LENGTH"] = str(args.batch_length)
    os.environ["NATIONAL_STANDARD_GRAPH_MAX_CHAR_BUFFER"] = str(args.max_char_buffer)

    input_path = args.input
    output_dir = args.output_dir
    _validate_paths(input_path, output_dir)
    standard_dirs = _discover_standard_dirs(input_path, args.limit)
    if not standard_dirs:
        raise FileNotFoundError(f"未在输入目录下发现包含 full.md 的标准目录: {input_path}")

    from app.infrastructure.information_extraction.national_standard.graph_extract import (
        NationalStandardGraphExtractor,
    )

    print("开始测试国家标准二阶段 LLM 图谱抽取")
    print("测试输入:", input_path)
    print("测试数量:", len(standard_dirs))
    print("输出目录:", output_dir)
    print("树节点限制:", args.max_tree_nodes if args.max_tree_nodes is not None else "不限制，完整抽取")
    print("LLM timeout:", args.timeout)
    print("LLM max_concurrent:", args.max_concurrent)
    print("LangExtract max_workers:", args.max_workers)
    print("LangExtract batch_length:", args.batch_length)
    print("LangExtract max_char_buffer:", args.max_char_buffer)

    extractor = NationalStandardGraphExtractor(
        max_concurrent=args.max_concurrent,
        max_tree_nodes=args.max_tree_nodes,
    )
    summary: dict[str, Any] = {
        "input": str(input_path),
        "output_dir": str(output_dir),
        "limit": args.limit,
        "total": len(standard_dirs),
        "success": 0,
        "failed": 0,
        "items": [],
    }
    for index, standard_dir in enumerate(standard_dirs, start=1):
        output_path = output_dir / f"{_safe_output_name(standard_dir.name)}_graph_llm_full.json"
        print(f"\n[{index}/{len(standard_dirs)}] 开始抽取: {standard_dir.name}")
        try:
            result = await extractor.extract(standard_dir)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
            _print_summary(result, output_path)
            status = result.get("status")
            if status == "success":
                summary["success"] += 1
            else:
                summary["failed"] += 1
            summary["items"].append(
                {
                    "name": standard_dir.name,
                    "input_path": str(standard_dir),
                    "output_path": str(output_path),
                    "status": status,
                    "stats": result.get("stats", {}),
                }
            )
        except Exception as exc:
            traceback.print_exc()
            summary["failed"] += 1
            summary["items"].append(
                {
                    "name": standard_dir.name,
                    "input_path": str(standard_dir),
                    "output_path": str(output_path),
                    "status": "failed",
                    "error": str(exc),
                }
            )

    summary_path = output_dir / DEFAULT_SUMMARY_FILE
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print("\n批量测试完成")
    print("summary_path:", summary_path)
    print("summary:", json.dumps({k: summary[k] for k in ("total", "success", "failed")}, ensure_ascii=False))
    return 0 if summary["failed"] == 0 else 1


def main() -> int:
    try:
        return asyncio.run(_run())
    except KeyboardInterrupt:
        print("用户中断测试。")
        return 130
    except Exception:
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
