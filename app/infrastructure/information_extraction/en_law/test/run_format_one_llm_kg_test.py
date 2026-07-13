r"""格式一英文法规真实 LLM 知识图谱抽取测试脚本。

本脚本用于手动运行格式一英文法规的端到端 LLM 抽取测试。
注意：本脚本不提供 run_llm=False 模式，所有抽取均走真实大模型路径。

推荐运行环境：
    conda activate cogmait312

常用命令：
    # 全量 26 个文件、整篇法规、真实 LLM 抽取；使用本文件“固定参数区”的配置。
    # 结果保存到 app\infrastructure\information_extraction\en_law\test\result2。
    python app\infrastructure\information_extraction\en_law\test\run_format_one_llm_kg_test.py

    # 临时只抽取一个或多个完整文件；传入命令行参数时会覆盖固定参数。
    python app\infrastructure\information_extraction\en_law\test\run_format_one_llm_kg_test.py --files "Parent-Subsidiary Directive.md"

    # 只抽取每个文件前 1 个 Article，仍然使用真实 LLM，适合快速验证链路。
    python app\infrastructure\information_extraction\en_law\test\run_format_one_llm_kg_test.py --files "EU Dual-Use Regulation.md" --max-articles 1
"""

from __future__ import annotations

import argparse
import asyncio
import copy
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

CURRENT = Path(__file__).resolve()
REPO_ROOT = CURRENT.parents[5]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# =========================
# 固定参数区
# =========================
# 无命令行参数直接运行本脚本时，会使用下面这些固定参数。
# 注意：本脚本没有 run_llm=False 分支，抽取始终使用真实大模型。
USE_FIXED_PARAMETERS_WHEN_NO_ARGS = True

# 原始格式一英文法规目录。
FIXED_DATA_DIR = r"D:\CogmAIT\8.1项目\8.1数据\爬取的数据\风险法规\第三版\分类\英文抽取所用数据\法文汇总\格式一"

# 真实 LLM 全量抽取结果输出目录。
FIXED_OUTPUT_DIR = str(
    REPO_ROOT
    / "app"
    / "infrastructure"
    / "information_extraction"
    / "en_law"
    / "test"
    / "result2"
)

# 默认直接跑全量。只有需要临时小样本调试时，才把 FIXED_ALL 改为 False 并填写 FIXED_FILES。
FIXED_FILES: list[str] = []
FIXED_ALL = True
FIXED_EXCLUDE: list[str] = []
FIXED_LIMIT: int | None = None

# None 表示抽取整份文件；整数 N 表示只抽取前 N 个 Article，但仍然调用真实 LLM。
# 全量正式抽取必须保持 None。
FIXED_MAX_ARTICLES: int | None = None

# Article 级并发。先用 1，确认稳定后再调高。
FIXED_MAX_CONCURRENT = 1

# False 表示宽松模式：单条 Article 失败时记录错误并继续，不生成 fallback 节点。
FIXED_STRICT = False

# True 表示覆盖同名输出，避免旧结果被跳过。
FIXED_OVERWRITE = True

# True 表示遇到第一个文件失败就停止。
FIXED_STOP_ON_ERROR = False

# 模型调用配置。这里在导入 en_law.config 前写入环境变量，使配置文件读取到固定值。
os.environ["EN_LAW_MAX_CONCURRENT"] = str(FIXED_MAX_CONCURRENT)
os.environ["EN_LAW_MAX_RETRIES"] = "1"
os.environ["EN_LAW_EXTRACTION_TIMEOUT"] = "3600"

from app.infrastructure.information_extraction.en_law.config import DEFAULT_ACTUAL_DATA_DIR
from app.infrastructure.information_extraction.en_law.extractor import FormatOneEnLawExtractor, save_json


DEFAULT_OUTPUT_DIR = (
    REPO_ROOT
    / "app"
    / "infrastructure"
    / "information_extraction"
    / "en_law"
    / "test"
    / "result2"
    / "llm_run"
)


def _assert_cogmait312() -> None:
    """防止误用 base Python 环境运行真实 LLM 测试。"""
    env_name = os.environ.get("CONDA_DEFAULT_ENV", "")
    executable = sys.executable.lower()
    if "cogmait312" not in env_name.lower() and "cogmait312" not in executable:
        raise RuntimeError(
            "请先切换到正确环境再运行：conda activate cogmait312\n"
            f"当前 CONDA_DEFAULT_ENV={env_name!r}, sys.executable={sys.executable!r}"
        )


def _fixed_args() -> argparse.Namespace:
    """构造无参数直接运行时使用的固定参数。"""
    return argparse.Namespace(
        data_dir=FIXED_DATA_DIR,
        output_dir=FIXED_OUTPUT_DIR,
        files=None if FIXED_ALL else FIXED_FILES,
        exclude=FIXED_EXCLUDE,
        limit=FIXED_LIMIT,
        all=FIXED_ALL,
        max_articles=FIXED_MAX_ARTICLES,
        max_concurrent=FIXED_MAX_CONCURRENT,
        strict=FIXED_STRICT,
        overwrite=FIXED_OVERWRITE,
        stop_on_error=FIXED_STOP_ON_ERROR,
    )


def _json_path(output_dir: Path, input_path_or_filename: str, suffix: str) -> Path:
    """根据输入文件名生成测试输出路径。"""
    stem = Path(input_path_or_filename).stem
    return output_dir / f"{stem}_{suffix}.json"


def _load_json(path: Path) -> dict[str, Any]:
    """读取 JSON 文件。"""
    return json.loads(path.read_text(encoding="utf-8"))


def _select_files(args: argparse.Namespace) -> list[Path]:
    """根据命令行参数选择要抽取的 Markdown 文件。"""
    data_dir = Path(args.data_dir)
    if args.files:
        files = [data_dir / item for item in args.files]
    else:
        files = sorted(data_dir.glob("*.md"))

    if args.exclude:
        excluded = set(args.exclude)
        files = [path for path in files if path.name not in excluded]

    missing = [str(path) for path in files if not path.exists()]
    if missing:
        raise FileNotFoundError("以下待测文件不存在：\n" + "\n".join(missing))

    if args.all:
        return files

    limit = args.limit if args.limit is not None else 1
    return files[:limit]


def _summarize_kg(
    input_path: Path,
    kg: dict[str, Any],
    elapsed_seconds: float,
    output_dir: Path,
    mode: str,
) -> dict[str, Any]:
    """从单份 KG 结果中提取摘要。"""
    metadata = kg.get("metadata", {}) or {}
    raw = kg.get("raw_llm_result", {}) or {}
    raw_stats = raw.get("stats", {}) or {}
    return {
        "filename": input_path.name,
        "mode": mode,
        "elapsed_seconds": round(elapsed_seconds, 3),
        "nodes": len(kg.get("nodes", [])),
        "edges": len(kg.get("edges", [])),
        "article_count": metadata.get("article_count"),
        "annex_count": metadata.get("annex_count"),
        "skipped_annex_count": metadata.get("skipped_annex_count"),
        "recital_count": metadata.get("recital_count"),
        "success_clause_count": metadata.get("success_clause_count"),
        "failed_clause_count": metadata.get("failed_clause_count"),
        "raw_article_total": raw_stats.get("article_total"),
        "raw_article_success": raw_stats.get("article_success"),
        "raw_article_failed": raw_stats.get("article_failed"),
        "warnings": len(metadata.get("warnings") or []),
        "errors": len(metadata.get("errors") or []),
        "kg_path": str(_json_path(output_dir, input_path.name, "kg")),
        "raw_llm_path": str(_json_path(output_dir, input_path.name, "raw_llm")),
    }


async def _extract_one_full(
    extractor: FormatOneEnLawExtractor,
    input_path: Path,
    output_dir: Path,
) -> dict[str, Any]:
    """对单个文件执行完整真实 LLM KG 抽取。"""
    started = time.time()
    kg = await extractor.extract_file_to_kg(str(input_path), run_llm=True, output_dir=str(output_dir))
    return _summarize_kg(
        input_path=input_path,
        kg=kg,
        elapsed_seconds=time.time() - started,
        output_dir=output_dir,
        mode="llm_run_true_full",
    )


async def _extract_one_sample(
    extractor: FormatOneEnLawExtractor,
    input_path: Path,
    output_dir: Path,
    max_articles: int,
) -> dict[str, Any]:
    """对单个文件执行真实 LLM 局部抽取，通常用于快速验证链路。"""
    started = time.time()
    split_result = extractor.split_file(str(input_path), include_annex_content=False)
    original_article_count = len(split_result.get("clauses", []))

    sample_split = copy.deepcopy(split_result)
    sample_split["clauses"] = sample_split.get("clauses", [])[:max_articles]
    sample_split["llm_test_scope"] = {
        "run_llm": True,
        "scope": f"file_info_and_first_{max_articles}_articles",
        "original_article_count": original_article_count,
        "sample_article_count": len(sample_split.get("clauses", [])),
    }
    save_json(sample_split, str(_json_path(output_dir, input_path.name, f"split_first_{max_articles}_articles")))

    raw_llm_result = await extractor.llm_extract_from_split_result(input_path.name, sample_split, str(output_dir))
    kg = extractor.build_graph(input_path.name, sample_split, raw_llm_result)
    kg["metadata"]["run_llm"] = True
    kg["metadata"]["llm_test_scope"] = f"file_info_and_first_{max_articles}_articles"
    kg["metadata"]["original_article_count"] = original_article_count
    kg["metadata"]["sample_article_count"] = len(sample_split.get("clauses", []))
    save_json(kg, str(_json_path(output_dir, input_path.name, "kg")))

    return _summarize_kg(
        input_path=input_path,
        kg=kg,
        elapsed_seconds=time.time() - started,
        output_dir=output_dir,
        mode=f"llm_run_true_first_{max_articles}_articles",
    )


async def run(args: argparse.Namespace) -> dict[str, Any]:
    """执行批量真实 LLM 抽取测试。"""
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    files = _select_files(args)

    extractor = FormatOneEnLawExtractor(max_concurrent=args.max_concurrent, lenient_mode=not args.strict)
    summaries: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    for index, input_path in enumerate(files, start=1):
        print(f"[{index}/{len(files)}] LLM extracting: {input_path.name}", flush=True)
        kg_path = _json_path(output_dir, input_path.name, "kg")
        if kg_path.exists() and not args.overwrite:
            kg = _load_json(kg_path)
            summaries.append(
                _summarize_kg(
                    input_path=input_path,
                    kg=kg,
                    elapsed_seconds=0,
                    output_dir=output_dir,
                    mode="skipped_existing_result",
                )
            )
            print(f"  skipped existing: {kg_path}", flush=True)
            continue

        try:
            if args.max_articles is None:
                item = await _extract_one_full(extractor, input_path, output_dir)
            else:
                item = await _extract_one_sample(extractor, input_path, output_dir, args.max_articles)
            summaries.append(item)
            print(
                "  done: "
                f"nodes={item['nodes']}, edges={item['edges']}, "
                f"article_success={item['raw_article_success']}, "
                f"article_failed={item['raw_article_failed']}, "
                f"seconds={item['elapsed_seconds']}",
                flush=True,
            )
        except Exception as exc:  # noqa: BLE001 - test script must continue collecting failures
            errors.append({"filename": input_path.name, "error": str(exc)})
            print(f"  failed: {exc}", flush=True)
            if args.stop_on_error:
                break

    report = {
        "mode": "real_llm_run_true",
        "data_dir": args.data_dir,
        "output_dir": str(output_dir),
        "selected_file_count": len(files),
        "success_count": len(summaries),
        "error_count": len(errors),
        "max_articles": args.max_articles,
        "max_concurrent": args.max_concurrent,
        "strict": args.strict,
        "overwrite": args.overwrite,
        "summaries": summaries,
        "errors": errors,
        "totals": {
            "nodes": sum(item.get("nodes") or 0 for item in summaries),
            "edges": sum(item.get("edges") or 0 for item in summaries),
            "articles": sum(item.get("article_count") or 0 for item in summaries),
            "raw_article_success": sum(item.get("raw_article_success") or 0 for item in summaries),
            "raw_article_failed": sum(item.get("raw_article_failed") or 0 for item in summaries),
            "annexes": sum(item.get("annex_count") or 0 for item in summaries),
        },
    }
    save_json(report, str(output_dir / "llm_kg_extraction_summary.json"))
    return report


def main() -> None:
    """命令行入口。"""
    _assert_cogmait312()
    parser = argparse.ArgumentParser(description="Run real LLM KG extraction for format-one English law files.")
    parser.add_argument("--data-dir", default=DEFAULT_ACTUAL_DATA_DIR, help="格式一英文法规 Markdown 原始目录。")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="真实 LLM 抽取结果输出目录。")
    parser.add_argument("--files", nargs="*", help="只测试指定文件名，文件名相对于 data-dir。")
    parser.add_argument("--exclude", nargs="*", help="从待测文件中排除指定文件名。")
    parser.add_argument("--limit", type=int, help="未指定 --all 时最多测试多少个文件；默认 1。")
    parser.add_argument("--all", action="store_true", help="测试 data-dir 下全部 Markdown 文件。")
    parser.add_argument("--max-articles", type=int, help="每个文件只抽取前 N 个 Article；不填则抽取整份文件。")
    parser.add_argument("--max-concurrent", type=int, default=1, help="Article 级 LLM 并发数；建议先用 1。")
    parser.add_argument("--strict", action="store_true", help="关闭宽松模式；Article 失败时直接中断。")
    parser.add_argument("--overwrite", action="store_true", help="覆盖输出目录中已有 KG 文件。")
    parser.add_argument("--stop-on-error", action="store_true", help="遇到第一个文件失败即停止。")
    if USE_FIXED_PARAMETERS_WHEN_NO_ARGS and len(sys.argv) == 1:
        args = _fixed_args()
        print("Using fixed parameters from run_format_one_llm_kg_test.py:", flush=True)
        print(
            json.dumps(
                {
                    "data_dir": args.data_dir,
                    "output_dir": args.output_dir,
                    "files": args.files,
                    "all": args.all,
                    "max_articles": args.max_articles,
                    "max_concurrent": args.max_concurrent,
                    "strict": args.strict,
                    "overwrite": args.overwrite,
                    "stop_on_error": args.stop_on_error,
                    "run_llm": True,
                },
                ensure_ascii=False,
                indent=2,
            ),
            flush=True,
        )
    else:
        args = parser.parse_args()

    report = asyncio.run(run(args))
    print(json.dumps(report["totals"], ensure_ascii=False, indent=2))
    if report["errors"]:
        print(json.dumps(report["errors"], ensure_ascii=False, indent=2))
        raise SystemExit(1)
    print("抽取结束\n\n请查看详细报告和 KG 结果：", flush=True)


if __name__ == "__main__":
    main()
