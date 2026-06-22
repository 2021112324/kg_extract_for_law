"""批量测试格式一切分器，统计切分单元字数范围。

本脚本处理指定数据目录下所有英文法规 Markdown 文件，
调用 split_format_one_document 进行规则切分，保存结果 JSON，
并输出切分单元的字数统计报告。
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from collections import defaultdict

# 将仓库根目录加入模块搜索路径。
CURRENT = Path(__file__).resolve()
REPO_ROOT = CURRENT.parents[5]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.infrastructure.information_extraction.en_law.splitter import (
    split_format_one_document,
    clean_text,
)
from app.infrastructure.information_extraction.en_law.config import DEFAULT_ACTUAL_DATA_DIR

# 输出目录
OUTPUT_DIR = Path(__file__).resolve().parent / "result"


def count_words(text: str) -> int:
    """统计英文文本的单词数。"""
    if not text:
        return 0
    return len(text.strip().split())


def build_stats_report(all_file_results: list[dict]) -> dict:
    """根据所有文件的切分结果，统计各切分单元的字数范围。"""
    # 收集所有切分单元的字数
    article_word_counts = []       # Article 正文字数
    recital_word_counts = []       # Recital 内容字数
    struct_unit_word_counts = []   # paragraph/point/subpoint/dash_item 字数
    file_header_word_counts = []   # 文件头字数
    file_info_word_counts = []     # 文件信息字数

    # 按文件汇总
    per_file_stats = []

    for file_result in all_file_results:
        filename = file_result.get("filename", "unknown")

        # 文件头字数
        fh_words = count_words(file_result.get("file_header", ""))
        file_header_word_counts.append(fh_words)

        # 文件信息字数
        fi_words = count_words(file_result.get("file_info", ""))
        file_info_word_counts.append(fi_words)

        # Article 字数
        file_article_counts = []
        for article in file_result.get("clauses", []):
            aw = count_words(article.get("content", ""))
            article_word_counts.append(aw)
            file_article_counts.append(aw)

            # 结构单元字数
            for unit in article.get("structural_units", []):
                uw = count_words(unit.get("text", ""))
                struct_unit_word_counts.append(uw)

        # Recital 字数
        file_recital_counts = []
        for recital in file_result.get("recitals", []):
            rw = count_words(recital.get("content", ""))
            recital_word_counts.append(rw)
            file_recital_counts.append(rw)

        per_file_stats.append({
            "filename": filename,
            "article_count": len(file_article_counts),
            "article_words_min": min(file_article_counts) if file_article_counts else 0,
            "article_words_max": max(file_article_counts) if file_article_counts else 0,
            "article_words_avg": round(sum(file_article_counts) / len(file_article_counts), 1) if file_article_counts else 0,
            "recital_count": len(file_recital_counts),
            "recital_words_min": min(file_recital_counts) if file_recital_counts else 0,
            "recital_words_max": max(file_recital_counts) if file_recital_counts else 0,
            "recital_words_avg": round(sum(file_recital_counts) / len(file_recital_counts), 1) if file_recital_counts else 0,
            "file_header_words": fh_words,
            "file_info_words": fi_words,
        })

    def _range_stats(data: list[int], name: str) -> dict:
        """计算一组字数的统计信息。"""
        if not data:
            return {"name": name, "count": 0, "min": 0, "max": 0, "avg": 0, "p50": 0, "p90": 0, "p95": 0, "p99": 0}
        sorted_data = sorted(data)
        n = len(sorted_data)
        return {
            "name": name,
            "count": n,
            "min": sorted_data[0],
            "max": sorted_data[-1],
            "avg": round(sum(sorted_data) / n, 1),
            "p50": sorted_data[int(n * 0.50)],
            "p90": sorted_data[int(n * 0.90)] if n > 1 else sorted_data[0],
            "p95": sorted_data[int(n * 0.95)] if n > 1 else sorted_data[0],
            "p99": sorted_data[int(n * 0.99)] if n > 1 else sorted_data[0],
        }

    return {
        "overall": {
            "total_files": len(all_file_results),
            "total_articles": len(article_word_counts),
            "total_recitals": len(recital_word_counts),
            "total_structural_units": len(struct_unit_word_counts),
        },
        "article_words": _range_stats(article_word_counts, "Article 正文"),
        "recital_words": _range_stats(recital_word_counts, "Recital"),
        "structural_unit_words": _range_stats(struct_unit_word_counts, "结构单元 (paragraph/point/subpoint/dash)"),
        "file_header_words": _range_stats(file_header_word_counts, "文件头"),
        "file_info_words": _range_stats(file_info_word_counts, "文件信息 (含 Whereas)"),
        "per_file": per_file_stats,
    }


def main():
    data_dir = DEFAULT_ACTUAL_DATA_DIR
    output_dir = OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    # 获取所有 .md 文件
    data_path = Path(data_dir)
    md_files = sorted(data_path.glob("*.md"))
    print(f"数据目录: {data_dir}")
    print(f"找到 {len(md_files)} 个 .md 文件")
    print(f"输出目录: {output_dir}")
    print("=" * 80)

    all_results = []
    errors = []

    for md_file in md_files:
        filename = md_file.name
        print(f"\n处理: {filename}")
        try:
            with open(md_file, "r", encoding="utf-8") as f:
                text = f.read()
            result = split_format_one_document(text, filename=filename, include_annex_content=False)

            # 保存切分结果
            output_name = f"{md_file.stem}_split.json"
            output_path = output_dir / output_name
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(result, f, ensure_ascii=False, indent=2)

            # 统计当前文件
            article_count = len(result.get("clauses", []))
            recital_count = len(result.get("recitals", []))
            annex_count = len(result.get("annexes_metadata", []))
            warnings = result.get("warnings", [])

            print(f"  Articles: {article_count}, Recitals: {recital_count}, Annexes: {annex_count}")
            if warnings:
                print(f"  Warnings: {warnings}")

            all_results.append(result)

        except Exception as exc:
            print(f"  错误: {exc}")
            errors.append({"filename": filename, "error": str(exc)})

    # 汇总统计
    print("\n" + "=" * 80)
    print("生成字数统计报告...\n")

    report = build_stats_report(all_results)
    report["errors"] = errors

    # 保存统计报告
    report_path = output_dir / "splitter_word_count_report.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print(f"统计报告已保存到: {report_path}")

    # 打印摘要
    print("\n" + "=" * 80)
    print("切分单元字数统计摘要")
    print("=" * 80)

    overall = report["overall"]
    print(f"\n总文件数: {overall['total_files']}")
    print(f"总 Article 数: {overall['total_articles']}")
    print(f"总 Recital 数: {overall['total_recitals']}")
    print(f"总结构单元数: {overall['total_structural_units']}")

    for key in ["article_words", "recital_words", "structural_unit_words", "file_header_words", "file_info_words"]:
        stats = report[key]
        print(f"\n--- {stats['name']} ---")
        print(f"  数量: {stats['count']}")
        print(f"  最小字数: {stats['min']}")
        print(f"  最大字数: {stats['max']}")
        print(f"  平均字数: {stats['avg']}")
        print(f"  P50: {stats['p50']}")
        print(f"  P90: {stats['p90']}")
        print(f"  P95: {stats['p95']}")
        print(f"  P99: {stats['p99']}")

    # 打印各文件 Article 字数范围
    print("\n" + "=" * 80)
    print("各文件 Article 字数范围")
    print("=" * 80)
    for fs in report["per_file"]:
        print(f"  {fs['filename']}: {fs['article_count']} articles, "
              f"min={fs['article_words_min']}, max={fs['article_words_max']}, "
              f"avg={fs['article_words_avg']}")

    if errors:
        print(f"\n错误文件数: {len(errors)}")
        for e in errors:
            print(f"  {e['filename']}: {e['error']}")


if __name__ == "__main__":
    main()
