"""格式一英文法规一阶段切分验证脚本。

本脚本用于批量测试真实法规文件的一阶段规则切分结果，不调用 LLM。
运行后会输出：
1. 每个样例文件的完整 split JSON。
2. 一个 `format_one_split_validation_summary.json` 汇总文件。
3. 控制台上的 totals 摘要。

典型运行方式：
`python app\\infrastructure\\information_extraction\\en_law\\test\\run_format_one_split_test.py`
"""

from __future__ import annotations

# argparse 用于支持命令行参数覆盖数据目录、输出目录和样例列表。
import argparse
# json 用于把最终 totals/errors 打印到控制台。
import json
# os 用于拼接默认输出目录。
import os
# sys 用于把仓库根目录加入模块搜索路径。
import sys
# Path 用于更安全地处理 Windows 路径。
from pathlib import Path

# 当前脚本路径。
CURRENT = Path(__file__).resolve()
# 从 test/run_format_one_split_test.py 向上 5 层得到仓库根目录。
REPO_ROOT = CURRENT.parents[5]
# 当脚本被直接 python 执行时，确保可以 import app 包。
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# 默认数据目录和报告目录来自配置文件。
from app.infrastructure.information_extraction.en_law.config import DEFAULT_ACTUAL_DATA_DIR, DEFAULT_REPORT_DIR
# 复用正式抽取器的 split_file 方法和 JSON 保存工具。
from app.infrastructure.information_extraction.en_law.extractor import FormatOneEnLawExtractor, save_json


# 默认代表性样例文件：覆盖 Regulation、Directive、较大正文、附件和常见格式差异。
DEFAULT_SAMPLES = [
    "GDPR.md",
    "CBAM Regulation.md",
    "VAT Directive.md",
    "REACH.md",
    "Union Customs Code.md",
    "CSDDD.md",
    "Waste Framework Directive.md",
]


def build_summary(filename: str, split_result: dict) -> dict:
    """从完整 split_result 中提取适合审查的摘要字段。"""
    return {
        # 当前样例文件名。
        "filename": filename,
        # 文首合规风险类型；缺失时为空字符串。
        "compliance_risk_type": split_result.get("compliance_risk_type", ""),
        # Article 数量。
        "article_count": len(split_result.get("clauses", [])),
        # Whereas / recital 数量。
        "recital_count": len(split_result.get("recitals", [])),
        # Annex 数量。
        "annex_count": len(split_result.get("annexes_metadata", [])),
        # 默认跳过的 Annex 数量。
        "skipped_annex_count": len(split_result.get("annexes_metadata", [])),
        # 是否识别到 Whereas。
        "has_whereas": bool(split_result.get("recitals")),
        # 是否识别到 TITLE/CHAPTER/Section/PART 层级。
        "has_hierarchy": split_result.get("stats", {}).get("has_hierarchy", False),
        # 第一条 Article 编号。
        "first_article": (
            split_result.get("clauses", [{}])[0].get("article_number")
            if split_result.get("clauses")
            else ""
        ),
        # 最后一条 Article 编号。
        "last_article": (
            split_result.get("clauses", [{}])[-1].get("article_number")
            if split_result.get("clauses")
            else ""
        ),
        # 切分器产生的非致命 warning。
        "warnings": split_result.get("warnings", []),
    }


def run(data_dir: str, output_dir: str, samples: list[str]) -> dict:
    """执行批量切分验证，并保存每个样例的 split JSON 和汇总报告。"""
    # 创建抽取器；本脚本只使用 split_file，不会触发 LLM 懒加载。
    extractor = FormatOneEnLawExtractor()
    # 输出目录不存在时自动创建。
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    # summaries 保存成功样例摘要。
    summaries = []
    # errors 保存缺失文件或切分异常。
    errors = []

    # 逐个处理样例文件。
    for sample in samples:
        # 样例路径由数据目录和文件名拼接得到。
        input_path = Path(data_dir) / sample
        if not input_path.exists():
            # 文件不存在时记录错误并继续处理其他样例。
            errors.append({"filename": sample, "error": "file_not_found"})
            continue
        try:
            # 执行一阶段切分；默认不输出 Annex 原文。
            split_result = extractor.split_file(str(input_path), include_annex_content=False)
            # 生成并保存摘要。
            summaries.append(build_summary(sample, split_result))
            # 保存完整切分结果，供人工审查具体 Article/Annex/Whereas 内容。
            save_json(split_result, str(output_path / f"{input_path.stem}_split.json"))
        except Exception as exc:  # noqa: BLE001 - validation script reports all failures
            # 验证脚本要尽量收集所有失败，而不是遇到第一个异常就停止。
            errors.append({"filename": sample, "error": str(exc)})

    # 汇总报告包含输入输出目录、每个样例摘要、错误列表和总计数量。
    report = {
        "data_dir": data_dir,
        "output_dir": output_dir,
        "samples": summaries,
        "errors": errors,
        "totals": {
            "files": len(summaries),
            "errors": len(errors),
            "articles": sum(item["article_count"] for item in summaries),
            "annexes": sum(item["annex_count"] for item in summaries),
            "recitals": sum(item["recital_count"] for item in summaries),
        },
    }
    # 保存汇总报告。
    save_json(report, str(output_path / "format_one_split_validation_summary.json"))
    return report


def main():
    """命令行入口。"""
    # 创建命令行参数解析器。
    parser = argparse.ArgumentParser(description="Run format-one split-only validation.")
    # 数据目录可通过 --data-dir 覆盖。
    parser.add_argument("--data-dir", default=DEFAULT_ACTUAL_DATA_DIR)
    # 输出目录可通过 --output-dir 覆盖。
    parser.add_argument("--output-dir", default=os.path.join(DEFAULT_REPORT_DIR, "format_one_split_test_output"))
    # 样例文件列表可通过 --samples 覆盖。
    parser.add_argument("--samples", nargs="*", default=DEFAULT_SAMPLES)
    # 解析命令行参数。
    args = parser.parse_args()
    # 执行验证。
    report = run(args.data_dir, args.output_dir, args.samples)
    # 打印 totals，便于命令行快速查看结果。
    print(json.dumps(report["totals"], ensure_ascii=False, indent=2))
    if report["errors"]:
        # 如果有错误，打印错误并返回非 0 退出码，方便 CI 或脚本检测。
        print(json.dumps(report["errors"], ensure_ascii=False, indent=2))
        raise SystemExit(1)


# 直接运行脚本时执行 main；被 pytest/import 时不自动运行。
if __name__ == "__main__":
    main()
