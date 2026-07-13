"""格式二美国法案一阶段切分测试脚本。

默认读取格式二数据目录，输出每个文件的 split JSON 和总览报告。
该脚本不调用大模型。
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[5]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.infrastructure.information_extraction.en_law_v2.config import (  # noqa: E402
    DEFAULT_ACTUAL_DATA_DIR,
    DEFAULT_REPORT_DIR,
)
from app.infrastructure.information_extraction.en_law_v2.extractor import (  # noqa: E402
    FormatTwoEnLawExtractor,
    save_json,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run format-two split test.")
    parser.add_argument("--data-dir", default=DEFAULT_ACTUAL_DATA_DIR)
    parser.add_argument(
        "--output-dir",
        default=os.path.join(DEFAULT_REPORT_DIR, "format_two_split_test"),
    )
    return parser.parse_args()


def markdown_files(data_dir: str) -> list[str]:
    return [
        str(path)
        for path in sorted(Path(data_dir).glob("*.md"), key=lambda item: item.name.lower())
    ]


def main() -> int:
    args = parse_args()
    extractor = FormatTwoEnLawExtractor()
    os.makedirs(args.output_dir, exist_ok=True)
    rows: list[dict[str, Any]] = []

    for path in markdown_files(args.data_dir):
        result = extractor.split_file(path, output_dir=args.output_dir)
        stats = result.get("stats") or {}
        validation = result.get("validation") or {}
        rows.append({
            "filename": os.path.basename(path),
            **stats,
            **validation,
            "warnings": result.get("warnings") or [],
        })
        print(
            os.path.basename(path),
            "sections=", stats.get("section_count"),
            "toc=", stats.get("table_of_contents_item_count"),
            "clauses=", stats.get("provision_clause_count"),
            "manual_review=", validation.get("requires_manual_review"),
        )

    summary = {
        "data_dir": args.data_dir,
        "file_count": len(rows),
        "complete_uniform_candidates": [
            row["filename"] for row in rows if row.get("is_complete_uniform_candidate")
        ],
        "manual_review_files": [
            row["filename"] for row in rows if row.get("requires_manual_review")
        ],
        "rows": rows,
    }
    save_json(summary, os.path.join(args.output_dir, "format_two_split_summary.json"))
    print("SUMMARY:", os.path.join(args.output_dir, "format_two_split_summary.json"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
