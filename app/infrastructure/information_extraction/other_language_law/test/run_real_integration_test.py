"""其他语种法规真实数据联调测试脚本。

默认使用德国法规真实数据。脚本默认不重新调用翻译 LLM，而是复用已有译文目录，
并执行质量校验和中文一阶段切分验证；传入 `--run-llm-extraction` 后会继续调用
中文 `ClauseExtractor.extract_clauses` 做完整大模型抽取。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[5]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.infrastructure.information_extraction.law_extract.clause_extract import ClauseExtractor  # noqa: E402
from app.infrastructure.information_extraction.other_language_law.config import (  # noqa: E402
    DEFAULT_CACHE_PATH,
    DEFAULT_OTHER_LANGUAGE_DATA_ROOT,
)
from app.infrastructure.information_extraction.other_language_law.extractor import (  # noqa: E402
    OtherLanguageLawExtractor,
    save_json,
)
from app.infrastructure.information_extraction.other_language_law.normalizer import (  # noqa: E402
    normalize_translated_file_for_chinese_extraction,
)
from app.infrastructure.information_extraction.other_language_law.quality import (  # noqa: E402
    validate_translation_directory,
    write_quality_outputs,
)
from app.infrastructure.information_extraction.other_language_law.translator import (  # noqa: E402
    translate_directory,
    write_translation_outputs,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run real multilingual law integration test.")
    parser.add_argument("--source-dir", default=str(DEFAULT_OTHER_LANGUAGE_DATA_ROOT / "德国"))
    parser.add_argument("--translated-dir", default=str(DEFAULT_OTHER_LANGUAGE_DATA_ROOT / "德国_翻译"))
    parser.add_argument(
        "--output-dir",
        default=str(Path(__file__).resolve().parents[1] / "test_data" / "real_integration"),
    )
    parser.add_argument("--cache-path", default=str(DEFAULT_CACHE_PATH))
    parser.add_argument("--overwrite-translation", action="store_true")
    parser.add_argument("--run-translation", action="store_true")
    parser.add_argument("--run-llm-extraction", action="store_true")
    parser.add_argument("--limit", type=int, default=1)
    return parser.parse_args()


async def run_split_validation(source_dir: Path, translated_dir: Path, output_dir: Path, limit: int) -> dict[str, Any]:
    extractor = ClauseExtractor(max_concurrent=1)
    split_dir = output_dir / "split"
    normalized_dir = output_dir / "translated_for_chinese_extraction"
    split_dir.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, Any]] = []
    files = sorted(translated_dir.glob("*.md"), key=lambda item: item.name.lower())
    if limit and limit > 0:
        files = files[:limit]
    for translated_path in files:
        normalized_path = normalized_dir / translated_path.name
        normalize_translated_file_for_chinese_extraction(translated_path, normalized_path)
        text = normalized_path.read_text(encoding="utf-8", errors="ignore")
        try:
            split_result = await extractor.split_clause(text)
            save_json(split_result, split_dir / f"{translated_path.stem}_split.json")
            rows.append(
                {
                    "filename": translated_path.name,
                    "status": "success",
                    "source_path": str(source_dir / translated_path.name),
                    "translated_path": str(translated_path),
                    "normalized_path": str(normalized_path),
                    "clause_count": len(split_result.get("clauses") or []),
                    "file_info_chars": len(split_result.get("file_info") or ""),
                    "error": "",
                }
            )
        except Exception as exc:
            rows.append(
                {
                    "filename": translated_path.name,
                    "status": "failed",
                    "source_path": str(source_dir / translated_path.name),
                    "translated_path": str(translated_path),
                    "normalized_path": str(normalized_path),
                    "clause_count": 0,
                    "file_info_chars": 0,
                    "error": str(exc),
                }
            )
    summary = {
        "source_dir": str(source_dir),
        "translated_dir": str(translated_dir),
        "output_dir": str(output_dir),
        "file_count": len(rows),
        "success_count": sum(1 for item in rows if item["status"] == "success"),
        "failed_count": sum(1 for item in rows if item["status"] == "failed"),
        "rows": rows,
    }
    save_json(summary, output_dir / "real_split_validation_summary.json")
    return summary


async def main_async() -> int:
    args = parse_args()
    source_dir = Path(args.source_dir)
    translated_dir = Path(args.translated_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    translation_summary = None
    if args.run_translation:
        translation_result = translate_directory(
            source_dir=source_dir,
            target_dir=translated_dir,
            cache_path=args.cache_path,
            overwrite=args.overwrite_translation,
            limit=args.limit,
        )
        write_translation_outputs(translation_result, output_dir)
        translation_summary = translation_result.to_dict()

    quality_summary = validate_translation_directory(source_dir, translated_dir)
    write_quality_outputs(quality_summary, output_dir)
    split_summary = await run_split_validation(source_dir, translated_dir, output_dir, args.limit)

    llm_summary = None
    if args.run_llm_extraction:
        result = await OtherLanguageLawExtractor(max_concurrent=1).extract_directory(
            source_dir=source_dir,
            translated_dir=translated_dir,
            cache_path=args.cache_path,
            output_dir=output_dir / "llm_extract",
            overwrite=False,
            limit=args.limit,
            run_extraction=True,
        )
        llm_summary = result.to_dict()

    final_summary = {
        "translation": translation_summary,
        "quality": quality_summary,
        "split_validation": split_summary,
        "llm_extraction": llm_summary,
    }
    save_json(final_summary, output_dir / "real_integration_summary.json")
    print(
        "REAL_INTEGRATION_DONE "
        f"split_success={split_summary['success_count']} "
        f"split_failed={split_summary['failed_count']} "
        f"llm_success={(llm_summary or {}).get('success_files', 0)} "
        f"llm_failed={(llm_summary or {}).get('failed_files', 0)} "
        f"summary={output_dir / 'real_integration_summary.json'}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main_async()))
