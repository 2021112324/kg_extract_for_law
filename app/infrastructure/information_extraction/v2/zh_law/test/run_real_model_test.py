from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import datetime
from pathlib import Path

from app.infrastructure.information_extraction.v2.zh_law import ClauseExtractor, ZhLawConfig
from app.infrastructure.information_extraction.v2.zh_law.io_utils import save_json


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a real-model V2 Chinese regulation extraction test.")
    parser.add_argument("input_path")
    parser.add_argument("output_dir")
    return parser.parse_args()


async def run(args: argparse.Namespace) -> int:
    config = ZhLawConfig.from_env()
    extractor = ClauseExtractor(config=config)
    started_at = datetime.now().astimezone()
    report = {
        "input_path": str(Path(args.input_path).resolve()),
        "output_dir": str(Path(args.output_dir).resolve()),
        "model_name": config.model_name,
        "api_url": config.api_url,
        "lenient_mode": config.lenient_mode,
        "started_at": started_at.isoformat(),
        "status": "running",
    }
    try:
        kg = await extractor.extract_file_to_kg(args.input_path, args.output_dir)
        report.update(
            {
                "status": "success",
                "node_count": len(kg.get("nodes") or []),
                "edge_count": len(kg.get("edges") or []),
                "metadata": kg.get("metadata") or {},
            }
        )
        return_code = 0
    except Exception as exc:
        logging.exception("Real-model V2 Chinese regulation extraction failed")
        report.update(
            {
                "status": "failed",
                "error": str(exc),
                "extraction_status": extractor.last_extraction_status,
                "run_stats": extractor.get_run_summary(),
            }
        )
        return_code = 1
    finally:
        report["finished_at"] = datetime.now().astimezone().isoformat()
        save_json(report, Path(args.output_dir) / "real_model_test_report.json")
        await extractor.logging_result_stats()
    return return_code


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    return asyncio.run(run(parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())

