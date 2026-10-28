"""Edit the settings below, then run this file. Offline parsing is the default."""
import asyncio
import logging
import os
from pathlib import Path
import sys

SCRIPT_DIR = Path(__file__).resolve().parent
REPO = next(p for p in SCRIPT_DIR.parents if (p / "app/infrastructure").is_dir())
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from app.infrastructure.information_extraction.v2.guide_p2.config import GuideP2Config
from app.infrastructure.information_extraction.v2.guide_p2.standalone import GuideP2StandaloneRunner
from app.infrastructure.information_extraction.v2.rule.run_extraction import create_storage

MODE = "parse"
INPUT_PATH = ""  # Required: one UTF-8 TXT/Markdown file or a dedicated directory.
OUTPUT_DIR = SCRIPT_DIR / "output"
TARGET_LABEL = ""  # New unused label; required only for write mode.
MODEL_NAME = os.getenv("GUIDE_P2_MODEL", "")
MODEL_URL = os.getenv("GUIDE_P2_API_URL", "")
BATCH_SIZE = 12
MAX_CONCURRENT = 3
MAX_ATTEMPTS = 5  # Total attempts per target, including batch attempts and split retries.
TIMEOUT_SECONDS = 180


def main():
    if not INPUT_PATH:
        raise ValueError("Configure INPUT_PATH first")
    config = GuideP2Config(model_name=MODEL_NAME, api_url=MODEL_URL, batch_size=BATCH_SIZE,
                          max_concurrent=MAX_CONCURRENT, max_attempts=MAX_ATTEMPTS, timeout=TIMEOUT_SECONDS)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    result = asyncio.run(GuideP2StandaloneRunner(config, create_storage() if MODE == "write" else None).run(INPUT_PATH, OUTPUT_DIR, MODE, TARGET_LABEL))
    print("status:", result["status"], "report:", Path(OUTPUT_DIR)/"service_summary.json")
    if result["status"] != "completed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
