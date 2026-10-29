"""Edit this configuration and run the file. Offline parsing is the default."""
import asyncio
import logging
import os
from pathlib import Path
import sys

SCRIPT_DIR = Path(__file__).resolve().parent
REPO = next(p for p in SCRIPT_DIR.parents if (p/"app/infrastructure").is_dir())
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from app.infrastructure.information_extraction.v2.paragraph.config import ParagraphConfig
from app.infrastructure.information_extraction.v2.paragraph.standalone import ParagraphStandaloneRunner
from app.infrastructure.information_extraction.v2.rule.run_extraction import create_storage

MODE = "parse"
INPUT_PATH = ""  # One UTF-8 TXT/Markdown file, or a dedicated directory.
OUTPUT_DIR = SCRIPT_DIR/"output"
TARGET_LABEL = ""  # A new unused label; write mode only.
PUBLICATION = "normative"  # reference: explicitly publish a separate reference graph.
MODEL_NAME = os.getenv("PARAGRAPH_MODEL", "")
MODEL_URL = os.getenv("PARAGRAPH_API_URL", "")
MAX_ATTEMPTS = 5
MAX_CONCURRENT = 3
TIMEOUT_SECONDS = 180
TARGET_CHARS = 6000
CONTEXT_CHARS = 12000
PARAGRAPH_MODE = "blank_lines"  # one_line only when the source is known to use one paragraph per line.


def main():
    if not INPUT_PATH:
        raise ValueError("Configure INPUT_PATH first; no automatic whole-project scan")
    config = ParagraphConfig(model_name=MODEL_NAME, api_url=MODEL_URL, publication=PUBLICATION,
        max_attempts=MAX_ATTEMPTS, max_concurrent=MAX_CONCURRENT, timeout=TIMEOUT_SECONDS,
        block_chars=TARGET_CHARS, context_chars=CONTEXT_CHARS, paragraph_mode=PARAGRAPH_MODE)
    storage = create_storage() if MODE == "write" else None
    from app.infrastructure.information_extraction.v2.paragraph.io_utils import RedactedFormatter
    console = logging.StreamHandler()
    console.setFormatter(RedactedFormatter([config.api_key, getattr(storage, "password", "")]))
    logging.basicConfig(level=logging.INFO, handlers=[console])
    result = asyncio.run(ParagraphStandaloneRunner(config, storage).run(INPUT_PATH, OUTPUT_DIR, MODE, TARGET_LABEL))
    print("status:", result["status"], "report:", Path(result["record_directory"])/"service_summary.json")
    return 0 if result["status"] == "completed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
