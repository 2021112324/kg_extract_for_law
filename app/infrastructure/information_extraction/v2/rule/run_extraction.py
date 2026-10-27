"""Edit these settings and execute this file; the default is offline parsing."""
from pathlib import Path
import asyncio
import logging
import os
import sys


SCRIPT_DIR = Path(__file__).resolve().parent
REPO = next(p for p in SCRIPT_DIR.parents if (p / "app/infrastructure").is_dir())
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from app.infrastructure.information_extraction.v2.rule.config import RuleConfig
from app.infrastructure.information_extraction.v2.rule.standalone import RuleStandaloneRunner


MODE = "parse"  # parse: offline; extract: model + local output; write: model + new Neo4j label.
INPUT_PATH = ""  # A .txt/.md file or a dedicated input directory; required.
OUTPUT_DIR = SCRIPT_DIR / "output"
TARGET_LABEL = ""  # A new unused label, required only in write mode.
MODEL_NAME = os.getenv("RULE_MODEL", "")
MODEL_URL = os.getenv("RULE_API_URL", "")
MAX_CONCURRENT = 3
MAX_ATTEMPTS = 5
TIMEOUT_SECONDS = 180


def create_storage():
    keys = ("NEO4J_URI", "NEO4J_USERNAME", "NEO4J_PASSWORD", "NEO4J_DATABASE")
    if any(not os.getenv(key) for key in keys):
        raise ValueError("Explicit Neo4j connection environment variables are required")
    from app.infrastructure.graph_storage.neo4j_adapter import Neo4jAdapter
    return Neo4jAdapter(uri=os.environ[keys[0]], username=os.environ[keys[1]],
                        password=os.environ[keys[2]], database=os.environ[keys[3]],
                        driver_options={"connection_timeout": 30})


def main():
    if not INPUT_PATH:
        raise ValueError("Configure INPUT_PATH before running; no implicit project-wide scan")
    config = RuleConfig(model_name=MODEL_NAME, api_url=MODEL_URL, max_concurrent=MAX_CONCURRENT,
                        max_attempts=MAX_ATTEMPTS, timeout=TIMEOUT_SECONDS)
    storage = create_storage() if MODE == "write" else None
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    result = asyncio.run(RuleStandaloneRunner(config, storage).run(INPUT_PATH, OUTPUT_DIR, MODE, TARGET_LABEL))
    print("status:", result["status"], "report:", Path(OUTPUT_DIR) / "service_summary.json")
    if result["status"] != "completed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
