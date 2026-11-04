"""Edit this file's configuration, then execute it directly. Default action is offline."""

import json
import logging
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# prepare: offline JSON preview (Neo4j sources explicitly connect read-only).
# import: import an existing prepared RUN_DIRECTORY; resume: reconcile that same run.
# query: read the explicitly selected published version, without writing data.
ACTION = "prepare"
DATASET_VERSION = "knowledge_20261103_v1"
OUTPUT_ROOT = SCRIPT_DIR / "output" / "storage"
RUN_DIRECTORY = None
BATCH_SIZE = 200

# Explicitly list sources; no implicit default remote Neo4j graph is selected.
SOURCES = [
    # {
    #     "source_type": "json", "converter": "chinese_law", "category": "法律法规条款",
    #     "graph_label": "法律法规条款_指标映射", "graph_source": "my-export",
    #     "node_path": r"D:\your-export\node.json", "edge_path": r"D:\your-export\edge.json",
    # },
]
TAGS = []
INDUSTRY_MAPPING = {}  # Reviewed original value -> one controlled value or array.
FILENAME_INDUSTRIES = {}  # Exact filename -> controlled value or array, if reviewed.
FORMAL_INDICATOR_PROPERTY = "三级指标映射列表"
FILE_INDICATOR_PROPERTIES = {}  # File-node type -> actual existing file indicator property.
INDICATOR_SYSTEM_VERSION = None
INITIALIZE_SCHEMA = True  # import/resume only; creates exactly the knowledge table.

# None reuses project SQLAlchemy configuration only on an explicit DB action.
# To choose a different database, set KNOWLEDGE_STORAGE_DB_URL in the environment.
# Never put passwords or database URLs in this tracked script.
QUERY = {"dataset_version": DATASET_VERSION, "page": 1, "page_size": 20}


def explicit_engine():
    import os
    url = os.environ.get("KNOWLEDGE_STORAGE_DB_URL")
    if not url:
        return None
    from sqlalchemy import create_engine
    return create_engine(url, pool_pre_ping=True, hide_parameters=True)


def main():
    from app.infrastructure.kg_to_natural_language.storage.config import SourceConfig, StorageConfig
    from app.infrastructure.kg_to_natural_language.storage.records import json_default
    if ACTION == "prepare":
        from app.infrastructure.kg_to_natural_language.storage.pipeline import prepare_knowledge
        config = StorageConfig(DATASET_VERSION, [SourceConfig(**source) for source in SOURCES], str(OUTPUT_ROOT),
                               batch_size=BATCH_SIZE, tags=TAGS, industry_mapping=INDUSTRY_MAPPING,
                               filename_industries=FILENAME_INDUSTRIES,
                               formal_indicator_property=FORMAL_INDICATOR_PROPERTY,
                               file_indicator_properties=FILE_INDICATOR_PROPERTIES,
                               indicator_system_version=INDICATOR_SYSTEM_VERSION)
        result = prepare_knowledge(config)
    elif ACTION in {"import", "resume", "query"}:
        engine = explicit_engine()
        try:
            if ACTION == "query":
                from app.infrastructure.kg_to_natural_language.storage.query import QuerySpec, query_knowledge
                result = query_knowledge(QuerySpec(**QUERY), engine=engine)
            else:
                if RUN_DIRECTORY is None:
                    raise ValueError("Set RUN_DIRECTORY to the prepared output/runs/run_id directory")
                from app.infrastructure.kg_to_natural_language.storage.pipeline import persist_knowledge
                from app.infrastructure.kg_to_natural_language.storage.repository import KnowledgeRepository
                result = persist_knowledge(RUN_DIRECTORY, repository=KnowledgeRepository(engine),
                                           initialize_schema=INITIALIZE_SCHEMA)
        finally:
            if engine is not None:
                engine.dispose()
    else:
        raise ValueError("ACTION must be prepare, import, resume or query")
    print(json.dumps(result, ensure_ascii=False, indent=2, default=json_default))
    return 0 if result.get("status", "published") in {"prepared", "empty", "published", "already_published"} else 2


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        raise SystemExit(main())
    except Exception as exc:
        from app.infrastructure.kg_to_natural_language.storage.local_records import redact
        logging.error("Knowledge storage failed (%s): %s", type(exc).__name__, redact(str(exc)))
        raise SystemExit(2)
