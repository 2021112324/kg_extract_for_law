"""一次执行：读取四类 Neo4j 图谱、生成知识条目并写入 MySQL 8。"""

import getpass
import json
import logging
import os
import re
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Neo4j 沿用项目配置；只读取以下标签，不修改源图谱。
GRAPH_SOURCE = "project-neo4j"
GRAPH_LABELS = [
    ("法律法规条款", "chinese_law"),
    ("国家标准", "national_standard"),
    ("合规指引", "guide"),
    ("行政监管规则", "chinese_law"),
]
# 再次运行会继续同一份冻结数据；源图谱或配置变化后请换一个新版本。
DATASET_VERSION = "knowledge_20261104_four_graphs_v1"
OUTPUT_ROOT = SCRIPT_DIR / "output" / "storage_four_graphs"
BATCH_SIZE = 200
MAX_SOURCE_BYTES = 536870912

# 请填写实际运行的 MySQL 8 连接，不要连接原来的 MySQL 5.6。
# 3308 只是建议使用的独立端口，不表示该端口已经有服务。
# 本脚本不会启动/安装 MySQL；目标数据库不存在时可以自动创建。
MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 3308
MYSQL_USER = "root"
MYSQL_DATABASE = "compliance_knowledge"
MYSQL_PASSWORD_ENV = "KNOWLEDGE_STORAGE_MYSQL_PASSWORD"
# 密码不写入代码；优先读取上面的环境变量，否则运行时提示输入。
CREATE_DATABASE_IF_MISSING = True
INITIALIZE_TABLE = True

# 不确定时保持空配置；不会根据常识补分类或补三级指标。
INDUSTRY_MAPPING = {}
FILENAME_INDUSTRIES = {}
FILE_INDICATOR_PROPERTIES = {}
FORMAL_INDICATOR_PROPERTY = "三级指标映射列表"
INDICATOR_SYSTEM_VERSION = None

logger = logging.getLogger(__name__)


def build_config():
    from app.infrastructure.kg_to_natural_language.storage.config import SourceConfig, StorageConfig

    config = StorageConfig(
        dataset_version=DATASET_VERSION,
        sources=[SourceConfig(
            source_type="neo4j", converter=converter, category=label,
            graph_label=label, graph_source=GRAPH_SOURCE, max_bytes=MAX_SOURCE_BYTES,
        ) for label, converter in GRAPH_LABELS],
        output_root=str(OUTPUT_ROOT / version_key()), batch_size=BATCH_SIZE,
        industry_mapping=INDUSTRY_MAPPING, filename_industries=FILENAME_INDUSTRIES,
        file_indicator_properties=FILE_INDICATOR_PROPERTIES,
        formal_indicator_property=FORMAL_INDICATOR_PROPERTY,
        indicator_system_version=INDICATOR_SYSTEM_VERSION,
    )
    config.validate()
    return config


def version_key():
    from app.infrastructure.kg_to_natural_language.storage.records import digest

    return digest(DATASET_VERSION)[:24]


def database_url(password, *, with_database=True):
    from sqlalchemy.engine import URL

    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,63}", MYSQL_DATABASE):
        raise ValueError("MYSQL_DATABASE must be an ASCII identifier of at most 64 characters")
    if not MYSQL_HOST or not MYSQL_USER or type(MYSQL_PORT) is not int or not 1 <= MYSQL_PORT <= 65535:
        raise ValueError("Check MYSQL_HOST, MYSQL_USER and MYSQL_PORT")
    return URL.create(
        "mysql+pymysql", username=MYSQL_USER, password=password,
        host=MYSQL_HOST, port=MYSQL_PORT,
        database=MYSQL_DATABASE if with_database else None,
        query={"charset": "utf8mb4"},
    )


def require_mysql8(version):
    match = re.match(r"^(\d+)\.(\d+)\.(\d+)", version)
    if "mariadb" in version.lower() or not match or tuple(map(int, match.groups())) < (8, 0, 17):
        raise ValueError("Requires MySQL 8.0.17 or later; the selected instance is not supported")


def make_repository(password):
    from sqlalchemy import create_engine, text
    from app.infrastructure.kg_to_natural_language.storage.repository import KnowledgeRepository

    options = dict(pool_pre_ping=True, hide_parameters=True,
                   connect_args={"connect_timeout": 10})
    if CREATE_DATABASE_IF_MISSING:
        server = create_engine(database_url(password, with_database=False), **options)
        try:
            with server.connect() as conn:
                require_mysql8(conn.execute(text("SELECT VERSION()")).scalar_one())
                exists = conn.execute(text(
                    "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME=:name"
                ), {"name": MYSQL_DATABASE}).scalar_one_or_none()
                if exists is None:
                    # database_url validates the identifier before interpolation.
                    conn.execute(text(
                        f"CREATE DATABASE `{MYSQL_DATABASE}` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin"
                    ))
                    conn.commit()
                    logger.info("已创建目标数据库：%s", MYSQL_DATABASE)
        finally:
            server.dispose()
    engine = create_engine(database_url(password), **options)
    try:
        repository = KnowledgeRepository(engine)
        repository.preflight()
        return repository
    except BaseException:
        engine.dispose()
        raise


def prepared_directory(config, control):
    from app.infrastructure.kg_to_natural_language.storage.pipeline import prepare_knowledge, runtime_hash
    from app.infrastructure.kg_to_natural_language.storage.records import digest

    pointer = control.directory / "prepared_run.json"
    if pointer.exists():
        saved = control.read("prepared_run.json")
        directory = Path(saved["record_directory"]).resolve()
        expected = (Path(config.output_root) / "runs").resolve()
        if directory.parent != expected or directory.name == "launcher":
            raise ValueError("Saved run directory is not within this version's output directory")
        with (directory / "manifest.json").open(encoding="utf-8") as stream:
            manifest = json.load(stream)
        if (manifest["dataset_version"] != config.dataset_version
                or manifest["config_hash"] != digest(config.semantic())
                or manifest["runtime_hash"] != runtime_hash()):
            raise ValueError("配置或运行代码已变化，请设置新的 DATASET_VERSION；不要删除旧运行记录")
        logger.info("继续已冻结的运行：%s；源图谱变化后请使用新的 DATASET_VERSION", directory)
        return directory
    logger.info("[2/3] 读取四类 Neo4j 图谱并生成知识条目")
    summary = prepare_knowledge(config)
    if summary["status"] != "prepared":
        raise ValueError("未生成可入库知识条目，请检查图谱标签及运行记录；未执行入库")
    directory = Path(summary["record_directory"])
    # A failed preparation for any label must not silently publish a three-label dataset.
    with (directory / "inputs.json").open(encoding="utf-8") as stream:
        snapshots = json.load(stream)
    empty_labels = [s["source"]["graph_label"] for s in snapshots if not s["nodes"]]
    if empty_labels:
        raise ValueError("以下标签没有节点，停止整体入库：" + "、".join(empty_labels))
    control.write("prepared_run.json", {"record_directory": str(directory)})
    return directory


def run(password):
    from app.infrastructure.kg_to_natural_language.storage.local_records import RunRecords
    from app.infrastructure.kg_to_natural_language.storage.pipeline import persist_knowledge

    config = build_config()
    try:
        control = RunRecords(config.output_root, "launcher")
    except FileExistsError:
        control = RunRecords(config.output_root, "launcher", resume=True)
    with control.writer_lock():
        logger.info("[1/3] 检查 MySQL %s:%s / %s（不会启动服务，也不会连接项目默认 MySQL）",
                    MYSQL_HOST, MYSQL_PORT, MYSQL_DATABASE)
        repository = make_repository(password)
        try:
            directory = prepared_directory(config, control)
            logger.info("[3/3] 将知识条目写入同一张表并核对发布；目录：%s", directory)
            summary = persist_knowledge(directory, repository=repository,
                                        initialize_schema=INITIALIZE_TABLE)
            control.write("last_result.json", summary)
            return summary
        finally:
            repository.engine.dispose()


def main():
    from app.infrastructure.kg_to_natural_language.storage.local_records import redact
    from app.infrastructure.kg_to_natural_language.storage.records import json_default

    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(), RotatingFileHandler(
            OUTPUT_ROOT / "launcher.log", maxBytes=4 * 1024 * 1024,
            backupCount=3, encoding="utf-8")],
    )
    password = None
    try:
        password = os.environ.get(MYSQL_PASSWORD_ENV)
        if password is None:
            if not sys.stdin.isatty():
                raise ValueError(f"Set {MYSQL_PASSWORD_ENV} before non-interactive execution")
            password = getpass.getpass("请输入目标 MySQL 8 的密码（输入不显示）：")
        summary = run(password)
        print(json.dumps(summary, ensure_ascii=False, indent=2, default=json_default))
        return 0 if summary["status"] in {"published", "already_published"} else 2
    except KeyboardInterrupt:
        logger.error("运行已中断；再次执行会核对并继续已保存的运行")
        return 130
    except Exception as exc:
        message = str(exc).replace(password, "[REDACTED]") if password else str(exc)
        logger.error("执行失败（%s）：%s", type(exc).__name__, redact(message))
        if is_database_connection_error(exc):
            logger.error("请检查 MySQL 地址、端口、服务状态及账户认证")
        return 2


def is_database_connection_error(exc):
    visited = set()
    while exc is not None and id(exc) not in visited:
        visited.add(id(exc))
        original = getattr(exc, "orig", exc)
        args = getattr(original, "args", ())
        if args and args[0] in (1044, 1045, 2002, 2003, 2005, 2006, 2013):
            return True
        exc = exc.__cause__
    return False


if __name__ == "__main__":
    raise SystemExit(main())
