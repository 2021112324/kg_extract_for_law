import inspect
import logging
import os
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from fastapi import BackgroundTasks

from app.api.v1.endpoints.kg import clause_extract_by_dir_standalone
from app.services.core import kg_service as kg_service_module
from app.services.core.kg_service import KGService


VALID_GRAPH = {
    "nodes": [
        {
            "node_id": "law_1",
            "node_name": "Test Law",
            "node_type": "regulation",
            "properties": {},
        },
        {
            "node_id": "article_1",
            "node_name": "Article 1",
            "node_type": "article",
            "properties": {},
        },
    ],
    "edges": [
        {
            "source_id": "law_1",
            "target_id": "article_1",
            "relation_type": "contains",
            "properties": {},
        }
    ],
}


class FakeResultStats:
    error = 0
    week_warning = 0
    strong_warning = 0
    error_msg = ""
    week_warning_msg = ""
    strong_warning_msg = ""


class FakeExtractor:
    def __init__(self, results):
        self.results = results
        self.result_stats = FakeResultStats()
        self.logging_calls = 0

    async def extract_clauses(self, filename, text):
        result = self.results[filename]
        if isinstance(result, Exception):
            raise result
        return result

    async def logging_result_stats(self):
        self.logging_calls += 1


class FakeGraphStorage:
    def __init__(self, save_results=None, merge_error=None):
        self.save_results = list(save_results or [])
        self.merge_error = merge_error
        self.saved = []
        self.merged = []
        self.deleted = []
        self.connect_calls = 0
        self.disconnect_calls = 0

    def connect(self):
        self.connect_calls += 1
        return True

    def disconnect(self):
        self.disconnect_calls += 1

    def add_subgraph_with_merge(self, graph, graph_name, graph_level, **kwargs):
        self.saved.append((graph_name, graph, graph_level, kwargs))
        return self.save_results.pop(0) if self.save_results else True

    def merge_graphs(self, source, target):
        self.merged.append((source, target))
        return SimpleNamespace(error=self.merge_error, node_count=2, edge_count=1)

    def delete_subgraph(self, graph_name):
        self.deleted.append(graph_name)
        return True


class FakeSession:
    def __init__(self):
        self.tasks = []
        self.add_calls = 0
        self.commit_calls = 0

    def add(self, item):
        self.add_calls += 1
        if item not in self.tasks:
            self.tasks.append(item)

    def flush(self):
        if self.tasks and getattr(self.tasks[-1], "id", None) is None:
            self.tasks[-1].id = 1

    def commit(self):
        self.commit_calls += 1

    def refresh(self, item):
        return item

    def query(self, model):
        return self

    def filter(self, *args, **kwargs):
        return self

    def all(self):
        return [task for task in self.tasks if getattr(task, "status", None) == 2]


def make_service(results, storage=None):
    service = KGService.__new__(KGService)
    service.clause_extractor = FakeExtractor(results)
    service.graph_storage = storage or FakeGraphStorage()
    return service


def test_standalone_graph_name_is_a_valid_neo4j_identifier():
    graph_name = KGService.generate_standalone_clause_graph_name("法规 目录（2026）/测试-文件")

    assert graph_name.startswith("zh_law_")
    assert graph_name.isidentifier()


@pytest.mark.asyncio
async def test_startup_can_skip_mysql_and_minio(monkeypatch):
    import app.main as main

    init_db = Mock()
    initialize_minio = Mock()
    monkeypatch.setattr(main, "init_db", init_db)
    monkeypatch.setattr(main, "initialize_minio", initialize_minio)
    monkeypatch.setattr(main.settings, "MYSQL_SKIP_INIT", True)
    monkeypatch.setattr(main.settings, "MINIO_SKIP_INIT", True)

    await main.startup_db_client()

    init_db.assert_not_called()
    initialize_minio.assert_not_called()


@pytest.mark.asyncio
async def test_startup_keeps_default_initialization(monkeypatch):
    import app.main as main

    init_db = Mock()
    initialize_minio = Mock()
    monkeypatch.setattr(main, "init_db", init_db)
    monkeypatch.setattr(main, "initialize_minio", initialize_minio)
    monkeypatch.setattr(main.settings, "MYSQL_SKIP_INIT", False)
    monkeypatch.setattr(main.settings, "MINIO_SKIP_INIT", False)

    await main.startup_db_client()

    init_db.assert_called_once_with()
    initialize_minio.assert_called_once_with()


@pytest.mark.asyncio
async def test_standalone_endpoint_has_no_db_dependency_and_returns_graph_name(tmp_path):
    signature = inspect.signature(clause_extract_by_dir_standalone)
    assert "db" not in signature.parameters

    background_tasks = BackgroundTasks()
    response = await clause_extract_by_dir_standalone(
        background_tasks=background_tasks,
        data_dir=str(tmp_path),
        if_del_task=True,
    )

    assert response["code"] == 200
    assert response["data"]["use_mysql"] is False
    assert response["data"]["kg_graph_name"].startswith("zh_law_")
    assert len(background_tasks.tasks) == 1
    params = background_tasks.tasks[0].args[1]
    assert params["db"] is None
    assert params["use_mysql"] is False
    assert params["kg_graph_name"] == response["data"]["kg_graph_name"]


@pytest.mark.asyncio
async def test_standalone_mode_merges_successes_and_reports_partial_failure(tmp_path):
    (tmp_path / "success.txt").write_text("valid", encoding="utf-8")
    (tmp_path / "failed.md").write_text("invalid", encoding="utf-8")
    storage = FakeGraphStorage()
    service = make_service(
        {
            "success": VALID_GRAPH,
            "failed": RuntimeError("model request failed"),
        },
        storage,
    )

    summary = await service.clause_extract_by_local_dir(
        clause_file_dir=str(tmp_path),
        if_del_task=True,
        db=None,
        use_mysql=False,
        kg_graph_name="target_graph",
    )

    assert summary["use_mysql"] is False
    assert summary["total"] == 2
    assert summary["success"] == 1
    assert summary["failed"] == 1
    assert summary["errors"][0]["file"] == "failed.md"
    assert len(storage.saved) == 1
    assert storage.merged == [(storage.saved[0][0], "target_graph")]
    assert storage.deleted == [storage.saved[0][0]]


@pytest.mark.asyncio
async def test_standalone_completion_log_is_concise_and_stats_are_last(tmp_path, caplog):
    (tmp_path / "law.txt").write_text("valid", encoding="utf-8")
    service = make_service({"law": VALID_GRAPH})
    service.clause_extractor.result_stats.week_warning = 2
    service.clause_extractor.result_stats.strong_warning = 1
    service.clause_extractor.result_stats.week_warning_msg = "不应出现在完成摘要中的超长详情"

    async def log_stats():
        logging.info("测试统计尾行")

    service.clause_extractor.logging_result_stats = log_stats
    caplog.set_level(logging.INFO)

    summary = await service.clause_extract_by_local_dir(
        clause_file_dir=str(tmp_path),
        if_del_task=True,
        db=None,
        use_mysql=False,
        kg_graph_name="target_graph",
    )

    completion = next(
        record.getMessage()
        for record in caplog.records
        if record.getMessage().startswith("无 MySQL 中文法规目录抽取完成:")
    )
    assert "不应出现在完成摘要中的超长详情" not in completion
    assert '"weak_warning": 2' in completion
    assert '"strong_warning": 1' in completion
    assert caplog.records[-1].getMessage() == "测试统计尾行"
    assert "不应出现在完成摘要中的超长详情" in summary["extractor_stats"]["weak_warning_msg"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("graph", "save_results"),
    [
        ({"nodes": [], "edges": []}, None),
        (VALID_GRAPH, [False]),
    ],
)
async def test_standalone_mode_rejects_invalid_or_unsaved_graphs(tmp_path, graph, save_results):
    (tmp_path / "law.txt").write_text("law", encoding="utf-8")
    storage = FakeGraphStorage(save_results=save_results)
    service = make_service({"law": graph}, storage)

    with pytest.raises(RuntimeError, match="没有成功入库"):
        await service.clause_extract_by_local_dir(
            clause_file_dir=str(tmp_path),
            if_del_task=False,
            db=None,
            use_mysql=False,
            kg_graph_name="target_graph",
        )

    assert storage.merged == []


@pytest.mark.asyncio
async def test_managed_mode_keeps_mysql_task_flow(tmp_path, monkeypatch):
    (tmp_path / "law.txt").write_text("law", encoding="utf-8")
    storage = FakeGraphStorage()
    service = make_service({"law": VALID_GRAPH}, storage)
    session = FakeSession()
    create_kg = AsyncMock(
        return_value={
            "data": {
                "id": 10,
                "name": "managed_law",
                "graph_name": "managed_target",
            }
        }
    )
    monkeypatch.setattr(kg_service_module.kg_service, "create_kg", create_kg)

    result = await service.clause_extract_by_local_dir(
        clause_file_dir=str(tmp_path),
        if_del_task=False,
        db=session,
    )

    assert result is True
    create_kg.assert_awaited_once()
    assert session.add_calls >= 2
    assert session.commit_calls >= 2
    assert len(storage.saved) == 1
    assert storage.merged == [(storage.saved[0][0], "managed_target")]


@pytest.mark.asyncio
async def test_standalone_mode_rejects_database_session(tmp_path):
    service = make_service({})

    with pytest.raises(ValueError, match="不得传入数据库会话"):
        await service.clause_extract_by_local_dir(
            clause_file_dir=str(tmp_path),
            if_del_task=False,
            db=FakeSession(),
            use_mysql=False,
        )


@pytest.mark.asyncio
@pytest.mark.skipif(
    os.getenv("RUN_STANDALONE_ZH_LAW_INTEGRATION") != "1",
    reason="requires the configured LLM and Neo4j services",
)
async def test_real_standalone_extraction_reaches_neo4j(tmp_path):
    from app.services.core.kg_service import kg_service

    source_file = tmp_path / "standalone_smoke_law.txt"
    source_file.write_text(
        "测试数据安全规定\n"
        "发布单位：测试机关\n"
        "发布日期：2026年8月27日\n"
        "生效日期：2026年8月27日\n\n"
        "第一条 为规范数据处理活动，保护数据安全，制定本规定。\n\n"
        "第二条 企业处理重要数据时，应当建立数据安全管理制度。\n",
        encoding="utf-8",
    )
    target_graph = kg_service.generate_standalone_clause_graph_name("standalone_smoke")

    try:
        summary = await kg_service.clause_extract_by_local_dir(
            clause_file_dir=str(tmp_path),
            if_del_task=True,
            db=None,
            use_mysql=False,
            kg_graph_name=target_graph,
        )
        assert summary["success"] == 1
        kg_service.graph_storage.connect()
        stats = kg_service.graph_storage.get_subgraph_stats(target_graph)
        assert stats.error is None
        assert stats.node_count > 0
    finally:
        try:
            kg_service.graph_storage.connect()
            kg_service.graph_storage.delete_subgraph(target_graph)
            kg_service.graph_storage.disconnect()
        except Exception:
            pass
