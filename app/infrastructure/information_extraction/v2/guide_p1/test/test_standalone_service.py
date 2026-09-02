from __future__ import annotations

import ast
import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from app.infrastructure.information_extraction.v2.guide_p1.config import DEFAULT_CONFIG
from app.infrastructure.information_extraction.v2.guide_p1.standalone import GuideP1StandaloneRunner
from app.infrastructure.information_extraction.v2.guide_p1.stats import ResultStats


class FakeGuideP1Extractor:
    def __init__(self, config) -> None:
        self.config = config
        self.result_stats = ResultStats(
            total_files=1,
            success_files=1,
            weak_warning=2,
            strong_warning=1,
            node_count=2,
            edge_count=1,
            weak_warning_messages=["weak-1", "weak-2"],
            strong_warning_messages=["strong-1"],
        )

    async def extract_file(self, input_path, output_dir, run_llm=True):
        assert run_llm is True
        graph = {
            "nodes": [
                {
                    "node_id": "document-1",
                    "node_name": "示例规划",
                    "node_type": "合规指引文件",
                    "properties": {},
                    "filename": Path(input_path).name,
                },
                {
                    "node_id": "knowledge-1",
                    "node_name": "推进示例任务",
                    "node_type": "指引知识单元",
                    "properties": {"知识内容": "推进示例任务"},
                    "filename": Path(input_path).name,
                },
            ],
            "edges": [
                {
                    "edge_id": "edge-1",
                    "source_id": "document-1",
                    "target_id": "knowledge-1",
                    "relation_type": "提出",
                    "properties": {},
                    "filename": Path(input_path).name,
                }
            ],
            "metadata": {},
        }
        return {
            "status": "success",
            "formal_graph_eligible": True,
            "neo4j_graph": graph,
        }


class FakeGraphStorage:
    def __init__(self) -> None:
        self.saved_graphs = []
        self.merged_graphs = []
        self.deleted_graphs = []

    def connect(self) -> None:
        return None

    def disconnect(self) -> None:
        return None

    def add_subgraph_with_merge(self, graph, graph_name, graph_level, filename=None):
        self.saved_graphs.append((graph_name, graph_level, filename, graph))
        return True

    def merge_graphs(self, source_graph, target_graph):
        self.merged_graphs.append((source_graph, target_graph))
        return True

    def delete_subgraph(self, graph_name):
        self.deleted_graphs.append(graph_name)
        return True


class GuideP1StandaloneServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_standalone_service_uses_local_output_and_reports_stats(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            input_dir = root / "input"
            output_dir = root / "output"
            input_dir.mkdir()
            (input_dir / "示例规划.txt").write_text("示例规划\n一、重点任务\n推进示例任务。", encoding="utf-8")

            graph_storage = FakeGraphStorage()
            config = replace(DEFAULT_CONFIG, strict_mode=False)
            runner = GuideP1StandaloneRunner(
                graph_storage=graph_storage,
                config=config,
                extractor_factory=FakeGuideP1Extractor,
            )
            with self.assertLogs(level="INFO") as captured:
                summary = await runner.run(
                    guide_data_dir=str(input_dir),
                    output_dir=str(output_dir),
                    if_del_task=True,
                    kg_graph_name="guide_p1_test_graph",
                )

            self.assertFalse(summary["use_mysql"])
            self.assertFalse(summary["use_minio"])
            self.assertTrue(summary["strict_mode"])
            self.assertEqual(1, summary["success"])
            self.assertEqual(0, summary["failed"])
            self.assertEqual(2, summary["stats"]["weak_warning"])
            self.assertEqual(1, summary["stats"]["strong_warning"])
            self.assertEqual(1, len(graph_storage.saved_graphs))
            self.assertEqual(1, len(graph_storage.merged_graphs))
            self.assertEqual(1, len(graph_storage.deleted_graphs))

            summary_path = output_dir / "service_summary.json"
            self.assertTrue(summary_path.exists())
            saved_summary = json.loads(summary_path.read_text(encoding="utf-8"))
            self.assertEqual(summary["stats"], saved_summary["stats"])
            self.assertTrue(captured.output[-2].endswith(": 2"))
            self.assertTrue(captured.output[-1].endswith(": 1"))

    def test_standalone_route_has_no_database_dependency(self) -> None:
        endpoint_path = (
            Path(__file__).parents[5] / "api" / "v1" / "endpoints" / "kg.py"
        )
        tree = ast.parse(endpoint_path.read_text(encoding="utf-8"))
        function = next(
            node
            for node in tree.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name == "guide_p1_extract_by_dir_standalone"
        )
        argument_names = {argument.arg for argument in function.args.args}
        self.assertNotIn("db", argument_names)
        function_text = ast.get_source_segment(endpoint_path.read_text(encoding="utf-8"), function)
        self.assertNotIn("get_db", function_text)
        self.assertNotIn("file_storage", function_text)


if __name__ == "__main__":
    unittest.main()
