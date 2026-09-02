from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from app.infrastructure import kg_to_natural_language as package
from app.infrastructure.kg_to_natural_language import converter, exporter, typed_batch
from app.infrastructure.kg_to_natural_language.typed_categories import (
    ALLOWED_TYPED_GRAPH_LABELS,
    TYPED_KNOWLEDGE_CATEGORIES,
)


def _node(
    identity: int,
    graph_label: str,
    business_label: str,
    filename: str,
    **properties: Any,
) -> dict[str, Any]:
    return {
        "n": {
            "identity": identity,
            "labels": [graph_label],
            "properties": {
                "id": f"node_{identity}",
                "name": properties.pop("name", f"{business_label}_{identity}"),
                "label": business_label,
                "filename": [filename],
                **properties,
            },
            "elementId": f"4:test:{identity}",
        }
    }


class _FakeSession:
    def __init__(self, queries: list[str]) -> None:
        self.queries = queries

    def __enter__(self) -> "_FakeSession":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def run(self, query: str) -> list[dict[str, object]]:
        self.queries.append(query)
        if "RETURN n" in query:
            return [{"n": "node-object"}]
        return [{"p": "relationship-object"}]


class _FakeDriver:
    def __init__(self, queries: list[str]) -> None:
        self.queries = queries
        self.closed = False

    def verify_connectivity(self) -> None:
        return None

    def session(self, **_: object) -> _FakeSession:
        return _FakeSession(self.queries)

    def close(self) -> None:
        self.closed = True


def test_fixed_registry_and_isolated_internal_export(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert {category.name for category in TYPED_KNOWLEDGE_CATEGORIES} == {
        "法律法规条款",
        "国家标准",
        "行政监管规则",
        "英文法规",
        "合规指引",
    }
    assert ALLOWED_TYPED_GRAPH_LABELS == {
        "法律法规条款",
        "国家标准",
        "行政监管规则",
        "英文法规",
        "合规指引",
    }

    queries: list[str] = []
    driver = _FakeDriver(queries)
    monkeypatch.setattr(exporter, "_get_driver", lambda: driver)
    monkeypatch.setattr(exporter, "_node_to_dict", lambda _: {"identity": 1})
    monkeypatch.setattr(exporter, "_relationship_to_dict", lambda _: {"identity": 2})

    first = exporter.export_graph_to_directory("合规指引", tmp_path / "first")
    second = exporter.export_graph_to_directory("国家标准", tmp_path / "second")

    assert first["node_count"] == 1
    assert first["edge_count"] == 1
    assert Path(first["node_path"]).parent == tmp_path / "first"
    assert Path(second["node_path"]).parent == tmp_path / "second"
    assert "MATCH (n:`合规指引`)-[p]->(q:`合规指引`) RETURN p" in queries
    assert "MATCH (n:`国家标准`)-[p]->(q:`国家标准`) RETURN p" in queries
    with pytest.raises(ValueError, match="不支持的分类图谱标签"):
        exporter.export_graph_to_directory("任意标签", tmp_path / "invalid")


def test_empty_label_export_writes_two_empty_arrays(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class EmptySession(_FakeSession):
        def run(self, query: str) -> list[dict[str, object]]:
            self.queries.append(query)
            return []

    class EmptyDriver(_FakeDriver):
        def session(self, **_: object) -> EmptySession:
            return EmptySession(self.queries)

    monkeypatch.setattr(exporter, "_get_driver", lambda: EmptyDriver([]))
    result = exporter.export_graph_to_directory("英文法规", tmp_path)

    assert result["node_count"] == result["edge_count"] == 0
    assert json.loads((tmp_path / "node.json").read_text(encoding="utf-8")) == []
    assert json.loads((tmp_path / "edge.json").read_text(encoding="utf-8")) == []


def test_generic_converter_supports_default_explicit_and_guide_data(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    default_root = tmp_path / "default"
    explicit_input = tmp_path / "explicit-input"
    explicit_output = tmp_path / "explicit-output"
    records = [
        _node(
            1,
            "合规指引",
            "指引要求",
            "测试合规指引.md",
            name="建立合规审查机制",
            要求内容="企业应建立合规审查机制",
        )
    ]
    for input_dir in (default_root / "json", explicit_input):
        input_dir.mkdir(parents=True)
        (input_dir / "node.json").write_text(
            json.dumps(records, ensure_ascii=False), encoding="utf-8"
        )
        (input_dir / "edge.json").write_text("[]", encoding="utf-8")

    monkeypatch.setattr(converter, "_TEMP_DIR", default_root)
    default_result = converter.convert_json_to_text("合规指引")
    explicit_result = converter.convert_json_to_text(
        "合规指引",
        input_dir=explicit_input,
        output_dir=explicit_output,
    )

    assert default_result["total_entries"] > 0
    assert explicit_result["total_entries"] == default_result["total_entries"]
    assert Path(default_result["txt_path"]).parent == default_root / "txt"
    assert Path(explicit_result["txt_path"]).parent == explicit_output / "txt"
    text = Path(explicit_result["txt_path"]).read_text(encoding="utf-8")
    assert "建立合规审查机制" in text
    assert "要求内容" in text


def _write_export_sample(label: str, output_dir: str | Path) -> dict[str, Any]:
    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    if label in {"法律法规条款", "行政监管规则"}:
        nodes = [
            _node(
                1,
                label,
                "法规文件",
                f"{label}示例.txt",
                name=f"{label}示例",
                文件性质="规范性文件",
            )
        ]
    elif label == "国家标准":
        nodes = [
            _node(
                1,
                label,
                "标准文件",
                "国家标准示例.md",
                name="国家标准示例",
                标准中文名称="国家标准示例",
                标准编号="GB/T 1-2026",
                标准性质="推荐性国家标准",
            )
        ]
    elif label == "英文法规":
        nodes = [
            _node(
                1,
                label,
                "LegalDocument",
                "Example Act.md",
                name="Example Act",
                document_name="Example Act",
                document_number="Act 1 of 2026",
                document_type="Act",
            )
        ]
    else:
        nodes = [
            _node(
                1,
                label,
                "指引要求",
                "合规指引示例.md",
                name="履行合规审查义务",
                要求内容="企业应履行合规审查义务",
            )
        ]
    (output_root / "node.json").write_text(
        json.dumps(nodes, ensure_ascii=False), encoding="utf-8"
    )
    (output_root / "edge.json").write_text("[]", encoding="utf-8")
    return {
        "node_count": len(nodes),
        "edge_count": 0,
        "node_path": str(output_root / "node.json"),
        "edge_path": str(output_root / "edge.json"),
    }


def test_batch_end_to_end_statistics_empty_category_and_repeated_publish(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    empty_categories: set[str] = set()

    def export_sample(label: str, output_dir: str | Path) -> dict[str, Any]:
        if label not in empty_categories:
            return _write_export_sample(label, output_dir)
        output_root = Path(output_dir)
        output_root.mkdir(parents=True, exist_ok=True)
        (output_root / "node.json").write_text("[]", encoding="utf-8")
        (output_root / "edge.json").write_text("[]", encoding="utf-8")
        return {"node_count": 0, "edge_count": 0}

    monkeypatch.setattr(typed_batch, "export_graph_to_directory", export_sample)
    first = typed_batch.process_typed_knowledge_entries(output_dir=tmp_path / "type")

    assert set(first["categories"]) == {
        category.name for category in TYPED_KNOWLEDGE_CATEGORIES
    }
    assert first["total_knowledge_entry_count"] == sum(
        item["knowledge_entry_count"] for item in first["categories"].values()
    )
    assert first["total_knowledge_entry_count"] > 0
    for category in TYPED_KNOWLEDGE_CATEGORIES:
        output = tmp_path / "type" / category.output_filename
        assert output.is_file()
        assert sum(1 for line in output.read_text(encoding="utf-8").splitlines() if line) == (
            first["categories"][category.name]["knowledge_entry_count"]
        )

    empty_categories.add("合规指引")
    second = typed_batch.process_typed_knowledge_entries(output_dir=tmp_path / "type")
    assert second["categories"]["合规指引"]["knowledge_entry_count"] == 0
    assert (tmp_path / "type" / "合规指引.txt").read_text(encoding="utf-8") == ""
    on_disk = json.loads(
        (tmp_path / "type" / "知识条目统计.json").read_text(encoding="utf-8")
    )
    assert on_disk["categories"] == second["categories"]
    assert on_disk["total_knowledge_entry_count"] == second["total_knowledge_entry_count"]
    assert not (tmp_path / "type" / ".staging").exists()


def test_failed_conversion_and_failed_publish_preserve_previous_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output_dir = tmp_path / "type"
    monkeypatch.setattr(typed_batch, "export_graph_to_directory", _write_export_sample)
    typed_batch.process_typed_knowledge_entries(output_dir=output_dir)
    before = {
        path.name: path.read_bytes()
        for path in output_dir.iterdir()
        if path.is_file()
    }

    original_converter = typed_batch.convert_english_law_json_to_text

    def broken_converter(*_: object, **__: object) -> dict[str, Any]:
        raise RuntimeError("模拟英文法规转换失败")

    monkeypatch.setattr(typed_batch, "convert_english_law_json_to_text", broken_converter)
    with pytest.raises(typed_batch.TypedKnowledgeExportError, match="知识条目转换"):
        typed_batch.process_typed_knowledge_entries(output_dir=output_dir)
    assert {path.name: path.read_bytes() for path in output_dir.iterdir() if path.is_file()} == before

    monkeypatch.setattr(typed_batch, "convert_english_law_json_to_text", original_converter)
    original_replace = typed_batch._replace_file
    calls = 0

    def fail_once(source: Path, target: Path) -> None:
        nonlocal calls
        calls += 1
        if calls == 3:
            raise OSError("模拟发布失败")
        original_replace(source, target)

    monkeypatch.setattr(typed_batch, "_replace_file", fail_once)
    with pytest.raises(typed_batch.TypedKnowledgeExportError, match="成果发布"):
        typed_batch.process_typed_knowledge_entries(output_dir=output_dir)
    assert {path.name: path.read_bytes() for path in output_dir.iterdir() if path.is_file()} == before


def test_nonempty_category_with_no_entries_fails_validation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(typed_batch, "export_graph_to_directory", _write_export_sample)

    def empty_converter(
        graph_type: str,
        *,
        input_dir: str | Path,
        output_dir: str | Path,
    ) -> dict[str, Any]:
        del input_dir
        txt_path = Path(output_dir) / "txt" / f"{graph_type}.txt"
        txt_path.parent.mkdir(parents=True, exist_ok=True)
        txt_path.write_text("", encoding="utf-8")
        return {"total_entries": 0, "source_files": 0, "txt_path": str(txt_path)}

    monkeypatch.setattr(typed_batch, "convert_json_to_text_v2", empty_converter)
    with pytest.raises(
        typed_batch.TypedKnowledgeExportError,
        match="图谱包含节点，但配置的转换器未生成任何知识条目",
    ):
        typed_batch.process_typed_knowledge_entries(output_dir=tmp_path / "type")


def test_package_function_is_exported_and_propagates_staged_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = {"status": "success", "total_knowledge_entry_count": 5}
    monkeypatch.setattr(package, "process_typed_knowledge_entries", lambda: expected)
    assert "process_graphs_by_type" in package.__all__
    assert package.process_graphs_by_type() is expected

    def fail() -> dict[str, Any]:
        raise typed_batch.TypedKnowledgeExportError("Neo4j 导出", "连接失败", "国家标准")

    monkeypatch.setattr(package, "process_typed_knowledge_entries", fail)
    with pytest.raises(typed_batch.TypedKnowledgeExportError, match="Neo4j 导出"):
        package.process_graphs_by_type()


def test_typed_batch_has_no_mysql_or_minio_dependencies() -> None:
    source = Path(typed_batch.__file__).read_text(encoding="utf-8").lower()
    assert "mysql" not in source
    assert "minio" not in source
