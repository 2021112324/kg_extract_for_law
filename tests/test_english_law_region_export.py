from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from app.infrastructure import kg_to_natural_language as package
from app.infrastructure.kg_to_natural_language.batch import region_processor


def _write_mapping(path: Path, mapping: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(mapping, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _fake_export(label: str, output_dir: str | Path) -> dict[str, Any]:
    assert label == "英文法规"
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    (root / "node.json").write_text("[]", encoding="utf-8")
    (root / "edge.json").write_text("[]", encoding="utf-8")
    return {
        "node_count": 8,
        "edge_count": 5,
        "node_path": str(root / "node.json"),
        "edge_path": str(root / "edge.json"),
    }


def _install_converter(
    monkeypatch: pytest.MonkeyPatch,
    records: list[dict[str, str]],
) -> list[tuple[str, Path, Path]]:
    calls: list[tuple[str, Path, Path]] = []

    def fake_converter(
        graph_type: str,
        *,
        input_dir: str | Path,
        output_dir: str | Path,
    ) -> dict[str, Any]:
        input_root = Path(input_dir)
        output_root = Path(output_dir)
        calls.append((graph_type, input_root, output_root))
        assert (input_root / "node.json").is_file()
        jsonl_path = output_root / "jsonl" / "英文法规.jsonl"
        txt_path = output_root / "txt" / "英文法规.txt"
        csv_path = output_root / "csv" / "英文法规.csv"
        jsonl_path.parent.mkdir(parents=True, exist_ok=True)
        txt_path.parent.mkdir(parents=True, exist_ok=True)
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        jsonl_path.write_text(
            "".join(
                json.dumps(record, ensure_ascii=False) + "\n"
                for record in records
            ),
            encoding="utf-8",
        )
        txt_path.write_text(
            "".join(record["content"] + "\n" for record in records),
            encoding="utf-8",
        )
        csv_path.write_text("source_file,content\n", encoding="utf-8")
        return {
            "total_entries": len(records),
            "source_files": len({record.get("source_file") for record in records}),
            "txt_path": str(txt_path),
            "jsonl_path": str(jsonl_path),
            "csv_path": str(csv_path),
        }

    monkeypatch.setattr(
        region_processor,
        "convert_english_law_json_to_text",
        fake_converter,
    )
    return calls


def test_region_export_splits_once_preserves_order_and_counts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mapping_path = tmp_path / "filename到region映射.json"
    output_dir = tmp_path / "type" / "地区"
    combined_path = output_dir.parent / "英文法规.txt"
    _write_mapping(
        mapping_path,
        {
            "US Act.md": "美国",
            "EU Directive.md": "欧盟及其成员国",
            "Unused.md": "美国",
        },
    )
    combined_path.parent.mkdir(parents=True)
    combined_path.write_bytes(b"existing combined artifact\n")
    before_combined = combined_path.read_bytes()
    records = [
        {"source_file": "US Act.md", "content": "US knowledge 1."},
        {"source_file": "EU Directive.md", "content": "EU knowledge 1."},
        {"source_file": "US Act.md", "content": "US knowledge 2."},
    ]
    monkeypatch.setattr(region_processor, "export_graph_to_directory", _fake_export)
    calls = _install_converter(monkeypatch, records)

    result = region_processor.process_english_law_knowledge_by_region(
        mapping_path=mapping_path,
        output_dir=output_dir,
    )

    assert len(calls) == 1
    assert calls[0][0] == "英文法规"
    assert (output_dir / "美国.txt").read_text(encoding="utf-8") == (
        "US knowledge 1.\nUS knowledge 2.\n"
    )
    assert (output_dir / "欧盟及其成员国.txt").read_text(encoding="utf-8") == (
        "EU knowledge 1.\n"
    )
    assert result["knowledge_entry_count"] == 3
    assert result["classified_knowledge_entry_count"] == 3
    assert result["count_conservation_valid"] is True
    assert result["regions"]["美国"]["knowledge_entry_count"] == 2
    assert result["regions"]["欧盟及其成员国"]["knowledge_entry_count"] == 1
    assert result["mapping"]["unused_filenames"] == ["Unused.md"]
    assert combined_path.read_bytes() == before_combined
    assert not (output_dir / ".staging").exists()

    on_disk = json.loads(
        (output_dir / "知识条目统计.json").read_text(encoding="utf-8")
    )
    assert on_disk == result


def test_filename_normalization_and_zero_knowledge_region(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mapping_path = tmp_path / "mapping.json"
    output_dir = tmp_path / "regions"
    _write_mapping(mapping_path, {" Law.MD ": "美国"})
    records = [
        {"source_file": r"C:\laws\LAW.md", "content": "Only US knowledge."},
    ]
    monkeypatch.setattr(region_processor, "export_graph_to_directory", _fake_export)
    _install_converter(monkeypatch, records)

    result = region_processor.process_english_law_knowledge_by_region(
        mapping_path=mapping_path,
        output_dir=output_dir,
    )

    assert result["regions"]["美国"]["knowledge_entry_count"] == 1
    assert result["regions"]["欧盟及其成员国"]["knowledge_entry_count"] == 0
    assert (output_dir / "欧盟及其成员国.txt").read_text(encoding="utf-8") == ""


@pytest.mark.parametrize("region", ["", "United States", "英国"])
def test_invalid_or_empty_region_fails_before_export(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    region: str,
) -> None:
    mapping_path = tmp_path / "mapping.json"
    _write_mapping(mapping_path, {"Law.md": region})
    called = False

    def should_not_export(*_: object, **__: object) -> dict[str, Any]:
        nonlocal called
        called = True
        return {}

    monkeypatch.setattr(region_processor, "export_graph_to_directory", should_not_export)
    with pytest.raises(
        region_processor.EnglishLawRegionExportError,
        match="地区映射校验",
    ):
        region_processor.process_english_law_knowledge_by_region(
            mapping_path=mapping_path,
            output_dir=tmp_path / "regions",
        )
    assert called is False


def test_normalized_mapping_conflict_fails(
    tmp_path: Path,
) -> None:
    mapping_path = tmp_path / "mapping.json"
    _write_mapping(
        mapping_path,
        {"Law.md": "美国", "LAW.MD": "欧盟及其成员国"},
    )
    with pytest.raises(
        region_processor.EnglishLawRegionExportError,
        match="规范化filename存在地区冲突",
    ):
        region_processor.process_english_law_knowledge_by_region(
            mapping_path=mapping_path,
            output_dir=tmp_path / "regions",
        )


@pytest.mark.parametrize(
    ("record", "message"),
    [
        ({"source_file": "Missing.md", "content": "Knowledge."}, "未映射知识"),
        ({"source_file": "", "content": "Knowledge."}, "无效知识"),
    ],
)
def test_missing_mapping_or_source_preserves_existing_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    record: dict[str, str],
    message: str,
) -> None:
    mapping_path = tmp_path / "mapping.json"
    output_dir = tmp_path / "regions"
    _write_mapping(mapping_path, {"Known.md": "美国"})
    output_dir.mkdir()
    old_files = {
        "美国.txt": b"old-us\n",
        "欧盟及其成员国.txt": b"old-eu\n",
        "知识条目统计.json": b'{"old": true}\n',
    }
    for name, content in old_files.items():
        (output_dir / name).write_bytes(content)
    monkeypatch.setattr(region_processor, "export_graph_to_directory", _fake_export)
    _install_converter(monkeypatch, [record])

    with pytest.raises(region_processor.EnglishLawRegionExportError, match=message):
        region_processor.process_english_law_knowledge_by_region(
            mapping_path=mapping_path,
            output_dir=output_dir,
        )
    assert {name: (output_dir / name).read_bytes() for name in old_files} == old_files


@pytest.mark.parametrize("failure_stage", ["export", "convert"])
def test_export_or_conversion_failure_preserves_existing_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_stage: str,
) -> None:
    mapping_path = tmp_path / "mapping.json"
    output_dir = tmp_path / "regions"
    _write_mapping(mapping_path, {"Law.md": "美国"})
    output_dir.mkdir()
    (output_dir / "美国.txt").write_text("old\n", encoding="utf-8")

    if failure_stage == "export":
        monkeypatch.setattr(
            region_processor,
            "export_graph_to_directory",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("offline")),
        )
        expected = "Neo4j导出"
    else:
        monkeypatch.setattr(region_processor, "export_graph_to_directory", _fake_export)
        monkeypatch.setattr(
            region_processor,
            "convert_english_law_json_to_text",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("broken")),
        )
        expected = "知识条目转换"

    with pytest.raises(region_processor.EnglishLawRegionExportError, match=expected):
        region_processor.process_english_law_knowledge_by_region(
            mapping_path=mapping_path,
            output_dir=output_dir,
        )
    assert (output_dir / "美国.txt").read_text(encoding="utf-8") == "old\n"


def test_publish_failure_restores_all_previous_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mapping_path = tmp_path / "mapping.json"
    output_dir = tmp_path / "regions"
    _write_mapping(mapping_path, {"Law.md": "美国"})
    output_dir.mkdir()
    old_files = {
        "美国.txt": b"old-us\n",
        "欧盟及其成员国.txt": b"old-eu\n",
        "知识条目统计.json": b'{"old": true}\n',
    }
    for name, content in old_files.items():
        (output_dir / name).write_bytes(content)
    monkeypatch.setattr(region_processor, "export_graph_to_directory", _fake_export)
    _install_converter(
        monkeypatch,
        [{"source_file": "Law.md", "content": "New knowledge."}],
    )
    original_replace = region_processor._replace_file
    calls = 0

    def fail_second(source: Path, target: Path) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("simulated publish failure")
        original_replace(source, target)

    monkeypatch.setattr(region_processor, "_replace_file", fail_second)
    with pytest.raises(
        region_processor.EnglishLawRegionExportError,
        match="成果发布",
    ):
        region_processor.process_english_law_knowledge_by_region(
            mapping_path=mapping_path,
            output_dir=output_dir,
        )
    assert {name: (output_dir / name).read_bytes() for name in old_files} == old_files


def test_package_api_and_region_module_dependencies() -> None:
    assert package.process_english_law_knowledge_by_region is (
        region_processor.process_english_law_knowledge_by_region
    )
    assert "process_english_law_knowledge_by_region" in package.__all__
    source = Path(region_processor.__file__).read_text(encoding="utf-8").lower()
    assert "mysql" not in source
    assert "minio" not in source
    assert "api_key" not in source
    assert "model_name" not in source
