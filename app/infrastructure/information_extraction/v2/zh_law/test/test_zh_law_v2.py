from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path

import pytest

from app.infrastructure.information_extraction.base import Entity, Relationship
from app.infrastructure.information_extraction.law_extract.clause_extract import (
    ClauseExtractor as V1ClauseExtractor,
    ResultStats as V1ResultStats,
)
from app.infrastructure.information_extraction.v2.zh_law import ClauseExtractor, ZhLawConfig
from app.infrastructure.information_extraction.v2.zh_law.clause_extract import (
    _normalize_function_type,
    _normalize_law_properties,
    _normalize_quant_condition,
    _normalize_risk_type,
)


SAMPLE_LAW = """中华人民共和国示例法
发布机关：示例机关

第一章 总则
第一条 企业应当建立管理制度，并依照《中华人民共和国民法典》第五条执行。
第二条 企业可以依法申请许可。
第二条之一 企业不得伪造申请材料。
"""


class FakeExtractor:
    def __init__(self, fail_clause: str = "", empty_clause: str = "", malformed_relation: bool = False):
        self.fail_clause = fail_clause
        self.empty_clause = empty_clause
        self.malformed_relation = malformed_relation

    async def entity_and_relationship_extract(self, *, input_text: str, **_kwargs):
        if "文件描述" in input_text:
            return {
                "entities": [
                    Entity(
                        name="中华人民共和国示例法",
                        entity_type="法规文件",
                        properties={
                            "文件全称": "中华人民共和国示例法",
                            "合规风险类型": "产品法律风险,企业信用风险",
                            "合规域": "企业管理",
                            "适用行业": "其他",
                        },
                    )
                ],
                "relations": [],
            }

        clause_number = next(
            number for number in ("第二条之一", "第一条", "第二条") if number in input_text
        )
        if clause_number == self.fail_clause:
            raise RuntimeError("simulated model failure")
        if clause_number == self.empty_clause:
            return {"entities": [], "relations": []}

        law = Entity(
            name=f"中华人民共和国示例法{clause_number}",
            entity_type="法条",
            properties={"核心主题": "企业管理", "经济行业": ["通用"]},
        )
        unit_name = f"{clause_number}第一款"
        content = "企业应当建立管理制度" if clause_number == "第一条" else "企业可以依法申请许可"
        if clause_number == "第二条之一":
            content = "企业不得伪造申请材料"
        unit = Entity(
            name=unit_name,
            entity_type="条款单元",
            properties={
                "条款单元内容": content,
                "单元编号": unit_name,
                "功能类型": "义务" if clause_number == "第一条" else "权利",
                "经济行业": ["通用"],
                "量化条件": None,
            },
        )
        entities = [law, unit]
        relations = []
        if clause_number == "第一条":
            reference = Entity(
                name="中华人民共和国民法典第五条",
                entity_type="引用依据",
                properties={
                    "引用类型": "条款",
                    "文件全称": "中华人民共和国民法典",
                    "条款编号": "第五条",
                    "是否本文件内引用": "否",
                },
            )
            entities.append(reference)
            relations.append(
                Relationship(
                    source="条款单元_不存在" if self.malformed_relation else f"条款单元_{unit_name}",
                    target="引用依据_中华人民共和国民法典第五条",
                    type="引用",
                )
            )
        return {"entities": entities, "relations": relations}


def config(**overrides) -> ZhLawConfig:
    values = {
        "model_name": "test-model",
        "api_key": "secret-test-key",
        "api_url": "http://example.invalid/v1/chat/completions",
        "max_char_buffer": 7500,
        "batch_length": 5,
        "max_workers": 3,
        "timeout": 30,
        "max_retries": 1,
        "max_concurrent": 3,
        "lenient_mode": False,
    }
    values.update(overrides)
    return ZhLawConfig(**values)


def make_extractor(fake: FakeExtractor | None = None, *, lenient: bool = False) -> ClauseExtractor:
    return ClauseExtractor(config=config(lenient_mode=lenient), extractor=fake or FakeExtractor())


@pytest.mark.asyncio
async def test_split_preserves_hierarchy_and_inserted_article():
    extractor = make_extractor()
    result = await extractor.split_clause(SAMPLE_LAW)
    assert [item["条款编号"] for item in result["clauses"]] == ["第一条", "第二条", "第二条之一"]
    assert all(item["章"] == "第一章 总则" for item in result["clauses"])
    assert "中华人民共和国示例法" in result["file_info"]


@pytest.mark.asyncio
async def test_v1_v2_split_regression():
    v1 = V1ClauseExtractor.__new__(V1ClauseExtractor)
    v1.result_stats = V1ResultStats()
    v2 = make_extractor()
    assert await v2.split_clause(SAMPLE_LAW) == await v1.split_clause(SAMPLE_LAW)


@pytest.mark.asyncio
async def test_v1_v2_graph_shape_regression():
    v1 = V1ClauseExtractor.__new__(V1ClauseExtractor)
    v1.extractor = FakeExtractor()
    v1.semaphore = asyncio.Semaphore(3)
    v1.result_stats = V1ResultStats()
    v1.lenient_mode = False
    v2 = make_extractor()

    v1_kg = await v1.extract_clauses("中华人民共和国示例法.txt", SAMPLE_LAW)
    v2_kg = await v2.extract_clauses("中华人民共和国示例法.txt", SAMPLE_LAW)

    assert sorted(node["node_type"] for node in v2_kg["nodes"]) == sorted(
        node["node_type"] for node in v1_kg["nodes"]
    )
    assert sorted(edge["relation_type"] for edge in v2_kg["edges"]) == sorted(
        edge["relation_type"] for edge in v1_kg["edges"]
    )
    v1_units = [node["properties"] for node in v1_kg["nodes"] if node["node_type"] == "条款单元"]
    v2_units = [node["properties"] for node in v2_kg["nodes"] if node["node_type"] == "条款单元"]
    assert v2_units == v1_units


def test_property_normalization_is_migrated():
    assert _normalize_risk_type("产品法律风险,企业信用风险,未知风险") == [
        "产品法律风险",
        "企业信用风险",
    ]
    assert _normalize_function_type("义务", "企业不得实施该行为") == "禁止"
    assert _normalize_quant_condition("五千元以上六千元以下的罚款") == {
        "原文件": "五千元以上六千元以下的罚款",
        "量化值类型": "金额",
        "最小值": 5000,
        "最大值": 6000,
        "单位": "元",
        "约束关系": "区间",
    }
    normalized = _normalize_law_properties({"适用行业": "其他"}, is_file_info=True)
    assert normalized["经济行业"] == ["通用"]


def test_config_can_be_loaded_from_environment(monkeypatch):
    monkeypatch.setenv("ZH_LAW_MODEL", "env-model")
    monkeypatch.setenv("ZH_LAW_MAX_CONCURRENT", "7")
    monkeypatch.setenv("ZH_LAW_LENIENT_MODE", "true")
    loaded = ZhLawConfig.from_env()
    assert loaded.model_name == "env-model"
    assert loaded.max_concurrent == 7
    assert loaded.lenient_mode is True


@pytest.mark.asyncio
async def test_successful_extraction_builds_graph_and_stats():
    extractor = make_extractor()
    kg = await extractor.extract_clauses("中华人民共和国示例法.txt", SAMPLE_LAW)
    node_types = {node["node_type"] for node in kg["nodes"]}
    relation_types = {edge["relation_type"] for edge in kg["edges"]}
    assert {"法规文件", "法条", "条款单元", "引用依据"} <= node_types
    assert {"包含", "涉及"} <= relation_types
    assert kg["metadata"]["status"] == "complete"
    assert kg["metadata"]["model_success"] is True
    assert extractor.get_run_summary()["success_files"] == 1


@pytest.mark.asyncio
async def test_strict_mode_propagates_clause_failure():
    extractor = make_extractor(FakeExtractor(fail_clause="第二条"))
    with pytest.raises(ValueError, match="存在处理失败的法条"):
        await extractor.extract_clauses("中华人民共和国示例法.txt", SAMPLE_LAW)
    summary = extractor.get_run_summary()
    assert summary["success_files"] == 0
    assert summary["extraction_error"] >= 1
    assert summary["file_processing_error"] == 1
    assert extractor.last_extraction_status["status"] == "failed"


@pytest.mark.asyncio
async def test_empty_model_result_is_not_success():
    extractor = make_extractor(FakeExtractor(empty_clause="第二条"))
    with pytest.raises(ValueError, match="存在处理失败的法条"):
        await extractor.extract_clauses("中华人民共和国示例法.txt", SAMPLE_LAW)
    assert extractor.get_run_summary()["extraction_error"] >= 1


@pytest.mark.asyncio
async def test_lenient_mode_marks_incomplete_without_model_success():
    extractor = make_extractor(FakeExtractor(fail_clause="第二条"), lenient=True)
    kg = await extractor.extract_clauses("中华人民共和国示例法.txt", SAMPLE_LAW)
    assert kg["metadata"]["status"] == "incomplete"
    assert kg["metadata"]["model_success"] is False
    assert kg["metadata"]["successful_clause_count"] == 2
    assert kg["metadata"]["failed_clause_count"] == 1
    assert extractor.get_run_summary()["success_files"] == 0
    assert extractor.get_run_summary()["strong_warning"] >= 1


@pytest.mark.asyncio
async def test_unresolved_relation_is_strong_warning():
    extractor = make_extractor(FakeExtractor(malformed_relation=True))
    kg = await extractor.extract_clauses("中华人民共和国示例法.txt", SAMPLE_LAW)
    assert kg["metadata"]["model_success"] is True
    assert extractor.get_run_summary()["strong_warning"] >= 1


@pytest.mark.asyncio
async def test_file_entry_saves_separate_stage_results(tmp_path: Path):
    source = tmp_path / "中华人民共和国示例法.txt"
    source.write_text(SAMPLE_LAW, encoding="utf-8")
    output_dir = tmp_path / "result"
    extractor = make_extractor()
    kg = await extractor.extract_file_to_kg(source, output_dir)
    for stage in ("split", "extraction", "summary", "kg"):
        assert (output_dir / f"中华人民共和国示例法_{stage}.json").is_file()
    saved = json.loads((output_dir / "中华人民共和国示例法_kg.json").read_text(encoding="utf-8"))
    assert saved["metadata"]["status"] == "complete"
    assert kg["output_paths"]["kg"].endswith("_kg.json")


@pytest.mark.asyncio
async def test_stats_reset_between_independent_runs():
    extractor = make_extractor(FakeExtractor(fail_clause="第二条"))
    with pytest.raises(ValueError):
        await extractor.extract_clauses("失败法规.txt", SAMPLE_LAW)
    extractor.extractor = FakeExtractor()
    await extractor.extract_clauses("成功法规.txt", SAMPLE_LAW)
    summary = extractor.get_run_summary()
    assert summary["total_files"] == 1
    assert summary["success_files"] == 1
    assert summary["error"] == 0


@pytest.mark.asyncio
async def test_logging_summary_does_not_expose_api_key(caplog):
    extractor = make_extractor()
    await extractor.extract_clauses("中华人民共和国示例法.txt", SAMPLE_LAW)
    with caplog.at_level(logging.INFO):
        await extractor.logging_result_stats()
    assert extractor.config.api_key not in caplog.text
