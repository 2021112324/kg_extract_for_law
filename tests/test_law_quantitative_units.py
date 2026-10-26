"""Quantitative normalization and opt-in retry tests; no external services."""

import asyncio
import copy
import importlib
import json
from unittest.mock import AsyncMock, Mock

import pytest


@pytest.mark.parametrize("package", ["law_extract", "v2.zh_law"])
@pytest.mark.parametrize("text,low,high,unit", [
    ("三个月以上二年以下", 3, 24, "月"),
    ("五千元以上一万元以下", 5000, 10000, "元"),
    ("所获收入30%以上3倍以下", 0.3, 3, "倍"),
])
def test_public_mixed_units(package, text, low, high, unit):
    module = importlib.import_module("app.infrastructure.information_extraction." + package + ".clause_extract")
    value = {"原文件": text, "最小值": 30, "最大值": 2, "单位": "月", "约束关系": "区间"}
    result = module._normalize_quant_condition(value)
    assert (result["最小值"], result["最大值"], result["单位"]) == (low, high, unit)
    assert result["原文件"] == text


def normal(value, **kwargs):
    from app.infrastructure.information_extraction.quantitative import normalize_quant_condition
    return normalize_quant_condition(value, **kwargs)


def obj(text="", low=3, high=2, unit="月", relation="区间", **extra):
    return {"原文件": text, "最小值": low, "最大值": high, "单位": unit, "约束关系": relation, **extra}


@pytest.mark.parametrize("value", [
    obj("三个月以上二年以下"), obj("五万元以上十万元以下"),
    obj("所获收入30%以上2倍以下"), obj("5,000元以上1万元以下"),
    obj("0.1万元以上0.3万元以下"), obj("", 0, None, "元", "下限"),
])
def test_idempotent_and_serialized(value):
    original = copy.deepcopy(value)
    once = normal(value)
    assert normal(once) == once
    assert normal(json.dumps(value, ensure_ascii=False)) == once
    assert value == original


@pytest.mark.parametrize("value", [
    obj("30%以上3倍以下", 30, 3, "%/倍"),
    obj("收入30%以上利润3倍以下", 30, 3, "%/倍"),
    obj("三个月以上二日以下", 3, 2, "月/日"),
    obj("三年以下二日以上", 3, 2, "年/日"),
    obj("无法识别的表述", 3, 2, "光年"),
    obj("", None, 7, "日", "上限"),
    "无法可靠解析的原文", "{broken json", None,
])
def test_uncertain_information_is_preserved(value):
    assert normal(value) == value


def test_decimal_and_extra_context():
    result = normal(obj("0.1万元以上0.3万元以下", 0.1, 0.3, "万元", 计算基数="收入", 适用条件="严重情形"))
    assert (result["最小值"], result["最大值"], result["单位"]) == (1000, 3000, "元")
    assert result["计算基数"] == "收入" and result["适用条件"] == "严重情形"


@pytest.mark.parametrize("bad", ["not a number", float("inf"), float("nan"), True])
def test_invalid_numeric_fields_raise(bad):
    from app.infrastructure.information_extraction.quantitative import QuantitativeValidationError
    with pytest.raises(QuantitativeValidationError):
        normal(obj("", bad, 2, "未知单位"))


@pytest.mark.parametrize("value", [obj("", 7, 6, "元"), obj("", 1, 2, "元", "等于"), obj("三元以上二日以下")])
def test_clear_errors_raise(value):
    from app.infrastructure.information_extraction.quantitative import QuantitativeValidationError
    with pytest.raises(QuantitativeValidationError):
        normal(value)


def test_parent_context_can_prove_shared_base():
    value = obj("30%以上3倍以下", 30, 3, "%/倍")
    assert normal(value, context="处所获收入30%以上3倍以下罚款")["最小值"] == 0.3
    assert normal(value, context="收入另有规定。处30%以上3倍以下罚款") == value


def test_lists_and_multiple_branches_are_not_truncated():
    values = [obj("三个月以上二年以下"), obj("所获收入30%以上3倍以下")]
    result = normal(values)
    assert len(result) == 2 and result[0]["最大值"] == 24 and result[1]["最小值"] == 0.3
    raw = "一般情形三个月以上二年以下；严重情形三年以上五年以下"
    assert normal(raw) == raw


@pytest.mark.parametrize("raw,expected", [
    ("所获收入百分之三十以上三倍以下", (0.3, 3, "倍")),
    ("百分之三十以上百分之五十以下", (30, 50, "%")),
    ("所获收入百分之三十以上百分之五十以下", (30, 50, "%")),
    ("30％以上50%以下", (30, 50, "%")),
])
def test_chinese_percent_and_same_percent_unit(raw, expected):
    result = normal(raw)
    assert (result["最小值"], result["最大值"], result["单位"]) == expected


def test_different_bases_even_with_same_percent_unit_are_not_merged():
    value = obj("收入30%以上利润20%以下", 30, 20, "%")
    assert normal(value) == value
    assert normal("收入30%以上利润20%以下") == "收入30%以上利润20%以下"


def documents(value):
    return [{"text": "SOURCE", "extractions": [{"extraction_class": "条款单元", "extraction_text": "第十条第一款",
        "attributes": {"条款单元内容": "原文", "量化条件": value}}]}]


@pytest.fixture
def adapter(monkeypatch):
    from app.infrastructure.information_extraction import langextract_adapter as module
    from app.infrastructure.information_extraction.method.base import LangextractConfig
    monkeypatch.setattr(module, "Langfuse", None)
    monkeypatch.setattr(module.time, "sleep", Mock())
    config = LangextractConfig(model_name="offline", api_key="test-key", api_url="http://example.invalid")
    result = module.LangextractAdapter(max_retries=5, config=config)
    monkeypatch.setattr(result, "convert_document_to_dict_with_text", lambda value: value)
    return module, result


def test_validator_retries_share_budget_and_latest_error(adapter, monkeypatch):
    from app.infrastructure.information_extraction.quantitative import law_document_validator
    module, instance = adapter
    bad1, bad2 = documents(obj("", 7, 6, "元")), documents(obj("", 1, 2, "元", "等于"))
    good = documents(obj("三个月以上二年以下"))
    calls = AsyncMock(side_effect=[RuntimeError("NETWORK_ERROR"), bad1, bad2, good])
    monkeypatch.setattr(module, "run_with_timeout_async", calls)
    result = asyncio.run(instance.extract_list_of_dict("BASE", ["EXAMPLE"], "INPUT",
        document_validator=law_document_validator("file.txt", "第十条", "父法条")))
    sent = [c.kwargs["kwargs"]["prompt_description"] for c in calls.await_args_list]
    assert calls.await_count == 4 and result[0]["extractions"][0]["attributes"]["量化条件"]["最大值"] == 24
    assert "NETWORK_ERROR" in sent[1]
    assert "最小值大于最大值" in sent[2] and "NETWORK_ERROR" not in sent[2]
    assert "等于" in sent[3] and "最小值大于最大值" not in sent[3]
    assert "file.txt" in sent[2] and "第十条第一款" in sent[2] and "对象1" in sent[2]


def test_validator_disabled_keeps_legacy_behavior(adapter, monkeypatch):
    module, instance = adapter
    bad = documents(obj("", 7, 6, "元"))
    calls = AsyncMock(return_value=bad)
    monkeypatch.setattr(module, "run_with_timeout_async", calls)
    assert asyncio.run(instance.extract_list_of_dict("BASE", ["EXAMPLE"], "INPUT")) == bad
    assert calls.await_count == 1


def test_reliable_conversion_does_not_request_another_attempt(adapter, monkeypatch):
    from app.infrastructure.information_extraction.quantitative import law_document_validator
    module, instance = adapter
    calls = AsyncMock(return_value=documents(obj("三个月以上二年以下", 1, 2, "年")))
    monkeypatch.setattr(module, "run_with_timeout_async", calls)
    result = asyncio.run(instance.extract_list_of_dict("BASE", ["EXAMPLE"], "INPUT",
        document_validator=law_document_validator("任意名称.txt", "第十条", "正文")))
    condition = result[0]["extractions"][0]["attributes"]["量化条件"]
    assert (condition["最小值"], condition["最大值"], condition["单位"]) == (3, 24, "月")
    assert calls.await_count == 1


def test_explicit_equality_conflict_is_not_ignored_for_unknown_unit():
    from app.infrastructure.information_extraction.quantitative import QuantitativeValidationError
    with pytest.raises(QuantitativeValidationError, match="等于"):
        normal(obj("", 1, 2, "未知单位", "等于"))


def test_serialized_list_error_keeps_position_and_values_even_with_long_source():
    from app.infrastructure.information_extraction.quantitative import QuantitativeValidationError, law_document_validator
    values = [obj("三个月以上二年以下"), obj("未识别文本" * 200, 7, 6, "元")]
    value = json.dumps(values, ensure_ascii=False)
    with pytest.raises(QuantitativeValidationError) as caught:
        law_document_validator("源文件.txt", "第十条", "正文")(documents(value))
    message = str(caught.value)
    assert "量化对象2" in message and "源文件.txt" in message
    assert '"最小值": 7' in message and '"最大值": 6' in message and '"单位": "元"' in message


@pytest.mark.parametrize("package", ["law_extract", "v2.zh_law"])
@pytest.mark.parametrize("recover", [True, False])
def test_full_article_path_uses_hook_and_counts_final_failure(adapter, monkeypatch, package, recover):
    from app.infrastructure.information_extraction.v2.zh_law.test.test_zh_law_v2 import config
    module, instance = adapter
    law = importlib.import_module("app.infrastructure.information_extraction." + package + ".clause_extract")
    bad = documents(obj("", 7, 6, "元"))
    good = documents(obj("三个月以上二年以下"))
    for doc in (bad, good):
        doc[0]["extractions"].insert(0, {"extraction_class": "法条", "extraction_text": "第十条", "attributes": {}})
    calls = AsyncMock(side_effect=[bad, good] if recover else [copy.deepcopy(bad) for _ in range(5)])
    monkeypatch.setattr(module, "run_with_timeout_async", calls)
    if package == "v2.zh_law":
        extractor = law.ClauseExtractor(config=config(), extractor=instance)
    else:
        # V1 exposes the same extractor member but not V2's injection constructor.
        monkeypatch.setattr(law.InformationExtractionFactory, "create", lambda *a, **kw: instance)
        extractor = law.ClauseExtractor()
    task = extractor.kg_extract_from_clause("泛化法规.txt", law.ClauseCache(),
        {"条款编号": "第十条", "条款内容": "适用期限为三个月以上二年以下。"})
    if recover:
        result = asyncio.run(task)
        assert result["条款单元"][0]["properties"]["量化条件"]["最大值"] == 24
        assert extractor.result_stats.error == 0
        assert calls.await_count == 2
    else:
        with pytest.raises(Exception, match="量化条件校验失败"):
            asyncio.run(task)
        assert extractor.result_stats.error == 1
        if package == "v2.zh_law":
            assert extractor.result_stats.extraction_error == 1
        assert calls.await_count == 5


def test_schema_and_examples_share_rules():
    for package in ("law_extract", "v2.zh_law"):
        root = "app.infrastructure.information_extraction." + package + ".prompt."
        schema = importlib.import_module(root + "schema").schema_for_clause
        examples = importlib.import_module(root + "example").example_for_clause
        assert "1年=12个月" in schema and "1万元=10000元" in schema
        assert "不得交换上下限" in schema
        assert any(n.get("attributes", {}).get("量化条件", {}).get("最大值") == 24
            for e in examples for n in e.get("nodes", []) if isinstance(n.get("attributes", {}).get("量化条件"), dict))


@pytest.mark.parametrize("package", ["law_extract", "v2.zh_law"])
def test_strict_file_failure_is_not_complete_or_double_counted(adapter, monkeypatch, package):
    from app.infrastructure.information_extraction.v2.zh_law.test.test_zh_law_v2 import config
    module, instance = adapter
    law = importlib.import_module("app.infrastructure.information_extraction." + package + ".clause_extract")
    bad = documents(obj("", 7, 6, "元"))
    calls = AsyncMock(side_effect=[copy.deepcopy(bad) for _ in range(5)])
    monkeypatch.setattr(module, "run_with_timeout_async", calls)
    if package == "law_extract":
        monkeypatch.setattr(law.InformationExtractionFactory, "create", lambda *a, **kw: instance)
        extractor = law.ClauseExtractor()
    else:
        extractor = law.ClauseExtractor(config=config(), extractor=instance)
    clause = {"条款编号": "第十条", "条款内容": "法律原文"}
    monkeypatch.setattr(extractor, "split_clause", AsyncMock(return_value={"file_info": "FILE", "clauses": [clause]}))
    monkeypatch.setattr(extractor, "kg_extract_from_file_info", AsyncMock(return_value={"node_id": "file"}))
    process = AsyncMock(return_value={})
    monkeypatch.setattr(extractor, "process_extracted_data", process)
    with pytest.raises(ValueError, match="失败"):
        asyncio.run(extractor.extract_clauses("任意法规.txt", "正文"))
    process.assert_not_awaited()
    assert calls.await_count == 5
    if package == "law_extract":
        assert extractor.result_stats.error == 1
    else:
        assert extractor.result_stats.extraction_error == 1
