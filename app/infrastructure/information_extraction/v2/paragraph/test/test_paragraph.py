import asyncio
from collections import Counter
import copy
from dataclasses import replace
import json
import logging
from pathlib import Path
from unittest.mock import patch

import pytest

from .. import ParagraphConfig, ParagraphExtractor, ParagraphStandaloneRunner
from ..graph_builder import ParagraphGraphBuilder
from ..io_utils import save_json, supported
from ..prompt.example import receipt, claim
from ..validator import validate_file, validate_content, file_properties, ReviewRequired, normalize, interface_eligibility
from ..validator import behavior_supported, name_supported

FORMAL = "示例规定\n\n发布单位：示例机构\n\n本规定适用于各类企业。\n\n企业应当保存业务记录。"
NEWS = "管理动态\n\n新华社北京2月24日电，某机关印发了《示例意见》。\n\n《示例意见》要求各部门建立协调机制。"


class FakeModel:
    """Synthetic oracle for infrastructure tests, never a production fallback."""
    def __init__(self, fail=0, delay=0, target_failure=False, transform=None):
        self.fail, self.delay, self.target_failure, self.transform = fail, delay, target_failure, transform
        self.calls, self.active, self.maximum, self.payloads = 0, 0, 0, []

    async def entity_and_relationship_extract(self, **kwargs):
        data = json.loads(kwargs["input_text"])
        self.calls += 1
        self.payloads.append(kwargs)
        self.active += 1
        self.maximum = max(self.maximum, self.active)
        try:
            await asyncio.sleep(self.delay)
            if self.calls <= self.fail or self.target_failure and "target_id" in data:
                raise RuntimeError("simulated 500 error")
            if "target_id" not in data:
                source = data["source"]
                news = bool(data["source_signals"])
                value = {"entities": [{"name": data["title"], "type": "来源文件", "attributes": {
                    "文件名称": data["title"], "来源性质": "新闻报道" if news else "正式文件",
                    "文件类型": "新闻稿" if news else "规定", "来源机构": "新华社" if news else "示例机构" if "示例机构" in source else None,
                    "证据原文": data["source_signals"][0]["text"] if news else source}}], "relations": []}
            else:
                target, key = data["target"], data["target_id"]
                is_claim = "应当" in target or "《示例意见》要求" in target
                entities = [receipt(key, "知识" if is_claim else "非知识", "完整要求" if is_claim else "测试背景或范围说明")]
                if is_claim:
                    props = {"功能类型": "必要", "行为描述": "保存业务记录" if "保存" in target else "建立协调机制", "适用主体": "企业"}
                    if data["source_type"] == "新闻报道":
                        props["归属说明"] = "《示例意见》"
                        props["适用主体"] = "各部门"
                    entities.append(claim(key, target.strip(), "规范要求", target.strip(), **props))
                value = {"entities": entities, "relations": []}
            return self.transform(value, data) if self.transform else value
        finally:
            self.active -= 1


def setup_case(source=FORMAL, filename="示例规定.txt", config=None):
    extractor = ParagraphExtractor(config or ParagraphConfig(retry_delay=0), FakeModel())
    parsed = extractor.structure_parser.parse_text(source, filename)
    plan = extractor.planner.plan(parsed)
    model = extractor._llm
    return extractor, parsed, plan, model


async def validated_case():
    extractor, parsed, plan, model = setup_case()
    raw = await model.entity_and_relationship_extract(input_text=json.dumps(plan["file_payload"]))
    file_result = validate_file(raw, parsed, plan["file_payload"])
    unit = plan["units"][-1]
    raw = await model.entity_and_relationship_extract(input_text=json.dumps({**unit, "source_type": "正式文件"}))
    return parsed, file_result, unit, raw


@pytest.mark.asyncio
async def test_successful_formal_and_news_have_separate_views_and_no_fake_articles():
    for source, filename, normative in ((FORMAL, "示例规定.txt", 1), (NEWS, "管理动态.txt", 0)):
        extractor = ParagraphExtractor(ParagraphConfig(retry_delay=0), FakeModel())
        result = await extractor.extract_text(source, filename, run_llm=True)
        assert result["status"] == "success" and result["semantic_complete"]
        assert bool(result["formal_graph_eligible"]) == bool(normative)
        assert result["stats"]["counts"].get("normative_knowledge", 0) == normative
        assert not any(n["node_type"] in {"法条", "条款单元", "段落处理"} for n in result["graph"]["nodes"])
        assert all("三级指标映射状态" not in n["properties"] for n in result["graph"]["nodes"])


@pytest.mark.asyncio
async def test_five_attempts_are_total_and_no_parser_semantic_fallback(tmp_path):
    model = FakeModel(fail=99)
    extractor = ParagraphExtractor(ParagraphConfig(retry_delay=0), model)
    extractor.audit_path = tmp_path/"audit.jsonl"
    result = await extractor.extract_text(FORMAL, "示例规定.txt", run_llm=True)
    assert model.calls == 5 and result["status"] == "incomplete"
    assert not result["formal_graph_eligible"] and not result["graph"]["nodes"]
    assert result["stats"]["counts"]["extraction_error"] == 1
    assert result["stats"]["counts"]["attempt_errors"] == 5
    assert "simulated 500 error" in model.payloads[1]["user_prompt"]
    assert len(extractor.audit_path.read_text(encoding="utf-8").splitlines()) == 5


@pytest.mark.asyncio
async def test_eventual_success_retains_attempt_error_counts():
    extractor = ParagraphExtractor(ParagraphConfig(retry_delay=0), FakeModel(fail=1))
    result = await extractor.extract_text(FORMAL, "示例规定.txt", run_llm=True)
    assert result["status"] == "success"
    assert result["stats"]["counts"]["attempt_errors"] == result["stats"]["counts"]["retries"] == 1
    assert not result["stats"]["errors"]


@pytest.mark.asyncio
async def test_target_failure_is_not_reasonable_nonknowledge():
    model = FakeModel(target_failure=True)
    result = await ParagraphExtractor(ParagraphConfig(retry_delay=0), model).extract_text(FORMAL, "示例规定.txt", run_llm=True)
    assert result["status"] == "incomplete" and result["stats"]["counts"]["extraction_error"] == 2
    assert all(r["status"] == "failed" for r in result["results"])


@pytest.mark.asyncio
async def test_limits_partial_selection_and_independent_heartbeat(caplog):
    model = FakeModel(delay=.02)
    source = "示例规定\n\n"+"\n\n".join("企业应当保存业务记录。" for _ in range(7))
    with caplog.at_level(logging.INFO):
        result = await ParagraphExtractor(ParagraphConfig(retry_delay=0, heartbeat_seconds=.01, max_concurrent=2), model).extract_text(source, "示例规定.txt", run_llm=True)
    assert result["status"] == "success" and model.maximum == 2
    assert any("心跳" in r.message for r in caplog.records)
    partial = await ParagraphExtractor(ParagraphConfig(retry_delay=0), FakeModel()).extract_text(FORMAL, "示例规定.txt", run_llm=True, max_targets=1)
    assert partial["status"] == "partial_test" and not partial["publish_graph_eligible"]


@pytest.mark.asyncio
async def test_parse_never_calls_model_and_no_knowledge_is_not_an_error():
    extractor = ParagraphExtractor(ParagraphConfig(), FakeModel(fail=99))
    parsed = await extractor.extract_text(FORMAL, "示例规定.txt")
    assert parsed["status"] == "structure_only" and extractor._llm.calls == 0 and not parsed["graph"]["nodes"]
    empty = await ParagraphExtractor(ParagraphConfig(retry_delay=0), FakeModel()).extract_text("示例规定\n\n本规定适用于相关管理工作。", "示例规定.txt", run_llm=True)
    assert empty["status"] == "no_knowledge" and not empty["stats"]["errors"] and not empty["publish_graph_eligible"]


@pytest.mark.asyncio
async def test_unknown_fields_missing_receipts_wrong_targets_and_endpoints():
    parsed, file_result, unit, raw = await validated_case()
    for mutation in ("field", "receipt", "target", "relation", "evidence"):
        value = copy.deepcopy(raw)
        if mutation == "field": value["entities"][1]["attributes"]["任意动态长句"] = "不允许"
        if mutation == "receipt": value["entities"] = value["entities"][1:]
        if mutation == "target": value["entities"][0]["attributes"]["目标ID"] = "invented"
        if mutation == "relation": value["relations"] = [{"source": value["entities"][1]["name"], "type": "包含", "target": "不存在"}]
        if mutation == "evidence": value["entities"][1]["attributes"]["原文依据"] = "上下文里也不存在的文本"
        with pytest.raises(ValueError):
            validate_content(value, unit, file_properties(file_result))


@pytest.mark.asyncio
async def test_news_identity_and_year_cannot_be_upgraded():
    extractor, parsed, plan, model = setup_case(NEWS, "管理动态.txt")
    raw = await model.entity_and_relationship_extract(input_text=json.dumps(plan["file_payload"]))
    for key, value in (("来源性质", "正式文件"), ("来源机构", "某机关"), ("发布日期", "2026年2月24日"), ("文号", "发〔2026〕1号")):
        altered = copy.deepcopy(raw)
        altered["entities"][0]["attributes"][key] = value
        with pytest.raises(ValueError):
            validate_file(altered, parsed, plan["file_payload"])


@pytest.mark.asyncio
async def test_reference_attribution_and_controlled_arrays():
    parsed, file_result, unit, raw = await validated_case()
    for key, value in (("合规风险类型", ["劳动法律风险"]), ("合规风险类型", "企业信用风险"),
                       ("经济行业", ["通用", "制造业"]), ("经济行业", ["电子信息业"]), ("知识类型", "行动任务")):
        changed = copy.deepcopy(raw)
        changed["entities"][1]["attributes"][key] = value
        with pytest.raises(ValueError):
            validate_content(changed, unit, file_properties(file_result))


@pytest.mark.asyncio
async def test_normative_direction_and_claim_identity():
    parsed, file_result, unit, raw = await validated_case()
    valid = validate_content(raw, unit, file_properties(file_result))
    valid.update(status="success", paragraph_id=unit["paragraph_id"])
    first = ParagraphGraphBuilder().build(parsed, file_result, [valid])
    renamed_raw = copy.deepcopy(raw)
    renamed_raw["entities"][1]["name"] = "different display name"
    renamed = validate_content(renamed_raw, unit, file_properties(file_result))
    renamed.update(status="success", paragraph_id=unit["paragraph_id"])
    second = ParagraphGraphBuilder().build(parsed, file_result, [renamed])
    assert {n["node_id"] for n in first["nodes"]} == {n["node_id"] for n in second["nodes"]}
    p = copy.deepcopy(raw)
    p["entities"][1]["attributes"].update(知识类型="概念定义", 功能类型="可选")
    with pytest.raises(ValueError):
        validate_content(p, unit, file_properties(file_result))


@pytest.mark.asyncio
async def test_simple_quantity_and_complex_branch_diagnostics():
    parsed, file_result, unit, raw = await validated_case()
    unit = {**unit, "target": "企业应当缴纳五千元以上一万元以下的罚款。"}
    p = raw["entities"][1]["attributes"]
    p.update(知识内容=unit["target"], 原文依据=unit["target"], 行为描述="缴纳", 量化特征="定量", 量化条件={
        "原文件": "五千元以上一万元以下的罚款", "量化值类型": "金额", "最小值": 5000,
        "最大值": 10000, "单位": "元", "约束关系": "区间"})
    value = validate_content(raw, unit, file_properties(file_result))
    assert value["entities"][0]["properties"]["量化条件"]["最大值"] == 10000
    p["量化条件"]["原文件"] = "五千元或者一万元"
    unit["target"] += "五千元或者一万元"
    p.update(原文依据=unit["target"], 知识内容=unit["target"])
    with pytest.raises(ReviewRequired):
        validate_content(raw, unit, file_properties(file_result))


@pytest.mark.asyncio
async def test_json_repair_is_audited_and_does_not_skip_validation():
    def corrupt(value, data):
        return json.dumps(value, ensure_ascii=False)[:-1] + ",}"
    result = await ParagraphExtractor(ParagraphConfig(retry_delay=0), FakeModel(transform=corrupt)).extract_text(FORMAL, "示例规定.txt", run_llm=True)
    assert result["status"] == "success"
    assert result["stats"]["counts"]["json_repair_successes"] >= 1


@pytest.mark.asyncio
async def test_recursive_same_basename_outputs_are_independent(tmp_path):
    source = tmp_path/"input"
    for name in ("a", "b"):
        (source/name).mkdir(parents=True)
        (source/name/"示例规定.txt").write_text(FORMAL, encoding="utf-8")
    runner = ParagraphStandaloneRunner(ParagraphConfig(retry_delay=0), extractor_factory=lambda config: ParagraphExtractor(config, FakeModel()))
    result = await runner.run(source, tmp_path/"out", "extract")
    assert result["status"] == "completed"
    assert len({row["record_directory"] for row in result["files"]}) == 2
    assert all((source/n/"示例规定.txt").read_text(encoding="utf-8") == FORMAL for n in ("a", "b"))


@pytest.mark.asyncio
async def test_invented_subject_and_context_only_behavior_are_rejected():
    parsed, file_result, unit, raw = await validated_case()
    raw["entities"][1]["attributes"]["适用主体"] = "所有境外企业"
    with pytest.raises(ValueError, match="适用主体"):
        validate_content(raw, unit, file_properties(file_result))
    raw["entities"][1]["attributes"].update(适用主体="企业", 行为描述="提交申请")
    unit["context"].append({"role": "neighbor_for_resolution_only", "text": "企业应当提交申请。", "start": 1000, "end": 1011})
    with pytest.raises(ReviewRequired, match="ownership"):
        validate_content(raw, unit, file_properties(file_result))


@pytest.mark.asyncio
async def test_explicit_numerical_obligation_cannot_be_silently_qualitative():
    parsed, file_result, unit, raw = await validated_case()
    unit["target"] = "企业应当缴纳五千元罚款。"
    raw["entities"][1]["attributes"].update(知识内容=unit["target"], 原文依据=unit["target"], 行为描述="缴纳五千元罚款")
    with pytest.raises(ValueError, match="omitted"):
        validate_content(raw, unit, file_properties(file_result))


@pytest.mark.asyncio
async def test_lost_explicit_exception_requires_review():
    parsed, file_result, unit, raw = await validated_case()
    unit["context"].append({"role": "exception", "text": "但依法豁免的情形除外。", "start": 100, "end": 112})
    with pytest.raises(ReviewRequired, match="exception"):
        validate_content(raw, unit, file_properties(file_result))


@pytest.mark.asyncio
async def test_short_complete_definition_is_not_rejected_by_length():
    parsed, file_result, unit, raw = await validated_case()
    unit["target"] = "甲指乙。"
    p = raw["entities"][1]["attributes"]
    p.update(知识内容="甲指乙。", 原文依据="甲指乙。", 知识类型="概念定义", 功能类型=None, 行为描述=None, 适用主体="甲")
    result = validate_content(raw, unit, file_properties(file_result))
    assert result["decision"] == "知识"


@pytest.mark.asyncio
async def test_unspecified_usage_is_derived_and_list_punctuation_is_not_semantic_error():
    parsed, file_result, unit, raw = await validated_case()
    p = raw["entities"][1]["attributes"]
    p["使用定位"] = None
    assert validate_content(raw, unit, file_properties(file_result))["entities"][0]["properties"]["使用定位"] == "规范知识"
    assert behavior_supported("保存记录、报告风险", "企业应当保存记录，并及时报告风险。")
    assert not behavior_supported("报告风险、保存记录", "企业应当保存记录，并及时报告风险。")
    assert behavior_supported("培育公而忘私甘于奉献的品格、报告风险", "培育公而忘私、甘于奉献的品格，并报告风险。")
    assert not behavior_supported("培育公而忘私甘于奉献的品格、消除风险", "培育公而忘私、甘于奉献的品格，并报告风险。")
    assert name_supported("《示例意见》", "示例意见")
    assert not name_supported("《另一份意见》", "示例意见")


@pytest.mark.asyncio
async def test_audit_keeps_structured_adapter_entities_and_redacts_secrets(tmp_path):
    from app.infrastructure.information_extraction.base import Entity
    from ..io_utils import append_audit
    path = tmp_path/"attempts.jsonl"
    append_audit(path, {"raw": Entity(name="sample", entity_type="来源文件", properties={"note": "secret-value"})}, ("secret-value",))
    saved = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(saved["raw"], dict) and saved["raw"]["properties"]["note"] == "[REDACTED]"


@pytest.mark.asyncio
async def test_same_target_attribution_prefix_is_authorized_but_invented_context_is_not():
    parsed, file_result, unit, raw = await validated_case()
    unit["target"] = "《规定》指出，应当保存记录。企业应当报告风险。"
    p = raw["entities"][1]["attributes"]
    p.update(知识内容="《规定》指出，企业应当报告风险。", 原文依据="企业应当报告风险。",
             行为描述="报告风险", 归属说明="《规定》", 上下文依据=["《规定》指出"], 使用定位=None)
    result = validate_content(raw, unit, {**file_properties(file_result), "来源性质": "新闻报道"})
    ranges = result["evidence_ranges"][raw["entities"][1]["name"]]
    assert ranges["context"][0]["start"] == unit["start"]
    assert result["entities"][0]["properties"]["使用定位"] == "参考知识"
    p["上下文依据"] = ["《不存在的规定》指出"]
    with pytest.raises(ValueError, match="上下文依据"):
        validate_content(raw, unit, file_properties(file_result))


@pytest.mark.asyncio
async def test_publication_metadata_is_not_a_knowledge_proposition():
    parsed, file_result, unit, raw = await validated_case()
    unit["target"] = "某机关印发了《示例管理意见》。"
    p = raw["entities"][1]["attributes"]
    p.update(知识类型="事实陈述", 知识内容=unit["target"], 原文依据=unit["target"], 功能类型=None,
             适用主体=None, 行为描述=None)
    with pytest.raises(ValueError, match="来源元信息"):
        validate_content(raw, unit, file_properties(file_result))
    unit["target"] = "本文件适用于各类企业。"
    p.update(知识内容=unit["target"], 原文依据=unit["target"])
    with pytest.raises(ValueError, match="单纯适用范围"):
        validate_content(raw, unit, file_properties(file_result))
    unit["target"] = "本规定适用于所有企业并要求企业保存记录。"
    p.update(知识类型="规范要求", 知识内容=unit["target"], 原文依据=unit["target"], 功能类型="必要", 行为描述="保存记录", 适用主体="企业")
    assert validate_content(raw, unit, file_properties(file_result))["decision"] == "知识"
    unit["target"] = "某企业缴纳了五千元罚款。"
    p.update(知识类型="事实陈述", 知识内容=unit["target"], 原文依据=unit["target"], 功能类型=None, 行为描述=None, 适用主体=None)
    assert validate_content(raw, unit, file_properties(file_result))["decision"] == "知识"


@pytest.mark.asyncio
async def test_formal_requirement_may_keep_an_unknown_direction_without_inventing_obligation():
    parsed, file_result, unit, raw = await validated_case()
    unit["target"] = "经营者保存交易记录并定期整理。"
    p = raw["entities"][1]["attributes"]
    p.update(知识内容=unit["target"], 原文依据=unit["target"], 功能类型=None,
             适用主体="经营者", 行为描述="保存交易记录并定期整理", 归属说明=None)
    value = validate_content(raw, unit, file_properties(file_result))["entities"][0]["properties"]
    assert value["功能类型"] is None and value["使用定位"] == "参考知识"
    assert not interface_eligibility(value, file_properties(file_result))["eligible"]
    p["功能类型"] = "必要"
    result = validate_content(raw, unit, file_properties(file_result))
    value = result["entities"][0]["properties"]
    assert value["功能类型"] is None and value["使用定位"] == "参考知识"
    assert result["normalizations"][0]["field"] == "功能类型"


@pytest.mark.asyncio
async def test_action_may_omit_a_separately_validated_numerical_modifier():
    parsed, file_result, unit, raw = await validated_case()
    unit["target"] = "企业应当缴纳五千元以上一万元以下的罚款。"
    p = raw["entities"][1]["attributes"]
    p.update(知识内容=unit["target"], 原文依据=unit["target"], 行为描述="缴纳罚款", 量化特征="定量",
             量化条件={"原文件": "五千元以上一万元以下的罚款", "量化值类型": "金额", "最小值": 5000,
                       "最大值": 10000, "单位": "元", "约束关系": "区间"})
    assert validate_content(raw, unit, file_properties(file_result))["decision"] == "知识"
    p["量化条件"]["最大值"] = 20000
    repaired = validate_content(raw, unit, file_properties(file_result))
    assert repaired["entities"][0]["properties"]["量化条件"]["最大值"] == 10000
    assert p["量化条件"]["最大值"] == 20000  # The raw response remains available for audit.
    p["量化条件"]["最大值"] = 10000
    for quote in ("五千元以上一万元以下", unit["target"]):
        p["量化条件"]["原文件"] = quote
        assert validate_content(raw, unit, file_properties(file_result))["decision"] == "知识"
    p["量化条件"]["对象"] = "罚款"
    with pytest.raises(ValueError, match="六个键"):
        validate_content(raw, unit, file_properties(file_result))
    p["量化条件"].pop("对象")
    p["行为描述"] = "免除罚款"
    with pytest.raises(ValueError, match="行为描述"):
        validate_content(raw, unit, file_properties(file_result))
