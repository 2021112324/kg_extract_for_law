import asyncio
from dataclasses import replace
import json
import logging
from pathlib import Path

import pytest

from ..config import RuleConfig
from ..extractor import RuleExtractor
from ..graph_builder import RuleGraphBuilder
from ..structure_parser import RuleStructureParser
from ..standalone import RuleStandaloneRunner
from ..unit_planner import RuleUnitPlanner, boundary_candidates, split_text
from ..validator import validate_result


class FakeModel:
    def __init__(self, *, fail=0, empty=False, sleep=0, boundary_mode=None):
        self.calls = []
        self.fail = fail
        self.empty = empty
        self.sleep = sleep
        self.boundary_mode = boundary_mode

    async def entity_and_relationship_extract(self, **kwargs):
        self.calls.append(kwargs)
        if self.sleep:
            await asyncio.sleep(self.sleep)
        if self.fail:
            self.fail -= 1
            raise ValueError("invalid JSON response")
        payload = json.loads(kwargs["input_text"])
        if "children" in payload:
            mode = self.boundary_mode or ("整组" if "同时满足" in payload["target"] else "继承")
            return {"entities": [{"name": payload["number"], "entity_type": "承载判定", "properties": {
                "处理方式": mode, "成员编号": [c["number"] for c in payload["children"]] if mode == "整组" else [],
                "证据原文": payload["target"]}}]}
        if "title" in payload:
            return {"entities": [{"name": payload["title"], "entity_type": "法规文件",
                                   "properties": {"文件全称": payload["title"], "经济行业": ["通用"]}}]}
        number, target = payload["number"], payload["target"]
        entities = [{"name": number, "entity_type": "法条", "properties": {"条": number, "原文依据": target}}]
        relations = []
        if not self.empty:
            props = {"单元编号": number, "单元层级": "条", "条款单元内容": target, "原文依据": target,
                     "功能类型": "必要", "量化特征": "定性", "量化条件": None}
            if payload["mode"] == "context":
                props["上下文依据"] = payload["context"]
                props["条款单元内容"] = payload["context"] + "\n" + target
            entities.append({"name": "单元", "entity_type": "条款单元", "properties": props})
            relations.append({"source": number, "target": "单元", "type": "包含"})
        return {"entities": entities, "relations": relations}


def config(**kwargs):
    return RuleConfig(retry_delay=0, **kwargs)


def parse(text):
    return RuleStructureParser().parse_text("示例规则\n" + text)


@pytest.mark.parametrize("number", ["1", "1.1", "1.1.1", "1.11", "11.1", "2.10", "01.02"])
def test_complete_numbers(number):
    result = parse(number + " 企业应保存记录。")
    assert result["nodes"][0]["number"] == number
    assert result["validation"]["passed"]


def test_real_mixed_hierarchy():
    result = parse("第二章 管理\n第一节 记录\n2.1.1 企业应保存记录。\n2.1.1.1 应保留原始凭证。")
    nodes = result["nodes"]
    assert result["validation"]["passed"]
    assert nodes[2]["parent_id"] == nodes[1]["node_id"]
    assert nodes[3]["parent_id"] == nodes[2]["node_id"]
    assert not any(n["number"] == "2.1" for n in nodes)


def test_numeric_prefix_parent_and_gaps():
    result = parse("1 总则\n1.1 范围\n1.1.3 企业应保存记录。")
    assert [n["number"] for n in result["nodes"]] == ["1", "1.1", "1.1.3"]
    assert result["nodes"][2]["parent_id"] == result["nodes"][1]["node_id"]


def test_conflicting_number_is_not_repaired():
    result = parse("1.1 企业应保存记录。\n1.1 企业应删除记录。")
    assert not result["validation"]["passed"]
    assert any(d["code"] == "duplicate_number" for d in result["diagnostics"])


@pytest.mark.parametrize("line", ["2026 年发布", "20 %以上", "100 元罚款", "GB/T 1234—2025", "银发〔2026〕1号", "应按1.1执行。", "— 3 —"])
def test_nonstructural_digits(line):
    result = parse("1.1 企业应保存记录。\n" + line)
    assert [n["number"] for n in result["nodes"]] == ["1.1"]


def test_wrapper_and_contents_label_do_not_erase_body():
    result = RuleStructureParser().parse_text("附件1\n示例规则\n目录\n第一章 总则\n1.1 企业应保存记录。\n附表\n表格内容")
    assert result["validation"]["passed"]
    assert any(n["number"] == "1.1" for n in result["nodes"])
    assert result["excluded"][-1]["reason"] == "trailing_attachment"
    assert len(result["line_dispositions"]) == 7


def test_inline_attachment_reference_is_not_stop():
    result = parse("1.1 企业应按附件格式报告。\n1.2 企业应保存记录。")
    assert len(result["nodes"]) == 2


def test_same_body_different_wrappers():
    parser = RuleStructureParser()
    a = parser.parse_text("附件1\n示例规则\n1.1 企业应保存记录。", "a.txt")
    b = parser.parse_text("示例规则\n1.1 企业应保存记录。", "b.txt")
    assert a["body_fingerprint"] == b["body_fingerprint"]


def test_parent_and_child_have_separate_target_text():
    parsed = parse("1.1 企业应建立制度。\n1.1.1 企业应每年评估制度。")
    plan = RuleUnitPlanner().plan(parsed)
    assert [u["number"] for u in plan["units"]] == ["1.1", "1.1.1"]
    assert "每年" not in plan["units"][0]["text"]
    assert all(c["kind"] == "target" for c in plan["coverage"])


def test_heading_not_empty_article():
    plan = RuleUnitPlanner().plan(parse("1 总则\n1.1 企业应保存记录。"))
    assert [u["number"] for u in plan["units"]] == ["1.1"]
    assert plan["coverage"][0]["kind"] == "heading"


def test_boundary_candidate_is_not_confirmed_offline():
    parsed = parse("1.1 同时满足以下全部条件才可申请：\n1.1.1 已登记。\n1.1.2 已提交材料。")
    plan = RuleUnitPlanner().plan(parsed)
    assert len(plan["pending_boundaries"]) == 1
    assert not plan["formal_plan_eligible"]
    assert len(plan["units"]) == 1
    assert len(plan["units"][0]["member_numbers"]) == 3


def test_chunk_text_preserved_and_not_hard_cut():
    text = "第一段。\n第二段。\n第三段。"
    chunks = split_text(text, 7)
    assert "".join(c["text"] for c in chunks) == text
    with pytest.raises(ValueError, match="oversized_sentence"):
        split_text("不含任何标点" * 10, 10)


@pytest.mark.asyncio
async def test_parse_does_not_create_model_or_graph():
    model = FakeModel()
    result = await RuleExtractor(config(), model).extract_text("示例规则\n1.1 企业应保存记录。")
    assert not model.calls
    assert result["status"] == "structure_only"
    assert not result["formal_graph_eligible"]
    assert not result["graph"]["nodes"]


@pytest.mark.asyncio
async def test_independent_articles_graph_contract():
    result = await RuleExtractor(config(), FakeModel()).extract_text("示例规则\n1.1 企业应建立制度。\n1.1.1 企业应每年评估制度。", run_llm=True)
    assert result["status"] == "success", result["stats"]
    articles = [n for n in result["graph"]["nodes"] if n["node_type"] == "法条"]
    assert len(articles) == 2
    assert articles[0]["properties"]["条"] == "1.1"
    assert "每年" not in articles[0]["properties"]["法条全文"]
    assert any(e["source_id"] == articles[0]["node_id"] and e["target_id"] == articles[1]["node_id"] for e in result["graph"]["edges"])


@pytest.mark.asyncio
async def test_group_has_one_article_and_unique_ownership():
    result = await RuleExtractor(config(), FakeModel()).extract_text("示例规则\n1.1 同时满足以下全部条件才可申请：\n1.1.1 已登记。\n1.1.2 已提交材料。", run_llm=True)
    assert result["status"] == "success", result["stats"]
    articles = [n for n in result["graph"]["nodes"] if n["node_type"] == "法条"]
    assert len(articles) == 1
    assert articles[0]["properties"]["来源编号列表"] == ["1.1", "1.1.1", "1.1.2"]
    assert len({c["owner"] for c in result["plan"]["coverage"]}) == 1
    assert all(e["source_id"] != e["target_id"] for e in result["graph"]["edges"])


@pytest.mark.asyncio
async def test_inherited_context_not_mixed_with_article_original():
    result = await RuleExtractor(config(), FakeModel()).extract_text("示例规则\n1.1 下列任一情形应启动应急处置：\n1.1.1 系统中断。\n1.1.2 数据损坏。", run_llm=True)
    assert result["status"] == "success", result["stats"]
    articles = [n for n in result["graph"]["nodes"] if n["node_type"] == "法条"]
    assert len(articles) == 2
    assert all("启动" not in a["properties"]["法条全文"] for a in articles)
    assert all("启动" in a["properties"]["上级上下文"] for a in articles)


@pytest.mark.asyncio
async def test_cannot_split_conjunctive_conditions():
    model = FakeModel(boundary_mode="继承")
    result = await RuleExtractor(config(), model).extract_text("示例规则\n1.1 同时满足以下全部条件才可申请：\n1.1.1 已登记。", run_llm=True)
    assert result["status"] == "incomplete"
    assert len(model.calls) == 5
    assert not result["formal_graph_eligible"]


@pytest.mark.asyncio
async def test_retry_five_including_first_and_error_feedback():
    model = FakeModel(fail=99)
    result = await RuleExtractor(config(), model).extract_text("示例规则\n1.1 企业应保存记录。", run_llm=True)
    assert len(model.calls) == 5
    assert "上次结果不可用" in model.calls[1]["user_prompt"]
    assert result["stats"]["counts"]["error"] == 1
    assert not result["graph"]["nodes"]


@pytest.mark.asyncio
async def test_retry_success_and_partial_not_formal():
    model = FakeModel(fail=1)
    result = await RuleExtractor(config(), model).extract_text("示例规则\n1.1 企业应保存记录。\n1.2 企业应定期检查。", run_llm=True, max_blocks=1)
    assert result["status"] == "partial_test"
    assert result["stats"]["counts"]["retries"] == 1
    assert not result["formal_graph_eligible"]


@pytest.mark.asyncio
async def test_empty_semantics_is_review_not_success():
    result = await RuleExtractor(config(), FakeModel(empty=True)).extract_text("示例规则\n1.1 企业应保存记录。", run_llm=True)
    assert result["status"] == "needs_review"
    assert not result["formal_graph_eligible"]
    assert not result["stats"]["errors"]


@pytest.mark.asyncio
async def test_heartbeat_during_model_wait_and_cleanup(caplog):
    caplog.set_level(logging.INFO)
    result = await RuleExtractor(config(heartbeat_seconds=.01), FakeModel(sleep=.03)).extract_text("示例规则\n1.1 企业应保存记录。", run_llm=True)
    assert result["status"] == "success"
    assert any("heartbeat" in r.message for r in caplog.records)
    count = len(caplog.records)
    await asyncio.sleep(.04)
    assert count == len(caplog.records)


def test_contract_rejects_unknown_fields_and_source_changes():
    unit = {"number": "1.1", "member_numbers": ["1.1"], "mode": "independent", "target": "1.1 企业应保存记录。", "context": ""}
    raw = {"entities": [{"name": "1.1", "entity_type": "法条", "properties": {"条": "1.1", "原文依据": "企业应保存记录。", "动态长句": "错误"}}]}
    with pytest.raises(ValueError, match="Schema outside"):
        validate_result(raw, source=unit["target"], unit=unit)
    props = raw["entities"][0]["properties"]
    del props["动态长句"]
    props["条"] = "1.11"
    with pytest.raises(ValueError, match="number changed"):
        validate_result(raw, source=unit["target"], unit=unit)
    props["条"] = "1.1"
    props["法条全文"] = "模型改写的原文"
    with pytest.raises(ValueError, match="rewritten"):
        validate_result(raw, source=unit["target"], unit=unit)


class FakeStorage:
    def __init__(self, saved=True, fail_verification=False):
        self.connected = 0
        self.saved = saved
        self.fail_verification = fail_verification
        self.graph = None
        self.labels = []

    def connect(self):
        self.connected += 1
        return True

    def disconnect(self):
        self.connected -= 1

    def check_subgraph_destination(self, label):
        if label in self.labels:
            raise ValueError("occupied target")

    def add_subgraph_with_merge(self, graph, label, *args):
        self.graph = graph
        self.labels.append(label)
        return self.saved

    def run_query(self, query, params):
        if self.fail_verification:
            return []
        return [{"count": len(self.graph["edges"] if "[r]" in query else self.graph["nodes"])}]


@pytest.mark.asyncio
async def test_runner_parse_duplicate_and_source_preservation(tmp_path):
    source = tmp_path / "in"
    source.mkdir()
    first = source / "a.txt"
    second = source / "b.txt"
    first.write_text("附件1\n示例规则\n1.1 企业应保存记录。", encoding="utf-8")
    second.write_text("示例规则\n1.1 企业应保存记录。", encoding="utf-8")
    before = first.read_bytes(), second.read_bytes()
    storage = FakeStorage()
    result = await RuleStandaloneRunner(config(), storage).run(source, tmp_path / "out")
    assert storage.connected == 0
    assert result["files"][1]["status"] == "duplicate_skipped"
    assert (first.read_bytes(), second.read_bytes()) == before
    assert (tmp_path / "out" / "run.log").exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("saved,verification,status", [(True, False, "confirmed"), (False, False, "failed_or_unknown"), (True, True, "failed_or_unknown"), (None, False, "failed_or_unknown")])
async def test_runner_confirmed_storage_only(tmp_path, saved, verification, status):
    source = tmp_path / "source.txt"
    source.write_text("示例规则\n1.1 企业应保存记录。", encoding="utf-8")
    storage = FakeStorage(saved, verification)
    runner = RuleStandaloneRunner(config(), storage, lambda config: RuleExtractor(config, FakeModel()))
    result = await runner.run(source, tmp_path / "out", "write", "rule_test_unique")
    assert result["files"][0]["storage_status"] == status
    assert storage.connected == 0


@pytest.mark.asyncio
async def test_incomplete_result_not_written(tmp_path):
    source = tmp_path / "source.txt"
    source.write_text("示例规则\n1.1 企业应保存记录。", encoding="utf-8")
    storage = FakeStorage()
    runner = RuleStandaloneRunner(config(), storage, lambda config: RuleExtractor(config, FakeModel(fail=99)))
    result = await runner.run(source, tmp_path / "out", "write", "rule_test_unique")
    assert storage.graph is None
    assert result["files"][0]["storage_status"] == "skipped_ineligible"


def test_configuration_and_lazy_entrypoint():
    with pytest.raises(ValueError):
        config(max_attempts=6)
    from .. import run_extraction
    assert run_extraction.MODE == "parse"
    assert run_extraction.INPUT_PATH == ""


def test_explicit_file_scope_carried_but_not_duplicated_as_target():
    parsed = parse("1.1 本规则适用于境内企业。\n2 管理\n2.1 企业应保存记录。")
    plan = RuleUnitPlanner().plan(parsed)
    unit = next(u for u in plan["units"] if u["number"] == "2.1")
    assert "境内企业" in unit["context"]
    assert "境内企业" not in unit["text"]
    assert len([c for c in plan["coverage"] if c["number"] == "1.1" and c["kind"] == "target"]) == 1


@pytest.mark.asyncio
async def test_long_article_chunks_merge_to_single_stable_article():
    text = "示例规则\n1.1 企业应保存记录。\n企业应核对记录。\n企业应按期检查。"
    result = await RuleExtractor(config(block_chars=20), FakeModel()).extract_text(text, run_llm=True)
    assert result["status"] == "success", result["stats"]
    assert result["plan"]["counts"]["execution_chunks"] > 1
    assert len([n for n in result["graph"]["nodes"] if n["node_type"] == "法条"]) == 1
    result2 = await RuleExtractor(config(block_chars=30), FakeModel()).extract_text(text, run_llm=True)
    assert [n["node_id"] for n in result["graph"]["nodes"] if n["node_type"] == "法条"] == [n["node_id"] for n in result2["graph"]["nodes"] if n["node_type"] == "法条"]


@pytest.mark.asyncio
async def test_definition_reasonable_empty_is_not_technical_failure():
    result = await RuleExtractor(config(), FakeModel(empty=True)).extract_text("示例规则\n1.1 本规则所称记录是指业务原始资料。", run_llm=True)
    assert result["status"] == "success"
    assert result["stats"]["counts"]["reasonable_empty_blocks"] == 1
    assert not result["stats"]["errors"]


class CitationModel(FakeModel):
    async def entity_and_relationship_extract(self, **kwargs):
        result = await super().entity_and_relationship_extract(**kwargs)
        payload = json.loads(kwargs["input_text"])
        if payload.get("number") == "1.2" and "children" not in payload:
            result["entities"].append({"name": "本规则1.1.1", "entity_type": "引用依据", "properties": {
                "文件全称": "", "条款编号": "1.1.1", "引用类型": "条款", "是否本文件内引用": "是", "引用原文": "本规则1.1.1"}})
            result["relations"].append({"source": "单元", "target": "本规则1.1.1", "type": "引用"})
        return result


@pytest.mark.asyncio
async def test_group_member_reference_keeps_specific_location_without_fake_article():
    text = "示例规则\n1.1 同时满足以下全部条件才可申请：\n1.1.1 已登记。\n1.1.2 已提交材料。\n1.2 企业应按本规则1.1.1的要求登记。"
    result = await RuleExtractor(config(), CitationModel()).extract_text(text, run_llm=True)
    assert result["status"] == "success", result["stats"]
    articles = [n for n in result["graph"]["nodes"] if n["node_type"] == "法条"]
    assert sorted(n["properties"]["条"] for n in articles) == ["1.1", "1.2"]
    assert any(e["properties"].get("引用定位") == "1.1.1" for e in result["graph"]["edges"])


def test_validator_preserves_quantitative_object_and_rejects_mismatch():
    unit = {"number": "1.1", "member_numbers": ["1.1"], "mode": "independent", "target": "1.1 企业应在5日内报告。", "context": ""}
    raw = {"entities": [{"name": "1.1", "entity_type": "法条", "properties": {"条": "1.1", "原文依据": unit["target"]}},
                        {"name": "报告", "entity_type": "条款单元", "properties": {"单元编号": "1.1", "条款单元内容": "企业应在5日内报告。", "原文依据": "企业应在5日内报告。", "功能类型": "必要", "量化特征": "定量",
                         "量化条件": {"原文件": "5日内", "量化值类型": "期限", "最小值": None, "最大值": 5, "单位": "日", "约束关系": "上限"}}}],
           "relations": [{"source": "1.1", "target": "报告", "type": "包含"}]}
    validated = validate_result(raw, source=unit["target"], unit=unit)
    assert isinstance(validated["entities"][1]["properties"]["量化条件"], dict)
    raw["entities"][1]["properties"]["量化特征"] = "混合"
    with pytest.raises(ValueError, match="inconsistent"):
        validate_result(raw, source=unit["target"], unit=unit)


@pytest.mark.asyncio
async def test_json_syntax_repair_and_schema_validation():
    class JsonModel(FakeModel):
        async def entity_and_relationship_extract(self, **kwargs):
            value = await super().entity_and_relationship_extract(**kwargs)
            return json.dumps(value, ensure_ascii=False)[:-1] + ",}"
    result = await RuleExtractor(config(), JsonModel()).extract_text("示例规则\n1.1 企业应保存记录。", run_llm=True)
    assert result["status"] == "success", result["stats"]


@pytest.mark.asyncio
async def test_oversize_parse_not_reported_as_success():
    result = await RuleExtractor(config(block_chars=8), FakeModel()).extract_text("示例规则\n1.1 企业应保存所有原始业务记录以及其他相关材料")
    assert result["status"] == "failed"
    assert result["stats"]["counts"]["file_processing_error"] == 1


@pytest.mark.asyncio
async def test_shared_scope_is_not_an_article_parent_or_cycle():
    text = "示例规则\n1.1 本规则适用于境内企业。\n1.2 本规则适用于境外分支机构。\n2 管理\n2.1 企业应保存记录。"
    result = await RuleExtractor(config(), FakeModel()).extract_text(text, run_llm=True)
    assert result["status"] == "success"
    articles = {n["node_id"] for n in result["graph"]["nodes"] if n["node_type"] == "法条"}
    assert not any(e["source_id"] in articles and e["target_id"] in articles and e["relation_type"] == "包含"
                   for e in result["graph"]["edges"])
    unit = next(u for u in result["plan"]["units"] if u["number"] == "2.1")
    assert "境内企业" in unit["context"] and "境外分支机构" in unit["context"]
    assert len(unit["parent_node_ids"]) == 1


@pytest.mark.asyncio
async def test_multiple_member_citations_keep_all_locations():
    class MultipleCitations(CitationModel):
        async def entity_and_relationship_extract(self, **kwargs):
            value = await super().entity_and_relationship_extract(**kwargs)
            payload = json.loads(kwargs["input_text"])
            if payload.get("number") == "1.2" and "children" not in payload:
                value["entities"].append({"name": "本规则1.1.2", "entity_type": "引用依据", "properties": {
                    "文件全称": "", "条款编号": "1.1.2", "引用类型": "条款", "是否本文件内引用": "是", "引用原文": "本规则1.1.2"}})
                value["relations"].append({"source": "单元", "target": "本规则1.1.2", "type": "引用"})
            return value
    text = "示例规则\n1.1 同时满足以下全部条件才可申请：\n1.1.1 已登记。\n1.1.2 已提交材料。\n1.2 企业应按本规则1.1.1与本规则1.1.2办理。"
    result = await RuleExtractor(config(), MultipleCitations()).extract_text(text, run_llm=True)
    assert result["status"] == "success", result["stats"]
    locations = [e["properties"]["引用定位"] for e in result["graph"]["edges"] if "引用定位" in e["properties"]]
    assert locations == [["1.1.1", "1.1.2"]]


def test_inseparable_group_cannot_be_split_across_model_inputs():
    parsed = parse("1.1 同时满足以下全部条件才可申请：\n1.1.1 已登记。\n1.1.2 已提交材料。")
    parent = parsed["nodes"][0]
    decision = {parent["node_id"]: {"mode": "group", "member_ids": parent["children"]}}
    plan = RuleUnitPlanner(block_chars=30).plan(parsed, decision)
    assert not plan["formal_plan_eligible"]
    assert any(e["code"] == "inseparable_group_too_long" for e in plan["errors"])
    assert not plan["units"][0]["chunks"]


def test_conjunctive_group_rejects_separate_child_semantic_unit():
    target = "1.1 同时满足以下全部条件才可申请：\n1.1.1 已登记。"
    payload = {"number": "1.1", "member_numbers": ["1.1", "1.1.1"], "mode": "group", "target": target, "context": ""}
    raw = {"entities": [{"name": "1.1", "entity_type": "法条", "properties": {"条": "1.1", "原文依据": target}},
            {"name": "子项", "entity_type": "条款单元", "properties": {"单元编号": "1.1.1", "条款单元内容": "已登记才可申请。", "原文依据": "已登记。", "功能类型": "必要"}}],
            "relations": [{"source": "1.1", "target": "子项", "type": "包含"}]}
    with pytest.raises(ValueError, match="conjunctive members"):
        validate_result(raw, source=target, unit=payload)


@pytest.mark.asyncio
async def test_exception_context_remains_source_backed():
    text = "示例规则\n1.1 除依法豁免的情形外，发生下列任一情形时企业应报告：\n1.1.1 主要业务中断。"
    result = await RuleExtractor(config(), FakeModel()).extract_text(text, run_llm=True)
    assert result["status"] == "success"
    article = next(n for n in result["graph"]["nodes"] if n["node_type"] == "法条")
    assert "豁免" not in article["properties"]["法条全文"]
    assert "豁免" in article["properties"]["上级上下文"]
    semantic = next(n for n in result["graph"]["nodes"] if n["node_type"] == "条款单元")
    assert "豁免" in semantic["properties"]["上下文依据"]


@pytest.mark.asyncio
async def test_offline_statistics_do_not_claim_incomplete_semantics():
    result = await RuleExtractor(config(), FakeModel()).extract_text("示例规则\n1.1 企业应保存记录。")
    assert result["stats"]["counts"].get("structure_only_files") == 1
    assert result["stats"]["counts"].get("incomplete_files", 0) == 0


def test_local_item_number_is_preserved_not_promoted_to_child_article():
    target = "1.1 记录管理\n（1）企业应保存记录。"
    payload = {"number": "1.1", "member_numbers": ["1.1"], "mode": "independent", "target": target, "context": ""}
    raw = {"entities": [{"name": "1.1", "entity_type": "法条", "properties": {"条": "1.1", "原文依据": target}},
        {"name": "保存记录", "entity_type": "条款单元", "properties": {"单元编号": "1.1（1）", "条款单元内容": "企业应保存记录。", "原文依据": "企业应保存记录。", "功能类型": "必要"}}],
        "relations": [{"source": "1.1", "target": "保存记录", "type": "包含"}]}
    assert validate_result(raw, source=target, unit=payload)
    raw["entities"][1]["properties"]["单元编号"] = "1.1.1"
    with pytest.raises(ValueError, match="不得把局部"):
        validate_result(raw, source=target, unit=payload)


@pytest.mark.parametrize("day_type", ["交易日", "工作日", "自然日"])
def test_explicit_day_type_cannot_be_collapsed(day_type):
    text = "1.1 企业应至少提前2个" + day_type + "报告。"
    payload = {"number": "1.1", "member_numbers": ["1.1"], "mode": "independent", "target": text, "context": ""}
    props = {"单元编号": "1.1", "条款单元内容": text, "原文依据": text, "功能类型": "必要", "量化特征": "定量",
             "量化条件": {"原文件": "至少提前2个" + day_type, "量化值类型": "期限", "最小值": 2, "最大值": None, "单位": "日", "约束关系": "下限"}}
    raw = {"entities": [{"name": "1.1", "entity_type": "法条", "properties": {"条": "1.1", "原文依据": text}},
                        {"name": "报告", "entity_type": "条款单元", "properties": props}],
           "relations": [{"source": "1.1", "target": "报告", "type": "包含"}]}
    with pytest.raises(ValueError, match="期限单位"):
        validate_result(raw, source=text, unit=payload)
    props["量化条件"]["单位"] = day_type
    assert validate_result(raw, source=text, unit=payload)
