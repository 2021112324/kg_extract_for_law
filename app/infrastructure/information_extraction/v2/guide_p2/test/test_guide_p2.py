import asyncio
import json
import logging
import pytest

from ..config import GuideP2Config
from ..extractor import GuideP2Extractor
from ..structure_parser import GuideP2StructureParser
from ..unit_planner import GuideP2UnitPlanner
from ..validator import validate_batch, CONTRACT
from ..standalone import GuideP2StandaloneRunner


def config(**kwargs):
    return GuideP2Config(retry_delay=0, **kwargs)


class FakeModel:
    def __init__(self, fail=0, empty=False, reverse=False, missing=False, delay=0):
        self.calls, self.fail, self.empty = [], fail, empty
        self.reverse, self.missing, self.delay = reverse, missing, delay

    async def entity_and_relationship_extract(self, **kwargs):
        self.calls.append(kwargs)
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.fail:
            self.fail -= 1
            raise ValueError("synthetic format failure")
        payload = json.loads(kwargs["input_text"])
        if "title" in payload:
            title = payload["title"]
            return {"entities": [{"name": title, "entity_type": "合规指引文件", "properties": {"文件全称": title, "文件类型": "目录", "证据原文": title}}]}
        entities = []
        for entry in payload["entries"]:
            key, target = entry["entry_id"], entry["target"]
            if self.missing and len(payload["entries"]) > 1:
                continue
            entities.append({"name": "回执_"+key, "entity_type": "条目处理", "properties": {"目标条目ID": key, "处理状态": "非知识" if self.empty else "知识", "理由": "模拟响应"}})
            if not self.empty:
                props = {"目标条目ID": key, "知识内容": entry["context"][0]["text"]+"包括："+target,
                         "知识类型": "目录归属", "约束强度": "不适用", "行为描述": "", "原文依据": target,
                         "上下文依据": [c["text"] for c in entry["context"]], "适用条件": "\n".join(c["text"] for c in entry["context"]), "是否知识条目候选": True}
                entities.append({"name": "知识_"+key, "entity_type": "指引知识单元", "properties": props})
        return {"entities": list(reversed(entities)) if self.reverse else entities, "relations": []}


SOURCE = "示例行业目录\n（2020年版）\n一、甲行业\n二、乙行业\n三、需要限制境外投资的行业：\n（一）房地产\n（二）酒店"


def parsed(text=SOURCE):
    return GuideP2StructureParser().parse_text(text)


def test_empty_content_entries_are_candidates():
    p = parsed()
    plan = GuideP2UnitPlanner().plan(p)
    assert p["validation"]["passed"]
    assert len(plan["units"]) == len(p["nodes"]) == 5
    assert plan["counts"]["empty_content_candidates"] == 5
    assert any("房地产" in u["target"] for u in plan["units"])


def test_parent_scope_not_copied_into_unrelated_entries():
    units = GuideP2UnitPlanner().plan(parsed())["units"]
    first = next(u for u in units if u["number"] == "一、")
    child = next(u for u in units if u["number"] == "（一）")
    assert not any("需要限制" in c["text"] for c in first["context"])
    assert any("需要限制" in c["text"] for c in child["context"])
    assert child["target"] == "（一）房地产"


@pytest.mark.parametrize("marker", ["附件：示例产品目录", "附件: 示例产品目录", "附件1：示例产品目录"])
def test_target_attachment_is_not_truncated(marker):
    text = "示例产品目录\n决定发布目录，对列入目录内的商品，未取得认证不得销售。" + "说明"*20 + "\n" + marker + "\n" + marker + "\n一、设备\n（一）某产品"
    p = parsed(text)
    assert p["validation"]["passed"], p["diagnostics"]
    assert any("某产品" in n["title"] for n in p["nodes"])
    assert len([e for e in p["excluded"] if e["reason"] == "target_attachment_pointer"]) == 2
    assert any(n["kind"] == "publication" for n in p["nodes"])


def test_real_form_is_excluded_but_ambiguous_document_fails():
    p = parsed("示例目录\n一、甲\n附表：空白登记表\n姓名：")
    assert p["validation"]["passed"] and any(e["reason"] == "non_target_form" for e in p["excluded"])
    p = parsed("示例目录\n一、甲\n附件：另一份独立目录\n一、乙")
    assert not p["validation"]["passed"]


def test_local_number_scopes_and_numeric_data():
    p = parsed("示例产品目录\n一、甲类\n1. 产品甲\n2. 产品乙\n二、乙类\n1. 产品丙\n额定电压450/750V及以下，型号1234。")
    assert p["validation"]["passed"]
    items = [n for n in p["nodes"] if n["number"] == "1."]
    assert len(items) == 2 and items[0]["parent_id"] != items[1]["parent_id"]
    assert "450/750V" in p["nodes"][-1]["content"]


def test_punctuation_and_exclusion_stay_in_original():
    p = parsed("示例产品目录\n一、设备（不包括低于36V的设备）\n产品甲（家用、工业用）、产品乙：应保留完整条件。")
    target = GuideP2UnitPlanner().plan(p)["units"][0]["target"]
    assert "家用、工业用" in target and "不包括低于36V" in target


def test_conflicts_and_numeric_rule_do_not_get_renumbered():
    assert not parsed("示例目录\n一、甲\n一、乙")["validation"]["passed"]
    assert not parsed("示例目录\n1.1 企业应报告。")["validation"]["passed"]


def test_plan_budget_and_transport_batches():
    p = parsed()
    plan = GuideP2UnitPlanner(config(batch_size=2)).plan(p)
    assert max(len(b["entries"]) for b in plan["batches"]) <= 2
    child_batch = next(b for b in plan["batches"] if any("房地产" in e["target"] for e in b["entries"]))
    assert all("甲行业" not in e["target"] for e in child_batch["entries"])
    small = GuideP2UnitPlanner(config(context_chars=5)).plan(p)
    assert small["errors"] and not small["formal_plan_eligible"]


@pytest.mark.asyncio
async def test_offline_never_calls_model_or_builds_semantic_graph():
    model = FakeModel()
    result = await GuideP2Extractor(config(), model).extract_text(SOURCE)
    assert result["status"] == "structure_only" and not model.calls
    assert not result["graph"]["nodes"] and not result["formal_graph_eligible"]


@pytest.mark.asyncio
async def test_success_keeps_new_enum_and_removes_transport_attributes():
    result = await GuideP2Extractor(config(), FakeModel(reverse=True)).extract_text(SOURCE, run_llm=True)
    assert result["status"] == "success", result["stats"]
    semantic = [n for n in result["graph"]["nodes"] if n["node_type"] == "指引知识单元"]
    assert len(semantic) == 5
    assert all(n["properties"]["知识类型"] == "目录归属" and n["properties"]["约束强度"] == "不适用" for n in semantic)
    assert not any("目标条目ID" in n["properties"] or n["node_type"] == "条目处理" for n in result["graph"]["nodes"])


@pytest.mark.asyncio
async def test_split_retry_budget_and_feedback():
    model = FakeModel(missing=True)
    result = await GuideP2Extractor(config(), model).extract_text(SOURCE, run_llm=True)
    assert result["status"] == "success", result["stats"]
    assert result["stats"]["counts"]["split_batches"] > 0
    assert max(result["extraction_result"]["attempts_by_entry"].values()) <= 5
    assert any("上次失败" in c["user_prompt"] for c in model.calls)


@pytest.mark.asyncio
async def test_all_five_attempts_fail_without_skeleton_fallback():
    model = FakeModel(fail=100)
    result = await GuideP2Extractor(config(), model).extract_text(SOURCE, run_llm=True)
    assert len(model.calls) == 5
    assert result["stats"]["counts"]["error"] == 1
    assert not result["graph"]["nodes"]


@pytest.mark.asyncio
async def test_entry_budget_is_not_reset_after_batch_split():
    class BadEntries(FakeModel):
        async def entity_and_relationship_extract(self, **kwargs):
            if "entries" in json.loads(kwargs["input_text"]):
                self.calls.append(kwargs)
                return {"entities": []}
            return await super().entity_and_relationship_extract(**kwargs)
    result = await GuideP2Extractor(config(), BadEntries()).extract_text(SOURCE, run_llm=True)
    assert result["status"] == "incomplete"
    assert set(result["extraction_result"]["attempts_by_entry"].values()) == {5}
    assert len(result["stats"]["errors"]) == 5


@pytest.mark.asyncio
async def test_empty_catalogue_entries_require_review():
    result = await GuideP2Extractor(config(), FakeModel(empty=True)).extract_text(SOURCE, run_llm=True)
    assert result["status"] == "needs_review" and not result["formal_graph_eligible"]


@pytest.mark.asyncio
async def test_partial_is_not_formal_and_batch_size_does_not_change_ids():
    first = await GuideP2Extractor(config(batch_size=1), FakeModel()).extract_text(SOURCE, run_llm=True)
    second = await GuideP2Extractor(config(batch_size=12), FakeModel()).extract_text(SOURCE, run_llm=True)
    assert {n["node_id"] for n in first["graph"]["nodes"]} == {n["node_id"] for n in second["graph"]["nodes"]}
    partial = await GuideP2Extractor(config(), FakeModel()).extract_text(SOURCE, run_llm=True, max_entries=1)
    assert partial["status"] == "partial_test" and not partial["formal_graph_eligible"]


@pytest.mark.asyncio
async def test_heartbeat_and_audit_redaction(tmp_path, caplog):
    caplog.set_level(logging.INFO)
    extractor = GuideP2Extractor(config(heartbeat_seconds=.01, api_key="fake_private_value"), FakeModel(delay=.03))
    extractor.audit_path = tmp_path/"audit.jsonl"
    await extractor.extract_text("示例目录\n一、甲", run_llm=True)
    assert any("guide_p2 heartbeat" in r.message for r in caplog.records)
    before = len(caplog.records)
    await asyncio.sleep(.04)
    assert before == len(caplog.records)
    assert "fake_private_value" not in extractor.audit_path.read_text(encoding="utf8")


@pytest.mark.asyncio
@pytest.mark.parametrize("defect", ["prohibition", "context", "field", "type", "id", "exclusion", "task", "orphan"])
async def test_invalid_semantics_are_rejected(defect):
    p = parsed("示例目录\n一、产品（不包括低于36V的设备）")
    entry = GuideP2UnitPlanner().plan(p)["batches"][0]["entries"][0]
    raw = await FakeModel().entity_and_relationship_extract(input_text=json.dumps({"entries": [entry]}))
    props = raw["entities"][1]["properties"]
    if defect == "prohibition": props["知识内容"] += "禁止所有企业生产。"
    elif defect == "context": props["上下文依据"] = ["别的兄弟条目的限制"]
    elif defect == "field": props["动态属性"] = "错误"
    elif defect == "type": props["知识类型"] = "新的未知类型"
    elif defect == "id": props["目标条目ID"] = "不存在"
    elif defect == "exclusion": props["知识内容"] = "示例目录包括产品。"; props["适用条件"] = ""
    elif defect == "task": props["行为描述"] = "实施认证"
    else: raw["entities"].append({"name": "无归属引用", "entity_type": "引用依据", "properties": {"目标条目ID": entry["entry_id"], "文件全称": "示例目录", "引用原文": "示例目录"}})
    with pytest.raises(ValueError): validate_batch(raw, [entry])


@pytest.mark.asyncio
async def test_syntax_repair_still_validates():
    class JsonModel(FakeModel):
        async def entity_and_relationship_extract(self, **kwargs):
            raw = await super().entity_and_relationship_extract(**kwargs)
            return json.dumps(raw, ensure_ascii=False)[:-1] + ",}"
    result = await GuideP2Extractor(config(), JsonModel()).extract_text(SOURCE, run_llm=True)
    assert result["status"] == "success", result["stats"]
    assert result["stats"]["counts"]["json_repair_successes"] > 0


@pytest.mark.asyncio
async def test_runner_and_storage_outcomes(tmp_path):
    from ...rule.test.test_rule import FakeStorage
    source = tmp_path/"in.txt"
    source.write_text(SOURCE, encoding="utf8")
    before = source.read_bytes()
    for saved in (True, False, None):
        storage = FakeStorage(saved=saved)
        runner = GuideP2StandaloneRunner(config(), storage, lambda config: GuideP2Extractor(config, FakeModel()))
        result = await runner.run(source, tmp_path/str(saved), "write", "p2_test_isolated")
        assert result["files"][0]["storage_status"] == ("confirmed" if saved is True else "failed_or_unknown")
        assert storage.connected == 0 and before == source.read_bytes()
    storage = FakeStorage()
    runner = GuideP2StandaloneRunner(config(), storage)
    result = await runner.run(source, tmp_path/"parse")
    assert storage.connected == 0 and result["status"] == "completed"


def test_schema_and_defaults_are_independent():
    from ...guide_p1.prompt.schema import KNOWLEDGE_TYPE_VALUES
    from .. import run_extraction
    assert "目录归属" not in KNOWLEDGE_TYPE_VALUES
    assert "上下文依据" in CONTRACT.allowed_properties["指引知识单元"]
    assert run_extraction.MODE == "parse" and not run_extraction.INPUT_PATH
    with pytest.raises(ValueError): config(batch_size=0)
    from ..prompt.schema import schema_for_content_model, schema_for_content
    assert "指引结构节点-提出" not in schema_for_content_model
    assert "指引结构节点-提出" in schema_for_content


@pytest.mark.asyncio
async def test_valid_normative_auxiliaries_require_explicit_relations():
    target = "一、企业应当在5日内按《甲办法》提交报告。"
    entry = GuideP2UnitPlanner().plan(parsed("示例指引\n"+target))["batches"][0]["entries"][0]
    raw = await FakeModel().entity_and_relationship_extract(input_text=json.dumps({"entries": [entry]}))
    key = entry["entry_id"]
    knowledge = raw["entities"][1]
    knowledge["properties"].update(知识内容=target, 知识类型="行动任务", 约束强度="强制任务", 行为描述="提交报告")
    extras = [
        {"name": "报告期限", "entity_type": "量化目标", "properties": {"目标条目ID": key, "原始文本": "5日内", "量化值类型": "期限", "最小值": None, "最大值": 5, "单位": "日", "约束关系": "上限"}},
        {"name": "企业", "entity_type": "责任主体", "properties": {"目标条目ID": key, "主体名称": "企业", "责任角色": "落实", "责任原文": target}},
        {"name": "甲办法", "entity_type": "引用依据", "properties": {"目标条目ID": key, "文件全称": "甲办法", "引用原文": "《甲办法》"}},
    ]
    raw["entities"].extend(extras)
    raw["relations"] = [{"source": knowledge["name"], "target": e["name"], "type": kind} for e, kind in zip(extras, ["设定目标", "由其负责", "引用"])]
    validated = validate_batch(raw, [entry])
    assert len(validated[0]["entities"]) == 4 and len(validated[0]["relations"]) == 3
    raw["relations"].pop()
    with pytest.raises(ValueError, match="unowned"):
        validate_batch(raw, [entry])


@pytest.mark.asyncio
@pytest.mark.parametrize("risks", ["产品法律风险", ["不是六类风险"], ["产品法律风险"]])
async def test_risk_types_cannot_be_inferred_or_stringified(risks):
    entry = GuideP2UnitPlanner().plan(parsed())["batches"][0]["entries"][0]
    raw = await FakeModel().entity_and_relationship_extract(input_text=json.dumps({"entries": [entry]}))
    raw["entities"][1]["properties"]["合规风险类型"] = risks
    with pytest.raises(ValueError, match="Risk types"):
        validate_batch(raw, [entry])


@pytest.mark.asyncio
async def test_control_receipt_duplicates_and_cross_entry_edges_fail():
    entries = GuideP2UnitPlanner().plan(parsed())["batches"][0]["entries"]
    raw = await FakeModel().entity_and_relationship_extract(input_text=json.dumps({"entries": entries}))
    raw["entities"].append(raw["entities"][0])
    with pytest.raises(ValueError): validate_batch(raw, entries)
    raw["entities"].pop()
    raw["entities"][0]["properties"]["理由"] = True
    with pytest.raises(ValueError): validate_batch(raw, entries)
    raw["entities"][0]["properties"]["理由"] = "有效处理理由"
    raw["relations"].append({"source": raw["entities"][1]["name"], "target": raw["entities"][3]["name"], "type": "引用"})
    with pytest.raises(ValueError): validate_batch(raw, entries)


@pytest.mark.asyncio
async def test_timeout_budget_and_error_category():
    model = FakeModel(delay=.03)
    result = await GuideP2Extractor(config(timeout=.005), model).extract_text(SOURCE, run_llm=True)
    assert len(model.calls) == 5 and result["status"] == "incomplete"
    assert result["stats"]["counts"]["model_attempt_errors"] == 5
    assert result["stats"]["counts"]["unexecuted_entries"] == 5


@pytest.mark.asyncio
async def test_non_catalogue_decorative_heading_can_be_reasonably_empty():
    result = await GuideP2Extractor(config(), FakeModel(empty=True)).extract_text("示例指引\n一、说明", run_llm=True)
    assert result["status"] == "success"
    assert result["stats"]["counts"]["entries_reasonable_empty"] == 1


@pytest.mark.asyncio
async def test_incomplete_never_written_and_verification_failure_counted(tmp_path):
    from ...rule.test.test_rule import FakeStorage
    source = tmp_path/"source.txt"
    source.write_text(SOURCE, encoding="utf8")
    storage = FakeStorage()
    runner = GuideP2StandaloneRunner(config(), storage, lambda config: GuideP2Extractor(config, FakeModel(fail=100)))
    result = await runner.run(source, tmp_path/"failed_model", "write", "p2_unique")
    assert storage.graph is None and result["files"][0]["storage_status"] == "skipped_ineligible"
    storage = FakeStorage(fail_verification=True)
    runner = GuideP2StandaloneRunner(config(), storage, lambda config: GuideP2Extractor(config, FakeModel()))
    result = await runner.run(source, tmp_path/"failed_verification", "write", "p2_unique")
    assert result["files"][0]["storage_status"] == "failed_or_unknown"
    assert result["stats"]["counts"]["storage_error"] == 1


@pytest.mark.asyncio
async def test_original_line_offsets_and_bom_are_preserved(tmp_path):
    path = tmp_path/"source.md"
    text = "\ufeff# 示例目录\r\n一、甲产品（家用、工业用）\r\n（二）乙产品\r\n"
    path.write_bytes(text.encode("utf8"))
    p = GuideP2StructureParser().parse_file(path)
    decoded = path.read_bytes().decode("utf-8-sig")
    for node in p["nodes"]:
        for row in node["own_lines"]:
            assert decoded[row["start"]:row["end"]].rstrip("\r\n") == row["raw"]
    assert path.read_bytes() == text.encode("utf8")


def test_standalone_numbered_list_has_no_invented_parent():
    p = parsed("示例清单\n1. 甲\n2. 乙\n3. 丙")
    assert p["validation"]["passed"]
    assert all(not n["parent_id"] for n in p["nodes"])


@pytest.mark.asyncio
async def test_unexecuted_selection_is_explicit_and_unknown_id_rejected():
    extractor = GuideP2Extractor(config(), FakeModel())
    with pytest.raises(ValueError, match="Unknown selected"):
        await extractor.extract_text(SOURCE, run_llm=True, entry_ids=["does_not_exist"])
    result = await extractor.extract_text(SOURCE, run_llm=True, max_entries=0)
    assert result["status"] == "partial_test" and not result["formal_graph_eligible"]


def test_source_identity_separates_explicit_versions_and_publication_dates():
    parser = GuideP2StructureParser()
    first = parser.parse_text("示例目录\n发布日期：2020.01.01\n一、甲产品")
    second = parser.parse_text("示例目录\n发布日期：2021.01.01\n一、甲产品")
    assert first["document_id"] != second["document_id"]
    first = parser.parse_text("示例目录\n(2020年版)\n一、甲产品")
    second = parser.parse_text("示例目录\n(2021年版)\n一、甲产品")
    assert first["document_id"] != second["document_id"]


def test_file_metadata_cannot_be_invented():
    from ..validator import validate_file
    source = "示例目录\n发布日期：2020.01.01"
    raw = {"entities": [{"name": "示例目录", "entity_type": "合规指引文件", "properties": {"文件全称": "示例目录", "证据原文": "示例目录", "发布单位": "原文没有的机构"}}]}
    with pytest.raises(ValueError, match="metadata"):
        validate_file(raw, "示例目录", source)
    raw["entities"][0]["properties"].update(发布单位="", 发布日期="2020年1月1日")
    assert validate_file(raw, "示例目录", source)
    raw["entities"][0]["properties"]["发布日期"] = "2021年1月1日"
    with pytest.raises(ValueError, match="date"):
        validate_file(raw, "示例目录", source)


def test_explicit_unnumbered_nominal_catalogue_has_no_fabricated_numbers():
    p = parsed("示例行业目录\n(2020年版)\n房地产\n酒店\n体育俱乐部")
    assert p["validation"]["passed"]
    assert [n["title"] for n in p["nodes"]] == ["房地产", "酒店", "体育俱乐部"]
    assert all(n["number"] == "" and n["parent_id"] == "" for n in p["nodes"])
    assert len(GuideP2UnitPlanner().plan(p)["units"]) == 3


@pytest.mark.asyncio
async def test_splitting_membership_and_scope_cannot_broaden_the_same_object():
    entry = GuideP2UnitPlanner().plan(parsed("示例产品目录\n一、甲产品：容量不超过500L。"))["batches"][0]["entries"][0]
    raw = await FakeModel().entity_and_relationship_extract(input_text=json.dumps({"entries": [entry]}))
    props = raw["entities"][1]["properties"]
    props.update(适用对象="甲产品", 知识内容="示例产品目录包括甲产品。", 适用条件="")
    scope = dict(props, 知识类型="范围界定", 知识内容="甲产品容量不超过500L。", 适用条件="容量不超过500L")
    raw["entities"].append({"name": "范围", "entity_type": "指引知识单元", "properties": scope})
    with pytest.raises(ValueError, match="范围条件"):
        validate_batch(raw, [entry])
    props["适用条件"] = "容量不超过500L"
    assert validate_batch(raw, [entry])


def test_explicit_item_can_start_with_numerical_product_name():
    p = parsed("示例产品目录\n一、电气装置\n1. 36V设备\n2. 500W设备")
    assert [n["number"] for n in p["nodes"]] == ["一、", "1.", "2."]
    assert p["validation"]["passed"]


@pytest.mark.asyncio
@pytest.mark.parametrize("source_connector,wrong", [("和", "或"), ("或", "和")])
async def test_negative_condition_connectors_cannot_be_swapped(source_connector, wrong):
    text = "示例指引\n一、未获得证书"+source_connector+"未加施标志的产品不得销售。"
    entry = GuideP2UnitPlanner().plan(parsed(text))["batches"][0]["entries"][0]
    raw = await FakeModel().entity_and_relationship_extract(input_text=json.dumps({"entries": [entry]}))
    props = raw["entities"][1]["properties"]
    props.update(知识类型="禁止性要求", 约束强度="强制任务", 行为描述="不得销售", 知识内容=entry["target"], 适用条件="未获得证书"+wrong+"未加施标志")
    with pytest.raises(ValueError, match="逻辑"):
        validate_batch(raw, [entry])
    props["适用条件"] = "未获得证书 " + wrong + " 未加施标志"
    with pytest.raises(ValueError, match="逻辑"):
        validate_batch(raw, [entry])
    props["适用条件"] = "未获得证书"+source_connector+"未加施标志"
    assert validate_batch(raw, [entry])
