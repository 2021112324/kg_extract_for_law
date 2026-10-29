from dataclasses import replace
import pytest

from ..config import ParagraphConfig
from ..structure_parser import ParagraphStructureParser
from ..unit_planner import ParagraphUnitPlanner, split_paragraph, sentence_ranges
from ..io_utils import quote_range


@pytest.mark.parametrize("newline", ["\n", "\r\n", "\r"])
def test_source_ranges_cover_bom_metadata_heading_and_body(newline):
    text = newline.join(["\ufeff示例规定", "", "来源：示例机构", "", "企业应当保存记录。", "", "![图片](a.png)", "", "末段。"])
    parsed = ParagraphStructureParser().parse_text(text, "示例规定.txt")
    assert parsed["validation"]["passed"]
    assert parsed["title"] == "示例规定" and parsed["title_origin"] == "source"
    assert len(parsed["paragraphs"]) == 2
    for row in parsed["coverage"]:
        assert text[row["start"]:row["end"]] == row["text"]
    assert parsed["excluded"][0]["reason"] == "image_markup"


def test_short_body_is_not_invented_title_and_no_blank_lines_stay_one_paragraph():
    parsed = ParagraphStructureParser().parse_text("企业必须报告。\n其应当留存记录。", "不同名称.txt")
    assert len(parsed["paragraphs"]) == 1 and "企业必须报告" in parsed["paragraphs"][0]["text"]
    assert parsed["title_origin"] == "filename"
    one_line = ParagraphStructureParser(ParagraphConfig(paragraph_mode="one_line")).parse_text("甲段。\n乙段。")
    assert len(one_line["paragraphs"]) == 2


@pytest.mark.parametrize("source,code", [("第一条 企业应当报告。\n第二条 企业应当登记。", "numbered_document_requires_other_workflow"),
    ("|指标|值|\n|甲|1|", "unsupported_embedded_structure"),
    ("新华社北京2月24日电，报道甲。\n\n中新社上海2月25日电，报道乙。", "possible_multiple_documents")])
def test_nonprose_or_multisource_is_reviewed_not_silently_routed(source, code):
    parsed = ParagraphStructureParser().parse_text(source)
    assert any(d["code"] == code for d in parsed["diagnostics"])
    assert not ParagraphUnitPlanner().plan(parsed)["formal_plan_eligible"]


def test_news_signal_is_independent_of_filename():
    parsed = ParagraphStructureParser().parse_text("正式意见\n\n新华社北京2月24日电，某机关发布了意见。", "正式意见.txt")
    assert parsed["source_signals"][0]["institution"] == "新华社"
    assert "2026" not in parsed["source_signals"][0]["text"]


def test_sentence_boundaries_protect_decimal_url_quote_and_abbreviation():
    text = '值为3.14。见https://example.com/a.html。Dr. Smith说：“甲。乙。”最后一句。'
    parts = [text[a:b] for a, b in sentence_ranges(text)]
    assert "值为3.14。" in parts and "见https://example.com/a.html。" in parts
    assert any('“甲。乙。”' in p for p in parts)
    assert not any(p.endswith("Dr.") for p in parts)
    assert "".join(parts) == text


@pytest.mark.parametrize("text", ["这是未闭合的“引文。", "甲"*31+"。"])
def test_unclosed_and_indivisible_text_is_not_hard_truncated(text):
    with pytest.raises(ValueError):
        split_paragraph(text, 20)
    parsed = ParagraphStructureParser().parse_text(text)
    plan = ParagraphUnitPlanner(ParagraphConfig(block_chars=20)).plan(parsed)
    assert plan["review"] and not plan["formal_plan_eligible"]


def test_chunk_plan_has_exact_target_coverage_and_stable_paragraph_ids():
    source = "示例规定\n\n"+"甲企业应当保存记录。乙企业应当报告风险。"*4
    parsed = ParagraphStructureParser().parse_text(source, "示例规定.txt")
    plan = ParagraphUnitPlanner(ParagraphConfig(block_chars=25, context_chars=500)).plan(parsed)
    assert len(plan["units"]) > 1
    assert "".join(u["target"] for u in plan["units"]) == parsed["paragraphs"][0]["text"]
    assert {u["paragraph_id"] for u in plan["units"]} == {parsed["paragraphs"][0]["paragraph_id"]}
    alternate = ParagraphUnitPlanner(ParagraphConfig(block_chars=60)).plan(parsed)
    assert {u["paragraph_id"] for u in alternate["units"]} == {u["paragraph_id"] for u in plan["units"]}


def test_heading_boundary_blocks_neighbor_scope_inheritance():
    source = "# 示例规定\n\n## 范围甲\n\n本规定仅适用于甲企业。\n\n## 范围乙\n\n其应当报告。"
    parsed = ParagraphStructureParser().parse_text(source, "示例规定.md")
    unit = ParagraphUnitPlanner().plan(parsed)["units"][-1]
    assert all("仅适用于甲企业" not in c["text"] for c in unit["context"])


def test_context_budget_does_not_silently_drop_required_scope():
    parsed = ParagraphStructureParser().parse_text("甲"*50+"。\n\n其应当报告。")
    plan = ParagraphUnitPlanner(ParagraphConfig(context_chars=10)).plan(parsed)
    assert any(r["reason"] == "necessary_context_exceeds_budget" for r in plan["review"])


def test_source_identity_and_quote_resolution():
    parser = ParagraphStructureParser()
    assert parser.parse_text("相同正文。", source_path="a/x.txt")["document_id"] != parser.parse_text("相同正文。", source_path="b/x.txt")["document_id"]
    assert quote_range("应当报告", "企业应当\n报告。", 10) == {"start": 12, "end": 17}
    with pytest.raises(ValueError, match="ambiguous"):
        quote_range("报告", "报告后再报告")


@pytest.mark.parametrize("kwargs", [{"max_attempts": 6}, {"max_attempts": True}, {"max_concurrent": 0}, {"timeout": float("nan")}, {"paragraph_mode": "guess"}, {"publication": "both"}])
def test_invalid_settings_are_rejected(kwargs):
    with pytest.raises(ValueError):
        ParagraphConfig(**kwargs)
