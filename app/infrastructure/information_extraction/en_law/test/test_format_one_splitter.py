"""格式一英文法规切分器单元测试。

这些测试只验证规则切分器，不调用 LLM。
测试重点：
1. 能否分离合规风险类型、Whereas、Article 和 Annex。
2. 能否识别 Article 标题和 CHAPTER 层级。
3. 没有 Whereas 的文件是否可以继续切分，并给出 warning。
"""

# 导入待测试的一阶段切分函数。
from app.infrastructure.information_extraction.en_law.splitter import split_format_one_document


def test_splitter_separates_recitals_articles_and_annex():
    """验证典型格式一文件能被拆成风险类型、Whereas、Article 和 Annex。"""
    # 构造一个最小但结构完整的格式一法规样例。
    text = """合规风险类型：产品法律风险
REGULATION (EU) 2023/000 OF THE EUROPEAN PARLIAMENT AND OF THE COUNCIL of 1 January 2023 on sample rules

Having regard to the Treaty on the Functioning of the European Union,

## Whereas:

(1) This Regulation has an objective.

(2) This Regulation is necessary.

HAVE ADOPTED THIS REGULATION:

## CHAPTER I
GENERAL PROVISIONS

Article 1
Subject matter
1. This Regulation establishes rules.
(a) Operators shall comply with this Regulation.

Article 2
Definitions
For the purposes of this Regulation, the following definitions apply.

## ANNEX I
Sample annex
<table><tr><td>A</td></tr></table>
"""
    # 执行切分。
    result = split_format_one_document(text, filename="sample.md")
    # 验证中文业务风险类型被正确解析。
    assert result["compliance_risk_type"] == "产品法律风险"
    # 验证两个 recital 都被识别。
    assert len(result["recitals"]) == 2
    # 验证两个 Article 都被识别。
    assert len(result["clauses"]) == 2
    # 验证 Annex 元数据被识别。
    assert len(result["annexes_metadata"]) == 1
    # 默认不应把 Annex 原文放入 metadata。
    assert "content" not in result["annexes_metadata"][0]
    # 验证第一条 Article 编号。
    assert result["clauses"][0]["article_number"] == "Article 1"
    # 验证 Article 下一行标题被识别。
    assert result["clauses"][0]["article_heading"] == "Subject matter"
    # 验证 CHAPTER 层级被挂载到 Article。
    assert result["clauses"][0]["chapter"] == "CHAPTER I"
    # 验证 Article 内部数字段落被识别为 paragraph。
    assert result["clauses"][0]["structural_units"][0]["unit_level"] == "paragraph"


def test_splitter_allows_missing_whereas():
    """验证没有 Whereas 的英文法规仍可切分，并产生 no_recitals_detected warning。"""
    # 构造没有 Whereas、只有一条 Article 的最小 Directive 样例。
    text = """DIRECTIVE 2000/1/EU OF THE EUROPEAN PARLIAMENT AND OF THE COUNCIL of 1 January 2000

Article 1
Scope
1. This Directive applies to products.
"""
    # 执行切分。
    result = split_format_one_document(text, filename="no_whereas.md")
    # 没有 Whereas 时 recital 列表为空。
    assert len(result["recitals"]) == 0
    # 切分器应给出非致命 warning。
    assert "no_recitals_detected" in result["warnings"]
    # Article 正文仍应正常识别。
    assert len(result["clauses"]) == 1
