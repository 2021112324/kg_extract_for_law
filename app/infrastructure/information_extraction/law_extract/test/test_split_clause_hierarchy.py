import asyncio
import json
import re
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(ROOT))

from app.infrastructure.information_extraction.law_extract.clause_extract import (
    ClauseExtractor,
    ResultStats,
)


OUT_DIR = Path(__file__).resolve().parent
SOURCES = [
    Path(r"D:\CogmAIT\8.1项目\8.1数据\处理的数据\法律规章全汇总\过长\民法典.txt"),
    Path(r"D:\CogmAIT\8.1项目\8.1数据\处理的数据\法律规章全汇总\过长\中华人民共和国民事诉讼法.txt"),
    Path(r"D:\CogmAIT\8.1项目\8.1数据\处理的数据\法律规章全汇总\过长\中华人民共和国刑法.txt"),
]

HEADING_STRIP_CHARS = " \t\r\n\f\v#-*•·"
CN_NUM_CHARS = r"零一二三四五六七八九十百千万\d"
ARTICLE_RE = re.compile(rf"^第([{CN_NUM_CHARS}]+)\s*条\s*(之[{CN_NUM_CHARS}]+)?")
PART_RE = re.compile(rf"^第[{CN_NUM_CHARS}]+\s*编\s+.*")
SUBPART_RE = re.compile(rf"^第[{CN_NUM_CHARS}]+\s*分编\s+.*")
CHAPTER_RE = re.compile(rf"^第[{CN_NUM_CHARS}]+\s*章\s+.*")
SECTION_RE = re.compile(rf"^第[{CN_NUM_CHARS}]+\s*节\s+.*")
END_MARKERS = ("附录", "附件", "附表", "后记", "参考文献", "索引")


def clean_heading(line: str) -> str:
    return line.strip().lstrip(HEADING_STRIP_CHARS)


def normalize_article_heading(line: str) -> str | None:
    match = ARTICLE_RE.match(clean_heading(line))
    if not match:
        return None
    return f"第{match.group(1)}条{match.group(2) or ''}"


def scan_raw_articles(text: str) -> list[dict]:
    rows = []
    started = False
    current = {"编": "", "分编": "", "章": "", "节": ""}
    for line_no, line in enumerate(text.splitlines(), start=1):
        line_clean = clean_heading(line)
        if not line_clean:
            continue
        if any(line_clean.startswith(marker) for marker in END_MARKERS):
            if started:
                break
        if PART_RE.match(line_clean):
            current.update({"编": line_clean, "分编": "", "章": "", "节": ""})
            continue
        if SUBPART_RE.match(line_clean):
            current.update({"分编": line_clean, "章": "", "节": ""})
            continue
        if CHAPTER_RE.match(line_clean):
            current.update({"章": line_clean, "节": ""})
            continue
        if SECTION_RE.match(line_clean):
            current["节"] = line_clean
            continue
        article = normalize_article_heading(line_clean)
        if article:
            started = True
            rows.append({"line_no": line_no, "条款编号": article, **current})
    return rows


def duplicate_values(values: list[str]) -> dict[str, int]:
    return {key: count for key, count in Counter(values).items() if count > 1}


def pick_samples(clauses: list[dict]) -> list[dict]:
    if not clauses:
        return []
    indexes = [0, 1, 20, len(clauses) - 2, len(clauses) - 1]
    samples = []
    for idx in dict.fromkeys(i for i in indexes if 0 <= i < len(clauses)):
        clause = clauses[idx]
        samples.append({
            "index": idx,
            "编": clause.get("编", ""),
            "分编": clause.get("分编", ""),
            "章": clause.get("章", ""),
            "节": clause.get("节", ""),
            "条款编号": clause.get("条款编号", ""),
            "条款内容前80字": (clause.get("条款内容", "") or "")[:80],
        })
    return samples


async def main():
    extractor = ClauseExtractor.__new__(ClauseExtractor)
    extractor.result_stats = ResultStats()

    summary = {
        "test_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "output_dir": str(OUT_DIR),
        "files": [],
    }

    for source in SOURCES:
        text = source.read_text(encoding="utf-8")
        raw_articles = scan_raw_articles(text)
        result = await extractor.split_clause(text)
        clauses = result["clauses"]

        raw_numbers = [item["条款编号"] for item in raw_articles]
        split_numbers = [item.get("条款编号", "") for item in clauses]
        raw_set = set(raw_numbers)
        split_set = set(split_numbers)

        file_result = {
            "source_file": str(source),
            "file_name": source.name,
            "raw_article_count": len(raw_articles),
            "split_clause_count": len(clauses),
            "count_match": len(raw_articles) == len(clauses),
            "raw_first_article": raw_articles[0] if raw_articles else None,
            "raw_last_article": raw_articles[-1] if raw_articles else None,
            "split_first_clause": {
                key: clauses[0].get(key, "")
                for key in ["编", "分编", "章", "节", "条款编号"]
            } if clauses else None,
            "split_last_clause": {
                key: clauses[-1].get(key, "")
                for key in ["编", "分编", "章", "节", "条款编号"]
            } if clauses else None,
            "last_article_match": bool(
                raw_articles and clauses and raw_articles[-1]["条款编号"] == clauses[-1].get("条款编号")
            ),
            "duplicate_raw_article_numbers": duplicate_values(raw_numbers),
            "duplicate_split_clause_numbers": duplicate_values(split_numbers),
            "missing_in_split": sorted(raw_set - split_set),
            "extra_in_split": sorted(split_set - raw_set),
            "clauses_with_part_count": sum(1 for item in clauses if item.get("编")),
            "clauses_with_subpart_count": sum(1 for item in clauses if item.get("分编")),
            "clauses_with_chapter_count": sum(1 for item in clauses if item.get("章")),
            "clauses_with_section_count": sum(1 for item in clauses if item.get("节")),
            "samples": pick_samples(clauses),
        }

        detail_path = OUT_DIR / f"split_clause_{source.stem}.json"
        detail_path.write_text(
            json.dumps({"source_file": str(source), "clauses": clauses}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        file_result["detail_path"] = str(detail_path)
        summary["files"].append(file_result)

    summary_path = OUT_DIR / "split_clause_hierarchy_test_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    md = ["# split_clause 编/分编功能测试报告", "", f"测试时间：{summary['test_time']}", ""]
    for item in summary["files"]:
        md.extend([
            f"## {item['file_name']}",
            "",
            f"- 原文识别条目数：{item['raw_article_count']}",
            f"- split_clause 切分条数：{item['split_clause_count']}",
            f"- 总条数是否一致：{'是' if item['count_match'] else '否'}",
            f"- 最后一条是否一致：{'是' if item['last_article_match'] else '否'}",
            f"- 原文最后一条：{item['raw_last_article']}",
            f"- 切分最后一条：{item['split_last_clause']}",
            f"- 含编属性条数：{item['clauses_with_part_count']}",
            f"- 含分编属性条数：{item['clauses_with_subpart_count']}",
            f"- 含章属性条数：{item['clauses_with_chapter_count']}",
            f"- 含节属性条数：{item['clauses_with_section_count']}",
            f"- 重复切分条号数量：{len(item['duplicate_split_clause_numbers'])}",
            f"- 原文有但切分缺失条号数量：{len(item['missing_in_split'])}",
            f"- 切分有但原文未识别条号数量：{len(item['extra_in_split'])}",
            "",
            "### 样例",
            "",
            "```json",
            json.dumps(item["samples"], ensure_ascii=False, indent=2),
            "```",
            "",
        ])
        if item["duplicate_split_clause_numbers"]:
            md.extend([
                "### 重复切分条号",
                "",
                "```json",
                json.dumps(item["duplicate_split_clause_numbers"], ensure_ascii=False, indent=2),
                "```",
                "",
            ])
        if item["missing_in_split"]:
            md.extend([
                "### 原文有但切分缺失",
                "",
                "```json",
                json.dumps(item["missing_in_split"], ensure_ascii=False, indent=2),
                "```",
                "",
            ])
        if item["extra_in_split"]:
            md.extend([
                "### 切分有但原文未识别",
                "",
                "```json",
                json.dumps(item["extra_in_split"], ensure_ascii=False, indent=2),
                "```",
                "",
            ])

    report_path = OUT_DIR / "split_clause_hierarchy_test_report.md"
    report_path.write_text("\n".join(md), encoding="utf-8")

    print(f"summary={summary_path}")
    print(f"report={report_path}")


if __name__ == "__main__":
    asyncio.run(main())
