"""Read actual local materials without changing them; formal text is an explicit excerpt."""
import asyncio
from hashlib import sha256
import os
from pathlib import Path
import re
import sys

REPO = next(p for p in Path(__file__).resolve().parents if (p/"app/infrastructure").is_dir())
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from app.infrastructure.information_extraction.v2.paragraph import ParagraphExtractor
from app.infrastructure.information_extraction.v2.paragraph.io_utils import save_json

ROOT = Path(os.getenv("PARAGRAPH_SAMPLE_ROOT", r"D:\CogmAIT\8.1项目\8.1数据\合规风险指标映射数据\v1_923\清洗名称后的纯文本数据\其他格式"))
OUTPUT = REPO/"app/infrastructure/information_extraction/v2/task/测试报告/paragraph_测试记录"


def actual_samples():
    news = ROOT/"关于加强新时代廉洁文化建设的意见.txt"
    formal = ROOT/"关于建立公共资源交易领域失信联合惩戒机制的指导意见.txt"
    rows = []
    for path in (news, formal):
        raw = path.read_bytes()
        text = raw.decode("utf-8-sig")
        record = {"original_path": str(path), "original_sha256": sha256(raw).hexdigest(), "actual_source": True}
        if path == news:
            record["selection"] = "full_current_news_material_not_policy_full_text"
            rows.append((path.name, text, str(path), record))
        else:
            lines = text.splitlines()
            title = next(line.strip() for line in lines if line.strip() == "关于对公共资源交易领域严重失信主体开展联合惩戒的备忘录")
            introduction = next(line for line in lines if "就针对公共资源交易领域严重失信主体开展联合惩戒工作达成如下一致意见" in line)
            paragraph = next(line for line in lines if line.startswith("各部门和单位对公共资源交易领域") and "应及时停止实施惩戒措施" in line)
            selected = [title, introduction, paragraph]
            record["selection"] = "formal_source_verbatim_unnumbered_excerpt_not_full_document"
            record["selected_original_ranges"] = [{"start": text.index(s), "end": text.index(s)+len(s)} for s in selected]
            record["coordinate_space"] = "excerpt; original ranges retained separately"
            rows.append((title+".txt", "\n\n".join(selected), str(path)+"#verbatim_unnumbered_excerpt", record))
    return rows


async def main():
    records = []
    for filename, text, identity, source in actual_samples():
        extractor = ParagraphExtractor()
        result = await extractor.extract_text(text, filename, identity)
        row = {**source, "filename": filename, "status": result["status"], "counts": result["stats"]["counts"],
               "source_signals": result["parse_result"]["source_signals"],
               "source_unchanged": sha256(Path(source["original_path"]).read_bytes()).hexdigest() == source["original_sha256"]}
        save_json(result, OUTPUT/"实际数据离线"/(filename+".json"))
        records.append(row)
    summary = {"samples": records, "passed": all(r["status"] == "structure_only" and r["source_unchanged"] for r in records),
               "full_corpus_acceptance": False, "model_requests": 0, "database_writes": 0}
    save_json(summary, OUTPUT/"实际数据离线/实际样本汇总.json")
    print(summary)
    return 0 if summary["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
