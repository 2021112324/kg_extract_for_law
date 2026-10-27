"""Read-only source checks using the actual rule parser and carrier planner."""
import asyncio
import hashlib
from pathlib import Path
import sys

REPO = next(p for p in Path(__file__).resolve().parents if (p / "app/infrastructure").is_dir())
sys.path.insert(0, str(REPO))
from app.infrastructure.information_extraction.v2.rule import RuleExtractor
from app.infrastructure.information_extraction.v2.rule.io_utils import save_json

INPUT = Path(r"D:\CogmAIT\8.1项目\8.1数据\合规风险指标映射数据\v1_923\清洗名称后的纯文本数据\其他格式")
OUTPUT = REPO / "app/infrastructure/information_extraction/v2/task/测试报告/rule_测试记录"


async def main():
    results = []
    for source in sorted(INPUT.iterdir()):
        if "规范运作" not in source.name and "危机处置预案" not in source.name:
            continue
        before = source.read_bytes()
        result = await RuleExtractor().extract_file(source, OUTPUT / "真实文件离线", run_llm=False)
        parsed, plan = result["parse_result"], result["plan"]
        assert parsed["validation"]["passed"], parsed["diagnostics"]
        assert not plan["errors"], plan["errors"]
        assert len(parsed["line_dispositions"]) == parsed["source_line_count"]
        source_text = before.decode("utf-8-sig")
        for node in parsed["nodes"]:
            for line in node["own_lines"]:
                assert source_text[line["start"]:line["end"]].rstrip("\r\n") == line["raw"]
        assert all(c["kind"] != "unresolved" for c in plan["coverage"])
        assert source.read_bytes() == before
        assert not result["graph"]["nodes"] and not result["stats"]["counts"].get("requests")
        results.append({"filename": source.name, "sha256": hashlib.sha256(before).hexdigest(),
            "title": parsed["title"], "source_lines": parsed["source_line_count"], "nodes": len(parsed["nodes"]),
            "counts": plan["counts"], "pending_boundaries": len(plan["pending_boundaries"]),
            "source_unchanged": True, "source_offsets_verified": True, "no_unassigned_substantive_nodes": True,
            "body_fingerprint": parsed["body_fingerprint"], "semantic_quality_claimed": False})
    assert len(results) == 3
    assert results[0]["body_fingerprint"] == results[1]["body_fingerprint"]
    save_json({"passed": True, "files": results, "model_called": False, "database_connected": False}, OUTPUT / "真实文件离线校验.json")
    print(results)


if __name__ == "__main__":
    asyncio.run(main())
