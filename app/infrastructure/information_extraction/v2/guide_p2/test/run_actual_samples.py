"""Source-level verification only: no model and no database calls."""
import asyncio
from pathlib import Path
import sys

REPO = next(p for p in Path(__file__).resolve().parents if (p / "app/infrastructure").is_dir())
sys.path.insert(0, str(REPO))
from app.infrastructure.information_extraction.v2.guide_p2 import GuideP2Extractor
from app.infrastructure.information_extraction.v2.guide_p2.io_utils import save_json

ROOT = Path(r"D:\CogmAIT\8.1项目\8.1数据\合规风险指标映射数据\v1_923\清洗名称后的纯文本数据\其他格式")
OUTPUT = REPO / "app/infrastructure/information_extraction/v2/task/测试报告/guide_p2_测试记录/真实文件离线"
FILES = ("境外投资敏感行业目录.md", "第一批实施强制性产品认证的产品目录.txt")


async def main():
    rows = []
    for filename in FILES:
        source = ROOT/filename
        before = source.read_bytes()
        text = before.decode("utf-8-sig")
        result = await GuideP2Extractor().extract_file(source, OUTPUT)
        p, plan = result["parse_result"], result["plan"]
        assert result["status"] == "structure_only", result["stats"]
        assert not result["graph"]["nodes"] and not result["stats"]["counts"].get("requests", 0)
        assert len(p["line_dispositions"]) == p["source_line_count"]
        assert len(plan["coverage"]) == len(p["nodes"])
        owners = {r["node_id"] for r in plan["coverage"]}
        assert len(owners) == len(p["nodes"])
        for node in p["nodes"]:
            for line in node["own_lines"]:
                assert text[line["start"]:line["end"]].rstrip("\r\n") == line["raw"]
        assert source.read_bytes() == before
        rows.append({"filename": filename, "title": p["title"], "source_sha256": p["source_sha256"],
                     "source_lines": p["source_line_count"], "counts": plan["counts"], "source_offsets_verified": True,
                     "source_unchanged": True, "all_nodes_have_targets": True,
                     "entries": [{"entry_id": u["entry_id"], "target": u["target"], "context": u["context"], "source_ranges": u["source_ranges"]} for u in plan["units"]],
                     "excluded": p["excluded"], "semantic_acceptance_claimed": False})
        print(filename, plan["counts"])
    save_json(rows, OUTPUT/"真实文件离线校验.json")


if __name__ == "__main__":
    asyncio.run(main())
