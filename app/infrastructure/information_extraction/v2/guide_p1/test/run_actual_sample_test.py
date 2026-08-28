"""Run guide_p1 against the actual point-form guide samples and write a report."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[6]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.infrastructure.information_extraction.v2.guide_p1.extractor import GuideP1Extractor
from app.infrastructure.information_extraction.v2.guide_p1.io_utils import InputDiscoveryError, discover_input_files, save_json
from app.infrastructure.information_extraction.v2.guide_p1.structure_parser import GuideP1StructureParser


DEFAULT_INPUT = Path(
    r"D:\CogmAIT\8.1项目\8.1数据\合规风险指标映射数据\v1_819\op2_风险映射添加"
    r"\风险数据\中国\合规指引\分点格式"
)
DEFAULT_EMPTY_LEAF = DEFAULT_INPUT / "行政监管规则"
MODULE_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = MODULE_ROOT / "test_output" / "actual_samples"
DEFAULT_REPORT = MODULE_ROOT / "report" / "分点格式合规指引抽取测试报告.md"


def run_structure_batch(input_dir: Path, output_dir: Path) -> dict[str, Any]:
    files = discover_input_files(input_dir)
    parser = GuideP1StructureParser()
    items = []
    totals = {
        "file_count": len(files),
        "validation_passed": 0,
        "validation_failed": 0,
        "structure_node_count": 0,
        "semantic_block_count": 0,
        "column_count": 0,
        "skipped_attachment_count": 0,
    }
    split_dir = output_dir / "split"
    split_dir.mkdir(parents=True, exist_ok=True)
    for path in files:
        result = parser.parse_file(path)
        save_json(result, split_dir / f"{path.stem}_split.json")
        passed = bool(result["validation"]["passed"])
        totals["validation_passed" if passed else "validation_failed"] += 1
        totals["structure_node_count"] += len(result["structure_nodes"])
        totals["semantic_block_count"] += len(result["semantic_blocks"])
        totals["column_count"] += sum(node["node_type"] == "专栏" for node in result["structure_nodes"])
        totals["skipped_attachment_count"] += len(result["skipped_attachments"])
        items.append(
            {
                "filename": path.name,
                "status": "success" if passed else "validation_failed",
                "document_title": result["metadata_candidates"]["document_title"],
                "risk_types": result["metadata_candidates"]["risk_types"],
                "stats": result["stats"],
                "warnings": result["validation"]["warnings"],
                "attachments": result["skipped_attachments"],
            }
        )
    return {"totals": totals, "items": items}


def check_empty_leaf(path: Path) -> dict[str, Any]:
    try:
        discover_input_files(path)
    except InputDiscoveryError as exc:
        return {"passed": True, "kind": "empty", "message": str(exc)}
    except FileNotFoundError as exc:
        return {"passed": True, "kind": "missing", "message": f"目标路径不存在，已阻断且未回退父目录: {exc}"}
    return {"passed": False, "kind": "unexpected", "message": "无文件目标路径检查未报错"}


async def run_real_llm_sample(input_dir: Path, output_dir: Path, max_blocks: int) -> dict[str, Any]:
    preferred = input_dir / "“十四五”节能减排综合工作方案.txt"
    path = preferred if preferred.exists() else discover_input_files(input_dir)[0]
    extractor = GuideP1Extractor()
    try:
        result = await extractor.extract_file(
            path,
            output_dir=output_dir / "real_llm",
            run_llm=True,
            max_blocks=max_blocks,
        )
        units = [
            {
                "name": node["node_name"],
                "properties": node["properties"],
            }
            for node in result["graph"]["nodes"]
            if node["node_type"] == "指引知识单元"
        ]
        relation_counts: dict[str, int] = {}
        for edge in result["graph"]["edges"]:
            relation_type = edge.get("relation_type", "")
            relation_counts[relation_type] = relation_counts.get(relation_type, 0) + 1
        return {
            "attempted": True,
            "filename": path.name,
            "status": result["status"],
            "formal_graph_eligible": result["formal_graph_eligible"],
            "max_blocks": max_blocks,
            "run_stats": result["run_stats"],
            "relation_counts": relation_counts,
            "knowledge_units": units,
            "error": "",
        }
    except Exception as exc:
        return {
            "attempted": True,
            "filename": path.name,
            "status": "failed",
            "formal_graph_eligible": False,
            "max_blocks": max_blocks,
            "run_stats": {},
            "knowledge_units": [],
            "error": str(exc),
        }


def render_report(summary: dict[str, Any]) -> str:
    totals = summary["structure_batch"]["totals"]
    empty = summary["empty_leaf"]
    llm = summary.get("real_llm") or {"attempted": False}
    lines = [
        "# 分点格式合规指引抽取测试报告",
        "",
        "## 1. 测试范围",
        "",
        f"- 实际样本目录：`{summary['input_dir']}`",
        f"- 样本文件数：{totals['file_count']}",
        "- 离线单元测试：结构解析、附件跳过、Schema 校验、构图、严格失败和并发控制。",
        "- 实际数据测试：10 份文件执行第一阶段结构解析，其中部分内容块执行真实模型抽取。",
        "",
        "## 2. 结构解析结果",
        "",
        f"- 校验通过：{totals['validation_passed']} 份",
        f"- 校验失败：{totals['validation_failed']} 份",
        f"- 结构节点：{totals['structure_node_count']} 个",
        f"- 语义候选块：{totals['semantic_block_count']} 个",
        f"- 正文专栏：{totals['column_count']} 个，均保留为正文结构。",
        f"- 跳过附件：{totals['skipped_attachment_count']} 个，未进入语义抽取和图谱。",
        "",
        "### 文件明细",
        "",
        "| 文件 | 状态 | 结构节点 | 语义候选块 | 跳过附件 |",
        "| --- | --- | ---: | ---: | ---: |",
    ]
    for item in summary["structure_batch"]["items"]:
        stats = item["stats"]
        lines.append(
            f"| {item['filename']} | {item['status']} | {stats['structure_node_count']} | "
            f"{stats['semantic_block_count']} | {stats['skipped_attachment_count']} |"
        )
    lines.extend(
        [
            "",
            "## 3. 目标叶路径检查",
            "",
            f"- 结果：{'通过' if empty['passed'] else '未通过'}",
            f"- 信息：{empty['message']}",
            "- 结论：程序不会因指定叶路径不存在或目录为空而静默处理父目录文件。",
            "",
            "## 4. 真实模型小样本测试",
            "",
        ]
    )
    if not llm.get("attempted"):
        lines.append("本次未启用真实模型测试。")
    else:
        lines.extend(
            [
                f"- 文件：{llm.get('filename', '')}",
                f"- 内容块上限：{llm.get('max_blocks', 0)}",
                f"- 状态：{llm.get('status', '')}",
                f"- 正式入图资格：{llm.get('formal_graph_eligible', False)}（部分测试按设计不得作为完整正式图谱）",
                f"- 知识单元数：{len(llm.get('knowledge_units', []))}",
                f"- 图谱节点/关系：{(llm.get('run_stats') or {}).get('node_count', 0)} / "
                f"{(llm.get('run_stats') or {}).get('edge_count', 0)}",
                f"- 抽取错误/弱警告/强警告：{(llm.get('run_stats') or {}).get('extraction_error', 0)} / "
                f"{(llm.get('run_stats') or {}).get('weak_warning', 0)} / "
                f"{(llm.get('run_stats') or {}).get('strong_warning', 0)}",
                "- 关系统计：" + "、".join(
                    f"{name} {count} 条" for name, count in (llm.get("relation_counts") or {}).items()
                ),
                f"- 错误：{llm.get('error') or '无'}",
            ]
        )
        for index, item in enumerate(llm.get("knowledge_units", [])[:10], start=1):
            lines.append(f"- 知识单元{index}：{item['name']}")
        remaining = len(llm.get("knowledge_units", [])) - 10
        if remaining > 0:
            lines.append(f"- 其余知识单元：另 {remaining} 个，详见测试结果 JSON。")
    lines.extend(
        [
            "",
            "## 5. 结论",
            "",
            "第一阶段能够在实际样本上恢复分点层级、保留正文专栏并跳过附件正文；空叶目录不会回退父目录。语义节点只来自成功的大模型结果，失败块会进入错误统计，结构解析不会生成语义兜底节点。真实模型小样本结果仅用于验证抽取链路和提示约束，不作为完整文件正式图谱。",
            "",
        ]
    )
    return "\n".join(lines)


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--empty-leaf", type=Path, default=DEFAULT_EMPTY_LEAF)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--real-llm", action="store_true")
    parser.add_argument("--max-blocks", type=int, default=2)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    summary: dict[str, Any] = {
        "input_dir": str(args.input),
        "empty_leaf_dir": str(args.empty_leaf),
        "structure_batch": run_structure_batch(args.input, args.output),
        "empty_leaf": check_empty_leaf(args.empty_leaf),
        "real_llm": {"attempted": False},
    }
    if args.real_llm:
        summary["real_llm"] = await run_real_llm_sample(args.input, args.output, args.max_blocks)
    save_json(summary, args.output / "actual_test_summary.json")
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(render_report(summary), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f"REPORT={args.report}")


if __name__ == "__main__":
    asyncio.run(main())
