import argparse
import asyncio
import json
import logging
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[5]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.infrastructure.information_extraction.law_extract.clause_extract import ClauseExtractor  # noqa: E402


DEFAULT_INPUT_DIR = Path(
    r"D:\CogmAIT\8.1项目\8.1数据\爬取的数据\风险法规\第二版\分类\中文抽取所用数据\法文分类\法律法规条款\test"
)
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data" / "result"
DEFAULT_REPORT_DIR = DEFAULT_OUTPUT_DIR / "report"

VALID_RISK_TYPES = {
    "产品法律风险",
    "供应链合规风险",
    "劳动用工法律合规风险",
    "企业关联方合规风险",
    "企业国际化经营合规风险",
    "企业信用风险",
}
VALID_FUNCTION_TYPES = {"禁止", "必要", "可选"}
VALID_QUANT_FEATURES = {"定性", "定量"}
QUANT_KEYS = {"原文件", "量化值类型", "最小值", "最大值", "单位", "约束关系"}


def safe_filename(name: str) -> str:
    cleaned = re.sub(r'[\\/:*?"<>|]+', "_", name).strip()
    return cleaned or "未命名文件"


def discover_text_files(input_dir: Path) -> list[Path]:
    if input_dir.is_file():
        return [input_dir]
    files: list[Path] = []
    for suffix in ("*.txt", "*.md"):
        files.extend(input_dir.rglob(suffix))
    return sorted(files, key=lambda item: str(item))


def read_text(path: Path) -> str:
    for encoding in ("utf-8", "utf-8-sig", "gb18030"):
        try:
            return path.read_text(encoding=encoding)
        except UnicodeDecodeError:
            continue
    return path.read_text(encoding="utf-8", errors="ignore")


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def save_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def list_nodes(graph: dict[str, Any]) -> list[dict[str, Any]]:
    nodes = graph.get("nodes", [])
    return nodes if isinstance(nodes, list) else []


def list_edges(graph: dict[str, Any]) -> list[dict[str, Any]]:
    edges = graph.get("edges", [])
    return edges if isinstance(edges, list) else []


def is_structured_quant_condition(value: Any) -> bool:
    if value is None or value == "":
        return False
    if isinstance(value, dict):
        return bool(QUANT_KEYS.issubset(set(value.keys())) and str(value.get("原文件") or "").strip())
    if isinstance(value, list):
        return bool(value) and all(is_structured_quant_condition(item) for item in value)
    return False


def validate_graph_schema(graph: dict[str, Any], source_clauses: list[dict[str, Any]]) -> list[str]:
    issues: list[str] = []
    nodes = list_nodes(graph)
    edges = list_edges(graph)
    node_ids = {node.get("node_id") for node in nodes if node.get("node_id")}

    if not nodes:
        issues.append("图谱节点为空")
    if not any(node.get("node_type") == "法规文件" for node in nodes):
        issues.append("缺少法规文件节点")

    for index, node in enumerate(nodes, start=1):
        node_id = node.get("node_id")
        node_type = node.get("node_type")
        props = node.get("properties") or {}
        if not node_id:
            issues.append(f"第 {index} 个节点缺少 node_id")
        if not node.get("node_name"):
            issues.append(f"节点 {node_id or index} 缺少 node_name")
        if not node_type:
            issues.append(f"节点 {node_id or index} 缺少 node_type")
        if not isinstance(props, dict):
            issues.append(f"节点 {node_id or index} properties 不是对象")
            continue

        if "适用行业" in props or "应用领域" in props:
            issues.append(f"节点 {node_id or node.get('node_name')} 仍存在旧字段：适用行业/应用领域")

        if node_type in {"法规文件", "法条", "条款单元"}:
            if not isinstance(props.get("合规域", []), list):
                issues.append(f"节点 {node.get('node_name')} 合规域不是字符串数组")
            economic_industry = props.get("经济行业")
            if not isinstance(economic_industry, list) or not economic_industry:
                issues.append(f"节点 {node.get('node_name')} 经济行业不是非空字符串数组")
            elif "通用" in economic_industry and len(economic_industry) > 1:
                issues.append(f"节点 {node.get('node_name')} 经济行业中“通用”和具体行业并列")

        if node_type == "法规文件":
            risk_types = props.get("合规风险类型")
            if not isinstance(risk_types, list):
                issues.append(f"法规文件节点 {node.get('node_name')} 合规风险类型不是字符串数组")
            else:
                invalid = [item for item in risk_types if item not in VALID_RISK_TYPES]
                if invalid:
                    issues.append(f"法规文件节点 {node.get('node_name')} 合规风险类型存在非法值：{invalid}")

        if node_type == "条款单元":
            function_type = props.get("功能类型")
            if function_type and function_type not in VALID_FUNCTION_TYPES:
                issues.append(f"条款单元 {node.get('node_name')} 功能类型非法：{function_type}")

        if node_type == "法条" and ("量化特征" in props or "量化条件" in props):
            issues.append(f"法条节点 {node.get('node_name')} 不应包含量化特征/量化条件")

        if node_type == "条款单元":
            quant_feature = props.get("量化特征")
            quant_condition = props.get("量化条件")
            has_quant_condition = is_structured_quant_condition(quant_condition)
            if quant_feature not in VALID_QUANT_FEATURES:
                issues.append(f"节点 {node.get('node_name')} 量化特征非法：{quant_feature}")
            if quant_feature == "定量" and not has_quant_condition:
                issues.append(f"节点 {node.get('node_name')} 量化特征为定量但量化条件不是结构化非空对象")
            if quant_feature == "定性" and has_quant_condition:
                issues.append(f"节点 {node.get('node_name')} 量化条件非空但量化特征为定性")
            if quant_condition not in (None, "") and not has_quant_condition:
                issues.append(f"节点 {node.get('node_name')} 量化条件不是规定结构：{quant_condition}")

    for index, edge in enumerate(edges, start=1):
        source_id = edge.get("source_id")
        target_id = edge.get("target_id")
        if not source_id or source_id not in node_ids:
            issues.append(f"第 {index} 条关系 source_id 不存在于节点集合：{source_id}")
        if not target_id or target_id not in node_ids:
            issues.append(f"第 {index} 条关系 target_id 不存在于节点集合：{target_id}")
        if not edge.get("relation_type"):
            issues.append(f"第 {index} 条关系缺少 relation_type")

    source_clause_numbers = [item.get("条款编号") for item in source_clauses if item.get("条款编号")]
    graph_clause_numbers = set()
    for node in nodes:
        if node.get("node_type") != "法条":
            continue
        props = node.get("properties") or {}
        graph_clause_numbers.add(props.get("条") or node.get("node_name"))
    missing = [number for number in source_clause_numbers if number not in graph_clause_numbers]
    if missing:
        issues.append(f"存在未覆盖法条编号：{missing[:20]}{'...' if len(missing) > 20 else ''}")
    if source_clause_numbers and len(graph_clause_numbers) < len(source_clause_numbers):
        issues.append(f"法条覆盖数量不足：原文 {len(source_clause_numbers)} 条，图谱 {len(graph_clause_numbers)} 条")

    return issues


async def split_source_clauses(extractor: ClauseExtractor, source_text: str) -> list[dict[str, Any]]:
    try:
        split_result = await extractor.split_clause(source_text)
    except Exception as exc:
        return [{"条款编号": "", "条款内容": f"split_clause 失败：{exc}"}]
    clauses = split_result.get("clauses", [])
    return clauses if isinstance(clauses, list) else []


async def extract_one_file(extractor: ClauseExtractor, source_path: Path, output_path: Path, skip_existing: bool) -> dict[str, Any]:
    if skip_existing and output_path.exists():
        return {"status": "skipped_existing", "source": str(source_path), "output": str(output_path)}

    text = read_text(source_path)
    graph = await extractor.extract_clauses(source_path.stem, text)
    save_json(output_path, graph)
    return {
        "status": "success",
        "source": str(source_path),
        "output": str(output_path),
        "nodes": len(list_nodes(graph)),
        "edges": len(list_edges(graph)),
    }


async def run_extract(args: argparse.Namespace) -> list[dict[str, Any]]:
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    files = discover_text_files(input_dir)
    if args.limit > 0:
        files = files[: args.limit]
    if not files:
        raise FileNotFoundError(f"未发现 txt/md 文件: {input_dir}")

    extractor = ClauseExtractor(max_concurrent=args.max_concurrent)
    extractor.lenient_mode = args.lenient_mode
    results: list[dict[str, Any]] = []
    for index, source_path in enumerate(files, start=1):
        output_path = output_dir / f"{safe_filename(source_path.stem)}.json"
        logging.info("开始抽取 %s/%s: %s", index, len(files), source_path)
        try:
            item = await extract_one_file(extractor, source_path, output_path, args.skip_existing)
        except Exception as exc:
            item = {"status": "failed", "source": str(source_path), "error": str(exc)}
            logging.exception("抽取失败: %s", source_path)
        results.append(item)
        save_json(output_dir / "task3_extract_summary.json", {"items": results})
    return results


async def run_validate(args: argparse.Namespace) -> dict[str, Any]:
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    source_files = discover_text_files(input_dir)
    if args.limit > 0:
        source_files = source_files[: args.limit]

    extractor = ClauseExtractor(max_concurrent=1)
    validation_items: list[dict[str, Any]] = []
    for source_path in source_files:
        output_path = output_dir / f"{safe_filename(source_path.stem)}.json"
        item: dict[str, Any] = {
            "source": str(source_path),
            "output": str(output_path),
            "status": "not_checked",
            "issues": [],
        }
        if not output_path.exists():
            item["status"] = "missing_result"
            item["issues"].append("结果文件不存在")
            validation_items.append(item)
            continue
        try:
            graph = load_json(output_path)
            source_text = read_text(source_path)
            source_clauses = await split_source_clauses(extractor, source_text)
            issues = validate_graph_schema(graph, source_clauses)
            item.update(
                {
                    "status": "passed" if not issues else "failed",
                    "issues": issues,
                    "source_clause_count": len([c for c in source_clauses if c.get("条款编号")]),
                    "node_count": len(list_nodes(graph)),
                    "edge_count": len(list_edges(graph)),
                }
            )
        except Exception as exc:
            item["status"] = "failed"
            item["issues"].append(f"校验异常：{exc}")
        validation_items.append(item)

    passed = sum(1 for item in validation_items if item["status"] == "passed")
    failed = sum(1 for item in validation_items if item["status"] == "failed")
    missing = sum(1 for item in validation_items if item["status"] == "missing_result")
    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "input_dir": str(input_dir),
        "output_dir": str(output_dir),
        "summary": {
            "total": len(validation_items),
            "passed": passed,
            "failed": failed,
            "missing_result": missing,
        },
        "items": validation_items,
    }
    save_json(report_dir / "task3_validation_report.json", report)
    write_markdown_report(report_dir / "task3_validation_report.md", report)
    return report


def write_markdown_report(path: Path, report: dict[str, Any]) -> None:
    lines = [
        "# 法规知识图谱抽取结果校验报告",
        "",
        f"- 生成时间：{report['generated_at']}",
        f"- 输入目录：{report['input_dir']}",
        f"- 结果目录：{report['output_dir']}",
        f"- 总数：{report['summary']['total']}",
        f"- 通过：{report['summary']['passed']}",
        f"- 失败：{report['summary']['failed']}",
        f"- 缺少结果：{report['summary']['missing_result']}",
        "",
        "## 校验口径",
        "",
        "- 结构完整性：节点、关系字段完整，关系端点能在节点集合中找到。",
        "- 原文重要内容覆盖：以条款切分结果为基准，校验原文法条编号是否均存在对应法条节点。",
        "- 专家反馈规则：校验 R1-R5 涉及的合规风险类型、合规域/经济行业、功能类型、量化特征和量化条件。",
        "",
        "## 明细",
        "",
    ]
    for item in report["items"]:
        lines.append(f"### {Path(item['source']).stem}")
        lines.append("")
        lines.append(f"- 状态：{item['status']}")
        lines.append(f"- 源文件：{item['source']}")
        lines.append(f"- 结果文件：{item['output']}")
        if "source_clause_count" in item:
            lines.append(f"- 原文法条数：{item['source_clause_count']}")
            lines.append(f"- 节点数：{item['node_count']}")
            lines.append(f"- 关系数：{item['edge_count']}")
        if item.get("issues"):
            lines.append("- 问题：")
            for issue in item["issues"]:
                lines.append(f"  - {issue}")
        else:
            lines.append("- 问题：无")
        lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="执行 law_extract task3 抽取与校验")
    parser.add_argument("--input-dir", default=str(DEFAULT_INPUT_DIR), help="待抽取 txt/md 目录")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="图谱 JSON 输出目录")
    parser.add_argument("--report-dir", default=str(DEFAULT_REPORT_DIR), help="校验报告输出目录")
    parser.add_argument("--limit", type=int, default=0, help="限制处理文件数量，0 表示全部")
    parser.add_argument("--max-concurrent", type=int, default=1, help="ClauseExtractor 内部并发数")
    parser.add_argument("--skip-existing", action="store_true", help="结果文件已存在时跳过抽取")
    parser.add_argument("--validate-only", action="store_true", help="只校验已有结果，不调用大模型抽取")
    parser.add_argument("--no-validate", action="store_true", help="只抽取，不校验")
    parser.add_argument("--lenient-mode", action="store_true", help="允许部分条款抽取失败后仍生成图谱")
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if not args.validate_only:
        extract_summary = await run_extract(args)
        logging.info("抽取完成：success=%s failed=%s skipped=%s",
                     sum(1 for item in extract_summary if item["status"] == "success"),
                     sum(1 for item in extract_summary if item["status"] == "failed"),
                     sum(1 for item in extract_summary if item["status"] == "skipped_existing"))
    if not args.no_validate:
        report = await run_validate(args)
        logging.info("校验完成：%s", report["summary"])


if __name__ == "__main__":
    asyncio.run(main())
