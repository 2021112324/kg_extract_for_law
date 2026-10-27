"""Small explicit real-model test; credentials are read in memory, never saved."""
import ast
import asyncio
from dataclasses import replace
import os
import json
import logging
from pathlib import Path
import sys

REPO = next(p for p in Path(__file__).resolve().parents if (p / "app/infrastructure").is_dir())
sys.path.insert(0, str(REPO))
from app.infrastructure.information_extraction.v2.rule import RuleConfig, RuleExtractor
from app.infrastructure.information_extraction.v2.rule.io_utils import save_json, redact

OUTPUT = REPO / "app/infrastructure/information_extraction/v2/task/测试报告/rule_测试记录/真实模型"


def project_model_config():
    """Read only literal/env expressions, without importing the mapping runner."""
    def value(node):
        if isinstance(node, ast.BoolOp) and isinstance(node.op, ast.Or):
            for arg in node.values:
                result = value(arg)
                if result:
                    return result
            return ""
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr in {"getenv", "get"}:
            args = [ast.literal_eval(a) for a in node.args]
            return os.getenv(*args)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "local_credential":
            name = ast.literal_eval(node.args[0])
            path = REPO / "app/infrastructure/risk_map/credentials.local.json"
            return json.loads(path.read_text(encoding="utf-8")).get(name, "") if path.exists() else ""
        return ast.literal_eval(node)
    settings = {}
    path = REPO / "app/infrastructure/risk_map/run_mapping.py"
    for node in ast.parse(path.read_text(encoding="utf-8-sig")).body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id in {"MODEL_NAME", "MODEL_API_URL", "API_KEY"}:
                    settings[target.id] = value(node.value)
    return RuleConfig(model_name=os.getenv("RULE_MODEL") or settings.get("MODEL_NAME", ""),
                      api_url=os.getenv("RULE_API_URL") or settings.get("MODEL_API_URL", ""),
                      api_key=os.getenv("RULE_API_KEY") or settings.get("API_KEY", ""), retry_delay=1)


async def main():
    config = project_model_config()
    output = OUTPUT / os.getenv("RULE_TEST_OUTPUT_SUBDIR", "本轮")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    samples = {
        "父子独立": "示例系统管理规则\n1.1 企业应建立合规管理制度。\n1.1.1 企业应每年评估制度执行情况。",
        "全部条件整组": "示例系统管理规则\n1.1 只有同时满足以下全部条件，企业才可以提交申请：\n1.1.1 已完成登记。\n1.1.2 已提交申请材料。",
        "继承前提": "示例系统管理规则\n1.1 发生下列任一情形时，企业应启动应急处置：\n1.1.1 核心业务系统无法正常运行。\n1.1.2 关键业务数据遭到破坏。",
        "定义与数量": "示例系统管理规则\n1.1 本规则所称业务记录是指办理业务形成的原始资料。\n1.2 企业应在收到通知之日起5个工作日内提交报告。",
        "内部引用": "示例系统管理规则\n1.1 企业应保存业务记录。\n1.2 企业应按本规则1.1的要求保存原始凭证。",
        "例外条件": "示例系统管理规则\n1.1 除依法获准豁免的情形外，企业应在发生下列任一情形时提交报告：\n1.1.1 主要业务系统停止运行。\n1.1.2 重要业务数据丢失。",
    }
    if os.getenv("RULE_TEST_CASES") is not None:
        selected = os.environ["RULE_TEST_CASES"].split("|")
        samples = {name: source for name, source in samples.items() if name in selected}
    summary = {"model": config.model_name, "samples": []}
    for name, source in samples.items():
        extractor = RuleExtractor(config)
        extractor.audit_path = output / (name + "_attempts.jsonl")
        result = await extractor.extract_text(source, filename=name + ".txt", run_llm=True)
        save_json(result, output / (name + ".json"))
        row = {"name": name, "status": result["status"], "formal_graph_eligible": result["formal_graph_eligible"],
               "article_numbers": [n["properties"]["条"] for n in result["graph"]["nodes"] if n["node_type"] == "法条"],
               "stats": result["stats"]}
        summary["samples"].append(row)
        print(name, result["status"], row["article_numbers"])
    actual_root = Path(r"D:\CogmAIT\8.1项目\8.1数据\合规风险指标映射数据\v1_923\清洗名称后的纯文本数据\其他格式")
    actual_files = ["上海证券交易所上市公司自律监管指引第1号——规范运作.txt", "电子商业汇票系统危机处置预案.txt"]
    if os.getenv("RULE_TEST_ACTUAL_FILES") is not None:
        actual_files = [name for name in os.environ["RULE_TEST_ACTUAL_FILES"].split("|") if name]
    for filename in actual_files:
        source = actual_root / filename
        extractor = RuleExtractor(config)
        result = await extractor.extract_file(source, output / "实际条款抽样", run_llm=True, max_blocks=1)
        summary["samples"].append({"name": filename, "status": result["status"], "expected_status": "partial_test",
            "formal_graph_eligible": result["formal_graph_eligible"], "stats": result["stats"],
            "article_numbers": [n["properties"]["条"] for n in result["graph"]["nodes"] if n["node_type"] == "法条"]})
        print(filename, result["status"])
    summary["all_expected_passed"] = all(row["status"] == row.get("expected_status", "success") for row in summary["samples"])
    save_json(summary, output / "真实模型测试汇总.json")
    if not summary["all_expected_passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    asyncio.run(main())
