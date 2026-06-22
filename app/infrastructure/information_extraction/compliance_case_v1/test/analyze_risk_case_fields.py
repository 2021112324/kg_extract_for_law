"""Field-level checks for exported risk compliance case Neo4j data."""

from __future__ import annotations

import json
import re
from collections import Counter
from pathlib import Path


NODE_PATH = Path(r"D:\CogmAIT\8.1项目\8.1数据\成果\部分知识库\6月11日\neo4j数据\案例\风险合规案例\node.json")
SRC_DIR = Path(r"F:\企业大脑知识库系统\8.1项目\抽取代码\kg_extract_for_law\app\infrastructure\information_extraction\compliance_case_v1\data\合规风险案例")
OUT_PATH = Path(r"D:\CogmAIT\8.1项目\8.1数据\成果\部分知识库\6月11日\neo4j数据\案例\风险合规案例\风险合规案例_字段专项校验.json")


def main() -> None:
    raw = json.loads(NODE_PATH.read_text(encoding="utf-8"))
    nodes = [item.get("n", item) for item in raw]
    case_nodes = [node for node in nodes if (node.get("properties") or {}).get("label") == "案例"]

    stats = Counter()
    examples = []
    prop_keys = Counter()
    for node in case_nodes:
        props = node.get("properties") or {}
        prop_keys.update(props.keys())
        filename = props.get("filename")
        if isinstance(filename, list):
            filename = filename[0] if filename else ""
        filename = str(filename or "")
        full_text = str(props.get("原文全文") or "").strip()
        material = _read_enterprise_material(filename)

        if not props.get("合规领域"):
            stats["missing_source_compliance_domain"] += 1
        if not props.get("关键词"):
            stats["missing_keywords"] += 1
        if not (props.get("案例名称") or props.get("name")):
            stats["missing_case_title"] += 1
        if not full_text:
            stats["missing_full_text"] += 1
        if material and full_text != material:
            stats["full_text_not_enterprise_material"] += 1
            if len(examples) < 10:
                examples.append(
                    {
                        "filename": filename,
                        "full_text_prefix": full_text[:160],
                        "enterprise_material_prefix": material[:160],
                    }
                )

    detail = {
        "case_node_count": len(case_nodes),
        "stats": dict(stats),
        "case_property_keys": dict(prop_keys),
        "full_text_not_enterprise_material_examples": examples,
    }
    OUT_PATH.write_text(json.dumps(detail, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(detail, ensure_ascii=False, indent=2))


def _read_enterprise_material(filename: str) -> str:
    if not filename:
        return ""
    path = SRC_DIR / filename
    if not path.exists():
        return ""
    text = path.read_text(encoding="utf-8")
    match = re.search(r"企业材料\s*(.*?)\s*结论", text, re.S)
    return match.group(1).strip() if match else ""


if __name__ == "__main__":
    main()
