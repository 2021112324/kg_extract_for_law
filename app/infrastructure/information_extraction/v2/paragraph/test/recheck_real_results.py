"""Revalidate recorded real responses against current code; no new model calls."""
import argparse
import json
from pathlib import Path
import sys

REPO = next(p for p in Path(__file__).resolve().parents if (p/"app/infrastructure").is_dir())
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from app.infrastructure.information_extraction.v2.paragraph.extractor import RUNTIME_IDENTITY
from app.infrastructure.information_extraction.v2.paragraph.graph_builder import ParagraphGraphBuilder
from app.infrastructure.information_extraction.v2.paragraph.io_utils import save_json
from app.infrastructure.information_extraction.v2.paragraph.validator import validate_file, validate_content, file_properties
from app.infrastructure.information_extraction.v2.paragraph.prompt.schema import RISK_TYPE_VALUES


def recheck(folder):
    rows = []
    for path in sorted(Path(folder).glob("*_result.json")):
        result = json.loads(path.read_text(encoding="utf-8"))
        audit = path.with_name(path.name.replace("_result.json", "_attempts.jsonl"))
        successful = {}
        with audit.open(encoding="utf-8") as stream:
            for line in stream:
                item = json.loads(line)
                if item["status"] == "success":
                    successful[item["task_id"]] = item["raw_response"]
        row = {"filename": result["parse_result"]["filename"], "recorded_status": result["status"],
               "passed": False, "new_model_calls": 0}
        try:
            parsed, plan = result["parse_result"], result["plan"]
            file_result = validate_file(successful["file_info"], parsed, plan["file_payload"])
            units = {u["target_id"]: u for u in plan["units"]}
            validated = []
            for task in result["results"]:
                if task["status"] not in {"success", "reasonable_empty"}:
                    raise ValueError("Recorded target was not complete: " + task["target_id"])
                value = validate_content(successful[task["target_id"]], units[task["target_id"]], file_properties(file_result),
                    [r for r in RISK_TYPE_VALUES if r in parsed["raw_text"]])
                value.update(status=task["status"], paragraph_id=task["paragraph_id"])
                validated.append(value)
            graph = ParagraphGraphBuilder().build(parsed, file_result, validated)
            if graph != result["graph"]:
                raise ValueError("Current graph differs from the recorded graph")
            row.update(passed=True, nodes=len(graph["nodes"]), relationships=len(graph["edges"]),
                       knowledge=sum(n["node_type"] == "段落知识单元" for n in graph["nodes"]))
        except Exception as exc:
            row.update(error_type=type(exc).__name__, error=str(exc))
        rows.append(row)
    record = {"kind": "offline_revalidation_of_real_model_responses", "samples": rows,
              "all_passed": bool(rows) and all(r["passed"] for r in rows), "runtime_identity": RUNTIME_IDENTITY,
              "new_model_calls": 0, "database_writes": 0}
    save_json(record, Path(folder)/"最终代码重校验.json")
    print(json.dumps({k: v for k, v in record.items() if k != "runtime_identity"}, ensure_ascii=False, indent=2))
    return record


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("record_directory", type=Path)
    args = parser.parse_args()
    raise SystemExit(0 if recheck(args.record_directory)["all_passed"] else 1)
