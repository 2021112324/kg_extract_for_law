"""Validate task4 parser/export fixes on risk compliance case source files."""

from __future__ import annotations

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[5]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.infrastructure.information_extraction.compliance_case_v1.compliance_case_parse import (
    discover_compliance_case_files,
    parse_compliance_case_file,
)


ROOT = Path(__file__).resolve().parents[1] / "data" / "合规风险案例"


def main() -> None:
    files = discover_compliance_case_files(ROOT)
    bad_full_text = []
    parser_extracted_knowledge_fields = []
    for path in files:
        document = parse_compliance_case_file(path)
        material = document.fields.get("企业材料", "")
        if material and document.full_text.strip() != material:
            bad_full_text.append(path.name)
        if document.compliance_domain or document.original_compliance_domains or document.keywords:
            parser_extracted_knowledge_fields.append(
                {
                    "filename": path.name,
                    "compliance_domain": document.compliance_domain,
                    "original_compliance_domains": document.original_compliance_domains,
                    "keywords": document.keywords,
                }
            )

    print(
        json.dumps(
            {
                "total": len(files),
                "bad_full_text_count": len(bad_full_text),
                "bad_full_text_examples": bad_full_text[:10],
                "parser_extracted_knowledge_field_count": len(parser_extracted_knowledge_fields),
                "parser_extracted_knowledge_field_examples": parser_extracted_knowledge_fields[:10],
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
