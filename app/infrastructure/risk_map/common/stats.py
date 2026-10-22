"""Diagnostics keep exact counts while limiting repeated examples."""

import logging
from collections import Counter

logger = logging.getLogger(__name__)


class Diagnostics:
    def __init__(self) -> None:
        self.counts = Counter()
        self.examples = []

    def add(self, code: str, message: str, severity: str = "weak_warning", **context) -> None:
        self.counts[code] += 1
        self.counts[severity] += 1
        if self.counts[code] <= 20:
            self.examples.append({"code": code, "severity": severity, "message": message, **context})
            logger.log(logging.ERROR if severity == "error" else logging.WARNING,
                       "%s: %s %s", code, message, context)

    def to_dict(self) -> dict:
        return {"error_count": self.counts["error"],
                "weak_warning_count": self.counts["weak_warning"],
                "strong_warning_count": self.counts["strong_warning"],
                "diagnostic_counts": {k: v for k, v in self.counts.items()
                                      if k not in {"error", "weak_warning", "strong_warning"}},
                "example_limit_per_code": 20}
