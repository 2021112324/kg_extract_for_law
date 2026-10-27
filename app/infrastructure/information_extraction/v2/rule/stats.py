"""Counters distinguish structure, semantics and confirmed storage."""
from collections import Counter


class ResultStats:
    def __init__(self):
        self.counts = Counter()
        self.errors = []
        self.warnings = []

    def add_error(self, category, task, message):
        self.counts["error"] += 1
        self.counts[category + "_error"] += 1
        self.errors.append({"category": category, "task": task, "message": message})

    def warn(self, code, message, strong=False):
        self.counts["strong_warning" if strong else "weak_warning"] += 1
        self.warnings.append({"code": code, "message": message, "strong": strong})

    def merge(self, value):
        self.counts.update(value["counts"])
        self.errors.extend(value["errors"])
        self.warnings.extend(value["warnings"])

    def to_dict(self):
        return {"counts": dict(self.counts), "errors": self.errors, "warnings": self.warnings}
