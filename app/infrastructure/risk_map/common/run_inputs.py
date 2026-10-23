"""Freeze actual inputs and public runtime identity; never serialize credentials."""

import hashlib
from pathlib import Path
import subprocess
from urllib.parse import urlsplit, urlunsplit

from .io import atomic_bytes, content_hash, file_hash
from ..preparation.document_identity import country_family


def code_identity():
    package = Path(__file__).resolve().parents[1]
    paths = list(package.glob("*.py"))
    for folder in ("common", "preparation", "matching", "review"):
        paths.extend((package / folder).glob("*.py"))
    hashes = {p.relative_to(package).as_posix(): file_hash(p) for p in sorted(paths)}
    info = {"files": hashes, "runtime_hash": content_hash(hashes)}
    try:
        root = package.parents[2]
        info["git_commit"] = subprocess.run(["git", "rev-parse", "HEAD"], cwd=root, capture_output=True,
                                             text=True, timeout=10, check=True).stdout.strip()
        info["worktree_dirty"] = bool(subprocess.run(["git", "status", "--porcelain"], cwd=root,
            capture_output=True, timeout=10, check=True).stdout)
    except (OSError, subprocess.SubprocessError):
        info["git_identity_available"] = False
    return info


def empty_attachments(index, region):
    return [{"attachment_id": aid, "indicator_number": a.indicator_number, "region": a.region,
             "file_name": a.mapped_name, "basis_name": a.basis_name,
             "tree_path": a.tree_path, "reason": a.source_verification.get("empty_reason", "empty_reason_unverified"),
             "reason_detail": a.source_verification["detail"],
             "family": country_family(a.identity[1], a.region)}
            for aid, a in enumerate(index.attachments) if a.region == region and not a.knowledge.strip()]


def skip_summary(index, matching):
    rows = list(matching.values())
    aids = {aid for r in rows for aid in r.get("skipped_attachment_ids", [])}
    reasons = {index.attachments[i].source_verification.get("empty_reason", "empty_reason_unverified") for i in aids}
    by_reason = {}
    for reason in sorted(reasons):
        selected = {i for i in aids if index.attachments[i].source_verification.get("empty_reason", "empty_reason_unverified") == reason}
        by_reason[reason] = {"unique_empty_attachments": len(selected),
            "document_families": len({(index.attachments[i].region,
                country_family(index.attachments[i].identity[1], index.attachments[i].region)) for i in selected}),
            "affected_articles": sum(bool(selected.intersection(r.get("skipped_attachment_ids", []))) for r in rows)}
    return {"unique_empty_attachments": len(aids),
            "document_families": len({(index.attachments[i].region,
                country_family(index.attachments[i].identity[1], index.attachments[i].region)) for i in aids}),
            "affected_articles": sum(bool(r.get("skipped_attachment_ids")) for r in rows),
            "all_empty_articles": sum(r["status"] == "无可用依据" for r in rows),
            "scope": "encountered_attachments_and_articles", "by_reason": by_reason,
            "reason_article_counts_overlap": True}


def provenance_identity(provenance_bytes, config):
    return {"enabled": provenance_bytes is not None,
            "hash": hashlib.sha256(provenance_bytes).hexdigest() if provenance_bytes is not None else None,
            "policy": config.model.semantic["provenance_policy"]}


def freeze_inputs(records, tree_bytes, config, prompts, identity, skipped, *, provenance_bytes=None, verification=()):
    atomic_bytes(tree_bytes, records.path / "inputs" / "indicator_tree.json")
    source_identity = provenance_identity(provenance_bytes, config)
    if provenance_bytes is not None:
        atomic_bytes(provenance_bytes, records.path / "inputs" / "provenance.json")
    settings = {**config.model.semantic, "request_timeout": config.model.request_timeout,
                "max_attempts": config.model.max_attempts, "retry_delay": config.model.retry_delay,
                "region": config.region, "batch_size": config.batch_size, "provenance": source_identity}
    endpoint = urlsplit(config.model.api_url)
    settings["endpoint"] = urlunsplit((endpoint.scheme, endpoint.netloc.rsplit("@", 1)[-1], endpoint.path, "", ""))
    records.save("inputs/prompts.json", prompts)
    records.save("inputs/configuration.json", settings)
    records.save("inputs/code_identity.json", identity)
    records.save("inputs/empty_attachments.json", skipped)
    records.save("inputs/source_verification.json", list(verification))
    inputs = records.path / "inputs"
    manifest = {"tree_hash": hashlib.sha256(tree_bytes).hexdigest(),
                "provenance": source_identity,
                "files": {p.name: file_hash(p) for p in sorted(inputs.iterdir()) if p.is_file()}}
    records.save("inputs/manifest.json", manifest)
    return content_hash(manifest)
