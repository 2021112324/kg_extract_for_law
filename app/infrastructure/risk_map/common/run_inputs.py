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
             "tree_path": a.tree_path, "reason": "empty_knowledge",
             "family": country_family(a.identity[1], a.region)}
            for aid, a in enumerate(index.attachments) if a.region == region and not a.knowledge.strip()]


def skip_summary(index, matching):
    rows = list(matching.values())
    aids = {aid for r in rows for aid in r.get("skipped_attachment_ids", [])}
    return {"unique_empty_attachments": len(aids),
            "document_families": len({(index.attachments[i].region,
                country_family(index.attachments[i].identity[1], index.attachments[i].region)) for i in aids}),
            "affected_articles": sum(bool(r.get("skipped_attachment_ids")) for r in rows),
            "all_empty_articles": sum(r["status"] == "无可用依据" for r in rows)}


def freeze_inputs(records, tree_bytes, config, prompts, identity, skipped):
    atomic_bytes(tree_bytes, records.path / "inputs" / "indicator_tree.json")
    settings = {**config.model.semantic, "request_timeout": config.model.request_timeout,
                "max_attempts": config.model.max_attempts, "retry_delay": config.model.retry_delay,
                "region": config.region, "batch_size": config.batch_size}
    endpoint = urlsplit(config.model.api_url)
    settings["endpoint"] = urlunsplit((endpoint.scheme, endpoint.netloc.rsplit("@", 1)[-1], endpoint.path, "", ""))
    records.save("inputs/prompts.json", prompts)
    records.save("inputs/configuration.json", settings)
    records.save("inputs/code_identity.json", identity)
    records.save("inputs/empty_attachments.json", skipped)
    inputs = records.path / "inputs"
    manifest = {"tree_hash": hashlib.sha256(tree_bytes).hexdigest(),
                "files": {p.name: file_hash(p) for p in sorted(inputs.iterdir()) if p.is_file()}}
    records.save("inputs/manifest.json", manifest)
    return content_hash(manifest)
