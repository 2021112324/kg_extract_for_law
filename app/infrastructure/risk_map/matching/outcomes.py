"""Local location outcomes; never create graph diagnostic properties."""

from collections import Counter, defaultdict

from ..common.indicators import decode_candidates
from ..config import FINAL, PROTOCOL
from .location import DocumentAmbiguity, descriptions, resolve_attachments


def location_counts(inventory, matching):
    links, article_ids, filenames, files = set(), set(), set(), set()
    statuses = Counter(row["status"] for row in matching.values())
    by_id = {article["id"]: article for article in inventory}
    for article_id, row in matching.items():
        for item in decode_candidates(row["properties"].get(FINAL) or []):
            links.add((article_id, item["指标序号"]))
            article_ids.add(article_id)
        if article_id in article_ids:
            filenames.update(by_id[article_id]["filenames"])
            files.update(tuple(identity) for identity in row.get("file_match", {}).get("matched_identities", []))
    return {"processed_articles": len(matching), "mapped_articles": len(article_ids),
            "complete_located_articles": statuses["定位完成"],
            "partial_articles": statuses["部分完成"], "saved_links": len(links),
            "mapped_files": len(files), "file_unit": "distinct confirmed (region, document identity)",
            "mapped_filenames": len(filenames), "filename_unit": "distinct raw source filename values",
            "no_match_articles": statuses["无匹配"],
            "file_unmatched_articles": statuses["文件未匹配"],
            "empty_basis_articles": statuses["无可用依据"],
            "ambiguous_articles": statuses["文件歧义"],
            "failed_articles": sum(bool(row.get("errors")) for row in matching.values()),
            "unprocessed_articles": max(0, len(inventory) - len(matching))}


def unresolved_basis(index, region, source, inventory, matching):
    """Reconcile every input attachment, including files with no graph articles.

    This is a location inventory, not a semantic judgment or a recall metric.
    Evidence already located elsewhere is retained even if another request fails.
    """
    present, ambiguous, encountered = set(), set(), set()
    located, failed = defaultdict(set), set()
    filename_groups = {tuple(article["filenames"]) for article in inventory}
    for wrapper in source["nodes"]:
        props = wrapper["n"]["properties"]
        if props.get("label") != "法规文件":
            continue
        names = props.get("filename", [])
        names = [names] if isinstance(names, str) else [v for v in names if isinstance(v, str)]
        if isinstance(props.get("文件全称"), str):
            names = [*names, props["文件全称"]]
        filename_groups.add(tuple(sorted(set(names))))
    for names in filename_groups:
        try:
            ids, _ = resolve_attachments(index, names, region)
            present.update(ids)
        except DocumentAmbiguity as exc:
            for identity in exc.details.get("matched_identities", []):
                ambiguous.update(index.files.get(tuple(identity), []))
    for article_id, row in matching.items():
        ids = row.get("file_attachment_ids", [])
        encountered.update(ids)
        for hit in row.get("sources", []):
            for aid in hit["attachment_ids"]:
                located[(aid, hit["description"])].add(article_id)
        for hit in row.get("failed_sources", []):
            failed.update((aid, hit["description"]) for aid in hit["attachment_ids"])
        if row.get("errors") and not row.get("failed_sources"):
            for aid in ids:
                failed.update((aid, unit) for unit in descriptions(index.attachments[aid].knowledge))
    entries = []
    for aid, attachment in enumerate(index.attachments):
        if attachment.region != region:
            continue
        units = descriptions(attachment.knowledge)
        for unit in units or [""]:
            confirmed = sorted(located.get((aid, unit), set()))
            if confirmed and (aid, unit) not in failed:
                continue
            if aid in ambiguous:
                reason = "file_ambiguity"
            elif aid not in present:
                reason = "graph_file_not_found"
            elif not unit:
                reason = attachment.source_verification.get("empty_reason", "empty_reason_unverified")
            elif (aid, unit) in failed:
                reason = "location_failed_or_incomplete"
            elif aid not in encountered:
                reason = "no_processed_article_for_reference"
            else:
                reason = "reference_not_located"
            entries.append({"attachment_id": aid, "indicator_number": attachment.indicator_number,
                            "region": region, "file_name": attachment.mapped_name,
                            "basis_name": attachment.basis_name, "original_text": attachment.original_text,
                            "knowledge": attachment.knowledge, "description": unit,
                            "tree_path": attachment.tree_path, "reason": reason,
                            "confirmed_article_ids": confirmed, "provenance": attachment.provenance})
    return {"protocol": PROTOCOL, "scope": "input attachments in configured region versus current source label",
            "source_label": source.get("source_label"),
            "unresolved_attachments": len({row["attachment_id"] for row in entries}),
            "unresolved_units": len(entries), "reason_counts": dict(Counter(row["reason"] for row in entries)),
            "entries": entries}
