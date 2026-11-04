"""Optional prepare/import/resume workflow. Database imports are deliberately lazy."""

from pathlib import Path
from copy import deepcopy

from .config import StorageConfig
from .enrichment import GraphContext, enrich_entry
from .inputs import read_source
from .local_records import Heartbeat, RunRecords, redact
from .records import (KnowledgeStorageError, canonical, digest, from_json_record,
                      record_digest, validate_record)


def runtime_hash():
    root = Path(__file__).resolve().parents[1]
    paths = sorted((root / "storage").glob("*.py")) + sorted((root / "converters").glob("*.py"))
    return digest({str(p.relative_to(root)): digest(p.read_text(encoding="utf-8")) for p in paths})


def merge_records(existing, incoming):
    for field in ("content", "knowledge_category", "knowledge_type", "dataset_version"):
        if existing[field] != incoming[field]:
            raise KnowledgeStorageError("Knowledge identity collision: " + field)
    result = dict(existing)
    for field in ("sources", "document_indicators", "attribute_provenance", "semantic_types"):
        result[field] = sorted({canonical(v): v for v in existing[field] + incoming[field]}.values(), key=canonical)
    for field in ("risk_types", "economic_industries", "indicators"):
        a, b = existing[field], incoming[field]
        state_values = [r["field_states"][field]["state"] for r in (existing, incoming)]
        if "invalid" in state_values or "conflict" in state_values:
            result[field] = None
            result["field_states"] = {**result["field_states"], field: {"state": "conflict" if "conflict" in state_values else "invalid"}}
        elif a is not None or b is not None:
            result[field] = sorted({canonical(v): v for v in (a or []) + (b or [])}.values(), key=canonical)
            result["field_states"] = {**result["field_states"], field: {"state": "present" if result[field] else "empty"}}
    if result.get("economic_industries") and "通用" in result["economic_industries"] and len(result["economic_industries"]) > 1:
        result["economic_industries"] = None
        result["field_states"] = {**result["field_states"], "economic_industries": {"state": "conflict"}}
    validity_states = [r["field_states"]["validity_status"]["state"] for r in (existing, incoming)]
    validity_values = {r["validity_status"] for r in (existing, incoming) if r["validity_status"] is not None}
    if "conflict" in validity_states or "invalid" in validity_states or len(validity_values) > 1:
        result["validity_status"] = None
        result["field_states"] = {**result["field_states"], "validity_status": {
            "state": "conflict" if "conflict" in validity_states or len(validity_values) > 1 else "invalid"}}
    elif validity_values:
        result["validity_status"] = next(iter(validity_values))
        result["field_states"] = {**result["field_states"], "validity_status": {"state": "present"}}
    definitions = {}
    for item in result.get("indicators") or []:
        key = (item["indicator_system_version"], item["indicator_number"])
        value = canonical([item["indicator_name"], item["indicator_description"]])
        if key in definitions and definitions[key] != value:
            result["metadata"] = {**result["metadata"], "conflicting_indicators": result["indicators"]}
            result["indicators"] = None
            result["field_states"] = {**result["field_states"], "indicators": {"state": "conflict"}}
            break
        definitions[key] = value
    file_definitions = {}
    for group in result["document_indicators"]:
        key = group["document_key"]
        file_definitions.setdefault(key, set()).add(canonical([group["mapping_state"], group["indicators"]]))
    if any(len(v) > 1 for v in file_definitions.values()):
        result["document_indicators"] = [{**g, "mapping_state": "conflict"} if len(file_definitions[g["document_key"]]) > 1 else g
                                          for g in result["document_indicators"]]
        result["field_states"] = {**result["field_states"], "document_indicators": {"state": "conflict"}}
    file_states = [g["mapping_state"] for g in result["document_indicators"]]
    result["field_states"] = {**result["field_states"], "document_indicators": {
        "state": next((s for s in ("conflict", "invalid", "present", "missing", "empty") if s in file_states), "missing")}}
    if incoming["field_states"]["sources"]["state"] == "conflict":
        result["field_states"] = {**result["field_states"], "sources": {"state": "conflict"}}
    result["metadata"] = {**result["metadata"], "merged_metadata": sorted({canonical(v): v for v in (
        existing["metadata"].get("merged_metadata") or [existing["metadata"]]) + [incoming["metadata"]]}.values(), key=canonical)}
    result["record_hash"] = record_digest(result)
    validate_record(result)
    return result


def prepare_knowledge(config: StorageConfig, *, snapshots=None, driver=None, database=None, run_id=None):
    config.validate()
    records = RunRecords(config.output_root, run_id)
    summary = {"run_id": records.run_id, "dataset_version": config.dataset_version,
               "record_directory": str(records.directory), "status": "preparing", "records": 0}
    try:
        with Heartbeat(records) as heartbeat:
            heartbeat.progress("freeze_inputs", 0, len(config.sources))
            inputs = deepcopy(snapshots) if snapshots is not None else [read_source(s, driver=driver, database=database) for s in config.sources]
            if len(inputs) != len(config.sources):
                raise KnowledgeStorageError("Snapshot and source configuration counts differ")
            # Do not trust a caller-supplied snapshot hash; bind the actual input bytes.
            for source, snapshot in zip(config.sources, inputs):
                import json
                snapshot["nodes"] = json.loads(canonical(snapshot["nodes"]))
                snapshot["edges"] = json.loads(canonical(snapshot["edges"]))
                snapshot["snapshot"] = digest([snapshot["nodes"], snapshot["edges"]])
                snapshot["source"] = {"converter": source.converter, "category": source.category,
                                      "graph_label": source.graph_label, "graph_source": source.graph_source}
            code_hash = runtime_hash()
            binding = digest([config.semantic(), [s["snapshot"] for s in inputs], code_hash])
            records.write("inputs.json", inputs)
            records.write("config.json", config.semantic())
            from .adapters import generate_entries
            selected = {}
            generated_count = 0
            ownership_diagnostics = []
            for i, snapshot in enumerate(inputs):
                heartbeat.progress("convert_and_enrich", i, len(inputs))
                context = GraphContext(snapshot)
                if context.ownership_diagnostics:
                    ownership_diagnostics.extend({**item, "graph_label": snapshot["source"]["graph_label"]}
                                                 for item in context.ownership_diagnostics)
                    records.write("ownership_diagnostics.json", ownership_diagnostics)
                    records.event("ownership_edges_ignored", graph_label=snapshot["source"]["graph_label"],
                                  count=len(context.ownership_diagnostics),
                                  reason="self_referential_ownership",
                                  details_file="ownership_diagnostics.json")
                entries = generate_entries(snapshot["source"]["converter"], snapshot["nodes"], snapshot["edges"],
                                           graph_label=snapshot["source"]["graph_label"])
                generated_count += len(entries)
                for entry_index, entry in enumerate(entries):
                    if entry_index % 200 == 0:
                        heartbeat.progress("enrich_" + snapshot["source"]["converter"], entry_index, len(entries))
                    row = enrich_entry(entry, context, config, records.run_id, binding, records.directory)
                    key = row["knowledge_id"]
                    selected[key] = merge_records(selected[key], row) if key in selected else row
            rows = sorted(selected.values(), key=lambda r: r["knowledge_id"])
            records.write("records.jsonl", rows)
            manifest = {"protocol": "single-table-knowledge-v1", "run_id": records.run_id,
                        "dataset_version": config.dataset_version, "input_binding": binding,
                        "runtime_hash": code_hash, "records_hash": digest(rows), "record_count": len(rows),
                        "batch_size": config.batch_size,
                        "input_hashes": [s["snapshot"] for s in inputs], "config_hash": digest(config.semantic())}
            records.write("manifest.json", manifest)
            if heartbeat.record_error:
                raise KnowledgeStorageError("Heartbeat local record failed: " + heartbeat.record_error)
            summary.update(status="prepared" if rows else "empty", records=len(rows),
                           input_nodes=sum(len(s["nodes"]) for s in inputs),
                           input_relationships=sum(len(s["edges"]) for s in inputs),
                           generated_entries=generated_count, merged_duplicates=generated_count - len(rows),
                           ownership_edges_ignored=len(ownership_diagnostics),
                           field_warnings=sum(v["state"] in {"invalid", "conflict"} for r in rows for v in r["field_states"].values()))
            records.event("prepared", records=len(rows), input_binding=binding)
    except BaseException as exc:
        summary.update(status="interrupted" if not isinstance(exc, Exception) else "prepare_failed",
                       error_type=type(exc).__name__, error=redact(str(exc)))
        records.write("summary.json", summary)
        raise
    records.write("summary.json", summary)
    return summary


def load_prepared(records):
    import json
    manifest = records.read("manifest.json")
    if manifest.get("protocol") != "single-table-knowledge-v1" or manifest["runtime_hash"] != runtime_hash():
        raise KnowledgeStorageError("Run protocol or runtime code changed; prepare a new version")
    with (records.directory / "records.jsonl").open(encoding="utf-8") as file:
        rows = [from_json_record(json.loads(line)) for line in file if line.strip()]
    if digest(rows) != manifest["records_hash"] or len(rows) != manifest["record_count"]:
        raise KnowledgeStorageError("Frozen record manifest mismatch")
    config = records.read("config.json")
    inputs = records.read("inputs.json")
    if digest(config) != manifest["config_hash"] or [digest([s["nodes"], s["edges"]]) for s in inputs] != manifest["input_hashes"]:
        raise KnowledgeStorageError("Frozen input/configuration changed")
    binding = digest([config, manifest["input_hashes"], manifest["runtime_hash"]])
    if binding != manifest["input_binding"] or any(r["metadata"].get("input_binding") != binding for r in rows):
        raise KnowledgeStorageError("Run input binding mismatch")
    return manifest, rows


def _verify_subset(actual, expected):
    by_id = {r["knowledge_id"]: r for r in actual}
    for row in expected:
        found = by_id.get(row["knowledge_id"])
        if found is None or found["record_hash"] != row["record_hash"] or record_digest(found) != row["record_hash"]:
            raise KnowledgeStorageError("Batch write is not confirmed by complete record readback")


def _persist_records(records, *, repository=None, initialize_schema=True):
    manifest, rows = load_prepared(records)
    if not rows:
        raise KnowledgeStorageError("An empty run cannot publish a database version")
    if repository is None:
        from .repository import KnowledgeRepository
        repository = KnowledgeRepository()
    engine = getattr(repository, "engine", None)
    if engine is not None:
        url = engine.url.set(password=None)
        target_identity = digest([url.drivername, url.host, url.port, url.username, url.database])
    else:
        target_identity = digest(type(repository).__name__)
    binding_path = records.directory / "target.json"
    if binding_path.exists() and records.read("target.json")["identity"] != target_identity:
        raise KnowledgeStorageError("Resume database target differs from the frozen run")
    summary = {"status": "running", "database_published": False, "run_id": records.run_id,
               "dataset_version": manifest["dataset_version"], "records": len(rows),
               "record_directory": str(records.directory), "confirmed": 0,
               "inserted": 0, "reused": 0, "recovered_acknowledgements": 0}
    try:
        with Heartbeat(records) as heartbeat:
            heartbeat.progress("database_preflight")
            database_info = repository.preflight() or {}
            records.event("database_preflight", database=database_info.get("database_name"),
                          server_version=database_info.get("server_version"), target_identity=target_identity)
            if initialize_schema:
                repository.initialize()
            with repository.lock(manifest["dataset_version"]):
                records.write("target.json", {"identity": target_identity})
                existing = repository.read_version(manifest["dataset_version"])
                initial_ids = {r["knowledge_id"] for r in existing}
                expected = {r["knowledge_id"]: r for r in rows}
                if any(r["knowledge_id"] not in expected for r in existing):
                    raise KnowledgeStorageError("Dataset version contains records from another input")
                _verify_subset(existing, [expected[r["knowledge_id"]] for r in existing])
                if any(r["publication_status"] == "published" for r in existing):
                    repository.verify(manifest["dataset_version"], rows)
                    if len(existing) != len(rows) or any(r["publication_status"] != "published" for r in existing):
                        raise KnowledgeStorageError("Partially published version is inconsistent")
                    summary.update(status="already_published", database_published=True, confirmed=len(rows), reused=len(rows))
                else:
                    batch_size = manifest["batch_size"]
                    for start in range(0, len(rows), batch_size):
                        batch = rows[start:start + batch_size]
                        heartbeat.progress("write_batch", start, len(rows))
                        records.write("intent.json", {"start": start, "ids": [r["knowledge_id"] for r in batch],
                                                     "hashes": [r["record_hash"] for r in batch]})
                        try:
                            repository.write_batch(batch)
                        except Exception:
                            # A failed acknowledgement is not proof of a rollback.
                            repository.assert_lock()
                            _verify_subset(repository.read_version(manifest["dataset_version"]), batch)
                            summary["recovered_acknowledgements"] += 1
                            records.event("write_ack_recovered", start=start, count=len(batch))
                        _verify_subset(repository.read_version(manifest["dataset_version"]), batch)
                        summary["confirmed"] = start + len(batch)
                        summary["inserted"] += sum(r["knowledge_id"] not in initial_ids for r in batch)
                        summary["reused"] += sum(r["knowledge_id"] in initial_ids for r in batch)
                        records.write("checkpoint.json", {"confirmed": summary["confirmed"], "binding": manifest["input_binding"]})
                        records.event("batch_confirmed", confirmed=summary["confirmed"], total=len(rows))
                    heartbeat.progress("verify_and_publish", len(rows), len(rows))
                    repository.verify(manifest["dataset_version"], rows)
                    records.write("publication_intent.json", {"version": manifest["dataset_version"], "records_hash": manifest["records_hash"]})
                    try:
                        repository.publish(manifest["dataset_version"], rows)
                    except Exception:
                        found = repository.read_version(manifest["dataset_version"])
                        _verify_subset(found, rows)
                        if len(found) != len(rows) or any(r["publication_status"] != "published" for r in found):
                            raise
                        records.event("publication_ack_recovered", count=len(rows))
                    summary.update(status="published", database_published=True)
            if heartbeat.record_error:
                raise KnowledgeStorageError("Heartbeat local record failed: " + heartbeat.record_error)
        records.write("summary.json", summary)
        return summary
    except BaseException as exc:
        summary.update(status="interrupted" if not isinstance(exc, Exception) else
                       "record_error" if summary["database_published"] else "failed_or_unconfirmed",
                       error_type=type(exc).__name__, error=redact(str(exc)))
        try:
            records.write("summary.json", summary)
        except OSError:
            summary["summary_record_error"] = True
        if not isinstance(exc, Exception):
            raise
        return summary


def persist_knowledge(run_directory, *, repository=None, initialize_schema=True):
    records = RunRecords.open(run_directory)
    with records.writer_lock():
        return _persist_records(records, repository=repository, initialize_schema=initialize_schema)
