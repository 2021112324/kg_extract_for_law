"""Scoped names and optional provenance; no production connections or real models."""

import copy
from dataclasses import replace
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from app.infrastructure.risk_map import MappingConfig, ModelConfig, execute_mapping
from app.infrastructure.risk_map.common.io import atomic_json, read_json
from app.infrastructure.risk_map.common.models import FileAttachment
from app.infrastructure.risk_map.common.run_inputs import empty_attachments, skip_summary
from app.infrastructure.risk_map.common.stats import Diagnostics
from app.infrastructure.risk_map.config import CANDIDATES, FINAL
from app.infrastructure.risk_map.local_records import RunRecords
from app.infrastructure.risk_map.matching.location import DocumentAmbiguity, resolve_attachments
from app.infrastructure.risk_map.preparation.tree_index import attach_provenance, build_tree_index
from app.infrastructure.risk_map.test.test_copy_first import Client, FakeAdapter, TEXT, tree


def files(data, number=1):
    return data["产品法律风险"]["一级"]["二级"][f"指标{number}"]["映射文件"]["中国"]


def named_tree(count=1, aliases=None, description=TEXT):
    data = tree(count, description)
    if aliases is not None:
        for i in range(1, count + 1):
            files(data, i)["测试法"]["别名"] = aliases
    return data


def provenance_for(data, status=None):
    index = build_tree_index(data, Diagnostics())
    sources, rows = {}, []
    for aid, a in enumerate(index.attachments):
        sid = f"{a.region}:{aid}:C"
        state = status or ("成功且有知识" if a.knowledge.strip() else "成功但当前风险无知识")
        has_source = state in {"成功且有知识", "成功但当前风险无知识"}
        aliases = getattr(a, "aliases", [])
        sources[sid] = {"来源ID": sid, "文件来源ID": f"{a.region}:{aid}", "地区": a.region,
            "文件名称": a.basis_name or a.mapped_name, "风险类型": a.risk_type, "采用知识文本": a.knowledge}
        names = [(a.mapped_name, "主名称")] + [(n, "别名") for n in aliases]
        details = [{"查询名称": n, "角色": role, "状态": "unmatched" if state == "未找到文件" else "matched",
                    "候选来源": [] if state == "未找到文件" else [list(a.identity)]} for n, role in names]
        if state == "匹配歧义":
            details[0]["状态"] = "ambiguous"
            details[0]["候选来源"].append([a.region, "另一个文件"])
        rows.append({"指标序号": a.indicator_number, "指标名称": index.indicators[a.indicator_number].name,
            "风险类型": a.risk_type, "地区": a.region, "文件名称": a.mapped_name, "主名称": a.mapped_name,
            "原始文本": a.original_text, "别名": aliases, "状态": state, "名称匹配明细": details,
            "来源ID": [sid] if has_source else [], "文件来源ID": [f"{a.region}:{aid}"] if has_source else [],
            "采用来源ID": [sid] if state == "成功且有知识" else [],
            "匹配文件身份": list(a.identity) if has_source else None})
    return {"来源单元格": sources, "指标文件挂接": rows}


class ScopedNameTests(unittest.TestCase):
    def resolve(self, data, names, region="中国"):
        return resolve_attachments(build_tree_index(data, Diagnostics()), names, region)

    def test_legacy_and_empty_aliases_keep_cardinality(self):
        for data in (tree(), named_tree(7, [])):
            index = build_tree_index(data, Diagnostics())
            self.assertEqual(len(index.attachments), 7)
            self.assertEqual(index.attachments[0].aliases, [])

    def test_legacy_attachment_position_arguments_stay_compatible(self):
        a = FileAttachment("2.1.1.1", "产品法律风险", "中国", "a", "b", "raw", "", "text", (), ("中国", "b"), [{"x": 1}])
        self.assertEqual(a.provenance, [{"x": 1}])
        self.assertEqual(a.aliases, [])

    def test_invalid_alias_types_are_locatable(self):
        for value in (None, "Example Act", {}, [1], [False], ["a", None]):
            data = tree(1)
            files(data)["测试法"]["别名"] = value
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "2.1.1.1.*中国.*测试法"):
                build_tree_index(data, Diagnostics())

    def test_alias_cleaning_keeps_first_spelling_and_original_tree(self):
        data = named_tree(1, [" Example Act ", "example act", " ", "测试法"])
        original = copy.deepcopy(data)
        index = build_tree_index(data, Diagnostics())
        self.assertEqual(index.attachments[0].aliases, ["Example Act", "测试法"])
        self.assertEqual(data, original)

    def test_alias_only_and_bilingual_names(self):
        data = named_tree(7, ["Example Act"])
        for names in (["Example Act.md"], ["测试法.txt", "Example Act"]):
            ids, details = self.resolve(data, names)
            self.assertEqual(len(ids), 7)
            self.assertTrue(any(b["role"] == "alias" for hit in details["name_hits"] for b in hit["bindings"]))

    def test_shared_alias_does_not_form_transitive_equivalence(self):
        data = named_tree(1, ["shared"])
        files(data)["Other Act"] = {"原始文本": "Other Act", "依据文件名": "Other Act", "别名": ["shared", "B"],
                                    "已找到": "", "知识依据": TEXT}
        self.assertEqual(len(self.resolve(data, ["测试法"])[0]), 1)
        self.assertEqual(len(self.resolve(data, ["B"])[0]), 1)
        for names in (["shared"], ["测试法", "shared"], ["测试法", "B"]):
            with self.subTest(names=names), self.assertRaises(DocumentAmbiguity):
                self.resolve(data, names)

    def test_exact_cannot_hide_other_names_fallback(self):
        data = tree(1)
        files(data)["另一法"] = {"原始文本": "另一法", "依据文件名": "另一法", "已找到": "", "知识依据": TEXT}
        with self.assertRaises(DocumentAmbiguity):
            self.resolve(data, ["测试法", "另一法(2024修订)"])

    def test_unknown_contradictory_name_is_not_dropped(self):
        with self.assertRaises(DocumentAmbiguity):
            self.resolve(tree(1), ["测试法", "未声明其他法"])

    def test_unknown_single_file_returns_no_match(self):
        self.assertEqual(self.resolve(tree(1), ["未声明其他法"])[0], [])

    def test_aliases_are_region_scoped(self):
        self.assertEqual(self.resolve(named_tree(1, ["Example Act"]), ["Example Act"], "美国")[0], [])

    def test_multiple_revision_fallback_remains_ambiguous(self):
        data = tree(1)
        files(data)["测试法(2020修订)"] = {**files(data)["测试法"], "依据文件名": "测试法(2020修订)"}
        with self.assertRaises(DocumentAmbiguity):
            self.resolve(data, ["测试法(2024修订)"])
        self.assertEqual(len(self.resolve(data, ["测试法"])[0]), 1)

    def test_explicit_input_versions_conflict(self):
        with self.assertRaises(DocumentAmbiguity):
            self.resolve(tree(1), ["测试法(2020修订)", "测试法(2024修订)"])

    def test_unique_fallback_and_prefix_still_work(self):
        for query in ("测试法(2024修订)", "中华人民共和国测试法", "中华人民共和国测试法(2024修订)"):
            self.assertEqual(len(self.resolve(tree(1), [query])[0]), 1)

    def test_identity_qualifiers_and_known_different_documents(self):
        pairs = [("保密资格管理办法", "保密资质管理办法"), ("EN ISO 9001", "DIN EN ISO 9001"),
                 ("BAT参考文件", "工业排放指令"), ("ISO 9001", "质量管理体系认证规则"),
                 ("ISO/IEC 27701", "ISO/IEC 27002"), ("测试法", "测试法(试行)"),
                 ("GB/T 4754-2017", "GB/T 4754-2024")]
        for title, other in pairs:
            data = tree(1)
            leaf = files(data).pop("测试法")
            files(data)[title] = {**leaf, "依据文件名": title}
            with self.subTest(title=title):
                self.assertEqual(self.resolve(data, [other])[0], [])


class ProvenanceTests(unittest.TestCase):
    def prepare(self, data, provenance):
        index, diagnostics = build_tree_index(data, Diagnostics()), Diagnostics()
        attach_provenance(index, provenance, diagnostics)
        return index, diagnostics

    def test_valid_nonempty_source_is_verified(self):
        data = named_tree(1, ["Example Act"])
        index, _ = self.prepare(data, provenance_for(data))
        self.assertTrue(index.attachments[0].source_verification["verified"])
        self.assertEqual(len(index.attachments[0].provenance), 1)

    def test_alias_can_repeat_primary_with_explicit_role(self):
        data = named_tree(1, ["测试法"])
        index, _ = self.prepare(data, provenance_for(data))
        self.assertTrue(index.attachments[0].source_verification["verified"])

    def test_three_verified_empty_reasons(self):
        data = tree(1, "")
        for status, reason in (("成功但当前风险无知识", "risk_column_empty"),
                               ("未找到文件", "source_not_found"), ("匹配歧义", "source_ambiguity")):
            index, _ = self.prepare(data, provenance_for(data, status))
            self.assertEqual(empty_attachments(index, "中国")[0]["reason"], reason)

    def test_missing_provenance_has_explicit_unknown_reason(self):
        index = build_tree_index(tree(1, ""), Diagnostics())
        row = empty_attachments(index, "中国")[0]
        self.assertEqual(row["reason"], "empty_reason_unverified")
        self.assertEqual(row["reason_detail"], "provenance_not_configured")

    def test_invalid_top_level_is_input_error(self):
        for value in ({}, {"来源单元格": [], "指标文件挂接": []}, {"来源单元格": {}, "指标文件挂接": {}}):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.prepare(tree(1), value)

    def test_mismatch_downgrades_explanation_not_tree(self):
        data = named_tree(1, ["Example Act"])
        for field, value in (("风险类型", "企业信用风险"), ("地区", "美国"), ("别名", ["Other"]),
                             ("原始文本", "another reference"), ("状态", "匹配歧义")):
            prov = provenance_for(data)
            prov["指标文件挂接"][0][field] = value
            index, diagnostics = self.prepare(data, prov)
            with self.subTest(field=field):
                self.assertFalse(index.attachments[0].source_verification["verified"])
                self.assertEqual(index.attachments[0].knowledge, TEXT)
                self.assertGreater(diagnostics.to_dict()["strong_warning_count"], 0)

    def test_mismatch_in_source_cells_is_not_accepted(self):
        for field, value in (("风险类型", "企业信用风险"), ("地区", "美国"), ("文件名称", "其他法"),
                             ("采用知识文本", "其他正文"), ("来源ID", "wrong")):
            data = tree(1)
            prov = provenance_for(data)
            next(iter(prov["来源单元格"].values()))[field] = value
            index, _ = self.prepare(data, prov)
            with self.subTest(field=field):
                self.assertFalse(index.attachments[0].source_verification["verified"])

    def test_conflicting_duplicate_rows_are_not_first_wins(self):
        data = tree(1)
        prov = provenance_for(data)
        other = copy.deepcopy(prov["指标文件挂接"][0])
        other["状态"] = "未找到文件"
        prov["指标文件挂接"].append(other)
        index, _ = self.prepare(data, prov)
        self.assertFalse(index.attachments[0].source_verification["verified"])

    def test_missing_row_and_empty_adopted_ids_do_not_verify(self):
        for empty_rows in (True, False):
            data = tree(1, "")
            prov = provenance_for(data)
            if empty_rows:
                prov["指标文件挂接"] = []
            else:
                prov["指标文件挂接"][0]["来源ID"] = []
            index, _ = self.prepare(data, prov)
            self.assertEqual(empty_attachments(index, "中国")[0]["reason"], "empty_reason_unverified")

    def test_multiple_reasons_overlap_articles_not_attachments(self):
        data = tree(2, "")
        prov = provenance_for(data)
        second = prov["指标文件挂接"][1]
        second.update({"状态": "未找到文件", "来源ID": [], "文件来源ID": [], "匹配文件身份": None})
        second["名称匹配明细"][0].update({"状态": "unmatched", "候选来源": []})
        index, _ = self.prepare(data, prov)
        result = skip_summary(index, {"a": {"skipped_attachment_ids": [0, 1], "status": "无可用依据"}})
        self.assertEqual(result["affected_articles"], 1)
        self.assertEqual(result["by_reason"]["risk_column_empty"]["affected_articles"], 1)
        self.assertEqual(result["by_reason"]["source_not_found"]["affected_articles"], 1)

    def test_provenance_display_punctuation_does_not_create_new_aliases(self):
        data = tree(1)
        leaf = files(data).pop("测试法")
        files(data)["《程序、记录规则》"] = {**leaf, "依据文件名": "程序、记录规则"}
        prov = provenance_for(data)
        row = prov["指标文件挂接"][0]
        row["主名称"] = "程序、记录规则"
        row["匹配文件身份"][1] = "程序,记录规则"
        row["名称匹配明细"][0]["候选来源"][0][1] = "程序,记录规则"
        index, _ = self.prepare(data, prov)
        self.assertTrue(index.attachments[0].source_verification["verified"])
        self.assertEqual(resolve_attachments(index, ["程序,记录规则"], "中国")[0], [])

    def test_adopted_source_order_not_lookup_order(self):
        data = tree(1, "甲正文\n\n乙正文")
        prov = provenance_for(data)
        sid, source = next(iter(prov["来源单元格"].items()))
        source["采用知识文本"] = "甲正文"
        prov["来源单元格"]["second"] = {**source, "来源ID": "second", "采用知识文本": "乙正文"}
        row = prov["指标文件挂接"][0]
        row["来源ID"] = ["second", sid]
        row["采用来源ID"] = [sid, "second"]
        index, _ = self.prepare(data, prov)
        self.assertTrue(index.attachments[0].source_verification["verified"])

    def test_invalid_status_is_diagnostic_not_crash(self):
        data = tree(1)
        for field in ("状态", "名称角色"):
            prov = provenance_for(data)
            if field == "状态":
                prov["指标文件挂接"][0]["状态"] = []
            else:
                prov["指标文件挂接"][0]["名称匹配明细"][0]["角色"] = []
            index, _ = self.prepare(data, prov)
            self.assertFalse(index.attachments[0].source_verification["verified"])


class EntryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.root = Path(self.folder.name)
        self.tree_path, self.prov_path = self.root / "tree.json", self.root / "provenance.json"
        self.data = named_tree(1, ["Example Act"])
        files(self.data)["测试法"]["已找到"] = ""
        atomic_json(self.data, self.tree_path)
        atomic_json(provenance_for(self.data), self.prov_path)
        self.config = MappingConfig("source", "target", tree_path=self.tree_path,
            output_root=self.root / "output", write_enabled=True, exclusive_target=True,
            model=ModelConfig(retry_delay=0), provenance_path=self.prov_path)
        self.adapter = FakeAdapter(1)
        self.adapter.source["nodes"][0]["n"]["properties"]["filename"] = ["测试法", "Example Act"]
        self.client = Client(self.adapter)

    def tearDown(self):
        self.folder.cleanup()

    async def run_map(self, **kwargs):
        return await execute_mapping(kwargs.pop("config", self.config), self.adapter, client=self.client, **kwargs)

    async def test_formal_entry_alias_provenance_and_exact_bytes(self):
        summary, code = await self.run_map()
        self.assertEqual(code, 0)
        folder = Path(summary["record_directory"])
        self.assertEqual((folder / "inputs/provenance.json").read_bytes(), self.prov_path.read_bytes())
        manifest = read_json(folder / "inputs/manifest.json")
        self.assertTrue(manifest["provenance"]["enabled"])
        self.assertTrue(read_json(folder / "inputs/source_verification.json")[0]["verified"])
        records = RunRecords(self.config.output_root, summary["run_id"], resume=True)
        match = records.batches()[0]["matching"]["ts0"]
        self.assertTrue(match["file_match"]["name_hits"])
        self.assertEqual(summary["input_diagnostics"]["provenance"]["verified"], 1)
        props = self.adapter.target["nodes"][0]["n"]["properties"]
        self.assertEqual(props[CANDIDATES], props[FINAL])
        self.assertFalse({"source_verification", "name_hits", "三级指标映射状态"} & props.keys())
        self.assertNotIn("api_key", json.dumps(read_json(folder / "inputs/configuration.json")))

    async def test_prepare_uses_provenance_without_copy_or_model(self):
        summary, code = await self.run_map(action="prepare")
        self.assertEqual(code, 0)
        self.assertEqual(summary["input_diagnostics"]["provenance"]["verified"], 1)
        self.assertEqual(self.client.calls, 0)
        self.assertEqual(self.adapter.calls, [])

    async def test_all_empty_does_not_request_model(self):
        data = named_tree(1, ["Example Act"], "")
        atomic_json(data, self.tree_path)
        atomic_json(provenance_for(data, "匹配歧义"), self.prov_path)
        summary, code = await self.run_map()
        self.assertEqual(code, 0)
        self.assertEqual(self.client.calls, 0)
        self.assertEqual(summary["basis_skips"]["by_reason"]["source_ambiguity"]["unique_empty_attachments"], 1)

    async def test_no_provenance_does_not_read_neighbor(self):
        self.prov_path.write_bytes(b"not json")
        summary, code = await self.run_map(config=replace(self.config, provenance_path=None))
        self.assertEqual(code, 0)
        self.assertFalse((Path(summary["record_directory"]) / "inputs/provenance.json").exists())

    async def test_bad_explicit_source_rejected_without_side_effects(self):
        for contents in (None, b"bad json", b"{}"):
            if contents is None:
                self.prov_path.unlink(missing_ok=True)
            else:
                self.prov_path.write_bytes(contents)
            with self.subTest(contents=contents), self.assertRaises(ValueError):
                await self.run_map()
            self.assertEqual(self.adapter.calls, [])
            self.assertEqual(self.client.calls, 0)

    async def test_output_overlap_is_rejected(self):
        inside = self.config.output_root / "p.json"
        atomic_json(provenance_for(self.data), inside)
        with self.assertRaises(ValueError):
            await self.run_map(config=replace(self.config, provenance_path=inside))
        self.assertEqual(self.adapter.calls, [])

    async def test_resume_and_inspect_allow_same_bytes_new_path(self):
        summary, _ = await self.run_map()
        relocated = self.root / "relocated.json"
        relocated.write_bytes(self.prov_path.read_bytes())
        config = replace(self.config, provenance_path=relocated)
        before = (len(self.adapter.calls), self.client.calls)
        for action in ("resume", "inspect"):
            result, code = await self.run_map(config=config, action=action, run_id=summary["run_id"])
            self.assertEqual(code, 0, result)
        self.assertEqual(before, (len(self.adapter.calls), self.client.calls))

    async def test_changed_source_and_enabled_state_refuse_restore(self):
        summary, _ = await self.run_map()
        before = (len(self.adapter.calls), self.client.calls)
        self.prov_path.write_bytes(self.prov_path.read_bytes() + b" ")
        for config in (self.config, replace(self.config, provenance_path=None)):
            for action in ("resume", "inspect"):
                result, code = await self.run_map(config=config, action=action, run_id=summary["run_id"])
                self.assertNotEqual(code, 0)
                self.assertIn("provenance", result["error"])
        self.assertEqual(before, (len(self.adapter.calls), self.client.calls))

    async def test_tampered_snapshot_refuses_restore(self):
        summary, _ = await self.run_map()
        (Path(summary["record_directory"]) / "inputs/provenance.json").write_bytes(b"{}")
        before = len(self.adapter.calls)
        result, code = await self.run_map(action="inspect", run_id=summary["run_id"])
        self.assertNotEqual(code, 0)
        self.assertIn("快照", result["error"])
        self.assertEqual(len(self.adapter.calls), before)

    async def test_runtime_change_refuses_restore(self):
        summary, _ = await self.run_map()
        with patch("app.infrastructure.risk_map.pipeline.code_identity", return_value={"runtime_hash": "changed"}):
            result, code = await self.run_map(action="resume", run_id=summary["run_id"])
        self.assertNotEqual(code, 0)
        self.assertIn("runtime_hash", result["error"])

    async def test_tree_alias_change_and_legacy_binding_preserve_old_summary(self):
        summary, _ = await self.run_map()
        folder = Path(summary["record_directory"])
        before = (folder / "summary.json").read_bytes()
        old_calls = (len(self.adapter.calls), self.client.calls)
        changed = copy.deepcopy(self.data)
        files(changed)["测试法"]["别名"].append("Another Name")
        atomic_json(changed, self.tree_path)
        result, code = await self.run_map(action="resume", run_id=summary["run_id"])
        self.assertNotEqual(code, 0)
        self.assertIn("tree_hash", result["error"])
        atomic_json(self.data, self.tree_path)
        run = read_json(folder / "run.json")
        run.pop("provenance")
        atomic_json(run, folder / "run.json")
        result, code = await self.run_map(action="inspect", run_id=summary["run_id"])
        self.assertNotEqual(code, 0)
        self.assertIn("provenance", result["error"])
        self.assertEqual(before, (folder / "summary.json").read_bytes())
        self.assertEqual(old_calls, (len(self.adapter.calls), self.client.calls))

    async def test_source_changes_isolate_model_cache(self):
        first, code = await self.run_map()
        self.assertEqual(code, 0)
        self.prov_path.write_bytes(self.prov_path.read_bytes() + b" ")
        second_adapter = FakeAdapter(1)
        second_adapter.source = copy.deepcopy(self.adapter.source)
        second_client = Client(second_adapter)
        second, code = await execute_mapping(self.config, second_adapter, client=second_client)
        self.assertEqual(code, 0)
        self.assertGreater(second_client.calls, 0)
        self.assertEqual(second["model"]["cache_hits"], 0)

    async def test_empty_attachment_cannot_borrow_neighbor_indicator_text(self):
        data = named_tree(2, ["Example Act"])
        files(data, 1)["测试法"]["知识依据"] = ""
        atomic_json(data, self.tree_path)
        atomic_json(provenance_for(data), self.prov_path)
        summary, code = await self.run_map()
        self.assertEqual(code, 0)
        props = self.adapter.target["nodes"][0]["n"]["properties"]
        self.assertEqual([json.loads(v)["指标序号"] for v in props[CANDIDATES]], ["2.1.1.2"])
        self.assertEqual(summary["basis_skips"]["unique_empty_attachments"], 1)

    async def test_enabling_source_after_disabled_run_is_incompatible(self):
        summary, code = await self.run_map(config=replace(self.config, provenance_path=None))
        self.assertEqual(code, 0)
        before = self.client.calls
        result, code = await self.run_map(action="resume", run_id=summary["run_id"])
        self.assertNotEqual(code, 0)
        self.assertIn("provenance", result["error"])
        self.assertEqual(self.client.calls, before)

    async def test_policy_change_is_rejected(self):
        summary, _ = await self.run_map()
        with patch("app.infrastructure.risk_map.config.FILENAME_POLICY", "incompatible-test-policy"):
            result, code = await self.run_map(action="resume", run_id=summary["run_id"])
        self.assertNotEqual(code, 0)
        self.assertIn("config_hash", result["error"])

    async def test_tree_and_source_bytes_read_once(self):
        calls = []
        original = Path.read_bytes
        def tracked(path):
            if path in {self.tree_path, self.prov_path}:
                calls.append(path)
            return original(path)
        with patch.object(Path, "read_bytes", tracked):
            summary, code = await self.run_map()
        self.assertEqual(code, 0)
        self.assertEqual(calls.count(self.tree_path), 1)
        self.assertEqual(calls.count(self.prov_path), 1)


if __name__ == "__main__":
    unittest.main()
