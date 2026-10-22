"""Index the existing four-level tree without interpreting clause semantics."""

from __future__ import annotations

import hashlib
import re
import copy

from .config import REGIONS, RISK_CODES
from .document_identity import normalize_document_name, revision_family
from ..common.models import EvidencePool, FileAttachment, IndicatorContext, RiskMapInputError, TreeIndex
from ..common.stats import Diagnostics

META = {"序号", "指标描述", "映射文件"}
LEAF_FIELDS = ("原始文本", "已找到", "知识依据", "依据文件名")


def build_tree_index(tree: dict, diagnostics: Diagnostics) -> TreeIndex:
    if not isinstance(tree, dict) or not tree:
        raise RiskMapInputError("指标树根必须是非空对象")
    index = TreeIndex()

    def visit(name, node, risk, depth, parent, path):
        if not isinstance(node, dict):
            raise RiskMapInputError(f"指标节点不是对象: {path}/{name}")
        code = node.get("序号")
        if not isinstance(code, str) or not re.fullmatch(r"\d+(?:\.\d+)*", code.strip()):
            raise RiskMapInputError(f"正式序号不合法: {path}/{name}")
        code = code.strip()
        if len(code.split(".")) != depth + 1 or (parent and code.rsplit(".", 1)[0] != parent):
            raise RiskMapInputError(f"指标层级或父编号不一致: {name} ({code})")
        if depth == 0 and code != RISK_CODES[risk]:
            raise RiskMapInputError(f"风险根编号不一致: {risk} ({code})")
        here = path + ((name, code),)
        if depth < 3:
            if "映射文件" in node or "指标描述" in node:
                raise RiskMapInputError(f"非三级节点包含三级属性: {name}")
            children = [(k, v) for k, v in node.items() if k not in META]
            if not children:
                raise RiskMapInputError(f"指标父节点没有孩子: {name}")
            for k, v in children:
                visit(k, v, risk, depth + 1, code, here)
            return
        if code in index.indicators:
            raise RiskMapInputError(f"三级指标正式序号重复: {code}")
        description = node.get("指标描述")
        mapping = node.get("映射文件")
        if not isinstance(description, str) or not description.strip() or not isinstance(mapping, dict):
            raise RiskMapInputError(f"三级指标缺少描述或合法映射文件: {code}")
        if set(node) - META:
            raise RiskMapInputError(f"三级节点有意外孩子: {code}")
        indicator = IndicatorContext(code, name, description, risk, here)
        index.indicators[code] = indicator
        for region, files in mapping.items():
            if region not in REGIONS or not isinstance(files, dict):
                raise RiskMapInputError(f"地区或文件集合不合法: {code}/{region}")
            for title, leaf in files.items():
                if not isinstance(title, str) or not title.strip() or not isinstance(leaf, dict):
                    raise RiskMapInputError(f"文件叶节点不合法: {code}/{region}/{title}")
                if any(not isinstance(leaf.get(k), str) for k in LEAF_FIELDS):
                    raise RiskMapInputError(f"文件叶字段缺失或不是字符串: {code}/{region}/{title}")
                canonical = leaf["依据文件名"].strip() or title
                identity = (region, normalize_document_name(canonical))
                if not identity[1]:
                    raise RiskMapInputError(f"空文件身份: {code}/{title}")
                attachment = FileAttachment(
                    code, risk, region, title, leaf["依据文件名"], leaf["原始文本"],
                    leaf["已找到"], leaf["知识依据"],
                    tuple(n for n, _ in here) + ("映射文件", region, title), identity,
                )
                aid = len(index.attachments)
                index.attachments.append(attachment)
                indicator.attachment_ids.append(aid)
                index.files.setdefault(identity, []).append(aid)
                for filename in {title, leaf["依据文件名"]} - {""}:
                    key = (region, normalize_document_name(filename))
                    index.names.setdefault(key, set()).add(identity)
                content = leaf["知识依据"].replace("\r\n", "\n").replace("\r", "\n").strip()
                if content:
                    digest = hashlib.sha256(content.encode("utf-8")).hexdigest()
                    poolkey = (*identity, risk, digest)
                    pool = index.pools.setdefault(poolkey, EvidencePool(identity, risk, digest, content))
                    pool.attachment_ids.append(aid)
                    pool.indicator_numbers.add(code)

    for risk, node in tree.items():
        if risk not in RISK_CODES:
            raise RiskMapInputError(f"未知风险根: {risk}")
        visit(risk, node, risk, 0, "", ())
    for key, identities in index.names.items():
        index.families.setdefault((key[0], revision_family(key[1])[0]), set()).update(identities)
        if len(identities) > 1:
            diagnostics.add("tree_name_collision", "名称对应多个文件身份", "strong_warning", name=key)
    return index


def attach_provenance(index: TreeIndex, provenance: dict, diagnostics: Diagnostics) -> None:
    if not isinstance(provenance, dict) or not isinstance(provenance.get("指标文件挂接"), list):
        diagnostics.add("provenance_invalid", "可选来源结构不合法，主树保持权威")
        return
    lookup = {}
    for row in provenance["指标文件挂接"]:
        if isinstance(row, dict):
            lookup.setdefault((row.get("指标序号"), row.get("地区"), row.get("文件名称")), []).append(row)
    sources = provenance.get("来源单元格", {})
    for a in index.attachments:
        rows = lookup.get((a.indicator_number, a.region, a.mapped_name), [])
        valid = False
        for row in rows:
            ids = row.get("采用来源ID", [])
            if not isinstance(ids, list) or any(not isinstance(sid, str) for sid in ids):
                continue
            values = []
            for sid in ids:
                record = sources.get(sid) if isinstance(sources, dict) else None
                if (not isinstance(record, dict) or record.get("风险类型") != a.risk_type
                        or record.get("地区") != a.region
                        or not isinstance(record.get("文件名称"), str)
                        or normalize_document_name(record["文件名称"]) != a.identity[1]):
                    break
                value = record.get("采用知识文本", "")
                if not isinstance(value, str):
                    break
                if value.strip() and value.strip() not in values:
                    values.append(value.strip())
            else:
                if "\n\n".join(values).replace("\r\n", "\n").strip() == a.knowledge.replace("\r\n", "\n").strip():
                    a.provenance.append(copy.deepcopy(row))
                    valid = True
        if not valid:
            diagnostics.add("provenance_mismatch", "来源明细与树挂接或正文不一致", path=a.tree_path)
