"""Index the existing four-level tree without interpreting clause semantics."""

from __future__ import annotations

import hashlib
import re
from collections import Counter

from .config import REGIONS, RISK_CODES
from .document_identity import normalize_document_name, revision_family
from ..common.models import EvidencePool, FileAttachment, IndicatorContext, NameBinding, RiskMapInputError, TreeIndex
from ..common.stats import Diagnostics

META = {"序号", "指标描述", "映射文件"}
LEAF_FIELDS = ("原始文本", "已找到", "知识依据", "依据文件名")


def clean_aliases(value, *, context, diagnostics):
    if not isinstance(value, list) or any(not isinstance(name, str) for name in value):
        raise RiskMapInputError(f"别名必须为字符串数组: {context}")
    result, seen = [], set()
    for name in value:
        normalized = normalize_document_name(name)
        if not normalized:
            diagnostics.add("alias_blank", "忽略空白别名", path=context)
        elif normalized in seen:
            diagnostics.add("alias_duplicate", "忽略规范化重复别名", path=context)
        else:
            seen.add(normalized)
            result.append(name.strip())
    return result


def build_tree_index(tree: dict, diagnostics: Diagnostics) -> TreeIndex:
    if not isinstance(tree, dict) or not tree:
        raise RiskMapInputError("指标树根必须是非空对象")
    index = TreeIndex()
    stats = Counter()

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
                aliases = clean_aliases(leaf.get("别名", []), context=f"{code}/{region}/{title}", diagnostics=diagnostics)
                stats["new_leaves" if "别名" in leaf else "legacy_leaves"] += 1
                stats["alias_attachments"] += bool(aliases)
                stats["aliases"] += len(aliases)
                canonical = leaf["依据文件名"].strip() or title
                identity = (region, normalize_document_name(canonical))
                if not identity[1]:
                    raise RiskMapInputError(f"空文件身份: {code}/{title}")
                attachment = FileAttachment(
                    indicator_number=code, risk_type=risk, region=region, mapped_name=title,
                    basis_name=leaf["依据文件名"], original_text=leaf["原始文本"],
                    found_filename=leaf["已找到"], knowledge=leaf["知识依据"],
                    tree_path=tuple(n for n, _ in here) + ("映射文件", region, title),
                    identity=identity, aliases=aliases,
                )
                aid = len(index.attachments)
                index.attachments.append(attachment)
                indicator.attachment_ids.append(aid)
                index.files.setdefault(identity, []).append(aid)
                for filename, role in [(title, "primary"), (leaf["依据文件名"], "basis")] + [(n, "alias") for n in aliases]:
                    if not filename.strip():
                        continue
                    key = (region, normalize_document_name(filename))
                    index.names.setdefault(key, set()).add(identity)
                    binding = NameBinding(aid, region, identity, role, filename, key[1])
                    index.name_bindings.setdefault(key, []).append(binding)
                    index.family_bindings.setdefault((region, revision_family(key[1])[0]), []).append(binding)
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
    index.input_stats = {**dict(stats), "indicators": len(index.indicators), "attachments": len(index.attachments)}
    index.input_stats["provenance"] = {"enabled": False, "verified": 0, "unverified": len(index.attachments)}
    return index


def attach_provenance(index: TreeIndex, provenance: dict, diagnostics: Diagnostics) -> None:
    from .provenance import verify_provenance
    return verify_provenance(index, provenance, diagnostics)
