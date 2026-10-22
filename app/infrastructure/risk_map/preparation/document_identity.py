"""Filename identities; explicit revision suffixes form families, not latest-version proofs."""

import ntpath
import re
import unicodedata


def normalize_document_name(value):
    name = unicodedata.normalize("NFKC", value.strip())
    explicit_path = bool(re.match(r"^(?:[A-Za-z]:[\\/]|[\\/]|\.{1,2}[\\/])", name) or "\\" in name)
    legal_slash = bool(re.search(r"\b(?:GB|SJ|JB)/[TZ]|\d+/\d+|/(?:EU|EC|EEC)\b", name, re.I))
    relative_path = "/" in name and not legal_slash and not re.search(r"\s", name) and bool(
        re.search(r"\.(?:txt|md|pdf|docx?|json)$", name, re.I))
    if explicit_path or relative_path:
        name = ntpath.basename(name.replace("/", "\\"))
    name = re.sub(r"\.(?:txt|md|pdf|doc|docx|docs|json)$", "", name, flags=re.I)
    name = name.translate(str.maketrans({"—": "-", "–": "-", "﹣": "-"}))
    return re.sub(r"\s+", "", name).replace("《", "").replace("》", "").casefold()


def revision_family(name):
    # Do not remove identity qualifiers such as 试行/暂行/草案, even when followed by a year.
    pattern = r"(?:\((?!(?:[^)]*(?:试行|暂行|草案)))(?:(?:19|20)\d{2}(?:年)?(?:修订|修正|修改|改编|版)?|(?:修订|修正|修改|改编)(?:(?:19|20)\d{2}年?)?)\)|[-](?:19|20)\d{2})$"
    annotations = []
    while match := re.search(pattern, name):
        if match.group().startswith("-") and re.match(r"^(?:gb|sj|jb|hg|ny|hj|db\d*)(?:/[tz])?\d", name):
            break
        annotations.insert(0, match.group())
        name = name[:match.start()]
    return name, tuple(annotations)


def country_variants(name, region):
    if region != "中国":
        return ()
    prefix = "中华人民共和国"
    variant = name[len(prefix):] if name.startswith(prefix) else prefix + name
    return (variant,) if variant else ()


def country_family(name, region):
    family = revision_family(name)[0]
    prefix = "中华人民共和国"
    return family[len(prefix):] if region == "中国" and family.startswith(prefix) else family
