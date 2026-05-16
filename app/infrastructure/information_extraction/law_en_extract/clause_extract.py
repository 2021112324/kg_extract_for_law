import asyncio
import json
import logging
import os
import re
from typing import Optional

from app.infrastructure.information_extraction.base import Entity, Relationship
from app.infrastructure.information_extraction.law_en_extract.prompt.example import (
    example_for_clause,
    example_for_file_info,
)
from app.infrastructure.information_extraction.law_en_extract.prompt.prompt import (
    prompt_for_clause,
    prompt_for_file_info,
)
from app.infrastructure.information_extraction.law_en_extract.prompt.schema import (
    schema_for_clause,
    schema_for_file_info,
)
from app.infrastructure.string_utils.id_tool import generate_hex_uuid
from app.infrastructure.string_utils.str_clean import (
    clean_string_for_neo4j_extended,
    clean_string_with_only_words,
    replace_full_corner_space,
    replace_zero_width_chars,
)

LAW_EN_CLAUSE_MODEL = os.getenv("LAW_EN_CLAUSE_MODEL", "qwen3-30b-a3b-instruct-2507")
LAW_EN_CLAUSE_MODEL_API_KEY = os.getenv(
    "LAW_EN_CLAUSE_MODEL_API_KEY",
    "gpustack_342609ce423be29a_4371426b285a91dc44fb4e8d72454847",
)
LAW_EN_CLAUSE_MODEL_API_URL = os.getenv(
    "LAW_EN_CLAUSE_MODEL_API_URL",
    "http://222.171.219.26:20001/v1/chat/completions",
)

MAX_CHAR_BUFFER = int(os.getenv("LAW_EN_MAX_CHAR_BUFFER", "7500"))
BATCH_LENGTH = int(os.getenv("LAW_EN_BATCH_LENGTH", "5"))
MAX_WORKERS = int(os.getenv("LAW_EN_MAX_WORKERS", "3"))
TIMEOUT = int(os.getenv("LAW_EN_TIMEOUT", "3000"))

HIERARCHY_LEVELS = [
    "title",
    "subtitle",
    "chapter",
    "subchapter",
    "part",
    "subpart",
    "division",
    "article",
]

HIERARCHY_PATTERNS = [
    ("title", re.compile(r"^(Title\s+[\w.\-]+(?:\s*[.\-\u2013\u2014:]\s*|\s+).*)$", re.IGNORECASE)),
    ("subtitle", re.compile(r"^(Subtitle\s+[\w.\-]+(?:\s*[.\-\u2013\u2014:]\s*|\s+).*)$", re.IGNORECASE)),
    ("chapter", re.compile(r"^(Chapter\s+[\w.\-]+(?:\s*[.\-\u2013\u2014:]\s*|\s+).*)$", re.IGNORECASE)),
    ("subchapter", re.compile(r"^(Subchapter\s+[\w.\-]+(?:\s*[.\-\u2013\u2014:]\s*|\s+).*)$", re.IGNORECASE)),
    ("part", re.compile(r"^(Part\s+[\w.\-]+(?:\s*[.\-\u2013\u2014:]\s*|\s+).*)$", re.IGNORECASE)),
    ("subpart", re.compile(r"^(Subpart\s+[\w.\-]+(?:\s*[.\-\u2013\u2014:]\s*|\s+).*)$", re.IGNORECASE)),
    ("division", re.compile(r"^(Division\s+[\w.\-]+(?:\s*[.\-\u2013\u2014:]\s*|\s+).*)$", re.IGNORECASE)),
]

SECTION_PATTERN = re.compile(
    r"^(?:(?P<section_symbol>[\u00a7\u6402])\s*(?P<section_number>[\dA-Za-z](?:[\dA-Za-z.\-:]*[\dA-Za-z])?)|"
    r"(?P<section_label>Section|Sec\.?|Article|Art\.?)\s*(?P<label_number>[\dA-Za-z](?:[\dA-Za-z.\-:]*[\dA-Za-z])?))"
    r"(?:\s*[.\-\u2013\u2014:]\s*|\s+)(?P<section_heading>.+)$",
    re.IGNORECASE,
)

BARE_SECTION_PATTERN = re.compile(
    r"^(?:(?P<section_symbol>[\u00a7\u6402])\s*(?P<section_number>[\dA-Za-z](?:[\dA-Za-z.\-:]*[\dA-Za-z])?)|"
    r"(?P<section_label>Section|Sec\.?|Article|Art\.?)\s*(?P<label_number>[\dA-Za-z](?:[\dA-Za-z.\-:]*[\dA-Za-z])?))\.?$",
    re.IGNORECASE,
)

END_MARKERS = (
    "Appendix",
    "Attachment",
    "Annex",
    "Schedule",
    "Table of Contents",
    "Historical and Statutory Notes",
    "Editorial Notes",
    "Credits",
    "References",
    "References in Text",
    "Codification",
    "Amendments",
    "Effective Date",
    "Index",
)

ENTITY_TYPE_ALIASES = {
    "legaldocument": "Legal Document",
    "document": "Legal Document",
    "statute": "Legal Document",
    "act": "Legal Document",
    "legalbasis": "Legal Basis",
    "basis": "Legal Basis",
    "authority": "Legal Basis",
    "legalprovision": "Legal Provision",
    "provision": "Legal Provision",
    "section": "Legal Provision",
    "article": "Legal Provision",
    "provisionunit": "Provision Unit",
    "unit": "Provision Unit",
    "subsection": "Provision Unit",
    "paragraph": "Provision Unit",
    "citation": "Citation",
    "reference": "Citation",
    "legalcitation": "Citation",
}

RELATION_TYPE_ALIASES = {
    "contains": "CONTAINS",
    "contain": "CONTAINS",
    "includes": "CONTAINS",
    "includedin": "CONTAINS",
    "basedon": "BASED_ON",
    "basis": "BASED_ON",
    "pursuantto": "BASED_ON",
    "cites": "CITES",
    "cite": "CITES",
    "references": "CITES",
    "reference": "CITES",
    "refersto": "CITES",
}

YES_VALUES = {"yes", "y", "true", "internal", "same document", "this document"}


class ResultStats:
    def __init__(self):
        self.error = 0
        self.error_msg = ""
        self.week_warning = 0
        self.week_warning_msg = ""
        self.strong_warning = 0
        self.strong_warning_msg = ""


class ClauseCache:
    def __init__(self):
        self.file_info = {}
        self.clause_cache = {}


def clean_string(text: str) -> str:
    if text is None:
        return ""
    cleaned = str(text)
    cleaned = cleaned.replace("\u6402", "Section")
    cleaned = replace_full_corner_space(cleaned)
    cleaned = replace_zero_width_chars(cleaned)
    cleaned = re.sub(r"\r\n|\r", "\n", cleaned)
    cleaned = re.sub(r"\n+", "\n", cleaned)
    cleaned = re.sub(r"^[ \t]+|[ \t]+$", "", cleaned, flags=re.MULTILINE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def clean_line(line: str) -> str:
    return line.strip().lstrip(" \t\r\n\f\v#-*")


def normalize_text_key(value: str) -> str:
    if value is None:
        return ""
    normalized = str(value)
    normalized = normalized.replace("\u00a7", " Section ")
    normalized = normalized.replace("\u6402", " Section ")
    normalized = re.sub(r"\bsecs?\.", "section", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"\bsections\b", "section", normalized, flags=re.IGNORECASE)
    normalized = clean_string_with_only_words(normalized).lower()
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def canonical_type(entity_type: str) -> str:
    type_key = re.sub(r"[^a-z0-9]+", "", str(entity_type or "").lower())
    return ENTITY_TYPE_ALIASES.get(type_key, entity_type or "")


def canonical_relation_type(relation_type: str) -> str:
    relation_key = re.sub(r"[^a-z0-9]+", "", str(relation_type or "").lower())
    if relation_key in RELATION_TYPE_ALIASES:
        return RELATION_TYPE_ALIASES[relation_key]
    return str(relation_type or "").strip().upper().replace(" ", "_")


def is_internal_reference(properties: dict) -> bool:
    value = (
        properties.get("is_internal_reference")
        or properties.get("internal_reference")
        or properties.get("same_document")
        or ""
    )
    return str(value).strip().lower() in YES_VALUES


def match_hierarchy(line: str) -> Optional[tuple[str, str]]:
    for level, pattern in HIERARCHY_PATTERNS:
        match = pattern.match(line)
        if match:
            return level, match.group(1).strip()
    return None


def match_section(line: str) -> Optional[re.Match]:
    section_match = SECTION_PATTERN.match(line)
    if section_match:
        return section_match
    return BARE_SECTION_PATTERN.match(line)


def update_hierarchy(hierarchy: dict, level: str, value: str):
    hierarchy[level] = value
    level_idx = HIERARCHY_LEVELS.index(level)
    for child_level in HIERARCHY_LEVELS[level_idx + 1:]:
        hierarchy[child_level] = ""


def build_clause(
        hierarchy: dict,
        section_number: str,
        section_heading: str,
        clause_content: str
) -> dict:
    classification = {
        key: clean_string(value)
        for key, value in hierarchy.items()
        if value
    }
    return {
        "title": classification.get("title", ""),
        "subtitle": classification.get("subtitle", ""),
        "chapter": classification.get("chapter", ""),
        "subchapter": classification.get("subchapter", ""),
        "part": classification.get("part", ""),
        "subpart": classification.get("subpart", ""),
        "division": classification.get("division", ""),
        "article": classification.get("article", ""),
        "classification": classification,
        "section_number": clean_string(section_number),
        "section_heading": clean_string(section_heading),
        "clause_content": clean_string(clause_content),
    }


def parse_section_number(section_match: re.Match) -> str:
    if section_match.groupdict().get("section_symbol"):
        return f"Section {section_match.group('section_number')}"

    label = section_match.group("section_label") or "Section"
    number = section_match.group("label_number")
    if label.lower().startswith("art"):
        return f"Article {number}"
    return f"Section {number}"


def split_clause(text: str) -> dict:
    """
    Split English legal text into section/article-level clause data.

    Output structure:
    {
        "file_info": "opening text",
        "clauses": [
            {
                "title": "Title 50. War and National Defense",
                "chapter": "Chapter 35. International Emergency Economic Powers",
                "classification": {...},
                "section_number": "Section 1701",
                "section_heading": "Unusual and extraordinary threat",
                "clause_content": "section content without the leading section number"
            }
        ]
    }
    """
    lines = text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    hierarchy = {level: "" for level in HIERARCHY_LEVELS}
    clauses = []

    first_section_idx = None
    for i, raw_line in enumerate(lines):
        line = clean_line(raw_line)
        if not line:
            continue

        if match_section(line):
            first_section_idx = i
            break

        hierarchy_match = match_hierarchy(line)
        if hierarchy_match:
            level, value = hierarchy_match
            update_hierarchy(hierarchy, level, value)

    if first_section_idx is None:
        raise ValueError("No English section or article heading found in text")

    file_info = "\n".join(lines[:first_section_idx]).rstrip()
    current_hierarchy = hierarchy.copy()
    current_section_number = ""
    current_section_heading = ""
    current_clause_content = ""

    def append_current_clause():
        if not current_section_number:
            return
        content = current_clause_content.strip()
        if not content and current_section_heading:
            content = current_section_heading
        clauses.append(
            build_clause(
                hierarchy=current_hierarchy,
                section_number=current_section_number,
                section_heading=current_section_heading,
                clause_content=content,
            )
        )

    for raw_line in lines[first_section_idx:]:
        line = clean_line(raw_line)
        if not line:
            continue

        if any(line.startswith(marker) for marker in END_MARKERS):
            append_current_clause()
            current_section_number = ""
            current_section_heading = ""
            current_clause_content = ""
            break

        section_match = match_section(line)
        if section_match:
            append_current_clause()
            current_section_number = parse_section_number(section_match)
            current_section_heading = (section_match.groupdict().get("section_heading") or "").strip()
            current_clause_content = current_section_heading
            continue

        hierarchy_match = match_hierarchy(line)
        if hierarchy_match:
            append_current_clause()
            current_section_number = ""
            current_section_heading = ""
            current_clause_content = ""
            level, value = hierarchy_match
            update_hierarchy(current_hierarchy, level, value)
            continue

        if current_section_number:
            if not current_section_heading:
                current_section_heading = line
                current_clause_content = line
            else:
                current_clause_content += "\n" + raw_line.strip()

    append_current_clause()

    if not clauses:
        raise ValueError("No English clause content found in text")

    file_info = clean_string(file_info)
    if clauses:
        file_info = clean_string(f"{file_info}\n{clauses[0]['clause_content']}")

    return {
        "file_info": file_info,
        "clauses": clauses,
    }


def save_clause_result(result: dict, output_dir: str, filename: str) -> str:
    os.makedirs(output_dir, exist_ok=True)
    base_name = os.path.splitext(os.path.basename(filename))[0]
    output_path = os.path.join(output_dir, f"{base_name}_clauses.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    return output_path


def split_clause_file(
        input_path: str,
        output_dir: str = os.path.join("data", "temp")
) -> tuple[dict, str]:
    with open(input_path, "r", encoding="utf-8") as f:
        text = f.read()
    result = split_clause(text)
    output_path = save_clause_result(result, output_dir, input_path)
    return result, output_path


class ClauseExtractor:
    def __init__(
            self,
            max_concurrent: int = 50,
            fallback_on_llm_failure: bool = True,
    ):
        self.extractor_config = None
        self.extractor = None
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.result_stats = ResultStats()
        self.lenient_mode = False
        self.fallback_on_llm_failure = fallback_on_llm_failure

    def _get_extractor(self):
        if self.extractor is None:
            from app.infrastructure.information_extraction.factory import InformationExtractionFactory
            from app.infrastructure.information_extraction.method.base import LangextractConfig

            if self.extractor_config is None:
                self.extractor_config = LangextractConfig(
                    model_name=LAW_EN_CLAUSE_MODEL,
                    api_key=LAW_EN_CLAUSE_MODEL_API_KEY,
                    api_url=LAW_EN_CLAUSE_MODEL_API_URL,
                    config={
                        "timeout": TIMEOUT,
                    },
                    max_char_buffer=MAX_CHAR_BUFFER,
                    batch_length=BATCH_LENGTH,
                    max_workers=MAX_WORKERS,
                )

            self.extractor = InformationExtractionFactory.create(
                "langextract",
                max_retries=5,
                config=self.extractor_config,
            )
        return self.extractor

    async def extract_clauses(
            self,
            filename: str,
            text: str
    ) -> dict:
        """
        Extract an English legal-provision knowledge graph from a legal document.
        The returned graph uses English node types, relation types and property keys.
        """
        try:
            logging.info("Starting English legal provision graph extraction")
            clauses_data = await self.split_clause(text)

            clause_cache = ClauseCache()
            file_info = clauses_data.get("file_info")
            if not file_info:
                self.result_stats.error += 1
                self.result_stats.error_msg += "File information is empty\n"
                raise ValueError("File information is empty")

            file_info_result = await self.kg_extract_from_file_info(
                filename=filename,
                clause_cache=clause_cache,
                file_info=file_info,
            )

            clauses = clauses_data.get("clauses")
            if not clauses:
                self.result_stats.error += 1
                self.result_stats.error_msg += "Clause data is empty\n"
                raise ValueError("Clause data is empty")

            tasks = [
                self.kg_extract_from_clause(
                    filename=filename,
                    clause_cache=clause_cache,
                    one_clause=clause,
                )
                for clause in clauses
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            failed_results = []
            failed_clauses = []
            successful_results = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    self.result_stats.error += 1
                    self.result_stats.error_msg += f"Clause {i + 1} failed: {result}\n"
                    failed_results.append((i + 1, result))
                    failed_clauses.append(clauses[i])
                elif result:
                    successful_results.append(result)
                else:
                    self.result_stats.error += 1
                    self.result_stats.error_msg += f"Clause {i + 1} returned empty result\n"
                    failed_clauses.append(clauses[i])

            if failed_results and not self.lenient_mode:
                raise ValueError("Some English legal provisions failed to process")

            return await self.process_extracted_data(
                filename=filename,
                extracted_file_info=file_info_result,
                extracted_success_clauses=successful_results,
                extracted_failed_clauses=failed_clauses,
            )
        except Exception as e:
            logging.error("English legal provision graph extraction failed: %s", e)
            raise

    async def split_clause(self, text: str) -> dict:
        return split_clause(text)

    async def split_clause_to_json(
            self,
            input_path: str,
            output_dir: str = os.path.join("data", "temp")
    ) -> dict:
        result, output_path = split_clause_file(input_path, output_dir)
        return {
            "output_path": output_path,
            **result,
        }

    async def kg_extract_from_file_info(
            self,
            filename: str,
            clause_cache: ClauseCache,
            file_info: str
    ) -> dict:
        try:
            file_info_result = {
                "node_id": "",
                "node_name": "",
                "node_type": "",
                "properties": {},
                "legal_basis": [],
            }

            extract_result = await self._get_extractor().entity_and_relationship_extract(
                user_prompt=prompt_for_file_info,
                schema=schema_for_file_info,
                input_text=f"{filename} document description:\n{file_info}",
                examples=example_for_file_info,
            )

            entities = extract_result.get("entities", []) if extract_result else []
            file_info_processed = False
            for entity in entities:
                if not isinstance(entity, Entity):
                    self.result_stats.error += 1
                    self.result_stats.error_msg += f"Invalid entity type: {entity}\n"
                    continue

                entity_type = canonical_type(entity.entity_type)
                if entity_type == "Legal Document":
                    if file_info_processed:
                        self.result_stats.strong_warning += 1
                        self.result_stats.strong_warning_msg += (
                            f"Only one Legal Document is expected in file info: {entity}\n"
                        )
                        continue
                    node_name = clean_string(entity.name)
                    if not node_name:
                        continue
                    file_info_result["node_id"] = self._new_node_id(entity_type)
                    file_info_result["node_name"] = node_name
                    file_info_result["node_type"] = entity_type
                    file_info_result["properties"] = entity.properties
                    file_info_processed = True
                elif entity_type == "Legal Basis":
                    node_name = clean_string(entity.name)
                    if not node_name:
                        continue
                    file_info_result["legal_basis"].append(
                        {
                            "node_id": self._new_node_id(entity_type),
                            "node_name": node_name,
                            "node_type": entity_type,
                            "properties": entity.properties,
                        }
                    )
                elif entity_type:
                    self.result_stats.strong_warning += 1
                    self.result_stats.strong_warning_msg += f"Unknown file-info entity type: {entity}\n"

            if not file_info_result["node_id"]:
                if not self.fallback_on_llm_failure:
                    raise ValueError("Legal Document entity was not extracted")
                file_info_result = self._fallback_file_info(filename, file_info)

            clause_cache.file_info = file_info_result
            return file_info_result
        except Exception as e:
            if self.fallback_on_llm_failure:
                logging.warning("Falling back to deterministic file-info extraction: %s", e)
                file_info_result = self._fallback_file_info(filename, file_info)
                clause_cache.file_info = file_info_result
                return file_info_result
            self.result_stats.error += 1
            self.result_stats.error_msg += f"File-info extraction failed: {e}\n{file_info[:500]}\n"
            raise ValueError(f"File-info extraction failed: {e}") from e

    async def kg_extract_from_clause(
            self,
            filename: str,
            clause_cache: ClauseCache,
            one_clause: dict
    ) -> dict:
        async with self.semaphore:
            try:
                clause_result = {
                    "node_id": "",
                    "node_name": "",
                    "node_type": "",
                    "properties": {},
                    "provision_units": [],
                }

                section_number = one_clause.get("section_number", "")
                section_heading = one_clause.get("section_heading", "")
                clause_content = one_clause.get("clause_content", "")
                if not section_number:
                    raise ValueError(f"Section number is empty: {one_clause}")

                extract_content = self._build_clause_extract_content(filename, one_clause)
                extract_result = await self._get_extractor().entity_and_relationship_extract(
                    user_prompt=prompt_for_clause,
                    schema=schema_for_clause,
                    input_text=extract_content,
                    examples=example_for_clause,
                )

                clause_units = {}
                citations = {}
                processed_citation_keys = set()
                entities = extract_result.get("entities", []) if extract_result else []
                relations = extract_result.get("relations", []) if extract_result else []
                provision_processed = False

                for entity in entities:
                    if not isinstance(entity, Entity):
                        self.result_stats.error += 1
                        self.result_stats.error_msg += f"Invalid entity type: {entity}\n"
                        continue

                    entity_type = canonical_type(entity.entity_type)
                    node_name = clean_string(entity.name)
                    if not node_name:
                        continue

                    if entity_type == "Legal Provision":
                        if provision_processed:
                            self.result_stats.strong_warning += 1
                            self.result_stats.strong_warning_msg += (
                                f"Only one Legal Provision is expected per section: {entity}\n"
                            )
                            continue
                        clause_result["node_id"] = self._new_node_id(entity_type)
                        clause_result["node_name"] = section_number
                        clause_result["node_type"] = entity_type
                        clause_result["properties"] = entity.properties
                        clause_result["properties"]["section_number"] = section_number
                        clause_result["properties"]["section_heading"] = section_heading
                        clause_result["properties"]["provision_text"] = clause_content
                        self._add_classification_properties(clause_result["properties"], one_clause)
                        provision_processed = True
                    elif entity_type == "Provision Unit":
                        node_id = self._new_node_id(entity_type)
                        unit_key = f"{entity_type}_{node_name}"
                        clause_units[unit_key] = {
                            "node_id": node_id,
                            "node_name": node_name,
                            "node_type": entity_type,
                            "properties": entity.properties,
                            "internal_citations": [],
                            "external_citations": [],
                        }
                    elif entity_type == "Citation":
                        node_id = self._new_node_id(entity_type)
                        citation_key = f"{entity_type}_{node_name}"
                        citations[citation_key] = {
                            "node_id": node_id,
                            "node_name": node_name,
                            "node_type": entity_type,
                            "properties": entity.properties,
                        }
                    elif entity_type:
                        self.result_stats.error += 1
                        self.result_stats.error_msg += f"Unknown clause entity type: {entity}\n"

                for relation in relations:
                    if not isinstance(relation, Relationship):
                        self.result_stats.error += 1
                        self.result_stats.error_msg += f"Invalid relation type: {relation}\n"
                        continue

                    relation_type = canonical_relation_type(relation.type)
                    if relation_type != "CITES":
                        continue

                    source_entity = self._lookup_entity_by_key(clause_units, relation.source)
                    target_entity = self._lookup_entity_by_key(citations, relation.target)
                    if not source_entity or not target_entity:
                        self.result_stats.strong_warning += 1
                        self.result_stats.strong_warning_msg += f"Unmatched citation relation: {relation}\n"
                        continue

                    if is_internal_reference(target_entity.get("properties", {})):
                        source_entity["internal_citations"].append(target_entity)
                    else:
                        source_entity["external_citations"].append(target_entity)
                    processed_citation_keys.add(self._matched_key(citations, target_entity))

                unused_citations = set(citations.keys()) - processed_citation_keys
                for key in unused_citations:
                    self.result_stats.strong_warning += 1
                    self.result_stats.strong_warning_msg += f"Citation entity was not used by a CITES relation: {key}\n"

                if not clause_result["node_id"]:
                    if not self.fallback_on_llm_failure:
                        raise ValueError(f"Legal Provision was not extracted for {section_number}")
                    clause_result = self._fallback_clause(filename, one_clause)
                else:
                    if not clause_units and self.fallback_on_llm_failure:
                        fallback_units = self._fallback_provision_units(filename, one_clause)
                        clause_units = {
                            f"{unit['node_type']}_{unit['node_name']}": unit
                            for unit in fallback_units
                        }
                    clause_result["provision_units"] = list(clause_units.values())

                clause_cache.clause_cache[section_number] = clause_result
                return clause_result
            except Exception as e:
                if self.fallback_on_llm_failure:
                    logging.warning("Falling back to deterministic clause extraction: %s", e)
                    fallback_result = self._fallback_clause(filename, one_clause)
                    clause_cache.clause_cache[one_clause.get("section_number", "")] = fallback_result
                    return fallback_result
                self.result_stats.error += 1
                self.result_stats.error_msg += f"Clause extraction failed for {filename}: {e}\n"
                raise ValueError(f"Clause extraction failed for {filename}: {e}") from e

    async def process_extracted_data(
            self,
            filename: str,
            extracted_file_info: dict,
            extracted_success_clauses: list[dict],
            extracted_failed_clauses: list[dict]
    ) -> dict:
        try:
            final_kg = {
                "nodes": [],
                "edges": [],
            }

            file_node_id = extracted_file_info.get("node_id")
            file_node_name = extracted_file_info.get("node_name")
            file_node_type = extracted_file_info.get("node_type")
            if not file_node_id or not file_node_name or not file_node_type:
                raise ValueError(f"Legal Document information is incomplete: {extracted_file_info}")

            final_kg["nodes"].append(
                {
                    "node_id": file_node_id,
                    "node_name": file_node_name,
                    "node_type": file_node_type,
                    "properties": extracted_file_info.get("properties", {}),
                    "filename": filename,
                }
            )

            for basis in extracted_file_info.get("legal_basis", []):
                basis_node_id = basis.get("node_id")
                basis_node_name = basis.get("node_name")
                basis_node_type = basis.get("node_type")
                if not basis_node_id or not basis_node_name or not basis_node_type:
                    self.result_stats.strong_warning += 1
                    self.result_stats.strong_warning_msg += f"Legal Basis is incomplete: {basis}\n"
                    continue
                final_kg["nodes"].append(
                    {
                        "node_id": basis_node_id,
                        "node_name": basis_node_name,
                        "node_type": basis_node_type,
                        "properties": basis.get("properties", {}),
                        "filename": filename,
                    }
                )
                final_kg["edges"].append(
                    self._build_edge(
                        source_id=file_node_id,
                        target_id=basis_node_id,
                        relation_type="BASED_ON",
                        filename=filename,
                    )
                )

            for clause in extracted_failed_clauses:
                fallback_clause = self._fallback_clause(filename, clause)
                extracted_success_clauses.append(fallback_clause)

            internal_reference_id_mapping = {}
            external_reference_id_mapping = {}
            unit_to_provision_mapping = {}
            internal_reference_mapping = {}
            external_reference_mapping = {}

            for clause in extracted_success_clauses:
                provision_node_id = clause.get("node_id")
                provision_node_name = clause.get("node_name")
                provision_node_type = clause.get("node_type")
                if not provision_node_id or not provision_node_name or not provision_node_type:
                    self.result_stats.strong_warning += 1
                    self.result_stats.strong_warning_msg += f"Legal Provision is incomplete: {clause}\n"
                    continue

                provision_properties = clause.get("properties", {})
                final_kg["nodes"].append(
                    {
                        "node_id": provision_node_id,
                        "node_name": provision_node_name,
                        "node_type": provision_node_type,
                        "properties": provision_properties,
                        "filename": filename,
                    }
                )
                final_kg["edges"].append(
                    self._build_edge(
                        source_id=file_node_id,
                        target_id=provision_node_id,
                        relation_type="CONTAINS",
                        filename=filename,
                    )
                )

                for key in (
                    provision_node_name,
                    provision_properties.get("section_number"),
                    provision_properties.get("provision_number"),
                ):
                    normalized_key = normalize_text_key(key)
                    if normalized_key:
                        internal_reference_id_mapping[normalized_key] = provision_node_id

                for unit in clause.get("provision_units", []):
                    unit_node_id = unit.get("node_id")
                    unit_node_name = unit.get("node_name")
                    unit_node_type = unit.get("node_type")
                    if not unit_node_id or not unit_node_name or not unit_node_type:
                        self.result_stats.strong_warning += 1
                        self.result_stats.strong_warning_msg += f"Provision Unit is incomplete: {unit}\n"
                        continue

                    unit_properties = unit.get("properties", {})
                    final_kg["nodes"].append(
                        {
                            "node_id": unit_node_id,
                            "node_name": unit_node_name,
                            "node_type": unit_node_type,
                            "properties": unit_properties,
                            "filename": filename,
                        }
                    )
                    final_kg["edges"].append(
                        self._build_edge(
                            source_id=provision_node_id,
                            target_id=unit_node_id,
                            relation_type="CONTAINS",
                            filename=filename,
                        )
                    )

                    unit_to_provision_mapping[unit_node_id] = provision_node_id
                    for key in (
                        unit_node_name,
                        unit_properties.get("unit_number"),
                        unit_properties.get("provision_number"),
                    ):
                        normalized_key = normalize_text_key(key)
                        if normalized_key:
                            internal_reference_id_mapping[normalized_key] = unit_node_id

                    internal_reference_mapping.setdefault(unit_node_id, []).extend(
                        unit.get("internal_citations", [])
                    )
                    external_reference_mapping.setdefault(unit_node_id, []).extend(
                        unit.get("external_citations", [])
                    )

            for unit_node_id, internal_refs in internal_reference_mapping.items():
                for ref in internal_refs:
                    target_id = self._resolve_internal_reference(ref, internal_reference_id_mapping)
                    if not target_id:
                        self._append_unresolved_citation_node(
                            final_kg=final_kg,
                            source_id=unit_node_id,
                            citation=ref,
                            filename=filename,
                            id_mapping=external_reference_id_mapping,
                        )
                        continue
                    if target_id in {unit_node_id, unit_to_provision_mapping.get(unit_node_id)}:
                        continue
                    final_kg["edges"].append(
                        self._build_edge(
                            source_id=unit_node_id,
                            target_id=target_id,
                            relation_type="CITES",
                            filename=filename,
                        )
                    )

            for unit_node_id, external_refs in external_reference_mapping.items():
                for ref in external_refs:
                    self._append_unresolved_citation_node(
                        final_kg=final_kg,
                        source_id=unit_node_id,
                        citation=ref,
                        filename=filename,
                        id_mapping=external_reference_id_mapping,
                    )

            return final_kg
        except Exception as e:
            self.result_stats.error += 1
            self.result_stats.error_msg += f"Failed to process extracted data: {e}\n"
            raise

    @staticmethod
    async def _save_cache_to_json(
            cache_data: ClauseCache,
            output_dir,
            filename
    ):
        os.makedirs(output_dir, exist_ok=True)
        base_name = os.path.splitext(os.path.basename(filename))[0]
        output_path = os.path.join(output_dir, f"{base_name}_law_en_cache.json")
        payload = {
            "file_info": cache_data.file_info,
            "clause_cache": cache_data.clause_cache,
        }
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        return output_path

    @staticmethod
    async def _load_cache_from_json(filepath):
        with open(filepath, "r", encoding="utf-8") as f:
            payload = json.load(f)
        cache = ClauseCache()
        cache.file_info = payload.get("file_info", {})
        cache.clause_cache = payload.get("clause_cache", {})
        return cache

    async def logging_result_stats(self):
        logging.info("English legal extraction stats")
        logging.info("Errors: %s", self.result_stats.error)
        logging.info("Weak warnings: %s", self.result_stats.week_warning)
        logging.info("Strong warnings: %s", self.result_stats.strong_warning)

    @staticmethod
    def _new_node_id(entity_type: str) -> str:
        safe_type = entity_type.replace(" ", "_")
        return clean_string_for_neo4j_extended(f"{safe_type}_{generate_hex_uuid()}")

    @staticmethod
    def _matched_key(mapping: dict, value: dict) -> str:
        for key, item in mapping.items():
            if item is value:
                return key
        return ""

    @staticmethod
    def _lookup_entity_by_key(mapping: dict, key: str) -> Optional[dict]:
        if key in mapping:
            return mapping[key]
        normalized_key = normalize_text_key(key)
        for candidate_key, candidate_value in mapping.items():
            if normalize_text_key(candidate_key) == normalized_key:
                return candidate_value
        for candidate_key, candidate_value in mapping.items():
            candidate_name = candidate_key.split("_", 1)[-1]
            key_name = str(key or "").split("_", 1)[-1]
            if normalize_text_key(candidate_name) == normalize_text_key(key_name):
                return candidate_value
        return None

    @staticmethod
    def _build_edge(source_id: str, target_id: str, relation_type: str, filename: str) -> dict:
        return {
            "source_id": source_id,
            "target_id": target_id,
            "relation_type": relation_type,
            "directionality": "directed",
            "properties": {},
            "filename": filename,
        }

    @staticmethod
    def _add_classification_properties(properties: dict, one_clause: dict):
        for key in HIERARCHY_LEVELS:
            value = one_clause.get(key, "")
            if value:
                properties[key] = value
        if one_clause.get("classification"):
            properties["classification"] = one_clause.get("classification")

    @staticmethod
    def _build_clause_extract_content(filename: str, one_clause: dict) -> str:
        parts = [filename]
        for level in HIERARCHY_LEVELS:
            value = one_clause.get(level, "")
            if value:
                parts.append(value)
        parts.append(one_clause.get("section_number", ""))
        heading = one_clause.get("section_heading", "")
        if heading:
            parts.append(heading)
        return " ".join(parts) + ":\n" + one_clause.get("clause_content", "")

    def _fallback_file_info(self, filename: str, file_info: str) -> dict:
        title = self._infer_document_title(filename, file_info)
        return {
            "node_id": self._new_node_id("Legal Document"),
            "node_name": title,
            "node_type": "Legal Document",
            "properties": {
                "official_title": title,
                "document_number": "",
                "alias": "",
                "document_type": self._infer_document_type(title),
                "publication_effective_info": "",
                "purpose": "",
                "domain": "",
                "applicable_industry": "",
                "scope_of_application": "",
                "issuing_authority": "",
                "publication_date": "",
                "effective_date": "",
                "status": "",
                "source_summary": clean_string(file_info[:1000]),
            },
            "legal_basis": self._extract_legal_basis_from_file_info(file_info),
        }

    def _fallback_clause(self, filename: str, one_clause: dict) -> dict:
        section_number = one_clause.get("section_number", "")
        section_heading = one_clause.get("section_heading", "")
        clause_content = one_clause.get("clause_content", "")
        properties = {
            "core_topic": section_heading,
            "scope_of_effect": "",
            "applicable_industry": "",
            "section_number": section_number,
            "section_heading": section_heading,
            "provision_text": clause_content,
        }
        self._add_classification_properties(properties, one_clause)
        return {
            "node_id": self._new_node_id("Legal Provision"),
            "node_name": section_number,
            "node_type": "Legal Provision",
            "properties": properties,
            "provision_units": self._fallback_provision_units(filename, one_clause),
        }

    def _fallback_provision_units(self, filename: str, one_clause: dict) -> list[dict]:
        section_number = one_clause.get("section_number", "")
        clause_content = one_clause.get("clause_content", "")
        unit_specs = self._split_top_level_units(section_number, clause_content)
        units = []
        for unit_number, unit_level, unit_content in unit_specs:
            citations = self._extract_citations(unit_content, filename)
            units.append(
                {
                    "node_id": self._new_node_id("Provision Unit"),
                    "node_name": unit_number,
                    "node_type": "Provision Unit",
                    "properties": {
                        "unit_content": clean_string(unit_content),
                        "unit_heading": "",
                        "unit_level": unit_level,
                        "unit_number": unit_number,
                        "applicable_industry": "",
                        "function_type": self._infer_function_type(unit_content),
                        "applicable_subject": "",
                        "responsible_role": "",
                        "conduct_description": "",
                        "condition": "",
                        "legal_consequence": "",
                        "exception": "",
                        "time_element": "",
                        "quantitative_standard": "",
                        "other_information": "",
                    },
                    "internal_citations": [citation for citation in citations if is_internal_reference(citation["properties"])],
                    "external_citations": [citation for citation in citations if not is_internal_reference(citation["properties"])],
                }
            )
        return units

    def _extract_legal_basis_from_file_info(self, file_info: str) -> list[dict]:
        bases = []
        seen = set()
        for match in re.finditer(r"\b(?:pursuant to|under|according to|in accordance with)\s+(?:the\s+)?([A-Z][A-Za-z]*(?:\s+[A-Z][A-Za-z]*)+\s+Act(?:\s+of\s+\d{4})?)", file_info):
            title = clean_string(match.group(1))
            key = normalize_text_key(title)
            if not title or key in seen:
                continue
            seen.add(key)
            bases.append(
                {
                    "node_id": self._new_node_id("Legal Basis"),
                    "node_name": title,
                    "node_type": "Legal Basis",
                    "properties": {
                        "official_title": title,
                        "alias": "",
                    },
                }
            )
        return bases

    @staticmethod
    def _infer_document_title(filename: str, file_info: str) -> str:
        base_name = os.path.splitext(os.path.basename(filename))[0]
        for raw_line in file_info.replace("\r\n", "\n").replace("\r", "\n").split("\n"):
            line = clean_string(raw_line)
            if not line:
                continue
            if len(line) <= 160 and re.search(r"\b(Act|Code|Regulation|Rules|Order|Directive|Statute|Law)\b", line):
                return line
        return base_name or "Unknown Legal Document"

    @staticmethod
    def _infer_document_type(title: str) -> str:
        for keyword in ("Act", "Code", "Regulation", "Rules", "Order", "Directive", "Statute", "Law"):
            if re.search(rf"\b{keyword}\b", title, flags=re.IGNORECASE):
                return keyword
        return ""

    @staticmethod
    def _split_top_level_units(section_number: str, text: str) -> list[tuple[str, str, str]]:
        content = clean_string(text)
        subsection_matches = list(re.finditer(r"(?<!\S)\(([a-z])\)\s", content))
        unit_level = "subsection"
        if not subsection_matches:
            subsection_matches = list(re.finditer(r"(?<!\S)\((\d+)\)\s", content))
            unit_level = "paragraph"
        if not subsection_matches:
            return [(section_number, "section", content)]

        units = []
        for index, match in enumerate(subsection_matches):
            start = match.start()
            end = subsection_matches[index + 1].start() if index + 1 < len(subsection_matches) else len(content)
            marker = match.group(1)
            unit_number = f"{section_number} {unit_level} ({marker})"
            units.append((unit_number, unit_level, content[start:end].strip()))
        return units

    def _extract_citations(self, text: str, filename: str) -> list[dict]:
        citations = []
        seen = set()
        doc_title = os.path.splitext(os.path.basename(filename))[0]

        section_ref_pattern = re.compile(
            r"\bsections?\s+([0-9A-Za-z.\-]+(?:\([A-Za-z0-9]+\))?(?:\s*(?:,|and|or)\s*[0-9A-Za-z.\-]+(?:\([A-Za-z0-9]+\))?)*)\s+of\s+"
            r"(this\s+(?:title|chapter|section|Act)|that\s+Act|the\s+[A-Z][A-Za-z]*(?:\s+[A-Z][A-Za-z]*)*\s+Act(?:\s+of\s+\d{4})?)",
            flags=re.IGNORECASE,
        )
        for match in section_ref_pattern.finditer(text):
            numbers = re.split(r"\s*(?:,|and|or)\s*", match.group(1))
            target_doc = clean_string(match.group(2))
            internal = target_doc.lower().startswith("this ")
            for number in numbers:
                if not number:
                    continue
                provision_number = f"Section {number.strip()}"
                title = doc_title if internal else re.sub(r"^the\s+", "", target_doc, flags=re.IGNORECASE)
                node_name = f"{title} {provision_number}".strip()
                citation = self._build_citation(
                    node_name=node_name,
                    official_title=title,
                    provision_number=provision_number,
                    citation_type="Provision",
                    internal=internal,
                )
                key = normalize_text_key(node_name)
                if key not in seen:
                    seen.add(key)
                    citations.append(citation)

        usc_pattern = re.compile(
            r"\b(\d+)\s+U\.S\.C\.?\s*(?:\u00a7|section|Sec\.)?\s*([0-9A-Za-z.\-]+(?:\([A-Za-z0-9]+\))?)",
            flags=re.IGNORECASE,
        )
        for match in usc_pattern.finditer(text):
            provision_number = f"{match.group(1)} U.S.C. Section {match.group(2)}"
            citation = self._build_citation(
                node_name=provision_number,
                official_title="United States Code",
                provision_number=provision_number,
                citation_type="Provision",
                internal=False,
            )
            key = normalize_text_key(provision_number)
            if key not in seen:
                seen.add(key)
                citations.append(citation)

        act_pattern = re.compile(
            r"\b(?:the\s+)?([A-Z][A-Za-z]*(?:\s+[A-Z][A-Za-z]*)+\s+Act(?:\s+of\s+\d{4})?)\b"
        )
        for match in act_pattern.finditer(text):
            title = clean_string(match.group(1))
            if normalize_text_key(title) == normalize_text_key(doc_title):
                continue
            citation = self._build_citation(
                node_name=title,
                official_title=title,
                provision_number="",
                citation_type="Document",
                internal=False,
            )
            key = normalize_text_key(title)
            if key not in seen:
                seen.add(key)
                citations.append(citation)

        return citations

    def _build_citation(
            self,
            node_name: str,
            official_title: str,
            provision_number: str,
            citation_type: str,
            internal: bool,
    ) -> dict:
        return {
            "node_id": self._new_node_id("Citation"),
            "node_name": clean_string(node_name),
            "node_type": "Citation",
            "properties": {
                "citation_type": citation_type,
                "official_title": clean_string(official_title),
                "alias": "",
                "document_type": self._infer_document_type(official_title),
                "provision_number": clean_string(provision_number),
                "is_internal_reference": "Yes" if internal else "No",
                "citation_relation": "reference",
                "citation_purpose": "",
            },
        }

    @staticmethod
    def _infer_function_type(text: str) -> str:
        lowered = text.lower()
        if re.search(r"\bshall not|may not|must not|prohibit|unlawful\b", lowered):
            return "prohibition"
        if re.search(r"\bshall|must|required|require\b", lowered):
            return "obligation"
        if re.search(r"\bmay|authorized|authority|power\b", lowered):
            return "authorization"
        if re.search(r"\bpenalty|liable|fine|imprisoned|sanction\b", lowered):
            return "liability"
        if re.search(r"\bmeans|defined|definition\b", lowered):
            return "definition"
        return "rule"

    @staticmethod
    def _resolve_internal_reference(ref: dict, internal_reference_id_mapping: dict) -> Optional[str]:
        properties = ref.get("properties", {})
        candidates = [
            properties.get("provision_number"),
            ref.get("node_name"),
        ]
        for candidate in candidates:
            normalized = normalize_text_key(candidate)
            if normalized and normalized in internal_reference_id_mapping:
                return internal_reference_id_mapping[normalized]

        provision_number = properties.get("provision_number", "")
        number_match = re.search(r"\b(?:Section|Article)\s+[\w.\-]+(?:\([A-Za-z0-9]+\))*", provision_number)
        if number_match:
            normalized = normalize_text_key(number_match.group(0))
            return internal_reference_id_mapping.get(normalized)
        return None

    def _append_unresolved_citation_node(
            self,
            final_kg: dict,
            source_id: str,
            citation: dict,
            filename: str,
            id_mapping: dict,
    ):
        ref_node_name = citation.get("node_name", "")
        ref_node_type = citation.get("node_type", "Citation")
        if not ref_node_name:
            return

        ref_key = normalize_text_key(
            f"{ref_node_type}_{ref_node_name}_{citation.get('properties', {}).get('provision_number', '')}"
        )
        ref_node_id = id_mapping.get(ref_key)
        if not ref_node_id:
            ref_node_id = citation.get("node_id") or self._new_node_id(ref_node_type)
            final_kg["nodes"].append(
                {
                    "node_id": ref_node_id,
                    "node_name": ref_node_name,
                    "node_type": ref_node_type,
                    "properties": citation.get("properties", {}),
                    "filename": filename,
                }
            )
            id_mapping[ref_key] = ref_node_id

        if source_id != ref_node_id:
            final_kg["edges"].append(
                self._build_edge(
                    source_id=source_id,
                    target_id=ref_node_id,
                    relation_type="CITES",
                    filename=filename,
                )
            )


if __name__ == "__main__":
    result_data, saved_path = split_clause_file(
        os.path.join("data", "International Emergency Economic Powers Act.txt")
    )
    print(saved_path)
    print(len(result_data["clauses"]))
