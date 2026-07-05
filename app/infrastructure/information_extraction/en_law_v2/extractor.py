"""格式一英文法规知识图谱抽取器入口。

本文件是 `en_law` 模块对外最主要的调用入口，负责把以下步骤串起来：
1. 使用 `splitter.py` 对英文法规 Markdown 做一阶段结构切分。
2. 将文件基础信息和 Article 内容组装为 LLM 输入。
3. 通过项目已有 `InformationExtractionFactory` 调用 Langextract。
4. 保留 LLM 原始输出，方便后续人工排查。
5. 使用 `graph_builder.py` 将切分结果和 LLM 结果合并成图谱 JSON。

使用方式：
- 只测试切分：调用 `split_file()` 或 `split_text()`。
- 跳过 LLM 只生成文件级图谱并记录 Article 缺失错误：调用 `extract_file_to_kg(..., run_llm=False)`。
- 完整抽取：调用 `extract_file_to_kg(..., run_llm=True)`。
"""

from __future__ import annotations

# asyncio 用于并发执行多个 Article 的 LLM 抽取任务。
import asyncio
# dataclass 用于定义轻量运行统计对象。
from dataclasses import dataclass, field
# json 用于读写结构化中间结果和最终图谱结果。
import json
# logging 用于输出运行统计信息。
import logging
# os 用于处理路径和文件名。
import os
# Any 用于描述 LLM 结果这类结构较灵活的数据。
from typing import Any

# 引入英文法规抽取的模型、并发、超时和宽松模式配置。
from app.infrastructure.information_extraction.en_law_v2.config import (
    EN_LAW_BATCH_LENGTH,
    EN_LAW_MAX_CHAR_BUFFER,
    EN_LAW_MAX_CONCURRENT,
    EN_LAW_MAX_TOKENS,
    EN_LAW_MAX_WORKERS,
    EN_LAW_MODEL,
    EN_LAW_MODEL_API_KEY,
    EN_LAW_MODEL_API_URL,
    EN_LAW_EXTRACTION_TIMEOUT,
    EN_LAW_TIMEOUT,
    EN_LAW_LENIENT_MODE,
    EN_LAW_MAX_RETRIES,
)
# 引入图谱构建器和 LLM 输出标准化工具。
from app.infrastructure.information_extraction.en_law_v2.graph_builder import FormatTwoGraphBuilder, normalize_extraction_result, to_plain
# 引入文件级和 Article 级抽取所需的 prompt、schema、example。
from app.infrastructure.information_extraction.en_law_v2.prompt import (
    example_for_article,
    example_for_file_info,
    example_for_provision_clause,
    prompt_for_article,
    prompt_for_file_info,
    prompt_for_provision_clause,
    schema_for_article,
    schema_for_file_info,
    schema_for_provision_clause,
)
# 引入规则切分入口和文本清理函数。
from app.infrastructure.information_extraction.en_law_v2.splitter import clean_text, split_v2_document


def save_json(data: dict[str, Any], output_path: str) -> str:
    """将字典保存为 UTF-8 JSON 文件，并返回实际输出路径。"""
    # 确保目标目录存在；如果目录已经存在则不会报错。
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    # 使用 ensure_ascii=False 保留英文法规中的符号和中文业务字段。
    with open(output_path, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)
    # 返回路径，方便调用方把 output_path 写回结果 metadata。
    return output_path


def load_json(input_path: str) -> dict[str, Any]:
    """从 UTF-8 JSON 文件读取字典数据。"""
    # 中间结果和最终图谱都统一按 UTF-8 保存和读取。
    with open(input_path, "r", encoding="utf-8") as file:
        return json.load(file)


def _base_output_path(input_path_or_filename: str, output_dir: str, suffix: str) -> str:
    """根据输入文件名和后缀生成输出 JSON 路径。"""
    # 去掉目录和扩展名，只保留文件主名作为输出文件名前缀。
    base_name = os.path.splitext(os.path.basename(input_path_or_filename))[0]
    # 约定输出格式为 `{原文件名}_{阶段名}.json`。
    return os.path.join(output_dir, f"{base_name}_{suffix}.json")


def _has_extraction_content(extraction: dict[str, Any]) -> bool:
    """判断标准化 LLM 抽取结果中是否真的包含实体或关系。"""
    return bool((extraction.get("entities") or []) or (extraction.get("relations") or []))


@dataclass
class ExtractionRunStats:
    """一次英文法规抽取运行的统计信息。

    该结构参考正确版本 `law_extract` 中的 ResultStats，
    用于记录错误、普通警告和强警告数量及其具体消息。
    """

    # error 表示实际失败，例如 LLM 调用异常或文件级抽取失败。
    error: int = 0
    # warning 表示弱警告，例如可继续处理但需要审查的情况。
    warning: int = 0
    # strong_warning 表示强警告，例如严格模式下存在失败 Article。
    strong_warning: int = 0
    # 逐条保存错误消息，便于在报告或 raw_llm_result 中查看。
    error_messages: list[str] = field(default_factory=list)
    # 逐条保存弱警告消息。
    warning_messages: list[str] = field(default_factory=list)
    # 逐条保存强警告消息。
    strong_warning_messages: list[str] = field(default_factory=list)

    def add_error(self, message: str) -> None:
        """记录一条错误。"""
        self.error += 1
        self.error_messages.append(message)

    def add_warning(self, message: str) -> None:
        """记录一条弱警告。"""
        self.warning += 1
        self.warning_messages.append(message)

    def add_strong_warning(self, message: str) -> None:
        """记录一条强警告。"""
        self.strong_warning += 1
        self.strong_warning_messages.append(message)

    def to_dict(self) -> dict[str, Any]:
        """转换为可 JSON 序列化的字典。"""
        return {
            "error": self.error,
            "warning": self.warning,
            "strong_warning": self.strong_warning,
            "error_messages": self.error_messages,
            "warning_messages": self.warning_messages,
            "strong_warning_messages": self.strong_warning_messages,
        }


class FormatTwoEnLawExtractor:
    """格式一英文法规抽取器。

    该类对外提供切分、LLM 抽取、图谱构建和文件保存能力。
    LLM 客户端采用懒加载：只有真正执行 LLM 抽取时才创建，
    因此 split-only 测试不依赖模型服务。
    """

    def __init__(
        self,
        max_concurrent: int = EN_LAW_MAX_CONCURRENT,
        lenient_mode: bool = EN_LAW_LENIENT_MODE,
    ):
        """初始化抽取器运行参数。"""
        # Article 级 LLM 抽取的最大并发量。
        self.max_concurrent = max_concurrent
        # 宽松模式：失败 Article 会记录错误并跳过，不生成 fallback 节点。
        self.lenient_mode = lenient_mode
        # 信号量用于限制 asyncio 并发请求数量。
        self.semaphore = asyncio.Semaphore(max_concurrent)
        # LLM 抽取器懒加载缓存；None 表示尚未创建。
        self._extractor = None
        # 当前运行的统计对象。
        self.result_stats = ExtractionRunStats()

    def _get_llm_extractor(self):
        """获取或创建底层 Langextract 抽取器。"""
        # 如果已经创建过，直接复用，避免重复初始化模型客户端。
        if self._extractor is not None:
            return self._extractor
        # 延迟导入，确保 split-only 路径不强制加载 LLM 相关依赖。
        from app.infrastructure.information_extraction.factory import InformationExtractionFactory
        from app.infrastructure.information_extraction.method.base import LangextractConfig

        # 将配置文件中的模型参数转换为 LangextractConfig。
        config = LangextractConfig(
            model_name=EN_LAW_MODEL,
            api_key=EN_LAW_MODEL_API_KEY,
            api_url=EN_LAW_MODEL_API_URL,
            config={
                "timeout": EN_LAW_TIMEOUT,
                "extraction_timeout": EN_LAW_EXTRACTION_TIMEOUT,
                "max_tokens": EN_LAW_MAX_TOKENS,
            },
            max_char_buffer=EN_LAW_MAX_CHAR_BUFFER,
            batch_length=EN_LAW_BATCH_LENGTH,
            max_workers=EN_LAW_MAX_WORKERS,
        )
        # 通过项目已有工厂创建 langextract 实例。
        self._extractor = InformationExtractionFactory.create(
            "langextract",
            max_retries=EN_LAW_MAX_RETRIES,
            config=config,
        )
        # 返回可复用的底层抽取器。
        return self._extractor

    def split_text(
        self,
        text: str,
        filename: str = "",
        include_annex_content: bool = False,
    ) -> dict[str, Any]:
        """对传入的法规文本做一阶段结构切分。"""
        # 直接委托给规则切分器；该方法适合单测或内存文本调用。
        return split_v2_document(text, filename=filename)

    def split_file(
        self,
        input_path: str,
        output_dir: str | None = None,
        include_annex_content: bool = False,
    ) -> dict[str, Any]:
        """读取法规 Markdown 文件并执行一阶段结构切分。"""
        # 格式一数据源是 Markdown 文本，统一按 UTF-8 读取。
        with open(input_path, "r", encoding="utf-8") as file:
            text = file.read()
        # 文件名会进入 split_result，后续作为图谱 metadata 和错误定位来源。
        result = self.split_text(text, filename=os.path.basename(input_path), include_annex_content=include_annex_content)
        # 如果指定输出目录，则保存 split JSON，便于审查切分结果。
        if output_dir:
            result["output_path"] = save_json(result, _base_output_path(input_path, output_dir, "split"))
        return result

    def build_file_info_llm_input(self, filename: str, split_result: dict[str, Any]) -> str:
        """构造法规文件级 LLM 输入文本。"""
        # 只选用 selected_recitals，避免把完整超长 Whereas 全部送入模型。
        recitals_text = "\n".join(
            f"({item.get('number')}) {item.get('content')}"
            for item in split_result.get("selected_recitals", [])
        )
        # Article hints 用于帮助模型判断文件主题和范围，但不替代正文 Article 抽取。
        article_hints = []
        # clauses 中存放规则切分得到的 Article 列表。
        clauses = split_result.get("clauses", [])
        if clauses:
            # 首条 Article 往往描述 subject matter、scope 或 definitions。
            first = clauses[0]
            # 末条 Article 往往描述生效、适用或废止条款。
            last = clauses[-1]
            article_hints.append(f"First Article: {first.get('article_number')} {first.get('article_heading')}")
            article_hints.append(f"Last Article: {last.get('article_number')} {last.get('article_heading')}")
        # clean_text 会收敛多余空白，但保留段落结构。
        return clean_text(
            "\n".join(
                [
                    f"Filename: {filename}",
                    "File header:",
                    split_result.get("file_header", ""),
                    "Selected recitals:",
                    recitals_text,
                    "Article hints:",
                    "\n".join(article_hints),
                ]
            )
        )

    def build_article_llm_input(self, filename: str, article: dict[str, Any]) -> str:
        """构造单个 Article 的 LLM 输入文本。"""
        # 使用 JSON 作为输入载体，让模型能明确看到 Article 编号、标题、层级和正文。
        payload = {
            "filename": filename,
            "article_number": article.get("article_number"),
            "article_heading": article.get("article_heading"),
            "classification_context": article.get("classification_context") or {},
            "is_amendment_article": article.get("is_amendment_article", False),
            "structural_units": article.get("structural_units", []),
            "text": article.get("content", ""),
        }
        # ensure_ascii=False 保留法规文本中的特殊符号和中文业务字段。
        return clean_text(json.dumps(payload, ensure_ascii=False, indent=2))

    def build_provision_clause_llm_input(
        self,
        filename: str,
        article: dict[str, Any],
        provision_clause: dict[str, Any],
    ) -> str:
        """构造单个 ProvisionClause 的 LLM 输入文本。"""
        payload = {
            "filename": filename,
            "article_number": article.get("article_number"),
            "article_heading": article.get("article_heading"),
            "classification_context": article.get("classification_context") or {},
            "provision_clause": {
                "unit_number": provision_clause.get("unit_number"),
                "unit_content": provision_clause.get("unit_content"),
                "unit_level": provision_clause.get("unit_level"),
                "line_start": provision_clause.get("line_start"),
                "line_end": provision_clause.get("line_end"),
                "explicit_boundary": provision_clause.get("explicit_boundary"),
            },
        }
        return clean_text(json.dumps(payload, ensure_ascii=False, indent=2))

    async def llm_extract_file_info(self, filename: str, split_result: dict[str, Any]) -> dict[str, Any]:
        """调用 LLM 抽取法规文件级节点和法律依据节点。"""
        # 先构造受控的文件级输入，避免直接传入完整法规。
        input_text = self.build_file_info_llm_input(filename, split_result)
        # 通过现有抽取器执行实体和关系抽取。
        result = await self._get_llm_extractor().entity_and_relationship_extract(
            user_prompt=prompt_for_file_info,
            schema=schema_for_file_info,
            input_text=input_text,
            examples=example_for_file_info,
        )
        normalized = normalize_extraction_result(result)
        if not _has_extraction_content(normalized):
            raise ValueError(f"File-info LLM extraction returned empty result for {filename}")
        # 同时返回标准化 extraction 和原始 raw，便于后续审查。
        return {
            "input_text": input_text,
            "extraction": normalized,
            "raw": to_plain(result),
        }

    async def llm_extract_article(self, filename: str, article: dict[str, Any]) -> dict[str, Any]:
        """调用 LLM 抽取单个 Article 的条文图谱信息。"""
        # 使用信号量限制并发，避免过多 Article 同时请求模型服务。
        async with self.semaphore:
            # 将 Article 结构化信息转换为模型输入。
            input_text = self.build_article_llm_input(filename, article)
            # 调用底层抽取器，schema 限定输出 LegalProvision、ProvisionUnit、Citation。
            result = await self._get_llm_extractor().entity_and_relationship_extract(
                user_prompt=prompt_for_article,
                schema=schema_for_article,
                input_text=input_text,
                examples=example_for_article,
            )
            normalized = normalize_extraction_result(result)
            if not _has_extraction_content(normalized):
                raise ValueError(f"Article LLM extraction returned empty result for {article.get('article_number')}")
            # 返回结果时保留 Article 定位信息，方便失败追踪和图谱装配。
            return {
                "article_number": article.get("article_number"),
                "article_heading": article.get("article_heading"),
                "line_start": article.get("line_start"),
                "line_end": article.get("line_end"),
                "input_article": article,
                "input_text": input_text,
                "extraction": normalized,
                "raw": to_plain(result),
            }

    async def llm_extract_provision_clause(
        self,
        filename: str,
        article: dict[str, Any],
        provision_clause: dict[str, Any],
    ) -> dict[str, Any]:
        """调用 LLM 抽取单个 ProvisionClause 的条款属性和文本片段。"""
        async with self.semaphore:
            input_text = self.build_provision_clause_llm_input(filename, article, provision_clause)
            result = await self._get_llm_extractor().entity_and_relationship_extract(
                user_prompt=prompt_for_provision_clause,
                schema=schema_for_provision_clause,
                input_text=input_text,
                examples=example_for_provision_clause,
            )
            normalized = normalize_extraction_result(result)
            if not _has_extraction_content(normalized):
                raise ValueError(
                    f"ProvisionClause LLM extraction returned empty result for "
                    f"{provision_clause.get('unit_number')}"
                )
            return {
                "article_number": article.get("article_number"),
                "article_heading": article.get("article_heading"),
                "provision_clause": provision_clause,
                "unit_number": provision_clause.get("unit_number"),
                "line_start": provision_clause.get("line_start"),
                "line_end": provision_clause.get("line_end"),
                "input_text": input_text,
                "extraction": normalized,
                "raw": to_plain(result),
            }

    async def llm_extract_from_split_result(
        self,
        filename: str,
        split_result: dict[str, Any],
        output_dir: str | None = None,
    ) -> dict[str, Any]:
        """基于一阶段切分结果执行完整 LLM 抽取。"""
        # 每份文件开始抽取时重置运行统计，避免跨文件污染。
        self.result_stats = ExtractionRunStats()
        try:
            # 文件级信息先抽取，因为最终图谱必须有法规文件主节点。
            file_info_result = await self.llm_extract_file_info(filename, split_result)
        except Exception as exc:
            # 文件级失败通常无法安全继续，因此记录错误后继续抛出。
            self.result_stats.add_error(f"File-info extraction failed for {filename}: {exc}")
            raise

        # 为每个 Article 创建一个异步任务；Article 阶段只抽 LegalProvision 属性。
        tasks = [self.llm_extract_article(filename, article) for article in split_result.get("clauses", [])]
        # return_exceptions=True 允许单条 Article 失败后继续收集其他结果。
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # 成功 Article 的抽取结果列表。
        article_results = []
        # 失败 Article 的错误信息列表。
        failed_article_results = []
        # 按 split_result 中 Article 原顺序合并任务结果。
        for article, result in zip(split_result.get("clauses", []), results):
            if isinstance(result, Exception):
                # 失败时保留原 Article 输入，方便后续错误记录或人工复查。
                failed_article_results.append(
                    {
                        "article_number": article.get("article_number"),
                        "article_heading": article.get("article_heading"),
                        "input_article": article,
                        "error": str(result),
                    }
                )
                # 同步写入运行统计。
                self.result_stats.add_error(
                    f"Article extraction failed for {filename} {article.get('article_number')}: {result}"
                )
            else:
                # 成功时直接加入结果列表。
                article_results.append(result)

        # 严格模式下 Article 失败必须中断，避免生成缺失 Article 的正式 KG。
        if failed_article_results and not self.lenient_mode:
            failed_numbers = ", ".join(
                str(item.get("article_number") or item.get("article_heading") or "unknown")
                for item in failed_article_results
            )
            message = (
                f"{len(failed_article_results)} Article extractions failed in strict mode "
                f"for {filename}: {failed_numbers}"
            )
            self.result_stats.add_strong_warning(message)
            raise ValueError(message)

        clause_jobs: list[tuple[dict[str, Any], dict[str, Any]]] = []
        for article in split_result.get("clauses", []):
            for provision_clause in article.get("provision_clauses") or []:
                clause_jobs.append((article, provision_clause))
        clause_tasks = [
            self.llm_extract_provision_clause(filename, article, provision_clause)
            for article, provision_clause in clause_jobs
        ]
        clause_results_raw = await asyncio.gather(*clause_tasks, return_exceptions=True) if clause_tasks else []
        provision_clause_results = []
        failed_provision_clause_results = []
        for (article, provision_clause), result in zip(clause_jobs, clause_results_raw):
            if isinstance(result, Exception):
                failed_provision_clause_results.append(
                    {
                        "article_number": article.get("article_number"),
                        "unit_number": provision_clause.get("unit_number"),
                        "provision_clause": provision_clause,
                        "error": str(result),
                    }
                )
                self.result_stats.add_error(
                    f"ProvisionClause extraction failed for {filename} "
                    f"{provision_clause.get('unit_number')}: {result}"
                )
            else:
                provision_clause_results.append(result)

        if failed_provision_clause_results and not self.lenient_mode:
            failed_numbers = ", ".join(str(item.get("unit_number") or "unknown") for item in failed_provision_clause_results)
            message = (
                f"{len(failed_provision_clause_results)} ProvisionClause extractions failed in strict mode "
                f"for {filename}: {failed_numbers}"
            )
            self.result_stats.add_strong_warning(message)
            raise ValueError(message)

        # raw_result 是 LLM 阶段的完整审查材料，不等同于最终图谱。
        raw_result = {
            "source_filename": filename,
            "file_info_extraction": file_info_result,
            "article_extractions": article_results,
            "provision_clause_extractions": provision_clause_results,
            "failed_article_extractions": failed_article_results,
            "failed_provision_clause_extractions": failed_provision_clause_results,
            "skipped_annexes": split_result.get("annexes_metadata", []),
            "stats": {
                "article_total": len(split_result.get("clauses", [])),
                "article_success": len(article_results),
                "article_failed": len(failed_article_results),
                "provision_clause_total": len(clause_jobs),
                "provision_clause_success": len(provision_clause_results),
                "provision_clause_failed": len(failed_provision_clause_results),
                "annex_skipped": len(split_result.get("annexes_metadata", [])),
            },
            "run_stats": self.result_stats.to_dict(),
            "errors": [],
        }
        # 如果指定输出目录，则把 raw LLM 结果保存下来。
        if output_dir:
            raw_result["output_path"] = save_json(raw_result, _base_output_path(filename, output_dir, "raw_llm"))
        return raw_result

    def build_graph(
        self,
        filename: str,
        split_result: dict[str, Any],
        raw_llm_result: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """把切分结果和 LLM 结果组装为最终图谱。"""
        # 每次构建图谱都创建新的 builder，避免 warnings 在多次调用间残留。
        builder = FormatTwoGraphBuilder(lenient_mode=self.lenient_mode)
        return builder.build(filename, split_result, raw_llm_result)

    async def extract_text_to_kg(
        self,
        text: str,
        filename: str = "",
        run_llm: bool = True,
        output_dir: str | None = None,
    ) -> dict[str, Any]:
        """从法规文本直接抽取最终图谱。"""
        # 第一步：规则切分，得到文件头、Whereas、Article、Annex 等结构。
        split_result = self.split_text(text, filename=filename)
        # 第二步：可选 LLM 抽取；run_llm=False 时不生成 Article 级 LLM 知识。
        raw_llm_result = await self.llm_extract_from_split_result(filename, split_result, output_dir) if run_llm else {}
        # 第三步：把规则切分和 LLM 输出合并为图谱节点/边。
        kg = self.build_graph(filename, split_result, raw_llm_result)
        # 第四步：如果需要则保存最终 KG JSON。
        if output_dir:
            kg["output_path"] = save_json(kg, _base_output_path(filename, output_dir, "kg"))
        return kg

    async def extract_file_to_kg(
        self,
        input_path: str,
        run_llm: bool = True,
        output_dir: str | None = None,
    ) -> dict[str, Any]:
        """从法规 Markdown 文件路径直接抽取最终图谱。"""
        # 读取完整文件内容；文件路径只用于读取，文件名会作为图谱 source_filename。
        with open(input_path, "r", encoding="utf-8") as file:
            text = file.read()
        # 复用文本入口，避免文件入口和文本入口出现两套逻辑。
        return await self.extract_text_to_kg(
            text,
            filename=os.path.basename(input_path),
            run_llm=run_llm,
            output_dir=output_dir,
        )

    async def logging_result_stats(self) -> None:
        """把当前运行统计输出到日志。"""
        logging.info("English law extraction stats")
        logging.info("errors: %s", self.result_stats.error)
        logging.info("warnings: %s", self.result_stats.warning)
        logging.info("strong warnings: %s", self.result_stats.strong_warning)


