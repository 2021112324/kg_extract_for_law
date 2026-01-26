# Copyright 2025 Google LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Provides functionality for annotating medical text using a language model.

The annotation process involves tokenizing the input text, generating prompts
for the language model, and resolving the language model's output into
structured annotations.

Usage example:
    annotator = Annotator(language_model, prompt_template)
    annotated_documents = annotator.annotate_documents(documents, resolver)
"""

from collections.abc import Iterable, Iterator, Sequence
import itertools
import time

from absl import logging

from . import chunking
from . import data
from . import exceptions
from . import inference
from . import progress
from . import prompting
from . import resolver as resolver_lib

ATTRIBUTE_SUFFIX = "_attributes"


class DocumentRepeatError(exceptions.LangExtractError):
    """Exception raised when identical document ids are present."""


def _merge_non_overlapping_extractions(
        all_extractions: list[Iterable[data.Extraction]],
) -> list[data.Extraction]:
    """Merges extractions from multiple extraction passes.

  When extractions from different passes overlap in their character positions,
  the extraction from the earlier pass is kept (first-pass wins strategy).
  Only non-overlapping extractions from later passes are added to the result.

  Args:
    all_extractions: List of extraction iterables from different sequential
      extraction passes, ordered by pass number.

  Returns:
    List of merged extractions with overlaps resolved in favor of earlier
    passes.
  """
    if not all_extractions:
        return []

    if len(all_extractions) == 1:
        return list(all_extractions[0])

    merged_extractions = list(all_extractions[0])

    for pass_extractions in all_extractions[1:]:
        for extraction in pass_extractions:
            overlaps = False
            if extraction.char_interval is not None:
                for existing_extraction in merged_extractions:
                    if existing_extraction.char_interval is not None:
                        if _extractions_overlap(extraction, existing_extraction):
                            overlaps = True
                            break

            if not overlaps:
                merged_extractions.append(extraction)

    return merged_extractions


def _extractions_overlap(
        extraction1: data.Extraction, extraction2: data.Extraction
) -> bool:
    """Checks if two extractions overlap based on their character intervals.

  Args:
    extraction1: First extraction to compare.
    extraction2: Second extraction to compare.

  Returns:
    True if the extractions overlap, False otherwise.
  """
    if extraction1.char_interval is None or extraction2.char_interval is None:
        return False

    start1, end1 = (
        extraction1.char_interval.start_pos,
        extraction1.char_interval.end_pos,
    )
    start2, end2 = (
        extraction2.char_interval.start_pos,
        extraction2.char_interval.end_pos,
    )

    if start1 is None or end1 is None or start2 is None or end2 is None:
        return False

    # Two intervals overlap if one starts before the other ends
    return start1 < end2 and start2 < end1


def _document_chunk_iterator(
        documents: Iterable[data.Document],
        max_char_buffer: int,
        restrict_repeats: bool = True,
) -> Iterator[chunking.TextChunk]:
    """Iterates over documents to yield text chunks along with the document ID.

  Args:
    documents: A sequence of Document objects.
    max_char_buffer: The maximum character buffer size for the ChunkIterator.
    restrict_repeats: Whether to restrict the same document id from being
      visited more than once.

  Yields:
    TextChunk containing document ID for a corresponding document.

  Raises:
    DocumentRepeatError: If restrict_repeats is True and the same document ID
      is visited more than once. Valid documents prior to the error will be
      returned.
  """
    visited_ids = set()
    for document in documents:
        tokenized_text = document.tokenized_text
        document_id = document.document_id
        if restrict_repeats and document_id in visited_ids:
            raise DocumentRepeatError(
                f"Document id {document_id} is already visited."
            )
        chunk_iter = chunking.ChunkIterator(
            text=tokenized_text,
            max_char_buffer=max_char_buffer,
            document=document,
        )
        visited_ids.add(document_id)

        yield from chunk_iter


class Annotator:
    """Annotates documents with extractions using a language model."""

    def __init__(
            self,
            language_model: inference.BaseLanguageModel,
            prompt_template: prompting.PromptTemplateStructured,
            format_type: data.FormatType = data.FormatType.YAML,
            attribute_suffix: str = ATTRIBUTE_SUFFIX,
            fence_output: bool = False,
    ):
        """Initializes Annotator.

    Args:
      language_model: Model which performs language model inference.
      prompt_template: Structured prompt.txt template where the answer is expected
        to be formatted text (YAML or JSON).
      format_type: The format type for the output (YAML or JSON).
      attribute_suffix: Suffix to append to attribute keys in the output.
      fence_output: Whether to expect/generate fenced output (```json or
        ```yaml). When True, the model is prompted to generate fenced output and
        the resolver expects it. When False, raw JSON/YAML is expected. Defaults
        to True.
    """
        self._language_model = language_model
        self._prompt_generator = prompting.QAPromptGenerator(
            prompt_template,
            format_type=format_type,
            attribute_suffix=attribute_suffix,
            fence_output=fence_output,
        )

        logging.debug(
            "Initialized Annotator with prompt.txt:\n%s", self._prompt_generator
        )

    def annotate_documents(
            self,
            documents: Iterable[data.Document],
            resolver: resolver_lib.AbstractResolver = resolver_lib.Resolver(
                format_type=data.FormatType.YAML,
            ),
            max_char_buffer: int = 200,
            batch_length: int = 1,
            debug: bool = True,
            extraction_passes: int = 1,
            **kwargs,
    ) -> Iterator[data.AnnotatedDocument]:
        """Annotates a sequence of documents with NLP extractions.

      Breaks documents into chunks, processes them into prompts and performs
      batched inference, mapping annotated extractions back to the original
      document. Batch processing is determined by batch_length, and can operate
      across documents for optimized throughput.

    Args:
      documents: Documents to annotate. Each document is expected to have a
        unique document_id.
      resolver: Resolver to use for extracting information from text.
      max_char_buffer: Max number of characters that we can run inference on.
        The text will be broken into chunks up to this length.
      batch_length: Number of chunks to process in a single batch.
      debug: Whether to populate debug fields.
      extraction_passes: Number of sequential extraction attempts to improve
        recall by finding additional entities. Defaults to 1, which performs
        standard single extraction.
        Values > 1 reprocess tokens multiple times, potentially increasing
        costs with the potential for a more thorough extraction.
      **kwargs: Additional arguments passed to LanguageModel.infer and Resolver.

    Yields:
      Resolved annotations from input documents.

    Raises:
      ValueError: If there are no scored outputs during inference.
    """

        if extraction_passes == 1:
            yield from self._annotate_documents_single_pass(
                documents, resolver, max_char_buffer, batch_length, debug, **kwargs
            )
        else:
            yield from self._annotate_documents_sequential_passes(
                documents,
                resolver,
                max_char_buffer,
                batch_length,
                debug,
                extraction_passes,
                **kwargs,
            )

    def _annotate_documents_single_pass(
            self,
            documents: Iterable[data.Document],
            resolver: resolver_lib.AbstractResolver,
            max_char_buffer: int,
            batch_length: int,
            debug: bool,
            **kwargs,
    ) -> Iterator[data.AnnotatedDocument]:
        """
单次遍历注释逻辑（原始实现）。
该函数接收文档集合，使用resolver对文档进行注释处理，支持最大字符缓冲区和批处理长度设置，以及调试模式。
参数:
    documents: 要处理的文档集合
    resolver: 抽象resolver接口实例
    max_char_buffer: 最大字符缓冲区大小
    batch_length: 批处理长度
    debug: 是否启用调试模式
    **kwargs: 额外的关键字参数

返回:
    带注释文档的迭代器
        """

        logging.info("Starting document annotation.")  # 记录开始文档注释的日志信息

        # 使用itertools.tee创建两个相同的文档迭代器
        doc_iter, doc_iter_for_chunks = itertools.tee(documents, 2)
        curr_document = next(doc_iter, None)  # 获取第一个文档
        if curr_document is None:  # 如果没有文档
            logging.warning("No documents to process.")  # 记录警告日志
            return  # 直接返回

        annotated_extractions: list[data.Extraction] = []  # 初始化抽取结果列表
        # 创建文档块迭代器，将文档按指定字符缓冲区大小切分
        chunk_iter = _document_chunk_iterator(doc_iter_for_chunks, max_char_buffer)

        # 按批处理长度将文本块分批
        batches = chunking.make_batches_of_textchunk(chunk_iter, batch_length)

        model_info = progress.get_model_info(self._language_model)  # 获取模型信息

        # 创建抽取进度条
        progress_bar = progress.create_extraction_progress_bar(
            batches, model_info=model_info, disable=not debug  # 如果不是调试模式则禁用进度条
        )

        chars_processed = 0  # 初始化已处理字符数

        for index, batch in enumerate(progress_bar):  # 遍历每个批次
            logging.info("Processing batch %d with length %d", index, len(batch))  # 记录处理批次的日志

            batch_prompts: list[str] = []  # 初始化批次提示列表

            # 为批次中的每个文本块生成提示
            for text_chunk in batch:
                batch_prompts.append(
                    self._prompt_generator.render(  # 使用提示生成器渲染提示
                        question=text_chunk.chunk_text,  # 文本块内容作为问题
                        additional_context=text_chunk.additional_context,  # 附加上下文
                    )
                )

            # 显示当前处理内容
            if debug and progress_bar:  # 如果是调试模式且有进度条
                batch_size = sum(len(chunk.chunk_text) for chunk in batch)  # 计算批次大小
                # 格式化抽取进度
                desc = progress.format_extraction_progress(
                    model_info,  # 模型信息
                    current_chars=batch_size,  # 当前字符数
                    processed_chars=chars_processed,  # 已处理字符数
                )
                progress_bar.set_description(desc)  # 设置进度条描述

            # 调用语言模型进行推理
            batch_scored_outputs = self._language_model.infer(
                batch_prompts=batch_prompts,  # 批次提示
                **kwargs,  # 额外参数
            )

            # 更新总处理进度
            if debug:  # 如果是调试模式
                for chunk in batch:  # 遍历批次中的每个块
                    if chunk.document_text:  # 如果有文档文本
                        char_interval = chunk.char_interval  # 获取字符区间
                        # 累加已处理字符数
                        chars_processed += char_interval.end_pos - char_interval.start_pos

                # 使用最终处理计数更新进度条
                if progress_bar:  # 如果有进度条
                    batch_size = sum(len(chunk.chunk_text) for chunk in batch)  # 计算批次大小
                    # 格式化抽取进度
                    desc = progress.format_extraction_progress(
                        model_info,  # 模型信息
                        current_chars=batch_size,  # 当前字符数
                        processed_chars=chars_processed,  # 已处理字符数
                    )
                    progress_bar.set_description(desc)  # 设置进度条描述

            # 遍历批次和得分输出的配对
            for text_chunk, scored_outputs in zip(batch, batch_scored_outputs):
                logging.debug("Processing chunk: %s", text_chunk)  # 记录处理文本块的日志

                if not scored_outputs:  # 如果没有得分输出
                    logging.error(  # 记录错误日志
                        "No scored outputs for chunk with ID %s.", text_chunk.document_id  # 对于ID为%s的块没有得分输出
                    )
                    raise inference.InferenceOutputError(  # 抛出推断输出错误
                        "No scored outputs from language model."  # 来自语言模型的无得分输出
                    )

                # 当前文档ID与文本块文档ID不匹配时
                while curr_document.document_id != text_chunk.document_id:
                    logging.info(  # 记录完成注释的日志
                        "Completing annotation for document ID %s.",  # 完成文档ID %s 的注释
                        curr_document.document_id,  # 当前文档ID
                    )
                    # 创建带注释的文档对象
                    annotated_doc = data.AnnotatedDocument(
                        document_id=curr_document.document_id,  # 文档ID
                        extractions=annotated_extractions,  # 抽取结果
                        text=curr_document.text,  # 文档文本
                    )
                    yield annotated_doc  # 生成带注释的文档
                    annotated_extractions.clear()  # 清空抽取结果列表

                    curr_document = next(doc_iter, None)  # 获取下一个文档
                    assert curr_document is not None, (  # 断言下一个文档不为空
                        f"Document should be defined for {text_chunk} per"  # 根据 _document_chunk_iterator(...) 规范
                        " _document_chunk_iterator(...) specifications."  # {text_chunk} 应该定义文档
                    )

            top_inference_result = scored_outputs[0].output  # 获取最高分推断结果
            logging.debug("Top inference result: %s", top_inference_result)  # 记录推断结果日志

            # 使用resolver解析推断结果
            annotated_chunk_extractions = resolver.resolve(
                top_inference_result, debug=debug, **kwargs  # 推断结果，调试模式，额外参数
            )

            chunk_text = text_chunk.chunk_text  # 获取文本块内容
            token_offset = text_chunk.token_interval.start_index  # 获取标记偏移量
            char_offset = text_chunk.char_interval.start_pos  # 获取字符偏移量

            # 对抽取结果进行对齐
            aligned_extractions = resolver.align(
                annotated_chunk_extractions,  # 注释的块抽取结果
                chunk_text,  # 文本块
                token_offset,  # 标记偏移量
                char_offset,  # 字符偏移量
                **kwargs,  # 额外参数
            )

            annotated_extractions.extend(aligned_extractions)  # 将对齐的抽取结果添加到列表

        progress_bar.close()  # 关闭进度条

        if debug:  # 如果是调试模式
            progress.print_extraction_complete()  # 打印抽取完成信息

        if curr_document is not None:  # 如果还有当前文档
            logging.info(  # 记录最终化注释的日志
                "Finalizing annotation for document ID %s.", curr_document.document_id  # 最终化文档ID %s 的注释
            )
            # 创建最终的带注释文档
            annotated_doc = data.AnnotatedDocument(
                document_id=curr_document.document_id,  # 文档ID
                extractions=annotated_extractions,  # 抽取结果
                text=curr_document.text,  # 文档文本
            )

            yield annotated_doc  # 生成最终的带注释文档

        logging.info("Document annotation completed.")  # 记录文档注释完成的日志

    def _annotate_documents_sequential_passes(
            self,
            documents: Iterable[data.Document],
            resolver: resolver_lib.AbstractResolver,
            max_char_buffer: int,
            batch_length: int,
            debug: bool,
            extraction_passes: int,
            **kwargs,
    ) -> Iterator[data.AnnotatedDocument]:
        """Sequential extraction passes logic for improved recall."""

        logging.info(
            "Starting sequential extraction passes for improved recall with %d"
            " passes.",
            extraction_passes,
        )

        document_list = list(documents)

        document_extractions_by_pass: dict[str, list[list[data.Extraction]]] = {}
        document_texts: dict[str, str] = {}

        for pass_num in range(extraction_passes):
            logging.info(
                "Starting extraction pass %d of %d", pass_num + 1, extraction_passes
            )

            for annotated_doc in self._annotate_documents_single_pass(
                    document_list,
                    resolver,
                    max_char_buffer,
                    batch_length,
                    debug=(debug and pass_num == 0),
                    **kwargs,  # Only show progress on first pass
            ):
                doc_id = annotated_doc.document_id

                if doc_id not in document_extractions_by_pass:
                    document_extractions_by_pass[doc_id] = []
                    document_texts[doc_id] = annotated_doc.text or ""

                document_extractions_by_pass[doc_id].append(
                    annotated_doc.extractions or []
                )

        for doc_id, all_pass_extractions in document_extractions_by_pass.items():
            merged_extractions = _merge_non_overlapping_extractions(
                all_pass_extractions
            )

            if debug:
                total_extractions = sum(
                    len(extractions) for extractions in all_pass_extractions
                )
                logging.info(
                    "Document %s: Merged %d extractions from %d passes into "
                    "%d non-overlapping extractions.",
                    doc_id,
                    total_extractions,
                    extraction_passes,
                    len(merged_extractions),
                )

            yield data.AnnotatedDocument(
                document_id=doc_id,
                extractions=merged_extractions,
                text=document_texts[doc_id],
            )

        logging.info("Sequential extraction passes completed.")

    def annotate_text(
            self,
            text: str,
            resolver: resolver_lib.AbstractResolver = resolver_lib.Resolver(
                format_type=data.FormatType.YAML,
            ),
            max_char_buffer: int = 200,
            batch_length: int = 1,
            additional_context: str | None = None,
            debug: bool = True,
            extraction_passes: int = 1,
            **kwargs,
    ) -> data.AnnotatedDocument:
        """Annotates text with NLP extractions for text input.

    Args:
      text: Source text to annotate.
      resolver: Resolver to use for extracting information from text.
      max_char_buffer: Max number of characters that we can run inference on.
        The text will be broken into chunks up to this length.
      batch_length: Number of chunks to process in a single batch.
      additional_context: Additional context to supplement prompt.txt instructions.
      debug: Whether to populate debug fields.
      extraction_passes: Number of sequential extraction passes to improve
        recall by finding additional entities. Defaults to 1, which performs
        standard single extraction. Values > 1 reprocess tokens multiple times,
        potentially increasing costs.
      **kwargs: Additional arguments for inference and resolver.

    Returns:
      Resolved annotations from text for document.
    """
        start_time = time.time() if debug else None

        documents = [
            data.Document(
                text=text,
                document_id=None,
                additional_context=additional_context,
            )
        ]

        annotations = list(
            self.annotate_documents(
                documents,
                resolver,
                max_char_buffer,
                batch_length,
                debug,
                extraction_passes,
                **kwargs,
            )
        )
        assert (
                len(annotations) == 1
        ), f"Expected 1 annotation but got {len(annotations)} annotations."

        if debug and annotations[0].extractions:
            elapsed_time = time.time() - start_time if start_time else None
            num_extractions = len(annotations[0].extractions)
            unique_classes = len(
                set(e.extraction_class for e in annotations[0].extractions)
            )
            num_chunks = len(text) // max_char_buffer + (
                1 if len(text) % max_char_buffer else 0
            )

            progress.print_extraction_summary(
                num_extractions,
                unique_classes,
                elapsed_time=elapsed_time,
                chars_processed=len(text),
                num_chunks=num_chunks,
            )

        return data.AnnotatedDocument(
            document_id=annotations[0].document_id,
            extractions=annotations[0].extractions,
            text=annotations[0].text,
        )
