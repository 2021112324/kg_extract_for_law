"""Thin extraction-and-storage entry point for national-standard v2."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from .graph_extract import NationalStandardGraphExtractor


async def extract_and_store_national_standard(
    input_path: str | Path,
    graph_name: str,
    graph_storage: Any,
    extractor: NationalStandardGraphExtractor | None = None,
) -> dict[str, Any]:
    """Extract one standard and write it through the existing graph-storage contract."""
    runner = extractor or NationalStandardGraphExtractor()
    result = await runner.extract(input_path)
    if result.get("status") != "success":
        raise RuntimeError(f"国家标准图谱抽取未成功: {result.get('status')}")

    graph = result.get("graph", {}) or {}
    if not graph.get("nodes"):
        raise RuntimeError("国家标准图谱节点为空")

    try:
        graph_storage.connect()
        saved = graph_storage.add_subgraph_with_merge(
            graph,
            graph_name,
            "DomainLevel",
            filename=result.get("filename") or Path(input_path).name,
        )
        if saved is not True:
            raise RuntimeError("国家标准图谱保存到 Neo4j 失败")
        logging.info(
            "国家标准 v2 图谱写入成功: file=%s graph=%s nodes=%s edges=%s",
            result.get("filename") or Path(input_path).name,
            graph_name,
            len(graph.get("nodes", [])),
            len(graph.get("edges", [])),
        )
        return result
    except Exception as exc:
        runner.result_stats.error += 1
        runner.result_stats.storage_errors += 1
        runner.result_stats.error_msg += f"入库失败: {input_path} error={exc}\n"
        logging.exception("国家标准 v2 图谱写入失败: %s", input_path)
        raise
    finally:
        try:
            graph_storage.disconnect()
        except Exception:
            logging.warning("国家标准 v2 图谱存储连接关闭失败", exc_info=True)

