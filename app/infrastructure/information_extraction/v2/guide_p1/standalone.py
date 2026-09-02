"""Standalone guide_p1 directory extraction without MySQL or MinIO."""

from __future__ import annotations

import logging
import re
import uuid
from dataclasses import replace
from pathlib import Path
from typing import Any, Callable

from .config import GuideP1Config
from .extractor import GuideP1Extractor
from .io_utils import discover_input_files, save_json
from .stats import ResultStats


logger = logging.getLogger(__name__)


def generate_standalone_guide_p1_graph_name(guide_data_dir: str) -> str:
    """Generate a legal and traceable Neo4j graph tag for a standalone run."""
    source_name = Path(str(guide_data_dir).rstrip("/\\")).name or "guide_p1"
    source_name = re.sub(r"[^\w]", "_", source_name, flags=re.UNICODE).strip("_") or "source"
    return f"guide_p1_{source_name}_kg_{uuid.uuid4().hex}"


def _generate_task_graph_name() -> str:
    return f"guide_p1_standalone_task_{uuid.uuid4().hex}"


class GuideP1StandaloneRunner:
    """Extract local files, persist Neo4j subgraphs, and report aggregate stats."""

    def __init__(
        self,
        graph_storage: Any,
        config: GuideP1Config | None = None,
        extractor_factory: Callable[..., GuideP1Extractor] = GuideP1Extractor,
    ) -> None:
        self.graph_storage = graph_storage
        self.config = replace(config or GuideP1Config.from_env(), strict_mode=True)
        self.extractor_factory = extractor_factory

    def _disconnect_safely(self) -> None:
        try:
            self.graph_storage.disconnect()
        except Exception as exc:
            logger.warning("断开 Neo4j 连接时出现问题: %s", exc)

    @staticmethod
    def _log_final_stats(summary: dict) -> None:
        stats = summary.get("stats") or {}
        logger.info(
            "分点格式合规指引独立抽取完成: target_graph=%s, output_dir=%s",
            summary.get("kg_graph_name"),
            summary.get("output_dir"),
        )
        logger.info("分点格式合规指引数据统计")
        logger.info("文件总数: %s", summary.get("total", 0))
        logger.info("成功文件数: %s", summary.get("success", 0))
        logger.info("失败文件数: %s", summary.get("failed", 0))
        logger.info("节点数: %s", summary.get("node_count", 0))
        logger.info("关系数: %s", summary.get("edge_count", 0))
        logger.info("错误数: %s", stats.get("error", 0))
        logger.info("抽取错误数: %s", stats.get("extraction_error", 0))
        logger.info("入库错误数: %s", stats.get("storage_error", 0))
        logger.info("文件处理错误数: %s", stats.get("file_processing_error", 0))
        logger.info("弱警告数: %s", stats.get("weak_warning", 0))
        logger.info("强警告数: %s", stats.get("strong_warning", 0))

    async def run(
        self,
        guide_data_dir: str,
        output_dir: str | None = None,
        if_del_task: bool = False,
        kg_graph_name: str | None = None,
    ) -> dict:
        source_root = Path(guide_data_dir)
        if not source_root.exists():
            raise FileNotFoundError(f"分点格式合规指引目录不存在: {source_root}")
        if not source_root.is_dir():
            raise ValueError(f"分点格式合规指引输入路径不是目录: {source_root}")

        files = discover_input_files(source_root, self.config.supported_suffixes)
        output_root = Path(output_dir) if output_dir else source_root.parent / f"{source_root.name}_guide_p1_output"
        output_root.mkdir(parents=True, exist_ok=True)
        target_graph_name = kg_graph_name or generate_standalone_guide_p1_graph_name(guide_data_dir)
        successful_graph_names: list[str] = []
        errors: list[dict] = []
        batch_stats = ResultStats()
        summary = {
            "kg_id": None,
            "kg_graph_name": target_graph_name,
            "input_dir": str(source_root),
            "output_dir": str(output_root),
            "use_mysql": False,
            "use_minio": False,
            "strict_mode": self.config.strict_mode,
            "total": len(files),
            "success": 0,
            "failed": 0,
            "node_count": 0,
            "edge_count": 0,
            "errors": errors,
        }

        def finalize_summary() -> dict:
            summary["failed"] = len(errors)
            summary["stats"] = batch_stats.to_dict()
            summary_path = output_root / "service_summary.json"
            summary["summary_path"] = str(summary_path)
            save_json(summary, summary_path)
            self._log_final_stats(summary)
            return summary

        logger.info(
            "开始分点格式合规指引独立目录抽取: directory=%s, target_graph=%s, files=%s, output_dir=%s",
            source_root,
            target_graph_name,
            len(files),
            output_root,
        )

        for file_path in files:
            extractor = self.extractor_factory(config=self.config)
            file_stats_merged = False
            stage = "大模型抽取"
            graph_name = _generate_task_graph_name()
            try:
                result = await extractor.extract_file(
                    file_path,
                    output_dir=output_root,
                    run_llm=True,
                )
                batch_stats.merge(extractor.result_stats)
                file_stats_merged = True
                if result.get("status") != "success" or not result.get("formal_graph_eligible"):
                    raise RuntimeError(f"分点格式合规指引图谱抽取未成功: {result.get('status')}")

                graph = result.get("neo4j_graph") or {}
                nodes = graph.get("nodes", []) or []
                edges = graph.get("edges", []) or []
                if not nodes:
                    raise ValueError("分点格式合规指引图谱节点为空")

                stage = "Neo4j 写入"
                self.graph_storage.connect()
                saved = self.graph_storage.add_subgraph_with_merge(
                    graph,
                    graph_name,
                    "DomainLevel",
                    filename=file_path.name,
                )
                if saved is not True:
                    raise RuntimeError("分点格式合规指引图谱保存到 Neo4j 失败")

                successful_graph_names.append(graph_name)
                summary["success"] += 1
                summary["node_count"] += len(nodes)
                summary["edge_count"] += len(edges)
                logger.info(
                    "分点格式合规指引文件处理成功: file=%s, graph=%s, nodes=%s, edges=%s",
                    file_path.name,
                    graph_name,
                    len(nodes),
                    len(edges),
                )
            except Exception as exc:
                if stage == "Neo4j 写入":
                    batch_stats.add_storage_error(f"{file_path.name}: {exc}")
                elif not file_stats_merged:
                    file_stats = ResultStats(total_files=1)
                    file_stats.add_file_processing_error(f"{file_path.name}: {exc}")
                    batch_stats.merge(file_stats)
                elif not extractor.result_stats.error:
                    batch_stats.add_file_processing_error(f"{file_path.name}: {exc}")
                errors.append({"file": file_path.name, "stage": stage, "error": str(exc)})
                logger.exception(
                    "分点格式合规指引文件处理失败: file=%s, stage=%s, error=%s",
                    file_path.name,
                    stage,
                    exc,
                )
            finally:
                self._disconnect_safely()

        if not successful_graph_names:
            finalize_summary()
            raise RuntimeError("分点格式合规指引目录下没有成功入库的图谱")

        try:
            for graph_name in successful_graph_names:
                self.graph_storage.connect()
                logger.info("正在合并分点格式合规指引图谱 %s -> %s", graph_name, target_graph_name)
                merge_result = self.graph_storage.merge_graphs(graph_name, target_graph_name)
                merge_error = getattr(merge_result, "error", None)
                if merge_result is False or merge_error:
                    raise RuntimeError(merge_error or f"图谱 {graph_name} 合并失败")
                if if_del_task:
                    deleted = self.graph_storage.delete_subgraph(graph_name)
                    if deleted is not True:
                        batch_stats.add_weak_warning(f"临时分点格式合规指引子图删除失败: {graph_name}")
                self.graph_storage.disconnect()
        except Exception as exc:
            batch_stats.add_storage_error(f"图谱合并失败: {exc}")
            errors.append({"file": None, "stage": "图谱合并", "error": str(exc)})
            logger.exception("分点格式合规指引独立图谱合并失败: %s", exc)
            finalize_summary()
            raise RuntimeError(f"分点格式合规指引图谱合并时出现问题: {exc}") from exc
        finally:
            self._disconnect_safely()

        return finalize_summary()
