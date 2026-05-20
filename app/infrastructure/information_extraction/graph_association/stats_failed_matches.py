"""
统计 graph_association/logs 目录下 JSON 文件中匹配失败的节点。

功能：
    1. 读取 logs 目录下所有关联处理 JSON 文件
    2. 提取状态为"失败"的条目
    3. 全局按「节点ID」去重 —— 同一个节点ID无论出现在哪个文件中都只计一次
    4. 按「文件全称」分组统计不同节点ID的数量
    5. 输出为 CSV 表格（两列：文件全称、失败次数）
"""

import csv
import json
import os
from collections import defaultdict

# ---------------------------------------------------------------------------
# 路径配置
# ---------------------------------------------------------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(SCRIPT_DIR, "logs")
OUTPUT_CSV = os.path.join(SCRIPT_DIR, "failed_match_stats.csv")


def collect_failed_entries(log_dir: str) -> list[tuple]:
    """
    遍历 logs 目录下所有 JSON 文件，收集匹配失败的条目。

    Returns:
        list of (节点ID, 文件全称)
    """
    failed_entries = []

    for fname in sorted(os.listdir(log_dir)):
        if not fname.endswith(".json"):
            continue

        fpath = os.path.join(log_dir, fname)
        with open(fpath, "r", encoding="utf-8") as f:
            data = json.load(f)

        items = data.get("处理明细", [])
        for item in items:
            if item.get("状态") != "失败":
                continue
            middle = item.get("中间节点", {})
            node_id = middle.get("节点ID", "")
            if not node_id:
                continue
            file_full_name = middle.get("文件全称", "")
            failed_entries.append((node_id, file_full_name))

    return failed_entries


def dedup_and_count(entries: list[tuple]) -> dict:
    """
    全局按节点ID去重，然后按文件全称分组统计。

    Returns:
        dict: {文件全称: 不同节点ID数量}
    """
    # 全局按节点ID去重，只保留首次出现的文件全称
    seen_ids: dict[str, str] = {}
    for node_id, file_full_name in entries:
        if node_id not in seen_ids:
            seen_ids[node_id] = file_full_name

    # 按文件全称分组计数
    counter = defaultdict(int)
    for file_full_name in seen_ids.values():
        counter[file_full_name] += 1

    return counter


def write_csv(stats: dict, output_path: str):
    """将统计结果写入 CSV 文件，按失败次数降序排列。"""
    with open(output_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["文件全称", "失败次数"])
        for file_full_name, count in sorted(
            stats.items(), key=lambda item: -item[1]
        ):
            writer.writerow([file_full_name, count])


def main():
    entries = collect_failed_entries(LOG_DIR)
    total_raw = len(entries)
    stats = dedup_and_count(entries)
    total_unique_nodes = sum(stats.values())
    total_groups = len(stats)

    print(f"原始失败记录数: {total_raw}")
    print(f"去重后唯一失败节点总数: {total_unique_nodes}")
    print(f"按文件全称分组记录数: {total_groups}")
    print()

    write_csv(stats, OUTPUT_CSV)
    print(f"CSV 已写入: {OUTPUT_CSV}")

    print()
    print("--- 失败次数 Top 20 ---")
    for file_full_name, count in sorted(
        stats.items(), key=lambda item: -item[1]
    )[:20]:
        print(f"  [{count}次] {file_full_name}")


if __name__ == "__main__":
    main()
