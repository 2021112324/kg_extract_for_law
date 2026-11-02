"""遍历指定目录下的子目录，统计各子目录的存储大小。

用法：
    python dir_cache_status.py [目标目录]

    - 不传参数时使用下方默认目录；
    - 传入目录路径时统计该目录下的各子目录大小。
"""

import os
import sys
from pathlib import Path


def get_directory_size(path):
    """递归计算目录的总存储大小（字节）。

    使用 os.scandir 遍历，跳过无法访问的条目和符号链接（避免死循环）。

    Args:
        path: 目录路径

    Returns:
        int: 目录总大小（字节）
    """
    total = 0
    try:
        with os.scandir(path) as entries:
            for entry in entries:
                try:
                    if entry.is_file(follow_symlinks=False):
                        total += entry.stat(follow_symlinks=False).st_size
                    elif entry.is_dir(follow_symlinks=False):
                        total += get_directory_size(entry.path)
                except (PermissionError, FileNotFoundError, OSError):
                    continue
    except (PermissionError, FileNotFoundError, OSError):
        pass
    return total


def format_size(size_bytes):
    """将字节数格式化为可读字符串（B/KB/MB/GB/TB）。"""
    units = ["B", "KB", "MB", "GB", "TB"]
    value = float(size_bytes)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.2f} {unit}"
        value /= 1024
    return f"{size_bytes} B"


def count_directory_sizes(root_path):
    """统计指定目录下各子目录的存储大小。

    Args:
        root_path: 根目录路径

    Returns:
        list[tuple[str, int]]: 按大小降序排列的 (目录名, 大小字节) 列表
    """
    root = Path(root_path)
    results = []

    if not root.is_dir():
        print(f"错误：目录不存在 - {root}")
        return results

    try:
        with os.scandir(root_path) as entries:
            subdirs = [entry for entry in entries if entry.is_dir(follow_symlinks=False)]
    except (PermissionError, FileNotFoundError, OSError) as exc:
        print(f"错误：无法访问目录 - {exc}")
        return results

    for entry in subdirs:
        size = get_directory_size(entry.path)
        results.append((entry.name, size))

    results.sort(key=lambda item: item[1], reverse=True)
    return results


if __name__ == "__main__":
    # 目标目录：优先取命令行参数，否则使用默认目录
    target_dir = sys.argv[1] if len(sys.argv) > 1 else rf"D:\CogmAIT\2.3行政复议项目"

    print("=" * 60)
    print(f"统计目录下各子目录的存储大小：{target_dir}")
    print("=" * 60)

    results = count_directory_sizes(target_dir)

    if not results:
        print("未找到子目录。")
    else:
        total = sum(size for _, size in results)
        print(f"\n子目录总数：{len(results)} 个")
        print(f"总大小：{format_size(total)}")
        print("\n按大小降序排列：\n")
        print(f"{'目录名':<50} {'大小':>12}")
        print("-" * 64)
        for name, size in results:
            print(f"{name:<50} {format_size(size):>12}")
