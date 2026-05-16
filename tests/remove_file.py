#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
对比两个目录并删除重复文件
对比 D:\CogmAIT\8.1项目\8.1数据\爬取的数据\裁判文书网_续（2-3月&deduplicated）\裁判文书网_续
和 D:\CogmAIT\8.1项目\8.1数据\爬取的数据\裁判文书网_续（2月）\裁判文书网_续
删除第一个目录中的重复文件
"""

import os
import hashlib
from pathlib import Path
from typing import Dict, Set, Tuple
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('duplicate_removal.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def calculate_file_md5(file_path: Path, chunk_size: int = 8192) -> str:
    """计算文件的MD5哈希值"""
    md5_hash = hashlib.md5()
    try:
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(chunk_size), b''):
                md5_hash.update(chunk)
        return md5_hash.hexdigest()
    except Exception as e:
        logger.error(f"计算文件 {file_path} 的MD5失败: {e}")
        return None


def scan_directory(directory: Path) -> Dict[str, Dict[str, any]]:
    """
    扫描目录，返回文件信息字典
    返回格式: {相对路径: {'absolute_path': 绝对路径, 'size': 文件大小, 'md5': MD5哈希}}
    """
    files_info = {}

    if not directory.exists():
        logger.error(f"目录不存在: {directory}")
        return files_info

    logger.info(f"开始扫描目录: {directory}")

    for file_path in directory.rglob('*'):
        if file_path.is_file():
            try:
                relative_path = file_path.relative_to(directory)
                file_size = file_path.stat().st_size

                files_info[str(relative_path)] = {
                    'absolute_path': file_path,
                    'size': file_size,
                    'md5': None  # 延迟计算MD5
                }
            except Exception as e:
                logger.warning(f"处理文件 {file_path} 时出错: {e}")

    logger.info(f"扫描完成，共找到 {len(files_info)} 个文件")
    return files_info


def find_duplicates(dir1: Path, dir2: Path) -> Set[str]:
    """
    查找两个目录中的重复文件
    返回在dir1中需要删除的文件的相对路径集合
    """
    logger.info("=" * 60)
    logger.info("开始扫描两个目录...")
    logger.info("=" * 60)

    # 扫描两个目录
    files1 = scan_directory(dir1)
    files2 = scan_directory(dir2)

    if not files1:
        logger.warning(f"目录1中没有找到文件: {dir1}")
        return set()

    if not files2:
        logger.warning(f"目录2中没有找到文件: {dir2}")
        return set()

    # 第一步：通过文件名和大小快速筛选可能的重复文件
    logger.info("\n第一步：通过文件名和大小初步筛选...")
    potential_duplicates = set()

    for rel_path, info1 in files1.items():
        if rel_path in files2:
            info2 = files2[rel_path]
            if info1['size'] == info2['size']:
                potential_duplicates.add(rel_path)

    logger.info(f"初步筛选出 {len(potential_duplicates)} 个可能的重复文件")

    if not potential_duplicates:
        logger.info("没有找到重复文件")
        return set()

    # 第二步：通过MD5精确比对
    logger.info("\n第二步：通过MD5哈希精确比对...")
    duplicates_to_delete = set()
    total = len(potential_duplicates)

    for idx, rel_path in enumerate(potential_duplicates, 1):
        if idx % 100 == 0:
            logger.info(f"进度: {idx}/{total}")

        info1 = files1[rel_path]
        info2 = files2[rel_path]

        # 计算MD5（如果还没计算）
        if info1['md5'] is None:
            info1['md5'] = calculate_file_md5(info1['absolute_path'])

        if info2['md5'] is None:
            info2['md5'] = calculate_file_md5(info2['absolute_path'])

        # 比较MD5
        if info1['md5'] and info2['md5'] and info1['md5'] == info2['md5']:
            duplicates_to_delete.add(rel_path)

    logger.info(f"\n精确比对完成，确认 {len(duplicates_to_delete)} 个重复文件")
    return duplicates_to_delete


def delete_duplicates(dir1: Path, duplicates: Set[str], dry_run: bool = True) -> Tuple[int, int]:
    """
    删除重复文件
    :param dir1: 目标目录
    :param duplicates: 要删除的文件相对路径集合
    :param dry_run: 是否为试运行模式（不实际删除）
    :return: (成功删除数量, 失败数量)
    """
    success_count = 0
    fail_count = 0

    mode = "试运行" if dry_run else "正式删除"
    logger.info("\n" + "=" * 60)
    logger.info(f"开始{mode}重复文件...")
    logger.info("=" * 60)

    for rel_path in sorted(duplicates):
        file_path = dir1 / rel_path

        try:
            if not file_path.exists():
                logger.warning(f"文件不存在，跳过: {rel_path}")
                fail_count += 1
                continue

            file_size = file_path.stat().st_size

            if dry_run:
                logger.info(f"[试运行] 将删除: {rel_path} (大小: {file_size / 1024:.2f} KB)")
                success_count += 1
            else:
                file_path.unlink()
                logger.info(f"[已删除] {rel_path} (大小: {file_size / 1024:.2f} KB)")
                success_count += 1

        except Exception as e:
            logger.error(f"处理文件失败 {rel_path}: {e}")
            fail_count += 1

    return success_count, fail_count


def main():
    """主函数"""
    # 定义两个目录
    dir1_str = r"D:\CogmAIT\8.1项目\8.1数据\爬取的数据\裁判文书网_续（2-3月&deduplicated）\裁判文书网_续"
    dir2_str = r"D:\CogmAIT\8.1项目\8.1数据\爬取的数据\裁判文书网_续（2月）\裁判文书网_续"

    dir1 = Path(dir1_str)
    dir2 = Path(dir2_str)

    logger.info("=" * 60)
    logger.info("重复文件检测和删除工具")
    logger.info("=" * 60)
    logger.info(f"目录1（将被清理）: {dir1}")
    logger.info(f"目录2（参考目录）: {dir2}")
    logger.info("")

    # 验证目录存在
    if not dir1.exists():
        logger.error(f"目录1不存在: {dir1}")
        return

    if not dir2.exists():
        logger.error(f"目录2不存在: {dir2}")
        return

    # 查找重复文件
    duplicates = find_duplicates(dir1, dir2)

    if not duplicates:
        logger.info("\n✓ 没有发现重复文件，无需操作")
        return

    # 显示统计信息
    total_size = 0
    for rel_path in duplicates:
        try:
            file_path = dir1 / rel_path
            if file_path.exists():
                total_size += file_path.stat().st_size
        except:
            pass

    logger.info("\n" + "=" * 60)
    logger.info("重复文件统计:")
    logger.info(f"  重复文件数量: {len(duplicates)}")
    logger.info(f"  总大小: {total_size / (1024*1024):.2f} MB")
    logger.info("=" * 60)

    # 试运行模式（默认）
    logger.info("\n首先进行试运行（不会实际删除文件）...")
    success_dry, fail_dry = delete_duplicates(dir1, duplicates, dry_run=True)

    logger.info(f"\n试运行结果: 成功 {success_dry}, 失败 {fail_dry}")

    # 询问是否执行正式删除
    print("\n" + "=" * 60)
    response = input("是否执行正式删除？(yes/no): ").strip().lower()

    if response == 'yes':
        logger.info("\n执行正式删除...")
        success_real, fail_real = delete_duplicates(dir1, duplicates, dry_run=False)

        logger.info("\n" + "=" * 60)
        logger.info("删除完成!")
        logger.info(f"  成功删除: {success_real} 个文件")
        logger.info(f"  失败: {fail_real} 个文件")
        logger.info(f"  释放空间: {total_size / (1024*1024):.2f} MB")
        logger.info("=" * 60)
    else:
        logger.info("\n已取消操作，未删除任何文件")


if __name__ == "__main__":
    main()


