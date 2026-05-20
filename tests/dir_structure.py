import os
from pathlib import Path

def generate_directory_structure(root_path, output_file=None, show_files=True, max_depth=None):
    """
    生成目录结构树

    Args:
        root_path: 根目录路径
        output_file: 输出文件路径（可选），如果为 None 则打印到控制台
        show_files: 是否显示文件
        max_depth: 最大深度（可选），None 表示不限制
    """
    root = Path(root_path).resolve()
    structure_lines = []

    def get_tree_string(path, prefix="", is_last=True, current_depth=0):
        """递归生成目录树字符串"""
        # 检查深度限制
        if max_depth is not None and current_depth > max_depth:
            return ""

        connector = "└── " if is_last else "├── "
        name = ""
        if path.is_dir():
            name = "📁"
        else:
            name = "📄"
        name += path.name + "/" if path.is_dir() else path.name
        result = f"{prefix}{connector}{name}\n"

        # 如果是目录，继续递归
        if path.is_dir():
            # 更新前缀
            new_prefix = prefix + ("    " if is_last else "│   ")

            # 获取目录内容并排序
            try:
                items = sorted(path.iterdir(), key=lambda x: (not x.is_dir(), x.name.lower()))
                for i, item in enumerate(items):
                    # 跳过隐藏文件和 .git 目录
                    if item.name.startswith('.') and item.name != '.qoder':
                        continue
                    if item.name == '.git':
                        continue

                    is_last_item = (i == len(items) - 1)
                    result += get_tree_string(item, new_prefix, is_last_item, current_depth + 1)
            except PermissionError:
                pass

        return result

    # 添加标题
    structure_lines.append(f"目录结构:\n")
    structure_lines.append(f" {root}\n")

    # 获取根目录下的所有项
    try:
        items = sorted(root.iterdir(), key=lambda x: (not x.is_dir(), x.name.lower()))
        for i, item in enumerate(items):
            # 跳过隐藏文件和 .git 目录
            if item.name.startswith('.') and item.name != '.qoder':
                continue
            if item.name == '.git':
                continue

            is_last = (i == len(items) - 1)
            tree_str = get_tree_string(item, "", is_last, current_depth=1)
            structure_lines.append(tree_str)
    except PermissionError as e:
        structure_lines.append(f"错误：无法访问目录 - {e}\n")

    # 组合结果
    structure_text = "".join(structure_lines)

    # 输出到文件或控制台
    if output_file:
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(structure_text)
        print(f"目录结构已保存到：{output_path}")
    else:
        print(structure_text)

    return structure_text


def count_files_and_dirs(root_path):
    """统计文件和目录数量"""
    root = Path(root_path).resolve()
    file_count = 0
    dir_count = 0

    for item in root.rglob('*'):
        if item.name.startswith('.') and item.name != '.qoder':
            continue
        if item.name == '.git':
            continue

        if item.is_dir():
            dir_count += 1
        else:
            file_count += 1

    return file_count, dir_count


if __name__ == "__main__":
    # 目标目录：国家规章库
    target_dir = r"D:\CogmAIT\8.1项目\8.1数据\爬取的数据\海外法律文件\法律文件"

    print("=" * 60)
    print("生成国家规章库目录结构")
    print("=" * 60)

    # 检查目录是否存在
    if not os.path.exists(target_dir):
        print(f"错误：目录不存在 - {target_dir}")
    else:
        # 方案 1：输出到控制台
        print("\n【输出目录结构】:\n")
        generate_directory_structure(target_dir, show_files=True)

        # 方案 2：保存到文件
        output_path = r"D:\CogmAIT\8.1项目\8.1数据\爬取的数据" + r"\国家规章库目录结构.txt"
        generate_directory_structure(target_dir, output_file=output_path, show_files=True)

        # 统计信息
        files, dirs = count_files_and_dirs(target_dir)
        print("\n" + "=" * 60)
        print(f"统计信息：{dirs} 个目录，{files} 个文件")
        print("=" * 60)
