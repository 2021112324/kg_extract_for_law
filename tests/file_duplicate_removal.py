import os

def print_directory_structure(root_path, prefix="", max_depth=None, current_depth=0):
    """
    递归打印目录结构

    Args:
        root_path: 根目录路径
        prefix: 前缀字符串，用于表示层级
        max_depth: 最大深度限制
        current_depth: 当前递归深度
    """
    if max_depth is not None and current_depth > max_depth:
        return

    # 获取目录内容
    try:
        items = sorted(os.listdir(root_path))
    except PermissionError:
        print(f"{prefix}[Permission Denied]")
        return
    except FileNotFoundError:
        print(f"{prefix}[Directory not found: {root_path}]")
        return

    directories = []
    files = []

    # 分离目录和文件
    for item in items:
        item_path = os.path.join(root_path, item)
        if os.path.isdir(item_path):
            directories.append(item)
        else:
            files.append(item)

    # 打印当前目录的所有子目录
    for i, directory in enumerate(directories):
        is_last = (i == len(directories) - 1) and (len(files) == 0)
        current_prefix = "└── " if is_last else "├── "
        print(f"{prefix}{current_prefix}{directory}/")

        # 递归打印子目录
        next_prefix = prefix + ("    " if is_last else "│   ")
        sub_dir_path = os.path.join(root_path, directory)
        print_directory_structure(sub_dir_path, next_prefix, max_depth, current_depth + 1)

    # 打印当前目录的所有文件
    for i, file in enumerate(files):
        is_last = (i == len(files) - 1)
        current_prefix = "└── " if is_last else "├── "
        print(f"{prefix}{current_prefix}{file}")


def main():
    # 设置源目录和输出目录
    source_directory = r"F:\企业大脑知识库系统\8.1项目\数据处理\清洗的数据\国家规章库"
    output_directory = r"F:\企业大脑知识库系统\8.1项目\抽取代码\kg_extract_for_law\tests\output"
    output_file = os.path.join(output_directory, "directory_structure.txt")

    # 创建输出目录（如果不存在）
    os.makedirs(output_directory, exist_ok=True)

    # 捕获目录结构输出
    import sys
    from io import StringIO

    old_stdout = sys.stdout
    sys.stdout = captured_output = StringIO()

    try:
        print(f"目录结构:\n {source_directory}")
        print_directory_structure(source_directory)

        # 获取捕获的输出
        output_str = captured_output.getvalue()
    finally:
        sys.stdout = old_stdout

    # 写入输出文件
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(output_str)

    print(f"国家规章库目录结构已保存到: {output_file}")
    print("\n目录结构如下:")
    print(output_str)


if __name__ == "__main__":
    main()