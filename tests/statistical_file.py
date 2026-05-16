import os

def count_files(directory):
    """
    统计指定目录及其子目录下的所有文件数量

    Args:
        directory: 要统计的目录路径

    Returns:
        文件总数
    """
    file_count = 0

    # 遍历目录及其所有子目录
    for root, dirs, files in os.walk(directory):
        file_count += len(files)

    return file_count

if __name__ == "__main__":
    # 目标目录路径
    target_dir = r"D:\CogmAIT\8.1项目\8.1数据\数据源\合规指引20260108"

    # 检查目录是否存在
    if not os.path.exists(target_dir):
        print(f"错误：目录不存在 - {target_dir}")
    else:
        total_files = count_files(target_dir)
        print(f"目录：{target_dir}")
        print(f"文件总数：{total_files}")


# F:\企业大脑知识库系统\8.1项目\爬取的数据\国家规章库\国家规章库
# F:\企业大脑知识库系统\8.1项目\爬取的数据\flk_out_api\flk_out_api
# F:\企业大脑知识库系统\8.1项目\法律法规\裁判文书网（分类后） (2)\裁判文书网（分类后）
# 上面修改为 D:\CogmAIT\8.1项目\8.1数据\爬取的数据\裁判文书网v2（分类后）\裁判文书网（分类后）
# F:\企业大脑知识库系统\8.1项目\裁判文书网_续\裁判文书网_续
# 上面修改为 D:\CogmAIT\8.1项目\8.1数据\爬取的数据\裁判文书网_续（2月）\裁判文书网_续
# D:\CogmAIT\8.1项目\8.1数据\爬取的数据\裁判文书网_续（3月）\裁判文书网_续
# D:\CogmAIT\8.1项目\8.1数据\爬取的数据\3月