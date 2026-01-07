result_list = [
    "《中华人民共和国刑法》",
    "《刑法》",
    "《中华人民共和国反不正当竞争法》",
    "《反不正当竞争法》",
    "《中华人民共和国反垄断法》",
    "《反垄断法》",
    "《中华人民共和国劳动合同法》",
    "《劳动合同法》",
    "《中华人民共和国劳动法》",
    "《劳动法》",
    "《中华人民共和国劳动争议调解仲裁法》",
    "《中华人民共和国社会保险法》",
    "《中华人民共和国工伤保险条例》",
    "《中华人民共和国对外劳务合作管理条例》",
    "《中华人民共和国对外承包工程管理条例》",
    "《中华人民共和国外汇管理条例》",
    "《中华人民共和国数据安全法》",
    "《数据安全法》",
    "《中华人民共和国网络安全法》",
    "《网络安全法》",
    "《中华人民共和国国家安全法》",
    "《国家安全法》",
    "《中华人民共和国监察法》",
    "《监察法》",
    "《中华人民共和国企业国有资产法》",
    "《中华人民共和国民法典》",
    "《民法典》",
    "《中华人民共和国民法通则》",
    "《民法通则》",
    "《中华人民共和国民事诉讼法》",
    "《刑事诉讼法》",
    "《中华人民共和国反洗钱法》",
    "《反洗钱法》",
    "《美国反海外腐败法》",
    "《美国反外国腐败法指南》",
    "《英国反贿赂法》",
    "《欧洲共同体条约》",
    "《欧盟运行条约》",
    "《欧盟合并控制条例》",
    "《谢尔曼法》",
    "《伊朗交易制裁条例》",
    "《古巴资产控制条例》",
    "《乌克兰制裁条例》",
    "《出口管制条例》",
    "《国际油污损害民事责任公约》",
    "《清洁水法》",
    "《石油污染法》",
    "《巴西环境犯罪法》",
    "《联邦宪法》",
    "《石油法》",
    "《美国经济间谍法》",
    "《经济间谍法》",
    "《统一商业秘密法》",
    "《美国洗钱法》",
    "《有组织及严重罪行条例》",
    "《美国出口管制条例》",
    "《美国外国投资风险审查现代化法案》",
    "《外国投资风险审查现代化法案》",
    "《职工带薪年休假条例》",
    "《企业职工带薪年休假实施办法》",
    "《最高人民法院、最高人民检察院关于办理贪污贿赂刑事案件适用法律若干问题的解释》",
    "《贪污贿赂司法解释》",
    "《最高人民检察院关于人民检察院直接受理立案侦查案件立案标准的规定（试行）》",
    "《关于办理受贿刑事案件适用法律若干问题的意见》",
    "《最高人民法院、最高人民检察院关于办理侵犯知识产权刑事案件具体应用法律若干问题的解释》",
    "《最高人民法院、最高人民检察院关于办理侵犯知识产权刑事案件具体应用法律若干问题的解释（二）》",
    "《最高人民法院、最高人民检察院关于办理侵犯知识产权刑事案件具体应用法律若干问题的解释（三）》",
    "《最高人民法院关于审理不正当竞争民事案件应用法律若干问题的解释》",
    "《不正当竞争司法解释》",
    "《最高人民法院关于审理侵犯商业秘密民事案件适用法律若干问题的规定》",
    "《商业秘密司法解释》",
    "《最高人民法院关于审理骗购外汇、非法买卖外汇刑事案件具体应用法律若干问题的解释》",
    "《最高人民法院关于审理工伤保险行政案件若干问题的规定》",
    "《国务院反垄断委员会关于相关市场界定的指南》",
    "《国务院办公厅关于建立国有企业违规经营投资责任追究制度的意见》",
    "《国务院办公厅转发国家发展改革委商务部人民银行外交部关于进一步引导和规范境外投资方向指导意见的通知》",
    "《企业境外投资管理办法》",
    "《中央企业境外国有资产监督管理暂行办法》",
    "《中央企业境外国有产权管理暂行办法》",
    "《中央企业境外投资监督管理办法》",
    "《中央企业投资监督管理办法》",
    "《国有企业境外投资财务管理办法》",
    "《企业国有资产评估管理暂行办法》",
    "《网络安全审查办法》",
    "《网络安全审查办法（修订草案征求意见稿）》",
    "《关键信息基础设施安全保护条例》",
    "《汽车数据安全管理若干规定（试行）》",
    "《汽车数据安全管理若干规定（征求意见稿）》",
    "《北京市高级人民法院、北京市劳动人事争议仲裁委员会关于审理劳动争议案件法律适用问题的解答》",
    "《根据第1/2003号条例第23（2）（a）条确定罚款的方法指南》",
    "《关于实施条约第81条和第82条制定的竞争规则的第1/2003号理事会条例》",
    "《第1/2003号条例》",
    "《宪法第五修正案》",
    "《通用数据保护条例》",
    "《个人外汇管理办法》",
    "《国防生产法》",
    "《企业国有资产监督管理暂行条例》"
]

import os
import re
from difflib import SequenceMatcher
from typing import List, Tuple, Dict

def extract_law_name(text: str) -> str:
    """
    提取《》中的文件名

    Args:
       text: 包含《》的字符串

    Returns:
       提取出的文件名
       """
    match = re.search(r'《([^》]+)》', text)
    if match:
        return match.group(1)
    return text


def remove_punctuation(text: str) -> str:
    """
    去除字符串中的所有标点符号

    Args:
        text: 输入字符串

    Returns:
        去除标点符号后的字符串
    """
    # 使用正则表达式去除所有标点符号
    # 匹配中文标点、英文标点和特殊符号
    return re.sub(r'[^\w\s]', '', text)


def similarity(a: str, b: str) -> float:
    """
    计算两个字符串的相似度

    Args:
        a: 字符串a
        b: 字符串b

    Returns:
        相似度（0-1之间的浮点数）
    """
    return SequenceMatcher(None, a, b).ratio()


def find_law_files_with_similarity(
    law_names: List[str],
    search_directory: str,
    threshold: float = 0.9
) -> Tuple[Dict[str, str], List[str]]:
    """
    在指定目录下搜索与法律文件名相似的文件

    Args:
        law_names: 法律文件名列表
        search_directory: 搜索目录路径
        threshold: 相似度阈值

    Returns:
        tuple: (找到的文件名映射字典, 未找到的文件名列表)
    """
    # 提取《》中的文件名并去除标点符号
    extracted_names = {}
    for name in law_names:
        extracted = extract_law_name(name)
        cleaned_extracted = remove_punctuation(extracted)
        extracted_names[name] = (extracted, cleaned_extracted)

    # 搜索目录下所有文件
    found_files = {}
    not_found_files = []

    # 遍历搜索目录及其子目录
    for root, dirs, files in os.walk(search_directory):
        for file in files:
            # 获取文件名（不包含扩展名）
            file_name_without_ext = os.path.splitext(file)[0]
            # 去除文件名中的标点符号
            cleaned_file_name = remove_punctuation(file_name_without_ext)

            # 检查是否与任何法律文件名匹配
            for original_name, (extracted_name, cleaned_extracted) in extracted_names.items():
                # 计算去除标点符号后的相似度
                sim_score = similarity(cleaned_extracted, cleaned_file_name)

                # 如果相似度高于阈值且未被匹配过
                if sim_score >= threshold and original_name not in found_files:
                    # 只保存文件名，不包括路径
                    found_files[original_name] = file
                    break

    # 找出未找到的文件
    for original_name in law_names:
        if original_name not in found_files:
            not_found_files.append(original_name)

    return found_files, not_found_files


def save_results_to_file(found_files: Dict[str, str], not_found_files: List[str], output_file: str):
    """
    将匹配结果保存到文件

    Args:
        found_files: 找到的文件名映射字典
        not_found_files: 未找到的文件名列表
        output_file: 输出文件路径
    """
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("匹配成功的文件名映射:\n")
        f.write("-" * 50 + "\n")
        for law_name, file_name in found_files.items():
            extracted = extract_law_name(law_name)
            f.write(f"{law_name} -> {file_name} (匹配: {extracted} vs {os.path.splitext(file_name)[0]})\n")

        f.write("\n匹配失败的文件名:\n")
        f.write("-" * 50 + "\n")
        for law_name in not_found_files:
            extracted = extract_law_name(law_name)
            f.write(f"{law_name} (提取名: {extracted})\n")

        f.write(f"\n统计信息:\n")
        f.write(f"总共查询: {len(found_files) + len(not_found_files)} 个法律文件\n")
        f.write(f"匹配成功: {len(found_files)} 个文件\n")
        f.write(f"匹配失败: {len(not_found_files)} 个文件\n")


# 使用示例
def main():
    search_directory = r"F:\企业大脑知识库系统\8.1项目\数据处理\清洗的数据"
    output_file = r"/app/tests/law_instances/law_file_match_results.txt"

    found_files, not_found_files = find_law_files_with_similarity(
        result_list,
        search_directory,
        threshold=0.9
    )

    # 打印结果到控制台
    print("匹配成功的文件名映射:")
    print("-" * 50)
    for law_name, file_name in found_files.items():
        extracted = extract_law_name(law_name)
        print(f"{law_name} -> {file_name} (匹配: {extracted} vs {os.path.splitext(file_name)[0]})")

    print("\n匹配失败的文件名:")
    print("-" * 50)
    for law_name in not_found_files:
        extracted = extract_law_name(law_name)
        print(f"{law_name} (提取名: {extracted})")

    print(f"\n统计信息:")
    print(f"总共查询: {len(result_list)} 个法律文件")
    print(f"匹配成功: {len(found_files)} 个文件")
    print(f"匹配失败: {len(not_found_files)} 个文件")

    # 将结果保存到文件
    save_results_to_file(found_files, not_found_files, output_file)
    print(f"\n结果已保存到: {output_file}")


if __name__ == "__main__":
    main()
