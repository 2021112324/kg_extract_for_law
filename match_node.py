#!/usr/bin/env python3
# -*- encoding utf-8 -*-
import os
import random
import re
import string
from difflib import SequenceMatcher
from typing import List, Optional

node_list = [
    '专利标识标注办法',
    '专利申请人和专利权人（单位）代码标准',
    '专利申请号标准',
    '专利行政执法办法',
    '专利费用基本信息代码规范(试行)',
    '专利费用基本信息代码规范（试行）',
    '关于专利电子申请的规定',
    '关于台湾同胞专利申请的若干规定',
    '关于规范专利申请行为的若干规定',
    '施行修改后的专利法实施细则的过渡办法',
    '施行修改后的专利法的过渡办法',
    '用于专利程序的生物材料保藏办法',
    '个人信息出境标准合同办法',
    '个人信息出境认证办法',
    '儿童个人信息网络保护规定',
    '电信和互联网用户个人信息保护规定',
    '中华人民共和国工业产品生产许可证管理条例实施办法',
    '中华人民共和国海关关于加工贸易边角料、剩余料件、残次品、副产品和受灾保税货物的管理办法',
    '产品质量监督抽查管理暂行办法',
    '产品防伪监督管理办法',
    '农业机械产品修理、更换、退货责任规定',
    '地理标志产品保护规定',
    '婴幼儿配方乳粉产品配方注册管理办法',
    '安全技术防范产品管理办法',
    '家用汽车产品修理更换退货责任规定',
    '工业产品生产单位落实质量安全主体责任监督管理规定',
    '合规经营法律规章',
    '专利代理人代码标准',
    '专利代理师资格考试办法',
    '专利代理管理办法',
    '专利优先审查管理办法',
    '专利实施强制许可办法',
    '专利实施许可合同备案办法',
    '专利审查指南（2010）',
    '专利数据元素标准第1部分_关于用XML处理复审请求审查决定、无效请求审查决定和司法判决文件的暂行办法',
    '专利数据元素标准第2部分_关于用XML处理中国发明、实用新型专利文献数据的暂行办法',
    '专利权质押登记办法',
    '工业产品销售单位落实质量安全主体责任监督管理规定',
    '废弃电器电子产品处理资格许可管理办法',
    '强制性产品认证机构和实验室管理办法',
    '强制性产品认证管理规定',
    '无公害农产品管理办法',
    '有机产品认证管理办法',
    '民用航空产品和零部件合格审定规定',
    '汽车产品外部标识管理办法',
    '消防产品监督管理规定',
    '电力产品增值税征收管理办法',
    '电器电子产品有害物质限制使用管理办法',
    '缺陷汽车产品召回管理条例实施办法',
    '节能低碳产品认证管理办法',
    '计量器具新产品管理办法',
    '进出境转基因产品检验检疫管理办法',
    '进出境非食用动物产品检验检疫监督管理办法',
    '进口旧机电产品检验监督管理办法',
    '进境动物和动物产品风险分析管理规定',
    '进境植物和植物产品风险分析管理规定',
    '金融机构产品适当性管理办法',
    '铁路专用设备缺陷产品召回管理办法',
    '银行业金融机构衍生产品交易业务管理暂行办法',
    '食品相关产品质量安全监督管理暂行办法',
    '食用农产品市场销售质量安全监督管理办法',
    '商标代理监督管理规定',
    '商标印制管理办法',
    '商标评审规则',
    '规范商标申请注册行为若干规定',
    '集体商标、证明商标注册和管理办法',
    '驰名商标认定和保护规定',
    '中央企业安全生产监督管理暂行办法',
    '中央企业安全生产禁令',
    '交通运输工程施工单位主要负责人、项目负责人和专职安全生产管理人员安全生产考核管理办法',
    '公路水运工程安全生产监督管理办法',
    '冶金企业和有色金属企业安全生产规定',
    '危险化学品生产企业安全生产许可证实施办法',
    '国防科研生产安全事故报告和调查处理办法',
    '安全生产事故隐患排查治理暂行规定',
    '安全生产培训管理办法',
    '安全生产监督罚款管理暂行办法',
    '安全生产监管监察职责和行政执法责任追究的规定',
    '安全生产监管监察部门信息公开办法',
    '安全生产行政处罚自由裁量适用规则（试行）',
    '安全生产行政复议规定',
    '安全生产违法行为行政处罚办法',
    '安全生产领域违法违纪行为政纪处分暂行规定',
    '建筑施工企业主要负责人、项目负责人和专职安全生产管理人员安全生产管理规定',
    '建筑施工企业安全生产许可证管理规定',
    '海洋石油安全生产规定',
    '烟花爆竹生产企业安全生产许可证实施办法',
    '烟花爆竹生产经营安全规定',
    '煤矿企业安全生产许可证实施办法',
    '特种设备生产单位落实质量安全主体责任监督管理规定',
    '生产安全事故信息报告和处置办法',
    '生产安全事故应急预案管理办法',
    '生产安全事故罚款处罚规定（试行）',
    '生产经营单位安全培训规定',
    '电力安全生产监督管理办法',
    '运输机场专业工程建设质量和安全生产监督管理规定',
    '金属与非金属矿产资源地质勘探安全生产监督管理暂行规定',
    '非煤矿矿山企业安全生产许可证实施办法',
    '食品生产企业安全生产监督管理暂行规定',
    '数据出境安全评估办法',
    '汽车数据安全管理若干规定（试行）',
    '中国政府网',
    '交通运输标准化管理办法',
    '企业标准化促进办法',
    '企业标准化管理办法',
    '全国专业标准化技术委员会管理办法',
    '内河运输船舶标准化管理规定',
    '农业农村标准化管理办法',
    '农业标准化管理办法',
    '民用航空标准化管理规定',
    '化妆品标识管理规定',
    '水效标识管理办法',
    '能源效率标识管理办法',
    '食品标识监督管理办法',
    '中华人民共和国海上船舶污染事故调查处理规定',
    '中华人民共和国航运公司安全与防污染管理规定',
    '中华人民共和国船舶及其有关作业活动污染海洋环境防治管理规定',
    '中华人民共和国船舶污染海洋环境应急防备和应急处置管理规定',
    '中华人民共和国防治船舶污染内河水域环境管理规定',
    '固定污染源排污许可分类管理名录（2019年版）',
    '尾矿污染环境防治管理办法',
    '污染地块土壤环境管理办法',
    '污染源自动监控管理办法',
    '污染源自动监控设施现场监督检查办法',
    '电子废物污染环境防治管理办法',
    '重点管控新污染物清单（2023年版）',
    '防止含多氯联苯电力装置及其废物污染环境的规定',
    '饮用水水源保护区污染防治管理规定',
    '网络安全审查办法',
    '网络食品安全违法行为查处办法',
    '网络餐饮服务食品安全监督管理办法',
    '中华人民共和国海关管道运输进口能源监管办法',
    '中央企业节约能源与生态环境保护监督管理办法',
    '乘用车企业平均燃料消耗量与新能源汽车积分并行管理办法',
    '公共机构能源审计管理暂行办法',
    '公路、水路交通实施《中华人民共和国节约能源法》办法',
    '能源计量监督管理办法',
    '中国质量奖管理办法',
    '公路水运工程质量检测管理办法',
    '公路水运工程质量监督管理规定',
    '医疗器械使用质量监督管理办法',
    '医疗器械生产企业质量体系考核办法',
    '医疗机构制剂配制质量管理规范（试行）',
    '商品煤质量管理暂行办法',
    '建设工程勘察质量管理办法',
    '建设工程质量检测管理办法',
    '房屋建筑和市政基础设施工程质量监督管理规定',
    '房屋建筑工程质量保修办法',
    '林木种子质量管理办法',
    '核电厂质量保证安全规定',
    '毛绒纤维质量监督管理办法',
    '汽车维修质量纠纷调解办法',
    '电能质量管理办法（暂行）',
    '粮食质量安全监管办法',
    '纤维制品质量监督管理办法',
    '茧丝质量监督管理办法',
    '药品生产质量管理规范',
    '药品经营和使用质量监督管理办法',
    '药品经营质量管理规范',
    '药物非临床研究质量管理规范',
    '道路运输服务质量投诉管理规定',
    '铁路建设工程质量监督管理规定',
    '铁路设备质量安全监督管理办法',
    '铁路运输服务质量监督管理办法',
    '食盐质量安全监督管理办法',
    '麻类纤维质量监督管理办法',
    '计算机软件著作权登记办法',
    '专利',
    '个人信息',
    '产品',
    '商标',
    '安全生产',
    '数据安全',
    '标准化',
    '标识',
    '污染',
    '网络安全',
    '能源',
    '质量',
    '软件著作权'
    ]


def sanitize_neo4j_property_name(name: str) -> str:
    """
    将字符串中不能作为Neo4j属性名的符号替换为单个下划线，但保留中文符号

    Args:
        name (str): 原始字符串

    Returns:
        str: 处理后的符合Neo4j属性名规范的字符串
    """
    if not name:
        return name

    # 保留字母、数字、下划线、中文字符以及常见的中文符号
    # 包括中文括号（）、书名号《》、引号""、顿号、问号、感叹号、冒号、分号等
    pattern = r'[^a-zA-Z0-9_\u4e00-\u9fff\uFF08\uFF09\u300A\u300B\u201C\u201D\u3001\uFF1F\uFF01\uFF1A\uFF1B\u3002\uFF0C]'
    sanitized = re.sub(pattern, '_', name)

    # 将连续的下划线替换为单个下划线
    sanitized = re.sub(r'_+', '_', sanitized)

    # 移除开头和结尾的下划线
    sanitized = sanitized.strip('_')

    # 如果结果为空，返回默认名称
    if not sanitized:
        return ""

    # 确保不以数字开头（Cypher要求）
    if sanitized[0].isdigit():
        sanitized = '_' + sanitized

    return sanitized


def find_best_match(
        target: str,
        candidates: List[str],
        threshold: float = 0.9
) -> Optional[str]:
    """
    在字符串列表中查找与目标字符串相似度最高的结果

    Args:
        target (str): 目标字符串
        candidates (List[str]): 候选字符串列表
        threshold (float): 相似度阈值，默认0.9

    Returns:
        Optional[str]: 匹配度最高的字符串，若低于阈值则返回None
    """
    if not target or not candidates:
        return None

    best_match = None
    highest_similarity = 0.0

    for candidate in candidates:
        similarity = SequenceMatcher(None, target, candidate).ratio()
        if similarity > highest_similarity:
            highest_similarity = similarity
            best_match = candidate

    # 只有当最高相似度超过阈值时才返回结果
    if highest_similarity >= threshold:
        return best_match
    else:
        return None


def modify_string_randomly(original: str, modification_probability: float = 0.1) -> str:
    """
    以一定概率随机修改字符串中的字符

    Args:
        original (str): 原始字符串
        modification_probability (float): 修改概率，默认0.1

    Returns:
        str: 修改后的字符串
    """
    if not original or random.random() >= modification_probability:
        return original

    # 将字符串转换为列表以便修改
    chars = list(original)

    # 随机选择要修改的字符数量（1到总长度的30%）
    num_chars_to_modify = random.randint(1, max(1, len(chars) // 3))

    # 随机选择要修改的位置
    positions_to_modify = random.sample(range(len(chars)), min(num_chars_to_modify, len(chars)))

    # 对选定位置的字符进行修改
    for i in positions_to_modify:
        # 随机选择修改方式：替换、删除或插入
        modification_type = random.choice(['replace', 'delete', 'insert'])

        if modification_type == 'replace':
            # 替换为随机字符（中文、英文或数字）
            if random.random() < 0.5:
                # 替换为随机中文字符（简化处理，使用常见汉字）
                chars[i] = chr(random.randint(0x4e00, 0x9fff))
            else:
                # 替换为随机英文或数字
                chars[i] = random.choice(string.ascii_letters + string.digits)

        elif modification_type == 'delete':
            # 删除字符
            chars[i] = ''

        elif modification_type == 'insert':
            # 在当前位置前插入随机字符
            if random.random() < 0.5:
                insert_char = chr(random.randint(0x4e00, 0x9fff))  # 中文字符
            else:
                insert_char = random.choice(string.ascii_letters + string.digits)
            chars[i] = insert_char + chars[i]

    return ''.join(chars)



def get_random_files_from_directory(base_path: str, num_files: int = 5) -> List[str]:
    """
    从指定目录及其子目录中随机获取指定数量的文件名（不带后缀）

    Args:
        base_path (str): 基础目录路径
        num_files (int): 需要获取的文件数量

    Returns:
        List[str]: 不带后缀的文件名列表
    """
    all_files = []

    # 遍历目录及其子目录
    for root, dirs, files in os.walk(base_path):
        for file in files:
            # 去除文件后缀，只保留文件名主体部分
            filename_without_extension = os.path.splitext(file)[0]
            all_files.append(filename_without_extension)

    # 随机选择指定数量的文件
    if len(all_files) >= num_files:
        return random.sample(all_files, num_files)
    else:
        return all_files


# 测试函数
if __name__ == "__main__":

    # print("处理结果示例:")
    # for name in node_list[:50]:  # 显示前10个示例
    #     result = sanitize_neo4j_property_name(name)
    #     print(f"'{name}' -> '{result}'")

    # 测试用例
    test_strings = [
        '专利代理人代码标准',
        '专利申请号标凖',  # 故意写错的
        '安全生成监督管理办法',  # 安全生产写成了安全生成
        '不存在的法规'
    ]

    for test_str in test_strings:
        result = find_best_match(test_str, node_list)
        print(f"查找'{test_str}' -> 匹配结果: {result}")

    # # 获取随机文件名
    # base_directory = r"F:\企业大脑知识库系统\8.1项目\爬取的数据\output\国家规章库"
    #
    # try:
    #     # 获取5个随机文件名
    #     random_files = get_random_files_from_directory(base_directory, 5)
    #     print("原始文件名:")
    #     for i, filename in enumerate(random_files, 1):
    #         print(f"{i}. {filename}")
    #
    #     # 以0.2的概率修改这些文件名
    #     modified_files = []
    #     print("\n修改后的文件名:")
    #     for i, filename in enumerate(random_files, 1):
    #         modified_filename = modify_string_randomly(filename, 0.5)
    #         modified_files.append(modified_filename)
    #         print(f"{i}. '{filename}' -> '{modified_filename}'")
    #
    #     # 测试 find_best_match 函数
    #     print("\n测试 find_best_match 函数:")
    #     from match_node import find_best_match, node_list
    #
    #     for modified_file in modified_files:
    #         best_match = find_best_match(modified_file, node_list)
    #         print(f"查找 '{modified_file}' -> 匹配结果: {best_match}")
    #
    # except FileNotFoundError:
    #     print(f"目录 {base_directory} 不存在，请检查路径是否正确")
    # except Exception as e:
    #     print(f"发生错误: {e}")
