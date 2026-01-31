import asyncio
import logging
import re


class ClauseExtractor:
    def __init__(self):
        pass

    async def split_clause(
            self,
            filename: str,
            graph_tag: str,
            text: str
    ) -> dict:
        """
实现功能：切分法规文件，将其整理成法规文件基础信息、条款的结构化数据，大致逻辑如下：
1. file_info记录文件开头内容，current_chapter记录当前章，current_section记录当前节。
2. 读取文件内容，若读取到章内容（即"第X章 XXX"），则current_chapter记录当前章内容（即"第X章 XXX"）；
    若读取到节内容（即"第X节 XXX"），则current_section记录当前节内容（即"第X节 XXX"）；
    若读取到条款内容（即"第X条 XXX"），则将current_chapter、current_section、条款内容记录到结果中；
3. file_info从开头开始记录，如果读取到"第一节"，则将"第一节"前的内容记录到file_info中，并删除file_info文本末尾的章和节（如果有的话）
4. 条款内容为上一个"第X条"到下一个"第X条"之间的内容，或者读到章或节的标志，或者读到文件末尾，或者读到"附录"、"附件"等条款部分结束标志。
    切分后的数据结构：
    {
        "filename": "文件名称",
        "graph_tag": "图谱标签",
        "file_info": "文件开头内容",
        "clauses": [
            {
                "章": "章内容",
                "节": "节内容",
                "条款编号": "第几条"
                "条款内容": "条款内容，不包含开头的"第几条""
            }
        ]
    }
        :param text:
        :param filename:
        :param graph_tag:
        :return:
        """
        # 定义正则表达式模式 - 匹配行首的"第X章/节/条 "格式（中间可能有空格，但后面必须有空格）
        chapter_pattern = re.compile(r'^第[一二三四五六七八九十百千万\d]+\s*章\s+.*', re.MULTILINE)
        section_pattern = re.compile(r'^第[一二三四五六七八九十百千万\d]+\s*节\s+.*', re.MULTILINE)
        clause_pattern = re.compile(r'^第([一二三四五六七八九十百千万\d]+)\s*条\s*(.*)', re.MULTILINE)

        # 定义结束标志
        end_markers = ['附录', '附件', '附表', '后记', '参考文献', '索引']

        lines = text.split('\n')

        # 初始化变量
        file_info = ""
        current_chapter = ""
        current_section = ""
        clauses = []

        # 记录file_info的结束位置（第一章或第一节）
        file_info_end_idx = None
        clause_start_idx = None
        for i, line in enumerate(lines):
            if not line.strip():
                continue
            # 逐行进行匹配
            if chapter_pattern.match(line.lstrip(' \t\r\n\f\v#-*•·')):
                # 记录开头内容的结束位置（不包含该行）
                file_info_end_idx = i
                # 将章内容记录到current_chapter中
                current_chapter = line.strip().lstrip(' \t\r\n\f\v#-*•·')
            if section_pattern.match(line.lstrip(' \t\r\n\f\v#-*•·')):
                # 将节内容记录到current_section中
                current_section = line.strip().lstrip(' \t\r\n\f\v#-*•·')
            if clause_pattern.match(line.lstrip(' \t\r\n\f\v#-*•·')):
                # 记录条款内容的开始位置（包含该行）
                clause_start_idx = i
                if not file_info_end_idx:
                    file_info_end_idx = i
                break

        if not clause_start_idx:
            logging.error("📄❌：文本中匹配条款失败")
            raise ValueError("文本中匹配条款失败")

        try:
            # 提取file_info：从开头到第一节之前的内容
            file_info = '\n'.join(lines[:file_info_end_idx]).rstrip()
            # 提取第一条的条款内容，并将其拼接进file_info中

            current_clause_content = ""
            current_clause_number = ""

            # 遍历条款内容
            for i in range(clause_start_idx, len(lines)):
                line = lines[i]
                if not line.strip():
                    continue
                # 检查是否到达结束标志
                is_end_marker = False
                for marker in end_markers:
                    if line.lstrip().startswith(marker):
                        is_end_marker = True
                        break
                if is_end_marker:
                    # 如果当前正在收集条款内容，则保存它
                    if current_clause_content:
                        clauses.append({
                            "章": current_chapter,
                            "节": current_section,
                            "条款编号": current_clause_number,
                            "条款内容": current_clause_content
                        })
                        current_clause_content = ""
                        current_clause_number = ""
                    break

                # 检查是否是章（匹配行首）
                if chapter_pattern.match(line.lstrip(' \t\r\n\f\v#-*•·')):
                    # 如果当前正在收集条款内容，则先保存它
                    if current_clause_content:
                        clauses.append({
                            "章": current_chapter,
                            "节": current_section,
                            "条款编号": current_clause_number,
                            "条款内容": current_clause_content
                        })
                        current_clause_content = ""
                        current_clause_number = ""

                    # 更新当前章
                    current_chapter = line.strip().lstrip(' \t\r\n\f\v#-*•·')
                    continue

                # 检查是否是节（匹配行首）
                if section_pattern.match(line.lstrip(' \t\r\n\f\v#-*•·')):
                    # 如果当前正在收集条款内容，则先保存它
                    if current_clause_content:
                        clauses.append({
                            "章": current_chapter,
                            "节": current_section,
                            "条款编号": current_clause_number,
                            "条款内容": current_clause_content
                        })
                        current_clause_content = ""
                        current_clause_number = ""

                    # 更新当前节
                    current_section = line.strip().lstrip(' \t\r\n\f\v#-*•·')
                    continue

                # 检查是否是条款（匹配行首）
                if clause_pattern.match(line.lstrip(' \t\r\n\f\v#-*•·')):
                    # 如果当前正在收集条款内容，则先保存之前的条款
                    if current_clause_content:
                        clauses.append({
                            "章": current_chapter,
                            "节": current_section,
                            "条款编号": current_clause_number,
                            "条款内容": current_clause_content
                        })

                    # 提取条款编号和内容
                    clause_match = clause_pattern.match(line.lstrip(' \t\r\n\f\v#-*•·'))
                    # 从捕获组直接获取编号
                    clause_num_part = clause_match.group(1)  # 编号部分
                    current_clause_number = f"第{clause_num_part}条"  # 重构完整编号
                    current_clause_content = clause_match.group(2).strip()  # 内容部分
                    continue

                # 如果当前正在收集条款内容，则添加到当前条款内容
                if current_clause_content:
                    if current_clause_content:  # 如果已有内容，在前面加上换行符
                        current_clause_content += '\n' + line
                    else:  # 如果还没有内容，直接赋值
                        current_clause_content = line
            # 处理最后一个条款（如果有）
            if current_clause_content:
                clauses.append({
                    "章": current_chapter,
                    "节": current_section,
                    "条款编号": current_clause_number,
                    "条款内容": current_clause_content
                })

            if not clauses:
                logging.error("📄❌：未找到条款内容")
                raise ValueError("未找到条款内容")
            # 将第一条的条款内容拼接至file_info中
            file_info += '\n' + clauses[0]['条款内容']
            return {
                "filename": filename,
                "graph_tag": graph_tag,
                "file_info": file_info,
                "clauses": clauses
            }
        except Exception as e:
            logging.error(f"📄❌：提取文件信息时出错 - {e}")
            raise ValueError(f"提取文件信息时出错 - {e}")


if __name__ == "__main__":
    # 读取示例文件
    file_path = r"F:\企业大脑知识库系统\8.1项目\数据处理\清洗的数据\国家规章库\安全生产\生产安全事故应急预案管理办法.txt"
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except FileNotFoundError:
        # 如果真实文件不存在，创建一个模拟的示例内容进行测试
        raise ValueError("文件不存在")

    extractor = ClauseExtractor()
    result = asyncio.run(
        extractor.split_clause(
            filename="生产安全事故应急预案管理办法.txt",
            graph_tag="emergency_plan_management",
            text=content
        )
    )

    # 打印结果
    print("文件信息:", result["file_info"])
    print("\n条款数量:", len(result["clauses"]))
    print("\n条款示例:")
    for i, clause in enumerate(result["clauses"]):
        print(f"\n条款 {i + 1}: ")
        print(f"  章: {clause['章']}")
        print(f"  节: {clause['节']}")
        print(f"  条款编号: {clause['条款编号']}")
        print(f"  条款内容: {clause['条款内容']}")

# 1. 统计抽取出来的条款数和最后一项条款的编号是对应上的
