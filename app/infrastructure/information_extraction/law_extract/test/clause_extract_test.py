import asyncio
import json
import logging
import os
import sys
from datetime import datetime
from colorama import init, Fore, Style

from io import StringIO

from app.infrastructure.graph_storage.neo4j_adapter import Neo4jAdapter
from app.infrastructure.information_extraction.law_extract.clause_extract import ClauseExtractor, ClauseCache

# 初始化colorama
init(autoreset=True)

# 设置日志目录
LOG_DIR = r"F:\企业大脑知识库系统\8.1项目\抽取代码\kg_extract_for_law\app\infrastructure\information_extraction\law_extract\test\logs"
os.makedirs(LOG_DIR, exist_ok=True)

# 配置日志记录器
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# 创建文件处理器
log_filename = f"clause_extract_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
log_filepath = os.path.join(LOG_DIR, log_filename)

file_handler = logging.FileHandler(log_filepath, encoding='utf-8')
file_handler.setLevel(logging.INFO)

# 自定义彩色控制台处理器
class ColoredConsoleHandler(logging.StreamHandler):
    def emit(self, record):
        # 获取日志消息
        msg = self.format(record)

        # 根据日志级别设置颜色
        if record.levelno == logging.WARNING:
            color_msg = Fore.YELLOW + msg
        elif record.levelno >= logging.ERROR:
            color_msg = Fore.RED + msg
        else:  # INFO 及其他级别使用白色
            color_msg = Fore.WHITE + msg

        # 输出到控制台
        stream = self.stream
        try:
            stream.write(color_msg + '\n')
            self.flush()
        except Exception:
            self.handleError(record)

# 创建彩色控制台处理器
colored_console_handler = ColoredConsoleHandler()
colored_console_handler.setLevel(logging.INFO)

# 创建格式器
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
file_handler.setFormatter(formatter)
colored_console_handler.setFormatter(formatter)

# 关键修改：将处理器添加到 root logger，这样所有日志都会被捕获
logging.getLogger().addHandler(file_handler)
logging.getLogger().addHandler(colored_console_handler)
logging.getLogger().setLevel(logging.INFO)

# 为当前模块创建专用logger（可选，用于特定的模块日志）
logger = logging.getLogger(__name__)


# 重定向stdout到日志和文件
class LoggerWriter:
    def __init__(self, logger, level):
        self.logger = logger
        self.level = level
        self.linebuf = ''
        self._in_write = False  # 防止递归

    def write(self, buf):
        if self._in_write:  # 防止递归调用
            return len(buf)

        self._in_write = True
        try:
            temp_linebuf = self.linebuf + buf
            self.linebuf = ''
            for line in temp_linebuf.splitlines(True):
                if line[-1] == '\n':
                    # 根据日志级别应用颜色
                    stripped_line = line.rstrip()

                    # 检测日志级别
                    if ":INFO:" in stripped_line or stripped_line.startswith("INFO:"):
                        colored_output = Fore.WHITE + stripped_line
                    elif ":WARNING:" in stripped_line or stripped_line.startswith("WARNING:"):
                        colored_output = Fore.YELLOW + stripped_line
                    elif ":ERROR:" in stripped_line or stripped_line.startswith("ERROR:"):
                        colored_output = Fore.RED + stripped_line
                    else:
                        # 默认白色
                        colored_output = Fore.WHITE + stripped_line

                    # 输出到控制台
                    sys.__stdout__.write(colored_output + '\n')
                    sys.__stdout__.flush()

                    # 同时记录到日志
                    self.logger.log(self.level, line.rstrip())
                else:
                    self.linebuf = line
        finally:
            self._in_write = False

        return len(buf)

    def flush(self):
        if self.linebuf != '' and not self._in_write:
            self._in_write = True
            try:
                self.logger.log(self.level, self.linebuf.rstrip())
                self.linebuf = ''
            finally:
                self._in_write = False


# 保存原始stdout
original_stdout = sys.stdout
# 将stdout重定向到日志
sys.stdout = LoggerWriter(logger, logging.INFO)

extractor = ClauseExtractor()


async def split_clause_test():
    # 读取示例文件
    file_path = r"F:\企业大脑知识库系统\8.1项目\数据处理\清洗的数据\国家规章库\安全生产\生产安全事故应急预案管理办法.txt"
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except FileNotFoundError:
        # 如果真实文件不存在，创建一个模拟的示例内容进行测试
        raise ValueError("文件不存在")

    result = await extractor.split_clause(
        text=content
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


async def extract_kg_test():
    # 读取示例文件
    file_path = r"F:\企业大脑知识库系统\8.1项目\数据处理\清洗的数据\国家规章库\安全生产\生产安全事故应急预案管理办法.txt"
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except FileNotFoundError:
        # 如果真实文件不存在，创建一个模拟的示例内容进行测试
        raise ValueError("文件不存在")
    result = await extractor.extract_clauses(
        filename="生产安全事故应急预案管理办法",
        text=content
    )
    neo4j_adapter = Neo4jAdapter()
    neo4j_adapter.connect()
    neo4j_adapter.add_subgraph_with_merge(result, "啊啊啊test", "DomainLevel")
    neo4j_adapter.disconnect()
    # 以格式化json形式打印结果
    print(json.dumps(result, ensure_ascii=False, indent=2))


async def kg_extract_from_clause_test():
#     one_clause = {
#         "章": "第五章 商标代理违法行为的处理",
#         "节": "",
#         "条款编号": "第二十七条",
#         "条款内容": """
# 有下列情形之一的，属于商标法第六十八条第一款第一项规定的办理商标事宜过程中，伪造、变造或者使用伪造、变造的法律文件、印章、签名的行为：
# （一）伪造、变造国家机关公文、印章的；
# （二）伪造、变造国家机关之外其他单位的法律文件、印章的；
# （三）伪造、变造签名的；
# （四）知道或者应当知道属于伪造、变造的公文、法律文件、印章、签名，仍然使用的；
# （五）其他伪造、变造或者使用伪造、变造的法律文件、印章、签名的情形。
# """
#     }
    one_clause = {
        "章": "",
        "节": "",
        "条款编号": "第十二条",
        "条款内容": """
国家积极参与个人信息保护国际

规则的制定，促进个人信息保护方面的国际交流与合作，推动与其他国家、地区、国际组织之间的个人信息保护规则、标准等互认。
    """
    }
    clause_cache = ClauseCache()
    print("开始提取知识图谱")
    result = await extractor.kg_extract_from_clause(
        filename="中华人民共和国个人信息保护法",
        clause_cache=clause_cache,
        one_clause=one_clause
    )
    print("知识图谱提取完成")
    # 以格式化json形式打印结果
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    logger.info("开始执行测试脚本")
    result = asyncio.run(
        extract_kg_test()
    )
    logger.info("测试脚本执行完成")

# 恢复原始stdout
sys.stdout = original_stdout
