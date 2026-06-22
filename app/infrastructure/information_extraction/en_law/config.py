"""格式一英文法规知识图谱抽取的配置文件。

本文件集中维护英文法规抽取流程会用到的固定配置：
1. 格式一文件的内部格式标识。
2. LLM 模型、API 地址、超时、并发等运行参数。
3. 文首“合规风险类型”允许值。
4. 默认数据目录与报告输出目录。

这些配置被 `splitter.py`、`extractor.py` 和测试脚本共同引用，目的是让
业务参数和算法逻辑分离，后续切换模型或数据目录时不需要改核心代码。
"""

# 使用 os.getenv 读取环境变量，使模型参数和路径可以在部署时覆盖。
import os


# 格式一英文法规文件的内部格式名；图谱 metadata 会使用该值标识数据来源格式。
FORMAT_ONE_DOCUMENT_FORMAT = "format_one_eu_regulation_directive"

# LLM 模型名称；优先读取环境变量，未配置时使用项目当前默认模型。
EN_LAW_MODEL = os.getenv("EN_LAW_MODEL", "qwen3-30b-a3b-instruct-2507")
# LLM API key；这里保持与现有 law_extract 代码风格一致，允许通过环境变量覆盖。
EN_LAW_MODEL_API_KEY = os.getenv(
    "EN_LAW_MODEL_API_KEY",
    "gpustack_342609ce423be29a_4371426b285a91dc44fb4e8d72454847",
)
# LLM API 地址；用于 InformationExtractionFactory 创建 langextract 调用客户端。
EN_LAW_MODEL_API_URL = os.getenv(
    "EN_LAW_MODEL_API_URL",
    "http://222.171.219.26:20001/v1/chat/completions",
)

# 单次 LLM 输入最大字符缓冲区，避免过长 Article 超出模型或框架处理能力。
EN_LAW_MAX_CHAR_BUFFER = int(os.getenv("EN_LAW_MAX_CHAR_BUFFER", "7500"))
# LLM 批处理长度，沿用参考 law_extract 中的批处理参数风格。
EN_LAW_BATCH_LENGTH = int(os.getenv("EN_LAW_BATCH_LENGTH", "5"))
# LLM worker 数量，用于底层 langextract 并发处理。
EN_LAW_MAX_WORKERS = int(os.getenv("EN_LAW_MAX_WORKERS", "3"))
# LLM 请求超时时间，单位取决于底层 LangextractConfig 的实现。
EN_LAW_TIMEOUT = int(os.getenv("EN_LAW_TIMEOUT", "3000"))
# Langextract 外层抽取调用超时时间，单位为秒；正式默认保留长超时，测试时可用环境变量缩短。
EN_LAW_EXTRACTION_TIMEOUT = int(os.getenv("EN_LAW_EXTRACTION_TIMEOUT", "3600"))
# Langextract 抽取失败后的重试次数；正式默认沿用参考实现的 5 次，测试时可通过环境变量降低。
EN_LAW_MAX_RETRIES = int(os.getenv("EN_LAW_MAX_RETRIES", "5"))
# Article 级并发抽取上限；避免一次性提交过多 Article 请求。
EN_LAW_MAX_CONCURRENT = int(os.getenv("EN_LAW_MAX_CONCURRENT", "5"))
# 宽松模式开关；开启时 LLM 某些 Article 失败仍会生成 fallback Article 节点。
EN_LAW_LENIENT_MODE = os.getenv("EN_LAW_LENIENT_MODE", "true").lower() in {"1", "true", "yes"}

# 文首“合规风险类型”允许值；用于校验文件头中给出的中文业务分类是否属于预期范围。
RISK_TYPE_VALUES = [
    "产品法律风险",
    "供应链合规风险",
    "劳动用工法律合规风险",
    "企业关联方合规风险",
    "企业国际化经营合规风险",
    "企业信用风险",
]

# 早期实际数据目录，保留作兼容默认值；测试脚本允许通过 --data-dir 覆盖。
DEFAULT_ACTUAL_DATA_DIR = (
    r"D:\CogmAIT\8.1项目\8.1数据\爬取的数据\风险法规\第三版\分类"
    r"\英文抽取所用数据\法文汇总\格式一"
)

# 默认报告输出目录；切分验证脚本和样例输出都会基于该目录生成子目录。
DEFAULT_REPORT_DIR = (
    r"D:\CogmAIT\en_law\kg_extract_for_law"
    r"\app\infrastructure\information_extraction\en_law\report"
)
