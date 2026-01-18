import os

# 模型设置
QWEN_PLUS_MODEL = "qwen-plus"
QWEN_MAX_MODEL = "qwen-max"
DASHSCOPE_API_KEY = os.getenv("DASHSCOPE_API_KEY")  # 设置环境变量或直接填写

# 文本长度阈值（token）
DIRECT_EXTRACT_THRESHOLD = 28000    # ≤28K 直接整段抽取
CHUNK_SIZE_THRESHOLD = 30000        # 分块最大长度
OVERLAP_SIZE = 512                  # 块间重叠长度（用于语义连续）

# 实体相似度阈值
ENTITY_SIMILARITY_THRESHOLD = 0.9   # 名字模糊匹配阈值
