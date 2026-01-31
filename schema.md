# F:\企业大脑知识库系统\8.1项目\抽取代码\kg_extract_for_law\app\infrastructure\information_extraction\graph_extraction.py
- 修改MAX_CHAR_BUFFER = 10000 ----几十KB文件

# 日志F:\企业大脑知识库系统\8.1项目\抽取代码\kg_extract_for_law\logs
报错：ERROR - Failed to parse content.

# 问题：
1. 法规条款-提及-法规条款，关系存在错误
2. 提示词位置 F:\企业大脑知识库系统\8.1项目\抽取代码\kg_extract_for_law\tests\prompt\国家规章库json.py

# 删除测试的图谱：MATCH (n:法规test_kg_538600652197068800) DETACH DELETE n;