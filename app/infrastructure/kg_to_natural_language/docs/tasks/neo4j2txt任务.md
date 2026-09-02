# 总体目标

输入图谱标签，导出图谱json数据，并转换成自然语言知识条目

# 任务

请你迁移 D:\CogmAIT\8.1项目\8.1数据\data_code\code\kg_to_natural_language\kg_to_natural_language_v2.py 中的json数据转自然语言文本知识条目的功能，在 F:\企业大脑知识库系统\8.1项目\抽取代码\kg_extract_for_law\app\infrastructure\kg_to_natural_language 下实现以下功能：

## 1. 知识图谱导出

向函数输入neo4j图谱标签，将图谱数据导出，保存为临时文件，保存在 F:\企业大脑知识库系统\8.1项目\抽取代码\kg_extract_for_law\app\infrastructure\kg_to_natural_language\temp\json

- 数据导出方式：

```
节点（node.json）：MATCH (n:tag) RETURN n;
	关系（edge.json）：MATCH (n:tag)-[p]->(q) RETURN p;
	以json格式导出
```


## 2. json图谱数据转自然语言知识

迁移 D:\CogmAIT\8.1项目\8.1数据\data_code\code\kg_to_natural_language\kg_to_natural_language_v2.py 中的json数据转自然语言文本知识条目的功能，实现json图谱数据转为自然语言知识的功能，将自然语言知识保存为临时文件，保存到 F:\企业大脑知识库系统\8.1项目\抽取代码\kg_extract_for_law\app\infrastructure\kg_to_natural_language\temp\txt；将统计csv保存为临时文件，保存到 F:\企业大脑知识库系统\8.1项目\抽取代码\kg_extract_for_law\app\infrastructure\kg_to_natural_language\temp\csv

### 实现细节

1. 统一按照 `法律法规条款/行政监管规则` 规则处理，不需要其他类型的处理
2. 不需要 按来源文件拆分的 txt 了，只需要 按类型汇总的 txt，即取消 D:\CogmAIT\8.1项目\8.1数据\data_code\code\kg_to_natural_language\task\task1.md 的"t1保存在对应filename的txt文件中"；使用“t2保存在对应type的txt文件中（如法律法规条款.txt）”
3. 对于 “法规文件”节点，由于法规文件节点的名称中带有法规文件，构造“{prefix}，{法规文件节点}{属性}{属性值}”的做法会导致法规文件重复，所以：此类节点不需要添加{prefix}。例如：原先构造为”境外投资管理办法中，境外投资管理办法的发布单位是中华人民共和国商务部。“，转换成构造”境外投资管理办法的发布单位是中华人民共和国商务部。

# 补充

1. json转自然语言知识的功能实现可参考D:\CogmAIT\8.1项目\8.1数据\data_code\code\kg_to_natural_language\task\task1.md、D:\CogmAIT\8.1项目\8.1数据\data_code\code\kg_to_natural_language\task\task2.md、D:\CogmAIT\8.1项目\8.1数据\data_code\code\kg_to_natural_language\task\task3.md
2. 每次执行代码，请先清空 F:\企业大脑知识库系统\8.1项目\抽取代码\kg_extract_for_law\app\infrastructure\kg_to_natural_language\temp 下的临时数据，即执行代码保存临时数据，下一次执行清楚临时数据保存新的临时数据
3. 请尽量只在 F:\企业大脑知识库系统\8.1项目\抽取代码\kg_extract_for_law\app\infrastructure\kg_to_natural_language 目录下实现任务
4. 实现代码后，请以 `e1_行政监管规则_kg_586736132520148992为测试数据进行测试，并审查结果是否存在问题
