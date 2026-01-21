#!/usr/bin/env python3
# -*- encoding utf-8 -*-

"""
@Product: PyCharm
@File: defination.py
@Date: 2025-09-02
@Author: zxy
@Version: 0.0
@Desc: 
    
"""

definition_for_entity1 = """
## "实体"定义
- 实体是现实世界中可以独立存在并能够被唯一标识的事物或对象。在知识图谱和信息抽取的上下文中，实体具有以下特征：
### "实体"的组成部分
1. 实体类型
2. 实体标识（实体名称）
3. 属性（attributes）
### 示例
{
    "小说": "三国演义", 
    "小说_attributes":  {
        "作者": "罗贯中", 
        "创作年代": "元末明初洪武年间"
    }
}
"""

definition_for_relation1 = """
## "关系"定义
- 关系是语义上连接两个或多个实体的语义纽带，用于表达实体之间的相互作用、关联或依赖。在知识图谱和信息抽取的上下文中，关系具有以下特征：
### "关系"的组成部分
1. 关系类型：作为特殊实体，标识出"关系"类型数据，固定为"关系"
2. 关系标识内容
3. "关系"三元组组成（attributes）
该关系三元组内的各个组成，包括：主体、谓词、客体
⚠️ 注意：主体、客体必须是已定义或已抽取的实体唯一标识,即“实体类型_实体名称”
⚠️ 注意：实体类型必须是schema内已定义的实体类型；关系三元组必需是schema内已定义的关系三元组；主体和客体的实体必需是抽取结果中存在的实体（包括实体类型和实体名称）
### 示例
{
    "关系": "华为研发了麒麟芯片", 
    "关系_attributes":  {
        "主体": "公司_华为", 
        "谓词": "研发", 
        "客体": "产品_麒麟芯片"
    }
}
"""

definition_for_entity = definition_for_entity1
definition_for_relation = definition_for_relation1
