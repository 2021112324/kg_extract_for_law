general_schema = """
{
    "nodes": [
        {
            "entity": "实体A",
            "description": "实体A表示现实世界中的对象或概念",
            "properties": [
                {
                    "属性X": "属性X是实体A具体特征的键值对信息"
                },
                ...
            ]
        },
        ...
    ],
    "edges": [
        {
            "source_name": "关系的源节点（主体）",
            "target_name": "关系的目标节点（客体）"
            "relation": "关系A"
            "description": "关系A是连接两个实体的语义链接，用于表达实体之间的关联性"
            "directionality": "关系方向性：单向或双向"
            "properties": [
                {
                    "属性X": "属性X是关系A具体特征的键值对信息"
                },
                ...
            ]
        },
        ...
    ]
}
"""

general_entity_schema = """
[
    {
        "entity": "实体A",
        "description": "实体A表示现实世界中的对象或概念",
        "properties": [
            {
                "属性X": "属性X是实体A具体特征的键值对信息"
            },
            ...
        ]
    },
    ...
]
"""

general_relation_schema = """
[
    {
        "source_name": "关系的源节点（主体）",
        "target_name": "关系的目标节点（客体）"
        "relation": "关系A"
        "description": "关系A是连接两个实体的语义链接，用于表达实体之间的关联性"
        "directionality": "关系方向性：单向或双向"
        "properties": [
            {
                "属性X": "属性X是关系A具体特征的键值对信息"
            },
            ...
        ]
    },
    ...
]
"""

{
    "nodes": """
# 节点schema
## 实体A
### 描述
实体A表示现实世界中的对象或概念
### 属性
- 属性X：属性X是实体A具体特征的键值对信息
- 属性Y：属性Y是实体A具体特征的键值对信息
...
## 实体B
...
""",
    "edges": """
# 关系
## 关系A
### 关系三元组
关系的源节点（主体）-关系A-关系的目标节点（客体）
### 描述
关系A是连接两个实体的语义链接，用于表达实体之间的关联性
### 关系方向性
关系方向性：单向或双向
### 属性
- 属性X：属性X是关系A具体特征的键值对信息
- 属性Y：属性Y是关系A具体特征的键值对信息
...
## 关系B
...
"""
}
