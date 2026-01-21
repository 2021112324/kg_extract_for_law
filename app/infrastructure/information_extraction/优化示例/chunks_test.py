import re
from difflib import SequenceMatcher

from app.infrastructure.information_extraction.base import Entity
from app.infrastructure.information_extraction.union.union_find import UnionFind

MAX_CHUNK_SIZE = 30
OVERLAP_SIZE = 5


def split_to_sync_extract(
        prompt,
        schema,
        input_text,
        examples
):
    try:
        if len(input_text) <= 0:
            return None
        chunks = split_text_by_paragraphs(input_text)
        print("分块数：", len(chunks))
        chunks_with_results = []
        for i, chunk in enumerate(chunks):
            # 为每个分块任务的提示词添加提示，告知这是文本片段及分块序号
            chunk_prompt = f"注意：当前处理的是文档的第{i + 1}个分块，共{len(chunks)}个分块。以下内容来自原文档的一部分，请结合整体文档上下文进行抽取，不要将其视为独立文档。\n\n{prompt}"
            result = get_result(chunk, i)
            chunks_with_results.append((chunk, result))

        # 使用并查集进行分块合并
        n = len(chunks_with_results)
        uf = UnionFind(n)
        for i in range(n):
            for j in range(i + 1, n):
                _, result_i = chunks_with_results[i]
                _, result_j = chunks_with_results[j]

                # 提取实体名称集合
                entities_i = result_i.get("entities", [])
                entities_j = result_j.get("entities", [])

                # 如果有相同实体，合并分块
                if is_duplicate_entity(entities_i, entities_j):  # 集合交集
                    uf.union(i, j)
            # 按连通分量分组
        groups = {}
        for i in range(n):
            root = uf.find(i)
            if root not in groups:
                groups[root] = []
            groups[root].append(i)

        # 处理每个连通分量
        final_results = []
        for group in groups.values():
            if len(group) == 1:
                # 单个分块，使用原有结果
                final_results.append(chunks_with_results[group[0]][1])
            else:
                # 多个分块合并，需要重新抽取
                merged_text = ""
                merged_examples = []
                for idx in group:
                    merged_text += chunks_with_results[idx][0] + "\n\n"
                    merged_examples.extend(chunks_with_results[idx][1].get("examples", []))

                # 重新抽取合并后的文本
                # 这里调用抽取函数
                # new_result = self.node_extractor.entity_and_relationship_extract(...)
                # final_results.append(new_result)

        # print("抽取结果数：", result_list)
        graph_result = {
            "entities": [],
            "relations": [],
            "texts_classes": []
        }
        return graph_result
    except Exception as e:
        print("分段抽取失败：", e)
        raise e


def split_text_by_paragraphs(
        text: str,
):
    """
    按段落分割文本，分段间保留重叠内容
    优先在换行符处分割，若无法在合理范围内找到换行符，则按句子分割

    Args:
        text: 输入文本
        max_chunk_size: 单段最大长度
        overlap_size: 分段间重叠内容长度

    Returns:
        List[str]: 分割后的文本片段列表
    """
    if not isinstance(text, str):
        raise ValueError("输入必须为字符串")

    # 按段落分割（以双换行符为分隔）
    paragraphs = text.split("\n")
    return paragraphs


def get_result(chunk, i):
    if i == 0:
        entities = []
        entities.append(
            Entity(
                name="E1",
                entity_type="G1",
                properties={},
            )
        )
        entities.append(
            Entity(
                name="E2",
                entity_type="G1",
                properties={},
            )
        )
        entities.append(
            Entity(
                name="E3",
                entity_type="G1",
                properties={},
            )
        )
        return {"entities": entities, "relations": [], "texts_classes": []}
    if i == 1:
        entities = []
        entities.append(
            Entity(
                name="E2",
                entity_type="G1",
                properties={},
            )
        )
        entities.append(
            Entity(
                name="E4",
                entity_type="G1",
                properties={},
            )
        )
        return {"entities": entities, "relations": [], "texts_classes": []}
    if i == 2:
        entities = []
        entities.append(
            Entity(
                name="E1",
                entity_type="G2",
                properties={},
            )
        )
        entities.append(
            Entity(
                name="E2",
                entity_type="G2",
                properties={},
            )
        )
        return {"entities": entities, "relations": [], "texts_classes": []}
    if i == 3:
        entities = []
        entities.append(
            Entity(
                name="E1",
                entity_type="G3",
                properties={},
            )
        )
        entities.append(
            Entity(
                name="E2",
                entity_type="G3",
                properties={},
            )
        )
        entities.append(
            Entity(
                name="E3",
                entity_type="G3",
                properties={},
            )
        )
        return {"entities": entities, "relations": [], "texts_classes": []}
    if i == 4:
        entities = []
        entities.append(
            Entity(
                name="E1",
                entity_type="G3",
                properties={},
            )
        )
        entities.append(
            Entity(
                name="E4",
                entity_type="G3",
                properties={},
            )
        )
        return {"entities": entities, "relations": [], "texts_classes": []}
    if i == 5:
        entities = []
        entities.append(
            Entity(
                name="E1",
                entity_type="G3",
                properties={},
            )
        )
        entities.append(
            Entity(
                name="E2",
                entity_type="G3",
                properties={},
            )
        )
        return {"entities": entities, "relations": [], "texts_classes": []}


def is_duplicate_entity(
        entities_i,
        entities_j
):
    for entity_i in entities_i:
        for entity_j in entities_j:
            i_name = getattr(entity_i, "name", None)
            i_type = getattr(entity_i, "entity_type", None)
            j_name = getattr(entity_j, "name", None)
            j_type = getattr(entity_j, "entity_type", None)
            if not i_name or not i_type or not j_name or not j_type:
                continue
            if i_type == j_type and clean_and_calculate_similarity(i_name, j_name) > 0.9:
                return True
    return False


def clean_and_calculate_similarity(str1: str, str2: str) -> float:
    """
    清洗字符串并使用SequenceMatcher计算两个字符串的相似度

    Args:
        str1: 第一个字符串
        str2: 第二个字符串

    Returns:
        float: 相似度值 [0.0, 1.0]，1.0表示完全相同
    """
    def clean_string(text: str) -> str:
        """
        清洗字符串：移除中文标点符号

        Args:
            text: 输入文本

        Returns:
            str: 清洗后的文本
        """
        # 定义中文和英文标点符号的正则表达式
        punctuation_pattern = r'[^\w\s\u4e00-\u9fff]'
        return re.sub(punctuation_pattern, '', text)

    # 1. 清洗字符串
    cleaned_str1 = clean_string(str1)
    cleaned_str2 = clean_string(str2)

    # 2. 使用SequenceMatcher计算相似度
    similarity = SequenceMatcher(None, cleaned_str1, cleaned_str2).ratio()

    return similarity


if __name__ == "__main__":
    input_text = """test1 test2 test3 test4 test5 test6 test7 test8 test9 test10
test11 test12 test13 test14 test15 test16 test17 test18 test19 test20
test21 test22 test23 test24 test25 test26 test27 test28 test29 test30
test31 test32 test33 test34 test35 test36 test37 test38 test39 test40
test41 test42 test43 test44 test45 test46 test47 test48 test49 test50
test51 test52 test53 test54 test55 test56 test57 test58 test59 test60"""
    split_to_sync_extract(
        prompt="test prompt",
        schema="test schema",
        input_text=input_text,
        examples=[]
    )
    # chunks = split_text_by_paragraphs(input_text)
    # print("分块数：", len(chunks))
    # for i, chunk in enumerate(chunks):
    #     print(f"分块 {i + 1}:\n{chunk}\n")
    #     result = get_result(chunk, i)
    #     print(f"分块 {i + 1} 的结果：", result)
