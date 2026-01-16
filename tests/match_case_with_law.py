# 实现案例知识图谱和法律法规知识图谱间的关联
import re

from app.infrastructure.graph_storage.neo4j_adapter import Neo4jAdapter
from app.services.core.kg_service import kg_service

# 实现方式
## 将合规知识库知识图谱合并到总法律法规知识库中，然后遍历合规知识库知识图谱“法律法规”节点，进行匹配

neo4j_adapter = Neo4jAdapter()


# 实现方式
## 将合规知识库知识图谱合并到总法律法规知识库中，然后遍历合规知识库知识图谱"法律法规"节点，进行匹配

def relate_instance_to_law(instance_kg_name, law_kg_name):
    """
    实现案例知识图谱和法律法规知识图谱间的关联
    :param instance_kg_name: 合规案例库知识图谱名称
    :param law_kg_name: 法律法规知识图谱名称
    :return: 关联结果
    """
    neo4j_adapter = Neo4jAdapter()
    neo4j_adapter.connect()

    try:
        # 获取合规案例库中的法律法规节点
        instance_law_list = neo4j_adapter.get_nodes_by_type(instance_kg_name, "法律法规")

        # 获取法律法规知识图谱中的法律文件节点
        law_list = (neo4j_adapter.get_nodes_by_type(law_kg_name, "法律文件") +
                    neo4j_adapter.get_nodes_by_type(law_kg_name, "法规文件") +
                    neo4j_adapter.get_nodes_by_type(law_kg_name, "规章文件"))

        # 创建名称到节点的映射，去除标点符号以便匹配
        def clean_name(name):
            # 去除标点符号和空格，只保留中文、英文、数字
            return re.sub(r'[^\w\u4e00-\u9fff]', '', name)

        # 遍历合规案例库中的法律法规节点
        kg_data = {
            "nodes": [],
            "edges": []
        }
        instance_law_name_to_id = {}
        for instance_law_node in instance_law_list:
            instance_law_name = getattr(instance_law_node, 'name', None)
            instance_id = getattr(instance_law_node, 'id', None)
            if instance_law_name and instance_id:
                clean_instance_name = clean_name(instance_law_name)
                instance_law_name_to_id[clean_instance_name] = instance_id
                print(f"添加实例法规节点: {clean_instance_name} -> {instance_id}")

        # 构建法律法规节点映射
        for law_node in law_list:
            clean_name_key = clean_name(getattr(law_node, 'name', None))
            law_node_id = getattr(law_node, 'id', None)
            if clean_name_key and law_node_id:
                for instance_law_name, instance_law_id in instance_law_name_to_id.items():
                    if clean_name_key in instance_law_name or clean_name_key in str("中华人民共和国" + instance_law_name):
                        print(f"匹配法规: 文件：{clean_name_key} -> 法律法规：{instance_law_name}")
                        kg_data["edges"].append(
                            {
                                "source_id": instance_law_id,
                                "target_id": law_node_id,
                                "relation_type": "依据",
                                "directionality": "单向",
                                "description": "法律法规依据法律文件",
                                "properties": {}
                            }
                        )
        # print(f"成功创建 {relationships_created} 个法规关联关系")
        return kg_data

    except Exception as e:
        print(f"关联过程中发生错误: {str(e)}")
        return 0
    finally:
        neo4j_adapter.disconnect()


def match_case_with_law_by_content(instance_kg_name, law_kg_name):
    """
    基于内容相似度进行案例和法规的匹配
    """
    neo4j_adapter = Neo4jAdapter()
    neo4j_adapter.connect()

    try:
        # 获取合规案例库中的法律案例节点
        case_list = neo4j_adapter.get_nodes_by_type(instance_kg_name, "法律案例")

        # 获取法律法规知识图谱中的法规节点
        law_list = (neo4j_adapter.get_nodes_by_type(law_kg_name, "法律文件") +
                    neo4j_adapter.get_nodes_by_type(law_kg_name, "法规文件") +
                    neo4j_adapter.get_nodes_by_type(law_kg_name, "规章文件"))

        relationships_created = 0

        for case_node in case_list:
            case_content = case_node.properties.get('案件摘要', '') + case_node.properties.get('争议焦点', '')

            for law_node in law_list:
                law_content = law_node.properties.get('法规内容摘要', '') + law_node.properties.get('法规名称', '')

                # 简单的关键词匹配（可以根据需要使用更复杂的相似度算法）
                case_keywords = set(case_content.split())
                law_keywords = set(law_content.split())

                common_keywords = case_keywords.intersection(law_keywords)
                if len(common_keywords) > 0:
                    similarity = len(common_keywords) / max(len(case_keywords), len(law_keywords))

                    if similarity > 0.1:  # 设置相似度阈值
                        relationship_result = neo4j_adapter.create_relationship(
                            instance_kg_name,
                            case_node.id,
                            law_node.id,
                            "内容关联",
                            {"匹配度": similarity, "匹配关键词数": len(common_keywords)}
                        )

                        if relationship_result:
                            relationships_created += 1
                            print(
                                f"基于内容创建关联: {case_node.properties.get('案例名称', case_node.name)} -> {law_node.properties.get('法规名称', law_node.name)}, 相似度: {similarity:.2f}")

        print(f"基于内容成功创建 {relationships_created} 个关联关系")
        return relationships_created

    except Exception as e:
        print(f"基于内容关联过程中发生错误: {str(e)}")
        return 0
    finally:
        neo4j_adapter.disconnect()


if __name__ == "__main__":
    result = relate_instance_to_law("合规案例库v1_kg_532522501696126976", "law_top_rules")
    print(result)
    # print(kg_service.process_source_text(data))
    # neo4j_adapter.connect()
    # result = neo4j_adapter.get_nodes_by_type("合规案例库v1_kg_532522501696126976", "法律法规")
    # print(result)
    # neo4j_adapter.disconnect()


