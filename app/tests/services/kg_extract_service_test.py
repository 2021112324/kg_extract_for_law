"""
测试 KGExtractService._merge_chunk_results 方法
验证多个文本块的抽取结果合并功能
"""
import sys
import os

# 添加项目根目录到路径
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from app.services.ai.kg_extract_service import KGExtractService
from app.schemas.kg import GraphNodeBase, GraphEdgeBase, TextClass


def merge_chunk_results_test():
    """
    测试 _merge_chunk_results 方法的合并功能
    
    测试场景：
    1. 模拟两个文本块的抽取结果
    2. 包含重复节点（应合并属性）
    3. 包含重复边（应合并属性）
    4. 包含重复文本类（应去重）
    5. 验证合并后的结果正确性
    """
    # 创建服务实例
    service = KGExtractService()
    
    # 构造测试数据 - 第一个文本块的结果
    result1 = {
        "nodes": [
            GraphNodeBase(
                node_id="当事人_深圳市科技创新发展有限公司",
                node_name="深圳市科技创新发展有限公司",
                node_type="当事人",
                properties={"角色": "上诉人（原审原告）"}
            ),
            GraphNodeBase(
                node_id="法院_中华人民共和国最高人民法院",
                node_name="中华人民共和国最高人民法院",
                node_type="法院",
                properties={"级别": "最高人民法院"},
                source_text_info={
                    "222b3d30fbb21c226b510223479d35a5": [
                        {"start_pos": 0, "end_pos": 13, "alignment_status": None}
                    ]
                }
            ),
            GraphNodeBase(
                node_id="时间_2023年2月15日",
                node_name="2023年2月15日",
                node_type="时间",
                properties={},
                source_text_info={
                    "222b3d30fbb21c226b510223479d35a5": [
                        {"start_pos": 385, "end_pos": 394, "alignment_status": None}
                    ]
                }
            ),
        ],
        "edges": [
            GraphEdgeBase(
                source_id="法院_中华人民共和国最高人民法院",
                target_id="案件类型_技术服务合同纠纷",
                relation_type="审理",
                properties={},
                weight=1.0,
                bidirectional=False
            ),
        ],
        "text_classes": [
            TextClass(
                text_id="222b3d30fbb21c226b510223479d35a5",
                text="中华人民共和国最高人民法院\n民事判决书\n（2023）最高法民终123号..."
            )
        ]
    }
    
    # 构造测试数据 - 第二个文本块的结果（包含重复节点和新节点）
    result2 = {
        "nodes": [
            # 重复节点 - 应合并属性
            GraphNodeBase(
                node_id="法院_中华人民共和国最高人民法院",
                node_name="中华人民共和国最高人民法院",
                node_type="法院",
                properties={"审理级别": "终审"},  # 新属性
                description="最高审判机关",  # 新描述
                source_text_info={
                    "text_chunk_2": [
                        {"start_pos": 100, "end_pos": 113, "alignment_status": None}
                    ]
                }
            ),
            # 新节点
            GraphNodeBase(
                node_id="当事人_杭州互联网技术有限公司",
                node_name="杭州互联网技术有限公司",
                node_type="当事人",
                properties={"角色": "被上诉人（原审被告）"}
            ),
            GraphNodeBase(
                node_id="案件类型_技术服务合同纠纷",
                node_name="技术服务合同纠纷",
                node_type="案件类型",
                properties={}
            ),
        ],
        "edges": [
            # 重复边 - 应合并属性
            GraphEdgeBase(
                source_id="法院_中华人民共和国最高人民法院",
                target_id="案件类型_技术服务合同纠纷",
                relation_type="审理",
                properties={"审理日期": "2023年2月15日"},  # 新属性
                weight=2.0,  # 更新权重
                bidirectional=False
            ),
            # 新边
            GraphEdgeBase(
                source_id="当事人_深圳市科技创新发展有限公司",
                target_id="当事人_杭州互联网技术有限公司",
                relation_type="合同",
                properties={},
                weight=1.0,
                bidirectional=False
            ),
        ],
        "text_classes": [
            # 重复文本类 - 应去重
            TextClass(
                text_id="222b3d30fbb21c226b510223479d35a5",
                text="中华人民共和国最高人民法院\n民事判决书\n（2023）最高法民终123号..."
            ),
            # 新文本类
            TextClass(
                text_id="text_chunk_2",
                text="案件事实部分内容..."
            )
        ]
    }
    
    # 执行合并
    results = [result1, result2]
    merged_result = service._merge_chunk_results(results, "test_case.md")
    
    # 验证合并结果
    print("=" * 80)
    print("测试 _merge_chunk_results 方法")
    print("=" * 80)
    
    # 1. 验证节点数量
    print(f"\n✓ 节点总数: {len(merged_result['nodes'])}")
    print(f"  预期: 5 个节点（3个来自result1，3个来自result2，其中1个合并）")
    assert len(merged_result['nodes']) == 5, f"节点数量错误，预期5个，实际{len(merged_result['nodes'])}个"
    
    # 2. 验证节点合并（检查重复节点的属性是否正确合并）
    court_node = None
    for node in merged_result['nodes']:
        if node.node_id == "法院_中华人民共和国最高人民法院":
            court_node = node
            break
    
    assert court_node is not None, "未找到法院节点"
    print(f"\n✓ 重复节点合并验证:")
    print(f"  node_id: {court_node.node_id}")
    print(f"  node_name: {court_node.node_name}")
    print(f"  node_type: {court_node.node_type}")
    print(f"  description: {court_node.description}")
    print(f"  properties: {court_node.properties}")
    
    # 验证属性合并
    assert "级别" in court_node.properties, "缺少原始属性'级别'"
    assert "审理级别" in court_node.properties, "缺少新增属性'审理级别'"
    assert court_node.description == "最高审判机关", "描述未正确更新"
    
    # 验证 source_text_info 合并
    assert court_node.source_text_info is not None, "source_text_info 为空"
    assert "222b3d30fbb21c226b510223479d35a5" in court_node.source_text_info, "缺少第一个文本块的溯源信息"
    assert "text_chunk_2" in court_node.source_text_info, "缺少第二个文本块的溯源信息"
    print(f"  source_text_info keys: {list(court_node.source_text_info.keys())}")
    
    # 3. 验证边数量
    print(f"\n✓ 边总数: {len(merged_result['edges'])}")
    print(f"  预期: 2 条边（1条重复合并，1条新增）")
    assert len(merged_result['edges']) == 2, f"边数量错误，预期2条，实际{len(merged_result['edges'])}条"
    
    # 4. 验证边合并
    merged_edge = None
    for edge in merged_result['edges']:
        if (edge.source_id == "法院_中华人民共和国最高人民法院" and 
            edge.target_id == "案件类型_技术服务合同纠纷" and
            edge.relation_type == "审理"):
            merged_edge = edge
            break
    
    assert merged_edge is not None, "未找到合并的边"
    print(f"\n✓ 重复边合并验证:")
    print(f"  source_id: {merged_edge.source_id}")
    print(f"  target_id: {merged_edge.target_id}")
    print(f"  relation_type: {merged_edge.relation_type}")
    print(f"  weight: {merged_edge.weight}")
    print(f"  properties: {merged_edge.properties}")
    
    assert merged_edge.weight == 2.0, f"权重未更新，预期2.0，实际{merged_edge.weight}"
    assert "审理日期" in merged_edge.properties, "缺少新增属性'审理日期'"
    
    # 5. 验证文本类去重
    print(f"\n✓ 文本类总数: {len(merged_result['text_classes'])}")
    print(f"  预期: 2 个文本类（1个去重，1个新增）")
    assert len(merged_result['text_classes']) == 2, f"文本类数量错误，预期2个，实际{len(merged_result['text_classes'])}个"
    
    text_ids = [tc.text_id for tc in merged_result['text_classes']]
    assert "222b3d30fbb21c226b510223479d35a5" in text_ids, "缺少第一个文本类"
    assert "text_chunk_2" in text_ids, "缺少第二个文本类"
    print(f"  text_ids: {text_ids}")
    
    # 6. 验证 filename
    assert "filename" in merged_result, "结果中缺少 filename"
    assert merged_result["filename"] == ["test_case.md"], "filename 不正确"
    print(f"\n✓ filename: {merged_result['filename']}")
    
    # 7. 输出所有节点信息（用于详细检查）
    print("\n" + "=" * 80)
    print("所有节点详细信息:")
    print("=" * 80)
    for i, node in enumerate(merged_result['nodes'], 1):
        print(f"\n节点 {i}:")
        print(f"  {node}")
    
    # 8. 输出所有边信息
    print("\n" + "=" * 80)
    print("所有边详细信息:")
    print("=" * 80)
    for i, edge in enumerate(merged_result['edges'], 1):
        print(f"\n边 {i}:")
        print(f"  {edge}")
    
    print("\n" + "=" * 80)
    print("✓ 所有测试通过！")
    print("=" * 80)
    
    return merged_result


def merge_empty_results_test():
    """测试空结果合并"""
    service = KGExtractService()
    
    # 测试空列表
    result = service._merge_chunk_results([], "empty.md")
    assert result == {}, "空结果应返回空字典"
    print("✓ 空结果测试通过")
    
    # 测试包含 None 的列表（会生成空结果，但带有 filename）
    result = service._merge_chunk_results([None, None], "none.md")
    # 实际会返回带有 filename 的空结果
    expected = {
        "nodes": [],
        "edges": [],
        "text_classes": [],
        "filename": ["none.md"]
    }
    assert result == expected, f"包含None的结果应返回带filename的空结果，实际：{result}"
    print("✓ None结果测试通过")


def merge_single_result_test():
    """测试单个结果（无需合并）"""
    service = KGExtractService()
    
    single_result = {
        "nodes": [
            GraphNodeBase(
                node_id="test_node",
                node_name="测试节点",
                node_type="测试",
                properties={"key": "value"}
            )
        ],
        "edges": [
            GraphEdgeBase(
                source_id="node1",
                target_id="node2",
                relation_type="测试关系"
            )
        ],
        "text_classes": [
            TextClass(text_id="text1", text="测试文本")
        ]
    }
    
    result = service._merge_chunk_results([single_result], "single.md")
    
    assert len(result['nodes']) == 1, "单个结果的节点数量应为1"
    assert len(result['edges']) == 1, "单个结果的边数量应为1"
    assert len(result['text_classes']) == 1, "单个结果的文本类数量应为1"
    print("✓ 单个结果测试通过")




if __name__ == "__main__":
    print("\n开始测试 KGExtractService._merge_chunk_results 方法\n")
    
    try:
        # 测试主要合并功能
        merge_chunk_results_test()
        
        # 测试边界情况
        print("\n" + "=" * 80)
        print("测试边界情况")
        print("=" * 80 + "\n")
        merge_empty_results_test()
        merge_single_result_test()
        
        print("\n" + "=" * 80)
        print("🎉 所有测试完成！")
        print("=" * 80)
        
    except AssertionError as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ 测试出错: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
