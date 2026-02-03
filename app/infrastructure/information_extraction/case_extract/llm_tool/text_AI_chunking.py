"""使用Qwen大模型对法律文书进行结构化分块

此模块实现了使用通义千问大模型对法律文书（如判决书）进行结构化分块的功能。
给定一份判决书内容和结构说明，将内容按逻辑结构拆分成多个文本块。
"""

import json
import os
from typing import Dict, List, Optional, Any
from openai import OpenAI
from loguru import logger
from json_repair import repair_json

from app.infrastructure.information_extraction.case_extract.prompt.prompt import default_chunking_prompt


class LegalDocumentAIChunker:
    """法律文书结构化分块器"""

    def __init__(
        self,
        model_name: str = "qwen3-30b-a3b-instruct-2507",
        api_key: str = "gpustack_342609ce423be29a_4371426b285a91dc44fb4e8d72454847",
        api_url: str = "http://222.171.219.26:20001/v1",
    ):
        """
        初始化法律文书分块器

        Args:
            model_name: 使用的大模型名称，默认为QWEN_PLUS_MODEL
            api_key: API密钥，默认从环境变量获取
        """
        self.model_name = model_name
        self.api_key = api_key
        self.api_url = api_url

        # 创建OpenAI客户端
        self.client = OpenAI(
            api_key=self.api_key,
            base_url=self.api_url,
        )

    def chunk_legal_document(
        self,
        document_text: str,
        user_prompt: str = None,
        ) -> Dict[str, str]:
        """
        使用Qwen大模型对法律文书进行结构化分块

        Args:
            document_text: 判决书内容
            user_prompt: 用户提示词，用于定义模型角色和任务

        Returns:
            字典，键为分块名称，值为对应的文本内容
        """
        # 构建提示词
        if not user_prompt:
            user_prompt = default_chunking_prompt()

        # 调用大模型
        response = self._call_qwen_model(user_prompt, document_text)

        # 解析模型响应
        result = response.choices[0].message.content
        return self._parse_chunking_result(result, document_text)

    def _call_qwen_model(self, system_prompt: str, user_input: str):
        """调用Qwen大模型

        Args:
            system_prompt: 系统提示词，用于定义模型角色和任务
            user_input: 用户输入的文本，即待处理的法律文书
        """
        try:
            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=[
                    {"role": "system", "content": system_prompt},  # 系统提示词
                    {"role": "user", "content": user_input}      # 用户输入文本
                ],
                temperature=0.01,  # 低温度确保输出一致性
                top_p=0.8,
                max_tokens=4000,
                response_format={"type": "json_object"}  # 指定期望的JSON对象格式输出
            )
            return response
        except Exception as e:
            raise Exception(f"调用Qwen模型时发生错误: {str(e)}")

    def _parse_chunking_result(self, result: str, original_text: str) -> Dict[str, str]:
        """
        解析模型返回的结果

        Args:
            result: 模型返回的字符串结果
            original_text: 原始文档文本

        Returns:
            分块结果字典
        """
        # 尝试直接解析JSON
        try:
            # 清理输出，移除可能的Markdown标记
            clean_result = result.strip()
            if clean_result.startswith('```json'):
                # 移除 ```json 开头和 ``` 结尾
                start_idx = clean_result.find('{')
                end_idx = clean_result.rfind('}') + 1
                clean_result = clean_result[start_idx:end_idx]
            elif clean_result.startswith('```'):
                # 移除 ``` 开头和 ``` 结尾
                start_idx = clean_result.find('\n') + 1
                end_idx = clean_result.rfind('```')
                if end_idx == -1:
                    end_idx = len(clean_result)
                clean_result = clean_result[start_idx:end_idx].strip()

            parsed_result = json.loads(clean_result)

            # 验证返回的是字典格式
            if isinstance(parsed_result, dict):
                # 验证所有值都是字符串类型且内容存在于原文中
                validated_result = {}
                for section_name, section_content in parsed_result.items():
                    if isinstance(section_content, str) and section_content.strip():
                        # 验证内容是否在原文中存在（允许一定容错）
                        if self._validate_content_in_original(section_content, original_text):
                            validated_result[section_name] = section_content.strip()

                if validated_result:
                    return validated_result

        except (json.JSONDecodeError, KeyError, TypeError):
            logger.warning(f"无法解析JSON格式的结果: {result[:100]}..., 尝试修复json")
            # 尝试使用json_repair修复JSON
            try:
                repaired_json = repair_json(result)
                parsed_result = json.loads(repaired_json)

                # 验证返回的是字典格式
                if isinstance(parsed_result, dict):
                    # 验证所有值都是字符串类型且内容存在于原文中
                    validated_result = {}
                    for section_name, section_content in parsed_result.items():
                        if isinstance(section_content, str) and section_content.strip():
                            # 验证内容是否在原文中存在（允许一定容错）
                            if self._validate_content_in_original(section_content, original_text):
                                validated_result[section_name] = section_content.strip()

                    if validated_result:
                        return validated_result
            except Exception as repair_error:
                logger.error(f"JSON修复失败: {repair_error}")
                raise Exception(f"JSON修复失败: {repair_error}")

    def _validate_content_in_original(self, chunk_content: str, original_text: str) -> bool:
        """
        验证分块内容是否在原文中存在
        """
        if not chunk_content or not original_text:
            return False

        # 简单的验证：检查关键片段是否在原文中
        chunk_lines = chunk_content.split('\n')
        if len(chunk_lines) > 0:
            # 取第一行的关键部分进行验证
            sample_text = chunk_lines[0].strip()
            if len(sample_text) > 10:  # 至少10个字符才进行验证
                return sample_text in original_text

        return True

# 使用示例
if __name__ == "__main__":
    # 示例用法
    sample_judgment = """
    北京市朝阳区人民法院
    民事判决书
    (2023)京0105民初12345号
    
    原告：张三，男，1980年1月1日出生，汉族，住北京市朝阳区xxx街道xxx号。
    被告：李四，男，1985年2月2日出生，汉族，住北京市海淀区xxx街道xxx号。
    
    本院在审理原告张三与被告李四合同纠纷一案中，依法组成合议庭，公开开庭进行了审理。
    原告张三及其委托代理人王律师，被告李四及其委托代理人赵律师到庭参加诉讼。
    
    经审理查明：2022年1月1日，原告与被告签订房屋租赁合同，约定原告将其位于朝阳区的房屋出租给被告使用。
    合同约定租期一年，租金每月5000元。合同签订后，原告依约交付房屋，但被告未按时支付租金。
    
    本院认为：原、被告签订的房屋租赁合同合法有效。被告未按时支付租金构成违约。
    
    依照《中华人民共和国民法典》第五百七十七条、第五百七十九条规定，判决如下：
    一、被告李四于本判决生效后十日内向原告张三支付租金60000元；
    二、驳回原告张三的其他诉讼请求。
    
    审判长：xxx
    审判员：xxx  
    人民陪审员：xxx
    二〇二三年五月十五日
    书记员：xxx
    """

    # 使用AI进行分块
    try:
        chunker = LegalDocumentAIChunker()
        result = chunker.chunk_legal_document(
            document_text=sample_judgment
        )

        print("分块结果:")
        for section_name, content in result.items():
            print(f"\n【{section_name}】:")
            print(content[:200] + "..." if len(content) > 200 else content)  # 只显示前200字符
    except Exception as e:
        print(f"处理过程中出现错误: {e}")
