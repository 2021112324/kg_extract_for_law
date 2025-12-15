from typing import Dict, Any, Optional
from tavily import TavilyClient
from app.core.config import settings


class TavilyAPIClient:
    """
    Tavily API客户端，处理与Tavily搜索API的底层通信
    """
    
    def __init__(self, api_key: Optional[str] = None):
        """
        初始化Tavily API客户端
        
        参数:
            api_key: API密钥，如果未提供则使用配置中的密钥
        """
        self.api_key = api_key or settings.TAVILY_API_KEY
        if not self.api_key:
            raise ValueError("未配置Tavily API密钥，请在环境变量中设置TAVILY_API_KEY")
        
        self.client = TavilyClient(api_key=self.api_key)
    
    async def search(self, query: str, max_results: int = 5) -> Dict[str, Any]:
        """
        执行搜索请求
        
        参数:
            query: 搜索查询
            max_results: 最大结果数量
            
        返回:
            原始API响应
            
        异常:
            Exception: API调用失败时抛出
        """
        try:
            response = self.client.search(query=query, max_results=max_results)
            return response
        except Exception as e:
            raise Exception(f"Tavily API调用失败: {str(e)}")