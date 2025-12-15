from langfuse import Langfuse
from app.core.config import settings


class LangfuseClient:
    def __init__(self):
        self.langfuse_connector = Langfuse(
            secret_key=settings.LANGFUSE_SECRET_KEY,
            public_key=settings.LANGFUSE_PUBLIC_KEY,
            host=settings.LANGFUSE_HOST
        )

    def get_prompt_string(self, name, label="production", variables={}) -> str:
        """
        从Langfuse获取指定名称和版本的prompt字符串内容

        Args:
            name: prompt的唯一名称标识
            label: prompt的版本标签，默认为'production'
            variables: 包含变量的字典，向prompt中传入变量
        """
        prompt_obj = self.langfuse_connector.get_prompt(name=name, label=label)
        prompt_str = prompt_obj.compile(**variables)
        return prompt_str

langfuse_client = LangfuseClient()