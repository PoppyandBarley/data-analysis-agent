# src/core/llm_client.py
from abc import ABC, abstractmethod
from typing import Optional
import logging

logger = logging.getLogger("industrial_agent.llm_client")

class LLMClient(ABC):
    """LLM 调用接口抽象"""
    
    @abstractmethod
    def chat(self, system_prompt: str, user_input: str, temperature: float = 0.7) -> str:
        """调用 LLM API"""
        pass

class OpenAIClient(LLMClient):
    """OpenAI GPT 实现"""
    
    def __init__(self, api_key: str, model: str = "gpt-4"):
        self.api_key = api_key
        self.model = model
    
    def chat(self, system_prompt: str, user_input: str, temperature: float = 0.7) -> str:
        import openai
        openai.api_key = self.api_key
        
        try:
            response = openai.ChatCompletion.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_input}
                ],
                temperature=temperature,
                max_tokens=2000
            )
            return response.choices[0].message.content
        except Exception as e:
            logger.error(f"LLM 调用失败: {e}")
            raise