# src/agents/corrector.py
import logging
import json
from typing import Optional
from pydantic import BaseModel, Field
from tenacity import retry, stop_after_attempt, wait_exponential

from core.llm_client import LLMClient
from core.memory import ExecutionMemory

logger = logging.getLogger("industrial_agent.corrector")

class CorrectionAttempt(BaseModel):
    """纠错尝试"""
    attempt_num: int = Field(..., description="纠错第几次")
    revised_query: str = Field(..., description="修正后的 SQL 查询")
    reasoning: str = Field(..., description="为什么这样修正可以解决问题")

class CorrectorAgent:
    """自我纠错 Agent"""
    
    def __init__(self, llm: LLMClient, memory: ExecutionMemory):
        self.llm = llm
        self.memory = memory
    
    @retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=1, min=1, max=5))
    def correct(self, failed_query: str, error_message: str, 
                schema_info: dict, attempt_num: int = 1) -> Optional[str]:
        """
        根据错误信息生成修正后的查询
        """
        logger.info(f"开始第 {attempt_num} 次纠错...")
        
        failure_context = self.memory.get_failure_context()
        
        system_prompt = f"""
        你是一个数据库查询修正专家。用户的 SQL 查询执行失败了。
        
        【失败的查询】
        {failed_query}
        
        【错误信息】
        {error_message}
        
        【前期失败记录】
        {failure_context}
        
        【可用的数据结构】
        {json.dumps(schema_info, ensure_ascii=False, indent=2)}
        
        请直接返回修正后的 SQL 查询，不要包含任何额外说明。
        """
        
        try:
            revised_query = self.llm.chat(
                system_prompt=system_prompt,
                user_input=f"这是第 {attempt_num} 次纠错尝试。请修正这个查询。",
                temperature=0.3  # 更加保守
            )
            
            logger.info(f"纠错完成: {revised_query[:80]}...")
            return revised_query.strip()
        except Exception as e:
            logger.error(f"纠错过程本身失败: {e}")
            return None