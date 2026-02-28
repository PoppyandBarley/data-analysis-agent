# src/tools/sql_generator.py
import logging
from typing import List, Dict, Any
import re
from core.llm_client import LLMClient

logger = logging.getLogger("industrial_agent.tools.sql_generator")

class SQLGenerator:
    """
    LLM 驱动的 SQL 生成工具
    负责：
    1. 自然语言 → SQL 转换
    2. SQL 语法验证
    3. 查询优化建议
    """
    
    def __init__(self, llm: LLMClient):
        self.llm = llm
        self.generated_queries = []
    
    def generate_from_nl(self, user_intent: str, schema_info: Dict[str, Any],
                         previous_context: str = "") -> str:
        """
        从自然语言生成 SQL
        
        :param user_intent: 用户意图描述
        :param schema_info: 数据库 Schema 信息
        :param previous_context: 前期执行步骤的上下文
        :return: 生成的 SQL 查询
        """
        import json
        
        system_prompt = f"""
你是一个数据库 SQL 专家。根据用户意图和数据库 Schema，生成精准的 SQL 查询。

【数据库 Schema】
{json.dumps(schema_info, ensure_ascii=False, indent=2)}

【要求】
1. 必须使用真实存在的表名和列名
2. 如果涉及大数据量操作，必须包含 LIMIT 或聚合函数
3. 只返回 SQL 语句，不要包含任何解释
4. 使用标准 SQL 语法

【前期上下文】
{previous_context if previous_context else "无"}
"""
        
        try:
            sql_query = self.llm.chat(
                system_prompt=system_prompt,
                user_input=f"生成 SQL: {user_intent}",
                temperature=0.2  # 保守模式
            )
            
            # 清理生成的 SQL（移除 markdown 包装等）
            sql_query = self._clean_sql(sql_query)
            self.generated_queries.append({
                "intent": user_intent,
                "query": sql_query
            })
            
            logger.info(f"✓ 生成 SQL: {sql_query[:80]}...")
            return sql_query
        
        except Exception as e:
            logger.error(f"SQL 生成失败: {e}")
            raise
    
    def _clean_sql(self, raw_sql: str) -> str:
        """清理 LLM 生成的 SQL（移除 markdown 包装、多余空格等）"""
        # 移除 ```sql``` 包装
        raw_sql = re.sub(r'```sql\n?', '', raw_sql)
        raw_sql = re.sub(r'```\n?', '', raw_sql)
        
        # 移除注释和多余空格
        raw_sql = re.sub(r'--.*$', '', raw_sql, flags=re.MULTILINE)
        raw_sql = re.sub(r'/\*.*?\*/', '', raw_sql, flags=re.DOTALL)
        raw_sql = ' '.join(raw_sql.split())
        
        return raw_sql.strip()
    
    def optimize_query(self, query: str, schema_info: Dict[str, Any]) -> str:
        """
        优化 SQL 查询的性能
        
        :param query: 原始查询
        :param schema_info: Schema 信息
        :return: 优化后的查询建议
        """
        import json
        
        system_prompt = f"""
你是一个 SQL 性能优化专家。分析以下查询并提供优化建议。

【Schema 信息】
{json.dumps(schema_info, ensure_ascii=False, indent=2)}

【原始查询】
{query}

请返回优化后的 SQL，如果无法优化则返回原查询。
"""
        
        try:
            optimized = self.llm.chat(
                system_prompt=system_prompt,
                user_input="优化这个查询",
                temperature=0.2
            )
            
            optimized = self._clean_sql(optimized)
            logger.info(f"✓ 查询优化完成")
            return optimized
        
        except Exception as e:
            logger.warning(f"优化失败，使用原查询: {e}")
            return query
    
    def get_query_history(self) -> List[Dict[str, str]]:
        """获取生成的查询历史"""
        return self.generated_queries