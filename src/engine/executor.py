# src/engine/executor.py
from typing import Tuple, Any
import logging
import pandas as pd
from .base import DataEngine

logger = logging.getLogger("industrial_agent.executor")

class QueryExecutor:
    """统一的查询执行器，负责实际执行并捕获错误"""
    
    def __init__(self, engine: DataEngine):
        self.engine = engine
    
    def execute_safely(self, query: str) -> Tuple[bool, Any, str]:
        """
        安全执行查询，返回 (success, result, error_message)
        """
        try:
            self.engine.connect()
            result = self.engine.execute_query(query)
            logger.info(f"查询执行成功，返回 {len(result)} 行数据")
            return True, result, ""
        except ValueError as e:
            error_msg = f"SQL 校验失败: {str(e)}"
            logger.warning(error_msg)
            return False, None, error_msg
        except Exception as e:
            error_msg = f"执行错误: {str(e)}"
            logger.error(error_msg)
            return False, None, error_msg
        finally:
            self.engine.close()