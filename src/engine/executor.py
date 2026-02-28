# src/engine/executor.py
from typing import Tuple, Any, List
import logging
import pandas as pd
from .base import DataEngine

logger = logging.getLogger("industrial_agent.executor")

class QueryExecutor:
    """统一的查询执行器，负责实际执行并捕获错误"""
    
    def __init__(self, engine: DataEngine):
        self.engine = engine
    
    def execute_safely(self, query: str) -> Tuple[bool, List[Any], str]:
        """
        安全执行查询。
        返回: (success, result, error_message)
        保证：无论成功失败，result 始终为列表类型 (成功为数据，失败为空列表)，方便调用方直接遍历。
        """

        if not hasattr(self.engine, 'validate_code'):
            msg = "数据引擎缺失 validate_code 方法"
            logger.error(msg)
            return False, [], msg
            
        if not hasattr(self.engine, 'execute_query'):
            msg = "数据引擎缺失 execute_query 方法"
            logger.error(msg)
            return False, [], msg

        try:
            if not self.engine.validate_code(query):
                msg = f"SQL 校验未通过: {query[:50]}..." # 记录部分 SQL 用于审计，注意脱敏
                logger.warning(msg)
                return False, [], msg
        except Exception as e:
            msg = f"SQL 校验过程异常: {str(e)}"
            logger.error(msg, exc_info=True)
            return False, [], msg

        connection_established = False
        
        try:
            self.engine.connect()
            connection_established = True
            
            result = self.engine.execute_query(query)
            
            if result is None:
                result = []
                
            logger.info(f"查询执行成功，返回 {len(result)} 行数据")
            return True, result, ""
            
        except ValueError as e:
            error_msg = f"SQL 执行失败: {str(e)}"
            logger.warning(error_msg)
            return False, [], error_msg
            
        except Exception as e:
            error_msg = f"系统执行错误: {str(e)}"
            logger.error(error_msg, exc_info=True) # ⚠️ 记录完整堆栈
            return False, [], error_msg
            
        finally:
            if connection_established:
                try:
                    self.engine.close()
                except Exception as close_err:
                    logger.error(f"关闭数据库连接时发生次要错误: {close_err}", exc_info=True)