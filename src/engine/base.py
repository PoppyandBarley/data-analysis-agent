# src/engine/base.py
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
import pandas as pd

class DataEngine(ABC):
    """
    所有数据引擎的基类。
    定义了 Agent 必须如何与底层数据进行交互的。
    """

    @abstractmethod
    def connect(self) -> None:
        """建立数据库/集群连接"""
        pass

    @abstractmethod
    def execute_query(self, query_code: str) -> pd.DataFrame:
        """
        执行查询逻辑。
        对于 100GB 数据，这里返回的应该是聚合后的结果或样本，而不是全量数据。
        """
        pass

    @abstractmethod
    def get_schema(self) -> Dict[str, Any]:
        """获取元数据（表名、列名、数据类型），这是 Agent 写 SQL 的依据"""
        pass

    @abstractmethod
    def close(self) -> None:
        """关闭连接，释放资源"""
        pass

    def validate_code(self, code: str) -> bool:
        """
        预校验代码安全性或语法。
        基类可以提供默认实现，子类也可以重写它。
        """
        if "DROP" in code.upper() or "DELETE" in code.upper():
            return False
        return True