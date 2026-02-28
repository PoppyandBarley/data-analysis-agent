# src/engine/duckdb_executor.py
import duckdb
import pandas as pd
from typing import Dict, Any
from .base import DataEngine  # 确保你已经创建了上一步的 base.py

class DuckDBEngine(DataEngine):
    def __init__(self, db_path: str = ":memory:"):
        """
        :param db_path: 数据库文件路径。如果是 ":memory:" 则在内存中运行。
                        对于 100GB 数据，建议指向一个磁盘路径，如 "data/my_warehouse.db"
        """
        self.db_path = db_path
        self.conn = None

    def connect(self) -> None:
        """建立连接"""
        try:
            self.conn = duckdb.connect(self.db_path)
            print(f"成功连接到 DuckDB: {self.db_path}")
        except Exception as e:
            print(f"连接失败: {e}")
            raise

    def execute_query(self, query_code: str) -> pd.DataFrame:
        """执行 SQL 并返回 Pandas DataFrame"""
        if not self.conn:
            self.connect()
        
        if not self.validate_code(query_code):
            raise ValueError("检测到不安全的 SQL 语句！")

        print(f"正在执行查询: {query_code}")

        return self.conn.execute(query_code).df()

    def get_schema(self) -> Dict[str, Any]:
        """提取数据库中所有表的结构信息，供 Agent 参考"""
        if not self.conn:
            self.connect()

        schema_info = {}
        # 查询 DuckDB 的元数据表
        tables = self.conn.execute("SHOW TABLES").fetchall()
        
        for (table_name,) in tables:
            columns = self.conn.execute(f"DESCRIBE {table_name}").fetchall()
            schema_info[table_name] = {col[0]: col[1] for col in columns}
        
        return schema_info

    def close(self) -> None:
        """释放资源"""
        if self.conn:
            self.conn.close()
            print("DuckDB 连接已关闭")
