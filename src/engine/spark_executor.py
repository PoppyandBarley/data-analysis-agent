# src/engine/spark_executor.py
from pyspark.sql import SparkSession
import pandas as pd
from typing import Dict, Any
from .base import DataEngine

class SparkEngine(DataEngine):
    def __init__(self, app_name: str = "IndustrialDataAgent", master: str = "local[*]"):
        """
        :param master: 集群地址。'local[*]' 表示使用本地所有 CPU 核心。
                       在生产环境，这里通常是 'yarn' 或 'k8s'。
        """
        self.app_name = app_name
        self.master = master
        self.spark = None

    def connect(self) -> None:
        """初始化 SparkSession"""
        try:
            # 工业级配置参考：针对大数据量调整内存和并行度
            self.spark = SparkSession.builder \
                .appName(self.app_name) \
                .master(self.master) \
                .config("spark.driver.memory", "4g") \
                .config("spark.executor.memory", "8g") \
                .config("spark.sql.shuffle.partitions", "200") \
                .get_session()
            print(f"成功启动 Spark Session: {self.app_name}")
        except Exception as e:
            print(f"Spark 连接失败: {e}")
            raise

    def execute_query(self, query_code: str) -> pd.DataFrame:
        """
        执行查询。
        在大数据量下，Agent 生成的 SQL 必须包含聚合（SUM/AVG）或 LIMIT。
        """
        if not self.spark:
            self.connect()

        if not self.validate_code(query_code):
            raise ValueError("检测到不安全的 SQL 语句！")

        print(f"Spark 正在分布式计算: {query_code}")
        
        sdf = self.spark.sql(query_code)
        print(f"Spark 计算完成，结果前 1000 行:")
        print(sdf.show(1000))
        
        return sdf.limit(1000).toPandas()

    def get_schema(self) -> Dict[str, Any]:
        """从 Spark Catalog 提取所有表的结构"""
        if not self.spark:
            self.connect()

        schema_info = {}
        # 获取当前数据库的所有表
        tables = self.spark.catalog.listTables()
        
        for table in tables:
            table_name = table.name
            columns = self.spark.table(table_name).schema
            schema_info[table_name] = {field.name: field.dataType.simpleString() for field in columns}
        
        return schema_info
    
    def validate_code(self, query_code: str) -> bool:
        """
        1. 基础关键词过滤
        2. Spark 执行计划预审 (Explain)
        3. 强制性能约束
        """
        # 1. 静态黑名单
        forbidden_words = ["DROP", "TRUNCATE", "INSERT", "DELETE", "UPDATE"]
        if any(word in query_code.upper() for word in forbidden_words):
            print("检测到非法修改数据的指令。")
            return False

        # 2. 强制聚合或限制检查
        # 如果没有聚合函数且没有 LIMIT，则视为危险查询
        safe_keywords = ["COUNT", "SUM", "AVG", "GROUP BY", "LIMIT", "MAX", "MIN"]
        if not any(k in query_code.upper() for k in safe_keywords):
            print("在大数据量下，必须包含聚合逻辑或 LIMIT 限制。")
            return False

        # 3. 动态预审：使用 Spark 的 EXPLAIN 模式
        try:
            self.spark.sql(query_code).explain()
            return True
        except Exception as e:
            print(f"语法/逻辑校验失败: {e}")
            return False

    def close(self) -> None:
        """关闭 Spark 环境"""
        if self.spark:
            self.spark.stop()
            print("Spark Session 已安全关闭")