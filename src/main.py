# src/main.py
import logging
from typing import Optional
import json

from core.llm_client import OpenAIClient
from core.memory import ExecutionMemory
from agents.planner import PlannerAgent
from agents.corrector import CorrectorAgent
from engine.spark_executor import SparkEngine
from engine.duckdb_executor import DuckDBEngine
from engine.executor import QueryExecutor, ExecutionStrategy
from tools.sql_generator import SQLGenerator
from tools.rag_search import RAGSearch
from tools.python_plotter import PythonPlotter

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("industrial_agent.main")

class DataAnalysisAgent:
    """主 Agent：协调规划、执行、纠错的全流程"""
    
    def __init__(self, llm_api_key: str, 
                 primary_engine: str = "spark",
                 enable_fallback: bool = True):
        """
        :param llm_api_key: OpenAI API Key
        :param primary_engine: 主引擎 ('spark' 或 'duckdb')
        :param enable_fallback: 是否启用降级引擎
        """
        self.llm = OpenAIClient(api_key=llm_api_key)
        self.memory = ExecutionMemory()
        self.planner = PlannerAgent(llm=self.llm)
        self.corrector = CorrectorAgent(llm=self.llm, memory=self.memory)
        
        # 初始化数据引擎
        if primary_engine == "spark":
            primary = SparkEngine()
            fallback = DuckDBEngine() if enable_fallback else None
        else:
            primary = DuckDBEngine()
            fallback = SparkEngine() if enable_fallback else None
        
        self.executor = QueryExecutor(
            primary_engine=primary,
            fallback_engine=fallback,
            enable_fallback=enable_fallback
        )
        
        # 初始化工具
        self.sql_generator = SQLGenerator(llm=self.llm)
        self.rag_search = RAGSearch()
        self.plotter = PythonPlotter()
        
        logger.info(f"Agent 初始化完成 [主引擎: {primary_engine}, "
                   f"降级: {enable_fallback}]")
    
    def analyze(self, user_query: str, enable_visualization: bool = True) -> dict:
        """
        主要分析流程
        
        :param user_query: 用户查询
        :param enable_visualization: 是否生成可视化
        :return: 分析结果
        """
        logger.info(f"{'='*60}")
        logger.info(f"开始分析任务")
        logger.info(f"用户查询: {user_query}")
        logger.info(f"{'='*60}")
        
        try:
            # 第 1 步：检索相关知识
            logger.info("【步骤 1】检索知识库...")
            similar_cases = self.rag_search.search_similar_cases(user_query, top_k=2)
            if similar_cases:
                logger.info(f"✓ 找到 {len(similar_cases)} 个相似案例")
            
            # 第 2 步：获取 Schema
            logger.info("【步骤 2】获取数据库 Schema...")
            primary_engine = self.executor.primary_engine
            primary_engine.connect()
            schema_info = primary_engine.get_schema()
            primary_engine.close()
            logger.info(f"✓ 获取到 {len(schema_info)} 个表的 Schema")
            
            # 第 3 步：规划
            logger.info("【步骤 3】生成执行计划...")
            plan = self.planner.generate_plan(user_query, schema_info)
            logger.info(f"✓ 生成 {len(plan.steps)} 个执行步骤")
            
            # 第 4 步：逐步执行
            logger.info("【步骤 4】开始执行查询...")
            results = {}
            all_data = []
            
            for step in plan.steps:
                logger.info(f"  └─ 执行步骤 {step.step_id}: {step.step_name}")
                
                if step.tool_needed == 'SQL_Executor':
                    # 尝试用 SQL Generator 优化查询
                    optimized_query = self.sql_generator.optimize_query(
                        step.description, schema_info
                    )
                    
                    success, result, error = self.executor.execute_safely(
                        optimized_query,
                        strategy=ExecutionStrategy.PRIMARY
                    )
                    
                    # 如果失败，自动纠错
                    if not success and error:
                        logger.warning(f"  ✗ 步骤 {step.step_id} 失败，尝试纠错...")
                        revised_query = self.corrector.correct(
                            failed_query=step.description,
                            error_message=error,
                            schema_info=schema_info,
                            attempt_num=1
                        )
                        
                        if revised_query:
                            success, result, error = self.executor.execute_safely(
                                revised_query,
                                strategy=ExecutionStrategy.PRIMARY
                            )
                            
                            if success:
                                logger.info(f"  ✓ 纠错成功！")
                                # 记录解决方案到知识库
                                self.rag_search.record_solution(
                                    error_pattern=error,
                                    solution="调整查询逻辑",
                                    sql_example=revised_query
                                )
                    
                    self.memory.record_step(
                        step_id=step.step_id,
                        step_name=step.step_name,
                        query=step.description,
                        result=result,
                        error=error,
                        success=success
                    )
                    
                    if success:
                        all_data.append(result)
                    
                    results[f"step_{step.step_id}"] = {
                        "success": success,
                        "rows": len(result) if result is not None else 0,
                        "error": error
                    }
            
            # 第 5 步：可视化
            logger.info("【步骤 5】生成可视化...")
            visualizations = {}
            if enable_visualization and all_data:
                for i, data in enumerate(all_data):
                    plot_type = self.plotter.suggest_plot_type(data)
                    plot_code = self.plotter.generate_plot_code(
                        data,
                        plot_type=plot_type,
                        title=f"Analysis Result {i+1}"
                    )
                    
                    if plot_code:
                        plot_path = self.plotter.save_plot_code(
                            plot_code,
                            f"result_{i+1}"
                        )
                        visualizations[f"plot_{i+1}"] = {
                            "type": plot_type,
                            "code_path": plot_path
                        }
            
            logger.info(f"{'='*60}")
            logger.info(f"✓ 分析任务完成")
            logger.info(f"{'='*60}\n")
            
            return {
                "status": "success",
                "plan": plan.dict() if hasattr(plan, 'dict') else str(plan),
                "results": results,
                "visualizations": visualizations,
                "execution_metrics": self.executor.get_metrics(),
                "execution_history": self.memory.history
            }
        
        except Exception as e:
            logger.error(f"任务执行失败: {e}", exc_info=True)
            return {
                "status": "failed",
                "error": str(e),
                "execution_history": self.memory.history
            }

if __name__ == "__main__":
    # 示例使用
    agent = DataAnalysisAgent(
        llm_api_key="sk-your-api-key",
        primary_engine="spark",
        enable_fallback=True
    )
    
    result = agent.analyze(
        user_query="近 30 天内销售额 TOP 10 的 产品 是什么？请分析增长趋势。",
        enable_visualization=True
    )
    
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))