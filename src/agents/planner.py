# src/agents/planner.py
import json
import logging
import re
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field, ValidationError, field_validator
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# 假设这是你的 LLM 接口抽象
from core.llm_client import LLMClient

# 配置标准日志
logger = logging.getLogger("industrial_agent.planner")

# ----------------------------------------------------------------
# 1. 定义数据结构 (Pydantic Models with Validation)
# ----------------------------------------------------------------

class AnalysisStep(BaseModel):
    step_id: int = Field(..., description="步骤序号，从1开始")
    step_name: str = Field(..., description="步骤简短名称")
    description: str = Field(..., description="详细说明，包含具体的过滤条件或聚合维度")
    tool_needed: str = Field(..., description="必须是: 'SQL_Executor', 'Python_Plotter', 'RAG_Search' 之一")
    reasoning: str = Field(..., description="思维链：为什么这一步在海量数据下是安全的？")

    @field_validator('tool_needed')
    def validate_tool(cls, v):
        allowed = ['SQL_Executor', 'Python_Plotter', 'RAG_Search']
        if v not in allowed:
            raise ValueError(f"工具 {v} 不在允许列表中: {allowed}")
        return v

class AnalysisPlan(BaseModel):
    goal: str = Field(..., description="用户需求的清晰重述")
    steps: List[AnalysisStep] = Field(..., description="执行步骤")
    risk_assessment: str = Field(..., description="针对100GB数据的性能风险评估")

    @field_validator('steps')
    def validate_steps_not_empty(cls, v):
        if not v:
            raise ValueError("生成的计划步骤不能为空")
        return v

# ----------------------------------------------------------------
# 2. 辅助工具：输出清洗器 (Output Sanitizer)
# ----------------------------------------------------------------

class OutputParser:
    @staticmethod
    def extract_json(text: str) -> str:
        """
        工业界常见痛点：LLM 经常在 JSON 外面包一层 markdown 代码块，
        或者在最后加一句 'Hope this helps'。这里强制提取 JSON 部分。
        """
        try:
            # 1. 尝试移除 markdown 代码块标记
            cleaned = re.sub(r"```json\s*", "", text, flags=re.IGNORECASE)
            cleaned = re.sub(r"```", "", cleaned)
            
            # 2. 如果包含非 JSON 字符，尝试寻找最外层的 {}
            start = cleaned.find('{')
            end = cleaned.rfind('}') + 1
            if start != -1 and end != 0:
                cleaned = cleaned[start:end]
            return cleaned.strip()
        except Exception:
            return text  # 提取失败则原样返回，交给 JSON 解析器报错

# ----------------------------------------------------------------
# 3. 核心 Agent 类
# ----------------------------------------------------------------

class PlannerAgent:
    def __init__(self, llm: LLMClient):
        self.llm = llm
    
    # 工业级重试机制：如果解析失败或网络错误，最多重试 3 次，每次间隔指数级增加 (1s, 2s, 4s...)
    @retry(
        stop=stop_after_attempt(3), 
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((json.JSONDecodeError, ValidationError, ValueError)),
        reraise=True
    )
    def generate_plan(self, user_query: str, schema_info: Dict[str, Any]) -> AnalysisPlan:
        """
        生成并校验执行计划。如果失败会自动重试。
        """
        logger.info(f"开始规划任务: {user_query[:50]}...")
        
        system_prompt = self._build_system_prompt(schema_info)
        
        try:
            # 调用 LLM
            response_text = self.llm.chat(
                system_prompt=system_prompt,
                user_input=user_query,
                temperature=0.2  # 降低温度，增加确定性
            )
            
            # 清洗输出
            cleaned_json = OutputParser.extract_json(response_text)
            logger.debug(f"LLM 原始输出清洗后: {cleaned_json[:100]}...")

            # 严格校验
            plan_dict = json.loads(cleaned_json)
            plan = AnalysisPlan(**plan_dict)
            
            logger.info(f"计划生成成功: {len(plan.steps)} 个步骤")
            return plan

        except (json.JSONDecodeError, ValidationError) as e:
            logger.warning(f"计划解析失败，准备重试。错误: {e}")
            # 在这里，高级做法是将错误信息 e 反馈给 LLM 进行 Self-Correction（自我修正）
            # 为了演示简洁，这里直接抛出异常触发 @retry
            raise e

    def _build_system_prompt(self, schema_info: Dict[str, Any]) -> str:
        # 如果 Schema 非常大，这里应该只放入 'Relevant Schema' (通过向量检索获得)
        schema_str = json.dumps(schema_info, ensure_ascii=False, indent=2)
        
        return f"""
        你是一个专门处理 PB 级数据的架构师。你的目标是将用户问题转化为结构化的执行步骤。

        【核心原则】
        1. **Cost Awareness**: 每一条 SQL 都在消耗巨大的计算资源。必须优先使用 `sample` 或 `approx_distinct` 等估算函数。
        2. **Iterative**: 不要试图用一个超级复杂的 SQL 解决所有问题。将其拆分为：提取子集 -> 聚合统计 -> 二次分析。
        3. **Fallback**: 如果用户提到的字段在 Schema 中不存在，请在 Reasoning 中说明，并尝试使用最接近的字段。

        【可用工具】
        - SQL_Executor: 执行 SQL 查询 (Spark/DuckDB)。
        - Python_Plotter: 生成图表代码。
        - RAG_Search: 检索业务文档（如计算公式定义）。

        【数据结构】
        {schema_str}

        请直接返回 JSON 对象，不要包含任何 Markdown 格式或额外说明。格式必须符合 AnalysisPlan 定义。
        """