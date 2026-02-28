# Industrial Data Analysis Agent

Lightweight orchestrator for LLM-driven data analysis: planning, safe SQL execution, self-correction and visualization.

## Features
- LLM-powered planning: [`agents.planner.PlannerAgent`](src/agents/planner.py)  
- Safe execution with fallback engines: [`engine.executor.QueryExecutor`](src/engine/executor.py) using [`engine.spark_executor.SparkEngine`](src/engine/spark_executor.py) and [`engine.duckdb_executor.DuckDBEngine`](src/engine/duckdb_executor.py)  
- Execution memory for diagnostics: [`core.memory.ExecutionMemory`](src/core/memory.py)  
- LLM adapter: [`core.llm_client.LLMClient`](src/core/llm_client.py) / [`core.llm_client.OpenAIClient`](src/core/llm_client.py)  
- SQL generation, RAG and plotting tools: [`tools.sql_generator.SQLGenerator`](src/tools/sql_generator.py), [`tools.rag_search.RAGSearch`](src/tools/rag_search.py), [`tools.python_plotter.PythonPlotter`](src/tools/python_plotter.py)  
- Main orchestrator: [`main.DataAnalysisAgent`](src/main.py)

## Quickstart

1. Create a virtual environment and install dependencies (pyproject.toml):
```sh
pip install -e .
# or
pip install -r requirements.txt
```

2. Set your OpenAI API key (or edit the example in `src/main.py`):
```sh
export OPENAI_API_KEY="sk-your-api-key"
```

3. Run the example:
```sh
python src/main.py
```

## Usage (programmatic)
Instantiate the agent and run analysis:
```py
from src.main import DataAnalysisAgent
agent = DataAnalysisAgent(llm_api_key="sk-xxx", primary_engine="duckdb", enable_fallback=True)
res = agent.analyze("近 30 天内销售额 TOP 10 的 产品 是什么？请分析增长趋势。")
print(res)
```

## Project layout
- Core LLM + memory: [src/core/llm_client.py](src/core/llm_client.py), [src/core/memory.py](src/core/memory.py)  
- Agents: [src/agents/planner.py](src/agents/planner.py), [src/agents/corrector.py](src/agents/corrector.py)  
- Engines: [src/engine/base.py](src/engine/base.py), [src/engine/duckdb_executor.py](src/engine/duckdb_executor.py), [src/engine/spark_executor.py](src/engine/spark_executor.py)  
- Tools: [src/tools/sql_generator.py](src/tools/sql_generator.py), [src/tools/rag_search.py](src/tools/rag_search.py), [src/tools/python_plotter.py](src/tools/python_plotter.py)  
- Entry point: [src/main.py](src/main.py)

## Testing
Run tests with pytest:
```sh
pytest
```

## Contributing
- Follow existing logging and typing patterns.
- Add unit tests under `tests/`.
- Keep prompts and LLM calls encapsulated in `core/llm_client.py`.

## License
See project root for license / authorship information.