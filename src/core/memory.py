# src/core/memory.py
from typing import List, Dict, Any
from datetime import datetime
import json

class ExecutionMemory:
    """存储执行过程中的中间结果、错误信息，用于自我纠错"""
    
    def __init__(self):
        self.history: List[Dict[str, Any]] = []
    
    def record_step(self, step_id: int, step_name: str, query: str, 
                    result: Any = None, error: str = None, success: bool = True):
        """记录每一步的执行过程"""
        record = {
            "timestamp": datetime.now().isoformat(),
            "step_id": step_id,
            "step_name": step_name,
            "query": query,
            "success": success,
            "result_preview": str(result)[:200] if result is not None else None,
            "error": error
        }
        self.history.append(record)
    
    def get_failure_context(self) -> str:
        """将失败信息格式化，供自我纠错提示词使用"""
        failures = [h for h in self.history if not h["success"]]
        if not failures:
            return ""
        
        context = "【前期执行失败记录】\n"
        for f in failures[-3:]:  # 只保留最近 3 条失败
            context += f"步骤 {f['step_id']}: {f['step_name']}\n"
            context += f"  查询: {f['query'][:100]}...\n"
            context += f"  错误: {f['error']}\n"
        return context
    
    def clear(self):
        """清空历史记录"""
        self.history = []