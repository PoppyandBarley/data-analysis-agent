# src/tools/rag_search.py
import logging
from typing import List, Dict, Any, Optional
import json
from pathlib import Path

logger = logging.getLogger("industrial_agent.tools.rag_search")

class RAGSearch:
    """
    检索增强生成 (RAG) 工具
    用于搜索相关文档、案例库、已知问题等
    """
    
    def __init__(self, knowledge_base_path: str = "./data/knowledge_base.json"):
        """
        :param knowledge_base_path: 知识库文件路径
        """
        self.knowledge_base_path = knowledge_base_path
        self.knowledge_base = self._load_knowledge_base()
    
    def _load_knowledge_base(self) -> Dict[str, Any]:
        """加载知识库"""
        kb_file = Path(self.knowledge_base_path)
        
        if kb_file.exists():
            try:
                with open(kb_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"加载知识库失败: {e}，使用默认空库")
        
        # 默认知识库结构
        return {
            "common_errors": [],
            "sql_patterns": [],
            "domain_knowledge": [],
            "schema_documentation": {}
        }
    
    def search_similar_cases(self, query: str, top_k: int = 3) -> List[Dict[str, Any]]:
        """
        搜索相似的历史案例
        
        :param query: 查询字符串
        :param top_k: 返回前 k 个结果
        :return: 相似案例列表
        """
        import difflib
        
        results = []
        search_candidates = self.knowledge_base.get("common_errors", [])
        
        # 简单的字符串相似度匹配（实际应用可使用向量数据库）
        matches = difflib.get_close_matches(
            query, 
            [case.get("error", "") for case in search_candidates],
            n=top_k,
            cutoff=0.3
        )
        
        for match in matches:
            for case in search_candidates:
                if case.get("error") == match:
                    results.append(case)
                    break
        
        logger.info(f"✓ 搜索到 {len(results)} 个相似案例")
        return results
    
    def search_sql_patterns(self, pattern_type: str) -> List[str]:
        """
        搜索 SQL 模式库
        
        :param pattern_type: 模式类型 (e.g., "aggregation", "join", "window_function")
        :return: 匹配的 SQL 模式列表
        """
        patterns = self.knowledge_base.get("sql_patterns", [])
        
        matching_patterns = [
            p.get("template")
            for p in patterns
            if p.get("type") == pattern_type
        ]
        
        logger.info(f"✓ 搜索到 {len(matching_patterns)} 个 {pattern_type} 模式")
        return matching_patterns
    
    def search_documentation(self, table_name: str) -> Optional[Dict[str, Any]]:
        """
        搜索表的文档说明
        
        :param table_name: 表名
        :return: 表的文档信息
        """
        docs = self.knowledge_base.get("schema_documentation", {})
        
        if table_name in docs:
            logger.info(f"✓ 找到表 {table_name} 的文档")
            return docs[table_name]
        
        logger.debug(f"⚠ 未找到表 {table_name} 的文档")
        return None
    
    def record_solution(self, error_pattern: str, solution: str, 
                       sql_example: str = "") -> None:
        """
        记录成功的解决方案到知识库
        用于不断改进 Agent 的能力
        
        :param error_pattern: 错误模式
        :param solution: 解决方法
        :param sql_example: SQL 示例
        """
        new_case = {
            "error": error_pattern,
            "solution": solution,
            "sql_example": sql_example,
            "timestamp": str(__import__('datetime').datetime.now())
        }
        
        self.knowledge_base["common_errors"].append(new_case)
        self._save_knowledge_base()
        
        logger.info(f"✓ 记录新的解决方案: {error_pattern[:50]}...")
    
    def _save_knowledge_base(self) -> None:
        """保存知识库到文件"""
        kb_file = Path(self.knowledge_base_path)
        kb_file.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            with open(kb_file, 'w', encoding='utf-8') as f:
                json.dump(self.knowledge_base, f, ensure_ascii=False, indent=2)
            logger.debug(f"知识库已保存到 {self.knowledge_base_path}")
        except Exception as e:
            logger.error(f"保存知识库失败: {e}")