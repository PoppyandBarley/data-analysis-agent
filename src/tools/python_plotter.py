# src/tools/python_plotter.py
import logging
from typing import Any, Dict, Optional
import pandas as pd
import json
from pathlib import Path

logger = logging.getLogger("industrial_agent.tools.python_plotter")

class PythonPlotter:
    """
    数据可视化工具
    负责生成图表、生成可视化代码
    """
    
    def __init__(self, output_dir: str = "./outputs/plots"):
        """
        :param output_dir: 图表输出目录
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.plot_history = []
    
    def generate_plot_code(self, data: pd.DataFrame, 
                          plot_type: str = "line",
                          title: str = "",
                          x_col: str = "",
                          y_col: str = "") -> str:
        """
        生成 matplotlib/plotly 绘图代码
        
        :param data: 数据 DataFrame
        :param plot_type: 图表类型 ('line', 'bar', 'scatter', 'heatmap')
        :param title: 图表标题
        :param x_col: X 轴列
        :param y_col: Y 轴列
        :return: 绘图代码
        """
        if data.empty:
            logger.warning("数据为空，无法生成图表")
            return ""
        
        # 自动推断列
        if not x_col and len(data.columns) > 0:
            x_col = data.columns[0]
        if not y_col and len(data.columns) > 1:
            y_col = data.columns[1]
        
        code_templates = {
            "line": self._template_line(data, title, x_col, y_col),
            "bar": self._template_bar(data, title, x_col, y_col),
            "scatter": self._template_scatter(data, title, x_col, y_col),
            "heatmap": self._template_heatmap(data, title)
        }
        
        code = code_templates.get(plot_type, code_templates["line"])
        logger.info(f"✓ 生成 {plot_type} 类型的绘图代码")
        
        return code
    
    def _template_line(self, data: pd.DataFrame, title: str, 
                       x_col: str, y_col: str) -> str:
        """折线图模板"""
        return f"""
import matplotlib.pyplot as plt
import pandas as pd

# 数据
data = {data.to_dict()}
df = pd.DataFrame(data)

# 绘图
plt.figure(figsize=(12, 6))
plt.plot(df['{x_col}'], df['{y_col}'], marker='o', linewidth=2)
plt.title('{title}')
plt.xlabel('{x_col}')
plt.ylabel('{y_col}')
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.show()
"""
    
    def _template_bar(self, data: pd.DataFrame, title: str,
                      x_col: str, y_col: str) -> str:
        """柱状图模板"""
        return f"""
import matplotlib.pyplot as plt
import pandas as pd

data = {data.to_dict()}
df = pd.DataFrame(data)

plt.figure(figsize=(12, 6))
plt.bar(df['{x_col}'], df['{y_col}'], color='steelblue')
plt.title('{title}')
plt.xlabel('{x_col}')
plt.ylabel('{y_col}')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
"""
    
    def _template_scatter(self, data: pd.DataFrame, title: str,
                         x_col: str, y_col: str) -> str:
        """散点图模板"""
        return f"""
import matplotlib.pyplot as plt
import pandas as pd

data = {data.to_dict()}
df = pd.DataFrame(data)

plt.figure(figsize=(12, 6))
plt.scatter(df['{x_col}'], df['{y_col}'], alpha=0.6, s=100)
plt.title('{title}')
plt.xlabel('{x_col}')
plt.ylabel('{y_col}')
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.show()
"""
    
    def _template_heatmap(self, data: pd.DataFrame, title: str) -> str:
        """热力图模板"""
        return f"""
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

data = {data.to_dict()}
df = pd.DataFrame(data)

plt.figure(figsize=(12, 8))
sns.heatmap(df, annot=True, fmt='.2f', cmap='YlOrRd')
plt.title('{title}')
plt.tight_layout()
plt.show()
"""
    
    def save_plot_code(self, code: str, filename: str) -> str:
        """
        保存绘图代码到文件
        
        :param code: 绘图代码
        :param filename: 文件名（不含后缀）
        :return: 保存路径
        """
        filepath = self.output_dir / f"{filename}.py"
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(code)
            
            logger.info(f"✓ 绘图代码已保存: {filepath}")
            return str(filepath)
        except Exception as e:
            logger.error(f"保存绘图代码失败: {e}")
            return ""
    
    def suggest_plot_type(self, data: pd.DataFrame, 
                         columns: Optional[list] = None) -> str:
        """
        根据数据自动推荐图表类型
        
        :param data: 数据
        :param columns: 要绘制的列
        :return: 推荐的图表类型
        """
        if data.empty:
            return "line"
        
        n_cols = len(columns) if columns else len(data.columns)
        n_rows = len(data)
        
        # 简单的启发式规则
        if n_rows > 100:
            return "heatmap"
        elif n_cols == 2:
            return "scatter"
        elif n_cols > 2:
            return "line"
        else:
            return "bar"