# local_chart_generator.py (最終修正版，移除字型防護邏輯，確保流程執行)

import matplotlib.pyplot as plt
import pandas as pd
import os
import re

# ⚠️ 警告：此版本移除了複雜的中文字型自動尋找邏輯，
# 如果圖表出現中文亂碼（方框），請手動在 plt.rcParams['font.family'] 中設定中文字型。

# 定義儲存圖表的資料夾
OUTPUT_DIR = 'charts'
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

# --- 繪圖函數 ---
def generate_local_chart(data_df: pd.DataFrame, chart_spec: dict, user_query: str) -> str:
    """
    使用 Matplotlib 根據 LLM 提供的規格繪製圖表，並將圖片儲存到本地。
    """
    chart_type = chart_spec.get('chart_type', '').lower()
    x_col = chart_spec.get('x_axis', data_df.columns[0])
    y_col = chart_spec.get('y_axis', data_df.columns[-1])
    title = chart_spec.get('title', '資料庫查詢結果圖表')
    
    if data_df.empty or x_col not in data_df.columns or y_col not in data_df.columns:
        print(f"❌ 繪圖錯誤：數據集為空或資料中缺少欄位 {x_col} 或 {y_col}。")
        return ""

    plt.figure(figsize=(12, 6))
    
    # --- 繪圖邏輯 ---
    if chart_type == 'bar':
        plt.bar(data_df[x_col], data_df[y_col], color='#4682B4')
        plt.xticks(rotation=45, ha='right')
        
    elif chart_type == 'line':
        plt.plot(data_df[x_col], data_df[y_col], marker='o', linestyle='-', color='#B8860B')
        
    elif chart_type == 'pie':
        plt.pie(data_df[y_col], labels=data_df[x_col], autopct='%1.1f%%', startangle=90, colors=plt.cm.Paired.colors)
        plt.axis('equal')
        
    else:
        plt.close()
        return ""

    # --- 通用設定與儲存 ---
    plt.title(title, fontsize=18)
    if chart_type != 'pie':
        plt.xlabel(x_col, fontsize=14)
        plt.ylabel(y_col, fontsize=14)
        plt.grid(axis='y', linestyle='--', alpha=0.7)
    
    plt.tight_layout()

    # 清理查詢文字以作為檔案名 (只保留中英文數字)
    safe_query = re.sub(r'[^\w\u4e00-\u9fa5]', '', user_query[:30]).replace(' ', '_')
    filename = os.path.join(OUTPUT_DIR, f"{safe_query}_{chart_type}_chart.png")
    
    try:
        plt.savefig(filename)
        plt.close() 
        return filename
    except Exception as e:
        print(f"❌ 儲存圖表時發生錯誤: {e}")
        plt.close()
        return ""