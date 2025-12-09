# db_agent_combined.py (最終穩定版)

import openai
import os
import json
import sys
import pyodbc 
import pandas as pd
from tabulate import tabulate 
from dotenv import load_dotenv

# ----------------------------------------------------------------------
# 匯入必要的模組
try:
    # 假設 local_chart_generator.py 已經存在且被修正 (已移除字體錯誤)
    from local_chart_generator import generate_local_chart 
except ImportError as e:
    print(f"❌ 錯誤：找不到必要的模組。請確認 local_chart_generator.py 存在。錯誤: {e}")
    sys.exit(1)
# ----------------------------------------------------------------------

# 載入環境變數並設定 OpenAI Key
load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")
MODEL = "gpt-4o-mini" # 使用 gpt-4o-mini 以提升格式穩定性與成本效益

# --- A. 資料庫連線配置 ---
SERVER = os.getenv("DB_SERVER")
DATABASE = os.getenv("DB_NAME")
DRIVER = os.getenv("DB_DRIVER")

CONNECTION_STRING = (
    f"Driver={DRIVER};"
    f"Server={SERVER};"
    f"Database={DATABASE};"
    "Trusted_Connection=yes;"
)

def execute_sql(sql_query: str) -> pd.DataFrame:
    """連線到 SQL Server，執行查詢，並返回 Pandas DataFrame。"""
    conn = None
    try:
        conn = pyodbc.connect(CONNECTION_STRING)
        df = pd.read_sql(sql_query, conn)
        return df
    except pyodbc.Error as ex:
        error_msg = f"SQL 執行錯誤: {ex.args[0]}"
        print(error_msg)
        raise ValueError(error_msg) 
    finally:
        if conn:
            conn.close()
# --- A. 結束 ---


# --- B. 系統提示 (System Prompt) ---
DB_SCHEMA = """
Table: Products (產品資料)
  - ProductID (INT): 產品唯一編號
  - ProductName (NVARCHAR): 產品名稱
  - UnitPrice (MONEY): 產品單價
  - UnitsInStock (SMALLINT): 庫存量

Table: Orders (訂單主表)
  - OrderID (INT): 訂單唯一編號
  - CustomerID (NCHAR): 客戶編號
  - OrderDate (DATETIME): 訂單日期

Table: "Order Details" (訂單明細表)
  - OrderID (INT): 關聯 Orders 表
  - ProductID (INT): 關聯 Products 表
  - Quantity (SMALLINT): 購買數量
  - UnitPrice (MONEY): 銷售時的單價
  -- 實際銷售金額 = Quantity * UnitPrice * (1 - Discount)

-- 關係提示 (JOIN)
-- 1. Orders <--> "Order Details" 透過 OrderID 連接
-- 2. Products <--> "Order Details" 透過 ProductID 連接
"""

SYSTEM_PROMPT = f"""
你是一位專業的 T-SQL 翻譯專家，專門負責將自然語言查詢轉換為**單一**且**可執行**的 T-SQL 查詢語句，並提供適合的圖表規格。
你的目標是提供準確的 SQL 語句，並讓數據能被完美視覺化。

[資料庫綱要]
{DB_SCHEMA}

[規則限制]
1. 你的輸出**只能包含 SQL 語句和圖表 JSON 規格**，**不能包含任何解釋性文字或額外符號**。
2. 你必須使用 T-SQL 語法，並且 **請務必使用雙引號 " 括住含有空格的表名，例如 "Order Details"**。
3. **重要：** 圖表 JSON 中指定的 `x_axis` 和 `y_axis` **必須**是你在 SQL 語句中 `SELECT` 的欄位名稱。

[最終輸出格式]
你的輸出必須包含兩部分，**用 '---CHART_SPEC---' 分隔**：
第一部分是 **T-SQL 語句** (只能有 SQL，且前後不能有任何換行或空格)。
第二部分是 **圖表 JSON 規格** (**必須是合法的單行 JSON 格式**，且前後不能有任何換行或空格)。

圖表 JSON 格式必須為:
{{
  "chart_type": "<Bar|Line|Pie|Table>", 
  "x_axis": "<SQL結果的欄位名>",      
  "y_axis": "<SQL結果的欄位名>",      
  "title": "<圖表標題>"             
}}
"""
# --- B. 結束 ---


# --- C. LLM 呼叫與 Token 追蹤 ---
def get_sql_from_query(user_query: str) -> tuple[str, dict]:
    """
    呼叫 LLM 服務，將使用者查詢轉換為 SQL 語句和圖表 JSON，
    並追蹤 Token 使用量。
    """
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_query}
    ]
    
    try:
        response = openai.chat.completions.create(
            model=MODEL,
            messages=messages,
            temperature=0
        )
        sql_output = response.choices[0].message.content.strip()
        
        # --- 追蹤 Token 使用量 ---
        usage = response.usage
        token_info = {
            "model": MODEL,
            "prompt_tokens": usage.prompt_tokens,
            "completion_tokens": usage.completion_tokens,
            "total_tokens": usage.total_tokens
        }
        
        return sql_output, token_info
    
    except Exception as e:
        return f"LLM 呼叫失敗: {e}", {"model": MODEL, "error": str(e)}
# --- C. 結束 ---


# --- D. Database Agent 主執行函式 ---
def run_database_query(user_query: str) -> str:
    """
    Database Agent 的主要執行函式。
    """
    print(f"\n==================================================")
    print(f"[Database Agent] 接收到查詢：{user_query}")
    print(f"==================================================")
    
    # 1. LLM 轉換 Text-to-SQL 並獲取 Token 資訊
    llm_output, token_info = get_sql_from_query(user_query)
    
    # 檢查 LLM 是否呼叫失敗
    if isinstance(llm_output, str) and "LLM 呼叫失敗" in llm_output:
        print(f"❌ LLM 錯誤：{llm_output}")
        return json.dumps({"error": llm_output, "token_info": token_info}, indent=2, ensure_ascii=False)
    
    # 打印 Token 資訊
    print("🧠 LLM 執行資訊：")
    print(f"  Model: {token_info['model']}")
    print(f"  Total Tokens: {token_info['total_tokens']}")
    
    # --- 解析 LLM 輸出 ---
    if "---CHART_SPEC---" not in llm_output:
        print(f"❌ LLM 輸出格式錯誤，缺少分隔符。原始輸出:\n{llm_output}")
        return f"❌ LLM 輸出格式錯誤，缺少分隔符 '---CHART_SPEC---'。"
        
    try:
        sql_command, chart_json_str = llm_output.split("---CHART_SPEC---", 1)
        sql_command = sql_command.strip()
        chart_json_str = chart_json_str.strip()

        # 修正 1: 移除 SQL 語句的 Markdown 程式碼標籤 (```sql)
        if sql_command.startswith("```sql"):
            sql_command = sql_command.replace("```sql", "", 1)
        if sql_command.endswith("```"):
            sql_command = sql_command[:-3]
        sql_command = sql_command.strip()

        # 修正 2: 移除 JSON 語句的 Markdown 程式碼標籤 (```json)
        if chart_json_str.startswith("```json"):
            chart_json_str = chart_json_str.replace("```json", "", 1)
        if chart_json_str.endswith("```"):
            chart_json_str = chart_json_str[:-3]
        
        # 最終嘗試解析 JSON
        chart_spec = json.loads(chart_json_str.strip())
        
    except Exception as e:
        print(f"❌ 解析 LLM 輸出時發生錯誤：{e}")
        print(f"🔍 嘗試解析的 JSON 字串:\n{chart_json_str}")
        return f"❌ 解析 LLM 輸出時發生錯誤，LLM 輸出可能不是合法的 JSON/SQL 格式。"

    print(f"🤖 LLM 生成 SQL：\n{sql_command}")
    
    # 2. 執行 SQL
    try:
        data_df = execute_sql(sql_command)
        
        if data_df.empty:
            return "✅ 查詢成功，但未找到任何符合條件的數據。"
            
        # 3. 生成本地圖表圖片路徑
        local_chart_path = ""
        chart_type = chart_spec.get('chart_type', '').lower()
        if chart_type != 'table':
            local_chart_path = generate_local_chart(data_df, chart_spec, user_query)
        
        # 4. 格式化表格 (修正點：使用 'fancy_grid' 讓終端機輸出更美觀)
        data_table_string = tabulate(
            data_df, 
            headers='keys', 
            tablefmt='fancy_grid', # ⬅️ 最終修正：使用 'fancy_grid'
            showindex=False,
            disable_numparse=True 
        )

        # 5. 準備最終回覆
        final_response = {
            "summary": f"查詢 '{user_query}' 成功，共獲取 {len(data_df)} 筆數據。",
            "data_table_string": data_table_string,                           
            "chart_path": local_chart_path,                           
            "chart_type": chart_spec.get('chart_type'),
            "token_info": token_info                                  
        }
        
        print(f"✅ 本地圖表路徑：{local_chart_path}")
        return json.dumps(final_response, indent=2, ensure_ascii=False)
    
    except ValueError as e:
        print(f"❌ 資料庫執行錯誤：{e}")
        return f"❌ 資料庫執行錯誤：{e}"
    except Exception as e:
        print(f"❌ 發生未知錯誤：{e}")
        return f"❌ 發生未知錯誤：{e}"

# --- 測試區塊 ---
if __name__ == "__main__":
    
    # 修正後的測試查詢，避免日期過濾問題
    test_query_fixed = "列出所有歷史訂單中，銷售量最高的五個產品名稱及其總銷售數量。"
    
    print("\n\n--- 執行修正後的測試 ---")
    
    final_json_output = run_database_query(test_query_fixed)
    
    print("\n--- Database Agent 最終 JSON 輸出 ---")
    print(final_json_output)
    
    # 單獨列印表格，以驗證 'fancy_grid' 的視覺效果
    print("\n[✅ 格式化文字表格 (data_table_string)]")
    try:
        result = json.loads(final_json_output)
        print(result.get('data_table_string'))
        print(f"\n💡 LLM Model: {result['token_info']['model']}, Total Tokens: {result['token_info']['total_tokens']}")
        print(f"💡 圖表已儲存於: {result['chart_path']}")
        
    except Exception:
        pass