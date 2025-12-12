# db_agent.py (最終完整 Schema 修正版)

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
    # 假設 local_chart_generator.py 已經存在且被修正 
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


# --- B. 資料庫綱要 (DB_SCHEMA) (已根據提供文件完整補充) ---
DB_SCHEMA = """
Table: Customers (客戶主表)
  - CustomerID (nchar): 客戶代號
  - CompanyName (nvarchar): 客戶(公司)名稱
  - ContactName (nvarchar): 聯絡人姓名
  - ContactTitle (nvarchar): 聯絡人職稱
  - Address (nvarchar): 地址
  - City (nvarchar): 城市
  - Region (nvarchar): 區域
  - PostalCode (nvarchar): 郵遞區號
  - Country (nvarchar): 國家
  - Phone (nvarchar): 電話
  - Fax (nvarchar): 傳真

Table: Employees (員工資料)
  - EmployeeID (int): 員工代號
  - LastName (nvarchar): 姓氏
  - FirstName (nvarchar): 名字
  - Title (nvarchar): 職稱
  - TitleOfCourtesy (nvarchar): 稱謂
  - BirthDate (datetime): 生日
  - HireDate (datetime): 到職日
  - Address (nvarchar): 地址
  - City (nvarchar): 城市
  - Region (nvarchar): 區域
  - PostalCode (nvarchar): 郵遞區號
  - Country (nvarchar): 國家
  - HomePhone (nvarchar): 住家電話
  - Extension (nvarchar): 分機
  - Photo (varbinary): 照片
  - Notes (nvarchar): 備註
  - ReportsTo (int): 主管員工代號
  - PhotoPath (nvarchar): 照片路徑

Table: Suppliers (供應商資料)
  - SupplierID (int): 供應商代號
  - CompanyName (nvarchar): 公司名稱
  - ContactName (nvarchar): 聯絡人姓名
  - ContactTitle (nvarchar): 聯絡人職稱
  - Address (nvarchar): 地址
  - City (nvarchar): 城市
  - Region (nvarchar): 區域
  - PostalCode (nvarchar): 郵遞區號
  - Country (nvarchar): 國家
  - Phone (nvarchar): 電話
  - Fax (nvarchar): 傳真
  - HomePage (nvarchar): 公司首頁

Table: Categories (產品類別)
  - CategoryID (int): 類別代號
  - CategoryName (nvarchar): 類別名稱
  
Table: Products (產品資料)
  - ProductID (INT): 產品編號
  - ProductName (NVARCHAR): 產品名稱
  - SupplierID (INT): 供應商代號
  - CategoryID (INT): 類別代號
  - QuantityPerUnit (NVARCHAR): 單位描述
  - UnitPrice (MONEY): 單價
  - UnitsInStock (SMALLINT): 庫存數量
  - UnitsOnOrder (SMALLINT): 在途數量
  - ReorderLevel (SMALLINT): 再訂購點
  - Discontinued (BIT): 是否停售

Table: Orders (訂單主表)
  - OrderID (INT): 訂單編號
  - CustomerID (nchar): 客戶代號
  - EmployeeID (int): 負責員工代號
  - OrderDate (DATETIME): 訂單日期
  - RequiredDate (DATETIME): 需求日期
  - ShippedDate (DATETIME): 出貨日期
  - ShipVia (INT): 出貨方式
  - Freight (MONEY): 運費
  - ShipName (NVARCHAR): 送貨客戶名稱
  - ShipAddress (NVARCHAR): 送貨地址
  - ShipCity (NVARCHAR): 送貨城市
  - ShipRegion (NVARCHAR): 送貨區域
  - ShipPostalCode (NVARCHAR): 送貨郵遞區號
  - ShipCountry (NVARCHAR): 送貨國家

Table: "Order Details" (訂單明細表)
  - OrderID (INT): 訂單編號
  - ProductID (INT): 產品編號
  - UnitPrice (MONEY): 產品單價
  - Quantity (SMALLINT): 訂購數量
  - Discount (REAL): 折扣
  -- 實際銷售金額計算公式: (Quantity * UnitPrice * (1 - Discount))

-- 關係提示 (JOIN)
-- 1. Orders <--> "Order Details" 透過 OrderID 連接
-- 2. Products <--> "Order Details" 透過 ProductID 連接
-- 3. Customers <--> Orders 透過 CustomerID 連接
-- 4. Employees <--> Orders 透過 EmployeeID 連接
-- 5. Products <--> Categories 透過 CategoryID 連接
-- 6. Products <--> Suppliers 透過 SupplierID 連接
"""

# --- C. 系統提示 (System Prompt) (強制中文別名) ---
SYSTEM_PROMPT = f"""
你是一位專業的 Text-to-SQL 翻譯專家，專門負責將自然語言查詢轉換為**單一**且**可執行**的 SQL 查詢語句。

[資料庫綱要]
{DB_SCHEMA}

[規則限制]
1. 你的輸出只能包含 SQL 語句,不能包含任何解釋性文字或額外符號 (尤其是前後的```)。
2. 請務必使用雙引號 " 括住含有空格的表名，例如 "Order Details"。
3. **極度重要：** 你的 SQL 查詢中，所有用於 `SELECT` 的欄位，無論是原始欄位還是聚合欄位（如 SUM, COUNT），**都必須**使用 **中文別名 (AS Chinese Name)**。
4. 數字欄位 (金額, 數量等) 請用千分號, 並取整數。 
"""
# --- C. 結束 ---


# --- D. LLM 呼叫與 Token 追蹤 (保持不變) ---
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
# --- D. 結束 ---

data_df = pd.DataFrame()
# --- E. Database Agent 主執行函式 (保持不變) ---
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
    # if "---CHART_SPEC---" not in llm_output:
    #     print(f"❌ LLM 輸出格式錯誤，缺少分隔符。原始輸出:\n{llm_output}")
    #     return f"❌ LLM 輸出格式錯誤，缺少分隔符 '---CHART_SPEC---'。"
        
    # try:
    #     sql_command, chart_json_str = llm_output.split("---CHART_SPEC---", 1)
    #     sql_command = sql_command.strip()
    #     chart_json_str = chart_json_str.strip()

    #     # 修正 1: 移除 SQL 語句的 Markdown 程式碼標籤 (```sql)
    #     if sql_command.startswith("```sql"):
    #         sql_command = sql_command.replace("```sql", "", 1)
    #     if sql_command.endswith("```"):
    #         sql_command = sql_command[:-3]
    #     sql_command = sql_command.strip()

    #     # 修正 2: 移除 JSON 語句的 Markdown 程式碼標籤 (```json)
    #     if chart_json_str.startswith("```json"):
    #         chart_json_str = chart_json_str.replace("```json", "", 1)
    #     if chart_json_str.endswith("```"):
    #         chart_json_str = chart_json_str[:-3]
        
    #     # 最終嘗試解析 JSON
    #     chart_spec = json.loads(chart_json_str.strip())
        
    # except Exception as e:
    #     print(f"❌ 解析 LLM 輸出時發生錯誤：{e}")
    #     print(f"🔍 嘗試解析的 JSON 字串:\n{chart_json_str}")
    #     return f"❌ 解析 LLM 輸出時發生錯誤，LLM 輸出可能不是合法的 JSON/SQL 格式。"

    sql_command = llm_output

  # 修正 1: 移除 SQL 語句的 Markdown 程式碼標籤 (```sql)
    if sql_command.startswith("```sql"):
        sql_command = sql_command.replace("```sql", "", 1)
    if sql_command.endswith("```"):
        sql_command = sql_command[:-3]
    sql_command = sql_command.strip()


    # chart_spec = {'chart_type': 'bar'}  # 預設為 table，避免後續錯誤
    print(f"🤖 LLM 生成 SQL：\n{sql_command}")
    
    # 2. 執行 SQL
    try:

        global data_df 
        data_df = execute_sql(sql_command)
        
        if data_df.empty:
            return "✅ 查詢成功，但未找到任何符合條件的數據。"
            
        # 3. 生成本地圖表圖片路徑
        # local_chart_path = ""
        # chart_type = chart_spec.get('chart_type', '').lower()
        # if chart_type != 'table':
        #     local_chart_path = generate_local_chart(data_df, chart_spec, user_query)
        
        # 4. 格式化表格 (使用 'fancy_grid' 讓終端機輸出最美觀)
        data_table_string = tabulate(
            data_df, 
            headers='keys', 
            tablefmt='fancy_grid',
            showindex=False,
            disable_numparse=True 
        )

        # 5. 準備最終回覆
        final_response = {
            "summary": f"查詢 '{user_query}' 成功，共獲取 {len(data_df)} 筆數據。",
            "data_table_string": data_table_string,                           
            # "chart_path": local_chart_path,                           
            # "chart_type": chart_spec.get('chart_type'),
            "token_info": token_info                                  
        }
        
        # print(f"✅ 本地圖表路徑：{local_chart_path}")
        return json.dumps(final_response, indent=2, ensure_ascii=False)
    
    except ValueError as e:
        print(f"❌ 資料庫執行錯誤：{e}")
        return f"❌ 資料庫執行錯誤：{e}"
    except Exception as e:
        print(f"❌ 發生未知錯誤：{e}")
        return f"❌ 發生未知錯誤：{e}"

# --- 測試區塊 ---
if __name__ == "__main__":
    
    # # 測試 1：長條圖 (測試中文別名、數值標籤和 ha 錯誤修正)
    # test_query_bar = "列出所有歷史訂單中，銷售量最高的五個產品名稱及其總銷售數量。"
    # print("\n\n--- 執行修正後的測試 1：長條圖 (Bar Chart) ---")
    # final_json_output_bar = run_database_query(test_query_bar)
    # print("\n--- Database Agent 最終 JSON 輸出 ---")
    # print(final_json_output_bar)
    
    # # 單獨列印表格，以驗證 'fancy_grid' 的視覺效果
    # print("\n[✅ 格式化文字表格 (data_table_string)]")
    # try:
    #     result = json.loads(final_json_output_bar)
    #     print(result.get('data_table_string'))
    #     print(f"💡 圖表已儲存於: {result['chart_path']}")
    # except Exception:
    #     pass
        
    # 測試 2：折線圖 (測試中文亂碼修正)
    #test_query_line = "計算 1997 年每月訂單的總銷售金額，並列出月份和總金額，生成折線圖。"
    test_query_line = "計算 1997 年每月訂單的總銷售金額，並列出月份和總金額。"
    print("\n\n--- 執行修正後的測試 2：折線圖 (Line Chart) ---")
    final_json_output_line = run_database_query(test_query_line)
    print("\n--- Database Agent 最終 JSON 輸出 ---")
    print(final_json_output_line)
    
    # 單獨列印表格
    print("\n[✅ 格式化文字表格 (data_table_string)]")
    try:
        result = json.loads(final_json_output_line)
        print(result.get('data_table_string'))
        #print(f"💡 圖表已儲存於: {result['chart_path']}")
    except Exception:
        pass


    generate_local_chart(data_df, {'chart_type':'bar'}, "測試標題")