import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine, text

# 1. 手動定義 NQ-100 清單 (這裡已經幫你整理好最核心的 100 支)
nq100_list = [
    "NVDA", "AMD", "TSM", "MU", "ASML", "ARM", "TSLA", "AMZN", "GOOG", "AAPL",
    "MSFT", "AVGO", "MRVL", "QCOM", "PLTR", "CRWD", "NET", "SNOW", "NOW", "NFLX",
    "APP", "OKLO", "SMR", "INTC", "LEU", "AVAV", "MP", "ALAB", "RKLB",
    "NBIS", "STX", "SNDK", "SMCI", "ONDS", "ORCL", "LITE",
    "COHR", "CRDO", "VST", "CIEN", "DELL" 
]

print(f"🚀 開始下載 {len(nq100_list)} 支股票的最近 30 天資料...")

# 2. 批次下載
data = yf.download(nq100_list, period="1mo")['Close']

# 3. 資料清洗 (寬轉長)
df_sql = data.reset_index().melt(id_vars=['Date'], var_name='ticker', value_name='close_price')
df_sql = df_sql.dropna()
df_sql.columns = ['date', 'ticker', 'close_price']

# 4. 寫入資料庫 (使用 UPSERT 語法防止重複)
engine = create_engine('postgresql://postgres:yourpassword@127.0.0.1:5432/SQL_Practice')

with engine.begin() as conn:
    # 建立臨時表
    conn.execute(text("CREATE TEMP TABLE temp_stocks (LIKE stock_prices INCLUDING ALL)"))
    df_sql.to_sql('temp_stocks', conn, if_exists='append', index=False)
    
    # 寫入主表，若重複則更新
    upsert_query = """
    INSERT INTO stock_prices (date, ticker, close_price)
    SELECT date, ticker, close_price FROM temp_stocks
    ON CONFLICT (ticker, date) DO UPDATE SET close_price = EXCLUDED.close_price;
    """
    conn.execute(text(upsert_query))
    print(f"✅ 成功同步 {len(df_sql)} 筆資料至 PostgreSQL！")