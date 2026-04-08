import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine, text

# 1. 定義 NQ100 及其產業分類
sector_data = {
    "Semiconductors": ["NVDA", "AMD", "TSM", "AVGO", "TXN", "QCOM", "ADI", "MU", "AMAT", "LRCX", "KLAC", "MRVL", "MCHP", "ASML", "INTC", "ARM"],
    "Software & Cloud": ["MSFT", "GOOGL", "GOOG", "META", "ADBE", "PANW", "SNPS", "CDNS", "INTU", "ORCL", "WDAY", "ROP", "TEAM", "DDOG", "ZS", "CRWD"],
    "Consumer & Retail": ["AMZN", "COST", "PEP", "TMUS", "SBUX", "MDLZ", "BKNG", "MNST", "MELI", "LULU", "MAR", "ORLY", "ROST", "KDP", "DLTR", "EBAY"],
    "Health Care": ["AMGN", "ISRG", "VRTX", "REGN", "GILD", "MDLZ", "IDXX", "BIIB", "DXCM", "MRNA", "ALGN"],
    "Hardware & Others": ["AAPL", "TSLA", "CSCO", "HON", "CMCSA", "ADP", "PYPL", "PDD", "ABNB", "PCAR", "AEP", "EXC", "GEHC", "FANG"]
}

# 轉換成 DataFrame 用於寫入分類表
sector_list = []
all_tickers = []
for sector, tickers in sector_data.items():
    for t in tickers:
        sector_list.append({'ticker': t, 'sector': sector})
        all_tickers.append(t)

df_sectors = pd.DataFrame(sector_list)
# 加入防呆：如果清單有重複的 ticker，只保留第一筆，防止資料庫報錯
df_sectors = df_sectors.drop_duplicates(subset=['ticker'], keep='first')

print(f"🚀 開始下載 NQ100 歷史資料 (共 {len(df_sectors)} 支)...")

# --- 替換掉原本 df_prices 的那段 ---
# 2. 批次下載股價
data = yf.download(df_sectors['ticker'].tolist(), period="1mo")['Close']
df_prices = data.reset_index().melt(id_vars=['Date'], var_name='ticker', value_name='close_price').dropna()
df_prices.columns = ['date', 'ticker', 'close_price']
# 加入防呆：確保同一天、同一檔股票只有一筆報價
df_prices = df_prices.drop_duplicates(subset=['date', 'ticker'])

# 3. 寫入資料庫
engine = create_engine('postgresql://postgres:yourpassword@127.0.0.1:5432/SQL_Practice')

# 注意：這裡只要一個 with 就好，裡面的內容要統一縮排
with engine.begin() as conn:
    # A. 確保正式表 stock_sectors 存在
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS stock_sectors (
            ticker VARCHAR(10) PRIMARY KEY,
            sector VARCHAR(50)
        );
    """))

    # B. 更新產業分類表
    df_sectors.to_sql('temp_sectors', conn, if_exists='replace', index=False)
    conn.execute(text("""
        INSERT INTO stock_sectors (ticker, sector)
        SELECT ticker, sector FROM temp_sectors
        ON CONFLICT (ticker) DO UPDATE SET sector = EXCLUDED.sector;
    """))

    # C. 更新股價表 (UPSERT 邏輯)
    conn.execute(text("CREATE TEMP TABLE temp_stocks (LIKE stock_prices INCLUDING ALL)"))
    df_prices.to_sql('temp_stocks', conn, if_exists='append', index=False)
    conn.execute(text("""
        INSERT INTO stock_prices (date, ticker, close_price)
        SELECT date, ticker, close_price FROM temp_stocks
        ON CONFLICT (ticker, date) DO UPDATE SET close_price = EXCLUDED.close_price;
    """))

print("✅ NQ100 數據與產業分類同步完成！")