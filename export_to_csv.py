import pandas as pd
from sqlalchemy import create_engine

print("⏳ 正在連接 PostgreSQL 資料庫，準備匯出資料...")

# 1. 建立資料庫連線
engine = create_engine('postgresql://postgres:yourpassword@127.0.0.1:5432/SQL_Practice')

# 2. 撰寫 SQL 語法：將「健康指標」與「產業分類」合併抽出
query_metrics = """
    SELECT 
        h.ticker,
        h.avg_drawdown,
        h.max_drawdown,
        h.downside_risk,
        h.ulcer_index,
        h.trading_days,
        s.sector
    FROM stock_health_check h
    LEFT JOIN stock_sectors s ON h.ticker = s.ticker;
"""

# 3. 撰寫 SQL 語法：將「每日股價」與「產業分類」合併抽出 (用於畫走勢圖)
query_prices = """
    SELECT 
        p.date,
        p.ticker,
        p.close_price,
        s.sector
    FROM stock_prices p
    LEFT JOIN stock_sectors s ON p.ticker = s.ticker
    ORDER BY p.date DESC;
"""

try:
    # 4. 使用 Pandas 讀取 SQL 並轉成 DataFrame
    print("📊 正在抽取「抗跌指標」數據...")
    df_metrics = pd.read_sql(query_metrics, engine)
    
    print("📈 正在抽取「每日股價」數據...")
    df_prices = pd.read_sql(query_prices, engine)

    # 5. 匯出成 CSV 檔案 (會存在你 VS Code 當前的資料夾裡)
    df_metrics.to_csv('nq100_health_metrics.csv', index=False, encoding='utf-8-sig')
    df_prices.to_csv('nq100_daily_prices.csv', index=False, encoding='utf-8-sig')

    print("✅ 大功告成！已成功匯出兩個檔案：")
    print("  👉 1. nq100_health_metrics.csv (用於畫象限圖、痛苦指數)")
    print("  👉 2. nq100_daily_prices.csv (用於畫歷史走勢圖)")

except Exception as e:
    print(f"❌ 匯出失敗，請檢查錯誤：{e}")