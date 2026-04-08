import yfinance as yf

# 測試抓取你關注的半導體股
ticker = "MU"  # 美光
data = yf.Ticker(ticker)

# 顯示最新的股價資訊
print(f"股票名稱: {data.info['longName']}")
print(f"目前市價: {data.fast_info['last_price']}")
print(f"本益比 (PE): {data.info.get('trailingPE')}")
