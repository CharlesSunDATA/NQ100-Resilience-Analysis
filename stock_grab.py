import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine

# 1. 準備 NQ-100 股票代碼 (字串形式，用空格隔開)
# 這裡列出前幾大權值股作為示範，你可以直接把 100 支的代碼貼在裡面
nq100_tickers = "AAPL MSFT NVDA AMZN META TSLA AVGO GOOGL GOOG COST PEP LIN CSCO TMUS INTU QCOM AMGN TXN HON CMCSA AMAT ISRG SBUX BKNG GILD VRTX MDLZ ADI ADP REGN LRCX PANW SNPS CDNS KLAC MELI MU CSX PYPL CRWD MAR CTAS NXPI ORLY MNST ABNB PCAR LULU WDAY KDP CHTR MRVL ROST PAYX MCHP IDXX AEP CPRT KHC BIIB EXC DEXM FAST CTSH VRSK EA BKR CEG GEHC CSGP ON ODFL DDOG TEAM FANG ANSS ZS TTD MDB WBD ILMN SIRI WBA DXCM DLTR EBAY GFS ZM PDD CRWD DASH ROKU MRNA ENPH ALGN LCID"

print("🚀 正在批次下載 NQ-100 歷史收盤價...")

# 2. 一次性下載所有股票的資料 (設定抓取過去 1 個月)
# 我們只取出 ['Close'] (收盤價) 這個欄位，會得到一個很寬的表格
data = yf.download(nq100_tickers, period="1mo")['Close']

# 3. 資料清洗：把「寬表格」轉換成資料庫要的「長表格」(Melt)
# 這是資料工程 (Data Engineering) 非常經典的一步！
df_sql = data.reset_index().melt(id_vars=['Date'], var_name='ticker', value_name='close_price')

# 清除沒有交易資料的空值 (例如假日)
df_sql = df_sql.dropna()

# 把欄位名稱改成全小寫，配合 PostgreSQL 習慣
df_sql.columns = ['date', 'ticker', 'close_price']

print(f"✅ 資料處理完成，總共取得 {len(df_sql)} 筆紀錄！")

# 4. 存入你的 PostgreSQL 資料庫
# (請替換成你實際的資料庫連線密碼與設定)
engine = create_engine('postgresql://postgres:yourpassword@127.0.0.1:5432/SQL_Practice')
df_sql.to_sql('stock_prices', engine, if_exists='append', index=False)