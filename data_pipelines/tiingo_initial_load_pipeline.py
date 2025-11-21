import sys
import pandas as pd
from pathlib import Path
import logging

# Get the path to your secrets directory
project_root = Path(__file__).parents[1]
sys.path.insert(0, str(project_root))

# must come after path is set
from util.to_postgres import PgHook
from util.tiingo_manager import TiingoDataManager

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('tiingo_load.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# instantiate Postgres database utility
db = PgHook()

# Instantiate Tiingo class
tdm = TiingoDataManager()

# Step 1: Get all tickers (including dead ones)
print("Fetching ticker universe...")
tiingo_df = tdm.get_all_tickers(include_delisted=True, enrich_names=True)

# Step 2: Get company names
enriched_ticker_df = tdm.enrich_company_names(tiingo_df['ticker'].to_list())

# Step 3: Merge company names into tiingo_df to make symbols_df
symbols_df = tdm.merge_names(tiingo_df, enriched_ticker_df)

# Step 4: Supplement any missing company names with database table
all_symbols_df = db.psy_query('select * from all_symbols;')
symbols_df = tdm.merge_names(symbols_df, all_symbols_df)

# Step 5: Remove rows with null company names before database insert
before_count = len(symbols_df)
symbols_df = symbols_df[symbols_df['company_name'].notna()]
after_count = len(symbols_df)
logger.info(f"Removed {before_count - after_count} symbols with null company names")

# Step 6: Save symbol data to table
print("Saving symbols...")
failed_symbols = tdm.upsert_symbols(symbols_df)
logger.info(f"Failed symbols inserted: {len(failed_symbols)}")

# Step 7: Get stocks from symbols df
proto_stock_df = symbols_df[symbols_df['asset_type'] == 'stock'].copy()

# Step 8: Drop symbol specific fields
symbol_cols = ['asset_type', 'is_etf', 'start_date', 'end_date']
proto_stock_df = proto_stock_df.drop(columns=[col for col in symbol_cols if col in proto_stock_df.columns])   

# Step 9: Get industry & sector from yFinance
yf_enriched_df = tdm.yfinance_metadata(proto_stock_df['ticker'].tolist())

# merge yFinance data into stocks_df
stocks_df = proto_stock_df.merge(
    yf_enriched_df[['ticker', 'industry', 'sector']],
    on='ticker',
    how='left'
)

# I need help getting this into Pandas
"""select s.ticker, s.company_name, y.industry, y.sector, s.exchange
from stocks_df s
left join yf_enriched_df y
on s.ticker = y.ticker

result stocks_df
"""

# Step 10: Get industry & sector from FMP for any remaining null or empty records
missing_mask = stocks_df['industry'].isna() | (stocks_df['industry'] == '')
tickers_needing_fmp = stocks_df.loc[missing_mask, 'ticker'].tolist()

if tickers_needing_fmp:
    logger.info(f"Fetching FMP data for {len(tickers_needing_fmp)} tickers missing industry & sector")
    fmp_enriched_df = tdm.fetch_industry_sector(tickers_needing_fmp)

    stocks_df = stocks_df.merge(
        fmp_enriched_df[['ticker', 'industry', 'sector']],
        on='ticker',
        how='left',
        suffixes=('', '_fmp')
    )

    stocks_df['industry'] = stocks_df['industry'].fillna(stocks_df['industry_fmp'])
    stocks_df['sector'] = stocks_df['sector'].fillna(stocks_df['sectory_fmp'])

    stocks_df = stocks_df.drop(colums=['industry_fmp', 'sector_fmp'], errors='ignore')

# After all merges, set explicit column order (like SQL SELECT)
stocks_df = stocks_df[['ticker', 'company_name', 'industry', 'sector', 'exchange']]

# Step 11: Save stocks data to table
failed_stocks = tdm.upsert_stocks(stocks_df)

# Step 12: Purge sketchy stock records
symbol_null = """
DELETE FROM symbols 
WHERE ticker IN (
    SELECT ticker FROM stocks 
    WHERE sector IS NULL OR industry IS NULL
);
"""

stock_null = """
DELETE FROM stocks 
WHERE sector IS NULL OR industry IS NULL;
"""

symbol_blank = """
DELETE FROM symbols 
WHERE ticker IN (
    SELECT ticker FROM stocks 
    WHERE sector = '' OR industry = ''
);
"""

stock_blank = """
DELETE FROM stocks 
WHERE sector = '' OR industry = '';
"""

db.execute_sql(symbol_null)
db.execute_sql(stock_null)
db.execute_sql(symbol_blank)
db.execute_sql(stock_blank)

# Step 13: Download price data for symbols tickers
print("Downloading price data...")
ticker_list = symbols_df['ticker'].tolist() 
failed_prices = tdm.download_price_data(ticker_list)

# Step 14: Validate and move price data to ohlcv table
print("Validating and moving to production...")
tdm.validate_and_move_staging()
