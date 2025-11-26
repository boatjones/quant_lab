#!/usr/bin/env python3
"""
Tiingo Maintenance Pipeline
Daily/Weekly maintenance updates for symbols, stocks, and price data

Usage:
    python tiingo_maintenance_pipeline.py [--mode daily|weekly] [--backfill-days N]
"""

import sys
import pandas as pd
from pathlib import Path
import logging
import argparse
from datetime import date

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

####################################################################################
#  Begin Process
####################################################################################

# instantiate Postgres database utility
db = PgHook()

# Instantiate Tiingo class
tdm = TiingoDataManager()

# A --- Symbols table

# Get all tickers from Tiingo
print("Fetching ticker universe...")
tiingo_df = tdm.get_all_tickers(include_delisted=False, filter_junk=True)

# Get current tickers and active status
cur_symb_df = db.psy_query("select ticker, is_active as is_active_old from symbols")

# Merge in current active flag to existing records just downloaded to find changes
existing_df = tiingo_df.merge(
    cur_symb_df[['ticker', 'is_active_old']],
    on='ticker',
    how='inner'
)

# Update newly inactive tickers and put in end date  
newly_inactive_mask = (existing_df['is_active'] == 0) & (existing_df['is_active_old'] == 1)
existing_df.loc[newly_inactive_mask, 'end_date'] = date.today()

# Get dataframe of newly inactive tickers
updates_df = existing_df.loc[
    existing_df['end_date'].notna(),
    ['ticker', 'is_active', 'end_date']
]
print(f"Newly inactive tickers to update: {len(updates_df)}")

if len(updates_df) > 0:
    # update symbols table in database
    for _, row in updates_df.iterrows():
        sql = """
            UPDATE symbols 
            SET is_active = %s, end_date = %s
            WHERE ticker = %s
        """
        db.execute_sql(sql, (row['is_active'], row['end_date'], row['ticker']))

    logger.info(f"Updated {len(updates_df)} symbols with inactive status")

else:
    logger.info("No updates on 'is_active' to symbols.")

# Find new tickers (exclude known junk)
excluded_df = db.psy_query("select ticker from excluded_tickers")
new_tickers_df = tiingo_df[
    ~tiingo_df['ticker'].isin(cur_symb_df['ticker']) &
    ~tiingo_df['ticker'].isin(excluded_df['ticker'])
]
print(f"New tickers to add: {len(new_tickers_df)}")

# Supplement any missing company names with database table
all_symbols_df = db.psy_query('select ticker, company_name from all_symbols;')
new_tickers_df = tdm.merge_names(new_tickers_df, all_symbols_df)

# Only enrich tickers that STILL have no name
needs_enrichment = new_tickers_df[new_tickers_df['company_name'].isna()]['ticker'].tolist()
print(f"Tickers needing API enrichment: {len(needs_enrichment)}")

# Get company names for new tickers
enriched_ticker_df = tdm.enrich_company_names(new_tickers_df['ticker'].to_list())

# Merge company names into tiingo_df to make new_symbols_df
new_tickers_df = tdm.merge_names(new_tickers_df, enriched_ticker_df)

# Fix mislabeled ETFs - check company name only
etf_mask = (
    (new_tickers_df['asset_type'] == 'stock') &
    new_tickers_df['company_name'].notna() &
    (
        new_tickers_df['company_name'].str.contains(' ETF ', case=False, na=False) |
        new_tickers_df['company_name'].str.lower().str.endswith('etf')
    )
)

new_tickers_df.loc[etf_mask, 'asset_type'] = 'etf'
new_tickers_df.loc[etf_mask, 'is_etf'] = 1

logger.info(f"Corrected {etf_mask.sum()} ETFs mislabeled as stocks")

# After all enrichment attempts...
new_symbols_df = new_tickers_df[
    new_tickers_df['company_name'].notna() & 
    (new_tickers_df['company_name'] != '') &
    (new_tickers_df['company_name'].str.lower() != 'null')
]

# Save junk tickers so we skip them next time
junk_tickers = new_tickers_df[
    new_tickers_df['company_name'].isna() | 
    (new_tickers_df['company_name'] == '') |
    (new_tickers_df['company_name'].str.lower() == 'null')
]['ticker'].tolist()

if junk_tickers:
    for ticker in junk_tickers:
        sql = """
            INSERT INTO excluded_tickers (ticker, reason)
            VALUES (%s, %s)
            ON CONFLICT (ticker) DO NOTHING
        """
        db.execute_sql(sql, (ticker, 'no_company_name'))
    logger.info(f"Added {len(junk_tickers)} to excluded_tickers")

# Insert new symbol records to table
print("Saving symbols...")
failed_symbols = tdm.upsert_symbols(new_symbols_df)
logger.info(f"Failed symbols inserted: {len(failed_symbols)}")

# B --- Stock table
# Get current stock records
cur_stock_df = db.psy_query("select * from stocks")

# Get stocks out of refreshed symbols table
new_stock_df = db.psy_query("select ticker, company_name, exchange from symbols where asset_type = 'stock'")

# Find new stock records that are not in current stock records
addl_stocks_df = new_stock_df[~new_stock_df['ticker'].isin(cur_stock_df['ticker'])]

if len(addl_stocks_df) == 0:
    logger.info("No new stocks to add")
else:
    # Get industry & sector from yFinance for new stock records
    yf_enriched_df = tdm.yfinance_metadata(addl_stocks_df['ticker'].tolist())

    # merge yFinance data into addl_stocks_df
    addl_stocks_df = addl_stocks_df.merge(
        yf_enriched_df[['ticker', 'industry', 'sector']],
        on='ticker',
        how='left'
    )

    # Get industry & sector from FMP for any remaining null or empty records
    missing_mask = addl_stocks_df['industry'].isna() | (addl_stocks_df['industry'] == '')
    tickers_needing_fmp = addl_stocks_df.loc[missing_mask, 'ticker'].tolist()

    if tickers_needing_fmp:
        logger.info(f"Fetching FMP data for {len(tickers_needing_fmp)} tickers missing industry & sector")
        fmp_enriched_df = tdm.fetch_industry_sector(tickers_needing_fmp)

        addl_stocks_df = addl_stocks_df.merge(
            fmp_enriched_df[['ticker', 'industry', 'sector']],
            on='ticker',
            how='left',
            suffixes=('', '_fmp')
        )

        addl_stocks_df['industry'] = addl_stocks_df['industry'].fillna(addl_stocks_df['industry_fmp'])
        addl_stocks_df['sector'] = addl_stocks_df['sector'].fillna(addl_stocks_df['sector_fmp'])

        addl_stocks_df = addl_stocks_df.drop(columns=['industry_fmp', 'sector_fmp'], errors='ignore')

    # Set field order
    stocks_df = addl_stocks_df[['ticker', 'company_name', 'industry', 'sector', 'exchange']]

    # Save junk stocks missing industry data so we skip them next time
    junk_stocks = stocks_df[
        stocks_df['industry'].isna() | 
        (stocks_df['industry'] == '')
    ]['ticker'].tolist()

    if junk_stocks:
        for ticker in junk_stocks:
            sql = """
                INSERT INTO excluded_tickers (ticker, reason)
                VALUES (%s, %s)
                ON CONFLICT (ticker) DO NOTHING
            """
            db.execute_sql(sql, (ticker, 'no_industry_data'))
        logger.info(f"Added {len(junk_stocks)} to excluded_tickers")

    # After all enrichment attempts...
    clean_stocks_df = stocks_df[
        stocks_df['industry'].notna() & 
        (stocks_df['industry'] != '')
    ]

    # Save new stocks data to table
    failed_stocks = tdm.upsert_stocks(clean_stocks_df)

# C --- Prices data
# Get max trade date in ohlcv table
max_trade_date = db.psy_query("select (max(trade_date)+1) as start_date from ohlcv")['start_date'].iloc[0]

# Get current symbols - only select active tickers.  Additional fields for potential diagnosis.
# indices are omitted because Tiingo does not have price data for them
symbols_df = db.psy_query("select ticker, company_name, asset_type from symbols where is_active = 1 and asset_type != 'index'")

# Get pricing data
print("Downloading price data...")
ticker_list = symbols_df['ticker'].tolist() 
failed_prices = tdm.download_price_data(ticker_list, max_trade_date)

# Validate and move price data to ohlcv table
print("Validating and moving to production...")
tdm.validate_and_move_staging()

# D --- Mark stale tickers as inactive
print("Checking for stale tickers...")

# First count how many will be affected
count_query = """
    SELECT COUNT(*) as cnt
    FROM symbols s
    WHERE s.is_active = 1
      AND s.asset_type != 'index'
      AND s.date_loaded < CURRENT_DATE - INTERVAL '30 days'
      AND s.ticker NOT IN (
          SELECT DISTINCT ticker 
          FROM ohlcv 
          WHERE trade_date >= CURRENT_DATE - INTERVAL '30 days'
      )
"""
stale_count = db.psy_query(count_query)['cnt'].iloc[0]
logger.info(f"Found {stale_count} stale tickers (no prices in 30 days)")

if stale_count > 0:
    # Now run the update
    update_sql = """
        UPDATE symbols s
        SET 
            is_active = 0,
            end_date = COALESCE(
                (SELECT MAX(trade_date) FROM ohlcv WHERE ticker = s.ticker),
                CURRENT_DATE
            )
        WHERE s.is_active = 1
          AND s.asset_type != 'index'
          AND s.date_loaded < CURRENT_DATE - INTERVAL '30 days'
          AND s.ticker NOT IN (
              SELECT DISTINCT ticker 
              FROM ohlcv 
              WHERE trade_date >= CURRENT_DATE - INTERVAL '30 days'
          )
    """
    db.execute_sql(update_sql)
    logger.info(f"Marked {stale_count} tickers as inactive")
else:
    logger.info("No stale tickers to mark inactive")

