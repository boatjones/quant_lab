from dotenv import load_dotenv
import sys
from os import getenv
import pandas as pd
from tiingo import TiingoClient
import time
from pathlib import Path
import requests
import yfinance as yf
import logging
from typing import List, Dict

# Get the path to your secrets directory
project_root = Path(__file__).parents[1]
sys.path.insert(0, str(project_root))

# must come after path is set
from util.to_postgres import PgHook
from util.rate_limiter import RateLimiter

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

class TiingoDataManager:
    def __init__(self):
        env_path = project_root / 'secrets' / '.env'
        load_dotenv(env_path)

        self.tiingo_key = getenv("TIINGO_API")
        self.fmp_key = getenv("FMP_API")
        self.client = TiingoClient({'api_key': self.tiingo_key})   
        self.db = PgHook()

        # Add rate limiters
        self.tiingo_limiter = RateLimiter(calls_per_minute=50)
        self.fmp_limiter = RateLimiter(calls_per_minute=30)

        logger.info("Initialize TiingoDataManager with rate limiting")

    @staticmethod
    def is_common_stock(ticker, asset_type=None):
        """
        Helper method: Return True only for common stock tickers.
        If asset_type is provided and is 'etf', always return True (don't filter ETFs)
        """
        ticker_upper = ticker.upper()

        # Even for ETFs/indices, reject obvious junk patterns
        # (Tiingo sometimes mislabels preferred shares, warrants as ETFs)
        obvious_junk_suffixes = ['-P', '-W', '-U', '-R', '-WT', '-UN']
        if any(ticker_upper.endswith(suffix) for suffix in obvious_junk_suffixes):
            return False
        
        # Don't filter ETFs or indices - they have clean tickers
        if asset_type and asset_type.lower() in ['etf', 'index', 'fund']:
            return True
        
        # Exclude mutual funds entirely
        if asset_type and asset_type.lower() == 'mutual fund':
            return False
        
        # Explicit list of known junk tickers (4-char units/rights/warrants)
        known_junk = {
            'WPAU', 'VITU', 'TTOU', 'TSYU', 'TBAU', 'RMGU', 
            'PICU', 'OACU', 'MBDU', 'LTNU', 'LCPU', 'JWSU', 
            'FTWU', 'FMGU', 'ETAU', 'DGCU', 'BNNR', 'BMYR'
        }
    
        if ticker_upper in known_junk:
            return False
        
        exclude_patterns = [
            '-W', '-WT', '-WS', '-U', '-UN', '.U', '/WS', '/U', '.WS', '-R', '.RT',  # Warrants, units, rights
            '-P-', '.P',  # Preferred shares (but not at start like "P" ticker)
            '-CL',  # Special classes
        ]
        
        # Exclude if ticker matches exclude patterns
        for pattern in exclude_patterns:
            if pattern in ticker_upper:
                # Special case: -P- should not match tickers starting with P
                if pattern == '-P-' and ticker_upper.startswith('P-'):
                    continue
                return False
            
        # Catch 5+ char tickers ending in U/W/R (warrants/units/rights without hyphens)
        if len(ticker) > 4:
            if ticker_upper.endswith(('U', 'W', 'R')):
                return False
            
        # Catch Preferred Stock listings
        if ticker_upper.endswith(('-A', '-B','-C','-D','-E','-F','-G','-H','-I','J',
                                  '-K','-L','-M','-N','-O','-P','-Q','-R','-S','-T',
                                  '-U','-V','-W','-X','-Y','-Z')):
            return False       
        
        # Exclude purely numeric tickers (often CUSIP-style junk)
        if ticker.replace('-', '').replace('.', '').isdigit():
            return False
            
        return True
        
    def get_all_tickers(self, include_delisted=True, filter_junk=True) -> pd.DataFrame:
        """Get complete universe including dead companies"""
        logger.info("Fetching all tickers from Tiingo...")

        # Rate limit the API call
        self.tiingo_limiter.wait()

        # This gets ALL available tickers
        tickers_meta = self.client.list_tickers()

        # Filter for US stocks/ETFs
        us_exchanges= {'NYSE', 'NASDAQ', 'NYSE ARCA', 'BATS'}
        us_tickers = []
        for ticker in tickers_meta:
            if ticker['exchange'] not in us_exchanges:
                # But still include if it's an index
                if asset_type != 'index':
                    continue

            asset_type = ticker.get('assetType')
            if asset_type:
                asset_type = asset_type.lower()
            else:
                asset_type = None    # DO NOT assume it's a stock

            name = ticker.get('name')
            company_name = name.strip() if name else None

            ticker_info = {
                'ticker': ticker['ticker'],
                'company_name': company_name, # will be None or valid string
                'exchange': ticker['exchange'],
                'asset_type': asset_type,
                'start_date': ticker.get('startDate'),
                'end_date': None if ticker.get('isActive', True) else ticker.get('endDate'),
                'is_active': 1 if ticker.get('isActive', True) else 0
            }
                
            # Include if active OR if delisted after 2015
            if ticker_info['is_active'] or (include_delisted and 
                ticker_info['end_date'] and 
                pd.to_datetime(ticker_info['end_date']) >= pd.to_datetime('2015-01-01')):
                us_tickers.append(ticker_info)

        ticker_df = pd.DataFrame(us_tickers)

        # Add is_etf column here
        ticker_df['is_etf'] = (ticker_df['asset_type'].str.lower() == 'etf').astype(int)

        # Filter out junk tickers (warrants, units, preferreds) but keep ETFs
        if filter_junk:
            before_count = len(ticker_df)
            ticker_df['is_common'] = ticker_df.apply(
                lambda row: self.is_common_stock(row['ticker'], row['asset_type']), 
                axis=1
            )
            ticker_df = ticker_df[ticker_df['is_common']].drop(columns=['is_common'])
            logger.info(f"Filtered out {before_count - len(ticker_df)} junk tickers (warrants, units, preferreds)")

        # Eliminate any duplicate ticker records
        ticker_df = ticker_df.drop_duplicates(subset=['ticker'], keep='first')     
        
        return ticker_df  
    
    def enrich_company_names(self, ticker_list) -> pd.DataFrame:
        """Enrich specific tickers with company names"""
        enriched = []
        errors = []
        count = 0
        for t in ticker_list:
            try:
                count += 1
                self.tiingo_limiter.wait()
                url = f"https://api.tiingo.com/tiingo/daily/{t}?token={self.tiingo_key}"
                response = requests.get(url, timeout=5)        
                data = response.json()
                new_name = data.get('name', '')
                new_row = (t, new_name)
                enriched.append(new_row)

            except Exception as e:
                errors.append((t, str(e)))

        print (f"Completed {count} iterations, {len(errors)} errors")
        enriched_df = pd.DataFrame(enriched, columns=['ticker', 'company_name'])
        null_names = enriched_df['company_name'].isna().sum()
        empty_names = (enriched_df['company_name'] == '').sum()
        logger.info(f"Null company names: {null_names}, Empty company names: {empty_names}")

        return enriched_df
    
    def merge_names(self, df, names_df):
        """Enrich missing company names from a dataframe containing them"""
        
        # Count nulls before
        null_before = df['company_name'].isna().sum()
        
        # Get only the rows we need from names_df
        all_symbols_lookup = names_df[['ticker', 'company_name']].copy()
        all_symbols_lookup = all_symbols_lookup[all_symbols_lookup['company_name'].notna()]
        
        # FIX: Remove duplicate tickers (keep first occurrence)
        all_symbols_lookup = all_symbols_lookup.drop_duplicates(subset='ticker', keep='first')
        
        # Update nulls in df with values from all_symbols
        null_mask = df['company_name'].isna()
        df.loc[null_mask, 'company_name'] = df.loc[null_mask, 'ticker'].map(
            all_symbols_lookup.set_index('ticker')['company_name']
        )
        
        # Count nulls after
        null_after = df['company_name'].isna().sum()
        enriched = null_before - null_after
        
        logger.info(f"Enriched {enriched} company names from all_symbols table")
        logger.info(f"Remaining nulls: {null_after}")
        
        return df
    
    def clean_date_value(self, date_val):
        """Helper method: Clean and validate date values"""
        if pd.isna(date_val) or date_val == '' or date_val is None:
            return None
        
        try:
            # Try to parse the date
            parsed = pd.to_datetime(date_val)
            # Return as string in PostgreSQL format
            return parsed.strftime('%Y-%m-%d') if parsed else None
        except:
            return None
        
    def yfinance_metadata(self, tickers, pause=0.10) -> pd.DataFrame:
        """Get metadata from yFinance"""
        logger.info("Fetching metadata from yFinance...")

        rows = []
        for t in tickers:
            try:
                yt = yf.Ticker(t)
                info = yt.get_info()

                if not isinstance(info, dict) or len(info) == 0:
                    continue

                rows.append({
                    "ticker": t,
                    "company_name_yf": info.get("longName") or info.get("shortName"),
                    "is_etf_yf": 1 if info.get("quoteType") == "ETF" else 0,
                    "industry": info.get("industry"),
                    "sector": info.get("sector"),
                    "exchange_yf": info.get("exchange"),
                })

            except Exception as e:
                logger.error(f"YF error for {t}: {e}")
                continue

            time.sleep(pause)

        logger.info(f"Found {len(rows)} tickers from yFinance.")
        return pd.DataFrame(rows)
        
    def upsert_symbols(self, df) -> List:
        """Upsert symbols with proper date handling and validation"""
        logger.info(f"Upserting {len(df)} symbols...")

        # Create a copy to avoid modifying original
        df = df.copy()

        # Clean date columns
        date_columns = ['start_date', 'end_date']
        for col in date_columns:
            if col in df.columns:
                df[col] = df[col].apply(self.clean_date_value)

        # Ensure is_etf column exists and is properly typed
        if 'is_etf' not in df.columns:
            # Infer from asset_type if needed
            df['is_etf'] = (df['asset_type'].str.lower() == 'etf').astype(int)
        else:
            df['is_etf'] = df['is_etf'].fillna(0).astype(int)
        
        # Ensure is_active is integer
        df['is_active'] = df['is_active'].fillna(0).astype(int)
        
        # Fill empty strings with None for text columns
        text_columns = ['company_name', 'exchange', 'asset_type']
        for col in text_columns:
            if col in df.columns:
                df[col] = df[col].replace('', None)

        # Batch insert with error handling
        batch_size = 1000
        total_inserted = 0
        failed_tickers = []

        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size]

            for _, row in batch.iterrows():
                try:

                    sql = """
                        INSERT INTO symbols (ticker, company_name, exchange, asset_type, is_etf, start_date, end_date, is_active, date_loaded)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_DATE)
                        ON CONFLICT (ticker)
                        DO UPDATE SET
                            company_name = EXCLUDED.company_name,
                            exchange = EXCLUDED.exchange,
                            asset_type = EXCLUDED.asset_type,
                            is_etf = EXCLUDED.is_etf,
                            start_date = EXCLUDED.start_date,
                            end_date = EXCLUDED.end_date,
                            is_active = EXCLUDED.is_active,
                            date_loaded = CURRENT_DATE;
                    """

                    params = (
                        row['ticker'],
                        row['company_name'],
                        row['exchange'],
                        row['asset_type'],
                        row['is_etf'],
                        row['start_date'],
                        row['end_date'],
                        row['is_active']
                    )

                    self.db.execute_sql(sql, params)
                    total_inserted += 1

                except Exception as e:
                    logger.error(f"Failed to insert {row['ticker']}: {e}")
                    failed_tickers.append(row['ticker'])
                    continue
                
            logger.info(f"Processed {min(i+batch_size, len(df))}/{len(df)} symbols")

        logger.info(f"Successfully upserted {total_inserted} symbols, {len(failed_tickers)} failed")
        if failed_tickers:
            logger.warning(f"Failed tickers: {failed_tickers[:10]}...") # Show first 10

        return failed_tickers

    def fetch_industry_sector(self, tickers) -> pd.DataFrame:
        """Fetch industry and sector info for a list of stock tickers"""
        logger.info(f"Fetching industry/sector data for {len(tickers)} tickers...")

        data = []
        failed = []

        for i, ticker in enumerate(tickers, 1):
            try:
                # Rate limit
                self.fmp_limiter.wait()

                url = f"https://financialmodelingprep.com/api/v3/profile/{ticker}?apikey={self.fmp_key}"
                r = requests.get(url, timeout=10)

                if r.status_code == 200:
                    js = r.json()
                    if js:
                        info = js[0]
                        data.append({
                            "ticker": ticker,
                            "industry": info.get("industry"),
                            "sector": info.get("sector")
                        })
                else:
                    logger.warning(f"API returned {r.status_code} for {ticker}")
                    failed.append(ticker)
                    
            except Exception as e:
                logger.error(f"Error fetching {ticker}: {e}")
                failed.append(ticker)

            # Progress update
            if i % 100 == 0:
                logger.info(f"Processed {i}/{len(tickers)} tickers")

        logger.info(f"Retrieved data for {len(data)} tickers, {len(failed)} failed")
        return pd.DataFrame(data)
    
    def upsert_stocks(self, df) -> List:
        """Upsert stocks with data handling"""
        logger.info(f"Upserting {len(df)} stocks...")

        # Create a copy to avoid modifying original
        df = df.copy()

        # Batch insert with error handling
        batch_size = 1000
        total_inserted = 0
        failed_tickers = []

        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size]

            for _, row in batch.iterrows():
                try:        
        
                    sql = """
                        INSERT INTO stocks (ticker, company_name, industry, sector, exchange)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (ticker) DO UPDATE SET
                            company_name = EXCLUDED.company_name,
                            industry = EXCLUDED.industry,
                            sector = EXCLUDED.sector,
                            exchange = EXCLUDED.exchange
                    """
                    params = (
                        row['ticker'], 
                        row['company_name'],
                        row.get('industry'),  # use .get() for safety
                        row.get('sector'),
                        row['exchange']
                    )
                    self.db.execute_sql(sql, params)
                    total_inserted += 1
                except Exception as e:
                    logger.error(f"Failed to insert {row['ticker']}: {e}")
                    failed_tickers.append(row['ticker'])
                    continue

            logger.info(f"Processed {min(i+batch_size, len(df))}/{len(df)} symbols")

        logger.info(f"Successfully upserted {total_inserted} symbols, {len(failed_tickers)} failed")
        if failed_tickers:
            logger.warning(f"Failed tickers: {failed_tickers[:10]}...")

        return failed_tickers

    def download_price_data(self, ticker_list, start_date='2015-01-01', end_date= None, batch_size=100) -> List:
        """Download price data in batches"""
        logger.info(f"Starting price data download for {len(ticker_list)}")

        if end_date is None:
            end_date = (pd.Timestamp.now() - pd.Timedelta(days=1)).strftime('%Y-%m-%d')

        # clear staging table
        self.db.execute_sql('TRUNCATE ohlcv_staging')

        failed = []
        # total_batches = (len(ticker_list) + ticker_list - 1) // ticker_list
        
        for i in range(0, len(ticker_list), batch_size):
            batch = ticker_list[i:i+batch_size]
            logger.info(f"Processing batch {i//batch_size +1}/{len(ticker_list)//batch_size+1}")

            batch_data = []

            for ticker in batch:           
                try:
                    # Rate limit
                    self.tiingo_limiter.wait()

                    # Get data for batch
                    df = self.client.get_dataframe(
                        ticker,
                        startDate=start_date,
                        endDate=end_date,
                        frequency='daily'
                    )

                    if df is None or df.empty:
                        continue
                        
                    # Reset index to make 'date' a column
                    df = df.reset_index()

                    # Check if 'date' column exists (it should after reset_index)
                    if 'date' not in df.columns:
                        logger.error(f"{ticker}: No 'date' column after reset_index. Columns: {df.columns.tolist()}")
                        failed.append(ticker)
                        continue

                    # Add ticker column
                    df['ticker'] = ticker

                    # FIX: Drop unadjusted OHLCV columns to avoid conflicts
                    # We only want adjusted data
                    unadj_cols = ['open', 'high', 'low', 'volume']
                    df = df.drop(columns=[col for col in unadj_cols if col in df.columns])                    
                    
                    # FIX: Use ALL adjusted data for consistency
                    # Rename to PostgreSQL-safe names + keep both adjusted and unadjusted close
                    df = df.rename(columns={
                        'date': 'trade_date',       # Avoid reserved word 'date'
                        'adjOpen': 'price_open',    # Avoid reserved word 'open'
                        'adjHigh': 'price_high',
                        'adjLow': 'price_low',
                        'adjClose': 'price_close',  # Avoid reserved word 'close', adjusted
                        'close': 'close_unadj',     # Keep unadjusted close for reference
                        'adjVolume': 'volume',
                        'divCash': 'dividend',
                        'splitFactor': 'split'
                    })

                    # Define columns we need
                    cols = ['ticker', 'trade_date', 'price_open', 'price_high', 'price_low', 
                            'price_close', 'close_unadj', 'volume', 'dividend', 'split']
                        
                    # Ensure all columns exist
                    for col in cols:
                        if col not in df.columns:
                            if col in ['dividend', 'split']:
                                df[col] = None
                            else:
                                df[col] = 0

                    # Select only needed columns
                    df = df[cols]

                    # Convert date to date-only 
                    df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date

                    batch_data.append(df)

                except Exception as e:
                    logger.error(f"Failed to download {ticker}: {e}")
                    failed.append(ticker)

            # Insert batch to staging
            if batch_data:
                try:
                    combined_df = pd.concat(batch_data, ignore_index=True)

                    # Verify no duplicate column names
                    if len(combined_df.columns) != len(set(combined_df.columns)):
                        dupes = [col for col in combined_df.columns if list(combined_df.columns).count(col) > 1]
                        logger.error(f"Duplicate columns detected: {dupes}")
                        continue

                    self.db.bulk_insert(combined_df, 'ohlcv_staging')
                    logger.info(f"Inserted {len(combined_df)} rows to staging")

                except Exception as e:
                    logger.error(f"Failed to insert batch to staging: {e}")

        logger.info(f"Download complete.  Failed tickers: {len(failed)}")
                           
        return failed 

    def validate_staging_data(self) -> Dict:
        """Helper method: Validate data in staging table before moving to production"""
        logger.info("Validating staging data")

        issues = {}

        # Check for duplicates
        dup_check = """
            select ticker, trade_date, count(*) as count
            from ohlcv_staging
            group by ticker, trade_date
            having count(*) > 1

        """
        duplicates = self.db.psy_query(dup_check)
        if not duplicates.empty:
            issues['duplicates'] = len(duplicates)
            logger.warning(f"Found {len(duplicates)} duplicate entries")

        # Check for data anomalies
        anomaly_check = """
            select count(*) as count from ohlcv_staging
            where price_close <= 0
              or price_high < price_low
              or price_close > price_high * 2
              or volume < 0
        """
        anomalies = self.db.psy_query(anomaly_check)
        if anomalies['count'].iloc[0] > 0:
            issues['anomalies'] = anomalies['count'].iloc[0]
            logger.warning(f"Found {anomalies['count'].iloc[0]} data anomalies")

        # Check row count
        count_check = "select count(*) as count from ohlcv_staging"
        row_count = self.db.psy_query(count_check)['count'].iloc[0]
        logger.info(f"Staging table contains {row_count:,} rows")

        if not issues:
            logger.info("Validation passed - no issues found")
        else:
            logger.warning(f"Validation found issues: {issues}")

        return issues
  
    def validate_and_move_staging(self) -> bool:
        """Validate staging data and move to production table"""

        # First validate
        issues = self.validate_staging_data()

        if issues and 'anomalies' in issues and issues['anomalies'] > 1000:
            logger.error("Too many anomalies - aborting move to production")
            return False
        logger.info("Moving staging data to production...")

        try:
            # Remove duplicates if any
            if 'duplicates' in issues:
                logger.info("Removing duplicates from staging...")
                self.db.execute_sql("""
                    delete from ohlcv_staging a
                    using ohlcv_staging b
                    where a.ctid < b.ctid
                      and a.ticker = b.ticker
                      and a.trade_date = b.trade_date
                """)

            # Insert into production with conflict handling
            result = self.db.execute_sql("""
                insert into ohlcv
                select * from ohlcv_staging
                on conflict (ticker, trade_date)
                do update set
                    price_open = EXCLUDED.price_open,
                    price_high = EXCLUDED.price_high,
                    price_low = EXCLUDED.price_low,
                    price_close = EXCLUDED.price_close,
                    close_unadj = EXCLUDED.close_unadj,
                    volume = EXCLUDED.volume,
                    dividend = EXCLUDED.dividend,
                    split = EXCLUDED.split
            """) 

            # Get row count from staging before clearing
            row_count = self.db.psy_query("select count(*) as count from ohlcv_staging")['count'].iloc[0]
            logger.info(f"Moved {row_count:,} rows to production")

            # Clear staging
            self.db.execute_sql("TRUNCATE ohlcv_staging")

            logger.info("Successfully moved staging to production")
            return True
        
        except Exception as e:
            logger.error(f"Failed to move staging to production: {e}")
            return False
        
    def calculate_log_returns(self):
        """Calculate log returns for the most recent trading day - a rolling 10 day windows computed"""
        query = """
            INSERT INTO daily_log_returns (ticker, trade_date, log_return)
            WITH latest_prices AS (
                SELECT 
                    ticker,
                    trade_date,
                    price_close,
                    LAG(price_close) OVER (PARTITION BY ticker ORDER BY trade_date) as prev_close
                FROM ohlcv
                WHERE trade_date >= (SELECT MAX(trade_date) - INTERVAL '10 days' FROM ohlcv)
            )
            SELECT 
                ticker,
                trade_date,
                LN(price_close / prev_close) as log_return
            FROM latest_prices
            WHERE prev_close IS NOT NULL  -- Remove the trade_date = MAX filter
            ON CONFLICT (ticker, trade_date) DO UPDATE
                SET log_return = EXCLUDED.log_return;
        """
        
        self.db.execute_sql(query)
        logger.info("Calculated log returns for latest trading day")