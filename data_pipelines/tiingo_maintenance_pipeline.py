#!/usr/bin/env python3
"""
Tiingo Maintenance Pipeline
Daily/Weekly maintenance updates for symbols, stocks, and price data

Usage:
    python tiingo_maintenance_pipeline.py [--mode daily|weekly] [--backfill-days N]
"""

from dotenv import load_dotenv
import sys
from os import getenv
import pandas as pd
from tiingo import TiingoClient
import time
from pathlib import Path
from datetime import datetime, timedelta
import requests
import logging
import argparse
from typing import List, Dict, Optional, Tuple

# Setup paths
project_root = Path(__file__).parents[1]
sys.path.insert(0, str(project_root))
env_path = project_root / 'secrets' / '.env'

# Import after path is set
from util.to_postgres import PgHook

# Load environment
load_dotenv(env_path)

# Configure logging with timestamp in filename
log_filename = f'maintenance_{datetime.now():%Y%m%d_%H%M%S}.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class RateLimiter:
    """Rate limiter for API calls"""
    def __init__(self, calls_per_minute: int = 50):
        self.calls_per_minute = calls_per_minute
        self.min_interval = 60.0 / calls_per_minute
        self.last_call = 0
    
    def wait(self):
        elapsed = time.time() - self.last_call
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self.last_call = time.time()


class MaintenancePipeline:
    """Handles daily/weekly maintenance updates"""
    
    def __init__(self):
        self.tiingo_key = getenv("TIINGO_API")
        self.fmp_key = getenv("FMP_API")
        self.client = TiingoClient({'api_key': self.tiingo_key})
        self.db = PgHook()
        
        # Rate limiters
        self.tiingo_limiter = RateLimiter(calls_per_minute=50)
        self.fmp_limiter = RateLimiter(calls_per_minute=30)
        
        # Configuration
        self.inactive_threshold_days = 5  # Mark as inactive after 5 days
        
        logger.info("Initialized MaintenancePipeline")
    
    def get_current_symbols(self) -> pd.DataFrame:
        """Get current symbols from database"""
        sql = """
            SELECT ticker, company_name, exchange, asset_type, 
                   is_etf, start_date, end_date, is_active, date_loaded
            FROM symbols
            ORDER BY ticker
        """
        return self.db.psy_query(sql)
    
    def fetch_active_tickers_from_fmp(self) -> pd.DataFrame:
        """
        Fetch all active tickers from FMP
        This is more reliable than Tiingo for maintenance
        """
        logger.info("Fetching active tickers from FMP screener...")
        
        all_tickers = []
        
        # FMP screener endpoint for active US stocks
        exchanges = ['NYSE', 'NASDAQ', 'NYSE ARCA']
        
        for exchange in exchanges:
            try:
                self.fmp_limiter.wait()
                
                # Use screener endpoint - more efficient
                url = f"https://financialmodelingprep.com/api/v3/stock-screener"
                params = {
                    'apikey': self.fmp_key,
                    'exchange': exchange,
                    'isActivelyTrading': 'true',
                    'limit': 10000  # Max allowed
                }
                
                r = requests.get(url, params=params, timeout=30)
                if r.status_code == 200:
                    data = r.json()
                    for item in data:
                        all_tickers.append({
                            'ticker': item['symbol'],
                            'company_name': item.get('companyName', ''),
                            'exchange': item.get('exchangeShortName', exchange),
                            'sector': item.get('sector'),
                            'industry': item.get('industry'),
                            'market_cap': item.get('marketCap'),
                            'is_etf': item.get('isEtf', False),
                            'is_active': True
                        })
                    logger.info(f"Found {len(data)} tickers from {exchange}")
                    
            except Exception as e:
                logger.error(f"Error fetching {exchange}: {e}")
        
        df = pd.DataFrame(all_tickers)
        
        # Derive asset_type
        df['asset_type'] = df['is_etf'].apply(lambda x: 'etf' if x else 'stock')
        df['is_etf'] = df['is_etf'].astype(int)
        df['is_active'] = 1
        
        logger.info(f"Total active tickers from FMP: {len(df)}")
        return df
    
    def identify_changes(self, current_df: pd.DataFrame, fmp_df: pd.DataFrame) -> Dict:
        """
        Identify new, updated, and inactive tickers
        """
        current_tickers = set(current_df['ticker'])
        fmp_tickers = set(fmp_df['ticker'])
        
        # New tickers (in FMP but not in DB)
        new_tickers = fmp_tickers - current_tickers
        
        # Potentially inactive (in DB but not in FMP)
        missing_tickers = current_tickers - fmp_tickers
        
        # Check which missing tickers should be marked inactive
        inactive_candidates = current_df[
            (current_df['ticker'].isin(missing_tickers)) &
            (current_df['is_active'] == 1)
        ]
        
        # Updated tickers (in both - check for changes)
        common_tickers = current_tickers & fmp_tickers
        
        changes = {
            'new': list(new_tickers),
            'inactive_candidates': inactive_candidates['ticker'].tolist(),
            'common': list(common_tickers),
            'stats': {
                'current_count': len(current_tickers),
                'fmp_count': len(fmp_tickers),
                'new_count': len(new_tickers),
                'missing_count': len(missing_tickers)
            }
        }
        
        logger.info(f"Changes identified: {changes['stats']}")
        return changes
    
    def update_symbols_and_stocks(self, fmp_df: pd.DataFrame, changes: Dict):
        """
        Update symbols and stocks tables with new and changed data
        """
        logger.info("Updating symbols and stocks tables...")
        
        # 1. Insert new tickers
        if changes['new']:
            new_df = fmp_df[fmp_df['ticker'].isin(changes['new'])].copy()
            new_df['start_date'] = datetime.now().date()
            new_df['date_loaded'] = datetime.now().date()
            
            # Upsert to symbols
            for _, row in new_df.iterrows():
                try:
                    sql = """
                        INSERT INTO symbols (ticker, company_name, exchange, asset_type, 
                                           is_etf, start_date, is_active, date_loaded)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (ticker) DO UPDATE SET
                            company_name = EXCLUDED.company_name,
                            exchange = EXCLUDED.exchange,
                            is_active = 1,
                            end_date = NULL,
                            date_loaded = CURRENT_DATE
                    """
                    params = (
                        row['ticker'], row['company_name'], row['exchange'],
                        row['asset_type'], row['is_etf'], row['start_date'],
                        1, row['date_loaded']
                    )
                    self.db.execute_sql(sql, params)
                    
                    # If it's a stock, update stocks table
                    if row['asset_type'] == 'stock':
                        sql = """
                            INSERT INTO stocks (ticker, company_name, industry, sector, exchange)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (ticker) DO UPDATE SET
                                company_name = EXCLUDED.company_name,
                                industry = EXCLUDED.industry,
                                sector = EXCLUDED.sector
                        """
                        params = (
                            row['ticker'], row['company_name'],
                            row.get('industry'), row.get('sector'), row['exchange']
                        )
                        self.db.execute_sql(sql, params)
                        
                except Exception as e:
                    logger.error(f"Error updating {row['ticker']}: {e}")
            
            logger.info(f"Added {len(new_df)} new tickers")
        
        # 2. Mark tickers as inactive if missing for threshold days
        if changes['inactive_candidates']:
            # Check last price update for each candidate
            for ticker in changes['inactive_candidates']:
                sql = """
                    SELECT MAX(date) as last_date 
                    FROM ohlcv 
                    WHERE ticker = %s
                """
                result = self.db.psy_query(sql, (ticker,))
                
                if not result.empty:
                    last_date = result['last_date'].iloc[0]
                    if last_date:
                        days_since = (datetime.now().date() - last_date).days
                        
                        if days_since > self.inactive_threshold_days:
                            # Mark as inactive
                            sql = """
                                UPDATE symbols 
                                SET is_active = 0, 
                                    end_date = %s
                                WHERE ticker = %s
                            """
                            self.db.execute_sql(sql, (datetime.now().date(), ticker))
                            logger.info(f"Marked {ticker} as inactive (no data for {days_since} days)")
        
        # 3. Update metadata for existing active tickers
        common_df = fmp_df[fmp_df['ticker'].isin(changes['common'])]
        for _, row in common_df.iterrows():
            try:
                # Update symbols
                sql = """
                    UPDATE symbols 
                    SET company_name = %s,
                        date_loaded = CURRENT_DATE
                    WHERE ticker = %s
                """
                self.db.execute_sql(sql, (row['company_name'], row['ticker']))
                
                # Update stocks if applicable
                if row['asset_type'] == 'stock':
                    sql = """
                        UPDATE stocks 
                        SET industry = COALESCE(%s, industry),
                            sector = COALESCE(%s, sector)
                        WHERE ticker = %s
                    """
                    self.db.execute_sql(sql, (
                        row.get('industry'), 
                        row.get('sector'), 
                        row['ticker']
                    ))
                    
            except Exception as e:
                logger.error(f"Error updating {row['ticker']}: {e}")
        
        logger.info(f"Updated {len(common_df)} existing tickers")
    
    def get_price_update_range(self) -> Tuple[datetime, datetime]:
        """
        Determine date range for price updates
        Returns (start_date, end_date)
        """
        # Get max date from ohlcv table
        sql = "SELECT MAX(date) as max_date FROM ohlcv"
        result = self.db.psy_query(sql)
        
        if result.empty or result['max_date'].iloc[0] is None:
            # No data, start from 30 days ago
            start_date = datetime.now().date() - timedelta(days=30)
        else:
            # Start from day after last date
            start_date = result['max_date'].iloc[0] + timedelta(days=1)
        
        # End date is yesterday (markets closed today)
        end_date = datetime.now().date() - timedelta(days=1)
        
        # Skip if dates don't make sense
        if start_date > end_date:
            logger.warning(f"Start date {start_date} is after end date {end_date}")
            return None, None
        
        logger.info(f"Price update range: {start_date} to {end_date}")
        return start_date, end_date
    
    def download_incremental_prices(self, start_date: datetime, end_date: datetime):
        """
        Download price data for active tickers for specified date range
        """
        # Get active tickers
        sql = """
            SELECT ticker 
            FROM symbols 
            WHERE is_active = 1
            ORDER BY ticker
        """
        active_tickers = self.db.psy_query(sql)['ticker'].tolist()
        
        logger.info(f"Downloading prices for {len(active_tickers)} tickers from {start_date} to {end_date}")
        
        # Clear staging
        self.db.execute_sql("TRUNCATE ohlcv_staging")
        
        # Process in batches
        batch_size = 100
        failed = []
        
        for i in range(0, len(active_tickers), batch_size):
            batch = active_tickers[i:i+batch_size]
            batch_data = []
            
            logger.info(f"Processing batch {i//batch_size + 1}/{(len(active_tickers)-1)//batch_size + 1}")
            
            for ticker in batch:
                try:
                    self.tiingo_limiter.wait()
                    
                    df = self.client.get_dataframe(
                        ticker,
                        startDate=start_date.strftime('%Y-%m-%d'),
                        endDate=end_date.strftime('%Y-%m-%d'),
                        frequency='daily'
                    )
                    
                    if df is not None and not df.empty:
                        df = df.reset_index()
                        df['ticker'] = ticker
                        
                        # Rename columns
                        df = df.rename(columns={
                            'date': 'date',
                            'adjClose': 'adj_close',
                            'adjHigh': 'high',
                            'adjLow': 'low',
                            'adjOpen': 'open',
                            'adjVolume': 'volume',
                            'close': 'close',
                            'divCash': 'dividend',
                            'splitFactor': 'split'
                        })
                        
                        # Ensure all columns
                        cols = ['ticker', 'date', 'open', 'high', 'low', 'close',
                                'adj_close', 'volume', 'dividend', 'split']
                        
                        for col in cols:
                            if col not in df.columns:
                                if col == 'close' and 'adj_close' in df.columns:
                                    df['close'] = df['adj_close']
                                else:
                                    df[col] = None
                        
                        df = df[cols]
                        batch_data.append(df)
                        
                except Exception as e:
                    logger.debug(f"Failed {ticker}: {e}")
                    failed.append(ticker)
            
            # Insert batch
            if batch_data:
                combined_df = pd.concat(batch_data, ignore_index=True)
                try:
                    self.db.bulk_insert(combined_df, 'ohlcv_staging')
                    logger.info(f"Inserted {len(combined_df)} rows to staging")
                except Exception as e:
                    logger.error(f"Batch insert failed: {e}")
        
        # Move staging to production
        self.move_staging_to_production()
        
        logger.info(f"Price update complete. Failed tickers: {len(failed)}")
        return failed
    
    def move_staging_to_production(self):
        """Move validated staging data to production"""
        # Quick validation
        sql = """
            SELECT COUNT(*) as count,
                   COUNT(DISTINCT ticker) as tickers,
                   MIN(date) as min_date,
                   MAX(date) as max_date
            FROM ohlcv_staging
        """
        stats = self.db.psy_query(sql)
        
        if stats['count'].iloc[0] > 0:
            logger.info(f"Moving {stats['count'].iloc[0]} rows to production "
                       f"({stats['tickers'].iloc[0]} tickers, "
                       f"{stats['min_date'].iloc[0]} to {stats['max_date'].iloc[0]})")
            
            # Upsert to production
            sql = """
                INSERT INTO ohlcv 
                SELECT * FROM ohlcv_staging
                ON CONFLICT (ticker, date) 
                DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    adj_close = EXCLUDED.adj_close,
                    volume = EXCLUDED.volume,
                    dividend = EXCLUDED.dividend,
                    split = EXCLUDED.split
            """
            self.db.execute_sql(sql)
            
            # Clear staging
            self.db.execute_sql("TRUNCATE ohlcv_staging")
            logger.info("Successfully moved staging to production")
        else:
            logger.warning("No data in staging to move")
    
    def generate_report(self):
        """Generate summary report of maintenance run"""
        report = []
        report.append("\n" + "="*60)
        report.append("MAINTENANCE PIPELINE REPORT")
        report.append("="*60)
        report.append(f"Run time: {datetime.now()}")
        
        # Symbol stats
        sql = """
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) as active,
                SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END) as inactive,
                SUM(CASE WHEN asset_type = 'stock' THEN 1 ELSE 0 END) as stocks,
                SUM(CASE WHEN asset_type = 'etf' THEN 1 ELSE 0 END) as etfs
            FROM symbols
        """
        stats = self.db.psy_query(sql).iloc[0]
        
        report.append("\nSymbol Statistics:")
        report.append(f"  Total symbols: {stats['total']:,}")
        report.append(f"  Active: {stats['active']:,}")
        report.append(f"  Inactive: {stats['inactive']:,}")
        report.append(f"  Stocks: {stats['stocks']:,}")
        report.append(f"  ETFs: {stats['etfs']:,}")
        
        # Price data stats
        sql = """
            SELECT 
                COUNT(DISTINCT ticker) as tickers,
                COUNT(*) as total_records,
                MIN(date) as min_date,
                MAX(date) as max_date,
                MAX(date) - MIN(date) as days_covered
            FROM ohlcv
        """
        price_stats = self.db.psy_query(sql).iloc[0]
        
        report.append("\nPrice Data Statistics:")
        report.append(f"  Tickers with prices: {price_stats['tickers']:,}")
        report.append(f"  Total records: {price_stats['total_records']:,}")
        report.append(f"  Date range: {price_stats['min_date']} to {price_stats['max_date']}")
        report.append(f"  Days covered: {price_stats['days_covered']}")
        
        # Recent updates
        sql = """
            SELECT date, COUNT(DISTINCT ticker) as tickers, COUNT(*) as records
            FROM ohlcv
            WHERE date >= CURRENT_DATE - INTERVAL '7 days'
            GROUP BY date
            ORDER BY date DESC
            LIMIT 7
        """
        recent = self.db.psy_query(sql)
        
        if not recent.empty:
            report.append("\nRecent Price Updates (last 7 days):")
            for _, row in recent.iterrows():
                report.append(f"  {row['date']}: {row['tickers']:,} tickers, {row['records']:,} records")
        
        report.append("="*60)
        
        report_text = '\n'.join(report)
        logger.info(report_text)
        
        # Save to file
        with open(f'maintenance_report_{datetime.now():%Y%m%d}.txt', 'w') as f:
            f.write(report_text)
    
    def run(self, mode: str = 'daily', backfill_days: Optional[int] = None):
        """
        Run maintenance pipeline
        
        Args:
            mode: 'daily' or 'weekly'
            backfill_days: Override to backfill N days of prices
        """
        logger.info(f"Starting {mode} maintenance run")
        start_time = time.time()
        
        try:
            # Step 1: Get current symbols from database
            current_symbols = self.get_current_symbols()
            logger.info(f"Current symbols in database: {len(current_symbols)}")
            
            # Step 2: Fetch active tickers from FMP
            fmp_df = self.fetch_active_tickers_from_fmp()
            
            # Step 3: Identify changes
            changes = self.identify_changes(current_symbols, fmp_df)
            
            # Step 4: Update symbols and stocks
            self.update_symbols_and_stocks(fmp_df, changes)
            
            # Step 5: Determine price update range
            if backfill_days:
                start_date = datetime.now().date() - timedelta(days=backfill_days)
                end_date = datetime.now().date() - timedelta(days=1)
            else:
                start_date, end_date = self.get_price_update_range()
            
            # Step 6: Download incremental prices
            if start_date and end_date:
                failed = self.download_incremental_prices(start_date, end_date)
                if failed:
                    logger.warning(f"Failed to update {len(failed)} tickers: {failed[:10]}...")
            else:
                logger.info("No price updates needed")
            
            # Step 7: Generate report
            self.generate_report()
            
            elapsed = time.time() - start_time
            logger.info(f"Maintenance run completed in {elapsed/60:.1f} minutes")
            
        except Exception as e:
            logger.error(f"Maintenance pipeline failed: {e}", exc_info=True)
            raise


def main():
    parser = argparse.ArgumentParser(description='Tiingo Maintenance Pipeline')
    parser.add_argument('--mode', choices=['daily', 'weekly'], default='daily',
                       help='Run mode (default: daily)')
    parser.add_argument('--backfill-days', type=int,
                       help='Backfill N days of prices (overrides auto-detection)')
    
    args = parser.parse_args()
    
    pipeline = MaintenancePipeline()
    pipeline.run(mode=args.mode, backfill_days=args.backfill_days)


if __name__ == "__main__":
    main()
