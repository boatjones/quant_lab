from dotenv import load_dotenv
import sys
from os import getenv
import pandas as pd
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
        logging.FileHandler('fmp_load.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class FmpDataManager:
    def __init__(self):
        env_path = project_root / 'secrets' / '.env'
        load_dotenv(env_path)

        self.fmp_key = getenv("FMP_API")
        self.db = PgHook()

        # Add rate limiters
        self.fmp_limiter = RateLimiter(calls_per_minute=30)

        logger.info("Initialized FmpDataManager")

    def fetch_fundamentals(self, ticker, limit_annual, limit_quarterly):
        """Fetch both annual and quarterly data"""

        is_url = f"https://financialmodelingprep.com/api/v3/income-statement/{ticker}"
        bs_url = f"https://financialmodelingprep.com/api/v3/balance-sheet-statement/{ticker}"
        cf_url = f"https://financialmodelingprep.com/api/v3/cash-flow-statement/{ticker}"
    
        # Fetch annual data - 3 calls
        a_params = {'apikey': self.fmp_key, 'period': 'annual', 'limit': limit_annual}

        try:
            self.fmp_limiter.wait()
            response = requests.get(is_url, params=a_params, timeout=10)
            response.raise_for_status()
            income_annual = response.json()
        except Exception as e:
            logger.error(f"Failed to fetch annual income statement for {ticker}: {e}")
            income_annual = []  # ← Default to empty list
        
        try:
            self.fmp_limiter.wait()
            response = requests.get(bs_url, params=a_params, timeout=10)
            response.raise_for_status()
            balance_annual = response.json()
        except Exception as e:
            logger.error(f"Failed to fetch annual balance sheet for {ticker}: {e}")
            balance_annual = []
        
        try:
            self.fmp_limiter.wait()
            response = requests.get(cf_url, params=a_params, timeout=10)
            response.raise_for_status()
            cash_annual = response.json()
        except Exception as e:
            logger.error(f"Failed to fetch annual cash flow for {ticker}: {e}")
            cash_annual = []
    
        # Fetch quarterly data (3 calls)
        q_params = {'apikey': self.fmp_key, 'period': 'quarter', 'limit': limit_quarterly}
        
        try:
            self.fmp_limiter.wait()
            response = requests.get(is_url, params=q_params, timeout=10)
            response.raise_for_status()
            income_quarterly = response.json()
        except Exception as e:
            logger.error(f"Failed to fetch quarterly income statement for {ticker}: {e}")
            income_quarterly = []
        
        try:
            self.fmp_limiter.wait()
            response = requests.get(bs_url, params=q_params, timeout=10)
            response.raise_for_status()
            balance_quarterly = response.json()
        except Exception as e:
            logger.error(f"Failed to fetch quarterly balance sheet for {ticker}: {e}")
            balance_quarterly = []
        
        try:
            self.fmp_limiter.wait()
            response = requests.get(cf_url, params=q_params, timeout=10)
            response.raise_for_status()
            cash_quarterly = response.json()
        except Exception as e:
            logger.error(f"Failed to fetch quarterly cash flow for {ticker}: {e}")
            cash_quarterly = []

        # Merge each set
        annual_df = self.merge_statements(income_annual, balance_annual, cash_annual)
        quarterly_df = self.merge_statements(income_quarterly, balance_quarterly, cash_quarterly)

        # Combine
        combined = pd.concat([annual_df, quarterly_df], ignore_index=True)
        return combined

    def merge_statements(self, income_stmt, balance_sheet, cashflow) -> pd.DataFrame:
        """Helper method to merge three json statements to single dataframe"""

        # Convert to DataFrames
        df_income = pd.DataFrame(income_stmt)
        df_balance = pd.DataFrame(balance_sheet)
        df_cashflow = pd.DataFrame(cashflow)

        # Merge on shared metadata columns
        merge_keys = ['symbol', 'date', 'period']

        merged = df_income.merge(df_balance, on=merge_keys, suffixes=('', '_drop'))
        merged = merged.merge(df_cashflow, on=merge_keys, suffixes=('', '_drop'))

        # Drop duplicate columns
        merged = merged[[col for col in merged.columns if not col.endswith('_drop')]]

        return merged

    def transform_to_schema(self, df) -> pd.DataFrame:
        """Map FMP fields to fundamentals table schema"""

        # Create new DataFrame with schema columns
        transformed = pd.DataFrame({
            'ticker'             : df['symbol'],
            'period_end_date'    : pd.to_datetime(df['date']).dt.date,
            'filing_date'        : pd.to_datetime(df['filingDate']).dt.date,
            'report_type'        : df['period'],
            'revenue'            : df['revenue'],
            'ebit'               : df['ebit'],
            'net_income'         : df['netIncome'],
            'total_assets'       : df['totalAssets'],
            'total_liabilities'  : df['totalLiabilities'],
            'equity'             : df['totalEquity'],
            'retained_earnings'  : df['retainedEarnings'],
            'current_assets'     : df['totalCurrentAssets'],
            'current_liabilities': df['totalCurrentLiabilities'],
            'total_debt'         : df['totalDebt'],
            'cash_and_equiv'     : df['cashAndCashEquivalents'],
            'cfo'                : df['netCashProvidedByOperatingActivities'],
            'cfi'                : df['netCashProvidedByInvestingActivities'],
            'cff'                : df['netCashProvidedByFinancingActivities'],
            'capex'              : df['capitalExpenditure'].fillna(0).abs(),
            'shares_outstanding' : df['weightedAverageShsOutDil']
        })

        return transformed

    def load_all_fundamentals(self, ticker_list, limit_annual, limit_quarterly, batch_size=100):
        """Load fundamentals for all tickers in batches"""
    
        failed = []
        
        for i in range(0, len(ticker_list), batch_size):
            batch = ticker_list[i:i+batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}/{len(ticker_list)//batch_size + 1}")
            
            batch_data = []  # ← Accumulate this batch
            
            for ticker in batch:
                try:
                    # Fetch wide DataFrame for this ticker
                    raw_df = self.fetch_fundamentals(ticker, limit_annual, limit_quarterly)
                    
                    if raw_df.empty:
                        continue
                    
                    # Transform to your schema
                    clean_df = self.transform_to_schema(raw_df)
                    
                    # Add to batch accumulator
                    batch_data.append(clean_df)
                    
                except Exception as e:
                    logger.error(f"Failed to process {ticker}: {e}")
                    failed.append(ticker)
            
            # Bulk insert this batch
            if batch_data:
                combined_batch = pd.concat(batch_data, ignore_index=True)
                self.bulk_insert_fundamentals(combined_batch)
                logger.info(f"Inserted {len(combined_batch)} rows from batch")
        
        return failed
    
    def bulk_insert_fundamentals(self, df):
        """Insert fundamentals data using staging table with upsert logic"""
        try:
            # Insert to staging table
            self.db.bulk_insert(df, 'fundamentals_staging')
            
            # Upsert from staging to production
            upsert_sql = """
                INSERT INTO fundamentals
                SELECT * FROM fundamentals_staging
                ON CONFLICT (ticker, period_end_date, report_type)
                DO UPDATE SET
                    filing_date = EXCLUDED.filing_date,
                    revenue = EXCLUDED.revenue,
                    ebit = EXCLUDED.ebit,
                    net_income = EXCLUDED.net_income,
                    total_assets = EXCLUDED.total_assets,
                    total_liabilities = EXCLUDED.total_liabilities,
                    equity = EXCLUDED.equity,
                    retained_earnings = EXCLUDED.retained_earnings,
                    current_assets = EXCLUDED.current_assets,
                    current_liabilities = EXCLUDED.current_liabilities,
                    total_debt = EXCLUDED.total_debt,
                    cash_and_equiv = EXCLUDED.cash_and_equiv,
                    cfo = EXCLUDED.cfo,
                    cfi = EXCLUDED.cfi,
                    cff = EXCLUDED.cff,
                    capex = EXCLUDED.capex,
                    shares_outstanding = EXCLUDED.shares_outstanding
            """
            
            self.db.execute_sql(upsert_sql)
            
            # Clear staging
            self.db.execute_sql("TRUNCATE fundamentals_staging")
            
            logger.info(f"Successfully upserted {len(df)} fundamental records")
            
        except Exception as e:
            logger.error(f"Failed to bulk insert fundamentals: {e}")
            raise