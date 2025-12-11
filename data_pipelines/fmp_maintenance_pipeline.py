#!/usr/bin/env python3
"""
FMP Maintenance Pipeline
Weekly maintenance updates for stocks for fundamental data

Usage:
    python fmp_maintenance_pipeline.py 
"""

import sys
import pandas as pd
from pathlib import Path
import logging
from datetime import date

# Get the path to your secrets directory
project_root = Path(__file__).parents[1]
sys.path.insert(0, str(project_root))

# must come after path is set
from util.to_postgres import PgHook
from util.fmp_manager import FmpDataManager

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

# instantiate FMP Data Manager
fmp = FmpDataManager()

# Get list of stock tickers for fundamentals
stock_list = db.psy_query('select ticker from stocks where ticker not in (select ticker from adr_whitelist)')['ticker'].tolist()

# Get fundamentals
failed = fmp.load_all_fundamentals(stock_list, limit_annual=1, limit_quarterly=2)
print(f"Loaded financials for {len(stock_list)} stocks with {len(failed)} failures.")

# Refresh materialized views
fmp.refresh_materialized_views()
