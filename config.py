"""
Configuration file for Streamlit RRG Indicator
In project root directory
"""

# =============================================================================
# PostgreSQL Database Configuration
# =============================================================================
OHLCV_TABLE = 'ohlcv'          # Your OHLCV table name
DATE_COL = 'trade_date'        # Date column name
SYMBOL_COL = 'ticker'          # Symbol/ticker column name (or 'symbol')
CLOSE_COL = 'price_close'      # Close price column (or 'close')

# =============================================================================
# Tickers File Configuration
# =============================================================================
TICKERS_FILE = 'Tickers.xlsx'  # Adjust path as needed
# TICKERS_FILE = '/absolute/path/to/Tickers.xlsx'  # Or use absolute path

# =============================================================================
# Sheet Configuration - Based on Your Tickers.xlsx
# =============================================================================
DEFAULT_SHEET = 'Sectors'  # Default sheet to load

# Sheet descriptions (shown in dropdown for easier identification)
TICKER_SHEETS = {
    'Octane': '⚡ Energy & Refiners',
    'Stocks': '📈 General Stock Watchlist',
    'Sectors': '🎯 Sector Rotation Analysis',
    'XLB': '🏗️ Materials - XLB Components',
    'Winners': '🏆 Current Top Performers',
    'rtport': '💼 RT Personal Portfolio'
}

# =============================================================================
# Benchmark Configuration - Tiingo-Compatible ETFs
# =============================================================================
# Default Benchmark
BENCHMARK = 'SPY'  # Default benchmark ticker

# Sheet-specific benchmarks (automatically switches when you change sheets)
# Using tradeable ETFs that Tiingo has data for
SHEET_BENCHMARKS = {
    'Octane': 'XLE',
    'Stocks': 'SPY',      # S&P 500 for US stocks
    'Sectors': 'SPY',
    'XLB': 'XLB',           # XLB ETF itself as benchmark
    'Winners': 'SPY',
    'rtport': 'SPY'
}

# =============================================================================
# RRG Calculation Parameters
# =============================================================================
WINDOW = 14  # Rolling window for RRG calculations (in weeks)

# =============================================================================
# Optional: Advanced Settings
# =============================================================================

# You can add different windows per sheet if needed:
# SHEET_WINDOWS = {
#     'Octane': 14,    # Standard 14 weeks for energy
#     'Winners': 8,    # Shorter window for fast movers
#     'rtport': 26     # Longer window for portfolio
# }

# Cache settings (in seconds)
# CACHE_TTL = 3600  # 1 hour

# Date range defaults
# DEFAULT_START_DAYS = 365  # Load 1 year of data by default

# =============================================================================
# Notes
# =============================================================================
"""
## 📋 Complete ETF Benchmark Reference

### Broad Market
| ETF     | Description           | Use For                 |
|---------|-----------------------|-------------------------|
| **SPY** | S&P 500 (most liquid) | General US stocks       |
| **VOO** | S&P 500 (Vanguard)    | Same as SPY, lower fees |
| **QQQ** | Nasdaq 100            | Tech-heavy stocks       |
| **DIA** | Dow Jones 30          | Blue chip stocks        |
| **IWM** | Russell 2000          | Small cap stocks        |
| **VTI** | Total US Market       | Entire US market        |

### Sector ETFs (Select Sector SPDRs)
| ETF      | Sector                 | Use For              |
|----------|------------------------|----------------------|
| **XLE**  | Energy                 | Energy & refiners ⭐ |
| **XLF**  | Financials             | Banks, insurance     |
| **XLK**  | Technology             | Tech stocks          |
| **XLV**  | Healthcare             | Pharma, biotech      |
| **XLI**  | Industrials            | Manufacturing        |
| **XLY**  | Consumer Discretionary | Retail, autos        |
| **XLP**  | Consumer Staples       | Food, household      |
| **XLU**  | Utilities              | Electric, gas        |
| **XLB**  | Materials              | Chemicals, metals ⭐ |
| **XLRE** | Real Estate            | REITs                |
| **XLC**  | Communication Services | Telecom, media       |

### International
| ETF     | Description             | Use For              |
|---------|-------------------------|----------------------|
| **EFA** | Developed Markets ex-US | International stocks |
| **EEM** | Emerging Markets        | EM exposure          |
| **VEU** | All-World ex-US         | Non-US global        |
| **VWO** | Emerging Markets        | EM (Vanguard)        |

### Style-Based
| ETF      | Description         | Use For       |
|-------- -|---------------------|---------------|
| **IWF**  | Russell 1000 Growth | Growth stocks |
| **IWD**  | Russell 1000 Value  | Value stocks  |
| **MTUM** | Momentum Factor     | High momentum |
| **QUAL** | Quality Factor      | High quality  |

### Commodity-Related
| ETF     | Description | Use For        |
|---------|-------------|----------------|
| **GLD** | Gold        | Gold miners    |
| **SLV** | Silver      | Silver miners  |
| **USO** | Oil         | Oil companies  |
| **DBA** | Agriculture | Ag commodities |

Recommended Benchmarks by Strategy:
- Energy stocks → XLE (Energy Sector ETF)
- Tech stocks → QQQ or XLK
- Small caps → IWM
- Dividend stocks → SPY or XLP
- Growth stocks → QQQ
- Value stocks → IWV (iShares S&P 500 Value ETF)

Workflow:
1. Open RRG page
2. Select sheet from dropdown (e.g., "Octane - Energy & Refiners")
3. Benchmark auto-switches to XLE (perfect for energy!)
4. Click "Load Tickers from Excel"
5. Analyze rotation!
6. Switch to different sheet anytime

"""

