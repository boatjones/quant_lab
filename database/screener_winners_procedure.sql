-- =====================================================================
-- Winners Screener Stored Procedure
-- =====================================================================
-- This procedure filters stocks based on:
-- 1. Minimum stock price
-- 2. Minimum market cap
-- 3. Relative Strength percentile over variable lookback period
-- 4. Minimum EBIT (quarterly)
-- 5. Minimum Revenue 3-Year CAGR
-- =====================================================================

CREATE OR REPLACE FUNCTION screener_winners(
    p_min_price NUMERIC DEFAULT 35,
    p_min_market_cap NUMERIC DEFAULT 50000000,
    p_lookback_days INT DEFAULT 252,
    p_min_ebit NUMERIC DEFAULT 10000000,
    p_min_revenue_cagr NUMERIC DEFAULT 0.10,
    p_min_rs_percentile NUMERIC DEFAULT 80
)
RETURNS TABLE (
    ticker TEXT,
    company_name TEXT,
    sector TEXT,
    industry TEXT,
    exchange TEXT,
    current_price NUMERIC,
    market_cap NUMERIC,
    rs_percentile NUMERIC,
    ebit NUMERIC,
    revenue_cagr_3y NUMERIC,
    pe_ratio NUMERIC
) AS $$
    WITH current_prices AS (
        -- Get the most recent price for each ticker
        SELECT DISTINCT ON (ticker)
            ticker,
            price_close as current_price,
            trade_date
        FROM ohlcv
        ORDER BY ticker, trade_date DESC
    ),
    rs_calculation AS (
        -- Calculate RS percentile based on sum of log returns over lookback period
        -- For each ticker, ensure it has sufficient data points (80% of expected days)
        SELECT 
            dlr.ticker,
            SUM(dlr.log_return) as cumulative_return,
            COUNT(*) as data_points,
            PERCENT_RANK() OVER (ORDER BY SUM(dlr.log_return)) * 100 as rs_percentile
        FROM (
	        SELECT ticker, trade_date, log_return,
	               ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY trade_date DESC) as rn
	        FROM daily_log_returns
	    ) dlr
        WHERE dlr.rn <= p_lookback_days  -- Use row number instead of date arithmetic
        GROUP BY dlr.ticker
        HAVING COUNT(*) >= (p_lookback_days * 0.8)
    ),
    latest_ebit AS (
        -- Get the most recent quarterly EBIT for each ticker
        SELECT DISTINCT ON (ticker)
            ticker,
            ebit,
            period_end_date
        FROM fundamentals
        WHERE report_type IN ('Q1', 'Q2', 'Q3', 'Q4')
            AND ebit IS NOT NULL
        ORDER BY ticker, period_end_date DESC
    ),
    revenue_cagr AS (
        -- Calculate 3-year revenue CAGR using annual (FY) data
        SELECT 
            ticker,
            CASE 
                WHEN COUNT(*) >= 4 THEN
                    POWER(
                        NULLIF(MAX(CASE WHEN rn = 1 THEN revenue END), 0) / 
                        NULLIF(MAX(CASE WHEN rn = 4 THEN revenue END), 0),
                        1.0/3.0
                    ) - 1
                ELSE NULL
            END as cagr_3y
        FROM (
            SELECT 
                ticker,
                revenue,
                period_end_date,
                ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY period_end_date DESC) as rn
            FROM fundamentals
            WHERE report_type = 'FY'
                AND revenue IS NOT NULL
                AND revenue > 0
        ) annual_revenue
        WHERE rn <= 4
        GROUP BY ticker
    ),
    latest_market_cap AS (
        -- Get the most recent market cap from fundamental_ratios MATERIALIZED VIEW
        SELECT DISTINCT ON (ticker)
            ticker,
            market_cap,
            pe_ratio
        FROM fundamental_ratios
        WHERE market_cap IS NOT NULL
        ORDER BY ticker, period_end_date DESC
    )
    -- Final join and filtering
    SELECT 
        s.ticker,
        s.company_name,
        s.sector,
        s.industry,
        s.exchange,
        ROUND(cp.current_price::NUMERIC, 2) as current_price,
        ROUND(mc.market_cap::NUMERIC, 0) as market_cap,
        ROUND(rs.rs_percentile::NUMERIC, 2) as rs_percentile,
        ROUND(le.ebit::NUMERIC, 0) as ebit,
        ROUND(rc.cagr_3y::NUMERIC, 4) as revenue_cagr_3y,
        ROUND(mc.pe_ratio::NUMERIC, 2) as pe_ratio
    FROM stocks s
    INNER JOIN current_prices cp ON s.ticker = cp.ticker
    INNER JOIN rs_calculation rs ON s.ticker = rs.ticker
    INNER JOIN latest_ebit le ON s.ticker = le.ticker
    INNER JOIN revenue_cagr rc ON s.ticker = rc.ticker
    INNER JOIN latest_market_cap mc ON s.ticker = mc.ticker
    WHERE cp.current_price >= p_min_price
        AND mc.market_cap >= p_min_market_cap
        AND rs.rs_percentile >= p_min_rs_percentile
        AND le.ebit >= p_min_ebit
        AND rc.cagr_3y >= p_min_revenue_cagr
    ORDER BY rs.rs_percentile DESC, mc.market_cap DESC;
$$ LANGUAGE sql STABLE;

-- =====================================================================
-- Usage Examples:
-- =====================================================================

-- Default parameters (Price>$35, MktCap>$50M, 1yr lookback, EBIT>$10M, RevCAGR>10%, RS>80)
-- SELECT * FROM screener_winners();

-- Custom 6-month lookback with lower market cap threshold
-- SELECT * FROM screener_winners(35, 25000000, 126, 10000000, 0.10, 80);

-- Aggressive growth filter: High CAGR, high RS, lower price threshold
-- SELECT * FROM screener_winners(20, 50000000, 252, 5000000, 0.20, 85);

-- =====================================================================
-- Notes:
-- =====================================================================
-- Lookback periods (approximate):
--   6 months  = 126 trading days
--   12 months = 252 trading days
--   18 months = 378 trading days
--   24 months = 504 trading days
--   36 months = 756 trading days
-- =====================================================================
