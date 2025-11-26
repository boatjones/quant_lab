
CREATE EXTENSION IF NOT EXISTS timescaledb;

create table symbols (
  ticker  text  primary key,
  company_name text,
  exchange  text,
  asset_type  text,
  is_etf  smallint,
  start_date  date,
  end_date   date,
  is_active  smallint,
  date_loaded date,
  PRIMARY KEY (ticker)
);

CREATE TABLE excluded_tickers (
    ticker text PRIMARY KEY,
    reason text,
    date_excluded DATE DEFAULT CURRENT_DATE
);
  
CREATE TABLE quant01.public.stocks (
	ticker text NOT NULL,
	company_name text NOT NULL,
	industry text,
	sector text,
	exchange text,
	PRIMARY KEY (ticker)
);
  
CREATE TABLE ohlcv (
    ticker TEXT REFERENCES symbols(ticker),
    trade_date DATE NOT NULL,
    price_open NUMERIC,
    price_high NUMERIC,
    price_low NUMERIC,
    price_close NUMERIC,
    close_unadj NUMERIC,
    volume BIGINT,
    dividend NUMERIC,   -- Optional but useful
    split NUMERIC,      -- Optional but very useful
    PRIMARY KEY (ticker, trade_date)
);
CREATE INDEX ON ohlcv(ticker, trade_date);
SELECT create_hypertable('ohlcv', 'trade_date');

-- Add to db_setup.sql
CREATE TABLE ohlcv_staging (
    LIKE ohlcv INCLUDING ALL
);

CREATE TABLE index_mapping (
   index_ticker  TEXT,
   etf_ticker    TEXT,
   PRIMARY KEY (index_ticker, etf_ticker)
);

CREATE or REPLACE TABLE fundamentals (
    ticker TEXT REFERENCES symbols(ticker),
    period_end_date DATE,
    filing_date  DATE,
    report_type char(1),  -- 'Q' or 'A'
    revenue NUMERIC,
    ebit NUMERIC,
    net_income NUMERIC,
    total_assets NUMERIC,
    total_liabilities NUMERIC,
    equity NUMERIC,
    retained_earnings NUMERIC,
    current_assets NUMERIC,
    current_liabilities NUMERIC,
    total_debt NUMERIC,
    cash_and_equiv NUMERIC,
    cfo NUMERIC,
    cfi NUMERIC,
    cff NUMERIC,
    capex NUMERIC,
    shares_outstanding NUMERIC,
    PRIMARY KEY (ticker, period_end_date, report_type)
);
CREATE INDEX ON fundamentals(ticker, period_end_date, report_type);

CREATE OR REPLACE VIEW fundamental_ratios AS
WITH price_on_report_date AS (
    SELECT o.ticker, o.date AS price_date, o.adj_close
    FROM ohlcv o
    JOIN (
        SELECT DISTINCT ticker, period_end_date
        FROM fundamentals
    ) f ON o.ticker = f.ticker
       AND o.date = (
           SELECT MIN(date)
           FROM ohlcv o2
           WHERE o2.ticker = f.ticker
             AND o2.date >= f.period_end_date
       )
),
enterprise_value_calc AS (
    SELECT
        f.ticker,
        f.period_end_date,
        f.report_type,
        p.adj_close * f.shares_outstanding AS market_cap,
        (p.adj_close * f.shares_outstanding) + f.total_debt - f.cash_and_equiv AS enterprise_value
    FROM fundamentals f
    JOIN price_on_report_date p
        ON f.ticker = p.ticker AND f.period_end_date = p.price_date
)

SELECT
    -- 🔑 Keys
    f.ticker,
    f.period_end_date,
    f.report_type,

    -- 🧮 Derived Values
    e.market_cap,
    e.enterprise_value,
    f.current_assets - f.current_liabilities           AS working_capital,

    -- 📊 Profitability Ratios
    f.net_income / NULLIF(f.equity, 0)                 AS roe,
    f.net_income / NULLIF(f.total_assets, 0)           AS roa,
    f.ebit / NULLIF((f.total_assets - f.total_liabilities), 0) AS roic,
    f.ebit / NULLIF(f.revenue, 0)                      AS operating_margin,
    f.net_income / NULLIF(f.revenue, 0)                AS net_margin,
    f.revenue / NULLIF(f.total_assets, 0)              AS asset_turnover,

    -- 💵 Cash Flow Ratios
    f.cfo / NULLIF(f.net_income, 0)                    AS cfo_to_ni,
    f.capex / NULLIF(f.revenue, 0)                     AS capex_to_revenue,
    (f.cfo - f.capex)                                  AS fcf,
    (f.cfo - f.capex) / NULLIF(f.revenue, 0)           AS fcf_margin,

    -- 🏦 Leverage & Solvency
    f.total_liabilities / NULLIF(f.equity, 0)          AS debt_to_equity,
    f.total_liabilities / NULLIF(f.total_assets, 0)    AS debt_to_assets,
    f.equity / NULLIF(f.total_assets, 0)               AS equity_ratio,
    f.total_assets / NULLIF(f.equity, 0)               AS financial_leverage,
    f.total_debt / NULLIF(f.ebit, 0)                   AS debt_to_ebit,

    -- 📈 Valuation Multiples
    e.market_cap / NULLIF(f.net_income, 0)       AS pe_ratio,
    e.market_cap / NULLIF((f.cfo - f.capex), 0)  AS p_fcf_ratio,
    e.market_cap / NULLIF(f.equity, 0)           AS pb_ratio,
    e.enterprise_value / NULLIF((f.cfo - f.capex), 0) AS ev_to_fcf,
    e.enterprise_value / NULLIF(f.revenue, 0)         AS ev_to_revenue,
    e.enterprise_value / NULLIF(f.ebit, 0)            AS ev_to_ebit

FROM fundamentals f
JOIN enterprise_value_calc e
  ON f.ticker = e.ticker AND f.period_end_date = e.period_end_date AND f.report_type = e.report_type;

CREATE OR REPLACE VIEW fundamental_per_share AS
SELECT
    f.ticker,
    f.period_end_date,
    f.report_type,

    -- Per-share metrics
    f.revenue / NULLIF(f.shares_outstanding, 0) AS revenue_per_share,
    f.ebit / NULLIF(f.shares_outstanding, 0)    AS ebit_per_share,
    f.net_income / NULLIF(f.shares_outstanding, 0) AS eps,
    (f.cfo - f.capex) / NULLIF(f.shares_outstanding, 0) AS fcf_per_share,
    f.cash_and_equiv / NULLIF(f.shares_outstanding, 0) AS cash_per_share,
    (f.total_assets - f.total_liabilities) / NULLIF(f.shares_outstanding, 0) AS book_value_per_share
FROM fundamentals f;
