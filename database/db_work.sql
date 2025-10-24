select count(*) from all_symbols


CREATE TABLE quant01.public.all_symbols (
	ticker text,
	company_name text,
	exchange_short text,
	asset_type text
);

create table symbols (
  ticker  text  primary key,
  company_name text,
  exchange  text,
  asset_type  text,
  is_etf  smallint,
  start_date  date,
  end_date   date,
  is_active  smallint);


CREATE TABLE quant01.public.stocks (
	ticker varchar(20) NOT NULL,
	company_name varchar(80) NOT NULL,
	industry varchar(50),
	sector varchar(50),
	exchange varchar(10),
	PRIMARY KEY (ticker)
);

CREATE TABLE ohlcv (
    ticker TEXT REFERENCES symbols(ticker),
    date DATE NOT NULL,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    adj_close NUMERIC,
    volume BIGINT,
    dividend NUMERIC,   -- Optional but useful
    split NUMERIC,      -- Optional but very useful
    PRIMARY KEY (ticker, date)
);
CREATE INDEX ON ohlcv(ticker, date);
SELECT create_hypertable('ohlcv', 'date');


CREATE or REPLACE TABLE fundamentals (
    ticker TEXT REFERENCES symbols(ticker),
    period_end_date DATE,
    report_type char(1),  -- 'Q' or 'A'
    revenue NUMERIC,
    ebit NUMERIC,
    net_income NUMERIC,
    total_assets NUMERIC,
    total_liabilities NUMERIC,
    equity NUMERIC,
    retained_earnings NUMERIC,
    working_capital NUMERIC,
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


CREATE EXTENSION IF NOT EXISTS timescaledb;


select distinct exchange_short from all_symbols

select distinct asset_type from all_symbols

ALTER TABLE quant01.public.all_symbols ADD start_date date

ALTER TABLE quant01.public.all_symbols ADD end_date date

ALTER TABLE quant01.public.all_symbols ADD is_active bool

select distinct * from symbols
where asset_type = 'stock'
 and  ticker not in (select ticker from stocks)
 
insert into stocks
	(ticker, company_name, exchange)
select distinct ticker, company_name, exchange from symbols
where asset_type = 'stock'
 and  ticker not in (select ticker from stocks)

select * from industry_sector_table where industry = ''

UPDATE stocks s
SET
  industry = i.industry,
  sector = i.sector
FROM industry_sector_table i
WHERE i.ticker = s.ticker
  AND i.industry != '';


select * from all_symbols where ticker in ('VTI'

select * from all_symbols where ticker in ('IEF','TLT','LQD','HYG','SHY','EEM','EFA','DXY','GLD','DBC','NYA')

INSERT INTO all_symbols (ticker, company_name, exchange_short, asset_type, is_active)
VALUES
  ('GSPC', 'S&P 500 Index', 'INDEXSP', 'index', true),
  ('DJI', 'Dow Jones Industrial Average', 'INDEXDJX', 'index', true),
  ('NDX', 'Nasdaq 100 Index', 'NASDAQ', 'index', true),
  ('RUT', 'Russell 2000 Index', 'INDEXRUSSELL', 'index', true),
  ('VIX', 'CBOE Volatility Index', 'CBOE', 'index', true),
  ('VXN', 'CBOE Nasdaq Volatility Index', 'CBOE', 'index', true),
  ('RVX', 'Russell 2000 Volatility Index', 'CBOE', 'index', true),
  ('DXY', 'US Dollar Index', 'ICE', 'index', true);

select distinct exchange_short from all_symbols

select * from all_symbols
WHERE exchange_short IN (
  'NYSE', 'NASDAQ', 'AMEX', 'CBOE'
  'INDEXSP', 'INDEXDJX', 'INDEXRUSSELL'
)

select * from all_symbols where asset_type = 'trust'
and exchange_short IN (
  'NYSE', 'NASDAQ', 'AMEX', 'CBOE',
  'INDEXSP', 'INDEXDJX', 'INDEXRUSSELL')

delete from all_symbols where exchange_short is null

ALTER TABLE quant01.public.all_symbols ADD is_etf bool

UPDATE all_symbols s
SET is_etf = t.is_etf::boolean
FROM temp_trust t
WHERE s.ticker = t.ticker;

ALTER TABLE all_symbols
ALTER COLUMN is_active TYPE SMALLINT
USING CASE
    WHEN is_active IS TRUE THEN 1
    WHEN is_active IS FALSE THEN 0
    ELSE NULL
END;


  
insert into symbols (
  ticker, company_name, exchange, asset_type, is_etf)
select distinct ticker, company_name, exchange_short, asset_type, is_etf
from all_symbols
where exchange_short IN (
  'NYSE', 'NASDAQ', 'AMEX', 'CBOE',
  'INDEXSP', 'INDEXDJX', 'INDEXRUSSELL')
  
select count (*) from symbols where is_etf = 0

select distinct asset_type from symbols

select * from symbols where asset_type = 'fund'

update symbols
set is_etf = 1
where asset_type = 'fund'

select * from symbols where is_etf is null and asset_type in ('fund','trust')

select distinct exchange from symbols

update us_stocks u
set is_etf = s.is_etf
from symbols s
where u.ticker = s.ticker;

select * from us_stocks where type in ('trust','fund') and is_etf is null

update us_stocks set is_etf = 1
where ticker in (
'LMUB',
'SMCY',
'RSSE',
'OMAH',
'WTMY',
'WTMU',
'LGDX',
'QDWN',
'CHPY',
'DFII',
'IBCA',
'IBIL',
'ETH',
'ICOI',
'OWNB',
'BFAP',
'PCFI')

update us_stocks set is_etf = 0
where is_etf is null
 and type in ('trust','fund')
 
select type, count(type) from us_stocks where is_etf = 1 or is_etf is null group by type

select asset_type, count(asset_type) from symbols 
where is_active = 1
 and (is_etf = 1) or (is_etf is null) 
group by asset_type

/* dead stuff not in the new data */ 
select asset_type, count(asset_type) from symbols
where ticker not in (select ticker from us_stocks)
group by asset_type

/* newly added symbols not in previous version */ 
select type, count(type) from us_stocks
where ticker not in (select ticker from symbols)
/* and is_etf = 1 or is_etf is null */
group by type

select * from us_stocks where is_etf is null and type in ('trust','fund')

/* flag inactive symbols as inactive */
update symbols
set is_active = 0
where ticker not in (select ticker from us_stocks)

update symbols set is_active = 1 where is_active is null

/* insert new records to symbols table */
insert into symbols (
  ticker,
  company_name,
  exchange,
  asset_type,
  is_etf)
select ticker,
       company_name,
       exchange_short,
       type,
       is_etf
from us_stocks
where ticker not in (select ticker from symbols)


select * from symbols where is_etf =1;

select distinct exchange, 
               count(ticker) 
from symbols 
where asset_type = 'stock' 
group by exchange 
order by exchange;

select distinct asset_type from symbols group by asset_type;

select * from all_symbols where ticker not in (select ticker from symbols);

select distinct asset_type, exchange from us_stocks 
where ticker not in (select ticker from symbols)
group by asset_type, exchange
order by asset_type, exchange;

select * from symbols where ticker = 'EVFM'

select distinct asset_type, exchange_short, count(ticker)
from all_symbols
group by asset_type, exchange_short
order by asset_type, exchange_short

select * from stocks where ticker not in (select ticker from industry_sector);

/*
WHERE exchange IN (
  'NYSE',
  'NASDAQ',
  'AMEX',
  'CBOE',         -- ETFs, options-based products, VIX-type instruments
  'INDEXSP',      -- S&P indices
  'INDEXDJX',     -- Dow Jones indices
  'INDEXRUSSELL'  -- Russell indices
)

tiingo price pull logoc
WHERE is_active = 1
  AND (is_etf = 1 OR is_etf IS NULL)
  
If Using Tiingo
Tiingo returns:
adjClose, divCash, and splitFactor fields
Which map cleanly to:
adj_close, dividend, and split in your schema
*/

