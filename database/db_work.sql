

/* ###################################
  Symbols Maintenance
 #################################### */

insert into symbols
select ticker, 
       company_name, 
       exchange, 
       asset_type, 
       0 as is_etf,
       null as start_date,
       null as end_date,
       1 as is_active,
       '2025-11-23' as date_loaded
from all_symbols
where asset_type = 'index'

-- add indices to symbols
INSERT INTO symbols (ticker, company_name, exchange_short, asset_type, is_active)
VALUES
  ('GSPC', 'S&P 500 Index', 'INDEXSP', 'index', true),
  ('DJI', 'Dow Jones Industrial Average', 'INDEXDJX', 'index', true),
  ('NDX', 'Nasdaq 100 Index', 'NASDAQ', 'index', true),
  ('RUT', 'Russell 2000 Index', 'INDEXRUSSELL', 'index', true),
  ('VIX', 'CBOE Volatility Index', 'CBOE', 'index', true),
  ('VXN', 'CBOE Nasdaq Volatility Index', 'CBOE', 'index', true),
  ('RVX', 'Russell 2000 Volatility Index', 'CBOE', 'index', true),
  ('DXY', 'US Dollar Index', 'ICE', 'index', true);

  
select distinct asset_type from symbols


select * from symbols where is_etf is null and asset_type in ('fund','trust')

select distinct exchange from symbols

update us_stocks u
set is_etf = s.is_etf
from symbols s
where u.ticker = s.ticker;

select * from us_stocks where type in ('trust','fund') and is_etf is null

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


select distinct asset_type from symbols

select * from symbols where asset_type in ('fund', 'trust')
order by asset_type

select * from symbols where is_etf = 1


select count(*) from symbols where asset_type = 'stock'

select count(*) from stocks

ALTER TABLE symbols ADD COLUMN date_loaded DATE;

select * from symbols where ticker = 'EVFM'
select * from symbols where is_active = 0

select distinct asset_type, count(*) from symbols
where company_name is null
group by asset_type
order by asset_type

select count(*) from symbols

select distinct is_etf, count(*) from symbols where asset_type = 'fund'
group by is_etf

update symbols set asset_type = lower(asset_type)

select * from symbols where ticker = 'IBM'

select count (*) from temp_fmp

select * from symbols where company_name is null


select * from symbols where is_etf is null

select distinct a.ticker, a.company_name, a.asset_type
from temp_fmp a
where a.ticker in (select ticker from temp_tiingo where company_name is null)

update symbols as s
set company_name = a.company_name 
from all_symbols as a
where s.ticker = a.ticker
 and s.company_name is null;
 
 
-- correct mislabelled etfs as stocks
update symbols
set asset_type = 'etf',
    is_etf = 1
where ticker in (select s.ticker
from symbols s
where s.ticker not in (select ticker from temp_orig_symbols)
 and  right(s.company_name, 3) = 'ETF'
 and  s.asset_type = 'stock')

-- delete excluded stocks from symbols table
delete from symbols
where ticker in (select ticker from excluded_tickers)

-- delete mutual funds from symbols table
delete from symbols where asset_type = 'mutual fund'

-- query to alert tiingo support of mislabelled etfs
select s.ticker, s.company_name, s.exchange, t.asset_type, t.is_etf, s.start_date, s.is_active
from symbols s, temp_tiingo t
where s.ticker not in (select ticker from temp_orig_symbols)
 and  s.ticker = t.ticker
 and  t.asset_type != 'etf'

select * from symbols where date_loaded = '2025-11-21'

delete from symbols WHERE date_loaded = '2025-11-17'

delete from symbols where company_name is null

select * from symbols where right(ticker, 1) in ('U', 'W', 'R') and length(ticker) > 4
-- remove warrants, rights
delete from symbols where right(ticker, 1) in ('U', 'W', 'R') and length(ticker) > 4

delete from symbols where ticker in ('WPAU', 'VITU', 'TTOU', 'TSYU', 'TBAU', 'RMGU', 
                                     'PICU', 'OACU', 'MBDU', 'LTNU', 'LCPU', 'JWSU', 
                                     'FTWU', 'FMGU', 'ETAU', 'DGCU', 'BNNR', 'BMYR')

select * from symbols where right(ticker, 1) = 'Q' and asset_type = 'stock'

select * from symbols where end_date is not null

select * from symbols where right(ticker, 2) in (
'-A', '-B','-C','-D','-E','-F','-G','-H','-I','J','-K','-L','-M','-N','-O','-P',
'-Q','-R','-S','-T','-U','-V','-W','-X','-Y','-Z')

delete from symbols where right(ticker, 2) in (
'-A', '-B','-C','-D','-E','-F','-G','-H','-I','J','-K','-L','-M','-N','-O','-P',
'-Q','-R','-S','-T','-U','-V','-W','-X','-Y','-Z')

DELETE FROM symbols 
WHERE ticker IN (
    SELECT ticker FROM stocks 
    WHERE sector IS NULL OR industry IS NULL
);

INSERT INTO symbols (ticker, company_name, exchange, asset_type, is_etf, is_active, date_loaded)
SELECT 
    ticker,
    company_name,
    exchange,
    'stock' as asset_type,
    0 as is_etf,
    1 as is_active,
    CURRENT_DATE as date_loaded
FROM stocks
WHERE ticker NOT IN (SELECT ticker FROM symbols)
  AND ticker NOT IN (SELECT ticker FROM excluded_tickers)
ON CONFLICT (ticker) DO NOTHING;

select * from symbols where date_loaded = '2025-12-04' order by ticker

/* ################################################
  Stocks table maintenance
 ################################################# */

-- add stock tickers discarded in initial load 4 days prior due to missing industry & sector data
insert into excluded_tickers (ticker, reason)
select s.ticker, 'missing sector and industry' as reason
from symbols s
where s.ticker not in (select ticker from temp_orig_symbols)
 and  s.asset_type = 'stock'

select count(*) from stocks where industry is not null

select count(*) from stocks where industry is not null

select * from stocks where industry = ''

delete from stocks where right(ticker, 2) = '/U'

DELETE FROM stocks 
WHERE sector IS NULL OR industry IS NULL;

DELETE FROM symbols 
WHERE ticker IN (
    SELECT ticker FROM stocks 
    WHERE sector = '' OR industry = ''
);

DELETE FROM stocks 
WHERE sector = '' OR industry = '';

/* ###########################################################
    Price level maintenance -- ohlcv & daily_log_returns table maintenance
 ############################################################# */

select count (*) from ohlcv

select (max(trade_date)+1) from ohlcv

select distinct trade_date, count(ticker) from ohlcv where trade_date > '2025-11-01'
group by trade_date
order by trade_date

select distinct trade_date, count(ticker) from daily_log_returns  where trade_date > '2025-11-01' 
group by trade_date
order by trade_date

select * from ohlcv where trade_date in ('2025-12-04', '2025-11-30', '2025-11-29', '2025-11-27', '2025-11-23', '2025-11-22')

select * from daily_log_returns where trade_date = '2024-09-02'

select * from ohlcv where trade_date = '2024-09-02'

delete from daily_log_returns where trade_date = '2024-09-02'

select * from stocks where ticker = 'TDWD'

select * from symbols where ticker = 'TDWD'

select * from fundamentals where ticker = 'TDWD'

select * from excluded_tickers where ticker = 'TDWD'

delete from daily_log_returns where ticker = 'STRY'

DELETE FROM ohlcv 
WHERE trade_date BETWEEN '2025-11-18' AND '2025-11-21';

-- find all tickers in symbols where no price data existed - goal to eliminate them to speed up price download
select s.ticker, s.company_name, s.asset_type, s.is_active
from symbols s
where s.ticker not in (select distinct ticker from ohlcv)
 and  s.asset_type != 'index'
 and  s.date_loaded <= '2025-11-01'

SELECT 
    s.asset_type,
    s.is_active,
    COUNT(*) as count
FROM symbols s
WHERE s.ticker NOT IN (SELECT DISTINCT ticker FROM ohlcv where trade_date > '2025-11-01')
GROUP BY s.asset_type, s.is_active
ORDER BY count DESC;

SELECT 
    s.ticker, 
    s.company_name, 
    s.asset_type,
    MAX(o.trade_date) as last_trade_date,
    s.is_active
FROM symbols s
LEFT JOIN ohlcv o ON s.ticker = o.ticker
WHERE s.is_active = 1
GROUP BY s.ticker, s.company_name, s.asset_type, s.is_active
HAVING MAX(o.trade_date) < '2025-11-01' OR MAX(o.trade_date) IS NULL
ORDER BY last_trade_date DESC NULLS LAST;

SELECT 
    s.asset_type,
    COUNT(*) as will_be_marked_inactive
FROM symbols s
WHERE s.is_active = 1
  AND s.asset_type != 'index'
  AND s.ticker NOT IN (
      SELECT DISTINCT ticker 
      FROM ohlcv 
      WHERE trade_date > '2025-11-01'
  )
GROUP BY s.asset_type;

-- Update database now so maintenance works correctly
UPDATE symbols s
SET 
    is_active = 0,
    end_date = COALESCE(
        (SELECT MAX(trade_date) FROM ohlcv WHERE ticker = s.ticker),
        '2025-01-01'
    )
WHERE s.is_active = 1
  and s.asset_type != 'index'
  AND s.ticker NOT IN (
      SELECT DISTINCT ticker 
      FROM ohlcv 
      WHERE trade_date > '2025-11-01'
  );

insert into excluded_tickers (ticker, reason)
select s.ticker, 'no prices in tiingo' as reason
from symbols s
where s.ticker not in (select distinct ticker from ohlcv)

delete from symbols where ticker in (select ticker from excluded_tickers)

delete from stocks where ticker in (select ticker from excluded_tickers)

select distinct asset_type, count(*) from all_symbols group by asset_type


select a.ticker, a.company_name 
from all_symbols a
where a.ticker in (select distinct ticker from ohlcv) 
 and  a.asset_type = 'index'
 


select distinct ticker, count(*)
from ohlcv
where ticker in ('SPY','IVV','VOO','QQQ','DIA','IWM','VXX','UVXY','UUP')
group by ticker

insert into index_mapping (index_ticker, etf_ticker)
select ticker, 'junk' from symbols where asset_type = 'index'

select s.ticker, s.company_name, s.is_active, s.date_loaded
from symbols s
where s.asset_type = 'stock'
 and  s.ticker not in (select ticker from stocks)
 
select s.ticker, s.company_name, s.asset_type, e.reason, e.date_excluded
from symbols s, excluded_tickers e
where s.ticker = e.ticker

delete from excluded_tickers where ticker = 'STRY'

delete from symbols where ticker = 'TDWD'

select * from symbols where ticker = 'AMZN'

select * from stocks where ticker = 'AMZN'

select * from ohlcv where ticker = 'AMZN' and trade_date > '2024-10-01'

select * from symbols where asset_type = 'index'

SELECT * FROM pg_available_extensions WHERE name LIKE '%python%';

SELECT DISTINCT s.ticker 
FROM stocks s
LEFT JOIN symbols sym ON s.ticker = sym.ticker
WHERE sym.ticker IS NULL;

select e.ticker, e.reason, e.date_excluded, s.company_name
from stocks s, excluded_tickers e
where e.ticker = s.ticker

select st.ticker, st.company_name, st.exchange
from stocks st
where st.ticker not in (select ticker from symbols)

SELECT COUNT(*) as junk_count
FROM stocks
WHERE ticker NOT IN (SELECT ticker FROM symbols)
  AND ticker ~ '.*(-P[A-Z]?|-W|-U|-R)$';
  
INSERT INTO excluded_tickers (ticker, reason)
SELECT ticker, 'orphaned_junk_pattern'
FROM stocks
WHERE ticker NOT IN (SELECT ticker FROM symbols)
  AND ticker ~ '.*(-P[A-Z]?|-W|-U|-R)$'
ON CONFLICT (ticker) DO NOTHING;

DELETE FROM stocks
WHERE ticker ~ '.*(-P[A-Z]?|-W|-U|-R)$'
  AND ticker NOT IN (SELECT ticker FROM symbols);

SELECT COUNT(*)
FROM stocks
WHERE ticker NOT IN (SELECT ticker FROM symbols)
  AND LENGTH(ticker) > 4
  AND RIGHT(ticker, 1) ~ '[A-Z]'
  AND ticker !~ '.*(-P[A-Z]?|-W|-U|-R)$';
  
INSERT INTO excluded_tickers (ticker, reason)
SELECT ticker, 'orphaned_share_class'
FROM stocks
WHERE ticker NOT IN (SELECT ticker FROM symbols)
  AND LENGTH(ticker) > 4
  AND RIGHT(ticker, 1) ~ '[A-Z]'
  AND ticker !~ '.*(-P[A-Z]?|-W|-U|-R)$'
ON CONFLICT (ticker) DO NOTHING;

DELETE FROM stocks
WHERE ticker IN (
  SELECT ticker FROM excluded_tickers 
  WHERE reason = 'orphaned_share_class'
);



INSERT INTO excluded_tickers (ticker, reason)
VALUES ('STRY', 'corrupt_weekend_data')
ON CONFLICT (ticker) DO NOTHING;

DELETE FROM ohlcv WHERE ticker = 'STRY';

DELETE FROM stocks WHERE ticker = 'STRY';

UPDATE symbols
SET is_active = 0,
    end_date = '2022-09-30'  -- Last quarterly statement from your screenshot
WHERE ticker = 'STRY';

select * from fundamentals where ticker = 'STRY'

insert into stocks
values ('STRY', 'Starry Group Holdings, Inc.', 'Telecommunications Services', 'Communication Services', 'NYSE')

SELECT ticker, company_name, exchange, is_active
FROM symbols
WHERE ticker IN (
    'BAESY', -- BAE Systems
    'EADSY', -- Airbus
    'LMTGY', -- Leonardo
    'RNMBY', -- Rheinmetall
    'SAABF'  -- Saab (OTC)
)
ORDER BY ticker;


select distinct o.ticker, count(*) from ohlcv o, adr_whitelist a where a.ticker = o.ticker group by o.ticker

-- query for indexes
SELECT
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE tablename IN ('daily_log_returns', 'ohlcv', 'fundamentals', 'stocks')
ORDER BY tablename, indexname;

-- run stored proc / function for relative strength
SELECT * FROM screener_winners(10, 1000000, 126, 1000000, 0.0, 50);

SELECT * FROM screener_winners();

DROP FUNCTION screener_winners(numeric,numeric,integer,numeric,numeric,numeric);

select * from fundamentals where ticker = 'KNSA'

select * from ohlcv where ticker = 'AMPS'