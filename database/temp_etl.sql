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

