-- compute log returns for everything
INSERT INTO daily_log_returns (ticker, trade_date, log_return)
WITH lagged_prices AS (
    SELECT 
        ticker,
        trade_date,
        price_close,
        LAG(price_close) OVER (PARTITION BY ticker ORDER BY trade_date) as prev_close
    FROM ohlcv
    WHERE price_close IS NOT NULL 
      AND price_close > 0  -- exclude zeros
)
SELECT 
    ticker,
    trade_date,
    LN(price_close / prev_close) as log_return
FROM lagged_prices
WHERE prev_close IS NOT NULL 
  AND prev_close > 0  -- exclude zeros in previous close too
ORDER BY ticker, trade_date;

-- compute log returns for a rolling 10 days
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


SELECT ticker, trade_date, price_close, volume
FROM ohlcv
WHERE price_close = 0 OR price_close IS NULL
ORDER BY ticker, trade_date
LIMIT 100;