UPDATE symbols s
SET start_date = (
    SELECT MIN(date)
    FROM prices p
    WHERE p.ticker = s.ticker
)
WHERE s.start_date IS NULL;

