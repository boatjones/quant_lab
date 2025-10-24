UPDATE symbols s
SET end_date = (
    SELECT MAX(p.date)
    FROM prices p
    WHERE p.ticker = s.ticker
),
    is_active = 0
WHERE is_active = 1
  AND (
      SELECT MAX(p.date)
      FROM prices p
      WHERE p.ticker = s.ticker
  ) < CURRENT_DATE - INTERVAL '3 days';

