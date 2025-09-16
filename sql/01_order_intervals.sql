-- 1) Nominalized to order granularity (1 order= 1 row)
WITH order_totals AS (
  SELECT
    order_id,
    ANY_VALUE(user_id) AS user_id,
    MIN(created_at)    AS order_ts,                     -- Exact Time
    DATE(MIN(created_at), 'America/New_York') AS order_date,
    SUM(sale_price)    AS order_total
  FROM `bigquery-public-data.thelook_ecommerce.order_items`
  GROUP BY order_id
),

-- 2) Sequence order interval per user
seq AS (
  SELECT
    user_id,
    order_ts,
    order_date,
    order_total,
    LAG(order_ts) OVER (PARTITION BY user_id ORDER BY order_ts) AS prev_ts
  FROM order_totals
),

gaps AS (
  SELECT
    user_id,
    order_ts,
    order_date,
    order_total,
    prev_ts,
    TIMESTAMP_DIFF(order_ts, prev_ts, DAY)  AS gap_days,   -- Daily basis
    TIMESTAMP_DIFF(order_ts, prev_ts, HOUR) AS gap_hours   -- Hourly basis
  FROM seq
  WHERE prev_ts IS NOT NULL                -- Exclude no prior occurence order 
)

-- 3) Distribution and Representative values (Overall)
SELECT
  COUNT(*) AS pairs,                                             -- the number of pairds ghaving seq orders
  APPROX_QUANTILES(gap_days, 100)[OFFSET(50)] AS p50_days,       -- median
  APPROX_QUANTILES(gap_days, 100)[OFFSET(90)] AS p90_days,       -- p90
  ROUND(AVG(gap_days), 2) AS avg_days
FROM gaps;

-- Group gaps into bins and examine their frequency (Gaps betweem 0 adn 60)
WITH bins AS (
  SELECT gap_days
  FROM gaps
  WHERE gap_days BETWEEN 0 AND 60
)
SELECT
  gap_days,
  COUNT(*) AS cnt,
  SAFE_DIVIDE(COUNT(*), SUM(COUNT(*)) OVER ()) AS pct
FROM bins
GROUP BY gap_days
ORDER BY gap_days;
