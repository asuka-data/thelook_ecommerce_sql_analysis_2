-- 1) Nominalize order granularity 
WITH orders AS(
  SELECT
    order_id,
    ANY_VALUE(user_id) AS user_id,
    DATE_TRUNC(DATE(MIN(created_at)), MONTH) AS order_month  -- monthly base
  FROM bigquery-public-data.thelook_ecommerce.order_items
  GROUP BY order_id
),

-- 2) Nominarized user granularity 
order_m AS(
  SELECT DISTINCT -- Exclude duplicate orders within same month by same user
    user_id,
    order_month
  FROM orders
),

-- 3) Cohort month per user
cohort AS(
  SELECT 
    user_id,
    MIN(order_month) AS cohort_month
  FROM order_m
  GROUP BY user_id
),

-- 4) Cohort size(denominator)
cohort_size AS(
  SELECT 
    cohort_month,
    COUNT(*) AS cohort_size
  FROM cohort
  GROUP BY cohort_month
),
-- 5) Active user = cohort month * gap (numarator)
user AS(
  SELECT 
    cohort.cohort_month,
    DATE_DIFF(order_month, cohort_month, MONTH) AS gap,
    COUNT(DISTINCT order_m.user_id) AS active
  FROM cohort
  JOIN order_m
  ON cohort.user_id = order_m.user_id
  WHERE order_month >= cohort.cohort_month  -- Exclude Invalid data
  GROUP BY cohort.cohort_month, gap
)

-- 6) Reatantion rate
SELECT 
  user.cohort_month,
  gap,
  active,
  cohort_size.cohort_size,
  SAFE_DIVIDE(active, cohort_size.cohort_size) AS retention_rate
FROM user
JOIN cohort_size
ON user.cohort_month = cohort_size.cohort_month
ORDER BY cohort_month, gap;
