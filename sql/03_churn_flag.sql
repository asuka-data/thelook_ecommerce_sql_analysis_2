DECLARE as_of DATE DEFAULT CURRENT_DATE();  -- Set the base date to today
DECLARE thres_days INT64 DEFAULT 30; -- Set T to the 30

-- Normarized order granularity 
WITH total_order AS(
  SELECT 
    order_id,
    ANY_VALUE(user_id) AS user_id,
    MIN(created_at) AS order_date_ts,
    DATE(MIN(created_at)) AS order_date,
    SUM(sale_price) AS order_total
  FROM bigquery-public-data.thelook_ecommerce.order_items
  GROUP BY order_id
),

-- Calculate RFM 
rfm AS(
  SELECT 
    user_id,
    MAX(order_date) AS recent_order,
    COUNT(*) AS frequency,
    SUM(order_total) AS monetary
  FROM total_order
  GROUP BY user_id
)
SELECT 
  user_id,
  recent_order,
  frequency,
  ROUND(monetary,2) AS monetary,
  DATE_DIFF(as_of, recent_order, DAY) AS recency_days,    -- The smaller, the better
  IF(DATE_DIFF(as_of, recent_order, DAY) >= thres_days, 1, 0) AS churn_flag_30d
FROM rfm
ORDER BY monetary;
