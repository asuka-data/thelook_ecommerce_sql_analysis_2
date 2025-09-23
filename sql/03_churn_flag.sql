# 基準日を今日に設定するver
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


#　ローリング分析を行うver
-- Nominalize order granularity 
WITH orders AS(
  SELECT 
    order_id,
    ANY_VALUE(user_id) AS user_id, 
    MIN(created_at)AS order_date_ts,
    DATE(MIN(created_at)) AS order_date
  FROM bigquery-public-data.thelook_ecommerce.order_items
  GROUP BY order_id
),
-- 初回注文日を取得
first_order AS(
  SELECT 
    user_id,
    MIN(order_date) AS first_order_date
  FROM orders
  GROUP BY user_id
),
-- ユーザーごとのカレンダーを作成（月単位）
user_calender AS(
  SELECT
    user_id,
    first_order_date,
    ym
  FROM first_order
  CROSS JOIN UNNEST(
    GENERATE_DATE_ARRAY(DATE_TRUNC(first_order_date, MONTH),DATE_TRUNC(CURRENT_DATE(),MONTH), INTERVAL 1 MONTH)
  ) AS ym
),
-- 最終購入日を取得（月単位）
per_user_month AS(
  SELECT 
    user_calender.user_id,
    ym,
    MAX(orders.order_date) AS last_in_month
  FROM user_calender
  JOIN orders
  ON user_calender.user_id = orders.user_id
  AND orders.order_date >= ym
  AND orders.order_date < DATE_ADD(ym, INTERVAL 1 MONTH)
  GROUP BY user_id, ym
),
-- 直近の購入日を取得
with_carries AS(
  SELECT 
    user_id,
    ym,
    LAST_VALUE(last_in_month IGNORE NULLS)OVER(PARTITION BY user_id ORDER BY ym ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS before_last_month,  -- 月初時点での直近の購入日
    LAST_VALUE(last_in_month IGNORE NULLS)OVER(PARTITION BY user_id ORDER BY ym ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_up_to_end_month --　月末時点での直近の購入日
  FROM per_user_month 
),
flags AS(
  SELECT
    user_id,
    ym,
    CASE WHEN before_last_month IS NOT NULL AND DATE_DIFF(ym, before_last_month, DAY) <= 90 THEN 1 ELSE 0 END AS active_at_start,
    CASE WHEN before_last_month IS NOT NULL AND before_last_month = last_up_to_end_month AND DATE_DIFF(DATE_ADD(ym, INTERVAL 1 MONTH), before_last_month, DAY) > 90 THEN 1 ELSE 0 END AS churned_this_month
  FROM with_carries
)
SELECT
  ym,
  SUM(active_at_start) AS at_risk,
  SUM(churned_this_month) AS churner,
  SAFE_DIVIDE(SUM(churned_this_month), NULLIF(SUM(active_at_start),0)) AS monthly_churn_rate
FROM flags 
GROUP BY ym 
ORDER BY ym;

