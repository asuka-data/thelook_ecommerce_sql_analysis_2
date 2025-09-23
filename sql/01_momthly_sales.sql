## Monthly sales report

-- Create CTE for faster scanning 
-- Nominarized order granularity (1 row = 1 order)
WITH orders AS(
  SELECT 
    order_id,
    ANY_VALUE(user_id) AS user_id, 
    MIN(created_at)AS order_date_ts,
    DATE(MIN(created_at)) AS order_date,
    SUM(sale_price) AS total_sales
  FROM bigquery-public-data.thelook_ecommerce.order_items
  GROUP BY order_id
),
-- Calculate total orders and sales by month
monthly_sales AS(
  SELECT 
    DATE_TRUNC(order_date, MONTH) AS ym,
    COUNT(*) AS total_orders,
    COUNT(DISTINCT user_id) AS total_user,
    SUM(total_sales) AS monthly_total,
    SAFE_DIVIDE(SUM(total_sales),NULLIF(COUNT(*),0)) AS aov
  FROM orders
  GROUP BY ym
),
prev AS(
  SELECT 
    ym,
    total_orders,
    total_user,
    monthly_total,
    aov,
    LAG(monthly_total)OVER(ORDER BY ym) AS prev_month,
    LAG(monthly_total,12)OVER(ORDER BY ym) AS prev_year
  FROM monthly_sales
)
-- Calculate MOM, YOY and convert data type into '%'
SELECT 
  ym,
  total_orders,
  total_user,
  monthly_total,
  ROUND(aov,2) AS aov,
  CONCAT(ROUND(SAFE_DIVIDE(monthly_total - prev_month, NULLIF(prev_month, 0))*100,2),'%') AS mom,
  CONCAT(ROUND(SAFE_DIVIDE(monthly_total - prev_year, NULLIF(prev_year,0))*100,2),'%') AS yoy
FROM prev
ORDER BY ym;
