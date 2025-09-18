## Monthly sales report

-- Create CTE for faster scanning 
WITH base AS(
  SELECT 
    order_id, user_id, DATE_TRUNC(DATE(created_at),MONTH) AS ym, sale_price
  FROM bigquery-public-data.thelook_ecommerce.order_items
  WHERE sale_price >= 0  --- Exclude return orders
), 
monthly_sales AS(
-- Calculate total orders and sales by month
 SELECT 
   ym,  
   COUNT(DISTINCT order_id) AS total_order,  -- total order 
   COUNT(DISTINCT user_id) AS total_user,    -- total user
   ROUND(SUM(sale_price),3) AS total_sale,   -- total sales
   SAFE_DIVIDE(SUM(sale_price), COUNT(DISTINCT(order_id)) AS aov     -- average sales
 FROM base
 GROUP BY ym
),
with_prev AS(
  SELECT
    monthly_sales.ym,
    monthly_sales.total_order,
    monthly_sales.total_user,
    monthly_sales.total_sale,
    monthly_sales.aov,
    LAG(monthly_sales.total_sale)OVER(ORDER BY monthly_sales.ym) AS pre_month
  FROM monthly_sales
)
-- Calculate growth rate by month
SELECT 
  monthly_sales.ym,
  monthly_sales.total_order,
  monthly_sales.total_user,
  monthly_sales.total_sale,
  monthly_sales.aov,
  ROUND(pre_month,2) AS prev_month_sales,  
  SAFE_DIVIDE(monthly_sales.total_sale - prev_sales, NULLIF(prev_sales,0)) AS growth_rate  
FROM with_prev
ORDER BY monthly_sales.ym;
