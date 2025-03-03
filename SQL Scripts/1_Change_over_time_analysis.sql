-- Change over Time Analysis

-- Analyze Sales Perfomance OVER Time
-- This query provides a monthly breakdown of sales performance, including total sales, number of customers, AND quantity sold. 
----------------------------------------------------------------------------------------
SELECT
  YEAR (order_date) AS order_year,
  MONTH (order_date) AS order_month,
  SUM(sales_amount) AS total_sales,
  COUNT(DISTINCT customer_key) AS total_customers,
  SUM(quantity) AS total_quantity
FROM
  gold.fact_sales
WHERE
  order_date IS NOT NULL
GROUP BY
  YEAR (order_date),
  MONTH (order_date)
ORDER BY
  YEAR (order_date),
  MONTH (order_date)

----------------------------------------------------------------------------------------
SELECT
  DATETRUNC (MONTH, order_date) AS order_date,
  SUM(sales_amount) AS total_sales,
  COUNT(DISTINCT customer_key) AS total_customers,
  SUM(quantity) AS total_quantity
FROM
  gold.fact_sales
WHERE
  order_date IS NOT NULL
GROUP BY
  DATETRUNC (MONTH, order_date)
ORDER BY
  DATETRUNC (MONTH, order_date)
