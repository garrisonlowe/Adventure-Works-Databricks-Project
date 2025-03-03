-- Cumulative Analysis

-- Calculate the total sales per month
-- AND the running total of sales OVER time.

----------------------------------------------------------------------------------------
-- This query provides a monthly breakdown of sales performance, including total sales AND running total.
SELECT
  order_date,
  total_sales,
  SUM(total_sales) OVER (ORDER BY order_date) AS running_total_sales
FROM
(
 SELECT
   DATETRUNC (YEAR, order_date) AS order_date,
   SUM(sales_amount) AS total_sales
 FROM
   gold.fact_sales
 WHERE
   order_date IS NOT NULL
 GROUP BY
   DATETRUNC (YEAR, order_date)
) AS monthly_sales
----------------------------------------------------------------------------------------
-- This query provides a yearly breakdown of sales performance, including total sales, running total, AND moving average price.
SELECT
  order_date,
  total_sales,
  SUM(total_sales) OVER (ORDER BY order_date) AS running_total_sales,
  AVG(avg_price) OVER (ORDER BY order_date) AS moving_avg_price
FROM
(
 SELECT
   DATETRUNC (YEAR, order_date) AS order_date,
   SUM(sales_amount) AS total_sales,
   AVG(price) AS avg_price
 FROM
   gold.fact_sales
 WHERE
   order_date IS NOT NULL
 GROUP BY
   DATETRUNC (YEAR, order_date)
) AS monthly_sales