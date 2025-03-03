-- Data Segmentation

-- Group the data based on a specific range
-- Helps understand the correlation between two measures.

-- Segment products into cost ranges and count hoe many products fall into each segment
----------------------------------------------------------------------------------------
WITH product_segment AS (
	SELECT
	  product_key,
	  product_name,
	  cost,
	  CASE
		WHEN cost < 100 THEN 'Below 100'
		WHEN cost BETWEEN 100 AND 500 THEN '100-500'
		WHEN cost BETWEEN 500 AND 1000 THEN '500-1000'
		ELSE 'Above 1000'
	  END AS cost_range
	FROM gold.dim_products
)
SELECT
cost_range,
COUNT(product_key) as total_products
FROM product_segment
GROUP BY cost_range
ORDER BY total_products DESC
----------------------------------------------------------------------------------------
/* Group customers into 3 segments based on thier spending behavior:
	- VIP: Customers with at least 12 months of history and spending more than $5,000.
	- Regular: Customers with at least 12 months of history but spending $5,000 or less.
	- New: Customers with a lifespan less than 12 months.
And find the total number of customers in each group.
*/

WITH customer_spending AS (
	SELECT
	  c.customer_key,
	  SUM(f.sales_amount) AS total_spending,
	  MIN(order_date) AS first_order,
	  MAX(order_date) AS last_order,
	  DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS lifespan
	FROM gold.fact_sales f
	LEFT JOIN gold.dim_customers c
	ON f.customer_key = c.customer_key
	GROUP BY c.customer_key
)
SELECT
  customer_segment,
  COUNT(customer_key) as customer_count
FROM (
	SELECT
	  customer_key,
	  CASE
		WHEN lifespan > 12 AND total_spending > 5000 THEN 'VIP'
		WHEN lifespan > 12 AND total_spending <= 5000 THEN 'Regular'
		ELSE 'New'
	  END customer_segment
	FROM customer_spending
) t
GROUP BY customer_segment
ORDER BY customer_count DESC