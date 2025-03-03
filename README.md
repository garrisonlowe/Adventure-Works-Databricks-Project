# SQL Adventure Works Project

This project leverages Databricks to perform exploratory data analysis on the Adventure Works sales dataset. The following outline details the project structure, methodology, and key findings.

## Project Overview

A comprehensive analysis of Adventure Works sales data to extract valuable business insights and patterns using SQL queries within the Databricks environment.

### Dataset Description

The Adventure Works dataset includes:

- Sales - Fact
- Customers - Dimension
- Products - Dimensions
    - The dataset spans multiple years and contains millions of records across related tables.

### Analysis Methods and Techniques

Key areas of analysis include:

- **Change over time analysis**
    - Detailed breakdown of sales performance across different territories and regions.
- **Cumulative analysis**
    - Analysis of best-selling and underperforming product categories and subcategories.
- **Performance analysis**
    - Identification of customer segments and their purchasing behaviors.
- **Part-to-Whole analysis**
    - Examination of sales fluctuations across different time periods.
- **Customer segmentation analysis**
    - Comparison of different sales channels and their contribution to overall revenue.

### Reports

- **Customer Report**
    - This report consolidates key customer metrics and behaviors
- **Product Report**
    - This report consolidates key product metrics and behaviors.

### SQL Techniques Used

- Complex joins across multiple tables
- Window functions for time-series analysis
- CTEs for modular query structure
- Aggregation functions for summarizing data
- Subqueries for nested analyses
- Case statements for conditional logic

### Visualizations

This project incorporates various visualization techniques from Databricks to represent the findings:

- Bar charts for categorical comparisons
- Line graphs for trend analysis
- Pie charts for market share representation

### Conclusions

Summary of key insights and business recommendations derived from the analysis.

# Analysis

### Change Over Time Analysis

- Analyze the data over time.
- This query provides a monthly and yearly breakdown of sales performance, including total sales, number of customers, AND quantity sold.

```sql
SELECT
  YEAR (order_date) AS order_year,
  MONTH (order_date) AS order_month,
  SUM(sales_amount) AS total_sales,
  COUNT(DISTINCT customer_key) AS total_customers,
  SUM(quantity) AS total_quantity
FROM
  gold_fact_sales
WHERE
  order_date IS NOT NULL
GROUP BY
  YEAR (order_date),
  MONTH (order_date)
ORDER BY
  YEAR (order_date),
  MONTH (order_date)
```

**Sales by Year**

![Image](https://github.com/user-attachments/assets/892ebec8-8e76-4b8b-8f91-2a55ca4d0416)

**Customers by Year**

![Image](https://github.com/user-attachments/assets/892ebec8-8e76-4b8b-8f91-2a55ca4d0416)

**Quantity by Year**

![Image](https://github.com/user-attachments/assets/343a0086-4f74-4dd8-84ef-a0bf2142eea7)

Another way of writing this using the date_trunc function. Full sales visualization.

```sql
SELECT
  CAST(date_trunc('MONTH', order_date) AS DATE) AS order_date,
  SUM(sales_amount) AS total_sales,
  COUNT(DISTINCT customer_key) AS total_customers,
  SUM(quantity) AS total_quantity
FROM
  gold_fact_sales
WHERE
  order_date IS NOT NULL
GROUP BY
  CAST(date_trunc('MONTH', order_date) AS DATE)
ORDER BY
  CAST(date_trunc('MONTH', order_date) AS DATE)
```

**Sales by Month and Year**

![Image](https://github.com/user-attachments/assets/c7b591e9-118b-492e-a5fb-436cea849629)

### Cumulative Analysis

- Another way of writing this using the date_trunc function. Full sales visualization.

This query provides a monthly breakdown of sales performance, including total sales AND running total for that Year.

```sql
SELECT
  order_date,
  total_sales,
  SUM(total_sales) OVER (PARTITION BY YEAR(order_date) ORDER BY order_date) AS running_total_sales
FROM
(
 SELECT
   CAST(DATE_TRUNC('MONTH', order_date) AS DATE) AS order_date,
   SUM(sales_amount) AS total_sales
 FROM
   gold_fact_sales
 WHERE
   order_date IS NOT NULL
 GROUP BY
   CAST(DATE_TRUNC('MONTH', order_date) AS DATE)
) AS monthly_sales
```

This query provides a yearly breakdown of sales performance, including total sales, running total, AND moving average price.

```sql
SELECT
  order_date,
  total_sales,
  SUM(total_sales) OVER (ORDER BY order_date) AS running_total_sales,
  AVG(avg_price) OVER (ORDER BY order_date) AS moving_avg_price
FROM
(
 SELECT
   CAST(DATE_TRUNC('YEAR', order_date) AS DATE) AS order_date,
   SUM(sales_amount) AS total_sales,
   AVG(price) AS avg_price
 FROM
   gold_fact_sales
 WHERE
   order_date IS NOT NULL
 GROUP BY
   CAST(DATE_TRUNC('YEAR', order_date) AS DATE)
) AS monthly_sales
```

**Sales and Moving Average Price by Year**

![Image](https://github.com/user-attachments/assets/cdbe2640-d5ab-4519-8344-80a317757fbb)

### Performance Analysis

- Comparing the current value to a target value.
- Helps measure success and compare performance.
- Analyze the yearly performance of products by comparing each product's sales to both its average sales performance and the previous year's sales.

Main query; can use to create new queries to investigate further.

```sql
WITH yearly_product_sales AS 
(
	SELECT
      YEAR(f.order_date) AS order_year,
      p.product_name,
      SUM(f.sales_amount) AS current_sales
    FROM gold_fact_sales f
      LEFT JOIN gold_dim_products p 
	  ON f.product_key = p.product_key
    WHERE f.order_date IS NOT NULL
    GROUP BY YEAR(f.order_date), p.product_name
)
SELECT
  order_year,
  product_name,
  current_sales,
  AVG(current_sales) OVER (PARTITION BY product_name) AS avg_sales,
  current_sales - AVG(current_sales) OVER (PARTITION BY product_name) AS diff_avg,
  CASE 
	  WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) > 0 THEN 'Above Avg'
	  WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) < 0 THEN 'Below Avg'
	ELSE 'Avg'
  END AS avg_change,
 -- Year-over-Year Analysis
  LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS py_sales,
  current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS diff_py,
  CASE 
	  WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) > 0 THEN 'Increase'
	  WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) < 0 THEN 'Decrease'
	ELSE 'No Change'
  END AS py_change
FROM yearly_product_sales
ORDER BY product_name, order_year;
```

Seeing the number of products that have increased vs decreased in sales in 2013 vs 2012. The vast majority of products increased in sales from the previous year.

```sql
WITH yearly_product_sales AS 
(
	SELECT
      YEAR(f.order_date) AS order_year,
      p.product_name,
      SUM(f.sales_amount) AS current_sales
    FROM gold_fact_sales f
      LEFT JOIN gold_dim_products p 
	  ON f.product_key = p.product_key
    WHERE f.order_date IS NOT NULL
    GROUP BY YEAR(f.order_date), p.product_name
)
SELECT
  py_change,
  COUNT(py_change)
FROM (
  SELECT
    order_year,
    product_name,
    current_sales,
    AVG(current_sales) OVER (PARTITION BY product_name) AS avg_sales,
    current_sales - AVG(current_sales) OVER (PARTITION BY product_name) AS diff_avg,
    CASE 
      WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) > 0 THEN 'Above Avg'
      WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) < 0 THEN 'Below Avg'
    ELSE 'Avg'
    END AS avg_change,
  -- Year-over-Year Analysis
    LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS py_sales,
    current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS diff_py,
    CASE 
      WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) > 0 THEN 'Increase'
      WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) < 0 THEN 'Decrease'
    ELSE 'No Change'
    END AS py_change
  FROM yearly_product_sales
  ORDER BY product_name, order_year
) AS subquery
WHERE diff_py IS NOT NULL
  AND order_year = 2013
GROUP BY py_change
ORDER BY COUNT(py_change) DESC;
```

**Increase vs Decrease from Previous Year**

![Image](https://github.com/user-attachments/assets/be6790ae-9bca-4908-966f-ae339d4dbeba)

### Part-to-Whole Analysis

- Which categories contribute the most to overall sales?

Using the CTE from the Base Query again.

```sql

WITH yearly_product_sales AS 
(
	SELECT
      YEAR(f.order_date) AS order_year,
      p.product_name,
      p.category,
      p.subcategory,
      SUM(f.sales_amount) AS current_sales
    FROM gold_fact_sales f
      LEFT JOIN gold_dim_products p 
	  ON f.product_key = p.product_key
    WHERE f.order_date IS NOT NULL
    GROUP BY YEAR(f.order_date), p.product_name, p.category, p.subcategory
)
SELECT
  category,
  SUM(current_sales) as total_sales
FROM yearly_product_sales
WHERE order_year <> 2010 -- we don't need 2010 for this.
GROUP BY category
ORDER BY total_sales DESC;
```

We can see this shop is completely dependent on Bike sales.

**Sales by Category**

![Image](https://github.com/user-attachments/assets/dadc0b4e-e8ea-40f7-9008-1abd58ef3dbf)
Here is a similar query to above, but depicting the percentage of sales for each category.

```sql
WITH category_sales as (
	SELECT
	category,
	SUM(sales_amount) AS total_sales
	FROM gold_fact_sales f
	LEFT JOIN gold_dim_products p
	ON p.product_key = f.product_key
	GROUP BY category
)
SELECT
  category,
  total_sales,
  SUM(total_sales) OVER () AS overall_sales,
  CONCAT(ROUND((CAST(total_sales AS FLOAT) / SUM(total_sales) OVER ()) * 100, 2), '%') AS percentage_of_total
FROM category_sales
ORDER BY total_sales DESC
```

**Sales by Category**

![Image](https://github.com/user-attachments/assets/22e0133b-ffc1-4fe7-91f2-4314e9f2f180)

### Customer Segmentation Analysis

- Group the data based on a specific range
- Helps understand the correlation between two measures.

Segment products into cost ranges and count hoe many products fall into each segment

```sql
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
	FROM gold_dim_products
)
SELECT
  cost_range,
COUNT(product_key) as total_products
FROM product_segment
GROUP BY cost_range
ORDER BY total_products DESC
```

Majority of products are below 100 dollars. Followed closely by products between 100 and 500 dollars.

**Products by Cost Range**

![Image](https://github.com/user-attachments/assets/13542333-f9b5-4bdd-9b16-eb0a18741b65)

Group customers into 3 segments based on thier spending behavior:

- VIP: Customers with at least 12 months of history and spending more than $5,000.
- Regular: Customers with at least 12 months of history but spending $5,000 or less.
- New: Customers with a lifespan less than 12 months.

```sql
WITH customer_spending AS (
	SELECT
	  c.customer_key,
	  SUM(f.sales_amount) AS total_spending,
	  MIN(order_date) AS first_order,
	  MAX(order_date) AS last_order,
	  DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS lifespan
	FROM gold_fact_sales f
	LEFT JOIN gold_dim_customers c
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
```

**Customer Segment Pie Chart**

![Image](https://github.com/user-attachments/assets/2d7119ef-bc99-4506-8f02-8761a50f760a)

**Customer Segment Bar Chart**

![Image](https://github.com/user-attachments/assets/67bfd5cf-fa76-49d9-8fac-5aa9211b8ee9)

# Reports

### Customer Report

**Purpose:**

- This report consolidates key customer metrics and behaviors

**Highlights:**

1. Gathers essential fields such as names, ages, and transaction details.
2. Segments customers into categories (VIP, Regular, New) and age groups.
3. Aggregates customer-level metrics:
    - total orders
    - total sales
    - total quantity purchased
    - total products
    - lifespan (in months)
4. Calculates valuable KPIs:
    - recency (months since last order)
    - average order value
    - average monthly spend

```sql
-- CREATE VIEW gold.report_customers AS

WITH base_query AS (
	-- Getting core columns from tables
	SELECT
	  f.order_number,
	  f.product_key,
	  f.order_date,
	  f.sales_amount,
	  f.quantity,
	  c.customer_key,
	  c.customer_number,
	  CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
	  DATEDIFF(YEAR, c.birthdate, GETDATE()) AS age
	FROM gold_fact_sales f
	LEFT JOIN gold_dim_customers c
	ON c.customer_key = f.customer_key
	WHERE order_date IS NOT NULL
)
, customer_aggregation AS (
	-- Customer Aggregations
	SELECT
	  customer_key,
	  customer_number,
	  customer_name,
	  age, 
	  COUNT(DISTINCT order_number) as total_orders,
	  SUM(sales_amount) AS total_sales,
	  SUM(quantity) AS total_quantity,
	  COUNT(DISTINCT product_key) AS total_products,
	  MAX(order_date) AS last_order,
	  DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS lifespan
	FROM base_query
	GROUP BY
	  customer_key,
	  customer_number,
	  customer_name,
	  age
)
SELECT
  customer_key,
  customer_number,
  customer_name,
  age,
  CASE
    WHEN age < 20 THEN 'Under 20'
    WHEN age BETWEEN 20 AND 29 THEN '20-29'
    WHEN age BETWEEN 30 AND 39 THEN '30-39'
    WHEN age BETWEEN 40 AND 49 THEN '40-49'
	ELSE '50 and above'
  END AS age_group,
  CASE
    WHEN lifespan > 12 AND total_sales > 5000 THEN 'VIP'
    WHEN lifespan > 12 AND total_sales <= 5000 THEN 'Regular'
	ELSE 'New'
  END AS customer_segment,
  last_order,
  DATEDIFF(MONTH, last_order, GETDATE()) as months_since_last_order,
  total_orders,
  total_sales,
  total_quantity,
  total_products,
  lifespan,
  -- Compute Average Order Value
  CASE
    WHEN total_orders = 0 THEN 0
    ELSE total_sales / total_orders 
  END AS avg_order_value,
  -- Compute Average Monthly Spend
  CASE
    WHEN lifespan = 0 THEN total_sales
    ELSE total_sales / lifespan
  END AS avg_monthly_spend
FROM customer_aggregation
```

### **Product Report**

**Purpose:**

This report consolidates key product metrics and behaviors.

**Highlights:**

- Gathers essential fields such as product name, category, subcategory, and cost.
- Segments products by revenue to identify **High-Performers**, **Mid-Range**, or **Low-Performers**.
- Aggregates product-level metrics:
    - total orders
    - total sales
    - total quantity sold
    - total customers (unique)
    - lifespan (in months)
- Calculates valuable KPIs:
    - recency (months since last sale)
    - average order revenue (AOR)
    - average monthly revenue

```sql
-- IF OBJECT_ID('gold.report_products', 'V') IS NOT NULL
--     DROP VIEW gold.report_products;
-- GO

-- CREATE VIEW gold.report_products AS

WITH base_query AS (
-- Retrieves core columns from fact_sales and dim_products
    SELECT
	  f.order_number,
      f.order_date,
	  f.customer_key,
      f.sales_amount,
      f.quantity,
      p.product_key,
      p.product_name,
      p.category,
      p.subcategory,
      p.cost
    FROM gold_fact_sales f
    LEFT JOIN gold_dim_products p
    ON f.product_key = p.product_key
    WHERE order_date IS NOT NULL
),

product_aggregations AS (
--Product Aggregations: Summarizes key metrics at the product level
SELECT
  product_key,
  product_name,
  category,
  subcategory,
  cost,
  DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS lifespan,
  MAX(order_date) AS last_sale_date,
  COUNT(DISTINCT order_number) AS total_orders,
  COUNT(DISTINCT customer_key) AS total_customers,
  SUM(sales_amount) AS total_sales,
  SUM(quantity) AS total_quantity,
  ROUND(AVG(CAST(sales_amount AS FLOAT) / NULLIF(quantity, 0)),1) AS avg_selling_price
FROM base_query

GROUP BY
    product_key,
    product_name,
    category,
    subcategory,
    cost
)
-- Final Query: Combines all product results into one output
SELECT 
  product_key,
  product_name,
  category,
  subcategory,
  cost,
  last_sale_date,
  DATEDIFF(MONTH, last_sale_date, GETDATE()) AS recency_in_months,
  CASE
    WHEN total_sales > 50000 THEN 'High-Performer' 
    WHEN total_sales >= 10000 THEN 'Mid-Range'
	ELSE 'Low-Performer'
  END AS product_segment,
  lifespan,
  total_orders,
  total_sales,
  total_quantity,
  total_customers,
  avg_selling_price,
  -- Average Order Revenue (AOR)
  CASE 
   	WHEN total_orders = 0 THEN 0
    ELSE total_sales / total_orders
  END AS avg_order_revenue,
  -- Average Monthly Revenue
  CASE
    WHEN lifespan = 0 THEN total_sales
    ELSE total_sales / lifespan
  END AS avg_monthly_revenue
FROM product_aggregations 
```