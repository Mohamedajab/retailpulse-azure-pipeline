-- Queries used to validate Power BI measures against the curated sales table.
SELECT month, ROUND(SUM(revenue), 2) AS monthly_revenue
FROM sales_detail
GROUP BY month
ORDER BY month;

SELECT product_name, SUM(quantity) AS units_sold, ROUND(SUM(revenue), 2) AS revenue
FROM sales_detail
GROUP BY product_name
ORDER BY units_sold DESC;

SELECT customer_id, customer_name, COUNT(DISTINCT order_id) AS order_count
FROM sales_detail
GROUP BY customer_id, customer_name
HAVING COUNT(DISTINCT order_id) > 1
ORDER BY order_count DESC;

