-- 1. Create Database if not exists
CREATE DATABASE IF NOT EXISTS ecommerce_dw;

USE ecommerce_dw;

-- 2. Drop table if it exists to ensure clean creation
DROP TABLE IF EXISTS streaming_orders;

-- 3. Create External Table mapped directly to HDFS Parquet storage
CREATE EXTERNAL TABLE streaming_orders (
    order_id INT,
    customer_id INT,
    product_id INT,
    quantity INT,
    price DOUBLE,
    total_amount DOUBLE,
    order_timestamp TIMESTAMP
)
STORED AS PARQUET
LOCATION '/user/hive/warehouse/ecommerce_dw.db/streaming_orders';

-- ==========================================
-- 6 Analytical OLAP Queries
-- ==========================================

-- Query 1: Total number of processed orders
SELECT COUNT(*) AS total_orders 
FROM streaming_orders;

-- Query 2: Total gross revenue
SELECT SUM(total_amount) AS total_gross_revenue 
FROM streaming_orders;

-- Query 3: Average order value (AOV)
SELECT AVG(total_amount) AS average_order_value 
FROM streaming_orders;

-- Query 4: Total sales broken down by product_id
SELECT product_id, SUM(total_amount) AS product_sales, SUM(quantity) AS total_units_sold
FROM streaming_orders
GROUP BY product_id
ORDER BY product_sales DESC;

-- Query 5: Total expenditure per customer_id
SELECT customer_id, SUM(total_amount) AS customer_total_spend, COUNT(order_id) AS order_count
FROM streaming_orders
GROUP BY customer_id
ORDER BY customer_total_spend DESC;

-- Query 6: Top 5 selling products ranked by gross revenue
SELECT product_id, SUM(total_amount) AS gross_revenue
FROM streaming_orders
GROUP BY product_id
ORDER BY gross_revenue DESC
LIMIT 5;
