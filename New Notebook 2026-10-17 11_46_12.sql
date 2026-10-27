-- Databricks notebook source
-- DBTITLE 1,Create Dim Customer
CREATE OR REPLACE TABLE olist_project.gold.dim_customer AS

SELECT DISTINCT

    customer_unique_id,
    customer_city,
    customer_state

FROM olist_project.silver.customers;

-- COMMAND ----------

-- DBTITLE 1,Create dim_product
CREATE OR REPLACE TABLE olist_project.gold.dim_product AS

SELECT DISTINCT

    product_id,
    category_name,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm

FROM olist_project.silver.products;

-- COMMAND ----------

-- DBTITLE 1,Create dim_seller
CREATE OR REPLACE TABLE olist_project.gold.dim_seller AS

SELECT DISTINCT

    seller_id,
    seller_city,
    seller_state

FROM olist_project.silver.sellers;

-- COMMAND ----------

-- DBTITLE 1,fact table
CREATE OR REPLACE TABLE
olist_project.gold.fact_order_items AS

SELECT

    oi.order_id,
    oi.order_item_id,

    o.customer_id,

    oi.product_id,
    oi.seller_id,

    o.order_purchase_timestamp,

    oi.price,
    oi.freight_value,
    oi.total_item_value,

    o.delivery_days,
    o.delivery_delay_days,
    o.is_late_delivery,

    r.review_score

FROM olist_project.silver.order_items oi

INNER JOIN olist_project.silver.orders o
    ON oi.order_id = o.order_id

LEFT JOIN olist_project.silver.reviews r
    ON oi.order_id = r.order_id;

-- COMMAND ----------

-- DBTITLE 1,Add Customer Information
CREATE OR REPLACE TABLE
olist_project.gold.fact_order_items AS

SELECT

    oi.order_id,
    oi.order_item_id,

    c.customer_unique_id,

    oi.product_id,
    oi.seller_id,

    o.order_purchase_timestamp,

    oi.price,
    oi.freight_value,

    oi.price + oi.freight_value
        AS total_item_value,

    o.delivery_days,
    o.delivery_delay_days,
    o.is_late_delivery,

    r.review_score

FROM olist_project.silver.order_items oi

JOIN olist_project.silver.orders o
    ON oi.order_id = o.order_id

JOIN olist_project.silver.customers c
    ON o.customer_id = c.customer_id

LEFT JOIN olist_project.silver.reviews r
    ON oi.order_id = r.order_id;

-- COMMAND ----------

-- DBTITLE 1,Monthly Sales Gold Table
CREATE OR REPLACE TABLE
olist_project.gold.monthly_sales AS

SELECT

    DATE_TRUNC(
        'MONTH',
        order_purchase_timestamp
    ) AS month,

    COUNT(DISTINCT order_id)
        AS total_orders,

    SUM(price)
        AS product_revenue,

    SUM(freight_value)
        AS freight_revenue,

    SUM(total_item_value)
        AS gross_sales,

    AVG(review_score)
        AS avg_review_score,

    AVG(delivery_days)
        AS avg_delivery_days,

    AVG(is_late_delivery) * 100
        AS late_delivery_rate

FROM olist_project.gold.fact_order_items

GROUP BY 1

ORDER BY 1;

-- COMMAND ----------

-- DBTITLE 1,Category Performance
CREATE OR REPLACE TABLE
olist_project.gold.category_performance AS

SELECT

    p.category_name,

    COUNT(DISTINCT f.order_id)
        AS orders,

    SUM(f.price)
        AS revenue,

    AVG(f.review_score)
        AS avg_review_score,

    AVG(f.delivery_days)
        AS avg_delivery_days

FROM olist_project.gold.fact_order_items f

LEFT JOIN olist_project.gold.dim_product p
    ON f.product_id = p.product_id

GROUP BY p.category_name;

-- COMMAND ----------

-- DBTITLE 1,Seller Performance
CREATE OR REPLACE TABLE
olist_project.gold.seller_performance AS

SELECT

    seller_id,

    COUNT(DISTINCT order_id)
        AS total_orders,

    SUM(price)
        AS revenue,

    AVG(review_score)
        AS avg_review_score,

    AVG(delivery_days)
        AS avg_delivery_days,

    AVG(is_late_delivery) * 100
        AS late_delivery_rate

FROM olist_project.gold.fact_order_items

GROUP BY seller_id;