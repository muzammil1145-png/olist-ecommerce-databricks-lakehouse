# Databricks notebook source
# DBTITLE 1,Test Order IDs
orders = spark.table(
    "olist_project.silver.orders"
)

total = orders.count()

distinct_orders = (
    orders.select("order_id")
    .distinct()
    .count()
)

assert total == distinct_orders

# COMMAND ----------

# DBTITLE 1,Test Null IDs
from pyspark.sql.functions import col

null_orders = (
    orders
    .filter(col ('order_id').isNull())
    .count()
)

assert null_orders == 0

# COMMAND ----------

# DBTITLE 1,Test Item Prices
items = spark.table(
    "olist_project.silver.order_items"
)

invalid_prices = (
    items
    .filter(col("price") < 0)
    .count()
)

assert invalid_prices == 0