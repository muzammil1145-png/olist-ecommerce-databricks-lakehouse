# Databricks notebook source
from pyspark.sql.functions import current_timestamp, col

base_path = "/Volumes/olist_project/raw/olist_files/"

def ingest_csv(file_name, table_name):

    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(base_path + file_name)
    )

    df = (
        df
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(
            f"olist_project.bronze.{table_name}"
        )
    )

# COMMAND ----------

# DBTITLE 1,Load Olist Marketplace CSV Datasets into Dataframes
ingest_csv(
    "olist_customers_dataset.csv",
    "customers"
)

ingest_csv(
    "olist_orders_dataset.csv",
    "orders"
)

ingest_csv(
    "olist_order_items_dataset.csv",
    "order_items"
)

ingest_csv(
    "olist_order_payments_dataset.csv",
    "payments"
)

ingest_csv(
    "olist_order_reviews_dataset.csv",
    "reviews"
)

ingest_csv(
    "olist_products_dataset.csv",
    "products"
)

ingest_csv(
    "olist_sellers_dataset.csv",
    "sellers"
)

ingest_csv(
    "olist_geolocation_dataset.csv",
    "geolocation"
)

ingest_csv(
    "product_category_name_translation.csv",
    "category_translation"
)