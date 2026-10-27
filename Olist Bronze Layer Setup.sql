-- Databricks notebook source
CREATE CATALOG IF NOT EXISTS olist_project;
USE CATALOG olist_project;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS raw;
CREATE VOLUME IF NOT EXISTS olist_project.raw.olist_files;


-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(
-- MAGIC     dbutils.fs.ls(
-- MAGIC         "/Volumes/olist_project/raw/olist_files/"
-- MAGIC     )
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC customers_df = (
-- MAGIC     spark.read
-- MAGIC     .format("csv")
-- MAGIC     .option("header", "true")
-- MAGIC     .option("inferSchema", "true")
-- MAGIC     .load(
-- MAGIC         "/Volumes/olist_project/raw/olist_files/olist_customers_dataset.csv"
-- MAGIC     )
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(customers_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC customers_df.printSchema()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import current_timestamp, input_file_name
-- MAGIC
-- MAGIC customers_bronze = (
-- MAGIC     customers_df
-- MAGIC     .withColumn("_ingested_at", current_timestamp())
-- MAGIC     .withColumn("_source_file", input_file_name())
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col, current_timestamp
-- MAGIC
-- MAGIC (
-- MAGIC     customers_df
-- MAGIC     .withColumn("_ingested_at", current_timestamp())
-- MAGIC     .withColumn("_source_file", col("_metadata.file_path"))
-- MAGIC     .write
-- MAGIC     .format("delta")
-- MAGIC     .mode("overwrite")
-- MAGIC     .saveAsTable("olist_project.bronze.customers")
-- MAGIC )

-- COMMAND ----------

SELECT *
FROM olist_project.bronze.customers
LIMIT 10;