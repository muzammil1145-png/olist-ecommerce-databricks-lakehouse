# Databricks notebook source
# Base location of your existing Olist files
base_path = "/Volumes/olist_project/raw/olist_files"

# Define incremental ingestion directories
incoming_path = f"{base_path}/incoming_orders"
staging_path = f"{base_path}/staging_orders"

schema_path = f"{base_path}/schemas/orders_incremental"
checkpoint_path = f"{base_path}/checkpoints/orders_incremental"

# Create directories
dbutils.fs.mkdirs(incoming_path)
dbutils.fs.mkdirs(staging_path)
dbutils.fs.mkdirs(schema_path)
dbutils.fs.mkdirs(checkpoint_path)

print("All incremental ingestion directories created successfully!")

# COMMAND ----------

from pyspark.sql.functions import col

orders = (
    spark.table("olist_project.bronze.orders")
    .drop("_ingested_at", "_source_file")
)

print("Total source orders:", orders.count())

# COMMAND ----------

batch_1 = orders.filter(
    col("order_purchase_timestamp") < "2018-01-01"
)

batch_2 = orders.filter(
    col("order_purchase_timestamp") >= "2018-01-01"
)

print("Batch 1:", batch_1.count())
print("Batch 2:", batch_2.count())

# COMMAND ----------

for df, name in [
    (batch_1, "batch_01"),
    (batch_2, "batch_02")
]:
    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .option("quoteAll", "true")
        .option("escape", '"')
        .csv(f"{staging_path}/{name}")
    )

print("Both batches prepared.")

# COMMAND ----------

def release_batch(batch_name):
    source_dir = f"{staging_path}/{batch_name}"

    csv_files = [
        f.path for f in dbutils.fs.ls(source_dir)
        if f.name.startswith("part-")
        and f.name.endswith(".csv")
    ]

    if len(csv_files) != 1:
        raise ValueError("Expected exactly one CSV part file.")

    destination = f"{incoming_path}/{batch_name}.csv"

    dbutils.fs.mv(csv_files[0], destination)

    print(f"Released {batch_name}.csv")

# COMMAND ----------

release_batch("batch_01")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

orders_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", schema_path)
    .option("cloudFiles.inferColumnTypes", "false")
    .option("header", "true")
    .load(incoming_path)
)

# COMMAND ----------

orders_stream = (
    orders_stream
    .withColumn("_source_file", col("_metadata.file_path"))
    .withColumn("_ingested_at", current_timestamp())
)

# COMMAND ----------

query = (
    orders_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .toTable("olist_project.bronze.orders_incremental")
)

query.awaitTermination()

print("First incremental load complete.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_rows
# MAGIC FROM olist_project.bronze.orders_incremental;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     _source_file,
# MAGIC     COUNT(*) AS records_loaded
# MAGIC FROM olist_project.bronze.orders_incremental
# MAGIC GROUP BY _source_file;

# COMMAND ----------

release_batch("batch_02")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     REGEXP_EXTRACT(
# MAGIC         _source_file,
# MAGIC         '(batch_[0-9]+\\.csv)',
# MAGIC         1
# MAGIC     ) AS source_batch,
# MAGIC     COUNT(*) AS rows_loaded
# MAGIC FROM olist_project.bronze.orders_incremental
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1;

# COMMAND ----------

query = (
    orders_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .toTable("olist_project.bronze.orders_incremental")
)

query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     (SELECT COUNT(*)
# MAGIC      FROM olist_project.bronze.orders) AS original_orders,
# MAGIC
# MAGIC     (SELECT COUNT(*)
# MAGIC      FROM olist_project.bronze.orders_incremental) AS incremental_orders;