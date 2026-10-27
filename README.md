# Olist E-Commerce Lakehouse Pipeline

**Databricks • PySpark • Spark SQL • Delta Lake • Unity Catalog • Auto Loader • Lakeflow Jobs • AI/BI Dashboards**

A data engineering portfolio project built around the public Olist Brazilian e-commerce dataset. The project covers nine source CSVs (about 1.55 million rows combined, including geolocation), the Bronze–Silver–Gold medallion architecture, data-quality checks, an incremental ingestion demonstration, and a dashboard refreshed in a five-task Databricks workflow.

## Pipeline orchestration

![Five successful Lakeflow tasks](Screenshot%202026-10-27%20154152.png)

The workflow screenshot documents these **five successful tasks**:

`bronze_ingestion → silver_transformations → data_quality → gold_transformations → refresh_olist_dashboard`

> **Implementation note:** Task success confirms the job completed. The Auto Loader demo writes to `olist_project.bronze.orders_incremental` while the Silver orders notebook reads `olist_project.bronze.orders`. This means the incremental test flow is not yet connected end to end to the dashboard.

## Dashboard

[**View the dashboard PDF**](OLIST%20ECOMMERCE%20DASHBOARD%202026-10-27%2020_51.pdf)

The dashboard reports six executive metrics: **total revenue**, **total orders**, **average order value**, **average review score**, **late delivery rate**, and **average delivery time**. Additional visuals explore monthly sales trends, state-level orders, category performance, top sellers, review scores, and delivery patterns.

The dashboard uses historical sample data; its calculated figures have not been independently audited.

## Architecture

```text
Olist CSV files (9 datasets)
       |
       v
Unity Catalog Volume
       |
       v
Bronze Delta tables (batch ingestion)
       |
       v
Silver (cleaning, standardization, data types)
       |
       v
Data-quality checks
       |
       v
Gold (dimensions, fact table, aggregates)
       |
       v
Databricks AI/BI Dashboard
       ^
       |
Lakeflow Jobs orchestrates notebook tasks and dashboard refresh

Separate demo: staged order files -> Auto Loader -> bronze.orders_incremental
```

## Project files

These links point to files **actually present** in this repository:

| Component | Source |
| --- | --- |
| Bronze setup | [Olist Bronze Layer Setup.sql](Olist%20Bronze%20Layer%20Setup.sql) |
| Bronze batch ingestion | [Olist Bronze Ingestion.py](Olist%20Bronze%20Ingestion.py) |
| Silver transformations | [Olist Silver Transformations.ipynb](Olist%20Silver%20Transformations.ipynb) |
| Data-quality checks | [Olist Data Quality Tests.py](Olist%20Data%20Quality%20Tests.py) |
| Gold modeling | [Gold SQL notebook](New%20Notebook%202026-10-17%2011_46_12.sql) |
| Auto Loader test | [05_incremental_ingestion.py](05_incremental_ingestion.py) |
| Dashboard | [Dashboard PDF](OLIST%20ECOMMERCE%20DASHBOARD%202026-10-27%2020_51.pdf) |
| Successful workflow | [Lakeflow Jobs screenshot](Screenshot%202026-10-27%20154152.png) |

## What the code does

- **Bronze:** Reads CSVs into Delta tables with ingestion timestamp and source-file metadata.
- **Silver:** Cleans customer/seller geography, order timestamps and delivery fields, product category names, order-item monetary fields, and review-score types.
- **Data quality:** Tests order ID uniqueness, non-null order IDs, and nonnegative prices.
- **Gold:** Creates customer/product/seller dimensions, order-item fact data, and monthly/category/seller aggregates.
- **Incremental ingestion demonstration:** Uses `cloudFiles`, separate staging/incoming paths, schema tracking, and checkpointing to demonstrate loading newly arriving order CSV batches.

## Reproduce

1. Download the [Olist Brazilian E-Commerce public dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).
2. Upload nine CSVs to a Databricks Volume at `/Volumes/olist_project/raw/olist_files/`.
3. Run the Bronze setup and batch-ingestion notebooks.
4. Run Silver transformations, then the data-quality notebook.
5. Run the Gold notebook to create dimensional and aggregate tables.
6. Build the AI/BI dashboard from Gold data and configure a five-task Lakeflow Job with a dashboard refresh at the end.
7. Run `05_incremental_ingestion.py` **separately** to see Auto Loader file discovery and checkpoint behavior. It is a demonstration notebook, not a repeatable production job.

## Engineering improvements to implement next

1. **Connect incremental updates downstream:** make the incremental Bronze stream feed the same logical input consumed by Silver, then verify Gold and dashboard results change when a new order batch arrives.
2. **Prevent fact-table duplication:** the Gold SQL joins order items directly to reviews; multiple review rows per order can inflate item-level sums. Aggregate or deduplicate reviews at order grain first.
3. **Validate customer dimension grain:** `customer_unique_id` and state should be modeled so each fact joins to an appropriate unique geography record.
4. **Strengthen quality tests:** include source-to-target counts, referential integrity, review-score validity, and uniqueness on `(order_id, order_item_id)`.
5. **Separate demo from production:** remove manual batch release from job notebooks and avoid overwriting live Bronze history.

## Dataset credit

Data: [Brazilian E-Commerce Public Dataset by Olist on Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce). Personal learning/portfolio project; not affiliated with Olist.
