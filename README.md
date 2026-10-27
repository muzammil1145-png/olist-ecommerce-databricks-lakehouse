# Olist E-Commerce Lakehouse | Databricks

**End-to-end e-commerce data engineering and business intelligence portfolio project** using Databricks, PySpark, Spark SQL, Delta Lake, Unity Catalog, Auto Loader, Lakeflow Jobs, and AI/BI Dashboards.

![Lakeflow orchestration](screenshots/lakeflow_orchestration.png)

## Overview

This project uses the public **Olist Brazilian E-Commerce** dataset (nine related CSVs, roughly 1.55 million source records including geolocation rows). Data is ingested to Bronze Delta tables, cleaned in Silver, modeled for analytics in Gold, and displayed in an AI/BI dashboard.

**Pipeline:** CSVs in Unity Catalog Volume → Bronze → Silver → Data Quality → Gold → AI/BI Dashboard Refresh

The attached orchestration capture shows five successfully executed Databricks Job tasks. Successful execution verifies task completion, not necessarily that incremental test-table updates are propagated to all downstream tables; see limitations below.

## Dashboard

![Olist dashboard](screenshots/olist_dashboard.png)

The submitted dashboard includes six KPIs: total revenue, total orders, average order value, average review score, late delivery rate, and average delivery time. Visuals examine monthly sales, state-level orders, category sales, top sellers, reviews, and delivery performance.

Dashboard values are exploratory outputs of this specific implementation, not independently audited business totals.

## Repository structure

- `notebooks/01_bronze_setup.sql` — catalog, schemas, Volume, exploratory setup
- `notebooks/02_bronze_ingestion.py` — initial batch ingestion of source CSVs
- `notebooks/03_silver_transformations.ipynb` — cleaned entities and type conversions
- `notebooks/04_data_quality.py` — assertions for IDs and nonnegative prices
- `notebooks/05_gold_model.sql` — dimensions, fact, and aggregate tables
- `notebooks/06_incremental_ingestion_demo.py` — incremental Auto Loader demonstration
- `screenshots/` — orchestration and dashboard evidence
- `docs/` — architecture diagram and documentation

## Pipeline design

### Bronze
Imports Olist CSV files into Delta tables. Ingestion metadata includes ingestion timestamp and source path.

### Silver
Standardizes customer and seller state fields, casts purchase/delivery timestamps and monetary columns, enriches product categories, and casts review scores safely.

### Gold
Builds customer, product, and seller dimensions; order-item fact data; and monthly, category, and seller performance aggregates for reporting.

### Orchestration
Lakeflow Jobs tasks: `bronze_ingestion` → `silver_transformations` → `data_quality` → `gold_transformations` → `refresh_olist_dashboard`.

## Incremental ingestion demonstration

The Auto Loader notebook creates staged historical order batches, releases them into an incoming directory, uses `cloudFiles` with checkpoint/schema tracking, and writes to `olist_project.bronze.orders_incremental`.

**Current limitation:** The regular Silver orders notebook reads `olist_project.bronze.orders`, not `orders_incremental`. The two paths must be integrated before the project can claim end-to-end incremental updates through Gold and the dashboard. The notebook is a **demo** and re-creates/moves test batches; it should not be used unchanged as a scheduled production ingestion job.

## Data-model caveats and next improvements

- Review data is associated with orders; joining review rows to order items can multiply sales or affect review averages if multiple reviews exist per order. Aggregate reviews to one row per order before joining and validate fact grain.
- `dim_customer` uses distinct customer identifiers and geography and may not be unique per `customer_unique_id`. Prefer a stable key/grain for location-based joins.
- Current Bronze batch ingestion uses overwrite mode. Separate initial backfill from append-only or idempotent production ingestion.
- Add data-quality tests for unique composite order-item keys, referential integrity, and source-to-target row counts.
- Keep raw data and credentials out of the repository.

## Reproduce

1. Obtain the **Brazilian E-Commerce Public Dataset by Olist** from Kaggle.
2. Upload the nine CSVs into a Databricks Unity Catalog Volume at `/Volumes/olist_project/raw/olist_files/`.
3. Run the setup and Bronze ingestion notebooks.
4. Run Silver transformations and data-quality checks.
5. Run the Gold SQL model and configure a Databricks AI/BI Dashboard against Gold.
6. Set up a Lakeflow Job with notebook task dependencies and a published dashboard refresh task.
7. Run the Auto Loader demo separately to experiment with arrival of new files.

## Dataset and attribution

Source: [Brazilian E-Commerce Public Dataset by Olist (Kaggle)](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).

This project is a personal portfolio demonstration using a public historical dataset, not a live Olist production system.
