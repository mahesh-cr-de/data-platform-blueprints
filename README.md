# PySpark Medallion Architecture Demo

A simple notebook-oriented pipeline using PySpark and Delta Lake.

## Run the notebooks

Import the Python files as notebooks and execute them in this order:

1. `bronze/ingest_raw.py` creates demo orders and lands the raw Bronze dataset.
2. `silver/clean_data.py` validates, types, normalizes, deduplicates, and quarantines bad rows.
3. `gold/aggregate_metrics.py` builds customer-level metrics from completed orders.

The files use Databricks source-notebook markers (`# COMMAND ----------`) and `display()`. In another PySpark notebook environment, split on those markers and replace `display(df)` with `df.show(truncate=False)`.

## Requirements

- A running Spark session exposed as `spark`
- Delta Lake support
- A writable location configured in `BASE_PATH`

The default output root is `/tmp/data-platform-blueprints`. Change `BASE_PATH` in all three notebooks for DBFS, cloud object storage, Fabric Lakehouse, or another platform-specific location.

## Expected result

The demo creates seven source rows. Silver retains four valid unique orders and quarantines three rows (duplicate ID, invalid date, and negative amount). Gold produces metrics for two customers:

| customer_id | order_count | total_revenue | average_order_value |
|---|---:|---:|---:|
| C-001 | 2 | 170.00 | 85.00 |
| C-002 | 1 | 75.00 | 75.00 |
