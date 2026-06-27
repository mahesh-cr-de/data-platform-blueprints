# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — ingest raw orders
# MAGIC Run this notebook first. It creates demo source data and lands it as Delta
# MAGIC without changing the source values.

# COMMAND ----------

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("orders-medallion-demo").getOrCreate()

# Change this to a writable location in your environment.
BASE_PATH = "/tmp/data-platform-blueprints"
RAW_PATH = f"{BASE_PATH}/raw/orders"
BRONZE_PATH = f"{BASE_PATH}/bronze/orders"

# COMMAND ----------

# A small source dataset containing valid, duplicate, and invalid rows.
demo_orders = [
    ("O-1001", "c-001", "2026-06-20", "120.50", "Completed"),
    ("O-1002", "c-002", "2026-06-21", "75.00", "completed"),
    ("O-1003", "c-001", "2026-06-22", "49.50", "COMPLETED"),
    ("O-1004", "c-003", "2026-06-23", "20.00", "pending"),
    ("O-1004", "c-003", "2026-06-23", "20.00", "pending"),
    ("O-1005", "c-004", "not-a-date", "30.00", "completed"),
    ("O-1006", "c-002", "2026-06-24", "-5.00", "completed"),
]
columns = ["order_id", "customer_id", "order_date", "amount", "status"]

spark.createDataFrame(demo_orders, columns).write.format("delta").mode("overwrite").save(RAW_PATH)

# COMMAND ----------

def ingest_to_bronze(source_path: str, target_path: str) -> DataFrame:
    """Land source rows in Bronze and add ingestion metadata."""
    raw_df = spark.read.format("delta").load(source_path)
    bronze_df = (
        raw_df
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source", F.lit(source_path))
    )
    bronze_df.write.format("delta").mode("overwrite").save(target_path)
    return bronze_df


bronze_df = ingest_to_bronze(RAW_PATH, BRONZE_PATH)
display(bronze_df)  # In non-Databricks notebooks, replace display() with show().
