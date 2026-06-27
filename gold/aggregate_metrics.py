# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — customer order metrics
# MAGIC Run the Silver notebook before this notebook.

# COMMAND ----------

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("orders-medallion-demo").getOrCreate()

BASE_PATH = "/tmp/data-platform-blueprints"
SILVER_PATH = f"{BASE_PATH}/silver/orders"
GOLD_PATH = f"{BASE_PATH}/gold/customer_metrics"

# COMMAND ----------

def build_customer_metrics(silver_df: DataFrame) -> DataFrame:
    """Create reporting metrics from completed orders."""
    return (
        silver_df
        .filter(F.col("status") == "completed")
        .groupBy("customer_id")
        .agg(
            F.count("order_id").alias("order_count"),
            F.sum("amount").cast("decimal(14,2)").alias("total_revenue"),
            F.avg("amount").cast("decimal(14,2)").alias("average_order_value"),
            F.max("order_date").alias("last_order_date"),
        )
        .orderBy("customer_id")
    )

# COMMAND ----------

silver_df = spark.read.format("delta").load(SILVER_PATH)
gold_df = build_customer_metrics(silver_df)
gold_df.write.format("delta").mode("overwrite").save(GOLD_PATH)

print(f"Gold customer rows: {gold_df.count()}")
display(gold_df)
