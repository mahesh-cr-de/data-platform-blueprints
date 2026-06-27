# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — clean and validate orders
# MAGIC Run the Bronze notebook before this notebook.

# COMMAND ----------

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("orders-medallion-demo").getOrCreate()

BASE_PATH = "/tmp/data-platform-blueprints"
BRONZE_PATH = f"{BASE_PATH}/bronze/orders"
SILVER_PATH = f"{BASE_PATH}/silver/orders"
REJECTED_PATH = f"{BASE_PATH}/silver/rejected_orders"

# COMMAND ----------

def transform_bronze(bronze_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return conformed orders and quarantined records with rejection reasons."""
    prepared = (
        bronze_df
        .withColumn("order_id", F.trim("order_id"))
        .withColumn("customer_id", F.upper(F.trim("customer_id")))
        .withColumn("parsed_order_date", F.to_date(F.trim("order_date"), "yyyy-MM-dd"))
        .withColumn("parsed_amount", F.trim("amount").cast("decimal(12,2)"))
        .withColumn("status", F.lower(F.trim("status")))
        .withColumn(
            "rejection_reason",
            F.when(F.col("order_id").isNull() | (F.col("order_id") == ""), "missing order_id")
            .when(F.col("customer_id").isNull() | (F.col("customer_id") == ""), "missing customer_id")
            .when(F.col("parsed_order_date").isNull(), "invalid order_date")
            .when(F.col("parsed_amount").isNull(), "invalid amount")
            .when(F.col("parsed_amount") < 0, "negative amount")
            .when(~F.col("status").isin("completed", "pending", "cancelled"), "invalid status")
        )
    )

    invalid_df = prepared.filter(F.col("rejection_reason").isNotNull())
    valid_df = prepared.filter(F.col("rejection_reason").isNull())

    # Keep the first ingested row for each business key and quarantine later copies.
    ranked = valid_df.withColumn(
        "_row_number",
        F.row_number().over(Window.partitionBy("order_id").orderBy("_ingested_at")),
    )
    duplicates = (
        ranked.filter(F.col("_row_number") > 1)
        .drop("_row_number", "rejection_reason")
        .withColumn("rejection_reason", F.lit("duplicate order_id"))
    )

    silver_df = (
        ranked.filter(F.col("_row_number") == 1)
        .select(
            "order_id",
            "customer_id",
            F.col("parsed_order_date").alias("order_date"),
            F.col("parsed_amount").alias("amount"),
            "status",
            "_ingested_at",
        )
    )
    rejected_df = invalid_df.unionByName(duplicates, allowMissingColumns=True)
    return silver_df, rejected_df

# COMMAND ----------

bronze_df = spark.read.format("delta").load(BRONZE_PATH)
silver_df, rejected_df = transform_bronze(bronze_df)

silver_df.write.format("delta").mode("overwrite").save(SILVER_PATH)
rejected_df.write.format("delta").mode("overwrite").save(REJECTED_PATH)

print(f"Silver rows: {silver_df.count()}; rejected rows: {rejected_df.count()}")
display(silver_df)
display(rejected_df.select("order_id", "rejection_reason"))
