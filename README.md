# Data Platform Blueprints
Reference implementations for building scalable data platforms using Medallion Architecture.

## Pipeline Structure
- `bronze/`: Raw data ingestion (landing).
- `silver/`: Cleaned, validated, and conformed data.
- `gold/`: Aggregated data for business reporting.

## Example: Bronze to Silver Transformation
```python
# bronze_to_silver.py
from pyspark.sql import functions as F

def process_bronze_to_silver(input_path, output_path):
    df = spark.read.table("raw_table")
    # Apply data quality rules
    silver_df = df.filter(F.col("id").isNotNull()).dropDuplicates()
    silver_df.write.format("delta").mode("overwrite").save(output_path)
