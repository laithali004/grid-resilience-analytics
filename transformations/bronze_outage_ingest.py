# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer: Raw Outage Ingestion
# MAGIC
# MAGIC ## Purpose
# MAGIC Ingest raw outage CSV files using CloudFiles Auto Loader.
# MAGIC Preserve source fields and add metadata columns for lineage.
# MAGIC
# MAGIC ## Expected Output
# MAGIC Delta streaming table: `outages_bronze`

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import LongType, StringType, StructField, StructType
from pyspark import pipelines as dp

# COMMAND ----------

source_path = spark.conf.get(
    "outage.source_path",
    "/Volumes/workspace/default/outage_risk/raw/outages/",
)

schema_location = spark.conf.get(
    "outage.schema_location",
    "/Volumes/workspace/default/outage_risk/checkpoints/bronze_schema/",
)

eaglei_schema = StructType(
    [
        StructField("fips_code", StringType(), True),
        StructField("county", StringType(), True),
        StructField("state", StringType(), True),
        StructField("sum", LongType(), True),
        StructField("run_start_time", StringType(), True),
    ]
)

dp.create_streaming_table(
    name="outages_bronze",
    comment="Bronze streaming table containing raw outage records and ingestion metadata",
)

# COMMAND ----------


@dp.append_flow(target="outages_bronze", name="outages_bronze_flow")
def outages_bronze_flow():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("rescuedDataColumn", "_rescued_data")
        .option("header", True)
        .schema(eaglei_schema)
        .load(source_path)
        .select(
            "*",
            col("_metadata.file_path").cast("string").alias("source_file"),
            current_timestamp().cast("string").alias("processing_time"),
        )
    )
