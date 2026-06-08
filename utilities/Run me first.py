# Databricks notebook source
# MAGIC %md
# MAGIC # Run Me First
# MAGIC
# MAGIC Use this notebook before running the pipeline. It creates a schema, volume
# MAGIC paths, and project configuration values you can copy into the pipeline settings.

# COMMAND ----------

catalog = "workspace"
schema = "default"
project_volume = "outage_risk"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{project_volume}")

source_path = f"/Volumes/{catalog}/{schema}/{project_volume}/raw/outages/"
schema_location = f"/Volumes/{catalog}/{schema}/{project_volume}/checkpoints/bronze_schema/"

print("Upload raw outage CSV files to:")
print(source_path)

print("\nPipeline configuration:")
print(f"outage.source_path = {source_path}")
print(f"outage.schema_location = {schema_location}")
print("outage.major_outage_threshold = 5000")
print("outage.model_uri = models:/workspace.default.outage_risk_model/1")

print("\nRecommended first run:")
print("Upload one yearly file first, such as eaglei_outages_2014.csv, before loading all years.")
print("After bronze, silver, and the gold feature table succeed, train/register the model and enable scoring.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Suggested Pipeline Source Files
# MAGIC
# MAGIC Add these files to the Databricks Pipeline:
# MAGIC
# MAGIC - `transformations/bronze_outage_ingest.py`
# MAGIC - `transformations/silver_outage_transform.py`
# MAGIC - `transformations/gold_outage_features.py`
# MAGIC - `transformations/gold_outage_risk_scores.py`
# MAGIC - `transformations/gold_outage_aggregations.sql`
