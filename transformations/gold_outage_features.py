# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer: County-Day Risk Features
# MAGIC
# MAGIC ## Purpose
# MAGIC Aggregate cleaned outage records into county-day features for dashboarding
# MAGIC and binary outage risk modeling.
# MAGIC
# MAGIC ## Expected Output
# MAGIC Delta streaming table: `outage_county_day_features`

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import avg, col, count, max, sum

# COMMAND ----------

major_outage_threshold = int(spark.conf.get("outage.major_outage_threshold", "5000"))

dp.create_streaming_table(
    name="outage_county_day_features",
    comment="Gold table with county-day outage risk features and binary major outage label",
)

# COMMAND ----------


@dp.append_flow(target="outage_county_day_features")
@dp.expect_or_drop("valid_feature_state", "state IS NOT NULL")
@dp.expect_or_drop("valid_feature_county", "county IS NOT NULL")
@dp.expect_or_drop("valid_feature_fips", "fips_code IS NOT NULL")
def outage_county_day_features_flow():
    df = spark.readStream.table("outages_silver")

    return (
        df.groupBy("event_date", "fips_code", "state", "county")
        .agg(
            count("*").alias("outage_observations"),
            avg("customers_out").alias("avg_customers_out"),
            max("customers_out").alias("max_customers_out"),
            sum("customers_out").alias("total_customers_out"),
            avg("latitude").alias("avg_latitude"),
            avg("longitude").alias("avg_longitude"),
        )
        .withColumn(
            "major_outage",
            (col("max_customers_out") >= major_outage_threshold).cast("int"),
        )
    )
