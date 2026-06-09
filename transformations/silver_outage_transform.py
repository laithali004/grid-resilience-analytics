# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer: Outage Cleaning and Standardization
# MAGIC
# MAGIC ## Purpose
# MAGIC Clean raw outage records, normalize county/state fields, cast numeric fields,
# MAGIC parse timestamps, and apply basic data quality expectations.
# MAGIC
# MAGIC ## Expected Output
# MAGIC Delta streaming table: `silver_outages`

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import coalesce, col, initcap, lit, regexp_replace, to_date, to_timestamp, trim, upper

# COMMAND ----------

dp.create_streaming_table(
    name="silver_outages",
    comment="Silver streaming table with standardized outage records",
)

# COMMAND ----------


@dp.append_flow(target="silver_outages", name="silver_outages_flow")
def silver_outages_flow():
    df = spark.readStream.table("bronze_outages")

    def first_existing(names):
        for name in names:
            if name in df.columns:
                return col(f"`{name}`")
        return lit(None)

    raw_fips_code = first_existing(["fips_code", "FIPS", "fips", "county_fips"])
    raw_timestamp = first_existing(
        ["run_start_time", "event_timestamp", "Run Start Time", "timestamp", "date", "Date"]
    )
    raw_state = first_existing(["state", "State", "STATE"])
    raw_county = first_existing(["county", "County", "COUNTY", "county_name"])
    raw_customers_out = first_existing(
        ["customers_out", "Customers Out", "customers_out_count", "outage_count", "sum"]
    )
    raw_latitude = first_existing(["latitude", "Latitude", "lat"])
    raw_longitude = first_existing(["longitude", "Longitude", "lon", "lng"])
    raw_utility = first_existing(["utility", "Utility", "provider", "Provider"])

    cleaned_customers = regexp_replace(raw_customers_out.cast("string"), ",", "")

    return (
        df.withColumn("state", upper(trim(raw_state)))
        .withColumn("fips_code", raw_fips_code.cast("string"))
        .withColumn("county", initcap(trim(raw_county)))
        .withColumn("county", regexp_replace(col("county"), r"\s+County$", ""))
        .withColumn("customers_out", cleaned_customers.cast("long"))
        .withColumn(
            "event_timestamp",
            coalesce(
                to_timestamp(raw_timestamp),
                to_timestamp(raw_timestamp, "yyyy-MM-dd HH:mm:ss"),
                to_timestamp(raw_timestamp, "M/d/yyyy H:mm"),
                to_timestamp(raw_timestamp, "M/d/yyyy"),
            ),
        )
        .withColumn("event_date", to_date(col("event_timestamp")))
        .withColumn("latitude", raw_latitude.cast("double"))
        .withColumn("longitude", raw_longitude.cast("double"))
        .withColumn("utility", raw_utility.cast("string"))
        .filter(col("state").isNotNull())
        .filter(col("county").isNotNull())
        .filter(col("event_date").isNotNull())
        .filter((col("customers_out") >= 0) | col("customers_out").isNull())
        .select(
            "event_timestamp",
            "event_date",
            "fips_code",
            "state",
            "county",
            "customers_out",
            "latitude",
            "longitude",
            "utility",
            "source_file",
            "processing_time",
        )
    )
