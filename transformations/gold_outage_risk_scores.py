# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer: Grid Resilience Risk Scores
# MAGIC
# MAGIC ## Purpose
# MAGIC Apply a registered MLflow model to gold county-day features and publish
# MAGIC dashboard-ready outage risk scores.
# MAGIC
# MAGIC ## Expected Output
# MAGIC Delta streaming table: `outage_county_risk_scores`
# MAGIC
# MAGIC Run `explorations/Outage Model Performance Analysis.py` first to train and
# MAGIC register a model, then set the `outage.model_uri` pipeline configuration.

# COMMAND ----------

import mlflow
from pyspark import pipelines as dp
from pyspark.ml.functions import vector_to_array
from pyspark.sql.functions import col, lit

# COMMAND ----------

model_uri = spark.conf.get(
    "outage.model_uri",
    "models:/workspace.default.outage_risk_model/1",
)

dp.create_streaming_table(
    name="outage_county_risk_scores",
    comment="Gold table with model-scored county-day outage risk probabilities",
)

# COMMAND ----------


@dp.append_flow(target="outage_county_risk_scores", name="outage_county_risk_scores_flow")
def outage_county_risk_scores_flow():
    features = spark.readStream.table("outage_county_day_features")

    try:
        model = mlflow.spark.load_model(model_uri)
        scored = model.transform(features)

        return (
            scored.withColumn("risk_probability", vector_to_array("probability")[1])
            .withColumn("model_uri", lit(model_uri))
            .select(
                "event_date",
                "fips_code",
                "state",
                "county",
                "outage_observations",
                "avg_customers_out",
                "max_customers_out",
                "total_customers_out",
                "major_outage",
                "risk_probability",
                "model_uri",
            )
        )
    except Exception:
        return (
            features.withColumn("risk_probability", lit(None).cast("double"))
            .withColumn("model_uri", lit("model_not_registered"))
            .select(
                "event_date",
                "fips_code",
                "state",
                "county",
                "outage_observations",
                "avg_customers_out",
                "max_customers_out",
                "total_customers_out",
                "major_outage",
                "risk_probability",
                "model_uri",
            )
        )
