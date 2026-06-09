# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer: Grid Resilience Risk Scores
# MAGIC
# MAGIC ## Purpose
# MAGIC Apply a registered MLflow scikit-learn model to gold county-day features and
# MAGIC publish dashboard-ready outage risk scores.
# MAGIC
# MAGIC ## Expected Output
# MAGIC Delta streaming table: `gold_county_risk_scores`
# MAGIC
# MAGIC Run `ml/Train and Evaluate Outage Risk Model.py` first to train and
# MAGIC register a model, then set the `outage.model_uri` pipeline configuration.

# COMMAND ----------

import pandas as pd
import mlflow.sklearn

from pyspark import pipelines as dp
from pyspark.sql.functions import lit, pandas_udf

# COMMAND ----------

try:
    model_uri = spark.conf.get("outage.model_uri")
except Exception:
    model_uri = "models:/workspace.default.outage_risk_model/1"

feature_cols = [
    "outage_observations",
    "avg_customers_out",
    "max_customers_out",
    "total_customers_out",
]

_risk_model = None


@pandas_udf("double")
def predict_risk_probability(
    outage_observations: pd.Series,
    avg_customers_out: pd.Series,
    max_customers_out: pd.Series,
    total_customers_out: pd.Series,
) -> pd.Series:
    global _risk_model

    if _risk_model is None:
        _risk_model = mlflow.sklearn.load_model(model_uri)

    batch = pd.DataFrame(
        {
            "outage_observations": outage_observations,
            "avg_customers_out": avg_customers_out,
            "max_customers_out": max_customers_out,
            "total_customers_out": total_customers_out,
        }
    )

    if hasattr(_risk_model, "predict_proba"):
        return pd.Series(_risk_model.predict_proba(batch)[:, 1])

    return pd.Series(_risk_model.predict(batch).astype(float))


dp.create_streaming_table(
    name="gold_county_risk_scores",
    comment="Gold table with model-scored county-day outage risk probabilities",
)

# COMMAND ----------


@dp.append_flow(target="gold_county_risk_scores", name="gold_county_risk_scores_flow")
def gold_county_risk_scores_flow():
    features = spark.readStream.table("gold_county_day_features")

    try:
        return (
            features.withColumn(
                "risk_probability",
                predict_risk_probability(
                    "outage_observations",
                    "avg_customers_out",
                    "max_customers_out",
                    "total_customers_out",
                ),
            )
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
