# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer: Grid Resilience Risk Scores
# MAGIC
# MAGIC ## Purpose
# MAGIC Apply a registered MLflow scikit-learn model to gold county-day features and
# MAGIC publish dashboard-ready outage risk scores.
# MAGIC
# MAGIC ## Expected Output
# MAGIC Materialized view: `gold_county_risk_scores`
# MAGIC
# MAGIC Run `ml/Train and Evaluate Outage Risk Model.py` first to train and
# MAGIC register a model, then set the `outage.model_uri` pipeline configuration.

# COMMAND ----------

import pandas as pd
import mlflow
import mlflow.sklearn

from pyspark.sql import Window
from pyspark import pipelines as dp
from pyspark.sql.functions import (
    avg,
    col,
    coalesce,
    datediff,
    dayofweek,
    lag,
    lit,
    max,
    month,
    pandas_udf,
    sum,
)

# COMMAND ----------

mlflow.set_tracking_uri("databricks")
mlflow.set_registry_uri("databricks-uc")

try:
    model_uri = spark.conf.get("outage.model_uri")
except Exception:
    model_uri = ""

try:
    enable_model_scoring = spark.conf.get("outage.enable_model_scoring").lower() == "true"
except Exception:
    enable_model_scoring = False

try:
    high_risk_threshold = float(spark.conf.get("outage.high_risk_threshold"))
except Exception:
    high_risk_threshold = 0.70

feature_cols = [
    "outage_observations",
    "avg_customers_out",
    "max_customers_out",
    "total_customers_out",
    "event_month",
    "event_day_of_week",
    "previous_major_outage",
    "previous_max_customers_out",
    "previous_avg_customers_out",
    "previous_total_customers_out",
    "previous_outage_observations",
    "rolling_3_day_max_customers_out",
    "rolling_3_day_avg_customers_out",
    "rolling_3_day_major_outages",
    "rolling_7_day_max_customers_out",
    "rolling_7_day_avg_customers_out",
    "rolling_7_day_major_outages",
    "days_since_previous_observation",
]

temporal_feature_cols = feature_cols[4:]

_risk_model = None


def patch_sklearn_tree_compatibility(model):
    """Handle minor scikit-learn tree attribute differences across Databricks runtimes."""
    estimators = getattr(model, "estimators_", [])

    for estimator in estimators:
        if not hasattr(estimator, "monotonic_cst"):
            estimator.monotonic_cst = None

    return model


@pandas_udf("double")
def predict_risk_probability(
    outage_observations: pd.Series,
    avg_customers_out: pd.Series,
    max_customers_out: pd.Series,
    total_customers_out: pd.Series,
    event_month: pd.Series,
    event_day_of_week: pd.Series,
    previous_major_outage: pd.Series,
    previous_max_customers_out: pd.Series,
    previous_avg_customers_out: pd.Series,
    previous_total_customers_out: pd.Series,
    previous_outage_observations: pd.Series,
    rolling_3_day_max_customers_out: pd.Series,
    rolling_3_day_avg_customers_out: pd.Series,
    rolling_3_day_major_outages: pd.Series,
    rolling_7_day_max_customers_out: pd.Series,
    rolling_7_day_avg_customers_out: pd.Series,
    rolling_7_day_major_outages: pd.Series,
    days_since_previous_observation: pd.Series,
) -> pd.Series:
    global _risk_model

    if _risk_model is None:
        mlflow.set_tracking_uri("databricks")
        mlflow.set_registry_uri("databricks-uc")
        _risk_model = patch_sklearn_tree_compatibility(
            mlflow.sklearn.load_model(model_uri)
        )

    batch = pd.DataFrame(
        {
            "outage_observations": outage_observations,
            "avg_customers_out": avg_customers_out,
            "max_customers_out": max_customers_out,
            "total_customers_out": total_customers_out,
            "event_month": event_month,
            "event_day_of_week": event_day_of_week,
            "previous_major_outage": previous_major_outage,
            "previous_max_customers_out": previous_max_customers_out,
            "previous_avg_customers_out": previous_avg_customers_out,
            "previous_total_customers_out": previous_total_customers_out,
            "previous_outage_observations": previous_outage_observations,
            "rolling_3_day_max_customers_out": rolling_3_day_max_customers_out,
            "rolling_3_day_avg_customers_out": rolling_3_day_avg_customers_out,
            "rolling_3_day_major_outages": rolling_3_day_major_outages,
            "rolling_7_day_max_customers_out": rolling_7_day_max_customers_out,
            "rolling_7_day_avg_customers_out": rolling_7_day_avg_customers_out,
            "rolling_7_day_major_outages": rolling_7_day_major_outages,
            "days_since_previous_observation": days_since_previous_observation,
        }
    )

    if hasattr(_risk_model, "predict_proba"):
        return pd.Series(_risk_model.predict_proba(batch)[:, 1])

    return pd.Series(_risk_model.predict(batch).astype(float))


def add_temporal_scoring_features(features):
    county_window = Window.partitionBy("fips_code").orderBy("event_date")
    rolling_3_day_window = county_window.rowsBetween(-3, -1)
    rolling_7_day_window = county_window.rowsBetween(-7, -1)

    return (
        features.withColumn("event_month", month("event_date"))
        .withColumn("event_day_of_week", (dayofweek("event_date") + lit(5)) % lit(7))
        .withColumn("previous_major_outage", lag("major_outage").over(county_window))
        .withColumn("previous_max_customers_out", lag("max_customers_out").over(county_window))
        .withColumn("previous_avg_customers_out", lag("avg_customers_out").over(county_window))
        .withColumn("previous_total_customers_out", lag("total_customers_out").over(county_window))
        .withColumn("previous_outage_observations", lag("outage_observations").over(county_window))
        .withColumn(
            "rolling_3_day_max_customers_out",
            max("max_customers_out").over(rolling_3_day_window),
        )
        .withColumn(
            "rolling_3_day_avg_customers_out",
            avg("avg_customers_out").over(rolling_3_day_window),
        )
        .withColumn(
            "rolling_3_day_major_outages",
            sum("major_outage").over(rolling_3_day_window),
        )
        .withColumn(
            "rolling_7_day_max_customers_out",
            max("max_customers_out").over(rolling_7_day_window),
        )
        .withColumn(
            "rolling_7_day_avg_customers_out",
            avg("avg_customers_out").over(rolling_7_day_window),
        )
        .withColumn(
            "rolling_7_day_major_outages",
            sum("major_outage").over(rolling_7_day_window),
        )
        .withColumn(
            "days_since_previous_observation",
            datediff("event_date", lag("event_date").over(county_window)),
        )
    )


@dp.table(
    name="gold_county_risk_scores",
    comment="Gold table with temporal outage features and model-scored risk probabilities",
)
def gold_county_risk_scores():
    features = add_temporal_scoring_features(spark.read.table("LIVE.gold_county_day_features"))

    for feature_col in temporal_feature_cols:
        features = features.withColumn(feature_col, coalesce(col(feature_col), lit(0)))

    if enable_model_scoring and model_uri:
        return (
            features.withColumn(
                "risk_probability",
                predict_risk_probability(*[col(feature_col) for feature_col in feature_cols]),
            )
            .withColumn("model_uri", lit(model_uri))
            .withColumn(
                "high_risk_flag",
                (col("risk_probability") >= lit(high_risk_threshold)).cast("int"),
            )
            .select(
                "event_date",
                "fips_code",
                "state",
                "county",
                *feature_cols,
                "major_outage",
                "risk_probability",
                "high_risk_flag",
                "model_uri",
            )
        )

    return (
        features.withColumn("risk_probability", lit(None).cast("double"))
        .withColumn("high_risk_flag", lit(None).cast("int"))
        .withColumn("model_uri", lit("model_scoring_disabled"))
        .select(
            "event_date",
            "fips_code",
            "state",
            "county",
            *feature_cols,
            "major_outage",
            "risk_probability",
            "high_risk_flag",
            "model_uri",
        )
    )
