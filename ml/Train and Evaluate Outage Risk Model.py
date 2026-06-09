# Databricks notebook source
# MAGIC %md
# MAGIC # Outage Model Training and Evaluation
# MAGIC
# MAGIC ## Purpose
# MAGIC Train and evaluate baseline outage risk classifiers using the gold feature table.
# MAGIC The model uses current and recent county-day outage features to predict whether
# MAGIC that same county has a major outage on the next observed day.
# MAGIC This notebook uses scikit-learn because Databricks Free/serverless environments
# MAGIC may block some Spark ML constructors.

# COMMAND ----------

import mlflow
import mlflow.sklearn
import matplotlib.pyplot as plt
import pandas as pd
import tempfile

from mlflow.models import infer_signature
from sklearn.base import clone
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    ConfusionMatrixDisplay,
    average_precision_score,
    classification_report,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")


def get_spark_conf(name: str, default: str) -> str:
    try:
        return spark.conf.get(name)
    except Exception:
        return default


feature_table = get_spark_conf(
    "outage.feature_table",
    "workspace.default.gold_county_day_features",
)
registered_model_name = get_spark_conf(
    "outage.registered_model_name",
    "workspace.default.outage_risk_model",
)

base_feature_cols = [
    "outage_observations",
    "avg_customers_out",
    "max_customers_out",
    "total_customers_out",
]
target_col = "next_day_major_outage"
thresholds = [0.05, 0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90, 0.95]

features_pdf = (
    spark.read.table(feature_table)
    .select("event_date", "fips_code", *(base_feature_cols + ["major_outage"]))
    .dropna()
    .toPandas()
)


def add_temporal_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["event_date"] = pd.to_datetime(df["event_date"])
    df = df.sort_values(["fips_code", "event_date"])
    county_group = df.groupby("fips_code", group_keys=False)

    df["event_month"] = df["event_date"].dt.month
    df["event_day_of_week"] = df["event_date"].dt.dayofweek
    df["previous_major_outage"] = county_group["major_outage"].shift(1)
    df["previous_max_customers_out"] = county_group["max_customers_out"].shift(1)
    df["previous_avg_customers_out"] = county_group["avg_customers_out"].shift(1)
    df["previous_total_customers_out"] = county_group["total_customers_out"].shift(1)
    df["previous_outage_observations"] = county_group["outage_observations"].shift(1)

    df["rolling_3_day_max_customers_out"] = county_group["max_customers_out"].transform(
        lambda values: values.shift(1).rolling(3, min_periods=1).max()
    )
    df["rolling_3_day_avg_customers_out"] = county_group["avg_customers_out"].transform(
        lambda values: values.shift(1).rolling(3, min_periods=1).mean()
    )
    df["rolling_3_day_major_outages"] = county_group["major_outage"].transform(
        lambda values: values.shift(1).rolling(3, min_periods=1).sum()
    )
    df["rolling_7_day_max_customers_out"] = county_group["max_customers_out"].transform(
        lambda values: values.shift(1).rolling(7, min_periods=1).max()
    )
    df["rolling_7_day_avg_customers_out"] = county_group["avg_customers_out"].transform(
        lambda values: values.shift(1).rolling(7, min_periods=1).mean()
    )
    df["rolling_7_day_major_outages"] = county_group["major_outage"].transform(
        lambda values: values.shift(1).rolling(7, min_periods=1).sum()
    )
    df["days_since_previous_observation"] = county_group["event_date"].diff().dt.days
    df[target_col] = county_group["major_outage"].shift(-1)

    return df


features_pdf = add_temporal_features(features_pdf)

temporal_feature_cols = [
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
feature_cols = base_feature_cols + temporal_feature_cols

features_pdf["event_date"] = pd.to_datetime(features_pdf["event_date"])
features_pdf = features_pdf.dropna(subset=[target_col])
features_pdf[target_col] = features_pdf[target_col].astype(int)
features_pdf[temporal_feature_cols] = features_pdf[temporal_feature_cols].fillna(0)

X = features_pdf[feature_cols]
y = features_pdf[target_col]

print(f"Training rows: {len(features_pdf):,}")
print("Target distribution:")
display(features_pdf[target_col].value_counts().rename_axis(target_col).reset_index(name="rows"))

feature_sets = {
    "outage_only": base_feature_cols,
    "temporal_history": feature_cols,
}

unique_dates = sorted(features_pdf["event_date"].drop_duplicates())
split_date = unique_dates[int(len(unique_dates) * 0.8)]
train_pdf = features_pdf[features_pdf["event_date"] < split_date]
test_pdf = features_pdf[features_pdf["event_date"] >= split_date]
validation_strategy = "time_based_holdout"

if train_pdf[target_col].nunique() < 2 or test_pdf[target_col].nunique() < 2:
    train_pdf, test_pdf = train_test_split(
        features_pdf,
        test_size=0.2,
        random_state=261,
        stratify=features_pdf[target_col],
    )
    validation_strategy = "stratified_random_fallback"
    split_date = None

print(f"Validation strategy: {validation_strategy}")
print(f"Split date: {split_date}")
print(f"Train rows: {len(train_pdf):,}")
print(f"Test rows: {len(test_pdf):,}")

candidate_models = {
    "logistic_regression": Pipeline(
        steps=[
            ("scaler", StandardScaler()),
            (
                "model",
                LogisticRegression(
                    max_iter=1000,
                    class_weight="balanced",
                    random_state=261,
                ),
            ),
        ]
    ),
    "random_forest": Pipeline(
        steps=[
            (
                "model",
                RandomForestClassifier(
                    n_estimators=150,
                    max_depth=10,
                    min_samples_leaf=5,
                    class_weight="balanced",
                    random_state=261,
                    n_jobs=1,
                ),
            ),
        ]
    ),
}

def build_threshold_metrics(y_true, y_prob, threshold_values):
    rows = []

    for threshold in threshold_values:
        y_pred_at_threshold = (y_prob >= threshold).astype(int)
        true_positives = int(((y_pred_at_threshold == 1) & (y_true == 1)).sum())
        false_positives = int(((y_pred_at_threshold == 1) & (y_true == 0)).sum())

        rows.append(
            {
                "threshold": threshold,
                "precision": precision_score(y_true, y_pred_at_threshold, zero_division=0),
                "recall": recall_score(y_true, y_pred_at_threshold, zero_division=0),
                "f1": f1_score(y_true, y_pred_at_threshold, zero_division=0),
                "flagged_count": int(y_pred_at_threshold.sum()),
                "true_positives": true_positives,
                "false_positives": false_positives,
            }
        )

    return pd.DataFrame(rows)


best = {
    "name": None,
    "roc_auc": -1,
    "average_precision": -1,
    "best_threshold_f1": -1,
    "selected_threshold": None,
    "run_id": None,
    "model": None,
    "feature_set": None,
    "feature_cols": None,
    "y_test": None,
}
model_results = []

# COMMAND ----------

for feature_set_name, candidate_feature_cols in feature_sets.items():
    X_train = train_pdf[candidate_feature_cols]
    y_train = train_pdf[target_col]
    X_test = test_pdf[candidate_feature_cols]
    y_test = test_pdf[target_col]

    for model_name, model_template in candidate_models.items():
        model = clone(model_template)
        run_name = f"outage_{feature_set_name}_{model_name}"

        with mlflow.start_run(run_name=run_name) as run:
            model.fit(X_train, y_train)

            y_pred = model.predict(X_test)
            y_prob = model.predict_proba(X_test)[:, 1]

            roc_auc = roc_auc_score(y_test, y_prob)
            average_precision = average_precision_score(y_test, y_prob)
            f1 = f1_score(y_test, y_pred, zero_division=0)
            precision = precision_score(y_test, y_pred, zero_division=0)
            recall = recall_score(y_test, y_pred, zero_division=0)
            threshold_df = build_threshold_metrics(y_test.to_numpy(), y_prob, thresholds)
            best_threshold = threshold_df.sort_values(
                ["f1", "precision", "recall"],
                ascending=False,
            ).iloc[0]

            mlflow.log_param("model_name", model_name)
            mlflow.log_param("feature_set", feature_set_name)
            mlflow.log_param("validation_strategy", validation_strategy)
            mlflow.log_param("split_date", str(split_date))
            mlflow.log_param("prediction_target", target_col)
            mlflow.log_param("feature_cols", ",".join(candidate_feature_cols))
            mlflow.log_param("selected_threshold", best_threshold["threshold"])
            mlflow.log_metric("roc_auc", roc_auc)
            mlflow.log_metric("average_precision", average_precision)
            mlflow.log_metric("f1", f1)
            mlflow.log_metric("precision", precision)
            mlflow.log_metric("recall", recall)
            mlflow.log_metric("best_threshold_f1", best_threshold["f1"])
            mlflow.log_metric("best_threshold_precision", best_threshold["precision"])
            mlflow.log_metric("best_threshold_recall", best_threshold["recall"])

            threshold_artifact = (
                f"{tempfile.gettempdir()}/"
                f"threshold_metrics_{feature_set_name}_{model_name}.csv"
            )
            threshold_df.to_csv(threshold_artifact, index=False)
            mlflow.log_artifact(threshold_artifact)

            input_example = X_train.head(5)
            signature = infer_signature(
                input_example,
                model.predict_proba(input_example)[:, 1],
            )
            mlflow.sklearn.log_model(
                model,
                artifact_path="model",
                input_example=input_example,
                signature=signature,
            )

            result = {
                "feature_set": feature_set_name,
                "model_name": model_name,
                "roc_auc": roc_auc,
                "average_precision": average_precision,
                "f1": f1,
                "precision": precision,
                "recall": recall,
                "best_threshold": best_threshold["threshold"],
                "best_threshold_f1": best_threshold["f1"],
                "best_threshold_precision": best_threshold["precision"],
                "best_threshold_recall": best_threshold["recall"],
                "run_id": run.info.run_id,
            }
            model_results.append(result)

            print(run_name, result)
            display(threshold_df)

            if best_threshold["f1"] > best["best_threshold_f1"]:
                best.update(
                    {
                        "name": model_name,
                        "roc_auc": roc_auc,
                        "average_precision": average_precision,
                        "best_threshold_f1": best_threshold["f1"],
                        "selected_threshold": best_threshold["threshold"],
                        "run_id": run.info.run_id,
                        "model": model,
                        "feature_set": feature_set_name,
                        "feature_cols": candidate_feature_cols,
                        "y_test": y_test,
                        "y_pred": y_pred,
                        "y_prob": y_prob,
                    }
                )

display(pd.DataFrame(model_results).sort_values("best_threshold_f1", ascending=False))

# COMMAND ----------

report = classification_report(
    best["y_test"],
    best["y_pred"],
    target_names=["No Major Outage", "Major Outage"],
    output_dict=True,
    zero_division=0,
)

report_df = pd.DataFrame(report).transpose()
display(report_df)

cm = confusion_matrix(best["y_test"], best["y_pred"])
disp = ConfusionMatrixDisplay(
    confusion_matrix=cm,
    display_labels=["No Major Outage", "Major Outage"],
)
disp.plot()
plt.title("Grid Resilience Model Confusion Matrix")
confusion_matrix_path = f"{tempfile.gettempdir()}/confusion_matrix.png"
plt.savefig(confusion_matrix_path, bbox_inches="tight")
plt.show()

# COMMAND ----------

with mlflow.start_run(run_name="outage_best_model_summary"):
    mlflow.log_param("best_model_name", best["name"])
    mlflow.log_param("best_feature_set", best["feature_set"])
    mlflow.log_param("best_model_run_id", best["run_id"])
    mlflow.log_param("selected_threshold", best["selected_threshold"])
    mlflow.log_metric("best_roc_auc", best["roc_auc"])
    mlflow.log_metric("best_average_precision", best["average_precision"])
    mlflow.log_metric("best_threshold_f1", best["best_threshold_f1"])
    mlflow.log_artifact(confusion_matrix_path)

model_uri = f"runs:/{best['run_id']}/model"

print(f"Best model: {best['name']}")
print(f"Best feature set: {best['feature_set']}")
print(f"Best model run ID: {best['run_id']}")
print(f"Best model URI: {model_uri}")
print(f"Selected review threshold: {best['selected_threshold']}")

if best["feature_set"] != "outage_only":
    print(
        "\nBefore using this model in the scoring pipeline, update "
        "`gold_outage_risk_scores.py` to create the same temporal feature columns."
    )

print("\nManual Unity Catalog registration steps:")
print("1. Open the MLflow run linked above.")
print("2. In Artifacts, open the `model` folder.")
print("3. Click Register model.")
print(f"4. Register it as: {registered_model_name}")
print("5. After registration, copy the model version number.")
print(f"6. Set pipeline config outage.model_uri = models:/{registered_model_name}/<version>")
print("7. Set pipeline config outage.enable_model_scoring = true")
print(f"8. Set pipeline config outage.high_risk_threshold = {best['selected_threshold']}")
