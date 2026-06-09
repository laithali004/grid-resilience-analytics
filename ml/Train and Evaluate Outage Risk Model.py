# Databricks notebook source
# MAGIC %md
# MAGIC # Outage Model Training and Evaluation
# MAGIC
# MAGIC ## Purpose
# MAGIC Train and evaluate baseline outage risk classifiers using the gold feature table.
# MAGIC The model uses current county-day outage features to predict whether that same
# MAGIC county has a major outage on the next observed day.
# MAGIC This notebook uses scikit-learn because Databricks Free/serverless environments
# MAGIC may block some Spark ML constructors.

# COMMAND ----------

import mlflow
import mlflow.sklearn
import matplotlib.pyplot as plt
import pandas as pd

from mlflow.models import infer_signature
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

feature_cols = [
    "outage_observations",
    "avg_customers_out",
    "max_customers_out",
    "total_customers_out",
]
target_col = "next_day_major_outage"
thresholds = [0.05, 0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90, 0.95]

features_pdf = (
    spark.read.table(feature_table)
    .select("event_date", "fips_code", *(feature_cols + ["major_outage"]))
    .dropna()
    .toPandas()
)

features_pdf["event_date"] = pd.to_datetime(features_pdf["event_date"])
features_pdf = features_pdf.sort_values(["fips_code", "event_date"])
features_pdf[target_col] = (
    features_pdf.groupby("fips_code")["major_outage"].shift(-1)
)
features_pdf = features_pdf.dropna(subset=[target_col])
features_pdf[target_col] = features_pdf[target_col].astype(int)

X = features_pdf[feature_cols]
y = features_pdf[target_col]

print(f"Training rows: {len(features_pdf):,}")
print("Target distribution:")
display(features_pdf[target_col].value_counts().rename_axis(target_col).reset_index(name="rows"))

X_train, X_test, y_train, y_test = train_test_split(
    X,
    y,
    test_size=0.2,
    random_state=261,
    stratify=y,
)

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
    "random_forest": RandomForestClassifier(
        n_estimators=150,
        max_depth=10,
        min_samples_leaf=5,
        class_weight="balanced",
        random_state=261,
        n_jobs=1,
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
}

# COMMAND ----------

for model_name, model in candidate_models.items():
    with mlflow.start_run(run_name=f"outage_{model_name}") as run:
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
        mlflow.log_param("prediction_target", target_col)
        mlflow.log_param("feature_cols", ",".join(feature_cols))
        mlflow.log_param("selected_threshold", best_threshold["threshold"])
        mlflow.log_metric("roc_auc", roc_auc)
        mlflow.log_metric("average_precision", average_precision)
        mlflow.log_metric("f1", f1)
        mlflow.log_metric("precision", precision)
        mlflow.log_metric("recall", recall)
        mlflow.log_metric("best_threshold_f1", best_threshold["f1"])
        mlflow.log_metric("best_threshold_precision", best_threshold["precision"])
        mlflow.log_metric("best_threshold_recall", best_threshold["recall"])

        threshold_artifact = f"threshold_metrics_{model_name}.csv"
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

        print(
            model_name,
            {
                "roc_auc": roc_auc,
                "average_precision": average_precision,
                "f1": f1,
                "precision": precision,
                "recall": recall,
                "best_threshold": best_threshold["threshold"],
                "best_threshold_f1": best_threshold["f1"],
                "best_threshold_precision": best_threshold["precision"],
                "best_threshold_recall": best_threshold["recall"],
            },
        )
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
                    "y_pred": y_pred,
                    "y_prob": y_prob,
                }
            )

# COMMAND ----------

report = classification_report(
    y_test,
    best["y_pred"],
    target_names=["No Major Outage", "Major Outage"],
    output_dict=True,
    zero_division=0,
)

report_df = pd.DataFrame(report).transpose()
display(report_df)

cm = confusion_matrix(y_test, best["y_pred"])
disp = ConfusionMatrixDisplay(
    confusion_matrix=cm,
    display_labels=["No Major Outage", "Major Outage"],
)
disp.plot()
plt.title("Grid Resilience Model Confusion Matrix")
plt.savefig("confusion_matrix.png", bbox_inches="tight")
plt.show()

# COMMAND ----------

with mlflow.start_run(run_name="outage_best_model_summary"):
    mlflow.log_param("best_model_name", best["name"])
    mlflow.log_param("best_model_run_id", best["run_id"])
    mlflow.log_param("selected_threshold", best["selected_threshold"])
    mlflow.log_metric("best_roc_auc", best["roc_auc"])
    mlflow.log_metric("best_average_precision", best["average_precision"])
    mlflow.log_metric("best_threshold_f1", best["best_threshold_f1"])
    mlflow.log_artifact("confusion_matrix.png")

model_uri = f"runs:/{best['run_id']}/model"

print(f"Best model: {best['name']}")
print(f"Best model run ID: {best['run_id']}")
print(f"Best model URI: {model_uri}")
print(f"Selected review threshold: {best['selected_threshold']}")

print("\nManual Unity Catalog registration steps:")
print("1. Open the MLflow run linked above.")
print("2. In Artifacts, open the `model` folder.")
print("3. Click Register model.")
print(f"4. Register it as: {registered_model_name}")
print("5. After registration, copy the model version number.")
print(f"6. Set pipeline config outage.model_uri = models:/{registered_model_name}/<version>")
