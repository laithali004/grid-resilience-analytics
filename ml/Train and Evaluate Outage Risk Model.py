# Databricks notebook source
# MAGIC %md
# MAGIC # Outage Model Training and Evaluation
# MAGIC
# MAGIC ## Purpose
# MAGIC Train and evaluate baseline outage risk classifiers using the gold feature table.
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
label_col = "major_outage"

features_pdf = (
    spark.read.table(feature_table)
    .select(*(feature_cols + [label_col]))
    .dropna()
    .toPandas()
)

X = features_pdf[feature_cols]
y = features_pdf[label_col].astype(int)

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

best = {"name": None, "roc_auc": -1, "run_id": None, "model": None}

# COMMAND ----------

for model_name, model in candidate_models.items():
    with mlflow.start_run(run_name=f"outage_{model_name}") as run:
        model.fit(X_train, y_train)

        y_pred = model.predict(X_test)
        y_prob = model.predict_proba(X_test)[:, 1]

        roc_auc = roc_auc_score(y_test, y_prob)
        f1 = f1_score(y_test, y_pred, zero_division=0)
        precision = precision_score(y_test, y_pred, zero_division=0)
        recall = recall_score(y_test, y_pred, zero_division=0)

        mlflow.log_param("model_name", model_name)
        mlflow.log_param("feature_cols", ",".join(feature_cols))
        mlflow.log_metric("roc_auc", roc_auc)
        mlflow.log_metric("f1", f1)
        mlflow.log_metric("precision", precision)
        mlflow.log_metric("recall", recall)

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
                "f1": f1,
                "precision": precision,
                "recall": recall,
            },
        )

        if roc_auc > best["roc_auc"]:
            best.update(
                {
                    "name": model_name,
                    "roc_auc": roc_auc,
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
    mlflow.log_metric("best_roc_auc", best["roc_auc"])
    mlflow.log_artifact("confusion_matrix.png")

model_uri = f"runs:/{best['run_id']}/model"
registered_model = mlflow.register_model(model_uri, registered_model_name)

print(f"Best model: {best['name']}")
print(f"Registered model: {registered_model.name}, version {registered_model.version}")
print(f"Set pipeline config outage.model_uri = models:/{registered_model.name}/{registered_model.version}")
