# Databricks notebook source
# MAGIC %md
# MAGIC # Outage Model Performance Analysis
# MAGIC
# MAGIC ## Purpose
# MAGIC Train and evaluate baseline outage risk classifiers using the gold feature table.
# MAGIC Log metrics and artifacts to MLflow, then register the best model for pipeline scoring.

# COMMAND ----------

import mlflow
import matplotlib.pyplot as plt
import pandas as pd

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from sklearn.metrics import ConfusionMatrixDisplay, classification_report, confusion_matrix

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")

feature_table = spark.conf.get("outage.feature_table", "outage_county_day_features")
registered_model_name = spark.conf.get(
    "outage.registered_model_name",
    "workspace.default.outage_risk_model",
)

df = spark.read.format("delta").table(feature_table).dropna(
    subset=[
        "outage_observations",
        "avg_customers_out",
        "max_customers_out",
        "total_customers_out",
        "major_outage",
    ]
)

feature_cols = [
    "outage_observations",
    "avg_customers_out",
    "max_customers_out",
    "total_customers_out",
]

train_df, test_df = df.randomSplit([0.8, 0.2], seed=261)

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
candidate_models = {
    "logistic_regression": LogisticRegression(featuresCol="features", labelCol="major_outage"),
    "random_forest": RandomForestClassifier(featuresCol="features", labelCol="major_outage", seed=261),
}

binary_eval = BinaryClassificationEvaluator(labelCol="major_outage", metricName="areaUnderROC")
f1_eval = MulticlassClassificationEvaluator(labelCol="major_outage", metricName="f1")

best = {"name": None, "roc_auc": -1, "run_id": None, "model": None}

# COMMAND ----------

for model_name, estimator in candidate_models.items():
    with mlflow.start_run(run_name=f"outage_{model_name}") as run:
        pipeline = Pipeline(stages=[assembler, estimator])
        fitted_model = pipeline.fit(train_df)
        predictions = fitted_model.transform(test_df)

        roc_auc = binary_eval.evaluate(predictions)
        f1 = f1_eval.evaluate(predictions)

        mlflow.log_param("model_name", model_name)
        mlflow.log_param("feature_cols", ",".join(feature_cols))
        mlflow.log_metric("roc_auc", roc_auc)
        mlflow.log_metric("f1", f1)
        mlflow.spark.log_model(fitted_model, artifact_path="model")

        print(model_name, {"roc_auc": roc_auc, "f1": f1})

        if roc_auc > best["roc_auc"]:
            best.update(
                {
                    "name": model_name,
                    "roc_auc": roc_auc,
                    "run_id": run.info.run_id,
                    "model": fitted_model,
                    "predictions": predictions,
                }
            )

# COMMAND ----------

best_predictions = best["predictions"].select("major_outage", "prediction").toPandas()
y_true = best_predictions["major_outage"].astype(int)
y_pred = best_predictions["prediction"].astype(int)

report = classification_report(
    y_true,
    y_pred,
    target_names=["No Major Outage", "Major Outage"],
    output_dict=True,
    zero_division=0,
)

report_df = pd.DataFrame(report).transpose()
display(report_df)

cm = confusion_matrix(y_true, y_pred)
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
