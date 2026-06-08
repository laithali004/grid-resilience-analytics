# Databricks Run Checklist

## First Pipeline Run

1. Import the GitHub repository into Databricks Repos.
2. Run `utilities/Run me first.py`.
3. Upload one EAGLE-I yearly CSV first, such as `eaglei_outages_2014.csv`, to the printed Volume path.
4. Create a Databricks Pipeline with these source files:
   - `transformations/bronze_outage_ingest.py`
   - `transformations/silver_outage_transform.py`
   - `transformations/gold_outage_features.py`
5. Run the pipeline and verify:
   - `outages_bronze` exists
   - `outages_silver` exists
   - `outage_county_day_features` exists
   - `fips_code` values keep leading zeros
   - `customers_out` is populated from source column `sum`

## Model Run

1. Run `explorations/Outage Model Performance Analysis.py`.
2. Register the best model in Unity Catalog.
3. Set the pipeline config value `outage.model_uri` to the registered model URI.
4. Add `transformations/gold_outage_risk_scores.py` to the pipeline.
5. Add `transformations/gold_outage_aggregations.sql` after scoring is available.
6. Re-run the pipeline and verify `outage_county_risk_scores`.

## Full Data Run

1. Upload the remaining yearly EAGLE-I CSV files.
2. Re-run the pipeline.
3. Check row counts by source file and year.
4. Build the dashboard from the gold feature, score, summary, and ranking tables.
