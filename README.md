# Grid Resilience Analytics

## Overview

This project builds an end-to-end Databricks pipeline for analyzing power outage risk across U.S. counties. The pipeline ingests raw outage records, standardizes location and outage fields, creates county-day risk features, applies machine learning for outage risk scoring, and supports dashboard-ready analytics.

**Core Technologies**: Spark Declarative Pipelines, Delta Lake, MLflow, Unity Catalog, Databricks Dashboards  
**Architecture**: Medallion Architecture (Bronze -> Silver -> Gold -> Application)  
**Primary Use Case**: Grid resilience analytics and county-level outage risk identification

---

## Project Objective

The project is designed to answer:

> Can historical outage data be used to identify counties with elevated risk of major outage events?

The first version focuses on historical outage patterns. Weather data can be added later as an enhancement to improve predictive power.

---

## Data Sources

### Primary Dataset

**ORNL / Constellation EAGLE-I Power Outage Data, 2014-2022**  
Source: https://doi.ccs.ornl.gov/dataset/ccec86f0-e144-5de8-aee0-fb26028b26e1

This dataset contains county-level U.S. power outage information at 15-minute intervals from November 1, 2014 through December 31, 2022. The downloaded yearly CSV files use the fields `fips_code`, `county`, `state`, `sum`, and `run_start_time`.

The source README notes that `sum` represents the total number of utility customers without power in a county at a timestamp. Entries with zero customers without power are not included. The `run_start_time` field is provided in GMT and marks the start of the 15-minute collection run.

The dataset also includes `coverage_history.csv`, which describes the percentage of electric utility customers captured by EAGLE-I for each state and year. This file is useful for interpreting differences in data coverage across states and time.

**License**: Creative Commons Attribution 4.0 International (CC BY 4.0)

**Recommended Citation**: Tansakul, Varisara, et al. (2023). "EAGLE-I Power Outage Data 2014 - 2022" [Data set]. Oak Ridge National Laboratory.

### Optional Supporting Datasets

**DOE OE-417 Annual Electric Disturbance Summaries**  
Source: https://openenergyhub.ornl.gov/explore/dataset/oe-417-annual-summaries/

This dataset can be used to compare outage patterns against larger reported electric disturbance events.

**NOAA Climate Data Online**  
Source: https://www.ncdc.noaa.gov/cdo-web/

NOAA weather data can be added later to include temperature, wind, precipitation, and storm-related variables.

---

## Pipeline Architecture

```text
Raw Outage CSV Files
    |
    v
Bronze Layer -> Raw outage ingestion with metadata
    |
    v
Silver Layer -> Cleaning, timestamp parsing, and county/state standardization
    |
    v
Gold Feature Table -> County-day feature engineering and major outage labels
    |
    v
Gold Prediction Table -> ML risk probabilities for county-day records
    |
    v
Application Layer -> Summaries, rankings, and dashboard tables
    |
    v
Dashboard + MLflow Tracking
```

---

## Bronze Layer: Raw Outage Ingestion

**Purpose**: Ingest raw outage CSV files using CloudFiles Auto Loader.

**Input**: Raw outage CSV files uploaded to a Databricks Volume or DBFS path  
**Output**: Delta streaming table `bronze_outages`

**Functionality**:

1. Read CSV files incrementally with CloudFiles Auto Loader
2. Preserve raw outage fields
3. Enforce the EAGLE-I source schema so FIPS codes remain strings with leading zeros
4. Add metadata columns for lineage:
   - `source_file`
   - `processing_time`
5. Support rescued data handling for unexpected source variation

**Pipeline File**: `transformations/bronze_outage_ingest.py`

---

## Silver Layer: Cleaning and Standardization

**Purpose**: Convert raw outage records into a clean, consistent analytical table.

**Input**: `bronze_outages`  
**Output**: Delta streaming table `silver_outages`

**Functionality**:

1. Standardize state and county fields
2. Parse GMT outage timestamps into `event_timestamp` and `event_date`
3. Convert outage counts into numeric `customers_out`
4. Preserve optional fields such as latitude, longitude, and utility
5. Apply quality expectations for valid dates, counties, states, and outage counts

**Pipeline File**: `transformations/silver_outage_transform.py`

---

## Gold Feature Layer: Feature Engineering

**Purpose**: Aggregate cleaned outage records into county-day features for analytics and modeling.

**Input**: `silver_outages`  
**Output**: Delta streaming table `gold_county_day_features`

**Functionality**:

1. Aggregate outage records by county and date
2. Calculate outage frequency and severity metrics:
   - `outage_observations`
   - `avg_customers_out`
   - `max_customers_out`
   - `total_customers_out`
3. Create a binary `major_outage` label using a configurable threshold
4. Prepare model-ready features for risk prediction

**Pipeline File**: `transformations/gold_outage_features.py`

---

## Gold Prediction Layer: Risk Scoring

**Purpose**: Apply a registered MLflow model to estimate outage risk.

**Input**: `gold_county_day_features`  
**Output**: Delta streaming table `gold_county_risk_scores`

**Functionality**:

1. Load a registered outage risk model from MLflow / Unity Catalog
2. Apply the model to county-day features
3. Extract `risk_probability`
4. Publish dashboard-ready scored records

**Pipeline File**: `transformations/gold_outage_risk_scores.py`

The project separates feature engineering from risk scoring intentionally. The feature table is reusable for exploration, dashboard metrics, and model training. The prediction table can be regenerated when a better model is registered without changing the upstream cleaning and aggregation logic.

---

## Application Layer: Analytics Aggregations

**Purpose**: Create summary tables for dashboard consumption.

**Input**: Gold feature and scoring tables  
**Output**: Materialized views for state summaries and county rankings

**Functionality**:

1. Summarize outage risk by state
2. Rank counties by outage severity and risk probability
3. Support dashboard visualizations without repeatedly recomputing aggregations

**Pipeline File**: `transformations/gold_outage_aggregations.sql`

---

## Model Training and Evaluation

The project includes a model evaluation notebook that trains and compares baseline classifiers.

**Notebook**: `ml/Train and Evaluate Outage Risk Model.py`

**Tracked with MLflow**:

- Model type
- Feature columns
- ROC-AUC
- F1 score
- Confusion matrix artifact
- Registered model version

Initial candidate models:

1. Logistic Regression
2. Random Forest

---

## Data Schemas

### Bronze Schema

| Column | Description |
| --- | --- |
| fips_code | County FIPS code |
| county | County name |
| state | State name |
| sum | Total utility customers without power; zero-outage observations are omitted from the raw data |
| run_start_time | GMT timestamp marking the start of the 15-minute collection run |
| source_file | File path from Auto Loader metadata |
| processing_time | Timestamp when the record was ingested |

### Silver Schema

| Column | Description |
| --- | --- |
| event_timestamp | Parsed outage timestamp |
| event_date | Date extracted from outage timestamp |
| fips_code | County FIPS code |
| state | Standardized state value |
| county | Standardized county name |
| customers_out | Numeric customer outage count derived from `sum` |
| latitude | Optional latitude |
| longitude | Optional longitude |
| utility | Optional utility/provider field |

### Gold Feature Schema

| Column | Description |
| --- | --- |
| event_date | County-day date |
| fips_code | County FIPS code |
| state | State value |
| county | County name |
| outage_observations | Number of outage records for the county-day |
| avg_customers_out | Average customers out |
| max_customers_out | Maximum customers out |
| total_customers_out | Total customers out |
| major_outage | Binary label for major outage event |

### Risk Score Schema

| Column | Description |
| --- | --- |
| risk_probability | Predicted probability of major outage |
| model_uri | MLflow model used for scoring |

---

## Dashboard

The dashboard is designed to show:

1. Total outage records processed
2. Major outage rate over time
3. Top counties by outage severity
4. Top counties by predicted risk probability
5. State-level outage summaries
6. County-level risk ranking table

The dashboard will be created in Databricks after the gold tables and model scores are finalized.

---

## Repository Structure

```text
grid-resilience-analytics/
├── transformations/
│   ├── bronze_outage_ingest.py
│   ├── silver_outage_transform.py
│   ├── gold_outage_features.py
│   ├── gold_outage_risk_scores.py
│   └── gold_outage_aggregations.sql
├── ml/
│   └── Train and Evaluate Outage Risk Model.py
├── utilities/
│   └── Run me first.py
├── requirements.txt
└── README.md
```

---

## Future Enhancements

- Join NOAA weather data to county-day outage features
- Add severe weather indicators such as wind, precipitation, and temperature extremes
- Add geospatial dashboard maps
- Tune major outage thresholds by state or population
- Compare additional models such as Gradient Boosted Trees
