# Project Plan

## Phase 1: Pipeline Setup

- Import the GitHub repo into Databricks Repos.
- Upload a small raw outage CSV sample to a Unity Catalog Volume.
- Run `utilities/Run me first.py`.
- Configure the Databricks Pipeline with the transformation files.

## Phase 2: Bronze and Silver

- Validate raw CSV ingestion with Auto Loader.
- Confirm schema, row counts, and metadata columns.
- Adjust timestamp parsing if the source date format differs.

## Phase 3: Gold Features

- Tune the major outage threshold.
- Add weather fields once the outage pipeline works.
- Create county-day feature table.

## Phase 4: Model and Dashboard

- Train baseline Logistic Regression and Random Forest models.
- Register the best model in Unity Catalog.
- Re-run scoring pipeline.
- Build Databricks dashboard from gold tables.
