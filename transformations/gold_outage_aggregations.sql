-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold Aggregations
-- MAGIC Dashboard helper queries for outage risk analysis.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW outage_state_summary AS
SELECT
  state,
  COUNT(DISTINCT fips_code) AS counties_observed,
  COUNT(*) AS county_day_records,
  AVG(major_outage) AS major_outage_rate,
  AVG(avg_customers_out) AS avg_customers_out,
  MAX(max_customers_out) AS worst_observed_outage
FROM LIVE.outage_county_day_features
GROUP BY state;

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW outage_county_rankings AS
SELECT
  state,
  fips_code,
  county,
  COUNT(*) AS observed_days,
  AVG(major_outage) AS major_outage_rate,
  AVG(avg_customers_out) AS avg_customers_out,
  MAX(max_customers_out) AS worst_observed_outage,
  AVG(risk_probability) AS avg_risk_probability
FROM LIVE.outage_county_risk_scores
GROUP BY state, fips_code, county;
