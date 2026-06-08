# Data Dictionary

## Bronze: outages_bronze

| Column | Description |
| --- | --- |
| fips_code | County FIPS code from EAGLE-I. |
| county | County name from EAGLE-I. |
| state | State name from EAGLE-I. |
| sum | Total number of utility customers without power in the county at the timestamp. Zero-customer outage rows are not included in the source files. |
| run_start_time | GMT timestamp marking the beginning of the 15-minute EAGLE-I collection run. |
| source_file | Ingested file path from Auto Loader metadata. |
| processing_time | Timestamp when the record was processed. |

## Silver: outages_silver

| Column | Description |
| --- | --- |
| event_timestamp | Parsed outage timestamp. |
| event_date | Date derived from timestamp. |
| fips_code | County FIPS code. |
| state | Standardized state name. |
| county | Standardized county name. |
| customers_out | Numeric utility customer outage count derived from EAGLE-I `sum`. This is not necessarily the number of people affected. |
| latitude | Latitude. |
| longitude | Longitude. |
| utility | Utility/provider field. |

## Gold: outage_county_day_features

| Column | Description |
| --- | --- |
| outage_observations | Number of outage records for county/date. |
| fips_code | County FIPS code. |
| avg_customers_out | Average customers out for county/date. |
| max_customers_out | Maximum customers out for county/date. |
| total_customers_out | Total customers out for county/date. |
| major_outage | Binary label based on configured customer outage threshold. |

## Gold: outage_county_risk_scores

| Column | Description |
| --- | --- |
| risk_probability | Predicted probability of a major outage. |
| model_uri | MLflow model URI used for scoring. |

## Supporting File: coverage_history.csv

| Column | Description |
| --- | --- |
| year | Date used to derive annual coverage information. |
| state | Two-character state identifier. |
| total_customers | Total utility customers in the state for the given date. |
| min_covered | Minimum utility customers covered by EAGLE-I in the calendar year. |
| max_covered | Maximum utility customers covered by EAGLE-I in the calendar year. |
| min_pct_covered | Minimum percentage of state utility customers covered in the year. |
| max_pct_covered | Maximum percentage of state utility customers covered in the year. |

## Source Notes

- Data collection range: November 1, 2014 through December 31, 2022.
- Geography: United States and territories.
- License: Creative Commons Attribution 4.0 International (CC BY 4.0).
- The outage CSV files are aggregated to county level for consistent reporting.
