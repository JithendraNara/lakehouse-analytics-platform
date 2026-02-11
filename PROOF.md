# Proof Map

| ID | Claim | Evidence | Metric |
|---|---|---|---|
| L1 | Built end-to-end medallion-style analytics pipeline from raw events to business marts. | `pipeline/run_all.py`, `sql/staging/*.sql`, `sql/marts/*.sql` | 4 marts generated in one run |
| L2 | Implemented automated data quality checks with machine-readable outputs. | `pipeline/quality.py`, `data/exports/quality_report.json` | 5 checks run with pass/fail statuses |
| L3 | Delivered BI-ready KPI exports for dashboard consumption. | `pipeline/exports.py`, `data/exports/daily_kpis.csv` | Daily KPI table exported with conversion and revenue metrics |
| L4 | Added Snowflake and Databricks/Spark parity artifacts for cloud-warehouse migration readiness. | `integrations/snowflake/*.sql`, `integrations/databricks/*`, `spark_jobs/lakehouse_parity_job.py` | SQL + Spark parity paths documented and versioned |
