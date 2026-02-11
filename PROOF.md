# Proof Map

| Claim | Evidence | Metric |
|---|---|---|
| Built end-to-end medallion-style analytics pipeline from raw events to business marts. | `pipeline/run_all.py`, `sql/staging/*.sql`, `sql/marts/*.sql` | 4 marts generated in one run |
| Implemented automated data quality checks with machine-readable outputs. | `pipeline/quality.py`, `data/exports/quality_report.json` | 5 checks run with pass/fail statuses |
| Delivered BI-ready KPI exports for dashboard consumption. | `pipeline/exports.py`, `data/exports/daily_kpis.csv` | Daily KPI table exported with conversion and revenue metrics |
