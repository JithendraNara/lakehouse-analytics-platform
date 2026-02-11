# Databricks Notebook Outline

1. Read raw source files from DBFS / cloud storage.
2. Build staging temp views (`staging_users`, `staging_events`, `staging_payments`).
3. Generate `marts_daily_kpis` using Spark SQL.
4. Persist outputs to Delta tables and export snapshots.
5. Run data quality assertions and publish summary.
