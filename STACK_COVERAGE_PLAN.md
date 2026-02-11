# Stack Coverage Plan - lakehouse-analytics-platform

## Target Tech
- Snowflake
- Databricks
- Apache Spark
- Azure Data Factory
- AWS / Azure
- R

## Planned Deliverables
1. `integrations/snowflake/`
- Snowflake DDL + transformation SQL equivalents for marts.

2. `integrations/databricks/`
- Notebook implementing PySpark pipeline equivalent of staging + marts.

3. `orchestration/adf/`
- ADF pipeline JSON template for running ingestion + transform steps.

4. `docs/deployment/`
- `aws.md` and `azure.md` deployment runbooks.

5. `notebooks/`
- `r_kpi_analysis.Rmd` for regression/hypothesis analysis over exported KPIs.

## Evidence Update Targets
- `PROOF.md`: add IDs for Snowflake/Spark/ADF/R artifacts.
- `RESUME_BULLETS.md`: add cloud + Spark + R bullets tied to evidence IDs.
