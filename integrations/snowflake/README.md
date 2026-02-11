# Snowflake Parity Artifacts

This folder contains Snowflake-compatible SQL equivalents of the local SQLite pipeline.

## Files
- `01_create_schemas.sql`: Creates `RAW`, `STAGING`, `MARTS` schemas.
- `02_create_raw_tables.sql`: Creates raw tables mirroring CSV ingest structures.
- `03_staging_models.sql`: Staging transforms for typed, canonical tables.
- `04_mart_models.sql`: KPI mart builds for analytics consumers.

## Usage Notes
- Replace placeholder database/warehouse names with your environment values.
- Use Snowflake tasks/streams or orchestration tools (ADF/Airflow) to schedule.
