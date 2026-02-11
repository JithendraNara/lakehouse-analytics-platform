# signal-lakehouse

End-to-end analytics engineering project using synthetic product, billing, and support datasets.

## Goals
- Build medallion-style data pipeline (raw -> staged -> marts).
- Provide KPI outputs for BI tools.
- Enforce data quality checks and lineage notes.

## Stack
- Python, SQL, SQLite (local warehouse)
- Optional portability notes for Snowflake/Databricks/ADF/Airflow

## Quick Start
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m pipeline.run_all
```

Outputs are written to `data/exports/`.
