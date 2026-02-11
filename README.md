# signal-lakehouse

End-to-end analytics engineering project using synthetic product, billing, and support datasets.

## Role Positioning
- Primary fit: Data Analyst, Analytics Engineer, Data Engineer
- Showcase focus: SQL modeling, pipeline orchestration, KPI analytics, quality checks
- Resume mapping: see `PROOF.md` and `RESUME_BULLETS.md`

## Goals
- Build medallion-style data pipeline (raw -> staged -> marts).
- Provide KPI outputs for BI tools.
- Enforce data quality checks and lineage notes.

## Stack
- Python, SQL, SQLite (local warehouse)
- Portability notes for Snowflake/Databricks/ADF/Airflow

## Quick Start
```bash
python3 -m venv --clear .venv
source .venv/bin/activate
pip install -r requirements.txt pytest
python -m pipeline.run_all
pytest -q
```

Outputs are written to `data/exports/`.

## Development Trail
- Roadmap: `ROADMAP.md`
- Changelog: `CHANGELOG.md`
- Proof mapping: `PROOF.md`
- Resume bullets: `RESUME_BULLETS.md`
