# lakehouse-analytics-platform

End-to-end analytics engineering project using synthetic product, billing, and support datasets.

## What This Demonstrates
- Medallion-style data flow (`raw -> staging -> marts`) with reproducible orchestration.
- KPI-centric marts for conversion, revenue, channel performance, customer health, and experiment analysis.
- Data quality checks with machine-readable outputs suitable for CI validation.

## Role Positioning
- Primary fit: Data Analyst, Analytics Engineer, Data Engineer
- Showcase focus: SQL modeling, pipeline orchestration, KPI analytics, quality checks
- Resume mapping: see `PROOF.md` and `RESUME_BULLETS.md`

## Architecture
- `pipeline/generate_data.py`: generates realistic synthetic source datasets.
- `sql/staging/*.sql`: canonical staging transformations.
- `sql/marts/*.sql`: analytics marts for downstream consumption.
- `pipeline/quality.py`: quality assertions and report generation.
- `pipeline/exports.py`: mart exports to CSV for API/dashboard layers.

## Stack
- Python, SQL, SQLite (local warehouse runtime)
- Pandas + NumPy for synthetic generation and export handling
- PyYAML configuration-driven runs
- Portability notes for Snowflake/Databricks/ADF/Airflow in docs and roadmap

## Repository Layout
```text
configs/                # Pipeline config
pipeline/               # Orchestration + generation + quality + exports
sql/staging/            # Staging layer SQL
sql/marts/              # Mart layer SQL
data/exports/           # Generated KPI outputs + quality reports
tests/                  # Smoke tests
```

## Quick Start
```bash
python3 -m venv --clear .venv
source .venv/bin/activate
pip install -r requirements.txt pytest
python -m pipeline.run_all
pytest -q
```

Outputs are written to `data/exports/`.

## Key Outputs
- `data/exports/daily_kpis.csv`
- `data/exports/channel_performance.csv`
- `data/exports/customer_health.csv`
- `data/exports/experiment_performance.csv`
- `data/exports/quality_report.json`

## CI
GitHub Actions runs pipeline + tests on push/PR:
- `.github/workflows/ci.yml`

## Development Trail
- Roadmap: `ROADMAP.md`
- Changelog: `CHANGELOG.md`
- Proof mapping: `PROOF.md`
- Resume bullets: `RESUME_BULLETS.md`

## Stack Coverage Extension
- Planned gap-coverage work is tracked in `STACK_COVERAGE_PLAN.md`.
