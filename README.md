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
- Snowflake + Databricks/Spark parity artifacts for multi-runtime portability
- Azure Data Factory pipeline template for orchestration handoff
- AWS + Azure deployment runbooks
- R notebook for KPI trend and hypothesis analysis

## Repository Layout
```text
configs/                # Pipeline config
pipeline/               # Orchestration + generation + quality + exports
sql/staging/            # Staging layer SQL
sql/marts/              # Mart layer SQL
data/exports/           # Generated KPI outputs + quality reports
spark_jobs/             # Spark/Databricks jobs
api/                    # REST API for real-time ingestion
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

## Financial Data Pipeline (Included)

This repo includes a production-grade financial transaction processing pipeline:

### Features
- **Spark-based processing** — Handle 10M+ records with distributed computing
- **Anomaly detection** — Flag unusual transactions (high amount, velocity, frequency)
- **Batch aggregations** — Daily and merchant-level summaries
- **REST API** — Real-time transaction ingestion
- **Data generator** — Create 10M+ test records

### Usage
```bash
# Generate 10M transactions
python -m spark_jobs.financial.data_generator --count 10000000 --output data/transactions.csv

# Run pipeline (requires Spark)
spark-submit spark_jobs/financial/transaction_processor.py --input data/transactions.csv --output output/

# Start API
uvicorn api.main:app --reload
```

### API Endpoints
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| POST | `/api/v1/ingest` | Ingest transaction |
| POST | `/api/v1/ingest/batch` | Batch ingest |
| GET | `/api/v1/anomalies` | List anomalies |

### Anomaly Detection Rules
- High Amount (>3 std deviations)
- Velocity (>10 txns/minute)
- Very High (> $10k)

See `spark_jobs/financial/` for full implementation.

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

## Platform Parity Artifacts
- Snowflake SQL equivalents: `integrations/snowflake/`
- Databricks/Spark parity job: `integrations/databricks/`, `spark_jobs/lakehouse_parity_job.py`
- Azure Data Factory template: `orchestration/adf/pipeline.json`
- AWS/Azure deployment runbooks: `docs/deployment/aws.md`, `docs/deployment/azure.md`
- R statistical notebook: `notebooks/r_kpi_analysis.Rmd`
