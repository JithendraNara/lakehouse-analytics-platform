# Lakehouse Analytics Platform

End-to-end analytics engineering project using the modern data lakehouse architecture with medallion layers.

## Architecture

```
Data Sources → Bronze (raw) → Silver (cleaned) → Gold (aggregated) → BI Layer
```

**Bronze** — raw ingestion with schema-on-read, immutable append-only storage
**Silver** — cleaned, deduplicated, and typed data with business logic applied
**Gold** — aggregated fact tables and dimensional models ready for analytics

## Tech Stack

| Layer | Technology |
|---|---|
| Processing | Apache Spark |
| Transformations | dbt |
| Table Format | Apache Iceberg / Delta Lake |
| Query Engine | Trino / Athena |
| Orchestration | Apache Airflow |
| Language | Python |

## Features

- Medallion architecture with Bronze / Silver / Gold layers
- ACID transactions via Apache Iceberg
- Schema evolution — handle upstream changes without breaking pipelines
- Time travel queries — query any historical state of the data
- Incremental processing — process only what changed since the last run

## Setup

```bash
git clone https://github.com/JithendraNara/lakehouse-analytics-platform.git
cd lakehouse-analytics-platform
pip install -r requirements.txt
```
