from __future__ import annotations

import sqlite3

import pandas as pd

from .config import EXPORT_DIR


MART_TABLES = [
    "marts_daily_kpis",
    "marts_channel_performance",
    "marts_customer_health",
    "marts_experiment_performance",
]


def export_marts(con: sqlite3.Connection) -> None:
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    for table_name in MART_TABLES:
        out_name = table_name.replace("marts_", "")
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
        df.to_csv(EXPORT_DIR / f"{out_name}.csv", index=False)
