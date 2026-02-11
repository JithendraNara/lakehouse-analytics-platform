from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

from .config import RAW_DIR, WAREHOUSE_PATH


def connect(reset_database: bool = False) -> sqlite3.Connection:
    if reset_database and WAREHOUSE_PATH.exists():
        WAREHOUSE_PATH.unlink()

    con = sqlite3.connect(WAREHOUSE_PATH)
    con.row_factory = sqlite3.Row
    return con


def load_raw_tables(con: sqlite3.Connection) -> None:
    tables = {
        "raw_users": RAW_DIR / "users.csv",
        "raw_events": RAW_DIR / "events.csv",
        "raw_payments": RAW_DIR / "payments.csv",
        "raw_support_tickets": RAW_DIR / "support_tickets.csv",
    }
    for table_name, csv_path in tables.items():
        df = pd.read_csv(csv_path)
        df.to_sql(table_name, con, if_exists="replace", index=False)


def execute_sql_folder(con: sqlite3.Connection, folder: Path) -> None:
    for sql_file in sorted(folder.glob("*.sql")):
        sql_text = sql_file.read_text(encoding="utf-8")
        con.executescript(sql_text)
    con.commit()
