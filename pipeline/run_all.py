from __future__ import annotations

from pathlib import Path

from .config import BASE_DIR, ensure_directories, load_config
from .exports import export_marts
from .generate_data import GeneratorConfig, generate_raw_data
from .quality import export_quality_report, run_checks
from .sql_runner import connect, execute_sql_folder, load_raw_tables


def run() -> None:
    cfg = load_config()
    ensure_directories()

    gen_cfg = GeneratorConfig(
        random_seed=int(cfg["pipeline"]["random_seed"]),
        days_back=int(cfg["pipeline"]["days_back"]),
        n_users=int(cfg["pipeline"]["n_users"]),
        avg_events_per_user=int(cfg["pipeline"]["avg_events_per_user"]),
        avg_tickets_per_user=float(cfg["pipeline"]["avg_tickets_per_user"]),
    )

    print("[1/6] Generating synthetic raw data")
    generate_raw_data(gen_cfg)

    print("[2/6] Connecting to warehouse")
    con = connect(reset_database=bool(cfg["pipeline"]["reset_database"]))

    try:
        print("[3/6] Loading raw tables")
        load_raw_tables(con)

        print("[4/6] Running staging SQL")
        execute_sql_folder(con, BASE_DIR / "sql" / "staging")

        print("[5/6] Running mart SQL")
        execute_sql_folder(con, BASE_DIR / "sql" / "marts")

        print("[6/6] Running quality checks and exports")
        quality_results = run_checks(con)
        export_quality_report(quality_results)
        export_marts(con)
    finally:
        con.close()

    print("Pipeline completed. See data/exports for outputs.")


if __name__ == "__main__":
    run()
