from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass

from .config import EXPORT_DIR


@dataclass
class QualityCheck:
    name: str
    query: str
    max_fail_count: int = 0


QUALITY_CHECKS = [
    QualityCheck(
        name="duplicate_user_ids",
        query="""
            SELECT COUNT(*) AS fail_count
            FROM (
              SELECT user_id
              FROM staging_users
              GROUP BY user_id
              HAVING COUNT(*) > 1
            ) d
        """,
    ),
    QualityCheck(
        name="null_event_timestamp",
        query="SELECT COUNT(*) AS fail_count FROM staging_events WHERE event_ts IS NULL",
    ),
    QualityCheck(
        name="negative_payment_amount",
        query="SELECT COUNT(*) AS fail_count FROM staging_payments WHERE amount_usd < 0",
    ),
    QualityCheck(
        name="daily_kpi_not_empty",
        query="SELECT CASE WHEN COUNT(*) > 0 THEN 0 ELSE 1 END AS fail_count FROM marts_daily_kpis",
    ),
    QualityCheck(
        name="conversion_rate_range",
        query="""
            SELECT COUNT(*) AS fail_count
            FROM marts_daily_kpis
            WHERE conversion_rate < 0 OR conversion_rate > 1
        """,
    ),
]


def run_checks(con: sqlite3.Connection) -> list[dict[str, object]]:
    results: list[dict[str, object]] = []
    for check in QUALITY_CHECKS:
        fail_count = int(con.execute(check.query).fetchone()[0])
        status = "PASS" if fail_count <= check.max_fail_count else "FAIL"
        results.append(
            {
                "check": check.name,
                "fail_count": fail_count,
                "threshold": check.max_fail_count,
                "status": status,
            }
        )
    return results


def export_quality_report(results: list[dict[str, object]]) -> None:
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    json_path = EXPORT_DIR / "quality_report.json"
    md_path = EXPORT_DIR / "quality_report.md"

    json_path.write_text(json.dumps(results, indent=2), encoding="utf-8")

    lines = ["# Quality Report", "", "| Check | Fail Count | Threshold | Status |", "|---|---:|---:|---|"]
    for r in results:
        lines.append(
            f"| {r['check']} | {r['fail_count']} | {r['threshold']} | {r['status']} |"
        )
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
