from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from .config import RAW_DIR


@dataclass
class GeneratorConfig:
    random_seed: int
    days_back: int
    n_users: int
    avg_events_per_user: int
    avg_tickets_per_user: float


def _random_timestamps(
    rng: np.random.Generator,
    start_dt: datetime,
    end_dt: datetime,
    count: int,
) -> pd.Series:
    total_seconds = int((end_dt - start_dt).total_seconds())
    offsets = rng.integers(0, max(total_seconds, 1), size=count)
    return pd.to_datetime([start_dt + timedelta(seconds=int(o)) for o in offsets])


def build_users(cfg: GeneratorConfig) -> pd.DataFrame:
    rng = np.random.default_rng(cfg.random_seed)
    end_dt = datetime.now().replace(microsecond=0, second=0)
    start_dt = end_dt - timedelta(days=cfg.days_back)

    signup_ts = _random_timestamps(rng, start_dt, end_dt, cfg.n_users)
    users = pd.DataFrame(
        {
            "user_id": np.arange(1, cfg.n_users + 1, dtype=np.int64),
            "signup_ts": signup_ts,
            "acquisition_channel": rng.choice(
                ["organic", "paid_search", "referral", "partner", "social"],
                size=cfg.n_users,
                p=[0.28, 0.24, 0.18, 0.12, 0.18],
            ),
            "country": rng.choice(
                ["US", "CA", "UK", "DE", "IN", "AU"],
                size=cfg.n_users,
                p=[0.48, 0.1, 0.1, 0.08, 0.18, 0.06],
            ),
            "plan_tier": rng.choice(
                ["free", "pro", "enterprise"],
                size=cfg.n_users,
                p=[0.57, 0.36, 0.07],
            ),
            "company_size": rng.choice(
                ["individual", "small", "mid", "large"],
                size=cfg.n_users,
                p=[0.45, 0.29, 0.18, 0.08],
            ),
        }
    )
    return users.sort_values("signup_ts").reset_index(drop=True)


def build_events(users: pd.DataFrame, cfg: GeneratorConfig) -> pd.DataFrame:
    rng = np.random.default_rng(cfg.random_seed + 1)
    end_dt = datetime.now().replace(microsecond=0, second=0)

    base_events = rng.poisson(lam=cfg.avg_events_per_user, size=len(users))
    uplift = users["plan_tier"].map({"free": 0, "pro": 5, "enterprise": 11}).to_numpy()
    n_events_per_user = np.clip(base_events + uplift, 4, None)

    rows: list[dict[str, object]] = []
    event_id = 1
    for user, n_events in zip(users.itertuples(index=False), n_events_per_user):
        signup_dt = pd.Timestamp(user.signup_ts).to_pydatetime()
        event_ts = _random_timestamps(rng, signup_dt, end_dt, int(n_events))
        event_ts = event_ts.sort_values()

        for ts in event_ts:
            event_type = rng.choice(
                ["session_start", "feature_used", "trial_started", "subscription_started", "churned"],
                p=[0.60, 0.25, 0.06, 0.06, 0.03],
            )
            exp_name = "new_onboarding" if rng.random() < 0.72 else None
            exp_variant = rng.choice(["A", "B"], p=[0.5, 0.5]) if exp_name else None
            rows.append(
                {
                    "event_id": event_id,
                    "user_id": int(user.user_id),
                    "event_ts": ts,
                    "event_type": event_type,
                    "feature_name": rng.choice(
                        ["dashboard", "alerts", "report_builder", "cohort_view", "copilot"],
                        p=[0.27, 0.17, 0.26, 0.16, 0.14],
                    ),
                    "experiment_name": exp_name,
                    "experiment_variant": exp_variant,
                    "session_duration_sec": int(rng.integers(20, 3600)) if event_type == "session_start" else None,
                }
            )
            event_id += 1

    events = pd.DataFrame(rows)
    return events.sort_values("event_ts").reset_index(drop=True)


def build_payments(users: pd.DataFrame, cfg: GeneratorConfig) -> pd.DataFrame:
    rng = np.random.default_rng(cfg.random_seed + 2)
    end_dt = datetime.now().replace(microsecond=0, second=0)

    paid_users = users[users["plan_tier"].isin(["pro", "enterprise"])].copy()
    rows: list[dict[str, object]] = []
    payment_id = 1

    for user in paid_users.itertuples(index=False):
        signup_dt = pd.Timestamp(user.signup_ts).to_pydatetime()
        tenure_days = max((end_dt - signup_dt).days, 1)
        n_invoices = max(1, int(tenure_days / 30) + int(rng.integers(0, 2)))
        base_price = 49.0 if user.plan_tier == "pro" else 199.0

        invoice_ts = _random_timestamps(rng, signup_dt, end_dt, n_invoices).sort_values()
        for ts in invoice_ts:
            status = rng.choice(["success", "refund", "failed"], p=[0.91, 0.05, 0.04])
            amount = max(5.0, float(base_price + rng.normal(0, base_price * 0.12)))
            rows.append(
                {
                    "payment_id": payment_id,
                    "user_id": int(user.user_id),
                    "payment_ts": ts,
                    "amount_usd": round(amount, 2),
                    "payment_status": status,
                    "invoice_type": rng.choice(["subscription", "upgrade"], p=[0.88, 0.12]),
                }
            )
            payment_id += 1

    payments = pd.DataFrame(rows)
    return payments.sort_values("payment_ts").reset_index(drop=True)


def build_support_tickets(users: pd.DataFrame, cfg: GeneratorConfig) -> pd.DataFrame:
    rng = np.random.default_rng(cfg.random_seed + 3)
    end_dt = datetime.now().replace(microsecond=0, second=0)
    start_dt = end_dt - timedelta(days=cfg.days_back)

    n_tickets = int(len(users) * cfg.avg_tickets_per_user)
    ticket_user_ids = rng.choice(users["user_id"].to_numpy(), size=n_tickets, replace=True)
    created_ts = _random_timestamps(rng, start_dt, end_dt, n_tickets)

    resolution_hours = rng.integers(2, 96, size=n_tickets)
    unresolved_mask = rng.random(n_tickets) < 0.14
    resolved_ts = []
    for created, hrs, unresolved in zip(created_ts, resolution_hours, unresolved_mask):
        if unresolved:
            resolved_ts.append(pd.NaT)
        else:
            resolved_ts.append(created + pd.Timedelta(hours=int(hrs)))

    tickets = pd.DataFrame(
        {
            "ticket_id": np.arange(1, n_tickets + 1, dtype=np.int64),
            "user_id": ticket_user_ids,
            "created_ts": created_ts,
            "resolved_ts": resolved_ts,
            "severity": rng.choice(["low", "medium", "high"], size=n_tickets, p=[0.46, 0.38, 0.16]),
            "csat_score": rng.choice([1, 2, 3, 4, 5], size=n_tickets, p=[0.06, 0.09, 0.19, 0.35, 0.31]),
        }
    )

    return tickets.sort_values("created_ts").reset_index(drop=True)


def write_raw_files(users: pd.DataFrame, events: pd.DataFrame, payments: pd.DataFrame, tickets: pd.DataFrame) -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    users.to_csv(RAW_DIR / "users.csv", index=False)
    events.to_csv(RAW_DIR / "events.csv", index=False)
    payments.to_csv(RAW_DIR / "payments.csv", index=False)
    tickets.to_csv(RAW_DIR / "support_tickets.csv", index=False)


def generate_raw_data(cfg: GeneratorConfig) -> None:
    users = build_users(cfg)
    events = build_events(users, cfg)
    payments = build_payments(users, cfg)
    tickets = build_support_tickets(users, cfg)
    write_raw_files(users, events, payments, tickets)
