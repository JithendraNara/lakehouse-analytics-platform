from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

BASE_DIR = Path(__file__).resolve().parents[1]
CONFIG_PATH = BASE_DIR / "configs" / "pipeline.yaml"
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
STAGED_DIR = DATA_DIR / "staged"
MARTS_DIR = DATA_DIR / "marts"
EXPORT_DIR = DATA_DIR / "exports"
WAREHOUSE_PATH = DATA_DIR / "warehouse.sqlite"


def load_config(path: Path = CONFIG_PATH) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def ensure_directories() -> None:
    for d in (RAW_DIR, STAGED_DIR, MARTS_DIR, EXPORT_DIR):
        d.mkdir(parents=True, exist_ok=True)
