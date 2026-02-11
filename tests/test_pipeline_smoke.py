from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.run_all import run


def test_pipeline_generates_exports() -> None:
    run()
    export_dir = Path(__file__).resolve().parents[1] / "data" / "exports"
    expected = [
        "daily_kpis.csv",
        "channel_performance.csv",
        "customer_health.csv",
        "experiment_performance.csv",
        "quality_report.json",
        "quality_report.md",
    ]
    for file_name in expected:
        assert (export_dir / file_name).exists(), f"missing export {file_name}"
