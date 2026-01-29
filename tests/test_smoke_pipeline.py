import subprocess
import sys

from dqa.connectors.duckdb_conn import DuckDBConnector
from dqa.utils.config import PATHS
from dqa.profiling.profiler import E_COMMERCE_SPECS, profile_table
from dqa.anomaly.detector import detect_anomalies


def seed(mode: str):
    subprocess.run([sys.executable, "scripts/seed_warehouse.py", "--mode", mode], check=True)


def test_pipeline_detects_bad_day_findings():
    # baseline profiles
    seed("baseline")
    con = DuckDBConnector(PATHS.db_path)
    baseline = {s.name: profile_table(con, s.name, s) for s in E_COMMERCE_SPECS}

    # bad day profiles + compare
    seed("bad_day")
    con = DuckDBConnector(PATHS.db_path)
    findings = []
    for s in E_COMMERCE_SPECS:
        today = profile_table(con, s.name, s)
        findings.extend(detect_anomalies(today, baseline[s.name]))

    # Expect at least one issue detected
    assert len(findings) > 0
