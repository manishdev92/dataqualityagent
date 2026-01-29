from __future__ import annotations

import argparse
import json
import subprocess
import sys
import uuid

from dqa.connectors.duckdb_conn import DuckDBConnector
from dqa.utils.config import PATHS
from dqa.profiling.profiler import E_COMMERCE_SPECS, profile_table
from dqa.anomaly.detector import detect_anomalies
from dqa.dbtgen.schema_generator import generate_dbt_schema, write_schema_yml
from dqa.reporting.report_writer import write_markdown_report


def seed(mode: str) -> None:
    subprocess.run([sys.executable, "scripts/seed_warehouse.py", "--mode", mode], check=True)


def run(target: str) -> dict:
    # 1) baseline profiles (in-memory for now)
    seed("baseline")
    con = DuckDBConnector(PATHS.db_path)
    baseline = {s.name: profile_table(con, s.name, s) for s in E_COMMERCE_SPECS}

    # 2) target data + profiles
    seed(target)
    con = DuckDBConnector(PATHS.db_path)
    profiles_today = {}
    findings = []
    for s in E_COMMERCE_SPECS:
        today = profile_table(con, s.name, s)
        profiles_today[s.name] = today
        findings.extend(detect_anomalies(today, baseline[s.name]))

    # 3) write artifacts
    schema = generate_dbt_schema(findings)
    write_schema_yml(schema, PATHS.generated_dbt_schema)

    run_id = uuid.uuid4().hex[:10]
    report_path = write_markdown_report(
        run_id=run_id,
        snapshot_name=target,
        findings=findings,
        profiles_today=profiles_today,
        out_dir=PATHS.generated_reports_dir,
    )

    summary = {
        "run_id": run_id,
        "target": target,
        "findings_count": len(findings),
        "dbt_schema_path": str(PATHS.generated_dbt_schema),
        "report_path": str(report_path),
    }
    return summary


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", choices=["baseline", "bad_day"], required=True)
    args = ap.parse_args()

    out = run(args.target)
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
