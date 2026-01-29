from __future__ import annotations

from typing import Any, Dict, List
import yaml

from dqa.anomaly.detector import Finding
from dqa.profiling.profiler import E_COMMERCE_SPECS


def _spec_for_table(table: str):
    for s in E_COMMERCE_SPECS:
        if s.name == table:
            return s
    return None


def generate_dbt_schema(findings: List[Finding]) -> Dict[str, Any]:
    """
    Turn anomaly findings into a dbt schema.yml structure.
    This is intentionally practical for a POC: minimal, readable tests.
    """
    grouped: Dict[str, List[Finding]] = {}
    for f in findings:
        grouped.setdefault(f.table, []).append(f)

    models = []

    for table, flist in grouped.items():
        spec = _spec_for_table(table)

        # Table-level tests
        table_tests: List[Any] = []
        if spec:
            # Use dbt_utils if available in real projects; okay for POC visibility.
            for k in spec.key_columns:
                table_tests.append(
                    {"dbt_utils.unique_combination_of_columns": {"combination_of_columns": [k]}}
                )

        # Column-level tests
        columns_map: Dict[str, List[Any]] = {}

        for f in flist:
            if f.kind == "NULL_SPIKE":
                col = f.details["column"]
                columns_map.setdefault(col, []).append("not_null")

            if f.kind == "FK_VIOLATION":
                col = f.details["column"]
                if spec and col in spec.fk:
                    rt, rc = spec.fk[col]
                    columns_map.setdefault(col, []).append(
                        {"relationships": {"to": f"ref('{rt}')", "field": rc}}
                    )

            if f.kind == "P99_JUMP":
                col = f.details["column"]
                # conservative cap for demo
                cap = float(f.details["today_p99"]) * 1.2
                columns_map.setdefault(col, []).append(
                    {"accepted_range": {"min_value": 0, "max_value": cap}}
                )

            if f.kind == "DUPLICATE_KEY":
                # if duplicates detected, also add per-column unique to be explicit
                col = f.details["column"]
                columns_map.setdefault(col, []).append("unique")

        columns = [{"name": c, "tests": tests} for c, tests in columns_map.items()]

        models.append(
            {
                "name": table,
                "description": f"Auto-generated DQ tests for {table}",
                "tests": table_tests,
                "columns": columns,
            }
        )

    return {"version": 2, "models": models}


def write_schema_yml(schema: Dict[str, Any], out_path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        yaml.safe_dump(schema, f, sort_keys=False, allow_unicode=True)
