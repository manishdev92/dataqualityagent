from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List

import pandas as pd

from dqa.connectors.duckdb_conn import DuckDBConnector


@dataclass(frozen=True)
class TableSpec:
    name: str
    key_columns: List[str]
    ts_columns: List[str]
    fk: Dict[str, tuple[str, str]]  # col -> (ref_table, ref_col)


E_COMMERCE_SPECS = [
    TableSpec("dim_customers", key_columns=["customer_id"], ts_columns=["created_at"], fk={}),
    TableSpec(
        "fact_orders",
        key_columns=["order_id"],
        ts_columns=["order_ts"],
        fk={"customer_id": ("dim_customers", "customer_id")},
    ),
    TableSpec(
        "fact_payments",
        key_columns=["payment_id"],
        ts_columns=["payment_ts"],
        fk={"order_id": ("fact_orders", "order_id")},
    ),
]


def _col_type_map(df: pd.DataFrame) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for c in df.columns:
        dt = str(df[c].dtype).lower()
        if "int" in dt or "float" in dt or "double" in dt:
            out[c] = "numeric"
        elif "datetime" in dt or "timestamp" in dt:
            out[c] = "timestamp"
        else:
            out[c] = "categorical"
    return out


def profile_table(con: DuckDBConnector, table: str, spec: TableSpec) -> Dict[str, Any]:
    df = con.fetchdf(f"SELECT * FROM {table};")
    row_count = int(len(df))
    col_types = _col_type_map(df)

    null_rates = {c: (float(df[c].isna().mean()) if row_count else 0.0) for c in df.columns}
    distinct_counts = {c: int(df[c].nunique(dropna=True)) for c in df.columns}

    key_dupes: Dict[str, float] = {}
    for k in spec.key_columns:
        if k in df.columns and row_count:
            dup_rate = float((df[k].duplicated(keep=False)).mean())
            key_dupes[k] = dup_rate

    numeric_stats: Dict[str, Dict[str, float]] = {}
    for c, t in col_types.items():
        if t == "numeric" and row_count:
            s = df[c].dropna()
            if len(s):
                numeric_stats[c] = {
                    "min": float(s.min()),
                    "max": float(s.max()),
                    "mean": float(s.mean()),
                    "std": float(s.std(ddof=0)) if len(s) > 1 else 0.0,
                    "p50": float(s.quantile(0.50)),
                    "p95": float(s.quantile(0.95)),
                    "p99": float(s.quantile(0.99)),
                }

    categorical_top: Dict[str, List[Dict[str, Any]]] = {}
    for c, t in col_types.items():
        if t == "categorical" and row_count:
            vc = df[c].dropna().value_counts().head(10)
            categorical_top[c] = [{"value": str(idx), "count": int(val)} for idx, val in vc.items()]

    freshness: Dict[str, Any] = {}
    for ts in spec.ts_columns:
        if ts in df.columns and row_count:
            mx = df[ts].dropna().max()
            freshness[ts] = str(mx) if pd.notna(mx) else None

    fk_violations: Dict[str, Dict[str, float]] = {}
    for col, (rt, rc) in spec.fk.items():
        if col in df.columns and row_count:
            q = f"""
            SELECT COUNT(*) AS bad
            FROM {table} t
            LEFT JOIN {rt} r
              ON t.{col} = r.{rc}
            WHERE t.{col} IS NOT NULL
              AND r.{rc} IS NULL;
            """
            bad = int(con.fetchdf(q)["bad"].iloc[0])
            fk_violations[col] = {
                "bad_rows": bad,
                "bad_rate": (bad / row_count if row_count else 0.0),
            }

    return {
        "table": table,
        "row_count": row_count,
        "null_rates": null_rates,
        "distinct_counts": distinct_counts,
        "key_duplicate_rates": key_dupes,
        "numeric_stats": numeric_stats,
        "categorical_top": categorical_top,
        "freshness": freshness,
        "fk_violations": fk_violations,
    }


def save_profile_snapshot(con: DuckDBConnector, snapshot_name: str, table: str, profile: Dict[str, Any]) -> None:
    payload = json.dumps(profile, ensure_ascii=False)
    con.exec(
        "INSERT INTO dq_profiles VALUES (?, ?, ?, NOW());",
        (snapshot_name, table, payload),
    )


def load_profile_snapshot(con: DuckDBConnector, snapshot_name: str) -> Dict[str, Dict[str, Any]]:
    df = con.fetchdf(
        "SELECT table_name, profile_json FROM dq_profiles WHERE snapshot_name = ?;",
        (snapshot_name,),
    )
    out: Dict[str, Dict[str, Any]] = {}
    for _, r in df.iterrows():
        out[str(r["table_name"])] = json.loads(r["profile_json"])
    return out
