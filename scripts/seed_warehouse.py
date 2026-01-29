from __future__ import annotations

import argparse
import random
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from dqa.connectors.duckdb_conn import DuckDBConnector
from dqa.utils.config import PATHS

TABLES = ["dim_customers", "fact_orders", "fact_payments"]

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS dim_customers (
  customer_id VARCHAR,
  email_hash VARCHAR,
  country VARCHAR,
  created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fact_orders (
  order_id VARCHAR,
  customer_id VARCHAR,
  order_ts TIMESTAMP,
  status VARCHAR,
  amount DOUBLE,
  currency VARCHAR,
  channel VARCHAR
);

CREATE TABLE IF NOT EXISTS fact_payments (
  payment_id VARCHAR,
  order_id VARCHAR,
  payment_ts TIMESTAMP,
  amount DOUBLE,
  method VARCHAR,
  status VARCHAR
);

CREATE TABLE IF NOT EXISTS dq_profiles (
  snapshot_name VARCHAR,
  table_name VARCHAR,
  profile_json VARCHAR,
  created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dq_runs (
  run_id VARCHAR,
  snapshot_name VARCHAR,
  created_at TIMESTAMP,
  summary_json VARCHAR
);
"""

def reset_tables(con: DuckDBConnector) -> None:
    for t in TABLES:
        con.exec(f"DROP TABLE IF EXISTS {t};")
    con.exec("DROP TABLE IF EXISTS dq_profiles;")
    con.exec("DROP TABLE IF EXISTS dq_runs;")
    con.exec(SCHEMA_SQL)

def make_baseline(seed: int = 7):
    random.seed(seed)
    np.random.seed(seed)

    n_customers = 2000
    countries = ["IN", "US", "SG", "AE", "UK"]
    channels = ["shopify", "amazon"]
    currencies = ["INR", "USD"]
    statuses = ["PLACED", "PAID", "CANCELLED", "REFUNDED"]
    pay_methods = ["card", "upi", "netbanking", "wallet"]

    base_date = datetime(2026, 1, 10, 10, 0, 0)

    customers = pd.DataFrame({
        "customer_id": [f"C{str(i).zfill(5)}" for i in range(1, n_customers + 1)],
        "email_hash": [f"hash_{i}" for i in range(1, n_customers + 1)],
        "country": np.random.choice(countries, size=n_customers, p=[0.5, 0.2, 0.1, 0.1, 0.1]),
        "created_at": [base_date - timedelta(days=int(x)) for x in np.random.randint(1, 60, size=n_customers)],
    })

    n_orders = 8000
    order_ids = [f"O{str(i).zfill(6)}" for i in range(1, n_orders + 1)]
    cust_ids = np.random.choice(customers["customer_id"], size=n_orders)
    order_ts = [base_date + timedelta(minutes=int(x)) for x in np.random.randint(0, 60 * 24, size=n_orders)]

    amount_inr = np.clip(np.random.normal(loc=1500, scale=700, size=n_orders), 50, 15000)
    amount_usd = np.clip(np.random.normal(loc=40, scale=25, size=n_orders), 1, 500)
    currency = np.random.choice(currencies, size=n_orders, p=[0.7, 0.3])
    amount = np.where(currency == "INR", amount_inr, amount_usd)

    orders = pd.DataFrame({
        "order_id": order_ids,
        "customer_id": cust_ids,
        "order_ts": order_ts,
        "status": np.random.choice(statuses, size=n_orders, p=[0.1, 0.75, 0.1, 0.05]),
        "amount": amount,
        "currency": currency,
        "channel": np.random.choice(channels, size=n_orders, p=[0.65, 0.35]),
    })

    paid_orders = orders[orders["status"].isin(["PAID", "REFUNDED"])].copy()
    n_pay = int(len(paid_orders) * 0.95)

    payment_orders = paid_orders.sample(n=n_pay, random_state=seed)
    payments = pd.DataFrame({
        "payment_id": [f"P{str(i).zfill(7)}" for i in range(1, n_pay + 1)],
        "order_id": payment_orders["order_id"].values,
        "payment_ts": payment_orders["order_ts"].values
            + pd.to_timedelta(np.random.randint(1, 20, size=n_pay), unit="m"),
        "amount": payment_orders["amount"].values,
        "method": np.random.choice(pay_methods, size=n_pay, p=[0.55, 0.25, 0.12, 0.08]),
        "status": np.random.choice(["SUCCESS", "FAILED"], size=n_pay, p=[0.98, 0.02]),
    })

    return customers, orders, payments

def inject_bad_day(customers, orders, payments, seed: int = 21):
    random.seed(seed)
    np.random.seed(seed)

    orders_bad = orders.copy()
    payments_bad = payments.copy()
    customers_bad = customers.copy()

    # 1) duplicate order_id rows
    dup_rows = orders_bad.sample(n=30, random_state=seed)
    orders_bad = pd.concat([orders_bad, dup_rows], ignore_index=True)

    # 2) null spike in customer_id
    idx = np.random.choice(orders_bad.index, size=120, replace=False)
    orders_bad.loc[idx, "customer_id"] = None

    # 3) outlier amounts in USD (currency conversion bug)
    out_idx = np.random.choice(orders_bad.index, size=25, replace=False)
    orders_bad.loc[out_idx, "currency"] = "USD"
    orders_bad.loc[out_idx, "amount"] = orders_bad.loc[out_idx, "amount"] * 300

    # 4) FK breaks: customer_id points to non-existing customers
    fk_idx = np.random.choice(orders_bad.index, size=40, replace=False)
    orders_bad.loc[fk_idx, "customer_id"] = "C99999"

    # 5) Orphan payments: order_id doesn't exist
    orphan = payments_bad.sample(n=10, random_state=seed).copy()
    orphan["order_id"] = "O999999"
    payments_bad = pd.concat([payments_bad, orphan], ignore_index=True)

    return customers_bad, orders_bad, payments_bad

def load_tables(con: DuckDBConnector, customers, orders, payments) -> None:
    with con.connect() as c:
        c.register("customers_df", customers)
        c.register("orders_df", orders)
        c.register("payments_df", payments)
        c.execute("INSERT INTO dim_customers SELECT * FROM customers_df;")
        c.execute("INSERT INTO fact_orders SELECT * FROM orders_df;")
        c.execute("INSERT INTO fact_payments SELECT * FROM payments_df;")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["baseline", "bad_day"], required=True)
    args = ap.parse_args()

    con = DuckDBConnector(PATHS.db_path)
    reset_tables(con)

    customers, orders, payments = make_baseline()

    if args.mode == "baseline":
        load_tables(con, customers, orders, payments)
        print("✅ Seeded BASELINE warehouse into DuckDB.")
    else:
        customers2, orders2, payments2 = inject_bad_day(customers, orders, payments)
        load_tables(con, customers2, orders2, payments2)
        print("✅ Seeded BAD_DAY warehouse into DuckDB (with anomalies).")

if __name__ == "__main__":
    main()
