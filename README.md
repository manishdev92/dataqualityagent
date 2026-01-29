# DataQualityAgent  
**An Agentic Data Quality System for E-commerce Analytics (DuckDB + dbt + FastAPI)**

![CI](https://github.com/manishdev92/dataqualityagent/actions/workflows/ci.yml/badge.svg)

---

## 📌 What is this project?

**DataQualityAgent** is a local, end-to-end **data quality automation system** built for data engineers and analytics engineers.

It simulates a real analytics warehouse and automatically:

- profiles tables
- detects data quality issues
- generates **dbt tests**
- produces a **human-readable report**
- exposes everything via a **REST API**
- validates logic using **pytest**
- runs **CI on every push using GitHub Actions**

This project is designed to be:
- ✅ beginner-friendly
- ✅ realistic (not toy data)
- ✅ reusable in real projects
- ✅ impressive for recruiters

---

## 🎯 Problem this project solves

In real data platforms, data breaks due to:

- duplicate primary keys  
- sudden null spikes  
- broken foreign keys  
- unexpected value spikes (currency / ingestion bugs)  
- silent data drift  

Most teams:
- detect issues **late**
- write tests **manually**
- debug issues **reactively**

**This project automates that workflow.**

---

## 🧠 What makes this project “agentic”?

Instead of hardcoding rules, the system follows a **reasoning pipeline**:

1. **Profile** tables (facts + dimensions)
2. **Compare** current data vs baseline
3. **Detect anomalies** based on evidence
4. **Decide which tests are needed**
5. **Generate dbt tests automatically**
6. **Explain findings in a report**

This mirrors how a real data engineer reasons about data quality.

---

## 🏗 High-level architecture
```
DuckDB (local warehouse)
        ↓
Table Profiler
        ↓
Anomaly Detector (baseline vs today)
        ↓
dbt Test Generator (schema.yml)
        ↓
Markdown DQ Report
        ↓
FastAPI Service
```

---

## 📦 Dataset used (realistic)

A realistic **e-commerce star schema**:

### Tables
- `dim_customers`
- `fact_orders`
- `fact_payments`

### Data modes
- **baseline** → clean, healthy data
- **bad_day** → injected issues:
  - duplicate IDs
  - null spikes
  - foreign key violations
  - outlier amounts

This lets you *see the system actually working*.

---

## 📁 Project structure
```
dataqualityagent/
├── scripts/
│   ├── seed_warehouse.py      # creates baseline / bad_day data
│   └── run_dq.py              # one-command end-to-end run
│
├── src/dqa/
│   ├── connectors/            # DuckDB connector
│   ├── profiling/             # table profiling logic
│   ├── anomaly/               # anomaly detection rules
│   ├── dbtgen/                # dbt schema.yml generator
│   ├── reporting/             # markdown report generator
│   └── api/                   # FastAPI service
│
├── generated/
│   ├── dbt/models/schema.yml  # auto-generated dbt tests
│   └── reports/               # DQ reports
│
├── tests/                     # pytest unit + smoke tests
├── pyproject.toml
└── README.md
```


---

## ⚙️ Setup (Mac / Linux)

### 1️⃣ Create virtual environment
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip```

## 2️⃣ Install dependencies
- pip install -e ".[dev]"

## ▶️ Run the system (one command)
- python scripts/run_dq.py --target bad_day

## What this command does

- Seeds baseline data

- Profiles baseline tables

- Seeds bad_day data

- Profiles current tables

- Detects anomalies

- Generates dbt tests

- Writes a DQ report

- 📄 Outputs to check

## 1️⃣ Generated dbt tests
- generated/dbt/models/schema.yml


## Example tests:

- unique

- not_null

- relationships

- accepted_range

## 2️⃣ Data Quality report
generated/reports/run_<run_id>.md


## Contains:

- findings grouped by table

- severity (CRITICAL / WARN)

- evidence summary

## 🌐 Run as an API (FastAPI)
- Start the service
```uvicorn dqa.api.main:app --reload --port 8000```

## Open Swagger UI
```http://127.0.0.1:8000/docs```

## Run DQ via API
```curl -X POST "http://127.0.0.1:8000/dq/run?target=bad_day"```

## Fetch the report
```curl "http://127.0.0.1:8000/dq/report/<run_id>"```

## 🧪 Tests
```pytest -q```


## Includes:

- unit tests for anomaly logic

- smoke test for full pipeline

- 🔄 Continuous Integration (CI)

- This project uses GitHub Actions for Continuous Integration.

## What runs automatically

- On every push or pull request to main:

- Python 3.11 environment is created

- Project dependencies are installed

- All pytest unit and smoke tests are executed

## Why this matters

- Prevents broken code from being merged

- Ensures data quality logic stays correct

- Demonstrates production-grade engineering practices

- CI status is visible in the Actions tab of the repository.

## 🔁 How to reuse this in real projects

- You can extend this system to:

- Postgres / Snowflake / BigQuery

- Parquet / S3 / Delta Lake

- dbt CI pipelines

- Airflow / Dagster scheduled runs

- Only the connector layer needs to change.

## 🚀 Future enhancements

- Persist historical baselines (trend over time)

- Smarter drift detection (MAD / robust z-score)

- LLM-based test recommendations

- Data Quality score per table

- Docker support

## ⭐ Why this project stands out

- This is not a tutorial toy.

- It demonstrates:

- system design thinking

- real-world data issues

- automation mindset

- clean Python engineering

- CI/CD awareness

## Ideal for:

- GitHub portfolio

- interviews

- internal tooling inspiration
