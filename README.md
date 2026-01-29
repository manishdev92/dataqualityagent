# DataQualityAgent (E-commerce) — DuckDB + dbt Test Generator

A local-first Data Quality system that:
- Builds a small e-commerce warehouse in DuckDB
- Profiles tables (nulls, duplicates, numeric drift, category drift, freshness)
- Detects anomalies vs baseline
- Generates dbt tests (`schema.yml`)
- Produces a Markdown DQ report
- Exposes FastAPI endpoints

## Quickstart (Mac)

### 1) Create venv
Option A (uv):
```bash
pip install uv
uv venv
source .venv/bin/activate
uv pip install -e ".[dev]"

OR
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e ".[dev]"

