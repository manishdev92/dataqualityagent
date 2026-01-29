from dataclasses import dataclass
from pathlib import Path

# Project root = dataqualityagent/
ROOT = Path(__file__).resolve().parents[3]

@dataclass(frozen=True)
class Paths:
    db_path: Path = ROOT / "data" / "warehouse.duckdb"
    generated_dbt_schema: Path = ROOT / "generated" / "dbt" / "models" / "schema.yml"
    generated_reports_dir: Path = ROOT / "generated" / "reports"

PATHS = Paths()
