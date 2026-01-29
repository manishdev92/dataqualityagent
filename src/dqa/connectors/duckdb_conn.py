from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

import duckdb


class DuckDBConnector:
    def __init__(self, db_path: Path):
        self.db_path = db_path

    def connect(self) -> duckdb.DuckDBPyConnection:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        return duckdb.connect(str(self.db_path))

    def exec(self, sql: str, params: Optional[tuple[Any, ...]] = None) -> None:
        with self.connect() as con:
            con.execute(sql, params or ())

    def fetchdf(self, sql: str, params: Optional[tuple[Any, ...]] = None):
        with self.connect() as con:
            return con.execute(sql, params or ()).fetchdf()
