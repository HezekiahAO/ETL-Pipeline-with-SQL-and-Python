"""Configuration: everything comes from environment variables (12-factor style)."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class Settings:
    # Either DATABASE_URL (postgresql://user:pw@host:5432/db) or the standard libpq PG* variables.
    database_url: str | None = field(default_factory=lambda: os.getenv("DATABASE_URL"))
    pipeline_name: str = field(default_factory=lambda: os.getenv("ETL_PIPELINE_NAME", "orders_xml"))
    batch_size: int = field(default_factory=lambda: int(os.getenv("ETL_BATCH_SIZE", "100")))
    sql_dir: Path = PROJECT_ROOT / "sql"
    reference_dir: Path = PROJECT_ROOT / "reference"

    def dsn(self) -> str:
        # An empty DSN makes libpq fall back to PGHOST / PGUSER / PGPASSWORD / PGDATABASE ...
        return self.database_url or ""
