"""
ddl_deploy.py — Applies every DDL file in this project against
PostgreSQL, in the correct dependency order, using the same
POSTGRES_* environment variables as the rest of the platform.

Usage:
    python -m sql.ddl_deploy

This is a thin wrapper around `psql -f`, not a full migration
framework (no version tracking table) — appropriate for this
project's full-refresh model, where `warehouse`/`analytics` tables
are DROP/CREATE'd and repopulated by the Spark jobs on every run
(see the header comment in sql/warehouse/02_facts.sql for why that
trade-off was chosen over incremental upserts at this scale).

For a production system with incremental loads, this would be
replaced with a real migration tool (Alembic, Flyway, sqitch) that
tracks which scripts have already run.
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

# Order matters: schemas first, then staging (no dependencies), then
# warehouse dimensions before facts (facts have FKs to dims), then
# dim_date population (facts reference it), then the analytics layer.
DDL_FILES = [
    "sql/ddl/01_create_schemas.sql",
    "sql/staging/01_staging_tables.sql",
    "sql/warehouse/01_dimensions.sql",
    "sql/warehouse/02_facts.sql",
    "sql/warehouse/03_populate_dim_date.sql",
    "sql/analytics/01_gold_tables.sql",
    "sql/analytics/02_customer_clv.sql",
    "sql/analytics/03_powerbi_views.sql",
    "sql/ddl/02_powerbi_reader_role.sql",
    "sql/analytics/04_pipeline_monitoring.sql",
]


def main() -> int:
    project_root = Path(__file__).resolve().parent.parent
    env = os.environ.copy()
    env["PGPASSWORD"] = env.get("POSTGRES_PASSWORD", "")

    host = env.get("POSTGRES_HOST", "localhost")
    port = env.get("POSTGRES_PORT", "5432")
    db = env.get("POSTGRES_DB", "customer360")
    user = env.get("POSTGRES_USER", "customer360_app")

    for rel_path in DDL_FILES:
        path = project_root / rel_path
        print(f"=== Applying {rel_path} ===")
        result = subprocess.run(
            ["psql", "-h", host, "-p", port, "-U", user, "-d", db,
             "-v", "ON_ERROR_STOP=1", "-f", str(path)],
            env=env,
        )
        if result.returncode != 0:
            print(f"FAILED applying {rel_path} (exit code {result.returncode})", file=sys.stderr)
            return result.returncode

    print("=== All DDL applied successfully ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
