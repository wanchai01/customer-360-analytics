"""
api/main.py — Read-only analytics API over the `analytics` gold-layer
schema (see sql/analytics/01_gold_tables.sql for table definitions).

This is a thin reporting layer, not part of the ETL pipeline itself:
it only ever runs SELECTs against tables that Airflow/Spark already
populated. Deployed independently of Airflow/Spark (which don't fit a
free web-service host) so the analytics output has something queryable
over HTTP, e.g. from Power BI, a frontend, or curl.
"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

DATABASE_URL = os.environ.get("DATABASE_URL", "")
STATIC_DIR = Path(__file__).parent / "static"

engine: Engine | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global engine
    if DATABASE_URL:
        engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_size=5, max_overflow=5)
    yield
    if engine is not None:
        engine.dispose()


app = FastAPI(
    title="Customer 360 Analytics API",
    description="Read-only API over the customer_360 / RFM / cohort analytics tables.",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/", include_in_schema=False)
def dashboard():
    """Human-facing dashboard (charts/tables over the same endpoints below).
    Kept separate from the JSON API surface, which starts at /health."""
    return FileResponse(STATIC_DIR / "dashboard.html")


def run_query(sql: str, params: dict | None = None) -> list[dict]:
    if engine is None:
        raise HTTPException(status_code=503, detail="Database not configured (DATABASE_URL missing)")
    try:
        with engine.connect() as conn:
            result = conn.execute(text(sql), params or {})
            return [dict(row._mapping) for row in result]
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Database error: {exc}") from exc


@app.get("/health")
def health():
    if engine is None:
        return {"status": "degraded", "database": "not_configured"}
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"status": "healthy", "database": "connected"}
    except Exception as exc:
        return {"status": "degraded", "database": "disconnected", "detail": str(exc)}


@app.get("/customers")
def list_customers(
    segment: str | None = Query(default=None, description="Filter by customer_segment"),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
):
    sql = "SELECT * FROM analytics.customer_360"
    params: dict = {"limit": limit, "offset": offset}
    if segment:
        sql += " WHERE customer_segment = :segment"
        params["segment"] = segment
    sql += " ORDER BY total_spend DESC NULLS LAST LIMIT :limit OFFSET :offset"
    return run_query(sql, params)


@app.get("/customers/{customer_id}")
def get_customer(customer_id: str):
    rows = run_query(
        "SELECT * FROM analytics.customer_360 WHERE customer_id = :customer_id",
        {"customer_id": customer_id},
    )
    if not rows:
        raise HTTPException(status_code=404, detail="Customer not found")
    return rows[0]


@app.get("/rfm")
def list_rfm(limit: int = Query(default=50, ge=1, le=500), offset: int = Query(default=0, ge=0)):
    return run_query(
        "SELECT * FROM analytics.customer_rfm ORDER BY rfm_sum DESC LIMIT :limit OFFSET :offset",
        {"limit": limit, "offset": offset},
    )


@app.get("/segments")
def segment_summary():
    return run_query(
        """
        SELECT customer_segment,
               COUNT(*) AS customer_count,
               ROUND(AVG(customer_lifetime_value), 2) AS avg_clv,
               ROUND(SUM(total_spend), 2) AS total_spend
        FROM analytics.customer_360
        GROUP BY customer_segment
        ORDER BY total_spend DESC NULLS LAST
        """
    )


@app.get("/metrics/monthly")
def monthly_metrics():
    return run_query("SELECT * FROM analytics.customer_monthly_metrics ORDER BY metric_month")


@app.get("/metrics/cohort")
def cohort_metrics():
    return run_query(
        "SELECT * FROM analytics.customer_cohort ORDER BY cohort_month, period_number"
    )


@app.get("/data-quality")
def data_quality(
    dataset: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
):
    sql = "SELECT * FROM analytics.data_quality_results"
    params: dict = {"limit": limit}
    if dataset:
        sql += " WHERE dataset = :dataset"
        params["dataset"] = dataset
    sql += " ORDER BY checked_at DESC LIMIT :limit"
    return run_query(sql, params)
