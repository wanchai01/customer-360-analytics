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
    q: str | None = Query(default=None, description="Search by customer_id or customer_name (substring)"),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
):
    sql = "SELECT * FROM analytics.customer_360 WHERE 1=1"
    params: dict = {"limit": limit, "offset": offset}
    if segment:
        sql += " AND customer_segment = :segment"
        params["segment"] = segment
    if q:
        sql += " AND (customer_id ILIKE :q OR customer_name ILIKE :q)"
        params["q"] = f"%{q}%"
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


@app.get("/customers/{customer_id}/orders")
def customer_orders(customer_id: str, limit: int = Query(default=20, ge=1, le=200)):
    return run_query(
        """
        SELECT fo.order_id, fo.order_date, fo.order_status, fo.payment_method, fo.total_amount
        FROM warehouse.fact_orders fo
        JOIN warehouse.dim_customer dc ON dc.customer_key = fo.customer_key
        WHERE dc.customer_id = :customer_id
        ORDER BY fo.order_date DESC
        LIMIT :limit
        """,
        {"customer_id": customer_id, "limit": limit},
    )


@app.get("/customers/{customer_id}/top-products")
def customer_top_products(customer_id: str, limit: int = Query(default=5, ge=1, le=50)):
    return run_query(
        """
        SELECT dp.product_name, dp.category, COUNT(*) AS order_count, SUM(foi.quantity) AS total_quantity
        FROM warehouse.fact_order_items foi
        JOIN warehouse.fact_orders fo ON fo.order_key = foi.order_key
        JOIN warehouse.dim_customer dc ON dc.customer_key = fo.customer_key
        JOIN warehouse.dim_product dp ON dp.product_key = foi.product_key
        WHERE dc.customer_id = :customer_id
        GROUP BY dp.product_name, dp.category
        ORDER BY order_count DESC, total_quantity DESC
        LIMIT :limit
        """,
        {"customer_id": customer_id, "limit": limit},
    )


@app.get("/customers/{customer_id}/website-activity")
def customer_website_activity(customer_id: str, limit: int = Query(default=10, ge=1, le=100)):
    devices = run_query(
        """
        SELECT COALESCE(dd.device_name, 'Unknown') AS device_name, COUNT(*) AS event_count
        FROM warehouse.fact_website_events fwe
        JOIN warehouse.dim_customer dc ON dc.customer_key = fwe.customer_key
        LEFT JOIN warehouse.dim_device dd ON dd.device_key = fwe.device_key
        WHERE dc.customer_id = :customer_id
        GROUP BY dd.device_name
        ORDER BY event_count DESC
        """,
        {"customer_id": customer_id},
    )
    recent_events = run_query(
        """
        SELECT fwe.event_timestamp, fwe.event_type, fwe.page, fwe.traffic_source
        FROM warehouse.fact_website_events fwe
        JOIN warehouse.dim_customer dc ON dc.customer_key = fwe.customer_key
        WHERE dc.customer_id = :customer_id
        ORDER BY fwe.event_timestamp DESC
        LIMIT :limit
        """,
        {"customer_id": customer_id, "limit": limit},
    )
    return {"devices": devices, "recent_events": recent_events}


@app.get("/rfm")
def list_rfm(limit: int = Query(default=50, ge=1, le=500), offset: int = Query(default=0, ge=0)):
    return run_query(
        "SELECT * FROM analytics.customer_rfm ORDER BY rfm_sum DESC LIMIT :limit OFFSET :offset",
        {"limit": limit, "offset": offset},
    )


@app.get("/rfm/distribution")
def rfm_distribution():
    """5x5 grid of customer counts by recency-score x monetary-score, for a
    heatmap view (never-purchased customers, whose scores are fixed at 1,1,1,
    are excluded since they'd all pile into a single cell)."""
    return run_query(
        """
        SELECT r_score, m_score, COUNT(*) AS customer_count
        FROM analytics.customer_rfm
        WHERE frequency > 0
        GROUP BY r_score, m_score
        ORDER BY r_score, m_score
        """
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


@app.get("/revenue-by-category")
def revenue_by_category():
    return run_query(
        """
        SELECT dp.category,
               ROUND(SUM(foi.quantity * foi.unit_price), 2) AS revenue,
               COUNT(DISTINCT fo.order_id) AS order_count
        FROM warehouse.fact_order_items foi
        JOIN warehouse.fact_orders fo ON fo.order_key = foi.order_key
        JOIN warehouse.dim_product dp ON dp.product_key = foi.product_key
        WHERE fo.order_status = 'completed'
        GROUP BY dp.category
        ORDER BY revenue DESC
        """
    )


@app.get("/behavior/devices")
def behavior_devices():
    return run_query(
        """
        SELECT COALESCE(dd.device_name, 'Unknown') AS device_name, COUNT(*) AS event_count
        FROM warehouse.fact_website_events fwe
        LEFT JOIN warehouse.dim_device dd ON dd.device_key = fwe.device_key
        GROUP BY dd.device_name
        ORDER BY event_count DESC
        """
    )


@app.get("/behavior/traffic-sources")
def behavior_traffic_sources():
    return run_query(
        """
        SELECT COALESCE(traffic_source, 'Unknown') AS traffic_source, COUNT(*) AS event_count
        FROM warehouse.fact_website_events
        GROUP BY traffic_source
        ORDER BY event_count DESC
        """
    )


@app.get("/behavior/top-products")
def behavior_top_products(limit: int = Query(default=10, ge=1, le=50)):
    return run_query(
        """
        SELECT dp.product_name, COUNT(*) AS view_count
        FROM warehouse.fact_website_events fwe
        JOIN warehouse.dim_product dp ON dp.product_key = fwe.product_key
        WHERE fwe.event_type = 'product_view'
        GROUP BY dp.product_name
        ORDER BY view_count DESC
        LIMIT :limit
        """,
        {"limit": limit},
    )


@app.get("/behavior/events-trend")
def behavior_events_trend():
    return run_query(
        """
        SELECT date_trunc('month', event_timestamp)::date AS month, COUNT(*) AS event_count
        FROM warehouse.fact_website_events
        GROUP BY 1
        ORDER BY 1
        """
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
