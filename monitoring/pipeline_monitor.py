"""
pipeline_monitor.py — Persists pipeline run/step execution metrics
(Section 23): execution time, records processed, failed records,
task failures, and overall pipeline status.

Designed to be called from Airflow's own `on_success_callback` /
`on_failure_callback` hooks (see `airflow/dags/customer_360_pipeline.py`)
rather than threaded through every task's business-logic function —
this keeps monitoring a cross-cutting concern that wraps tasks from
the outside, instead of scattering `record_step(...)` calls inside
`spark.jobs.run_all`, `data_quality.run_checks`, etc., which would
couple those modules to Airflow for no functional reason (they're all
independently runnable and tested outside Airflow, on purpose).

Usage (see the DAG for the real integration):
    run_id = start_run(batch_date, dag_run_id)
    ...
    record_step(run_id, "spark_transform", started_at, ended_at,
                status="SUCCESS", records_processed=12345)
    ...
    complete_run(run_id, status="SUCCESS")
"""
from __future__ import annotations

import logging
import os
from datetime import datetime

import psycopg2

logger = logging.getLogger("pipeline_monitor")
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")


def get_connection():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ.get("POSTGRES_DB", "customer360"),
        user=os.environ.get("POSTGRES_USER", "customer360_app"),
        password=os.environ.get("POSTGRES_PASSWORD", ""),
    )


def start_run(batch_date: str, dag_run_id: str | None = None) -> int:
    """Records the start of a new pipeline run. Returns the run_id to
    pass to every subsequent record_step()/complete_run() call for
    this run (in Airflow, pushed to XCom by the calling task)."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO analytics.pipeline_runs (batch_date, dag_run_id, status) "
                "VALUES (%s, %s, 'RUNNING') RETURNING run_id",
                (batch_date, dag_run_id),
            )
            (run_id,) = cur.fetchone()
        conn.commit()
        logger.info("Pipeline run started: run_id=%s batch_date=%s dag_run_id=%s", run_id, batch_date, dag_run_id)
        return run_id
    finally:
        conn.close()


def record_step(
    run_id: int,
    step_name: str,
    started_at: datetime,
    ended_at: datetime,
    status: str,
    records_processed: int | None = None,
    error_message: str | None = None,
) -> None:
    """Records one task's execution outcome within a run."""
    duration = (ended_at - started_at).total_seconds()
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO analytics.pipeline_run_steps
                    (run_id, step_name, started_at, ended_at, duration_seconds, status, records_processed, error_message)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (run_id, step_name, started_at, ended_at, duration, status, records_processed, error_message),
            )
        conn.commit()
        log_fn = logger.info if status == "SUCCESS" else logger.error
        log_fn(
            "[run_id=%s] step=%s status=%s duration=%.1fs records=%s%s",
            run_id, step_name, status, duration, records_processed,
            f" error={error_message}" if error_message else "",
        )
    finally:
        conn.close()


def complete_run(run_id: int, status: str) -> dict:
    """
    Marks a run complete, rolls up total records processed/failed
    from its steps, pulls the Data Quality gate's overall status for
    this run's batch_date, and returns a summary dict — this is what
    the DAG's `pipeline_summary` task logs as the final human-readable
    line for the run (Section 23: "readable logging").
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT batch_date, started_at FROM analytics.pipeline_runs WHERE run_id = %s", (run_id,))
            batch_date, started_at = cur.fetchone()

            cur.execute(
                "SELECT COALESCE(SUM(records_processed), 0), "
                "COUNT(*) FILTER (WHERE status = 'FAILED') "
                "FROM analytics.pipeline_run_steps WHERE run_id = %s",
                (run_id,),
            )
            total_processed, failed_steps = cur.fetchone()

            cur.execute(
                "SELECT COALESCE(SUM(failed_records), 0) FROM analytics.data_quality_results WHERE batch_date = %s",
                (batch_date,),
            )
            (total_dq_failed,) = cur.fetchone()

            cur.execute(
                "SELECT CASE WHEN COUNT(*) FILTER (WHERE status = 'FAIL') > 0 THEN 'FAIL' ELSE 'PASS' END "
                "FROM analytics.data_quality_results WHERE batch_date = %s",
                (batch_date,),
            )
            row = cur.fetchone()
            dq_status = row[0] if row else None

            ended_at = datetime.now()
            duration = (ended_at - started_at).total_seconds()

            cur.execute(
                """
                UPDATE analytics.pipeline_runs
                SET ended_at = %s, duration_seconds = %s, status = %s,
                    total_records_processed = %s, total_records_failed = %s, dq_overall_status = %s
                WHERE run_id = %s
                """,
                (ended_at, duration, status, total_processed, total_dq_failed, dq_status, run_id),
            )
        conn.commit()

        summary = {
            "run_id": run_id,
            "batch_date": str(batch_date),
            "status": status,
            "duration_seconds": round(duration, 1),
            "total_records_processed": total_processed,
            "total_records_failed": int(total_dq_failed),
            "dq_overall_status": dq_status,
            "failed_steps": failed_steps,
        }
        logger.info(
            "=== Pipeline run complete: run_id=%s batch_date=%s status=%s duration=%.1fs "
            "records_processed=%s records_failed=%s dq_status=%s failed_steps=%s ===",
            run_id, batch_date, status, duration, total_processed, total_dq_failed, dq_status, failed_steps,
        )
        return summary
    finally:
        conn.close()


def get_recent_runs(limit: int = 10) -> list[dict]:
    """Convenience query for a 'pipeline health' view — the last N
    runs with their status and duration, newest first."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT run_id, batch_date, status, duration_seconds, "
                "total_records_processed, total_records_failed, dq_overall_status "
                "FROM analytics.pipeline_runs ORDER BY started_at DESC LIMIT %s",
                (limit,),
            )
            cols = [d.name for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()
