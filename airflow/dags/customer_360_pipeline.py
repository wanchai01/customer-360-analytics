"""
customer_360_pipeline.py — The end-to-end orchestration DAG.

Task list matches Section 8 of the spec exactly (plus one Phase-14
addition, `start_pipeline_run`, at the very front — see Monitoring
below):
    start_pipeline_run -> check_source_data -> ingest_customers/orders/
    products/events (+ 5 more, parallel TaskGroup) -> validate_data ->
    spark_transform -> build_customer_360 -> build_rfm_and_segments ->
    load_postgresql -> data_quality_check -> pipeline_summary

Two tasks are intentionally implemented as clearly-marked placeholders
right now: `build_customer_360` and `build_rfm_and_segments`. Those are
Phase 8/9 deliverables in this project's own phased build order
(Section 27), which come *after* this Airflow phase — so the scripts
they will eventually call don't exist yet. Rather than leave the DAG's
task list incomplete or restructure it later, the task shape is final
today and only the callable body will change (from a logged stub to a
real script invocation) once those phases land — no DAG rewrite needed.

Idempotency: every step keys off `{{ ds }}` (the DAG run's logical
date) as `batch_date`, and every underlying script this DAG calls
(data_generator, sync_to_lake, load_staging, Spark jobs, load_warehouse)
was built in earlier phases to overwrite-by-batch-date or full-refresh
rather than append — so re-running any task, or the whole DAG, for the
same date does not duplicate data.

Monitoring (Section 23, Phase 14): `on_success_callback` /
`on_failure_callback` are set once in `default_args`, so every task
automatically gets its execution time, status, and any error message
persisted to `analytics.pipeline_run_steps` — no per-task monitoring
code scattered through the business logic. `start_pipeline_run`
creates the run row and pushes `run_id` via XCom; every other task's
callback reads that same `run_id` back to attribute its step to the
right run; `pipeline_summary` (already `trigger_rule="all_done"`,
so it runs even after an upstream failure) closes out the run with a
single readable summary line.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

logger = logging.getLogger("airflow.task")


def _on_task_success(context) -> None:
    _record_task_outcome(context, status="SUCCESS")


def _on_task_failure(context) -> None:
    _record_task_outcome(context, status="FAILED")


def _record_task_outcome(context, status: str) -> None:
    """Shared by on_success_callback/on_failure_callback (set in
    DEFAULT_ARGS below, so this runs for every task automatically).
    Reads run_id back from the start_pipeline_run task's XCom rather
    than needing every task to know about monitoring itself."""
    from monitoring.pipeline_monitor import record_step
    from airflow.utils.timezone import utcnow

    ti = context["task_instance"]
    run_id = ti.xcom_pull(task_ids="start_pipeline_run")
    if run_id is None:
        # start_pipeline_run itself failing before it can push an
        # XCom is the one case with nothing to attribute a step to —
        # log it plainly rather than raising a second failure out of
        # the failure-handling path itself.
        logger.error("No run_id available (start_pipeline_run may have failed) — cannot record step %s", ti.task_id)
        return

    exception = context.get("exception")
    record_step(
        run_id=run_id,
        step_name=ti.task_id,
        # ti.start_date/end_date are timezone-aware (Airflow uses UTC
        # throughout) — the fallback for a missing value must be too,
        # or subtracting the two raises "can't subtract offset-naive
        # and offset-aware datetimes". Found by actually running this
        # callback through Airflow: end_date can genuinely be None at
        # the moment a success callback fires.
        started_at=ti.start_date or utcnow(),
        ended_at=ti.end_date or utcnow(),
        status=status,
        records_processed=None,  # per-step record counts are logged by each task's own business logic; this callback captures timing/status uniformly across every task regardless of what it does
        error_message=str(exception) if exception else None,
    )


DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "execution_timeout": timedelta(minutes=30),
    "on_success_callback": _on_task_success,
    "on_failure_callback": _on_task_failure,
}

BASE_DIR = "/opt/data"
INGEST_DATASETS = [
    "customers", "products", "orders", "order_items", "payments",
    "website_events", "customer_support", "marketing_campaigns", "campaign_interactions",
]


# --------------------------------------------------------------------
# Task callables — each is a thin wrapper around a script this project
# already built and tested in an earlier phase. Keeping the Airflow
# layer thin (no business logic here, just "call the real function
# with today's batch_date") means every one of these was already unit-
# /integration-tested in its own phase's test suite, and this DAG only
# needs to test that the *wiring* is correct.
# --------------------------------------------------------------------
def _start_pipeline_run(ds: str, **context) -> int:
    """First task in the DAG: creates the pipeline_runs row and
    returns run_id (auto-pushed to XCom under 'return_value', read
    back by _record_task_outcome for every other task and by
    _pipeline_summary at the end)."""
    from monitoring.pipeline_monitor import start_run

    dag_run_id = context["dag_run"].run_id if context.get("dag_run") else None
    return start_run(batch_date=ds, dag_run_id=dag_run_id)


def _check_source_data(ds: str, **_):
    """Verify every raw file this DAG depends on exists and is non-empty
    for {{ ds }} — fails fast with a clear message instead of letting a
    missing-file error surface confusingly deep inside a later task."""
    from pathlib import Path

    from data_lake.lake_paths import RAW_DATASETS, local_raw_path

    missing = []
    for dataset in RAW_DATASETS:
        path = Path(local_raw_path(BASE_DIR, dataset, ds))
        if not path.exists() or path.stat().st_size == 0:
            missing.append(dataset)

    if missing:
        raise FileNotFoundError(
            f"Missing or empty raw files for batch_date={ds}: {missing}. "
            f"Run data_generator.generate_all for this date first."
        )
    logger.info("All %s raw datasets present for batch_date=%s", len(RAW_DATASETS), ds)


def _ingest_dataset(dataset: str, ds: str, **_):
    """Upload one dataset's raw file to the S3/MinIO data lake."""
    from data_lake.s3_client import ensure_bucket, get_bucket_name, get_s3_client
    from data_lake.lake_paths import local_raw_path, raw_key

    client = get_s3_client()
    bucket = get_bucket_name()
    ensure_bucket(client, bucket)
    local_path = local_raw_path(BASE_DIR, dataset, ds)
    key = raw_key(dataset, ds)
    client.upload_file(local_path, bucket, key)
    logger.info("Ingested %s -> s3://%s/%s", dataset, bucket, key)


def _validate_data(ds: str, **_):
    """Load raw data into staging, then run the Data Quality gate.
    Raises (failing the task, and the DAG run) if any critical check
    is below threshold — this is the "pipeline must fail on bad data"
    requirement from Section 22, enforced as early as possible rather
    than after the expensive Spark transform has already run."""
    from data_quality.checks import run_all_checks
    from data_quality.load_staging import load_all

    load_all(BASE_DIR, ds)
    results, overall_passed = run_all_checks(ds, report_dir=f"{BASE_DIR}/../data_quality/reports")
    if not overall_passed:
        failed = [f"{r.dataset}.{r.check_name}" for r in results if r.status == "FAIL" and r.critical]
        raise ValueError(f"Data Quality gate failed for batch_date={ds}: {failed}")
    logger.info("Data Quality gate passed for batch_date=%s (%s checks)", ds, len(results))


def _spark_transform(ds: str, **_):
    """Run every Spark cleaning/transform job for this batch_date."""
    from spark.jobs.run_all import run_all

    row_counts = run_all(ds, base_dir=BASE_DIR)
    logger.info("Spark transform complete for batch_date=%s: %s", ds, row_counts)


def _build_customer_360(ds: str, **_):
    """Builds analytics.customer_360 from the warehouse Star Schema.
    Full-refresh (TRUNCATE + rebuild), matching this project's
    full-refresh model — see spark/jobs/build_customer_360.py for the
    business definitions behind each column."""
    from spark.jobs.build_customer_360 import build_customer_360

    row_count = build_customer_360()
    logger.info("customer_360 built for batch_date=%s: %s rows", ds, row_count)


def _build_rfm_and_segments(ds: str, **_):
    """Builds RFM scores + segments (back-filling customer_360),
    monthly retention/churn + cohort matrix, and Estimated CLV — in
    that order, since build_clv.py's lifespan model depends on the
    churn rate build_retention.py computes."""
    from spark.jobs.build_rfm import build_rfm
    from spark.jobs.build_retention import build_retention
    from spark.jobs.build_clv import build_clv

    rfm_result = build_rfm()
    retention_result = build_retention()
    clv_result = build_clv()
    logger.info(
        "RFM/retention/CLV built for batch_date=%s: rfm=%s retention=%s clv=%s",
        ds, rfm_result, retention_result, clv_result,
    )


def _load_postgresql(ds: str, **_):
    """Load cleaned processed Parquet into the warehouse Star Schema."""
    from spark.jobs.load_warehouse import load_warehouse

    load_warehouse(base_dir=BASE_DIR)
    logger.info("Warehouse load complete (triggered by batch_date=%s)", ds)


def _data_quality_check(ds: str, **_):
    """Post-load smoke test on the warehouse — distinct in scope from
    `validate_data` (which checks raw/staging *before* transformation):
    this checks that the load into `warehouse.*` actually produced a
    sane, non-empty, referentially-consistent result."""
    from spark.jobs.load_warehouse import get_connection

    checks = {
        "dim_customer has rows": "SELECT COUNT(*) FROM warehouse.dim_customer",
        "dim_date is populated": "SELECT COUNT(*) FROM warehouse.dim_date",
        "fact_orders has rows": "SELECT COUNT(*) FROM warehouse.fact_orders",
        "fact_orders has no null customer_key": "SELECT COUNT(*) FROM warehouse.fact_orders WHERE customer_key IS NULL",
    }
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            for description, sql in checks.items():
                cur.execute(sql)
                (value,) = cur.fetchone()
                logger.info("[post-load smoke test] %s -> %s", description, value)
                if "no null" in description and value > 0:
                    raise ValueError(f"Post-load smoke test failed: {description} -> {value}")
                if "has rows" in description and value == 0:
                    raise ValueError(f"Post-load smoke test failed: {description} -> {value}")
    finally:
        conn.close()


def _pipeline_summary(ds: str, **context):
    """Pulls the Spark row counts via XCom, determines overall status
    from whether any step so far recorded a failure, and closes out
    the monitored run (Phase 14) with one readable summary line —
    exactly the kind of thing that's genuinely useful in the Airflow
    UI's task log when triaging a run days later."""
    from monitoring.pipeline_monitor import complete_run, get_connection

    ti = context["ti"]
    row_counts = ti.xcom_pull(task_ids="spark_transform") or {}
    run_id = ti.xcom_pull(task_ids="start_pipeline_run")

    if run_id is not None:
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM analytics.pipeline_run_steps WHERE run_id = %s AND status = 'FAILED'",
                    (run_id,),
                )
                (failed_step_count,) = cur.fetchone()
        finally:
            conn.close()
        overall_status = "FAILED" if failed_step_count > 0 else "SUCCESS"
        summary = complete_run(run_id, status=overall_status)
        logger.info("=== Pipeline summary for batch_date=%s ===\nSpark ETL row counts: %s\nRun summary: %s", ds, row_counts, summary)
    else:
        logger.warning("=== Pipeline summary for batch_date=%s: no run_id found, monitoring was not recorded for this run ===\nSpark ETL row counts: %s", ds, row_counts)


with DAG(
    dag_id="customer_360_pipeline",
    description="End-to-end Customer 360 pipeline: ingest -> validate -> transform -> load -> quality gate",
    default_args=DEFAULT_ARGS,
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,  # this pipeline's full-refresh warehouse load is not safe to run concurrently with itself
    tags=["customer-360", "portfolio-project"],
) as dag:

    start_pipeline_run = PythonOperator(
        task_id="start_pipeline_run",
        python_callable=_start_pipeline_run,
        op_kwargs={"ds": "{{ ds }}"},
    )

    check_source_data = PythonOperator(
        task_id="check_source_data",
        python_callable=_check_source_data,
        op_kwargs={"ds": "{{ ds }}"},
    )

    with TaskGroup(group_id="ingest_raw_to_lake") as ingest_group:
        ingest_tasks = [
            PythonOperator(
                task_id=f"ingest_{dataset}",
                python_callable=_ingest_dataset,
                op_kwargs={"dataset": dataset, "ds": "{{ ds }}"},
            )
            for dataset in INGEST_DATASETS
        ]
        # All 9 ingest tasks run in parallel — they're independent
        # uploads with no dependency on each other, so there's no
        # reason to serialize them and every reason (wall-clock time)
        # not to.

    validate_data = PythonOperator(
        task_id="validate_data",
        python_callable=_validate_data,
        op_kwargs={"ds": "{{ ds }}"},
        execution_timeout=timedelta(minutes=15),
    )

    spark_transform = PythonOperator(
        task_id="spark_transform",
        python_callable=_spark_transform,
        op_kwargs={"ds": "{{ ds }}"},
        execution_timeout=timedelta(hours=1),  # Spark jobs are the slowest step, especially at `large` scale
        retries=1,  # a flaky Spark executor OOM is worth one retry; a real bug is not worth three
    )

    build_customer_360 = PythonOperator(
        task_id="build_customer_360",
        python_callable=_build_customer_360,
        op_kwargs={"ds": "{{ ds }}"},
    )

    build_rfm_and_segments = PythonOperator(
        task_id="build_rfm_and_segments",
        python_callable=_build_rfm_and_segments,
        op_kwargs={"ds": "{{ ds }}"},
    )

    load_postgresql = PythonOperator(
        task_id="load_postgresql",
        python_callable=_load_postgresql,
        op_kwargs={"ds": "{{ ds }}"},
        execution_timeout=timedelta(minutes=30),
    )

    data_quality_check = PythonOperator(
        task_id="data_quality_check",
        python_callable=_data_quality_check,
        op_kwargs={"ds": "{{ ds }}"},
    )

    pipeline_summary = PythonOperator(
        task_id="pipeline_summary",
        python_callable=_pipeline_summary,
        op_kwargs={"ds": "{{ ds }}"},
        trigger_rule="all_done",  # log a summary even if an upstream task failed, so failures are visible in one place
    )

    (
        start_pipeline_run
        >> check_source_data
        >> ingest_group
        >> validate_data
        >> spark_transform
        >> build_customer_360
        >> build_rfm_and_segments
        >> load_postgresql
        >> data_quality_check
        >> pipeline_summary
    )
