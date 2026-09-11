"""
test_airflow_dag.py — Tests for the Phase 7 Airflow DAG.

Two kinds of test here:
  1. Structural: does the DAG parse, does it have the right tasks,
     dependencies, retries, and timeouts (fast, no external services).
  2. Behavioral: do the task callables that touch Postgres actually
     work and actually fail correctly (skipped if Postgres isn't
     reachable, same pattern as test_database_schema.py).

Run with:  pytest tests/test_airflow_dag.py -v

Requires apache-airflow to be installed (`pip install apache-airflow`,
see README) — skipped entirely otherwise, since Airflow is a heavy
dependency this project doesn't want to force on every contributor.
"""
from __future__ import annotations

import os
import sys
from datetime import timedelta
from pathlib import Path

import pytest

pytest.importorskip("airflow")

DAGS_FOLDER = str(Path(__file__).resolve().parent.parent / "airflow" / "dags")
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


@pytest.fixture(scope="module")
def dagbag():
    from airflow.models import DagBag
    return DagBag(dag_folder=DAGS_FOLDER, include_examples=False)


@pytest.fixture(scope="module")
def dag(dagbag):
    return dagbag.get_dag("customer_360_pipeline")


# --------------------------------------------------------------------
# Structural checks
# --------------------------------------------------------------------
def test_dag_has_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_dag_is_found(dag):
    assert dag is not None


def test_dag_has_expected_top_level_tasks(dag):
    task_ids = {t.task_id for t in dag.tasks}
    expected = {
        "check_source_data", "validate_data", "spark_transform",
        "build_customer_360", "build_rfm_and_segments", "load_postgresql",
        "data_quality_check", "pipeline_summary",
    }
    assert expected <= task_ids


def test_dag_has_one_ingest_task_per_raw_dataset(dag):
    from data_lake.lake_paths import RAW_DATASETS
    ingest_task_ids = {t.task_id for t in dag.tasks if t.task_id.startswith("ingest_raw_to_lake.ingest_")}
    assert len(ingest_task_ids) == len(RAW_DATASETS)


def test_start_pipeline_run_is_the_first_task(dag):
    """Phase 14: monitoring needs a run_id created before any other
    task's success/failure callback tries to record a step against it."""
    start_task = dag.get_task("start_pipeline_run")
    check_task = dag.get_task("check_source_data")
    assert start_task.upstream_list == []  # nothing runs before it
    assert check_task.task_id in [d.task_id for d in start_task.downstream_list]


def test_every_task_has_monitoring_callbacks_configured(dag):
    """on_success_callback/on_failure_callback are set once in
    DEFAULT_ARGS (not per-task), so every task should have them —
    this is what makes Phase 14 monitoring automatic instead of
    something each task has to remember to call."""
    for task in dag.tasks:
        assert task.on_success_callback is not None, f"{task.task_id} has no on_success_callback"
        assert task.on_failure_callback is not None, f"{task.task_id} has no on_failure_callback"


def test_ingest_tasks_run_in_parallel_not_chained(dag):
    """The 9 ingest tasks should have no dependencies on each other —
    only on check_source_data upstream and validate_data downstream."""
    ingest_tasks = [t for t in dag.tasks if t.task_id.startswith("ingest_raw_to_lake.ingest_")]
    for t in ingest_tasks:
        upstream_ids = {u.task_id for u in t.upstream_list}
        downstream_ids = {d.task_id for d in t.downstream_list}
        other_ingest_ids = {o.task_id for o in ingest_tasks if o.task_id != t.task_id}
        assert not (upstream_ids & other_ingest_ids)
        assert not (downstream_ids & other_ingest_ids)


def test_task_dependency_order(dag):
    def task(tid):
        return dag.get_task(tid)

    check = task("check_source_data")
    validate = task("validate_data")
    transform = task("spark_transform")
    c360 = task("build_customer_360")
    rfm = task("build_rfm_and_segments")
    load = task("load_postgresql")
    dq = task("data_quality_check")
    summary = task("pipeline_summary")

    assert "ingest_raw_to_lake.ingest_customers" in [d.task_id for d in check.downstream_list]
    assert validate.task_id in [d.task_id for d in task("ingest_raw_to_lake.ingest_customers").downstream_list]
    assert transform.task_id in [d.task_id for d in validate.downstream_list]
    assert c360.task_id in [d.task_id for d in transform.downstream_list]
    assert rfm.task_id in [d.task_id for d in c360.downstream_list]
    assert load.task_id in [d.task_id for d in rfm.downstream_list]
    assert dq.task_id in [d.task_id for d in load.downstream_list]
    assert summary.task_id in [d.task_id for d in dq.downstream_list]


def test_retries_and_timeouts_are_configured(dag):
    for task in dag.tasks:
        assert task.retries is not None and task.retries >= 1, f"{task.task_id} has no retries configured"
        assert task.execution_timeout is not None, f"{task.task_id} has no execution_timeout configured"


def test_spark_transform_has_longer_timeout_than_default(dag):
    """Spark jobs are the slowest step (Section 9's own performance
    notes), especially at `large` scale — its timeout should reflect that."""
    spark_task = dag.get_task("spark_transform")
    default_task = dag.get_task("check_source_data")
    assert spark_task.execution_timeout > default_task.execution_timeout


def test_dag_does_not_allow_concurrent_runs(dag):
    """The warehouse load is a full-refresh (TRUNCATE ... CASCADE) —
    two concurrent runs racing on that would corrupt the load."""
    assert dag.max_active_runs == 1


def test_pipeline_summary_runs_even_after_upstream_failure(dag):
    summary = dag.get_task("pipeline_summary")
    assert summary.trigger_rule == "all_done"


def test_dag_is_not_backfilling_by_default(dag):
    assert dag.catchup is False


# --------------------------------------------------------------------
# Behavioral checks (require Postgres; skip gracefully otherwise)
# --------------------------------------------------------------------
def _pg_available() -> bool:
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=os.environ.get("POSTGRES_HOST", "localhost"),
            port=os.environ.get("POSTGRES_PORT", "5432"),
            dbname=os.environ.get("POSTGRES_DB", "customer360"),
            user=os.environ.get("POSTGRES_USER", "customer360_app"),
            password=os.environ.get("POSTGRES_PASSWORD", ""),
        )
        conn.close()
        return True
    except Exception:
        return False


@pytest.mark.skipif(not _pg_available(), reason="Postgres not reachable")
def test_data_quality_check_callable_raises_on_empty_warehouse(monkeypatch):
    """If the warehouse is genuinely empty, the post-load smoke test
    must raise, not silently succeed — this is the difference between
    a check that actually gates the pipeline and one that's decorative.

    IMPORTANT: this test truncates the real warehouse to simulate the
    empty case, and the check function it calls uses its own separate
    DB connection — so the truncate has to be committed for that
    connection to see it, meaning it can't be undone with a simple
    rollback. It MUST restore real data in a `finally` block (by
    re-running the actual warehouse loader against whatever raw/
    processed data is on disk), or every test file that runs
    afterward and assumes a populated warehouse — the analytics query
    suite, most obviously — silently starts failing for a reason that
    has nothing to do with its own code. Found exactly that way: this
    test originally didn't restore anything, and 9 unrelated query
    tests started failing the next time the full suite ran."""
    from spark.jobs.load_warehouse import get_connection, load_warehouse

    # Import the task module directly by path since it's not an installed package
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "customer_360_pipeline_dag", Path(DAGS_FOLDER) / "customer_360_pipeline.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE warehouse.dim_customer CASCADE")
    conn.commit()
    conn.close()

    try:
        with pytest.raises(ValueError):
            module._data_quality_check(ds="2026-09-06")
    finally:
        # Restore real data so subsequent test files (analytics
        # queries, customer_360, rfm/clv/retention) don't inherit an
        # empty warehouse from this test's deliberate truncate.
        base_dir = os.environ.get("C360_DATA_DIR", "data")
        if Path(base_dir, "processed", "customers").exists():
            load_warehouse(base_dir=base_dir)
