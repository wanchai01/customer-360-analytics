-- =========================================================
-- 04_pipeline_monitoring.sql — Section 23 monitoring tables.
--
-- Two tables, not one: `pipeline_runs` is the run-level summary (one
-- row per DAG run — status, total duration, overall record counts),
-- `pipeline_run_steps` is the task-level detail (one row per task
-- within a run). Keeping them separate means the common "how did the
-- last 30 runs go" dashboard query never has to aggregate step rows
-- first, while the detailed "why did step X fail on run Y" question
-- is still one join away.
-- =========================================================

SET search_path TO analytics;

DROP TABLE IF EXISTS pipeline_run_steps CASCADE;
DROP TABLE IF EXISTS pipeline_runs CASCADE;

CREATE TABLE pipeline_runs (
    run_id                      SERIAL PRIMARY KEY,
    batch_date                  DATE NOT NULL,
    dag_run_id                  TEXT,               -- Airflow's own run id string; NULL for a manual/non-Airflow run
    started_at                  TIMESTAMP NOT NULL DEFAULT now(),
    ended_at                    TIMESTAMP,
    duration_seconds            NUMERIC(10,2),
    status                      TEXT NOT NULL DEFAULT 'RUNNING',  -- RUNNING | SUCCESS | FAILED
    total_records_processed     BIGINT,
    total_records_failed        BIGINT,
    dq_overall_status           TEXT,               -- PASS | FAIL, pulled from analytics.data_quality_results for this batch_date
    notes                       TEXT
);
CREATE INDEX idx_pipeline_runs_batch_date ON pipeline_runs (batch_date);
CREATE INDEX idx_pipeline_runs_status     ON pipeline_runs (status);

CREATE TABLE pipeline_run_steps (
    step_id             SERIAL PRIMARY KEY,
    run_id              INTEGER NOT NULL REFERENCES pipeline_runs (run_id) ON DELETE CASCADE,
    step_name           TEXT NOT NULL,              -- Airflow task_id, e.g. 'spark_transform'
    started_at          TIMESTAMP,
    ended_at            TIMESTAMP,
    duration_seconds    NUMERIC(10,2),
    status              TEXT NOT NULL,              -- SUCCESS | FAILED
    records_processed   BIGINT,
    error_message       TEXT
);
CREATE INDEX idx_pipeline_run_steps_run    ON pipeline_run_steps (run_id);
CREATE INDEX idx_pipeline_run_steps_status ON pipeline_run_steps (status);
