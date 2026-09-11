# Architecture

## System diagram (local development)

```
data_generator/  --->  data/raw/ (CSV/JSON)  --->  data_lake/ (S3/MinIO)
                              |
                              v
                    data_quality/load_staging.py
                              |
                              v
                   Postgres: staging.* (dirty, as-is)
                              |
                    data_quality/run_checks.py  ---> analytics.data_quality_results
                              |  (gate: fails pipeline if < 95% pass rate)
                              v
                    spark/jobs/*.py (PySpark clean + transform)
                              |
                              v
                    data/processed/ (Parquet)
                              |
                    spark/jobs/load_warehouse.py
                              |
                              v
                Postgres: warehouse.dim_*/fact_* (Star Schema)
                              |
        spark/jobs/build_customer_360.py / build_rfm.py /
        build_retention.py / build_clv.py
                              |
                              v
                   Postgres: analytics.* (Gold layer)
                              |
                              v
                        Power BI dashboards

  Orchestrated end-to-end by: airflow/dags/customer_360_pipeline.py
  Every step's timing/status recorded by: monitoring/pipeline_monitor.py
```

For the AWS production version of this same diagram (S3, RDS, EMR,
MWAA in place of the local containers), see
[`../aws/architecture.md`](../aws/architecture.md) — the two are
deliberately drawn to mirror each other component-for-component.

## Design principles behind this architecture

**Local-first, cloud-ready, without two codebases.** Every piece of
business logic (data generation, cleaning, RFM scoring, ...) is plain
Python/PySpark/SQL that runs identically whether the "S3" it's
talking to is MinIO on `localhost:9000` or the real AWS S3, and
whether "Postgres" is the local Docker container or an RDS endpoint.
The environment-specific difference is always exactly one or two
environment variables (`AWS_S3_ENDPOINT_URL`, `POSTGRES_HOST`) — see
`data_lake/s3_client.py`'s docstring for where this pattern was first
established and why.

**Medallion layering (raw -> staging/processed -> warehouse -> Gold),
each layer with exactly one job.** Raw is an untouched record of what
arrived. Staging exists specifically so dirty data has somewhere to
be measured (Phase 6) before it's ever cleaned. Processed is cleaned
but still shaped like the source. Warehouse is the general-purpose
Star Schema. Gold is pre-computed answers to specific business
questions. A query or a bug is always attributable to exactly one
layer's job, not a tangle of "cleaning and business logic in the same
step."

**Full-refresh over incremental upsert.** Revisited in
`docs/data_model.md` and `aws/rds.md` — the short version: this
project's synthetic data generator produces a complete history each
run, so a full refresh matches the data's actual shape, and it also
turns "the warehouse is in a bad state" into a solved problem ("just
re-run the pipeline") rather than a debugging session.

**Orchestration is a thin wrapper, not where logic lives.** The
Airflow DAG's task callables are short — each one just imports and
calls a function from `data_generator`, `spark.jobs`, `data_quality`,
etc. Every one of those modules is independently runnable and
independently tested (146 tests, Phase 13) without Airflow in the
loop at all. This is why Phase 14's monitoring could be added as
`on_success_callback`/`on_failure_callback` in one place instead of
touching business logic: the DAG layer and the logic layer were kept
separate from the start.

**Data quality is a gate, not a report.** `data_quality.run_checks`
exits non-zero and the Airflow task raises when quality is below
threshold — verified three separate ways in Phase 13's test suite
(unit-level, CLI exit code, and through Airflow's own retry/failure
machinery), because a check that only logs a warning isn't actually
protecting anything downstream.

## Where each phase's work lives

| Concern | Primary location |
|---------|-------------------|
| Synthetic data generation | `data_generator/` |
| Database schema (DDL) | `sql/ddl/`, `sql/staging/`, `sql/warehouse/`, `sql/analytics/` |
| S3/data lake | `data_lake/` |
| Spark ETL | `spark/jobs/`, `spark/utils/` |
| Data quality | `data_quality/` |
| Orchestration | `airflow/dags/` |
| Business logic queries | `sql/analytics/queries/` |
| Power BI design | `powerbi/` |
| AWS design | `aws/` |
| Monitoring | `monitoring/` |
| Tests | `tests/` |
