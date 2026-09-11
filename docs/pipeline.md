# Pipeline

End-to-end data flow, phase by phase, with the actual command/module
that performs each step — this is the "how does data actually move
through this system" reference; see `docs/data_model.md` for *what*
the data looks like at each stage instead.

## Flow

```
1. data_generator.generate_all        (Phase 2)
   -> writes data/raw/<dataset>/batch_date=.../  (CSV/JSON, intentionally dirty)

2. data_lake.sync_to_lake              (Phase 4)
   -> uploads raw/ to S3 (MinIO locally)

3. data_quality.load_staging           (Phase 6)
   -> loads raw data into Postgres staging.* (still dirty, as-is)

4. data_quality.run_checks             (Phase 6)
   -> runs 23 checks against staging.*, writes analytics.data_quality_results
   -> FAILS THE PIPELINE (non-zero exit) if any critical check < 95% pass rate

5. spark.jobs.run_all                  (Phase 5)
   -> reads raw/, cleans + transforms with PySpark, writes data/processed/ (Parquet)

6. spark.jobs.load_warehouse           (Phase 7)
   -> reads processed/ Parquet, full-refresh loads warehouse.dim_*/fact_*

7. spark.jobs.build_customer_360       (Phase 8)
   -> one SQL statement against warehouse.*, builds analytics.customer_360

8. spark.jobs.build_rfm                (Phase 9)
   -> RFM scores + 9 segments, back-fills customer_360.rfm_score/customer_segment

9. spark.jobs.build_retention          (Phase 9)
   -> monthly active/new/returning/retention/churn, cohort matrix

10. spark.jobs.build_clv               (Phase 9)
    -> Estimated CLV using build_retention's churn rate

11. (Power BI connects to warehouse.*/analytics.* directly — Phase 11)
```

Steps 3-10 are exactly what `airflow/dags/customer_360_pipeline.py`
(Phase 7) orchestrates, with Phase 14's monitoring recording every
step's timing/status automatically along the way.

## Idempotency, end to end

Every single step above is safe to re-run for the same `batch_date`
without duplicating data — this was a deliberate, consistent design
choice from Phase 2 onward, not something bolted on later:

| Step | Idempotency mechanism |
|------|------------------------|
| `generate_all` | Overwrites the same `batch_date=` partition file |
| `sync_to_lake` | S3 `PutObject` has no "already exists" failure — same key, new content |
| `load_staging` | `DELETE ... WHERE _batch_date = %s` before `COPY` |
| `run_checks` | `DELETE ... WHERE batch_date = %s` before inserting fresh results |
| `run_all` (Spark) | `coalesce().write.mode("overwrite")` on every output path |
| `load_warehouse` | `TRUNCATE ... CASCADE` (full refresh) before reload |
| `build_customer_360`/`build_rfm`/`build_retention`/`build_clv` | `TRUNCATE` before `INSERT` |

## Running it yourself

**Full pipeline, locally, one command per step** (see each phase's
README section for `docker compose exec etl ...` equivalents):

```bash
python -m data_generator.generate_all --scale test --batch-date 2026-09-06 --out-dir data
python -m data_lake.sync_to_lake --batch-date 2026-09-06 --base-dir data
python -m data_quality.load_staging --batch-date 2026-09-06 --base-dir data
python -m data_quality.run_checks --batch-date 2026-09-06
python -m spark.jobs.run_all --batch-date 2026-09-06 --base-dir data
python -m spark.jobs.load_warehouse --base-dir data
python -m spark.jobs.build_customer_360
python -m spark.jobs.build_rfm
python -m spark.jobs.build_retention
python -m spark.jobs.build_clv
```

**Or via Airflow** (Phase 7), which runs the same steps automatically
on a daily schedule with retries, timeouts, and monitoring:

```bash
docker compose up -d --build airflow-webserver airflow-scheduler
# trigger customer_360_pipeline from http://localhost:8080, or wait for @daily
```

## What happens on failure

- **A Data Quality gate failure** (step 4) stops the pipeline before
  the expensive Spark transform ever runs — `validate_data` raises,
  Airflow retries it up to twice (`DEFAULT_ARGS["retries"] = 2`), and
  if it still fails, the DAG run is marked failed. `pipeline_summary`
  still runs (`trigger_rule="all_done"`) and logs the failure via
  Phase 14's monitoring.
- **A Spark job failure** gets one retry (`spark_transform` overrides
  the default retry count down to 1 — a transient executor OOM is
  worth retrying once; a real bug in the transform logic isn't worth
  three attempts at the same wrong answer).
- **Every task's outcome** — success or failure, with duration and
  any error message — lands in `analytics.pipeline_run_steps`
  (Phase 14) regardless of which step failed, so a failed run is
  fully diagnosable from that one table without needing to dig
  through Airflow's own logs first.
