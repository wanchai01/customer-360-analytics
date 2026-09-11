# Interview Preparation

36 questions across every skill area this project demonstrates, with
answers grounded in what was actually built (not generic textbook
answers) — many reference real bugs found and fixed during
development, since being able to explain a real debugging story is
one of the strongest signals in a technical interview.

---

## SQL

**1. What's the difference between `WHERE` and `HAVING`?**
`WHERE` filters rows before aggregation; `HAVING` filters groups after
`GROUP BY`. In this project's Data Quality checks
(`data_quality/checks.py`), the uniqueness check uses `HAVING`-style
logic implicitly via `COUNT(*) - COUNT(DISTINCT customer_id)` in the
`SELECT` list rather than a literal `HAVING`, since it needs one row
per check, not one row per group.

**2. Explain a window function you used, and why not a subquery instead.**
SQL Analytics query #20 uses `SUM(revenue) OVER (ORDER BY revenue DESC
ROWS UNBOUNDED PRECEDING)` for a cumulative revenue share by segment.
A subquery re-running the same aggregation per row would work but
re-scans the data for every row; the window function computes the
running total in a single pass over the already-grouped result.

**3. How would you find duplicate rows in a table?**
`SELECT key, COUNT(*) FROM t GROUP BY key HAVING COUNT(*) > 1`. This
project's actual uniqueness check computes it slightly differently —
`COUNT(*) - COUNT(DISTINCT key)` — because the DQ framework needs a
single number ("how many rows are duplicates") rather than a list of
duplicate keys, to compute a pass rate.

**4. What's a CTE and why use one over a subquery?**
A `WITH` clause that names an intermediate result. Used throughout
`sql/analytics/queries/` (e.g. query #17's session funnel) purely for
readability — a deeply nested subquery computing the same funnel
would be far harder to read, even though the query planner may
produce an identical execution plan either way.

**5. How do you handle NULLs safely in aggregation?**
`COALESCE(SUM(x), 0)` — found necessary the hard way in this
project's query #19 (customer profitability): a customer whose only
order item had a cleaned/nulled `unit_price` produced `SUM(line_amount)
= NULL` for their entire row, and Postgres sorts `NULL` **first** in
`DESC` order, silently ranking a near-zero-revenue customer #1.

---

## Python

**6. List vs. generator — when does it matter?**
A list holds every value in memory; a generator produces them lazily,
one at a time. This project's `data_generator/` deliberately avoids
both in favor of NumPy/pandas vectorized array operations — even
faster than a generator for this workload, since the whole point is
avoiding a Python-level loop over potentially millions of rows.

**7. What is vectorization, and why does it matter here?**
Replacing a per-row Python loop with a single operation over a whole
array (NumPy/pandas), so the loop actually runs in optimized C, not
the Python interpreter. `data_generator/orders.py` generates every
order and order-item field this way — measured at ~39 seconds for
100K customers/500K orders/2.3M events, which a naive per-row loop
would not come close to.

**8. How do context managers (`with`) help with database code?**
They guarantee cleanup (closing a cursor/connection) even if an
exception is raised partway through. Every database-touching module
in this project (`data_quality/checks.py`, `spark/jobs/load_warehouse.py`,
...) uses `with conn.cursor() as cur:` for exactly this reason.

**9. Mutable default arguments — what's the classic Python pitfall?**
`def f(x, cache={})` reuses the *same* dict across every call, since
default arguments are evaluated once at function definition time, not
per call. Not directly hit in this codebase, but a real interview
staple worth having ready.

---

## Apache Spark

**10. What is a partition, and why does the number of partitions matter?**
A partition is a chunk of a distributed dataset that one executor
task processes independently. Too few partitions underuses available
cores; too many adds scheduling overhead. `spark/jobs/*.py` controls
output partition count explicitly via `coalesce(N)` before every
Parquet write, rather than accepting Spark's default of one file per
input task.

**11. What triggers a shuffle, and why is it expensive?**
Any operation needing rows that started on different partitions to
end up together — `GROUP BY`, `JOIN`, `ORDER BY` (except within an
already-co-located frame). Expensive because it moves data across the
network between executors. `spark/utils/spark_session.py` raises the
broadcast-join threshold specifically to avoid triggering a shuffle
for joins against small dimension tables.

**12. When would you use a broadcast join, and when would you avoid one?**
Broadcast when one side is small enough to ship a full copy to every
executor cheaply (this project: `products`, `marketing_campaigns` —
thousands of rows even at `large` scale). Avoid it for a large
dimension like `customers` (up to ~1M rows) — broadcasting something
that big costs more in serialization/network than a normal sort-merge
join, and risks executor out-of-memory errors. See
`spark/utils/cleaning.py`'s `anti_join_orphans()` for this exact
trade-off applied per call site.

**13. Explain lazy evaluation — transformations vs. actions.**
Transformations (`.filter()`, `.withColumn()`, `.join()`) build up a
query plan without executing anything; actions (`.count()`,
`.collect()`, `.write()`) trigger actual execution. This project's
cleaning jobs call `.cache()` before a DataFrame is used in multiple
downstream `.count()` calls (e.g. `clean_customers.py`'s before/after
row counts) — without caching, Spark would re-read and re-parse the
raw file from scratch for every action.

**14. What's the difference between `repartition()` and `coalesce()`?**
`repartition(n)` does a full shuffle to redistribute data into
exactly `n` partitions (can increase or decrease count).
`coalesce(n)` only merges existing partitions without a shuffle
(can only decrease count, more efficiently). Every write in
`spark/utils/cleaning.py`'s `write_processed_parquet()` uses
`coalesce()`, since the goal is just fewer output files, not
rebalancing data across the cluster.

---

## Apache Airflow

**15. What's a DAG, and what's the difference between a task and an operator?**
A DAG is the acyclic graph of dependencies between tasks. An operator
is a *template* for a unit of work (e.g. `PythonOperator`); a task is
one specific instantiation of an operator within a DAG. This
project's `customer_360_pipeline` DAG uses `PythonOperator` for every
task, each wrapping a real, independently-testable function from
`data_generator`, `spark.jobs`, or `data_quality`.

**16. How do retries and `execution_timeout` interact?**
`execution_timeout` bounds how long a single attempt may run before
being killed; `retries` controls how many additional attempts happen
after a failure (including a timeout). `spark_transform` overrides
the DAG's default retry count down to 1 — a transient executor OOM is
worth one retry, but a real bug in the transform logic isn't worth
three attempts at the same wrong answer.

**17. What is XCom, and where did this project actually need it?**
Airflow's mechanism for small pieces of data to pass between tasks
in the same DAG run. `start_pipeline_run` pushes a `run_id` via XCom
(its return value, auto-pushed); every other task's
`on_success_callback`/`on_failure_callback` reads that same `run_id`
back to attribute its own execution metrics to the right run in
`analytics.pipeline_run_steps`.

**18. What's `trigger_rule`, and why does `pipeline_summary` use `all_done`?**
`trigger_rule` controls when a task is eligible to run based on its
upstream tasks' states — the default (`all_success`) means a task is
skipped if any upstream failed. `pipeline_summary` uses
`trigger_rule="all_done"` specifically so it still runs (and logs a
failure summary) even when an earlier task failed — a failure should
be visible in one place, not silently skipped over.

**19. A task's `on_success_callback` crashed with "can't subtract
offset-naive and offset-aware datetimes" — what's going on?**
A real bug found in this project: `TaskInstance.start_date`/`.end_date`
are timezone-aware (Airflow uses UTC throughout), but a fallback of
plain `datetime.now()` for a missing value is timezone-naive —
subtracting an aware and a naive datetime raises exactly this error.
Fixed with `airflow.utils.timezone.utcnow()` as the fallback instead.

---

## AWS

**20. S3 storage classes — when would you use Glacier vs. Standard?**
Standard for frequently-accessed data; Glacier Instant Retrieval for
data rarely read but still needed with reasonably fast access. This
project's `aws/s3.md` recommends transitioning `raw/` to Glacier
Instant Retrieval after 30 days — raw batches are rarely re-read once
processed, but kept for audit/replay.

**21. What does "least privilege" mean in an IAM policy, concretely?**
Scoping every permission to exactly the resource and action needed —
never `Resource: "*"`. `aws/iam.md`'s EMR execution role policy grants
`s3:GetObject`/`PutObject`/`DeleteObject` scoped to
`arn:aws:s3:::customer-360-data/*` only, with `s3:DeleteBucket` and
any public-access permission deliberately absent.

**22. RDS Multi-AZ vs. a read replica — what's each one actually for?**
Multi-AZ is a synchronous standby for automatic failover (availability);
a read replica is an asynchronous copy for scaling *read* throughput
(performance). `aws/architecture.md` recommends Multi-AZ for
availability and, separately, a read replica specifically so Power
BI's DirectQuery load doesn't compete with the pipeline's own nightly
load job on the same instance.

---

## Data Warehouse

**23. Star schema vs. snowflake schema — what's the actual trade-off?**
Star: dimensions are denormalized (one flat table per dimension).
Snowflake: dimensions are further normalized into sub-tables. Star
was chosen here because every business question in this project is
"aggregate a fact, grouped by a dimension attribute" — snowflaking
`dim_product` further (e.g. a separate `dim_category` table) would
add joins with no query benefit for that access pattern.

**24. Surrogate key vs. natural key — why bother with both?**
A surrogate key (e.g. `customer_key`, an auto-incrementing integer)
is stable and small, independent of whatever the natural/business key
(`customer_id`) looks like. This project's Gold layer
(`customer_360`, `customer_rfm`) actually keys on the *natural* key
instead, since nothing else references those tables as a foreign key
— a deliberate exception, not an oversight (see `docs/data_model.md`).

**25. What does "grain" mean for a fact table, and why must it be explicit?**
The grain is what one row represents. Getting it wrong is the most
common Star Schema mistake — mixing, say, order-level and
line-item-level rows in one fact table breaks every aggregation.
`docs/data_model.md` states the grain of every fact table explicitly
for exactly this reason.

---

## Data Lake

**26. Data lake vs. data warehouse — what's actually different?**
A data lake stores raw/semi-structured data cheaply at any scale
before it's modeled; a warehouse stores structured, modeled data
optimized for query performance. This project's medallion layers map
directly onto that distinction: `raw/`/`processed/` in S3 is the
lake, `warehouse.*`/`analytics.*` in Postgres is the warehouse.

**27. Why store raw data as CSV/JSON but processed data as Parquet?**
Raw should be an unmodified record of what arrived — CSV/JSON needs
no schema assumptions. Parquet for processed/curated because it's
columnar (queries touching 2 of 10 columns read far less data),
supports predicate pushdown via per-column statistics, and compresses
3-10x better than row-based CSV — detailed in `aws/s3.md`.

---

## ETL / ELT

**28. ETL vs. ELT — which did this project use, and why?**
ETL: transform before loading into the target. ELT: load raw data
first, transform in the target system afterward. This project is
mostly ETL (PySpark cleans/transforms before the warehouse load) but
the Gold layer (`build_customer_360.py`, etc.) is ELT-style — it
loads warehouse data via SQL and transforms in-database with a single
large query, since Postgres itself is well-suited to that
aggregation work.

**29. What does idempotency mean for a pipeline step, and how did you
actually implement it, not just claim it?**
Re-running the same step for the same input produces the same end
state, not duplicated data. Every step in this project implements it
differently based on what fits: partition overwrite (`data_generator`),
`DELETE ... WHERE batch_date = %s` before insert (staging, DQ
results), or full `TRUNCATE` before reload (warehouse, Gold) — see
`docs/pipeline.md`'s table mapping each step to its specific
mechanism.

**30. How do you handle dirty/malformed data arriving from upstream?**
Don't clean it before you can measure it. This project's staging
layer deliberately preserves dirty data as-is specifically so the
Data Quality gate can quantify the problem (Phase 6) before Spark
cleaning (Phase 5) ever touches it — cleaning first would make "how
bad was the raw data, really" unanswerable.

---

## Data Quality

**31. What are the six standard data quality dimensions?**
Completeness (no missing required values), Uniqueness (no duplicates
on a key), Validity (matches format/allowed values), Accuracy (values
are plausible/correct), Referential Integrity (foreign keys resolve),
Consistency (the same fact agrees across tables). This project has
concrete SQL checks for all six in `data_quality/checks.py`.

**32. How do you make a pipeline actually *fail*, not just log a warning,
on bad data?**
The check function must return a boolean/exit code the caller acts
on, and the caller must propagate it. This project verifies this
three separate ways: the check engine returns `overall_passed=False`,
the CLI (`data_quality/run_checks.py`) exits non-zero, and the
Airflow task (`validate_data`) raises an exception — tested by
deliberately forcing an impossible threshold and confirming the task
is marked `UP_FOR_RETRY` by Airflow's own retry policy, not just
printing an error.

**33. Completeness vs. Validity — what's the practical difference?**
Completeness asks "is this field present at all"; validity asks "does
the present value make sense." An email that's `NULL` is a
completeness failure; an email of `"not_an_email"` is a validity
failure. This project's `email_format_valid` check actually treats
`NULL` as *also* invalid for that specific check (`email IS NULL OR
email !~ '...'`), since a dashboard can't use either one.

---

## Power BI

**34. Why avoid many-to-many relationships in a Power BI data model?**
They make filter direction ambiguous and can silently produce
incorrect aggregations (double-counting). This project's model
(`powerbi/data_model.md`) is naturally free of them because every
warehouse table has a real primary key enforced at the database
level — every relationship is genuinely `1:*` or `1:1`.

**35. Import mode vs. DirectQuery — how would you choose for this project?**
Import loads a snapshot into Power BI's in-memory engine (fast, needs
a refresh schedule); DirectQuery queries the source live (always
current, but query performance depends on the source). This project
recommends Import for the small, slow-changing Gold tables and dims,
reserving DirectQuery consideration only for `fact_website_events` if
it grows past what Import can comfortably hold at `large` scale.

---

## Customer 360, RFM, and CLV

**36. What makes a "Customer 360" view different from just querying the
orders table?**
It aggregates *every* customer touchpoint — orders, support tickets,
website behavior, campaign interactions — into one row per customer,
computed once and reused everywhere, rather than every downstream
consumer re-deriving the same joins independently (and risking subtly
different definitions of "total spend" in different reports). This
project's `customer_360` table explicitly documents its business
definitions (e.g. "total_spend counts completed orders only") for
exactly that reason.

**Bonus — RFM scoring pitfall**: `NTILE(5)` needs enough rows to
actually spread across 5 buckets — with too few buyers, Postgres
front-loads low bucket numbers regardless of value (a single
standout buyer among a handful of others lands in bucket 1, not 5).
Found and documented as a real limitation of this project's small
`test`-scale preset, not something to silently "fix" by lowering a
threshold in production code.

**Bonus — Historical vs. Estimated CLV**: Historical CLV is realized
revenue to date (`= total_spend`, simple and always correct by
definition); Estimated CLV is a forward-looking prediction
(`AOV × purchase frequency × estimated lifespan`, where lifespan `= 1
/ average monthly churn rate`) — inherently a model with real
limitations (this project documents them explicitly in
`build_clv.py`: one global lifespan applied to every customer,
assumes constant churn probability, a point-in-time estimate that
needs refreshing). Keeping the two as separate columns rather than
one conflated number is itself a design decision worth being able to
explain.
