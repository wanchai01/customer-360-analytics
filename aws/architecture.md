# AWS Production Architecture (Section 19)

## Architecture diagram (text form)

```
                     ┌─────────────┐
                     │   Users     │
                     │ (Analysts,  │
                     │ Executives) │
                     └──────┬──────┘
                            │
                     ┌──────▼──────┐
                     │  Power BI   │  (DirectQuery or scheduled Import
                     │             │   refresh via Power BI Gateway)
                     └──────┬──────┘
                            │  PostgreSQL protocol, TLS
                     ┌──────▼──────────────┐
                     │  Amazon RDS for      │  Multi-AZ, private subnet
                     │  PostgreSQL          │  (warehouse + analytics schemas)
                     └──────┬───────────────┘
                            │  loaded by
                     ┌──────▼───────────────┐
                     │  Apache Spark         │  Amazon EMR (or self-managed
                     │  (ETL + Gold builds)  │  on EC2 / EKS)
                     └──────┬───────────────┘
                            │  reads/writes
                     ┌──────▼───────────────┐
                     │  Amazon S3            │  raw / processed / curated
                     │  (Data Lake)          │  (see aws/s3.md for detail)
                     └──────▲───────────────┘
                            │  orchestrated by
                     ┌──────┴───────────────┐
                     │  Apache Airflow       │  Amazon MWAA (managed) or
                     │  (orchestration)      │  self-managed on ECS/EC2
                     └───────────────────────┘
```

This mirrors the local Docker Compose stack component-for-component
(`postgres` → RDS, `minio` → S3, `spark-master`/`spark-worker` → EMR,
`airflow-*` → MWAA) — the same reasoning behind every "local vs AWS"
design decision in this project (see `data_lake/s3_client.py`'s
docstring for the S3/MinIO example): build and test the real logic
locally at zero cost, swap the environment-specific plumbing
underneath it when deploying, without rewriting the pipeline itself.

## Data flow

1. Source data lands in S3 `raw/` (in production, from whatever
   upstream system replaces this project's synthetic
   `data_generator/` — a real e-commerce platform's CDC stream, a
   nightly export, etc.)
2. Airflow (MWAA) triggers the DAG on schedule, which:
   a. Validates raw data exists and passes the Data Quality gate
      (staging load + checks — Phase 6, unchanged from local)
   b. Submits Spark jobs (via `EMR Step` API or `EmrAddStepsOperator`
      in place of this project's local `spark.jobs.run_all` call) to
      clean/transform raw → S3 `processed/`
   c. Builds the Gold layer (`customer_360`, `customer_rfm`, etc.) and
      loads everything into RDS PostgreSQL
   d. Runs the post-load Data Quality smoke test
3. Power BI connects to RDS (DirectQuery for near-real-time
   dashboards, or scheduled Import refresh via an on-premises data
   gateway if RDS isn't publicly reachable — it shouldn't be, see
   Security below) and analysts/executives view the 5 dashboard pages.

## Security

- **Network isolation**: RDS lives in a **private subnet** with no
  public IP — reachable only from the VPC (Spark/Airflow compute) and,
  for Power BI, through a VPN/Direct Connect or an on-premises data
  gateway. It is never exposed directly to the internet.
- **IAM least privilege**: see [`aws/iam.md`](iam.md) for the exact
  policy — the pipeline's execution role gets only the S3/RDS/Secrets
  Manager actions it actually needs, scoped to this project's specific
  bucket and database, never account-wide access.
- **Secrets Manager**, not environment variables or `.env` files, for
  RDS credentials and any API keys in production — the local
  `.env`/`.env.example` pattern is a development convenience, not
  what should hold real credentials once deployed.
- **Encryption in transit**: TLS enforced on both the S3 bucket policy
  (`aws:SecureTransport = false` → Deny, see `aws/s3.md`) and the RDS
  connection (`rds.force_ssl = 1`).
- **Encryption at rest**: S3 SSE-S3/SSE-KMS (see `aws/s3.md`) and RDS
  storage encryption (AES-256, enabled at instance creation — this
  cannot be turned on for an existing unencrypted instance, so it
  must be set correctly from day one).
- **Audit**: CloudTrail logs every S3/RDS/IAM API call; RDS's own
  query logs (or `pgAudit`) for database-level access auditing.

## Scalability

- **S3**: effectively unlimited, scales automatically — not a design
  concern for this project (see `aws/s3.md`'s Availability &
  Scalability section for the full reasoning).
- **Spark (EMR)**: scales by adding task nodes (manual, scheduled, or
  EMR Managed Scaling based on YARN memory/pending-task metrics) —
  this is exactly why Phase 5's cleaning jobs were written with
  partitioning/broadcast-join awareness already: the *code* scales
  from a 3-node dev cluster to a 50-node production cluster without
  changes, only the cluster size and `spark.sql.shuffle.partitions`
  tuning change.
- **RDS**: vertical scaling (bigger instance class) for more write/
  compute capacity, plus **read replicas** if Power BI's DirectQuery
  load on the analytics tables ever competes with the pipeline's own
  load jobs — routing BI traffic to a read replica keeps a heavy
  Power BI refresh from slowing down the nightly pipeline run, and
  vice versa.
- **Airflow (MWAA)**: environment size (small/medium/large) controls
  the number of concurrent tasks; this project's DAG structure
  (Section 8's `ingest_raw_to_lake` TaskGroup running 9 tasks in
  parallel) already assumes and benefits from actual worker
  parallelism, not a single-threaded scheduler.

## Cost

Rough ordering of what actually costs money at this project's scale
(dev/test volumes are effectively free-tier-eligible; costs below
assume `large`-scale, sustained production use):

| Component | Primary cost driver | Notes |
|-----------|----------------------|-------|
| EMR (Spark) | Instance-hours, only while the DAG's transform step runs | The single biggest lever: use Spot instances for task nodes (not the master), and terminate the cluster after each run (`EMR on EKS` or a transient cluster-per-run pattern) rather than a permanently-running cluster. |
| RDS | Instance class (vCPU/RAM) + storage (GB) + read replica if added | Right-size to the Gold-layer query pattern (dashboards, not the raw fact tables) — Power BI mostly hits small pre-aggregated tables (`customer_360`, `customer_monthly_metrics`), not `fact_website_events`. |
| S3 | Storage (GB-month) + requests | The `aws/s3.md` lifecycle rules (raw → Glacier Instant Retrieval after 30 days) are the highest-leverage S3 cost control. |
| MWAA | Environment size × hours running | A `small` environment is enough for this project's single daily DAG — no need for `large` unless running many concurrent pipelines. |
| Data transfer | Cross-AZ / internet egress | Keep RDS, EMR, and the S3 bucket in the same region (and ideally the same AZ where possible) to avoid unnecessary inter-AZ transfer charges. |

## Availability

- **RDS Multi-AZ**: a synchronous standby in a second Availability
  Zone, automatic failover (typically under a minute) if the primary
  fails — this is the standard, low-effort way to get real HA for the
  warehouse without any application-level failover logic.
- **S3**: 99.99% availability SLA, inherited for free (see `aws/s3.md`).
- **EMR/Spark**: not a 24/7 service in this architecture (it runs only
  for the duration of the daily transform step), so "availability" for
  Spark means "the next scheduled run succeeds," not continuous
  uptime — Airflow's retry policy (Phase 7: `retries=2` on most tasks,
  a longer `execution_timeout` on `spark_transform`) is what actually
  provides resilience here, not a highly-available cluster.
- **MWAA**: AWS-managed and Multi-AZ by default for the Airflow
  scheduler/webserver themselves.
