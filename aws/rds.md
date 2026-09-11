# Amazon RDS for PostgreSQL (Section 19)

## Why RDS instead of self-managed PostgreSQL on EC2

This project's local Docker Compose `postgres` service already
demonstrates the schema/DDL/loading logic works against real
PostgreSQL 16 — moving to RDS changes *where Postgres runs*, not
*what runs on it*. RDS's value here is entirely operational: managed
backups, automated Multi-AZ failover, managed minor-version patching,
and one-click storage encryption — none of which this project's code
needs to know about or handle itself, which is the point of using a
managed service instead of re-implementing that operational work.

## Instance sizing

| Environment | Instance class | Storage | Multi-AZ |
|-------------|-----------------|---------|----------|
| Dev/test (this project's `test`/`dev` scale) | `db.t4g.medium` (burstable, 2 vCPU / 4GB) | 20-50 GB gp3 | No — single-AZ is fine for non-production |
| Production (`large` scale) | `db.r6g.xlarge` (memory-optimized, 4 vCPU / 32GB) or larger, sized against actual query patterns | 200+ GB gp3, provisioned IOPS if `fact_website_events` at 50M+ rows shows I/O-bound query plans | Yes — see Availability in `aws/architecture.md` |

Start smaller than the production sizing above and scale up based on
`pg_stat_statements`/CloudWatch metrics (CPU, `FreeableMemory`,
`ReadIOPS`/`WriteIOPS`) rather than guessing — this project's own
warehouse tables are small relative to `fact_website_events`, so most
of the actual load is from that one table's bulk loads and any
DirectQuery traffic hitting it directly.

## Database/user setup (mirrors Phase 3 exactly)

The same least-privilege split from `sql/ddl/00_init_databases.sh`
(separate `customer360_app` and `airflow_app` users/databases)
applies unchanged on RDS — RDS is still real PostgreSQL, so
`sql/ddl_deploy.py` runs against an RDS endpoint exactly as it does
against the local instance, just by changing `POSTGRES_HOST` to the
RDS endpoint (and using AWS Secrets Manager to store
`POSTGRES_PASSWORD`, per `aws/architecture.md`'s Security section,
rather than an `.env` file).

```bash
# Same command, different target — no code changes:
POSTGRES_HOST=customer360-prod.xxxxxxxxxx.ap-southeast-1.rds.amazonaws.com \
POSTGRES_PORT=5432 \
POSTGRES_DB=customer360 \
POSTGRES_USER=customer360_app \
POSTGRES_PASSWORD=$(aws secretsmanager get-secret-value --secret-id prod/customer360/db-password --query SecretString --output text) \
python -m sql.ddl_deploy
```

## Connections & networking

- RDS in a **private subnet** (no public accessibility) within the
  same VPC as the EMR cluster and MWAA environment.
- A **security group** allowing inbound TCP 5432 only from: the EMR
  cluster's security group, the MWAA environment's security group,
  and (if Power BI needs DirectQuery) the on-premises gateway's
  security group / VPN CIDR range — never `0.0.0.0/0`.
- `rds.force_ssl = 1` parameter group setting, so any connection
  attempt without TLS is rejected outright rather than merely
  discouraged.

## Backup & recovery

- **Automated backups**: daily snapshot + transaction log shipping,
  enabling point-in-time restore to any second within the retention
  window (7-35 days, set based on how far back a "we need to recover
  from a bad pipeline run" scenario should reasonably reach).
- Given this project's **full-refresh model** (Phase 3's documented
  trade-off: `warehouse`/`analytics` tables are rebuilt from S3
  `processed/`/`curated` on every run, not incrementally upserted), a
  bad load is recoverable two ways: point-in-time restore, **or**
  simply re-running the pipeline against the same S3 data — the
  latter is usually faster and is a direct benefit of the
  full-refresh design decision paying off operationally, not just
  simplifying the ETL code.
- **Read replica** (optional, see `aws/architecture.md`'s Scalability
  section) can also serve as a promotion target in a regional
  disaster-recovery scenario, though Multi-AZ failover already covers
  the much more common single-AZ-failure case.
