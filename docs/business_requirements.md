# Business Requirements

## Problem statement

A retail business's customer data is scattered across separate
systems — order management, web analytics, customer support, and
marketing — with no single place to answer basic questions about a
customer or the customer base as a whole. This project builds that
single place: a Customer 360° analytics platform.

## Business questions this platform must answer

| Question | Answered by |
|----------|-------------|
| Who is this customer? What have they bought? | `analytics.customer_360` (Phase 8) |
| How often do they buy, and how much do they spend? | `customer_360.purchase_frequency`/`total_spend` |
| Which customer segments generate the most revenue? | `analytics.customer_segments` + SQL Analytics query #3/#20 (Phase 9/10) |
| Which customers are at risk of churning? | RFM segmentation — "At Risk"/"Can't Lose Them" segments (Phase 9) |
| Which customers are likely to come back? | Cohort retention matrix (`analytics.customer_cohort`, Phase 9) |
| What is a customer worth, historically and going forward? | `customer_360.customer_lifetime_value` (historical) + `analytics.customer_clv` (estimated), Phase 8/9 |
| How healthy is customer retention overall? | `analytics.customer_monthly_metrics` (Phase 9) |
| How does each customer segment actually behave? | Page 3 of the Power BI dashboard (Phase 11) |

## Functional requirements

1. Ingest customer, order, product, payment, website event, support,
   and marketing data from multiple sources into a single lake (Phase 2/4).
2. Clean and validate data against defined quality rules before it
   reaches any analytical table, with the pipeline stopping on
   unacceptable quality rather than silently propagating bad data
   (Phase 5/6).
3. Model the data as a queryable Star Schema warehouse (Phase 3/7).
4. Compute a unified Customer 360 view per customer (Phase 8).
5. Segment customers using RFM analysis into actionable groups with
   documented, defensible logic (Phase 9).
6. Estimate Customer Lifetime Value, both historical (realized) and
   estimated (forward-looking), kept as distinct numbers (Phase 8/9).
7. Track retention and churn over time, including cohort-level detail
   (Phase 9).
8. Expose all of the above through a business intelligence dashboard
   usable by non-technical stakeholders (Phase 11).
9. Run on a repeatable schedule without manual intervention, with
   visibility into whether each run succeeded (Phase 7/14).
10. Be deployable to a real cloud environment without rewriting the
    core logic (Phase 4/12).

## Non-functional requirements

- **Idempotency**: any pipeline step must be safely re-runnable for
  the same batch without duplicating or corrupting data (see
  `docs/pipeline.md`'s Idempotency section — satisfied throughout).
- **Data quality gate**: the pipeline must halt, not merely warn, when
  quality falls below a 95% pass-rate threshold on any critical check
  (Section 22; satisfied — see Phase 6/13's three-layer verification
  of this exact requirement in the main README's Testing section).
- **Auditability**: every pipeline run's timing, record counts, and
  failures must be queryable after the fact, not just visible in logs
  at the time (Phase 14; satisfied via `analytics.pipeline_runs`/`pipeline_run_steps`).
- **Cost-consciousness**: the system must be fully developable and
  testable without incurring real cloud costs, with a documented,
  low-effort path to production deployment when needed (Phase 4/12;
  satisfied via the MinIO/S3 and local-Postgres/RDS environment-swap
  pattern used throughout).
- **Least privilege**: no component should have broader access than
  it needs — enforced concretely, not just stated, via separate
  database users/roles (Phase 3) and scoped IAM policies (Phase 12).

## Explicit non-goals (documented for clarity, not implemented)

- **Real-time/streaming ingestion.** This platform is batch-oriented
  (daily pipeline runs); a genuinely real-time Customer 360 (e.g. for
  live personalization) would need a different architecture
  (streaming ingestion, a feature store) that this project doesn't
  attempt.
- **Type-2 slowly-changing dimension history** for `dim_customer` —
  see `docs/data_model.md`'s Design Decisions section and the main
  README's Future Improvements section.
- **Multi-tenant / multi-brand support** — the schema and pipeline
  assume a single retail business, not a platform serving multiple
  independent businesses' data side by side.
