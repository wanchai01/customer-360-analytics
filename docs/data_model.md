# Data Model

This is the general Star Schema design and its rationale. For the
Power BI-specific relationship setup (which fields to connect in
Power BI Desktop's Model view), see
[`powerbi/data_model.md`](../powerbi/data_model.md) instead — that
document assumes this one as background.

## Why a Star Schema

A Star Schema (dimensions with surrogate keys, facts with foreign
keys to those dimensions) was chosen over either a fully normalized
(3NF) transactional schema or a flat single-table denormalized dump,
for three concrete reasons that matter for this project specifically:

1. **Query simplicity for the actual workload.** Every business
   question in Section 16 (Phase 10's 20 SQL queries) is some
   combination of "aggregate a fact table, grouped by a dimension
   attribute" — Star Schema is *the* shape that pattern wants. A 3NF
   schema would need 3-4 extra joins per query to get from a fact row
   to a human-readable attribute like a product's category.
2. **BI tool compatibility.** Power BI (and every mainstream BI tool)
   is designed around Star Schemas specifically — its own official
   modeling guidance recommends this shape, and DAX time-intelligence
   functions assume a proper date dimension exists (`dim_date`).
3. **Clear separation of "when this happened" from "what happened."**
   Surrogate integer keys (`customer_key`, `product_key`, ...) are
   stable and small, decoupled from whatever the natural/business key
   looks like — a real production system where `customer_id` formats
   might change over time wouldn't need to touch a single fact table.

## Layers

```
raw (S3, CSV/JSON) -> staging (Postgres, dirty-as-is) ->
  processed (S3, Parquet, cleaned) -> warehouse (Postgres, Star Schema) ->
  analytics (Postgres, Gold/business-ready)
```

Each layer has one job:

| Layer | Job | Built in |
|-------|-----|----------|
| Raw | Exact record of what was generated/received, no interpretation | Phase 2 |
| Staging | Queryable mirror of raw, still dirty — exists for Phase 6 DQ checks to measure | Phase 6 |
| Processed | Cleaned, type-cast, referentially valid | Phase 5 |
| Warehouse | Star Schema — dimensions + facts, ready for arbitrary analytical joins | Phase 3 (schema), Phase 7 (load) |
| Analytics/Gold | Pre-aggregated, business-question-shaped tables (`customer_360`, `customer_rfm`, ...) | Phases 6, 8, 9, 14 |

## Grain of every fact table

Getting the grain (what one row represents) wrong is the most common
Star Schema design mistake, so it's stated explicitly for every fact
table here rather than left implicit:

| Fact table | One row = |
|------------|-----------|
| `fact_orders` | one order |
| `fact_order_items` | one line item within an order |
| `fact_payments` | one payment (assumed 1:1 with an order in this project's synthetic data, but the schema doesn't enforce that — a real system could have partial/split payments) |
| `fact_support_tickets` | one support ticket |
| `fact_website_events` | one clickstream event |
| `fact_campaign_interactions` | one campaign touchpoint (impression, click, or conversion) |

## Full column-level reference

See [`data_dictionary.md`](data_dictionary.md) — generated directly
from the live database's `information_schema`, not hand-typed, so it
can't drift out of sync with the real schema the way a hand-maintained
copy inevitably would.

## Design decisions worth calling out

- **`dim_customer` is Type-1 (overwrite), not Type-2 (versioned).**
  `_loaded_at`/`_updated_at` columns are already in place so this
  could be upgraded later, but Type-2 history tracking wasn't
  implemented — see "Future Improvements" in the main README for why
  this is a deliberate scope decision, not an oversight.
- **`fact_website_events` is RANGE-partitioned by year** — the one
  table where partitioning was worth the complexity, since it's the
  largest by an order of magnitude (Section 5: up to 50M+ rows at
  `large` scale) and time-filtered queries are the norm for
  clickstream analysis.
- **The Gold layer keys on natural keys (`customer_id`), not
  surrogate keys.** `customer_360`/`customer_rfm`/`customer_segments`/
  `customer_clv` were built directly with SQL against business keys —
  they never needed a surrogate key of their own, since nothing else
  references them as a foreign key. This is why relating them to
  `dim_customer` in Power BI must go through `customer_id`, not
  `customer_key` (see `powerbi/data_model.md`'s explicit warning
  about this).
- **Full-refresh, not incremental upsert**, for `warehouse.*` and
  `analytics.*`. Documented in `sql/warehouse/02_facts.sql`'s header
  comment and revisited in `aws/rds.md`'s Backup & Recovery section —
  the short version: this project's data generator produces a
  complete synthetic history each run rather than incremental daily
  deltas, so a full refresh is the correct match for the data's
  actual shape, not a shortcut taken to avoid writing merge logic.
