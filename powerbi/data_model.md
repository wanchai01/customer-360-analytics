# Power BI Data Model (Section 18)

## Connecting Power BI to this project

Power BI Desktop → **Get Data → Database → PostgreSQL database**
(requires the Npgsql .NET driver — Power BI prompts to install it on
first use if missing).

| Setting  | Value (local dev via Docker) |
|----------|-------------------------------|
| Server   | `localhost:5432` |
| Database | `customer360` |
| Username / Password | `POSTGRES_USER` / `POSTGRES_PASSWORD` from `.env` |
| Mode     | **Import** for `warehouse.dim_*` and the `analytics.*` Gold tables (small, slow-changing — refresh on a schedule); consider **DirectQuery** only if `fact_website_events` grows past what Import can comfortably hold at `large` scale. |

Load these tables (schema.table):
- `warehouse.dim_customer`, `dim_product`, `dim_date`, `dim_location`, `dim_device`, `dim_campaign`
- `warehouse.fact_orders`, `fact_order_items`, `fact_payments`, `fact_support_tickets`, `fact_website_events`, `fact_campaign_interactions`
- `analytics.customer_360`, `customer_rfm`, `customer_segments`, `customer_clv`, `customer_monthly_metrics`, `customer_cohort`

## Relationships to create (Model view)

**Dimension → Fact** (all single-direction, `1 : *`, filter flows
from the "1" side to the "*" side — the standard star-schema setup
Power BI's own guidance recommends):

| From (1 side)        | To (* side)                          | Key |
|-----------------------|----------------------------------------|-----|
| `dim_customer`         | `fact_orders`, `fact_support_tickets`, `fact_website_events`, `fact_campaign_interactions` | `customer_key` |
| `dim_product`          | `fact_order_items`, `fact_website_events` | `product_key` |
| `dim_campaign`         | `fact_campaign_interactions` | `campaign_key` |
| `dim_device`           | `fact_website_events` | `device_key` |
| `dim_location`         | `dim_customer` | `location_key` |
| `dim_date`             | `fact_orders` (`order_date_key`), `fact_payments` (`payment_date_key`), `fact_support_tickets` (`created_date_key`), `fact_website_events` (`event_date_key`) | `date_key` |
| `fact_orders`          | `fact_order_items`, `fact_payments` | `order_key` |

**Note on `fact_payments`**: it has no direct `customer_key` — it only
carries `order_key`. To slice payments by customer in Power BI, the
filter has to flow `dim_customer → fact_orders → fact_payments`
(via the `fact_orders ↔ fact_payments` relationship above), which
works automatically once both relationships are in place with the
correct single cross-filter direction — no extra join needed, just
don't expect a customer slicer to filter `fact_payments` directly
without `fact_orders` also being in the visual's filter context.

**Gold layer → `dim_customer`** (`1 : 1`, by `customer_id`, *not*
`customer_key`): `customer_360`, `customer_rfm`, `customer_segments`,
and `customer_clv` all key on the natural `customer_id` (text),
because they were built directly against business keys in Postgres —
they never got a surrogate key of their own. Relate each of these to
`dim_customer.customer_id` as a `1:1`, single-direction relationship.
**This is the one place new Power BI builders on this project
typically get tripped up**: joining a Gold table to a *fact* table
instead of to `dim_customer` creates an ambiguous multi-path
relationship (fact → dim_customer → Gold table vs. a hypothetical
direct fact → Gold table link) — always route through `dim_customer`.

## Avoiding the three things Section 18 explicitly warns about

- **No many-to-many relationships**: every dimension key is unique in
  its own table (enforced by the `PRIMARY KEY` constraints from Phase
  3's DDL), so every relationship here is naturally `1:*` or `1:1`.
- **No circular relationships**: filters only flow dimension → fact
  → (nothing loops back to a dimension). `dim_location → dim_customer`
  is the only dimension-to-dimension link, and nothing flows back
  from `dim_customer` to `dim_location`.
- **No ambiguous relationships**: only ONE active path should ever
  exist between any two tables. If Power BI's autodetect proposes a
  second (inactive) relationship between `fact_orders` and `dim_date`
  (it sometimes guesses extra date columns), leave only
  `order_date_key → dim_date.date_key` active — inactive
  relationships are fine to keep for `USERELATIONSHIP()` in specific
  measures, but should not be silently relied upon as ambiguous
  defaults.

## Star schema diagram (text form)

```
                    dim_date
                       |
        +--------------+--------------+--------------+
        |              |              |              |
   fact_orders    fact_payments  fact_support   fact_website_events
        |                             tickets          |     |
   fact_order_items                                dim_product
        |                                               |
   dim_product                                     dim_device
        |
   dim_customer ---- dim_location
        |
   customer_360 / customer_rfm / customer_segments / customer_clv
   (1:1 on customer_id)

   dim_campaign ---- fact_campaign_interactions ---- dim_customer
```
