# Power BI Dashboard Design (Section 17)

5 pages, each with a stated purpose, so a design/layout choice can be
checked against "does this serve the question this page exists to
answer" rather than added just because it looks good in isolation.

Screenshots aren't included in this repo (no Power BI Desktop license
available in the environment that built this project) — `powerbi/screenshots/`
is the intended location once a person with Power BI Desktop connects
using [`data_model.md`](data_model.md) and builds these pages; this
document is the complete build spec for doing that.

---

## Page 1 — Executive Overview

**Purpose**: a 10-second answer to "how is the business doing right now."

**KPI cards** (top row): Total Customers · Active Customers · Total
Revenue · Total Orders · Average Order Value · Customer Lifetime
Value (historical) · Retention Rate — each using the measures in
[`dax_measures.md`](dax_measures.md).

**Charts**:
- **Revenue Trend** — line chart, `dim_date[full_date]` (month
  granularity) on the X-axis, `[Total Revenue]` on Y, with `[Revenue
  MoM Growth %]` as a secondary line on a second axis.
- **Customer Growth** — area chart, cumulative customer count over
  time (`[Customer Growth (Cumulative)]`).
- **Revenue by Segment** — donut chart, `customer_segments[customer_segment]`
  by `[Total Revenue]` — the Pareto-style "which segments actually
  drive revenue" view (same shape as SQL Analytics query #20).
- **Revenue by Category** — horizontal bar chart, `dim_product[category]`
  by `[Total Revenue]`.

**Filters (page-level slicers)**: date range, province, customer segment.

---

## Page 2 — Customer 360

**Purpose**: pick one customer, see everything about them on one screen.

**Layout**: a customer search/slicer (searchable list of `customer_id`
+ `customer_name`) pinned to the left; everything else responds to
that selection.

**Customer Profile card**: name, age, gender, location, registration
date, customer_status — direct fields from `customer_360`, no measures needed.

**Purchase History**: table visual — order date, status, total
amount, sorted descending — sourced from `fact_orders` filtered to
the selected customer.

**Metrics cards**: Total Spend, Number of Orders, Average Order
Value, Favorite Category, Favorite Product — all direct fields on
`customer_360` for this one customer (a card visual with a single-row
filter context doesn't need aggregation).

**Website Activity**: card row — Website Sessions, Website Events,
Days Since Last Purchase.

**Support Tickets**: table — issue type, priority, status, resolution
time, from `fact_support_tickets` filtered to the customer.

**RFM / Segment / CLV**: card row — RFM Score (`customer_rfm[rfm_score]`),
Segment (`customer_segments[customer_segment]`), Estimated CLV
(`customer_clv[estimated_clv]`).

---

## Page 3 — Customer Segmentation

**Purpose**: understand the 9 RFM segments as groups, not individuals.

**Charts**:
- **Customer count by segment** — bar chart, `customer_segment` by
  `[Segment Customer Count]`.
- **Revenue by segment** — bar chart (or reuse Page 1's donut, filtered
  differently) with `[Revenue % of Total]` as data labels.
- **CLV by segment** — bar chart, segment by `AVERAGE(customer_clv[estimated_clv])`.
- **RFM distribution** — a matrix or scatter: R-score vs F-score,
  bubble size = customer count, colored by segment — this is the
  classic "RFM grid" visualization and directly mirrors SQL Analytics
  query #14.
- **Segment trend** — if tracking segment assignments over multiple
  pipeline runs becomes a future improvement (Section 21 lists this
  as one), a line chart of segment size over time; with the current
  full-refresh model there's a single point-in-time segment
  assignment, so this chart is a placeholder for that future capability.

**Filters**: segment multi-select (so a viewer can isolate just
"Champions vs At Risk" for a comparison).

---

## Page 4 — Retention & Churn

**Purpose**: are we keeping the customers we acquire.

**KPI cards**: Retention Rate, Churn Rate, Repeat Purchase Rate (all
for the currently-selected month range).

**Charts**:
- **Retention Rate trend** — line chart, `customer_monthly_metrics[metric_month]`
  by `[Retention Rate]`, with Churn Rate as a second line (they're
  complementary — seeing both on one chart makes the "1 - retention =
  churn" relationship visually obvious).
- **New vs Returning Customers** — stacked column chart,
  `new_customers` and `returning_customers` by month.
- **Cohort Analysis** — matrix visual: `cohort_month` on rows,
  `period_number` on columns, `[Cohort Retention %]` as the value,
  with conditional-formatting color scale (dark green = high
  retention, red = low) — the standard cohort-retention heatmap.
- **Repeat Purchase Rate trend** — line chart by month.

---

## Page 5 — Customer Behavior

**Purpose**: what are people doing on the site, and does it convert.

**Charts**:
- **Website Events** — line chart, event count by day/week,
  optionally split by `event_type`.
- **Sessions** — KPI card, `[Website Sessions]`.
- **Conversion** — KPI card, `[Conversion Rate]`, plus a funnel
  visual: page_view → product_view → add_to_cart → checkout_start →
  purchase (matches SQL Analytics query #17's funnel shape).
- **Device** — donut chart, sessions by `dim_device[device_name]`,
  with conversion rate as a secondary measure/tooltip.
- **Traffic Source** — bar chart, sessions by `fact_website_events[traffic_source]`.
- **Product Interest** — table, top-viewed products
  (`event_type = "product_view"`) by view count — same shape as the
  Spark-computed `daily_top_products` from Phase 5, now sliceable by
  any date range interactively instead of being fixed per-day.
- **Campaign Interaction** — table, campaign name/channel by
  impressions/clicks/conversions/CTR (same shape as SQL Analytics
  query #15).

---

## Cross-page consistency notes

- Every page's date-based visuals should reference `dim_date`, not a
  date column on a fact table directly — this is what makes a single
  page-level date slicer filter every visual on the page consistently
  (Power BI can only cross-filter through the model's relationships,
  and `dim_date` is the one table every date-based fact is related to).
- Color for `customer_segment` should be a shared custom color theme
  across Pages 1, 2, and 3 (e.g. Champions = green, Lost Customers =
  gray, At Risk = orange) so a segment reads as the same color
  everywhere in the report, not just within one visual.
