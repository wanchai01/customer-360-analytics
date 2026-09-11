# DAX Measures (Section 18)

All measures assume the relationships in
[`data_model.md`](data_model.md) are in place. Written as a
dedicated **Measures** table (a common Power BI practice: an empty
table that exists only to hold measures, keeping them out of the
physical fact/dim tables) — create one via *Modeling → New Table* with
`Measures = {}` if you don't already have one.

## Core measures (Section 18's required list)

```dax
Total Revenue =
CALCULATE(
    SUM(fact_orders[total_amount]),
    fact_orders[order_status] = "completed"
)
```

```dax
Total Customers = DISTINCTCOUNT(dim_customer[customer_key])
```

```dax
Active Customers =
CALCULATE(
    DISTINCTCOUNT(fact_orders[customer_key]),
    fact_orders[order_status] = "completed"
)
-- "Active" here = has at least one completed order in the current
-- filter context (e.g. the selected month, if a date slicer is
-- applied) — matches the same definition used by
-- analytics.customer_monthly_metrics.active_customers.
```

```dax
Average Order Value =
DIVIDE([Total Revenue], [Completed Order Count])

Completed Order Count =
CALCULATE(
    COUNTROWS(fact_orders),
    fact_orders[order_status] = "completed"
)
```

```dax
Customer Lifetime Value =
AVERAGE(customer_360[customer_lifetime_value])
-- Historical CLV (Phase 8's definition = total_spend). For the
-- forward-looking model, use [Estimated CLV] below instead — the two
-- are intentionally different numbers; see build_clv.py's docstring
-- for why they're kept separate rather than one column doing both jobs.

Estimated CLV = AVERAGE(customer_clv[estimated_clv])
```

```dax
Retention Rate =
AVERAGE(customer_monthly_metrics[retention_rate])
-- Averages the pre-computed monthly rate (Phase 9) across whatever
-- months are in the current filter context — e.g. drop a
-- metric_month slicer on the page and this becomes "retention rate
-- for the selected month(s)" for free, no extra DAX needed.
```

```dax
Churn Rate = AVERAGE(customer_monthly_metrics[churn_rate])
```

```dax
Repeat Purchase Rate = AVERAGE(customer_monthly_metrics[repeat_purchase_rate])
```

## Page-specific measures

**Executive Overview (Page 1)**

```dax
Revenue MoM Growth % =
VAR CurrentRevenue = [Total Revenue]
VAR PreviousMonthRevenue =
    CALCULATE([Total Revenue], DATEADD(dim_date[full_date], -1, MONTH))
RETURN DIVIDE(CurrentRevenue - PreviousMonthRevenue, PreviousMonthRevenue)
-- Requires dim_date to be marked as the model's official Date table
-- (Modeling -> Mark as Date Table) for DATEADD/time-intelligence
-- functions to work correctly.
```

```dax
Customer Growth (Cumulative) =
CALCULATE(
    DISTINCTCOUNT(dim_customer[customer_key]),
    FILTER(
        ALL(dim_customer[registration_date]),
        dim_customer[registration_date] <= MAX(dim_date[full_date])
    )
)
```

**Customer Segmentation (Page 3)**

```dax
Revenue % of Total =
DIVIDE([Total Revenue], CALCULATE([Total Revenue], ALL(customer_segments)))
-- ALL(customer_segments) removes the segment filter so the
-- denominator is always grand-total revenue, regardless of which
-- segment slice the visual is currently showing — this is what makes
-- a stacked bar's segments actually sum to 100%.

Segment Customer Count = DISTINCTCOUNT(customer_segments[customer_id])
```

**Retention & Churn (Page 4)**

```dax
Cohort Retention % = AVERAGE(customer_cohort[retention_rate])
-- Used as the Values field in a matrix visual with cohort_month on
-- rows and period_number on columns — this single measure, dropped
-- into that matrix shape, produces the classic cohort retention
-- heatmap without any additional DAX.
```

**Customer Behavior (Page 5)**

```dax
Website Sessions = DISTINCTCOUNT(fact_website_events[session_id])

Conversion Rate =
VAR PurchasingSessions =
    CALCULATE(
        DISTINCTCOUNT(fact_website_events[session_id]),
        fact_website_events[event_type] = "purchase"
    )
RETURN DIVIDE(PurchasingSessions, [Website Sessions])
```

## Why AVERAGE() on pre-computed monthly rates, not a DAX re-derivation

`retention_rate`/`churn_rate`/`repeat_purchase_rate` are already
computed correctly in Postgres (Phase 9, `build_retention.py`) with
the exact business logic this project defines for them (see that
module's docstring for the precise formulas). Re-deriving "retention"
from raw fact tables in DAX would risk a subtly different definition
quietly drifting from the documented one — simpler and safer to treat
Postgres as the source of truth for these specific numbers and let
DAX's job be aggregating them across whatever time window the user
has selected, not recalculating them from scratch.
