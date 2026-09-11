# SQL Analytics — Business Questions (Section 16)

20 SQL queries, each answering a specific business question against
the warehouse/analytics tables built in Phases 3, 5, 7, 8, and 9.
Every query has been run against real pipeline-generated data (see
`tests/test_analytics_queries.py`), not just written and assumed correct.

| # | File | Question | Key technique(s) |
|---|------|----------|-------------------|
| 1 | `01_top_customers_by_revenue.sql` | Who are our top 10 customers by revenue? | ORDER BY / LIMIT |
| 2 | `02_top_products_by_revenue.sql` | Which products generate the most revenue? | JOIN, GROUP BY |
| 3 | `03_revenue_by_segment.sql` | How does revenue break down by customer segment? | GROUP BY, aggregation |
| 4 | `04_monthly_revenue_trend.sql` | How is monthly revenue trending? | CTE, `DATE_TRUNC`, `LAG()` |
| 5 | `05_customer_retention_trend.sql` | How has retention moved over the last 12 months? | Window frame (`ROWS BETWEEN`) moving average |
| 6 | `06_churned_customers.sql` | Which customers have churned, and why? | CASE, filtering |
| 7 | `07_at_risk_customers.sql` | Which valuable customers are at risk of leaving? | JOIN, scalar subquery |
| 8 | `08_average_order_value.sql` | What's our AOV, and how is it trending? | CTE, running average window |
| 9 | `09_repeat_purchase_rate.sql` | What fraction of customers buy more than once? | Correlated-free subqueries |
| 10 | `10_revenue_by_location.sql` | Which provinces drive the most revenue? | JOIN chain, `SUM() OVER ()` share-of-total |
| 11 | `11_revenue_by_category.sql` | Which product categories perform best? | JOIN, `RANK()` |
| 12 | `12_customer_acquisition_trend.sql` | How fast are we acquiring new customers? | Date trunc, cumulative `SUM() OVER` |
| 13 | `13_clv_distribution.sql` | How is Estimated CLV distributed across the base? | CASE bucketing |
| 14 | `14_rfm_distribution.sql` | What do RFM scores look like per segment? | JOIN, multi-column aggregation |
| 15 | `15_campaign_performance.sql` | Which campaigns convert best? | `COUNT(*) FILTER`, CASE-free rate math |
| 16 | `16_support_ticket_analysis.sql` | Where are support tickets concentrated, and how fast are they resolved? | Multi-dimension GROUP BY |
| 17 | `17_website_conversion_funnel.sql` | Where do sessions drop off in the purchase funnel? | CTE, conditional aggregation funnel |
| 18 | `18_device_performance.sql` | Which device converts best? | JOIN, conditional aggregation |
| 19 | `19_customer_profitability.sql` | Who are our most profitable customers (after cost)? | JOIN, `RANK()`, `COALESCE` (see note below) |
| 20 | `20_revenue_contribution_by_segment.sql` | How concentrated is revenue across segments (Pareto view)? | Two window functions: share-of-total + running cumulative share |

## Running a query

```bash
docker compose exec postgres psql -U $POSTGRES_USER -d $POSTGRES_DB \
  -f /path/to/sql/analytics/queries/01_top_customers_by_revenue.sql
```

## Real bugs found while testing against real data

1. **Query 19** referenced `dim_customer.customer_name`, which doesn't
   exist — the actual column is `full_name` (`customer_360.customer_name`
   is derived *from* it in Phase 8, a different table). Caught the
   instant the query actually ran.
2. **Query 19, more subtly**: a customer whose order history included
   an item with a cleaned/nulled `unit_price` (Phase 5's negative-amount
   handling) had `SUM(line_amount) = NULL` for their entire row — and
   Postgres sorts `NULL` **first** in a `DESC` order, silently placing a
   near-zero-revenue customer at rank #1 in "most profitable customers."
   Fixed with `COALESCE(..., 0)` on both the revenue and cost sums.
   Caught by actually reading the top-10 output, not by assuming it
   was correct because the query ran without an error.
