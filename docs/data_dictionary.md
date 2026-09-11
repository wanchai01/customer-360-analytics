# Data Dictionary

Generated directly from `information_schema` against the live database (not hand-typed) — every column name and type here is guaranteed to match reality, avoiding the kind of column-name mismatch bugs found and fixed during Phase 10 and Phase 11 (e.g. `customer_name` vs `full_name`).

## Staging Layer (`staging.*`)

Raw-shaped landing tables — loosely typed, no constraints, holding data exactly as generated/ingested (including intentional dirty data for the Phase 6 Data Quality checks to measure). Every table also has `_batch_date` and `_loaded_at` audit columns (added by `data_quality/load_staging.py`, not shown per-table below since they're identical across all nine).

### `staging.stg_campaign_interactions`

| Column | Type | Nullable |
|--------|------|----------|
| `interaction_id` | text | YES |
| `campaign_id` | text | YES |
| `customer_id` | text | YES |
| `interaction_type` | text | YES |
| `interaction_timestamp` | timestamp without time zone | YES |
| `_batch_date` | date | NO |
| `_loaded_at` | timestamp without time zone | NO |
| `_source_file` | text | YES |

### `staging.stg_customer_support`

| Column | Type | Nullable |
|--------|------|----------|
| `ticket_id` | text | YES |
| `customer_id` | text | YES |
| `created_at` | timestamp without time zone | YES |
| `issue_type` | text | YES |
| `priority` | text | YES |
| `status` | text | YES |
| `resolution_time` | numeric | YES |
| `_batch_date` | date | NO |
| `_loaded_at` | timestamp without time zone | NO |
| `_source_file` | text | YES |

### `staging.stg_customers`

| Column | Type | Nullable |
|--------|------|----------|
| `customer_id` | text | YES |
| `first_name` | text | YES |
| `last_name` | text | YES |
| `email` | text | YES |
| `phone` | text | YES |
| `gender` | text | YES |
| `date_of_birth` | date | YES |
| `city` | text | YES |
| `province` | text | YES |
| `country` | text | YES |
| `registration_date` | date | YES |
| `customer_status` | text | YES |
| `_batch_date` | date | NO |
| `_loaded_at` | timestamp without time zone | NO |
| `_source_file` | text | YES |

### `staging.stg_marketing_campaigns`

| Column | Type | Nullable |
|--------|------|----------|
| `campaign_id` | text | YES |
| `campaign_name` | text | YES |
| `channel` | text | YES |
| `start_date` | date | YES |
| `end_date` | date | YES |
| `budget` | numeric | YES |
| `_batch_date` | date | NO |
| `_loaded_at` | timestamp without time zone | NO |
| `_source_file` | text | YES |

### `staging.stg_order_items`

| Column | Type | Nullable |
|--------|------|----------|
| `order_item_id` | text | YES |
| `order_id` | text | YES |
| `product_id` | text | YES |
| `quantity` | integer | YES |
| `unit_price` | numeric | YES |
| `discount` | numeric | YES |
| `_batch_date` | date | NO |
| `_loaded_at` | timestamp without time zone | NO |
| `_source_file` | text | YES |

### `staging.stg_orders`

| Column | Type | Nullable |
|--------|------|----------|
| `order_id` | text | YES |
| `customer_id` | text | YES |
| `order_date` | timestamp without time zone | YES |
| `order_status` | text | YES |
| `payment_method` | text | YES |
| `total_amount` | numeric | YES |
| `_batch_date` | date | NO |
| `_loaded_at` | timestamp without time zone | NO |
| `_source_file` | text | YES |

### `staging.stg_payments`

| Column | Type | Nullable |
|--------|------|----------|
| `payment_id` | text | YES |
| `order_id` | text | YES |
| `payment_date` | timestamp without time zone | YES |
| `payment_method` | text | YES |
| `payment_status` | text | YES |
| `amount` | numeric | YES |
| `_batch_date` | date | NO |
| `_loaded_at` | timestamp without time zone | NO |
| `_source_file` | text | YES |

### `staging.stg_products`

| Column | Type | Nullable |
|--------|------|----------|
| `product_id` | text | YES |
| `product_name` | text | YES |
| `category` | text | YES |
| `subcategory` | text | YES |
| `brand` | text | YES |
| `cost` | numeric | YES |
| `selling_price` | numeric | YES |
| `_batch_date` | date | NO |
| `_loaded_at` | timestamp without time zone | NO |
| `_source_file` | text | YES |

### `staging.stg_website_events`

| Column | Type | Nullable |
|--------|------|----------|
| `event_id` | text | YES |
| `customer_id` | text | YES |
| `session_id` | text | YES |
| `event_timestamp` | timestamp without time zone | YES |
| `event_type` | text | YES |
| `page` | text | YES |
| `product_id` | text | YES |
| `device` | text | YES |
| `traffic_source` | text | YES |
| `_batch_date` | date | NO |
| `_loaded_at` | timestamp without time zone | NO |
| `_source_file` | text | YES |

## Warehouse Layer — Dimensions (`warehouse.dim_*`)

Star Schema dimensions, Phase 3. Surrogate keys (`*_key`) are what fact tables reference; natural/business keys (`customer_id`, `product_id`, ...) are what the Gold layer (below) keys on instead.

### `warehouse.dim_campaign`

| Column | Type | Nullable |
|--------|------|----------|
| `campaign_key` | integer | NO |
| `campaign_id` | text | NO |
| `campaign_name` | text | YES |
| `channel` | text | YES |
| `start_date` | date | YES |
| `end_date` | date | YES |
| `budget` | numeric | YES |
| `_loaded_at` | timestamp without time zone | NO |

### `warehouse.dim_customer`

| Column | Type | Nullable |
|--------|------|----------|
| `customer_key` | integer | NO |
| `customer_id` | text | NO |
| `first_name` | text | YES |
| `last_name` | text | YES |
| `full_name` | text | YES |
| `email` | text | YES |
| `phone` | text | YES |
| `gender` | text | YES |
| `date_of_birth` | date | YES |
| `location_key` | integer | YES |
| `registration_date` | date | YES |
| `customer_status` | text | YES |
| `_loaded_at` | timestamp without time zone | NO |
| `_updated_at` | timestamp without time zone | NO |

### `warehouse.dim_date`

| Column | Type | Nullable |
|--------|------|----------|
| `date_key` | integer | NO |
| `full_date` | date | NO |
| `year` | smallint | NO |
| `quarter` | smallint | NO |
| `month` | smallint | NO |
| `month_name` | text | NO |
| `day` | smallint | NO |
| `day_of_week` | smallint | NO |
| `day_name` | text | NO |
| `week_of_year` | smallint | NO |
| `is_weekend` | boolean | NO |

### `warehouse.dim_device`

| Column | Type | Nullable |
|--------|------|----------|
| `device_key` | integer | NO |
| `device_name` | text | NO |

### `warehouse.dim_location`

| Column | Type | Nullable |
|--------|------|----------|
| `location_key` | integer | NO |
| `city` | text | NO |
| `province` | text | NO |
| `country` | text | NO |

### `warehouse.dim_product`

| Column | Type | Nullable |
|--------|------|----------|
| `product_key` | integer | NO |
| `product_id` | text | NO |
| `product_name` | text | YES |
| `category` | text | YES |
| `subcategory` | text | YES |
| `brand` | text | YES |
| `cost` | numeric | YES |
| `selling_price` | numeric | YES |
| `margin_pct` | numeric | YES |
| `_loaded_at` | timestamp without time zone | NO |

## Warehouse Layer — Facts (`warehouse.fact_*`)

`fact_website_events` is RANGE-partitioned by year (Phase 3) — `fact_website_events_y2019` through `_y2026` and a `_default` catch-all partition exist as physical tables but share the exact same column definitions as the parent, so they're omitted here to avoid nine identical copies of the same table.

### `warehouse.fact_campaign_interactions`

| Column | Type | Nullable |
|--------|------|----------|
| `interaction_key` | integer | NO |
| `interaction_id` | text | NO |
| `campaign_key` | integer | YES |
| `customer_key` | integer | YES |
| `interaction_type` | text | YES |
| `interaction_timestamp` | timestamp without time zone | YES |
| `_loaded_at` | timestamp without time zone | NO |

### `warehouse.fact_order_items`

| Column | Type | Nullable |
|--------|------|----------|
| `order_item_key` | integer | NO |
| `order_item_id` | text | NO |
| `order_key` | integer | NO |
| `product_key` | integer | YES |
| `quantity` | integer | YES |
| `unit_price` | numeric | YES |
| `discount` | numeric | YES |
| `line_amount` | numeric | YES |
| `_loaded_at` | timestamp without time zone | NO |

### `warehouse.fact_orders`

| Column | Type | Nullable |
|--------|------|----------|
| `order_key` | integer | NO |
| `order_id` | text | NO |
| `customer_key` | integer | NO |
| `order_date_key` | integer | YES |
| `order_date` | timestamp without time zone | NO |
| `order_status` | text | YES |
| `payment_method` | text | YES |
| `total_amount` | numeric | YES |
| `_loaded_at` | timestamp without time zone | NO |

### `warehouse.fact_payments`

| Column | Type | Nullable |
|--------|------|----------|
| `payment_key` | integer | NO |
| `payment_id` | text | NO |
| `order_key` | integer | NO |
| `payment_date_key` | integer | YES |
| `payment_date` | timestamp without time zone | YES |
| `payment_method` | text | YES |
| `payment_status` | text | YES |
| `amount` | numeric | YES |
| `_loaded_at` | timestamp without time zone | NO |

### `warehouse.fact_support_tickets`

| Column | Type | Nullable |
|--------|------|----------|
| `ticket_key` | integer | NO |
| `ticket_id` | text | NO |
| `customer_key` | integer | YES |
| `created_date_key` | integer | YES |
| `created_at` | timestamp without time zone | YES |
| `issue_type` | text | YES |
| `priority` | text | YES |
| `status` | text | YES |
| `resolution_time` | numeric | YES |
| `_loaded_at` | timestamp without time zone | NO |

### `warehouse.fact_website_events`

| Column | Type | Nullable |
|--------|------|----------|
| `event_key` | bigint | NO |
| `event_id` | text | NO |
| `customer_key` | integer | YES |
| `session_id` | text | YES |
| `event_date_key` | integer | YES |
| `event_timestamp` | timestamp without time zone | NO |
| `event_type` | text | YES |
| `page` | text | YES |
| `product_key` | integer | YES |
| `device_key` | integer | YES |
| `traffic_source` | text | YES |
| `_loaded_at` | timestamp without time zone | NO |

## Analytics / Gold Layer (`analytics.*`)

Business-ready tables built in Phases 6, 8, 9, and 14. These key on **natural** `customer_id`/`campaign_id` text keys, not warehouse surrogate keys — see `powerbi/data_model.md` for why that matters when relating them to `dim_customer` in a BI tool.

### `analytics.customer_360`

| Column | Type | Nullable |
|--------|------|----------|
| `customer_id` | text | NO |
| `customer_name` | text | YES |
| `age` | integer | YES |
| `gender` | text | YES |
| `location` | text | YES |
| `registration_date` | date | YES |
| `total_orders` | integer | YES |
| `completed_orders` | integer | YES |
| `cancelled_orders` | integer | YES |
| `total_spend` | numeric | YES |
| `average_order_value` | numeric | YES |
| `first_purchase_date` | date | YES |
| `last_purchase_date` | date | YES |
| `days_since_last_purchase` | integer | YES |
| `purchase_frequency` | numeric | YES |
| `favorite_category` | text | YES |
| `favorite_product` | text | YES |
| `website_sessions` | integer | YES |
| `website_events` | integer | YES |
| `support_tickets` | integer | YES |
| `resolved_tickets` | integer | YES |
| `average_resolution_time` | numeric | YES |
| `campaign_interactions` | integer | YES |
| `customer_lifetime_value` | numeric | YES |
| `rfm_score` | text | YES |
| `customer_segment` | text | YES |
| `_built_at` | timestamp without time zone | NO |

### `analytics.customer_rfm`

| Column | Type | Nullable |
|--------|------|----------|
| `customer_id` | text | NO |
| `recency_days` | integer | YES |
| `frequency` | integer | YES |
| `monetary` | numeric | YES |
| `r_score` | smallint | YES |
| `f_score` | smallint | YES |
| `m_score` | smallint | YES |
| `rfm_score` | text | YES |
| `rfm_sum` | smallint | YES |
| `calculated_at` | timestamp without time zone | NO |

### `analytics.customer_segments`

| Column | Type | Nullable |
|--------|------|----------|
| `customer_id` | text | NO |
| `customer_segment` | text | NO |
| `segment_logic_version` | text | NO |
| `assigned_at` | timestamp without time zone | NO |

### `analytics.customer_clv`

| Column | Type | Nullable |
|--------|------|----------|
| `customer_id` | text | NO |
| `historical_clv` | numeric | YES |
| `average_order_value` | numeric | YES |
| `purchase_frequency_monthly` | numeric | YES |
| `estimated_lifespan_months` | numeric | YES |
| `estimated_clv` | numeric | YES |
| `calculated_at` | timestamp without time zone | NO |

### `analytics.customer_monthly_metrics`

| Column | Type | Nullable |
|--------|------|----------|
| `metric_month` | date | NO |
| `active_customers` | integer | YES |
| `new_customers` | integer | YES |
| `returning_customers` | integer | YES |
| `retention_rate` | numeric | YES |
| `churn_rate` | numeric | YES |
| `repeat_purchase_rate` | numeric | YES |
| `total_revenue` | numeric | YES |
| `_built_at` | timestamp without time zone | NO |

### `analytics.customer_cohort`

| Column | Type | Nullable |
|--------|------|----------|
| `cohort_month` | date | NO |
| `period_number` | integer | NO |
| `active_customers` | integer | NO |
| `cohort_size` | integer | NO |
| `retention_rate` | numeric | YES |
| `_built_at` | timestamp without time zone | NO |

### `analytics.data_quality_results`

| Column | Type | Nullable |
|--------|------|----------|
| `check_id` | integer | NO |
| `dataset` | text | NO |
| `check_name` | text | NO |
| `total_records` | integer | NO |
| `failed_records` | integer | NO |
| `pass_rate` | numeric | YES |
| `status` | text | NO |
| `execution_time` | numeric | YES |
| `batch_date` | date | NO |
| `checked_at` | timestamp without time zone | NO |

### `analytics.pipeline_runs`

| Column | Type | Nullable |
|--------|------|----------|
| `run_id` | integer | NO |
| `batch_date` | date | NO |
| `dag_run_id` | text | YES |
| `started_at` | timestamp without time zone | NO |
| `ended_at` | timestamp without time zone | YES |
| `duration_seconds` | numeric | YES |
| `status` | text | NO |
| `total_records_processed` | bigint | YES |
| `total_records_failed` | bigint | YES |
| `dq_overall_status` | text | YES |
| `notes` | text | YES |

### `analytics.pipeline_run_steps`

| Column | Type | Nullable |
|--------|------|----------|
| `step_id` | integer | NO |
| `run_id` | integer | NO |
| `step_name` | text | NO |
| `started_at` | timestamp without time zone | YES |
| `ended_at` | timestamp without time zone | YES |
| `duration_seconds` | numeric | YES |
| `status` | text | NO |
| `records_processed` | bigint | YES |
| `error_message` | text | YES |

### `analytics.vw_powerbi_customer_360`

| Column | Type | Nullable |
|--------|------|----------|
| `customer_id` | text | YES |
| `customer_name` | text | YES |
| `age` | integer | YES |
| `gender` | text | YES |
| `location` | text | YES |
| `registration_date` | date | YES |
| `total_orders` | integer | YES |
| `completed_orders` | integer | YES |
| `cancelled_orders` | integer | YES |
| `total_spend` | numeric | YES |
| `average_order_value` | numeric | YES |
| `first_purchase_date` | date | YES |
| `last_purchase_date` | date | YES |
| `days_since_last_purchase` | integer | YES |
| `purchase_frequency` | numeric | YES |
| `favorite_category` | text | YES |
| `favorite_product` | text | YES |
| `website_sessions` | integer | YES |
| `website_events` | integer | YES |
| `support_tickets` | integer | YES |
| `resolved_tickets` | integer | YES |
| `average_resolution_time` | numeric | YES |
| `campaign_interactions` | integer | YES |
| `rfm_score` | text | YES |
| `customer_segment` | text | YES |
| `historical_clv` | numeric | YES |
| `estimated_clv` | numeric | YES |
| `estimated_lifespan_months` | numeric | YES |
