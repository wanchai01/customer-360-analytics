"""
schemas.py — Explicit StructType schemas for every raw dataset.

Reading raw files with an explicit schema instead of
`inferSchema=True` is a deliberate performance choice: inferSchema
makes Spark read the entire file once just to guess types before the
real read happens, doubling I/O for large raw files. It also makes
parsing behavior predictable — an unparsable value (e.g. a corrupted
date) becomes a clean NULL in the target type rather than silently
widening the whole column to StringType, which is what schema
inference would do if even one row looks unexpected.

These schemas intentionally mirror data_generator's output columns
exactly (see data_generator/*.py) so the raw -> clean mapping in
spark/jobs/*.py has no surprises.
"""
from __future__ import annotations

from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("date_of_birth", DateType(), True),
    StructField("city", StringType(), True),
    StructField("province", StringType(), True),
    StructField("country", StringType(), True),
    StructField("registration_date", DateType(), True),
    StructField("customer_status", StringType(), True),
])

PRODUCTS_SCHEMA = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("subcategory", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("cost", DoubleType(), True),
    StructField("selling_price", DoubleType(), True),
])

ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_date", TimestampType(), True),
    StructField("order_status", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("total_amount", DoubleType(), True),
])

ORDER_ITEMS_SCHEMA = StructType([
    StructField("order_item_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("discount", DoubleType(), True),
])

PAYMENTS_SCHEMA = StructType([
    StructField("payment_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("payment_date", TimestampType(), True),
    StructField("payment_method", StringType(), True),
    StructField("payment_status", StringType(), True),
    StructField("amount", DoubleType(), True),
])

WEBSITE_EVENTS_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("event_timestamp", TimestampType(), True),
    StructField("event_type", StringType(), True),
    StructField("page", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("device", StringType(), True),
    StructField("traffic_source", StringType(), True),
])

CUSTOMER_SUPPORT_SCHEMA = StructType([
    StructField("ticket_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("issue_type", StringType(), True),
    StructField("priority", StringType(), True),
    StructField("status", StringType(), True),
    StructField("resolution_time", DoubleType(), True),
])

MARKETING_CAMPAIGNS_SCHEMA = StructType([
    StructField("campaign_id", StringType(), True),
    StructField("campaign_name", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("start_date", DateType(), True),
    StructField("end_date", DateType(), True),
    StructField("budget", DoubleType(), True),
])

CAMPAIGN_INTERACTIONS_SCHEMA = StructType([
    StructField("interaction_id", StringType(), True),
    StructField("campaign_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("interaction_type", StringType(), True),
    StructField("interaction_timestamp", TimestampType(), True),
])

RAW_SCHEMAS = {
    "customers": CUSTOMERS_SCHEMA,
    "products": PRODUCTS_SCHEMA,
    "orders": ORDERS_SCHEMA,
    "order_items": ORDER_ITEMS_SCHEMA,
    "payments": PAYMENTS_SCHEMA,
    "website_events": WEBSITE_EVENTS_SCHEMA,
    "customer_support": CUSTOMER_SUPPORT_SCHEMA,
    "marketing_campaigns": MARKETING_CAMPAIGNS_SCHEMA,
    "campaign_interactions": CAMPAIGN_INTERACTIONS_SCHEMA,
}
