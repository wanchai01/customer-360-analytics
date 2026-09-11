"""
test_data_generator.py — Unit tests for the Phase 2 data generator.

Run with:  pytest tests/test_data_generator.py -v

These tests use the small "test" scale preset and dirty_rate=0 for
deterministic structural checks, plus a couple of dirty_rate>0 cases
to confirm the intentional data-quality issues are actually injected
(so the Phase 6 Data Quality framework has something real to catch).
"""
import numpy as np
import pandas as pd
import pytest

from data_generator import config as cfg
from data_generator.customer_support import generate_customer_support
from data_generator.customers import generate_customers
from data_generator.marketing import generate_campaign_interactions, generate_campaigns
from data_generator.orders import generate_orders_and_items
from data_generator.payments import generate_payments
from data_generator.products import generate_products
from data_generator.utils import to_epoch_seconds
from data_generator.website_events import generate_website_events

N_CUSTOMERS = 300
N_PRODUCTS = 50
N_ORDERS = 800
N_EVENTS = 2_000
N_TICKETS = 60
N_CAMPAIGNS = 5
N_INTERACTIONS = 500


@pytest.fixture(scope="module")
def customers_df():
    return generate_customers(N_CUSTOMERS, dirty_rate=0)


@pytest.fixture(scope="module")
def products_df():
    return generate_products(N_PRODUCTS, dirty_rate=0)


@pytest.fixture(scope="module")
def orders_items(customers_df, products_df):
    return generate_orders_and_items(customers_df, products_df, N_ORDERS, dirty_rate=0)


# --------------------------------------------------------------------
# customers
# --------------------------------------------------------------------
def test_customers_row_count(customers_df):
    assert len(customers_df) == N_CUSTOMERS


def test_customers_ids_unique_when_clean(customers_df):
    assert customers_df["customer_id"].is_unique


def test_customers_no_nulls_when_clean(customers_df):
    assert customers_df.isna().sum().sum() == 0


def test_customers_status_values_are_valid(customers_df):
    assert set(customers_df["customer_status"].unique()) <= set(cfg.CUSTOMER_STATUSES)


def test_customers_dirty_rate_injects_issues():
    dirty = generate_customers(2_000, dirty_rate=0.05)
    assert dirty["customer_id"].isna().sum() > 0 or dirty["email"].str.contains("_at_", na=False).sum() > 0


def test_customers_dirty_rate_injects_duplicate_rows():
    dirty = generate_customers(2_000, dirty_rate=0.05)
    non_null_ids = dirty["customer_id"].dropna()
    assert non_null_ids.duplicated().sum() > 0


# --------------------------------------------------------------------
# products
# --------------------------------------------------------------------
def test_products_row_count(products_df):
    assert len(products_df) == N_PRODUCTS


def test_products_cost_below_selling_price_when_clean(products_df):
    # dirty_rate=0 fixture -> should hold for every row
    assert (products_df["cost"] < products_df["selling_price"]).all()


def test_products_categories_are_known(products_df):
    assert set(products_df["category"].unique()) <= set(cfg.PRODUCT_CATEGORIES.keys())


# --------------------------------------------------------------------
# orders + order_items
# --------------------------------------------------------------------
def test_orders_row_count(orders_items):
    orders_df, _ = orders_items
    assert len(orders_df) == N_ORDERS


def test_order_items_reference_valid_orders(orders_items):
    orders_df, items_df = orders_items
    assert items_df["order_id"].isin(orders_df["order_id"]).all()


def test_order_items_reference_valid_products(orders_items, products_df):
    _, items_df = orders_items
    assert items_df["product_id"].isin(products_df["product_id"]).all()


def test_order_total_matches_sum_of_items(orders_items):
    """total_amount must reconcile with the sum of its line items (within rounding)."""
    orders_df, items_df = orders_items
    line_total = items_df["quantity"] * items_df["unit_price"] * (1 - items_df["discount"])
    computed = (
        pd.DataFrame({"order_id": items_df["order_id"], "line_total": line_total})
        .groupby("order_id")["line_total"].sum()
    )
    joined = orders_df.set_index("order_id")["total_amount"].to_frame().join(computed)
    diff = (joined["total_amount"] - joined["line_total"]).abs()
    assert (diff < 0.01).all()


def test_order_date_after_customer_registration(customers_df, orders_items):
    orders_df, _ = orders_items
    merged = orders_df.merge(customers_df[["customer_id", "registration_date"]], on="customer_id")
    order_epoch = to_epoch_seconds(merged["order_date"])
    reg_epoch = to_epoch_seconds(merged["registration_date"])
    assert (order_epoch >= reg_epoch).all()


def test_orders_dirty_rate_injects_orphan_customers(customers_df, products_df):
    orders_df, _ = generate_orders_and_items(customers_df, products_df, 2_000, dirty_rate=0.05)
    orphan_count = (~orders_df["customer_id"].isin(customers_df["customer_id"])).sum()
    assert orphan_count > 0


def test_orders_dirty_rate_injects_duplicate_rows(customers_df, products_df):
    orders_df, _ = generate_orders_and_items(customers_df, products_df, 2_000, dirty_rate=0.05)
    assert orders_df["order_id"].duplicated().sum() > 0


# --------------------------------------------------------------------
# payments
# --------------------------------------------------------------------
def test_payments_one_per_order(orders_items):
    orders_df, _ = orders_items
    payments_df = generate_payments(orders_df, dirty_rate=0)
    assert len(payments_df) == len(orders_df)
    assert set(payments_df["order_id"]) == set(orders_df["order_id"])


# --------------------------------------------------------------------
# website_events
# --------------------------------------------------------------------
def test_website_events_reference_valid_customers(customers_df, products_df):
    events_df = generate_website_events(customers_df, products_df, N_EVENTS, dirty_rate=0)
    assert events_df["customer_id"].isin(customers_df["customer_id"]).all()


def test_website_events_product_only_on_relevant_types(customers_df, products_df):
    events_df = generate_website_events(customers_df, products_df, N_EVENTS, dirty_rate=0)
    no_product_types = {"page_view", "checkout_start", "search"}
    subset = events_df[events_df["event_type"].isin(no_product_types)]
    assert subset["product_id"].isna().all()


# --------------------------------------------------------------------
# customer_support
# --------------------------------------------------------------------
def test_support_resolution_time_only_when_resolved(customers_df):
    tickets_df = generate_customer_support(customers_df, N_TICKETS, dirty_rate=0)
    resolved = tickets_df[tickets_df["status"].isin(["resolved", "closed"])]
    unresolved = tickets_df[~tickets_df["status"].isin(["resolved", "closed"])]
    assert resolved["resolution_time"].notna().all()
    assert unresolved["resolution_time"].isna().all()


# --------------------------------------------------------------------
# marketing
# --------------------------------------------------------------------
def test_campaign_interactions_within_campaign_window():
    campaigns_df = generate_campaigns(N_CAMPAIGNS)
    customers_df = generate_customers(100, dirty_rate=0)
    interactions_df = generate_campaign_interactions(campaigns_df, customers_df, N_INTERACTIONS, dirty_rate=0)
    joined = interactions_df.merge(campaigns_df, on="campaign_id")
    ts = to_epoch_seconds(joined["interaction_timestamp"])
    start = to_epoch_seconds(joined["start_date"])
    end = to_epoch_seconds(joined["end_date"])
    assert ((ts >= start) & (ts <= end)).all()


# --------------------------------------------------------------------
# reproducibility
# --------------------------------------------------------------------
def test_same_seed_is_reproducible():
    a = generate_customers(100, seed=123, dirty_rate=0)
    b = generate_customers(100, seed=123, dirty_rate=0)
    pd.testing.assert_frame_equal(a, b)


def test_different_seed_gives_different_data():
    a = generate_customers(100, seed=1, dirty_rate=0)
    b = generate_customers(100, seed=2, dirty_rate=0)
    assert not a["first_name"].equals(b["first_name"])
