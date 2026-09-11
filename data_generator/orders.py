"""
orders.py — Generates `orders` and `order_items` together.

Orders and order_items are generated in the same function because
order.total_amount must reconcile with sum(order_items), and because
both need the same customer/product reference data. Implementation is
fully vectorized (NumPy/pandas, no per-row Python loops) so it stays
fast even at the "large" (10M+ orders) scale target in the spec.

Fields:
  orders:      order_id, customer_id, order_date, order_status,
               payment_method, total_amount
  order_items: order_item_id, order_id, product_id, quantity,
               unit_price, discount
"""
from __future__ import annotations

from datetime import datetime

import numpy as np
import pandas as pd

from data_generator import config as cfg
from data_generator.utils import (
    inject_duplicates,
    inject_missing,
    inject_negative_amounts,
    inject_orphan_fk,
    logger,
    make_id,
    to_epoch_seconds,
)

DISCOUNT_CHOICES = [0.0, 0.0, 0.0, 0.0, 0.05, 0.10, 0.15, 0.20]


def _customer_activity_weights(n_customers: int, rng: np.random.Generator) -> np.ndarray:
    """
    Skewed weights so a minority of customers place disproportionately
    more orders (realistic for retail: a small % of customers drive a
    large % of revenue — this is what makes RFM/CLV segmentation
    meaningful later instead of every customer looking identical).
    """
    raw = rng.pareto(a=1.5, size=n_customers) + 1.0
    return raw / raw.sum()


def generate_orders_and_items(
    customers_df: pd.DataFrame,
    products_df: pd.DataFrame,
    n_orders: int,
    seed: int = cfg.RANDOM_SEED,
    dirty_rate: float = cfg.DEFAULT_DIRTY_RATE,
    as_of: datetime | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Generate `n_orders` orders and their line items in vectorized fashion."""
    rng = np.random.default_rng(seed)
    as_of = as_of or datetime(2026, 9, 6)

    n_customers = len(customers_df)
    reg_epoch = to_epoch_seconds(customers_df["registration_date"])
    end_epoch = int(pd.Timestamp(as_of).timestamp())

    # ---- pick a customer per order, skewed toward "repeat buyers" ----
    weights = _customer_activity_weights(n_customers, rng)
    chosen_idx = rng.choice(n_customers, size=n_orders, p=weights)
    customer_ids = customers_df["customer_id"].to_numpy()[chosen_idx]
    lower_bound = reg_epoch[chosen_idx]

    # order_date: uniformly after the customer's registration date
    span = np.clip(end_epoch - lower_bound, a_min=1, a_max=None)
    order_epoch = lower_bound + (rng.random(n_orders) * span).astype("int64")
    order_dates = pd.to_datetime(order_epoch, unit="s")

    order_ids = np.array([make_id("ORD", i) for i in range(1, n_orders + 1)])
    order_status = rng.choice(cfg.ORDER_STATUSES, size=n_orders, p=cfg.ORDER_STATUS_WEIGHTS)
    payment_method = rng.choice(cfg.PAYMENT_METHODS, size=n_orders, p=cfg.PAYMENT_METHOD_WEIGHTS)

    # ---- order_items: variable number of lines per order, vectorized ----
    items_per_order = rng.poisson(lam=max(cfg.AVG_ITEMS_PER_ORDER - 1, 0.1), size=n_orders) + 1
    total_items = int(items_per_order.sum())

    order_id_repeated = np.repeat(order_ids, items_per_order)
    product_ids_all = products_df["product_id"].to_numpy()
    chosen_products = rng.choice(product_ids_all, size=total_items)

    price_lookup = products_df.set_index("product_id")["selling_price"]
    unit_price = price_lookup.reindex(chosen_products).to_numpy()

    quantity = rng.integers(1, 6, size=total_items)
    discount = rng.choice(DISCOUNT_CHOICES, size=total_items)

    order_item_ids = np.array([make_id("OITEM", i) for i in range(1, total_items + 1)])

    order_items_df = pd.DataFrame({
        "order_item_id": order_item_ids,
        "order_id": order_id_repeated,
        "product_id": chosen_products,
        "quantity": quantity,
        "unit_price": np.round(unit_price, 2),
        "discount": np.round(discount, 2),
    })

    # ---- roll order_items up into order.total_amount ----
    line_total = order_items_df["quantity"] * order_items_df["unit_price"] * (1 - order_items_df["discount"])
    order_totals = (
        pd.DataFrame({"order_id": order_id_repeated, "line_total": line_total})
        .groupby("order_id", sort=False)["line_total"].sum()
    )

    orders_df = pd.DataFrame({
        "order_id": order_ids,
        "customer_id": customer_ids,
        "order_date": order_dates,
        "order_status": order_status,
        "payment_method": payment_method,
    })
    orders_df["total_amount"] = orders_df["order_id"].map(order_totals).round(2)

    # ---- Intentional data-quality issues (controlled, reproducible) ----
    if dirty_rate > 0:
        orders_df["customer_id"] = inject_orphan_fk(orders_df["customer_id"], dirty_rate, rng, "CUST")
        orders_df["order_status"] = inject_missing(orders_df["order_status"], dirty_rate / 2, rng)
        orders_df["total_amount"] = inject_negative_amounts(orders_df["total_amount"], dirty_rate / 2, rng)

        order_items_df["product_id"] = inject_orphan_fk(order_items_df["product_id"], dirty_rate, rng, "PROD")
        order_items_df["unit_price"] = inject_negative_amounts(order_items_df["unit_price"], dirty_rate / 3, rng)
        orders_df = inject_duplicates(orders_df, dirty_rate / 3, rng)

    logger.info("Generated %s orders / %s order_items (dirty_rate=%s)", len(orders_df), len(order_items_df), dirty_rate)
    return orders_df, order_items_df


if __name__ == "__main__":
    from data_generator.customers import generate_customers
    from data_generator.products import generate_products

    scale = cfg.SCALE_PRESETS["test"]
    customers_df = generate_customers(scale.n_customers, dirty_rate=0)
    products_df = generate_products(scale.n_products, dirty_rate=0)
    orders_df, items_df = generate_orders_and_items(customers_df, products_df, scale.n_orders)
    print(orders_df.head())
    print(items_df.head())
    print(orders_df.shape, items_df.shape)
