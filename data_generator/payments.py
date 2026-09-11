"""
payments.py — Generates the `payments` dataset, one row per order.

Fields: payment_id, order_id, payment_date, payment_method, payment_status, amount

payment_status is correlated with the parent order's order_status
(e.g. a "refunded" order should mostly have a "refunded" payment) so
downstream reconciliation logic has something realistic to check.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from data_generator import config as cfg
from data_generator.utils import inject_missing, inject_negative_amounts, logger, make_id, to_epoch_seconds

# order_status -> plausible payment_status distribution
_STATUS_MAP: dict[str, tuple[list[str], list[float]]] = {
    "completed": (["success", "refunded"], [0.97, 0.03]),
    "refunded": (["refunded", "success"], [0.9, 0.1]),
    "cancelled": (["failed", "pending"], [0.7, 0.3]),
    "pending": (["pending", "success"], [0.8, 0.2]),
}
_DEFAULT_STATUS = (["pending"], [1.0])


def generate_payments(orders_df: pd.DataFrame, seed: int = cfg.RANDOM_SEED, dirty_rate: float = cfg.DEFAULT_DIRTY_RATE) -> pd.DataFrame:
    """Generate one payment per order in `orders_df`."""
    rng = np.random.default_rng(seed)
    n = len(orders_df)

    # payment_date: a few hours to a few days after order_date
    delay_seconds = rng.integers(60, 60 * 60 * 72, size=n)  # 1 min to 72 hours
    order_epoch = to_epoch_seconds(orders_df["order_date"])
    payment_dates = pd.to_datetime(order_epoch + delay_seconds, unit="s")

    payment_status = np.empty(n, dtype=object)
    for status, (choices, probs) in {**_STATUS_MAP, None: _DEFAULT_STATUS}.items():
        if status is None:
            mask = ~orders_df["order_status"].isin(_STATUS_MAP.keys())
        else:
            mask = orders_df["order_status"] == status
        count = int(mask.sum())
        if count:
            payment_status[mask.to_numpy()] = rng.choice(choices, size=count, p=probs)

    payments_df = pd.DataFrame({
        "payment_id": [make_id("PAY", i) for i in range(1, n + 1)],
        "order_id": orders_df["order_id"].to_numpy(),
        "payment_date": payment_dates,
        "payment_method": orders_df["payment_method"].to_numpy(),
        "payment_status": payment_status,
        "amount": orders_df["total_amount"].to_numpy(),
    })

    if dirty_rate > 0:
        payments_df["payment_status"] = inject_missing(payments_df["payment_status"], dirty_rate / 2, rng)
        payments_df["amount"] = inject_negative_amounts(payments_df["amount"], dirty_rate / 3, rng)

    logger.info("Generated %s payments (dirty_rate=%s)", len(payments_df), dirty_rate)
    return payments_df


if __name__ == "__main__":
    from data_generator.customers import generate_customers
    from data_generator.products import generate_products
    from data_generator.orders import generate_orders_and_items

    scale = cfg.SCALE_PRESETS["test"]
    customers_df = generate_customers(scale.n_customers, dirty_rate=0)
    products_df = generate_products(scale.n_products, dirty_rate=0)
    orders_df, _ = generate_orders_and_items(customers_df, products_df, scale.n_orders, dirty_rate=0)
    payments_df = generate_payments(orders_df)
    print(payments_df.head())
    print(payments_df.shape)
