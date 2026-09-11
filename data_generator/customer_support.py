"""
customer_support.py — Generates the `customer_support` (tickets) dataset.

Fields: ticket_id, customer_id, created_at, issue_type, priority, status,
        resolution_time (hours; null for tickets not yet resolved/closed)
"""
from __future__ import annotations

from datetime import datetime

import numpy as np
import pandas as pd

from data_generator import config as cfg
from data_generator.utils import inject_missing, logger, make_id

_RESOLVED_STATUSES = {"resolved", "closed"}


def generate_customer_support(
    customers_df: pd.DataFrame,
    n_tickets: int,
    seed: int = cfg.RANDOM_SEED,
    dirty_rate: float = cfg.DEFAULT_DIRTY_RATE,
    as_of: datetime | None = None,
) -> pd.DataFrame:
    """Generate `n_tickets` synthetic support tickets."""
    rng = np.random.default_rng(seed)
    as_of = as_of or datetime(2026, 9, 6)

    n_customers = len(customers_df)
    # Slight skew: customers who churned or are "at risk" file more tickets
    weights = np.where(customers_df["customer_status"].to_numpy() == "churned", 2.0, 1.0)
    weights = weights / weights.sum()
    chosen_idx = rng.choice(n_customers, size=n_tickets, p=weights)
    customer_ids = customers_df["customer_id"].to_numpy()[chosen_idx]

    window_start = pd.Timestamp("2019-01-01").timestamp()
    window_end = pd.Timestamp(as_of).timestamp()
    created_epoch = rng.integers(int(window_start), int(window_end), size=n_tickets)
    created_at = pd.to_datetime(created_epoch, unit="s")

    issue_type = rng.choice(cfg.ISSUE_TYPES, size=n_tickets)
    priority = rng.choice(cfg.PRIORITIES, size=n_tickets, p=cfg.PRIORITY_WEIGHTS)
    status = rng.choice(cfg.TICKET_STATUSES, size=n_tickets, p=cfg.TICKET_STATUS_WEIGHTS)

    is_resolved = np.isin(status, list(_RESOLVED_STATUSES))
    # resolution time in hours: urgent tickets resolved faster on average
    base_hours = np.where(priority == "urgent", 4, np.where(priority == "high", 12, np.where(priority == "medium", 30, 60)))
    resolution_time = np.where(
        is_resolved,
        np.round(rng.exponential(scale=base_hours) + 0.5, 1),
        np.nan,
    )

    tickets_df = pd.DataFrame({
        "ticket_id": [make_id("TCK", i) for i in range(1, n_tickets + 1)],
        "customer_id": customer_ids,
        "created_at": created_at,
        "issue_type": issue_type,
        "priority": priority,
        "status": status,
        "resolution_time": resolution_time,
    })

    if dirty_rate > 0:
        tickets_df["issue_type"] = inject_missing(tickets_df["issue_type"], dirty_rate / 3, rng)
        tickets_df["customer_id"] = inject_missing(tickets_df["customer_id"], dirty_rate / 4, rng)

    logger.info("Generated %s customer_support tickets (dirty_rate=%s)", len(tickets_df), dirty_rate)
    return tickets_df


if __name__ == "__main__":
    from data_generator.customers import generate_customers

    scale = cfg.SCALE_PRESETS["test"]
    customers_df = generate_customers(scale.n_customers, dirty_rate=0)
    tickets_df = generate_customer_support(customers_df, scale.n_support_tickets)
    print(tickets_df.head())
    print(tickets_df.shape)
