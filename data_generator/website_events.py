"""
website_events.py — Generates the `website_events` (clickstream) dataset.

Fields: event_id, customer_id, session_id, event_timestamp, event_type,
        page, product_id, device, traffic_source

Events are generated per *session* (a customer browsing in one sitting
shares the same device/traffic_source and has monotonically increasing
timestamps), then exploded into individual event rows — this keeps the
data internally consistent instead of assigning every field independently.
"""
from __future__ import annotations

from datetime import datetime

import numpy as np
import pandas as pd

from data_generator import config as cfg
from data_generator.utils import inject_missing, logger, make_id

AVG_EVENTS_PER_SESSION = 6

_PAGE_BY_EVENT = {
    "page_view": ["/home", "/category", "/deals", "/about"],
    "product_view": ["/product/{pid}"],
    "add_to_cart": ["/product/{pid}"],
    "remove_from_cart": ["/cart"],
    "checkout_start": ["/checkout"],
    "purchase": ["/checkout/confirmation"],
    "search": ["/search"],
}
_EVENTS_WITH_PRODUCT = {"product_view", "add_to_cart", "remove_from_cart", "purchase"}


def generate_website_events(
    customers_df: pd.DataFrame,
    products_df: pd.DataFrame,
    n_events: int,
    seed: int = cfg.RANDOM_SEED,
    dirty_rate: float = cfg.DEFAULT_DIRTY_RATE,
    as_of: datetime | None = None,
) -> pd.DataFrame:
    """Generate roughly `n_events` clickstream events across many sessions."""
    rng = np.random.default_rng(seed)
    as_of = as_of or datetime(2026, 9, 6)

    n_sessions = max(int(n_events / AVG_EVENTS_PER_SESSION), 1)
    events_per_session = rng.poisson(lam=AVG_EVENTS_PER_SESSION, size=n_sessions) + 1
    total_events = int(events_per_session.sum())

    # ---- session-level attributes ----
    n_customers = len(customers_df)
    session_customer_idx = rng.integers(0, n_customers, size=n_sessions)
    session_customer_ids = customers_df["customer_id"].to_numpy()[session_customer_idx]
    session_ids = np.array([make_id("SESS", i) for i in range(1, n_sessions + 1)])
    session_device = rng.choice(cfg.DEVICES, size=n_sessions, p=cfg.DEVICE_WEIGHTS)
    session_traffic = rng.choice(cfg.TRAFFIC_SOURCES, size=n_sessions, p=cfg.TRAFFIC_SOURCE_WEIGHTS)

    window_start = pd.Timestamp("2019-01-01").timestamp()
    window_end = pd.Timestamp(as_of).timestamp()
    session_start_epoch = rng.integers(int(window_start), int(window_end), size=n_sessions)

    # ---- explode sessions into events ----
    customer_id = np.repeat(session_customer_ids, events_per_session)
    session_id = np.repeat(session_ids, events_per_session)
    device = np.repeat(session_device, events_per_session)
    traffic_source = np.repeat(session_traffic, events_per_session)
    start_epoch = np.repeat(session_start_epoch, events_per_session)

    # seconds-within-session offsets: cumulative sum of small gaps, reset per session
    gaps = rng.integers(5, 180, size=total_events)  # 5s - 3min between actions
    session_group_id = np.repeat(np.arange(n_sessions), events_per_session)
    df_tmp = pd.DataFrame({"grp": session_group_id, "gap": gaps})
    within_session_offset = df_tmp.groupby("grp", sort=False)["gap"].cumsum().to_numpy() - gaps

    event_timestamp = pd.to_datetime(start_epoch + within_session_offset, unit="s")

    event_type = rng.choice(cfg.EVENT_TYPES, size=total_events, p=cfg.EVENT_TYPE_WEIGHTS)

    product_ids_all = products_df["product_id"].to_numpy()
    chosen_products = rng.choice(product_ids_all, size=total_events)
    has_product = np.isin(event_type, list(_EVENTS_WITH_PRODUCT))
    product_id = np.where(has_product, chosen_products, None)

    page = np.empty(total_events, dtype=object)
    for etype, templates in _PAGE_BY_EVENT.items():
        mask = event_type == etype
        n_mask = int(mask.sum())
        if n_mask == 0:
            continue
        chosen_templates = rng.choice(templates, size=n_mask)
        if etype in _EVENTS_WITH_PRODUCT:
            page[mask] = [t.format(pid=pid) for t, pid in zip(chosen_templates, chosen_products[mask])]
        else:
            page[mask] = chosen_templates

    events_df = pd.DataFrame({
        "event_id": [make_id("EVT", i) for i in range(1, total_events + 1)],
        "customer_id": customer_id,
        "session_id": session_id,
        "event_timestamp": event_timestamp,
        "event_type": event_type,
        "page": page,
        "product_id": product_id,
        "device": device,
        "traffic_source": traffic_source,
    })

    if dirty_rate > 0:
        events_df["customer_id"] = inject_missing(events_df["customer_id"], dirty_rate / 3, rng)
        events_df["event_type"] = inject_missing(events_df["event_type"], dirty_rate / 4, rng)

    logger.info("Generated %s website_events across %s sessions (dirty_rate=%s)", len(events_df), n_sessions, dirty_rate)
    return events_df


if __name__ == "__main__":
    from data_generator.customers import generate_customers
    from data_generator.products import generate_products

    scale = cfg.SCALE_PRESETS["test"]
    customers_df = generate_customers(scale.n_customers, dirty_rate=0)
    products_df = generate_products(scale.n_products, dirty_rate=0)
    events_df = generate_website_events(customers_df, products_df, scale.n_website_events)
    print(events_df.head())
    print(events_df.shape)
