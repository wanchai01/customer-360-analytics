"""
marketing.py — Generates `marketing_campaigns` and `campaign_interactions`.

marketing_campaigns: campaign_id, campaign_name, channel, start_date, end_date, budget
campaign_interactions: interaction_id, campaign_id, customer_id, interaction_type, interaction_timestamp
"""
from __future__ import annotations

from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from data_generator import config as cfg
from data_generator.utils import inject_missing, logger, make_id, to_epoch_seconds

_CAMPAIGN_THEMES = [
    "New Year Sale", "Summer Blowout", "Flash Deal", "Loyalty Rewards",
    "Back to School", "Mid-Year Mega Sale", "Weekend Special",
    "App Exclusive", "Clearance Event", "Anniversary Sale",
]


def generate_campaigns(n_campaigns: int, seed: int = cfg.RANDOM_SEED) -> pd.DataFrame:
    """Generate `n_campaigns` marketing campaigns."""
    rng = np.random.default_rng(seed)

    window_start = datetime(2019, 1, 1)
    total_days = (datetime(2026, 9, 6) - window_start).days

    start_offsets = rng.integers(0, total_days, size=n_campaigns)
    durations = rng.integers(3, 30, size=n_campaigns)  # campaigns run 3-30 days
    start_dates = [window_start + timedelta(days=int(o)) for o in start_offsets]
    end_dates = [s + timedelta(days=int(d)) for s, d in zip(start_dates, durations)]

    campaigns_df = pd.DataFrame({
        "campaign_id": [make_id("CMP", i) for i in range(1, n_campaigns + 1)],
        "campaign_name": [f"{rng.choice(_CAMPAIGN_THEMES)} {s.year}" for s in start_dates],
        "channel": rng.choice(cfg.CAMPAIGN_CHANNELS, size=n_campaigns),
        "start_date": [d.date().isoformat() for d in start_dates],
        "end_date": [d.date().isoformat() for d in end_dates],
        "budget": np.round(rng.uniform(5_000, 500_000, size=n_campaigns), 2),
    })

    logger.info("Generated %s marketing_campaigns", len(campaigns_df))
    return campaigns_df


def generate_campaign_interactions(
    campaigns_df: pd.DataFrame,
    customers_df: pd.DataFrame,
    n_interactions: int,
    seed: int = cfg.RANDOM_SEED,
    dirty_rate: float = cfg.DEFAULT_DIRTY_RATE,
) -> pd.DataFrame:
    """Generate `n_interactions` campaign touchpoints (impression/click/conversion)."""
    rng = np.random.default_rng(seed)

    n_campaigns = len(campaigns_df)
    n_customers = len(customers_df)

    chosen_campaign_idx = rng.integers(0, n_campaigns, size=n_interactions)
    chosen_customer_idx = rng.integers(0, n_customers, size=n_interactions)

    campaign_ids = campaigns_df["campaign_id"].to_numpy()[chosen_campaign_idx]
    customer_ids = customers_df["customer_id"].to_numpy()[chosen_customer_idx]

    campaign_start_epoch = to_epoch_seconds(campaigns_df["start_date"])
    campaign_end_epoch = to_epoch_seconds(campaigns_df["end_date"])
    lower = campaign_start_epoch[chosen_campaign_idx]
    upper = np.maximum(campaign_end_epoch[chosen_campaign_idx], lower + 1)
    interaction_epoch = lower + (rng.random(n_interactions) * (upper - lower)).astype("int64")

    interactions_df = pd.DataFrame({
        "interaction_id": [make_id("INT", i) for i in range(1, n_interactions + 1)],
        "campaign_id": campaign_ids,
        "customer_id": customer_ids,
        "interaction_type": rng.choice(cfg.INTERACTION_TYPES, size=n_interactions, p=cfg.INTERACTION_TYPE_WEIGHTS),
        "interaction_timestamp": pd.to_datetime(interaction_epoch, unit="s"),
    })

    if dirty_rate > 0:
        interactions_df["customer_id"] = inject_missing(interactions_df["customer_id"], dirty_rate / 3, rng)

    logger.info("Generated %s campaign_interactions (dirty_rate=%s)", len(interactions_df), dirty_rate)
    return interactions_df


if __name__ == "__main__":
    from data_generator.customers import generate_customers

    scale = cfg.SCALE_PRESETS["test"]
    customers_df = generate_customers(scale.n_customers, dirty_rate=0)
    campaigns_df = generate_campaigns(scale.n_campaigns)
    interactions_df = generate_campaign_interactions(campaigns_df, customers_df, scale.n_campaign_interactions)
    print(campaigns_df.head())
    print(interactions_df.head())
