"""
customers.py — Generates the `customers` dataset (Customer Master Data).

Fields: customer_id, first_name, last_name, email, phone, gender,
date_of_birth, city, province, country, registration_date, customer_status
"""
from __future__ import annotations

import random
from datetime import datetime

import numpy as np
import pandas as pd
from faker import Faker

from data_generator import config as cfg
from data_generator.utils import (
    inject_duplicates,
    inject_invalid_emails,
    inject_missing,
    logger,
    make_id,
    random_date_between,
    weighted_choice,
)

fake = Faker()


def generate_customers(n: int, seed: int = cfg.RANDOM_SEED, dirty_rate: float = cfg.DEFAULT_DIRTY_RATE) -> pd.DataFrame:
    """Generate `n` synthetic customer records."""
    rng = random.Random(seed)
    np_rng = np.random.default_rng(seed)
    Faker.seed(seed)

    reg_start = datetime(2019, 1, 1)
    reg_end = datetime(2026, 9, 1)
    dob_start = datetime(1955, 1, 1)
    dob_end = datetime(2007, 1, 1)

    rows = []
    for i in range(1, n + 1):
        gender = weighted_choice(rng, cfg.GENDERS, cfg.GENDER_WEIGHTS)
        first_name = fake.first_name_male() if gender == "Male" else fake.first_name_female() if gender == "Female" else fake.first_name()
        last_name = fake.last_name()
        province, city = rng.choice(cfg.PROVINCES_CITIES)
        registration_date = random_date_between(rng, reg_start, reg_end)
        dob = random_date_between(rng, dob_start, dob_end)

        rows.append({
            "customer_id": make_id("CUST", i),
            "first_name": first_name,
            "last_name": last_name,
            "email": f"{first_name}.{last_name}{i}@example.com".lower(),
            "phone": fake.numerify("08########"),
            "gender": gender,
            "date_of_birth": dob.date().isoformat(),
            "city": city,
            "province": province,
            "country": cfg.COUNTRY,
            "registration_date": registration_date.date().isoformat(),
            "customer_status": weighted_choice(rng, cfg.CUSTOMER_STATUSES, cfg.CUSTOMER_STATUS_WEIGHTS),
        })

    df = pd.DataFrame(rows)

    # ---- Intentional data-quality issues (controlled, reproducible) ----
    if dirty_rate > 0:
        df["email"] = inject_invalid_emails(df["email"], dirty_rate, np_rng)
        df["first_name"] = inject_missing(df["first_name"], dirty_rate / 2, np_rng)
        df["phone"] = inject_missing(df["phone"], dirty_rate, np_rng)
        # A few customer_ids blanked out to violate "must not be NULL"
        blank_mask = np_rng.random(len(df)) < (dirty_rate / 4)
        df.loc[blank_mask, "customer_id"] = None
        # Duplicate whole rows (upstream re-ingestion is a common real-world
        # cause) so the Phase 6 uniqueness check has genuine duplicates to find.
        df = inject_duplicates(df, dirty_rate / 2, np_rng)

    logger.info("Generated %s customers (dirty_rate=%s)", len(df), dirty_rate)
    return df


if __name__ == "__main__":
    df = generate_customers(n=cfg.SCALE_PRESETS["test"].n_customers)
    print(df.head())
    print(df.shape)
