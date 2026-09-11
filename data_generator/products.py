"""
products.py — Generates the `products` dataset.

Fields: product_id, product_name, category, subcategory, brand, cost, selling_price
"""
from __future__ import annotations

import random

import numpy as np
import pandas as pd
from faker import Faker

from data_generator import config as cfg
from data_generator.utils import inject_missing, logger, make_id

fake = Faker()

# A handful of descriptive nouns per category to build believable product names
_NAME_ADJECTIVES = ["Pro", "Max", "Lite", "Plus", "Classic", "Ultra", "Essential", "Compact", "Series X", "2.0"]


def generate_products(n: int, seed: int = cfg.RANDOM_SEED, dirty_rate: float = cfg.DEFAULT_DIRTY_RATE) -> pd.DataFrame:
    """Generate `n` synthetic product records with cost < selling_price."""
    rng = random.Random(seed)
    np_rng = np.random.default_rng(seed)
    Faker.seed(seed)

    categories = list(cfg.PRODUCT_CATEGORIES.keys())

    rows = []
    for i in range(1, n + 1):
        category = rng.choice(categories)
        subcategory = rng.choice(cfg.PRODUCT_CATEGORIES[category])
        brand = rng.choice(cfg.BRANDS)
        adjective = rng.choice(_NAME_ADJECTIVES)
        product_name = f"{brand} {subcategory.split()[0]} {adjective}"

        # Cost/price scale roughly by category to feel realistic
        base = {
            "Electronics": (800, 45000),
            "Fashion": (150, 4500),
            "Home & Living": (200, 12000),
            "Beauty & Health": (80, 2500),
            "Sports & Outdoor": (150, 8000),
            "Groceries": (20, 600),
            "Books & Stationery": (30, 900),
            "Baby & Kids": (100, 3500),
        }[category]
        selling_price = round(rng.uniform(*base), 2)
        margin = rng.uniform(0.15, 0.45)  # cost is 55-85% of selling price
        cost = round(selling_price * (1 - margin), 2)

        rows.append({
            "product_id": make_id("PROD", i),
            "product_name": product_name,
            "category": category,
            "subcategory": subcategory,
            "brand": brand,
            "cost": cost,
            "selling_price": selling_price,
        })

    df = pd.DataFrame(rows)

    if dirty_rate > 0:
        df["brand"] = inject_missing(df["brand"], dirty_rate / 2, np_rng)
        # A few products with cost > selling_price (accuracy issue)
        flip_mask = np_rng.random(len(df)) < (dirty_rate / 3)
        df.loc[flip_mask, ["cost", "selling_price"]] = df.loc[flip_mask, ["selling_price", "cost"]].values

    logger.info("Generated %s products (dirty_rate=%s)", len(df), dirty_rate)
    return df


if __name__ == "__main__":
    df = generate_products(n=cfg.SCALE_PRESETS["test"].n_products)
    print(df.head())
    print(df.shape)
