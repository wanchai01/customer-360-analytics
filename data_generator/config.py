"""
config.py — Central configuration for the Customer 360 data generator.

Keeping all volumes, distributions, and reference lists in one module
means every generator script (customers, products, orders, ...) stays
consistent with the same scale and the same reference data (e.g. the
same list of provinces / categories), and a scale change only touches
one place.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ScaleConfig:
    """Row-count targets for one named scale profile."""
    name: str
    n_customers: int
    n_products: int
    n_orders: int
    n_website_events: int
    n_support_tickets: int
    n_campaigns: int
    n_campaign_interactions: int


# ---------------------------------------------------------------------
# Scale presets
#   test  -> fast smoke test, runs in seconds, used in CI / local dev
#   dev   -> matches the "Development" volume in the spec (Section 5)
#   large -> matches the "Large Dataset Simulation" volume (Section 5)
# ---------------------------------------------------------------------
SCALE_PRESETS: dict[str, ScaleConfig] = {
    "test": ScaleConfig(
        name="test",
        n_customers=500,
        n_products=200,
        n_orders=1_500,
        n_website_events=5_000,
        n_support_tickets=150,
        n_campaigns=10,
        n_campaign_interactions=2_000,
    ),
    "dev": ScaleConfig(
        name="dev",
        n_customers=100_000,
        n_products=5_000,
        n_orders=500_000,
        n_website_events=2_000_000,
        n_support_tickets=25_000,
        n_campaigns=50,
        n_campaign_interactions=400_000,
    ),
    "large": ScaleConfig(
        name="large",
        n_customers=1_000_000,
        n_products=20_000,
        n_orders=10_000_000,
        n_website_events=50_000_000,
        n_support_tickets=500_000,
        n_campaigns=200,
        n_campaign_interactions=8_000_000,
    ),
}

# Average order_items per order (used to derive order_items volume)
AVG_ITEMS_PER_ORDER = 2.3

RANDOM_SEED = 42

# ---------------------------------------------------------------------
# Reference / dimension-like data shared across generators
# ---------------------------------------------------------------------
PROVINCES_CITIES = [
    # (province, representative city), skewed toward Bangkok metro like
    # a real Thai e-commerce customer base.
    ("Bangkok", "Bangkok"),
    ("Bangkok", "Bangkok"),
    ("Bangkok", "Bangkok"),
    ("Nonthaburi", "Nonthaburi"),
    ("Pathum Thani", "Pathum Thani"),
    ("Samut Prakan", "Samut Prakan"),
    ("Chiang Mai", "Chiang Mai"),
    ("Chiang Rai", "Chiang Rai"),
    ("Khon Kaen", "Khon Kaen"),
    ("Nakhon Ratchasima", "Nakhon Ratchasima"),
    ("Chonburi", "Pattaya"),
    ("Chonburi", "Chonburi"),
    ("Rayong", "Rayong"),
    ("Phuket", "Phuket"),
    ("Surat Thani", "Surat Thani"),
    ("Songkhla", "Hat Yai"),
    ("Udon Thani", "Udon Thani"),
    ("Ubon Ratchathani", "Ubon Ratchathani"),
    ("Nakhon Pathom", "Nakhon Pathom"),
    ("Ayutthaya", "Ayutthaya"),
]
COUNTRY = "Thailand"

CUSTOMER_STATUSES = ["active", "inactive", "churned"]
CUSTOMER_STATUS_WEIGHTS = [0.65, 0.20, 0.15]

GENDERS = ["Male", "Female", "Other"]
GENDER_WEIGHTS = [0.47, 0.47, 0.06]

ORDER_STATUSES = ["completed", "cancelled", "pending", "refunded"]
ORDER_STATUS_WEIGHTS = [0.78, 0.10, 0.07, 0.05]

PAYMENT_METHODS = ["credit_card", "bank_transfer", "e_wallet", "cod", "installment"]
PAYMENT_METHOD_WEIGHTS = [0.35, 0.20, 0.25, 0.15, 0.05]

PAYMENT_STATUSES = ["success", "failed", "pending", "refunded"]
PAYMENT_STATUS_WEIGHTS = [0.88, 0.05, 0.04, 0.03]

PRODUCT_CATEGORIES = {
    "Electronics": ["Mobile Phones", "Laptops", "Audio", "Cameras", "Accessories"],
    "Fashion": ["Men's Clothing", "Women's Clothing", "Shoes", "Bags", "Watches"],
    "Home & Living": ["Furniture", "Kitchenware", "Bedding", "Decor", "Appliances"],
    "Beauty & Health": ["Skincare", "Makeup", "Haircare", "Supplements", "Personal Care"],
    "Sports & Outdoor": ["Fitness Equipment", "Cycling", "Camping", "Sportswear", "Footwear"],
    "Groceries": ["Snacks", "Beverages", "Fresh Food", "Instant Food", "Household Supplies"],
    "Books & Stationery": ["Fiction", "Non-fiction", "Office Supplies", "Notebooks", "Art Supplies"],
    "Baby & Kids": ["Toys", "Baby Care", "Kids Clothing", "School Supplies", "Baby Gear"],
}
BRANDS = [
    "Nova", "Zenith", "Urbanix", "Kestrel", "Lumen", "Terra", "Vantage",
    "Solace", "Nimbus", "Everline", "Crestworth", "Halcyon", "Meridian",
    "Brightfield", "Norwood", "Aster & Co.", "Bluepeak", "Ridgeline",
]

EVENT_TYPES = ["page_view", "product_view", "add_to_cart", "remove_from_cart", "checkout_start", "purchase", "search"]
EVENT_TYPE_WEIGHTS = [0.35, 0.25, 0.13, 0.04, 0.08, 0.05, 0.10]

DEVICES = ["mobile", "desktop", "tablet"]
DEVICE_WEIGHTS = [0.68, 0.27, 0.05]

TRAFFIC_SOURCES = ["organic_search", "paid_search", "social_media", "direct", "email", "affiliate"]
TRAFFIC_SOURCE_WEIGHTS = [0.28, 0.22, 0.20, 0.15, 0.10, 0.05]

ISSUE_TYPES = ["delivery_delay", "product_defect", "wrong_item", "refund_request", "payment_issue", "general_inquiry", "account_issue"]
PRIORITIES = ["low", "medium", "high", "urgent"]
PRIORITY_WEIGHTS = [0.35, 0.40, 0.18, 0.07]
TICKET_STATUSES = ["open", "in_progress", "resolved", "closed"]
TICKET_STATUS_WEIGHTS = [0.08, 0.10, 0.32, 0.50]

CAMPAIGN_CHANNELS = ["email", "social_media", "search_ads", "display_ads", "sms", "affiliate"]
INTERACTION_TYPES = ["impression", "click", "conversion"]
INTERACTION_TYPE_WEIGHTS = [0.70, 0.24, 0.06]

# Data-quality "dirt" injection rate — intentionally introduces a small,
# controlled percentage of bad records (nulls, invalid formats, negative
# amounts, orphan foreign keys) so the Phase 6 Data Quality framework has
# real issues to detect. 0.0 disables dirt injection entirely.
DEFAULT_DIRTY_RATE = 0.02
