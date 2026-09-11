"""
clean_marketing.py — Cleans raw `marketing_campaigns` and
`campaign_interactions`, validates interactions against both cleaned
campaigns and cleaned customers, writes processed Parquet.

Usage:
    python -m spark.jobs.clean_marketing --batch-date 2026-09-06
"""
from __future__ import annotations

import argparse

from pyspark.sql import functions as F

from spark.utils.cleaning import (
    anti_join_orphans,
    deduplicate,
    drop_null_key,
    logger,
    read_raw_csv,
    write_processed_parquet,
)
from spark.utils.schemas import CAMPAIGN_INTERACTIONS_SCHEMA, MARKETING_CAMPAIGNS_SCHEMA
from spark.utils.spark_session import get_spark_session


def clean_marketing(spark, raw_campaigns_path: str, raw_interactions_path: str, clean_customers_path: str):
    customers = spark.read.parquet(clean_customers_path).select("customer_id")

    # ---- campaigns ----
    campaigns = read_raw_csv(spark, raw_campaigns_path, MARKETING_CAMPAIGNS_SCHEMA).cache()
    raw_campaign_count = campaigns.count()
    logger.info("[marketing_campaigns] read %s raw rows from %s", raw_campaign_count, raw_campaigns_path)

    campaigns = drop_null_key(campaigns, "campaign_id", "marketing_campaigns")
    campaigns = deduplicate(campaigns, ["campaign_id"], "marketing_campaigns")
    campaigns = campaigns.withColumn("_processed_at", F.current_timestamp())
    campaigns = campaigns.cache()

    clean_campaign_count = campaigns.count()
    logger.info("[marketing_campaigns] %s clean rows out of %s raw", clean_campaign_count, raw_campaign_count)

    # ---- campaign_interactions ----
    interactions = read_raw_csv(spark, raw_interactions_path, CAMPAIGN_INTERACTIONS_SCHEMA).cache()
    raw_interaction_count = interactions.count()
    logger.info("[campaign_interactions] read %s raw rows from %s", raw_interaction_count, raw_interactions_path)

    interactions = drop_null_key(interactions, "interaction_id", "campaign_interactions")
    interactions = deduplicate(interactions, ["interaction_id"], "campaign_interactions")
    # campaigns is always small (tens to hundreds of rows even at
    # `large` scale) -> broadcast is the right call here, unlike the
    # customers join right below it.
    interactions, _campaign_orphans = anti_join_orphans(
        interactions, campaigns.select("campaign_id"), "campaign_id", "campaign_interactions", use_broadcast=True
    )
    interactions, _customer_orphans = anti_join_orphans(
        interactions, customers, "customer_id", "campaign_interactions", use_broadcast=False
    )
    interactions = interactions.withColumn("_processed_at", F.current_timestamp())

    clean_interaction_count = interactions.count()
    logger.info("[campaign_interactions] %s clean rows out of %s raw (%.1f%% retained)",
                clean_interaction_count, raw_interaction_count, 100 * clean_interaction_count / max(raw_interaction_count, 1))

    return campaigns, interactions


def main() -> None:
    parser = argparse.ArgumentParser(description="Clean raw marketing data.")
    parser.add_argument("--batch-date", required=True)
    parser.add_argument("--base-dir", default="data")
    args = parser.parse_args()

    spark = get_spark_session("clean_marketing")
    try:
        bd, base = args.batch_date, args.base_dir
        campaigns, interactions = clean_marketing(
            spark,
            raw_campaigns_path=f"{base}/raw/marketing_campaigns/batch_date={bd}/marketing_campaigns.csv",
            raw_interactions_path=f"{base}/raw/campaign_interactions/batch_date={bd}/campaign_interactions.csv",
            clean_customers_path=f"{base}/processed/customers/batch_date={bd}/",
        )
        write_processed_parquet(campaigns, f"{base}/processed/marketing_campaigns/batch_date={bd}/", target_files=1)
        write_processed_parquet(interactions, f"{base}/processed/campaign_interactions/batch_date={bd}/", target_files=1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
