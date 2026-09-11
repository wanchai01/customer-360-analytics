"""
cleaning.py — Shared I/O and data-cleaning helpers for every Spark job.

Centralizing these means each job (clean_customers.py, clean_orders.py,
...) reads as a short, readable pipeline of named steps instead of
repeating the same boilerplate regex/anti-join/reporting logic nine
times with subtle inconsistencies between copies.
"""
from __future__ import annotations

import logging
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

logger = logging.getLogger("spark_etl")
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")

EMAIL_REGEX = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"

# A row is considered plausible if its date falls in this window —
# anything outside it (e.g. a corrupted "1901-01-01" or a future date
# past the pipeline's own "as_of") is treated as invalid, not just
# "unusual", since no real customer/order should fall outside it.
MIN_VALID_DATE = date(2015, 1, 1)
MAX_VALID_DATE = date(2030, 12, 31)


# --------------------------------------------------------------------
# I/O
# --------------------------------------------------------------------
def read_raw_csv(spark: SparkSession, path: str, schema) -> DataFrame:
    """Read a raw CSV with an explicit schema (see schemas.py docstring
    for why: avoids the double-read cost and type-widening surprises
    of inferSchema=True)."""
    return (
        spark.read
        .schema(schema)
        .option("header", True)
        .option("mode", "PERMISSIVE")  # unparsable values -> NULL in that column, row is kept
        .csv(path)
    )


def read_raw_json(spark: SparkSession, path: str, schema) -> DataFrame:
    """Read raw newline-delimited JSON (website_events) with an explicit schema."""
    return spark.read.schema(schema).option("mode", "PERMISSIVE").json(path)


def write_processed_parquet(df: DataFrame, path: str, partition_cols: list[str] | None = None, target_files: int = 1) -> None:
    """
    Write a cleaned DataFrame to the processed layer as Parquet.

    `target_files` controls the output file count via coalesce():
    at `test`/`dev` scale a dataset comfortably fits in 1-4 files, and
    writing hundreds of tiny part-files (Spark's default, one per
    input task) would hurt every downstream reader — more files means
    more S3 LIST/GET overhead and more per-file overhead in Spark's
    own planning. At `large` scale, callers pass a higher
    `target_files` so each output file stays in the 128MB-1GB sweet
    spot for Parquet readers instead of coalescing everything into a
    single giant file that can't be read in parallel.
    """
    writer = df.coalesce(target_files).write.mode("overwrite")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.parquet(path)
    logger.info("Wrote processed Parquet -> %s (files=%s, partitioned_by=%s)", path, target_files, partition_cols)


# --------------------------------------------------------------------
# Cleaning primitives
# --------------------------------------------------------------------
def drop_null_key(df: DataFrame, key_col: str, dataset_name: str) -> DataFrame:
    """Drop rows with a NULL primary/business key — completeness rule:
    a record with no identifying key can't be reasoned about downstream."""
    before = df.count()
    result = df.filter(F.col(key_col).isNotNull())
    after = result.count()
    if before != after:
        logger.info("[%s] dropped %s/%s rows with NULL %s", dataset_name, before - after, before, key_col)
    return result


def deduplicate(df: DataFrame, key_cols: list[str], dataset_name: str) -> DataFrame:
    """Drop exact duplicate rows on the business key, keeping one copy
    (uniqueness rule). Uses dropDuplicates rather than a window+rank
    because the generator's injected duplicates are byte-for-byte
    copies — there's no "most recent" version to prefer, so the
    cheaper set-based dedup is both correct and faster than a
    row_number() window here."""
    before = df.count()
    result = df.dropDuplicates(key_cols)
    after = result.count()
    if before != after:
        logger.info("[%s] removed %s/%s duplicate rows on %s", dataset_name, before - after, before, key_cols)
    return result


def flag_invalid_email(df: DataFrame, email_col: str = "email") -> DataFrame:
    """Add `is_valid_email`; invalid emails are nulled out in a
    `_clean` column rather than dropping the whole row — a bad email
    doesn't invalidate the rest of a customer record."""
    return df.withColumn(
        "is_valid_email",
        F.col(email_col).isNotNull() & F.col(email_col).rlike(EMAIL_REGEX),
    ).withColumn(
        email_col,
        F.when(F.col("is_valid_email"), F.col(email_col)).otherwise(F.lit(None)),
    )


def clip_negative_amount(df: DataFrame, amount_col: str) -> DataFrame:
    """Null out negative amounts (validity rule: amounts must be >= 0)
    rather than silently flipping their sign — a negative amount is a
    data error, and guessing the "correct" positive value would be
    fabricating data. Downstream aggregations should COALESCE or
    filter these out explicitly, which is easier to reason about than
    a value that was silently mutated."""
    before = df.filter(F.col(amount_col) < 0).count()
    if before:
        logger.info("Nulling %s rows with negative %s", before, amount_col)
    return df.withColumn(
        amount_col,
        F.when(F.col(amount_col) >= 0, F.col(amount_col)).otherwise(F.lit(None)),
    )


def filter_valid_date_range(df: DataFrame, date_col: str, dataset_name: str,
                              min_date=MIN_VALID_DATE, max_date=MAX_VALID_DATE) -> DataFrame:
    """Null out dates outside a plausible range — catches corrupted
    dates (already NULL from schema mis-parse) plus implausible-but-
    parseable ones like a year-1900 date_of_birth."""
    before = df.filter(
        F.col(date_col).isNotNull() & ((F.col(date_col) < min_date) | (F.col(date_col) > max_date))
    ).count()
    if before:
        logger.info("[%s] nulling %s rows with %s outside [%s, %s]", dataset_name, before, date_col, min_date, max_date)
    return df.withColumn(
        date_col,
        F.when(
            F.col(date_col).isNull() | ((F.col(date_col) >= min_date) & (F.col(date_col) <= max_date)),
            F.col(date_col),
        ).otherwise(F.lit(None)),
    )


def validate_allowed_values(df: DataFrame, col: str, allowed: list[str], dataset_name: str) -> DataFrame:
    """Null out any value not in an allowed set (e.g. order_status must
    be one of the known statuses) — catches both NULLs and any
    unexpected/typo'd category value the same way."""
    before = df.filter(~F.col(col).isin(allowed) | F.col(col).isNull()).count()
    if before:
        logger.info("[%s] %s rows have invalid/missing %s (not in %s)", dataset_name, before, col, allowed)
    return df.withColumn(col, F.when(F.col(col).isin(allowed), F.col(col)).otherwise(F.lit(None)))


def anti_join_orphans(
    fact_df: DataFrame,
    dim_df: DataFrame,
    join_key: str,
    dataset_name: str,
    use_broadcast: bool = True,
) -> tuple[DataFrame, DataFrame]:
    """
    Referential integrity check: split `fact_df` into (valid, orphan)
    based on whether `join_key` exists in `dim_df`.

    `use_broadcast=True` applies a broadcast hint — appropriate when
    `dim_df` is small enough to ship to every executor (e.g. products,
    campaigns: thousands of rows even at `large` scale) so the join
    avoids a full shuffle of the much bigger fact table entirely. For
    a potentially-large dimension (e.g. validating against `customers`
    at `large` scale, ~1M rows), callers should pass
    `use_broadcast=False` — broadcasting a dimension that big would
    spend more time serializing/shipping it to every executor than a
    normal sort-merge join would take, and risks executor OOMs.
    """
    dim_keys = dim_df.select(join_key).distinct()
    if use_broadcast:
        dim_keys = broadcast(dim_keys)

    valid = fact_df.join(dim_keys, on=join_key, how="left_semi")
    orphans = fact_df.join(dim_keys, on=join_key, how="left_anti")

    orphan_count = orphans.count()
    if orphan_count:
        logger.info("[%s] %s rows reference a %s not found in the dimension (dropped)", dataset_name, orphan_count, join_key)
    return valid, orphans
