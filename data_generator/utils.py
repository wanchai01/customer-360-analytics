"""
utils.py — Shared helpers for all data_generator scripts.

Centralizing RNG handling, weighted sampling, and file output keeps each
per-entity generator script focused on *what* data looks like, not *how*
it gets written or sampled.
"""
from __future__ import annotations

import logging
import os
import random
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

logger = logging.getLogger("data_generator")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)


def set_global_seed(seed: int) -> None:
    """Seed every RNG we use so a run is fully reproducible."""
    random.seed(seed)
    np.random.seed(seed)


def weighted_choice(rng: random.Random, options: list, weights: list):
    """Thin wrapper around random.choices for single-item weighted picks."""
    return rng.choices(options, weights=weights, k=1)[0]


def random_date_between(rng: random.Random, start: datetime, end: datetime) -> datetime:
    """Uniform random datetime between two datetimes (inclusive of start)."""
    delta = end - start
    total_seconds = max(int(delta.total_seconds()), 1)
    offset = rng.randint(0, total_seconds)
    return start + timedelta(seconds=offset)


def make_id(prefix: str, number: int, width: int = 8) -> str:
    """Consistent, human-readable surrogate key, e.g. CUST00000123."""
    return f"{prefix}{number:0{width}d}"


def get_output_dir(base_data_dir: str, layer: str, dataset: str, batch_date: str) -> Path:
    """
    Build (and create) a partitioned output path mirroring the S3 raw
    layer layout described in the architecture doc:
        data/raw/<dataset>/batch_date=<YYYY-MM-DD>/
    Partitioning by batch_date makes re-runs idempotent: re-generating
    a batch overwrites only that partition, not the whole dataset.
    """
    path = Path(base_data_dir) / layer / dataset / f"batch_date={batch_date}"
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_dataframe(df: pd.DataFrame, out_dir: Path, dataset: str, fmt: str = "csv") -> Path:
    """Write a DataFrame as CSV or JSON-lines, matching the raw-layer format."""
    if fmt == "csv":
        out_path = out_dir / f"{dataset}.csv"
        df.to_csv(out_path, index=False)
    elif fmt == "json":
        out_path = out_dir / f"{dataset}.json"
        # date_format="iso" is required here: pandas' default for
        # datetime columns in to_json is epoch *milliseconds*, but
        # Spark's TimestampType JSON parser (spark/utils/schemas.py)
        # interprets a bare numeric literal as epoch *seconds* — a
        # 1000x scale mismatch that silently turns 2026 into the year
        # 58642. Discovered when Spark's window-function output on
        # website_events came back with dates in the year 50000s;
        # ISO8601 strings are unambiguous across every reader (Spark,
        # pandas, jq, a human) and are what the CSV raw files already
        # use for their date columns, so this also makes the two raw
        # formats consistent with each other.
        df.to_json(out_path, orient="records", lines=True, force_ascii=False, date_format="iso")
    else:
        raise ValueError(f"Unsupported raw format: {fmt}")
    logger.info("Wrote %s rows -> %s", len(df), out_path)
    return out_path


def inject_missing(series: pd.Series, rate: float, rng: np.random.Generator) -> pd.Series:
    """Randomly null out `rate` fraction of values — simulates real-world
    missing data for the Data Quality framework to catch (completeness)."""
    if rate <= 0:
        return series
    mask = rng.random(len(series)) < rate
    result = series.copy()
    result[mask] = None
    return result


def inject_duplicates(df: pd.DataFrame, rate: float, rng: np.random.Generator) -> pd.DataFrame:
    """Append duplicate rows of a random `rate` fraction of records —
    simulates upstream duplicate ingestion (uniqueness checks)."""
    if rate <= 0 or len(df) == 0:
        return df
    n_dupes = max(int(len(df) * rate), 0)
    if n_dupes == 0:
        return df
    dupe_rows = df.sample(n=n_dupes, replace=True, random_state=int(rng.integers(0, 1_000_000)))
    return pd.concat([df, dupe_rows], ignore_index=True)


def inject_invalid_emails(series: pd.Series, rate: float, rng: np.random.Generator) -> pd.Series:
    """Corrupt `rate` fraction of emails to violate a basic email regex
    (validity check)."""
    if rate <= 0:
        return series
    mask = rng.random(len(series)) < rate
    result = series.copy()
    corrupted = result[mask].astype(str).str.replace("@", "_at_", regex=False)
    result.loc[mask] = corrupted
    return result


def inject_negative_amounts(series: pd.Series, rate: float, rng: np.random.Generator) -> pd.Series:
    """Flip the sign of `rate` fraction of a numeric amount column
    (accuracy / validity check: amounts must be >= 0)."""
    if rate <= 0:
        return series
    mask = rng.random(len(series)) < rate
    result = series.copy()
    result[mask] = -result[mask].abs()
    return result


def inject_orphan_fk(series: pd.Series, rate: float, rng: np.random.Generator, bogus_prefix: str) -> pd.Series:
    """Replace `rate` fraction of a foreign-key column with an ID that
    does not exist in the parent table (referential integrity check)."""
    if rate <= 0:
        return series
    mask = rng.random(len(series)) < rate
    result = series.copy()
    bogus_ids = [f"{bogus_prefix}{rng.integers(9_000_000, 9_999_999)}" for _ in range(int(mask.sum()))]
    result.loc[mask] = bogus_ids
    return result


def to_epoch_seconds(series: pd.Series) -> np.ndarray:
    """
    Convert a datetime-like Series to whole-second Unix epoch integers.

    NOTE: pandas may represent datetime64 with 'ns', 'us', or 's'
    resolution depending on version/input, so a naive
    `.astype("int64") // 10**9` silently gives wrong results when the
    underlying unit isn't nanoseconds (e.g. 'us' -> values 1000x too
    large -> divide by 10**9 leaves microsecond-scale numbers, not
    seconds). Going through Timedelta-since-epoch avoids that trap
    regardless of the Series' internal resolution.
    """
    dt = pd.to_datetime(series)
    return ((dt - pd.Timestamp("1970-01-01")) // pd.Timedelta(seconds=1)).to_numpy()


def env_int(name: str, default: int) -> int:
    """Read an int from the environment, falling back to a default."""
    val = os.environ.get(name)
    return int(val) if val else default
