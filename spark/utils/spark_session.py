"""
spark_session.py — Builds a configured SparkSession shared by every job.

Centralizing this means every job gets the same performance settings
(and the same S3/MinIO wiring) without repeating boilerplate, and
there's exactly one place to tune settings for the whole pipeline.
"""
from __future__ import annotations

import os

from pyspark.sql import SparkSession


def get_spark_session(app_name: str) -> SparkSession:
    """
    Build a SparkSession configured for this project's scale and
    environment (local dev / Docker Spark cluster / eventually EMR).

    Key settings and why:
      - spark.sql.adaptive.enabled: Adaptive Query Execution lets Spark
        re-optimize the shuffle partition count and join strategy at
        runtime based on actual data size, instead of the static plan
        chosen before any data was read — this matters a lot here
        because dataset sizes vary 1000x between the `test` and
        `large` scale presets (Section 5), and a fixed plan tuned for
        one would be wrong for the other.
      - spark.sql.shuffle.partitions: default is 200, tuned for
        cluster-scale data. At `test`/`dev` scale (thousands to low
        millions of rows) 200 tiny shuffle partitions add scheduling
        overhead with no parallelism benefit — we size it from
        available cores instead, and AQE coalesces further at runtime.
      - spark.sql.autoBroadcastJoinThreshold: raised from the 10MB
        default to comfortably cover the `products`/`campaigns`
        dimension tables at `dev`/`large` scale, so the broadcast
        joins in the cleaning jobs (see spark/utils/cleaning.py)
        actually engage automatically, without every call site
        needing an explicit broadcast() hint.
      - spark.sql.parquet.compression.codec: snappy — the standard
        trade-off of fast decompression (important for repeated
        analytical reads) over the last few % of compression ratio
        that a slower codec like gzip would buy.
    """
    master = os.environ.get("SPARK_MASTER_URL", "local[*]")
    driver_memory = os.environ.get("SPARK_DRIVER_MEMORY", "2g")
    executor_memory = os.environ.get("SPARK_EXECUTOR_MEMORY", "2g")

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.driver.memory", driver_memory)
        .config("spark.executor.memory", executor_memory)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", str(_default_shuffle_partitions()))
        .config("spark.sql.autoBroadcastJoinThreshold", 64 * 1024 * 1024)  # 64MB
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.session.timeZone", "UTC")
    )

    endpoint_url = os.environ.get("AWS_S3_ENDPOINT_URL")
    if endpoint_url:
        # MinIO / S3-compatible endpoint for local dev — mirrors the
        # same env-var switch used by data_lake/s3_client.py, so
        # Spark and the plain-Python boto3 code agree on where "the
        # lake" is without a second config source to keep in sync.
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.endpoint", endpoint_url)
            .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID", ""))
            .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY", ""))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _default_shuffle_partitions() -> int:
    """2x available cores is a common starting heuristic (enough
    parallelism to keep every core busy, not so many that tiny
    partitions dominate scheduling overhead) — AQE adjusts from here
    at runtime based on actual shuffle data size."""
    cpu_count = os.cpu_count() or 4
    return max(cpu_count * 2, 8)
