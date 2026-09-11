"""
checks.py — The Data Quality check engine.

Each check is a single SQL statement of the shape:

    SELECT COUNT(*) AS total, COUNT(*) FILTER (WHERE <bad_condition>) AS failed
    FROM <table> [JOIN ...]
    WHERE _batch_date = %(batch_date)s [AND ...]

`<bad_condition>` describes rows that VIOLATE the rule (the opposite
of what you'd write for a "valid" filter) — this keeps every check's
SQL readable as "count everything, then count what's wrong", and
keeps `failed_records` and `pass_rate` derivable the same way for
every check regardless of which quality dimension it covers.

Six dimensions are represented (Section 10 of the spec), each with
several concrete checks rather than one check per dimension — a real
DQ suite has many small, specific rules, not one giant one per
category:
    Completeness         : required column IS NOT NULL
    Uniqueness            : business key has no duplicate rows
    Validity              : format/range/allowed-value rules
    Accuracy              : value plausibility (e.g. cost <= selling_price)
    Referential integrity : foreign key exists in the parent table
    Consistency           : the same fact agrees across two tables
"""
from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import psycopg2

logger = logging.getLogger("data_quality")
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")

DEFAULT_MIN_PASS_RATE = float(os.environ.get("DQ_MIN_PASS_RATE", "0.95"))


@dataclass(frozen=True)
class DQCheck:
    dataset: str
    check_name: str
    dimension: str  # completeness | uniqueness | validity | accuracy | referential_integrity | consistency
    sql: str        # must SELECT total, failed and accept %(batch_date)s
    critical: bool = True  # if False, a failure is recorded but doesn't fail the overall run


@dataclass
class DQResult:
    dataset: str
    check_name: str
    dimension: str
    total_records: int
    failed_records: int
    pass_rate: float  # computed in Python for logic/reporting; NOT inserted (the DB column is GENERATED ALWAYS from total/failed_records)
    status: str  # PASS | FAIL
    execution_time_seconds: float
    critical: bool


def get_connection():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ.get("POSTGRES_DB", "customer360"),
        user=os.environ.get("POSTGRES_USER", "customer360_app"),
        password=os.environ.get("POSTGRES_PASSWORD", ""),
    )


# --------------------------------------------------------------------
# Check registry
# --------------------------------------------------------------------
CHECKS: list[DQCheck] = [
    # ---------------- Completeness ----------------
    DQCheck("customers", "customer_id_not_null", "completeness",
            "SELECT COUNT(*) AS total, COUNT(*) FILTER (WHERE customer_id IS NULL) AS failed "
            "FROM staging.stg_customers WHERE _batch_date = %(batch_date)s"),
    DQCheck("customers", "email_not_null", "completeness",
            "SELECT COUNT(*) AS total, COUNT(*) FILTER (WHERE email IS NULL) AS failed "
            "FROM staging.stg_customers WHERE _batch_date = %(batch_date)s"),
    DQCheck("orders", "order_id_not_null", "completeness",
            "SELECT COUNT(*) AS total, COUNT(*) FILTER (WHERE order_id IS NULL) AS failed "
            "FROM staging.stg_orders WHERE _batch_date = %(batch_date)s"),
    DQCheck("orders", "customer_id_not_null", "completeness",
            "SELECT COUNT(*) AS total, COUNT(*) FILTER (WHERE customer_id IS NULL) AS failed "
            "FROM staging.stg_orders WHERE _batch_date = %(batch_date)s"),
    DQCheck("order_items", "product_id_not_null", "completeness",
            "SELECT COUNT(*) AS total, COUNT(*) FILTER (WHERE product_id IS NULL) AS failed "
            "FROM staging.stg_order_items WHERE _batch_date = %(batch_date)s"),

    # ---------------- Uniqueness ----------------
    DQCheck("customers", "customer_id_unique", "uniqueness",
            "SELECT COUNT(*) AS total, "
            "GREATEST(COUNT(*) FILTER (WHERE customer_id IS NOT NULL) - COUNT(DISTINCT customer_id), 0) AS failed "
            "FROM staging.stg_customers WHERE _batch_date = %(batch_date)s"),
    DQCheck("orders", "order_id_unique", "uniqueness",
            "SELECT COUNT(*) AS total, "
            "GREATEST(COUNT(*) FILTER (WHERE order_id IS NOT NULL) - COUNT(DISTINCT order_id), 0) AS failed "
            "FROM staging.stg_orders WHERE _batch_date = %(batch_date)s"),
    DQCheck("order_items", "order_item_id_unique", "uniqueness",
            "SELECT COUNT(*) AS total, "
            "GREATEST(COUNT(*) FILTER (WHERE order_item_id IS NOT NULL) - COUNT(DISTINCT order_item_id), 0) AS failed "
            "FROM staging.stg_order_items WHERE _batch_date = %(batch_date)s"),

    # ---------------- Validity ----------------
    DQCheck("customers", "email_format_valid", "validity",
            r"SELECT COUNT(*) AS total, "
            r"COUNT(*) FILTER (WHERE email IS NULL OR email !~ '^[A-Za-z0-9._%%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$') AS failed "
            r"FROM staging.stg_customers WHERE _batch_date = %(batch_date)s"),
    DQCheck("customers", "customer_status_valid", "validity",
            "SELECT COUNT(*) AS total, "
            "COUNT(*) FILTER (WHERE customer_status IS NULL OR customer_status NOT IN ('active','inactive','churned')) AS failed "
            "FROM staging.stg_customers WHERE _batch_date = %(batch_date)s"),
    DQCheck("orders", "order_status_valid", "validity",
            "SELECT COUNT(*) AS total, "
            "COUNT(*) FILTER (WHERE order_status IS NULL OR order_status NOT IN ('completed','cancelled','pending','refunded')) AS failed "
            "FROM staging.stg_orders WHERE _batch_date = %(batch_date)s"),
    DQCheck("orders", "total_amount_non_negative", "validity",
            "SELECT COUNT(*) AS total, COUNT(*) FILTER (WHERE total_amount < 0) AS failed "
            "FROM staging.stg_orders WHERE _batch_date = %(batch_date)s"),
    DQCheck("order_items", "unit_price_non_negative", "validity",
            "SELECT COUNT(*) AS total, COUNT(*) FILTER (WHERE unit_price < 0) AS failed "
            "FROM staging.stg_order_items WHERE _batch_date = %(batch_date)s"),
    DQCheck("payments", "amount_non_negative", "validity",
            "SELECT COUNT(*) AS total, COUNT(*) FILTER (WHERE amount < 0) AS failed "
            "FROM staging.stg_payments WHERE _batch_date = %(batch_date)s"),

    # ---------------- Accuracy ----------------
    DQCheck("products", "cost_not_exceeding_selling_price", "accuracy",
            "SELECT COUNT(*) AS total, "
            "COUNT(*) FILTER (WHERE cost IS NOT NULL AND selling_price IS NOT NULL AND cost > selling_price) AS failed "
            "FROM staging.stg_products WHERE _batch_date = %(batch_date)s"),
    DQCheck("orders", "total_amount_matches_order_items", "accuracy",
            "WITH item_totals AS ("
            "  SELECT order_id, SUM(quantity * unit_price * (1 - COALESCE(discount,0))) AS computed_total "
            "  FROM staging.stg_order_items WHERE _batch_date = %(batch_date)s GROUP BY order_id"
            ") "
            "SELECT COUNT(*) AS total, "
            "COUNT(*) FILTER (WHERE ABS(COALESCE(o.total_amount,0) - COALESCE(it.computed_total,0)) > 1.0) AS failed "
            "FROM staging.stg_orders o LEFT JOIN item_totals it ON it.order_id = o.order_id "
            "WHERE o._batch_date = %(batch_date)s", critical=False),

    # ---------------- Referential Integrity ----------------
    DQCheck("orders", "customer_id_exists_in_customers", "referential_integrity",
            "SELECT COUNT(*) AS total, "
            "COUNT(*) FILTER (WHERE o.customer_id IS NOT NULL AND c.customer_id IS NULL) AS failed "
            "FROM staging.stg_orders o "
            "LEFT JOIN staging.stg_customers c ON c.customer_id = o.customer_id AND c._batch_date = %(batch_date)s "
            "WHERE o._batch_date = %(batch_date)s"),
    DQCheck("order_items", "product_id_exists_in_products", "referential_integrity",
            "SELECT COUNT(*) AS total, "
            "COUNT(*) FILTER (WHERE oi.product_id IS NOT NULL AND p.product_id IS NULL) AS failed "
            "FROM staging.stg_order_items oi "
            "LEFT JOIN staging.stg_products p ON p.product_id = oi.product_id AND p._batch_date = %(batch_date)s "
            "WHERE oi._batch_date = %(batch_date)s"),
    DQCheck("order_items", "order_id_exists_in_orders", "referential_integrity",
            "SELECT COUNT(*) AS total, "
            "COUNT(*) FILTER (WHERE oi.order_id IS NOT NULL AND o.order_id IS NULL) AS failed "
            "FROM staging.stg_order_items oi "
            "LEFT JOIN staging.stg_orders o ON o.order_id = oi.order_id AND o._batch_date = %(batch_date)s "
            "WHERE oi._batch_date = %(batch_date)s"),
    DQCheck("payments", "order_id_exists_in_orders", "referential_integrity",
            "SELECT COUNT(*) AS total, "
            "COUNT(*) FILTER (WHERE pay.order_id IS NOT NULL AND o.order_id IS NULL) AS failed "
            "FROM staging.stg_payments pay "
            "LEFT JOIN staging.stg_orders o ON o.order_id = pay.order_id AND o._batch_date = %(batch_date)s "
            "WHERE pay._batch_date = %(batch_date)s"),
    DQCheck("website_events", "customer_id_exists_in_customers", "referential_integrity",
            "SELECT COUNT(*) AS total, "
            "COUNT(*) FILTER (WHERE e.customer_id IS NOT NULL AND c.customer_id IS NULL) AS failed "
            "FROM staging.stg_website_events e "
            "LEFT JOIN staging.stg_customers c ON c.customer_id = e.customer_id AND c._batch_date = %(batch_date)s "
            "WHERE e._batch_date = %(batch_date)s"),
    DQCheck("campaign_interactions", "campaign_id_exists_in_campaigns", "referential_integrity",
            "SELECT COUNT(*) AS total, "
            "COUNT(*) FILTER (WHERE ci.campaign_id IS NOT NULL AND camp.campaign_id IS NULL) AS failed "
            "FROM staging.stg_campaign_interactions ci "
            "LEFT JOIN staging.stg_marketing_campaigns camp ON camp.campaign_id = ci.campaign_id AND camp._batch_date = %(batch_date)s "
            "WHERE ci._batch_date = %(batch_date)s"),

    # ---------------- Consistency ----------------
    # payments.amount should agree with the order it's paying for —
    # a cross-table fact that should never disagree with itself.
    DQCheck("payments", "amount_matches_order_total", "consistency",
            "SELECT COUNT(*) AS total, "
            "COUNT(*) FILTER (WHERE pay.amount IS NOT NULL AND o.total_amount IS NOT NULL "
            "AND ABS(pay.amount - o.total_amount) > 0.01) AS failed "
            "FROM staging.stg_payments pay "
            "JOIN staging.stg_orders o ON o.order_id = pay.order_id AND o._batch_date = %(batch_date)s "
            "WHERE pay._batch_date = %(batch_date)s", critical=False),
]


# --------------------------------------------------------------------
# Runner
# --------------------------------------------------------------------
def run_check(conn, check: DQCheck, batch_date: str, min_pass_rate: float = DEFAULT_MIN_PASS_RATE) -> DQResult:
    start = time.perf_counter()
    with conn.cursor() as cur:
        cur.execute(check.sql, {"batch_date": batch_date})
        total, failed = cur.fetchone()
    elapsed_seconds = time.perf_counter() - start

    total = total or 0
    failed = failed or 0
    pass_rate = 1.0 if total == 0 else round((total - failed) / total, 4)
    status = "PASS" if pass_rate >= min_pass_rate else "FAIL"

    return DQResult(
        dataset=check.dataset,
        check_name=check.check_name,
        dimension=check.dimension,
        total_records=total,
        failed_records=failed,
        pass_rate=pass_rate,
        status=status,
        execution_time_seconds=round(elapsed_seconds, 4),
        critical=check.critical,
    )


def persist_result(conn, result: DQResult, batch_date: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO analytics.data_quality_results
                (dataset, check_name, total_records, failed_records, status, execution_time, batch_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (result.dataset, result.check_name, result.total_records,
             result.failed_records, result.status, result.execution_time_seconds, batch_date),
        )
    conn.commit()


def run_all_checks(batch_date: str, min_pass_rate: float | None = None, report_dir: str = "data_quality/reports") -> tuple[list[DQResult], bool]:
    """
    Runs every registered check, persists each result to
    analytics.data_quality_results, writes a JSON summary report, and
    returns (results, overall_passed).

    overall_passed is False if ANY critical check falls below
    min_pass_rate — non-critical checks (accuracy/consistency checks
    marked critical=False above) are recorded and reported but don't
    block the pipeline, since a few penny-level rounding mismatches
    shouldn't halt a production run the way a broken foreign key
    should.
    """
    threshold = min_pass_rate if min_pass_rate is not None else DEFAULT_MIN_PASS_RATE
    conn = get_connection()
    results: list[DQResult] = []
    try:
        # Idempotent re-run: clear this batch_date's previous results
        # before inserting fresh ones, matching the same
        # delete-then-load pattern used by staging and the data
        # generator's partitioned output elsewhere in this project.
        with conn.cursor() as cur:
            cur.execute("DELETE FROM analytics.data_quality_results WHERE batch_date = %s", (batch_date,))
        conn.commit()

        for check in CHECKS:
            result = run_check(conn, check, batch_date, min_pass_rate=threshold)
            persist_result(conn, result, batch_date)
            results.append(result)
            log_fn = logger.info if result.status == "PASS" else logger.warning
            log_fn(
                "[%s] %s/%s :: %s/%s failed, pass_rate=%.2f%% -> %s",
                result.dataset, result.check_name, result.dimension,
                result.failed_records, result.total_records, result.pass_rate * 100, result.status,
            )
    finally:
        conn.close()

    critical_failures = [r for r in results if r.critical and r.pass_rate < threshold]
    overall_passed = len(critical_failures) == 0

    _write_report(batch_date, results, threshold, overall_passed, report_dir)

    if critical_failures:
        logger.error(
            "=== DATA QUALITY GATE FAILED: %s critical check(s) below %.0f%% threshold: %s ===",
            len(critical_failures), threshold * 100, [f"{r.dataset}.{r.check_name}" for r in critical_failures],
        )
    else:
        logger.info("=== DATA QUALITY GATE PASSED: all %s critical checks >= %.0f%% ===",
                     len(results) - sum(not r.critical for r in results), threshold * 100)

    return results, overall_passed


def _write_report(batch_date: str, results: list[DQResult], threshold: float, overall_passed: bool, report_dir: str) -> Path:
    Path(report_dir).mkdir(parents=True, exist_ok=True)
    report_path = Path(report_dir) / f"dq_report_{batch_date}.json"

    payload = {
        "batch_date": batch_date,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "min_pass_rate_threshold": threshold,
        "overall_status": "PASS" if overall_passed else "FAIL",
        "checks_total": len(results),
        "checks_failed": sum(1 for r in results if r.status == "FAIL"),
        "results": [
            {
                "dataset": r.dataset, "check_name": r.check_name, "dimension": r.dimension,
                "total_records": r.total_records, "failed_records": r.failed_records,
                "pass_rate": r.pass_rate, "status": r.status, "critical": r.critical,
                "execution_time_seconds": r.execution_time_seconds,
            }
            for r in results
        ],
    }
    report_path.write_text(json.dumps(payload, indent=2))
    logger.info("Wrote DQ report -> %s", report_path)
    return report_path
