"""
run_checks.py — CLI entry point for the Data Quality gate.

Exits non-zero when any critical check falls below the pass-rate
threshold, so this can be wired directly into Airflow (Phase 7) as a
task that fails the DAG run — "pipeline must fail if data quality is
below the required threshold" (Section 22 of the spec).

Usage:
    python -m data_quality.run_checks --batch-date 2026-09-06
    python -m data_quality.run_checks --batch-date 2026-09-06 --min-pass-rate 0.90
"""
from __future__ import annotations

import argparse
import sys

from data_quality.checks import run_all_checks


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the Data Quality check suite for one batch_date.")
    parser.add_argument("--batch-date", required=True)
    parser.add_argument("--min-pass-rate", type=float, default=None, help="Override DQ_MIN_PASS_RATE from the environment.")
    parser.add_argument("--report-dir", default="data_quality/reports")
    args = parser.parse_args()

    results, overall_passed = run_all_checks(args.batch_date, args.min_pass_rate, args.report_dir)

    failed = [r for r in results if r.status == "FAIL"]
    print(f"\n{'='*70}\nData Quality Summary — batch_date={args.batch_date}\n{'='*70}")
    print(f"{'Dataset':<24}{'Check':<38}{'Pass Rate':>10}  Status")
    print("-" * 70)
    for r in results:
        marker = "CRIT" if r.critical else "info"
        print(f"{r.dataset:<24}{r.check_name:<38}{r.pass_rate*100:>9.1f}%  {r.status} [{marker}]")
    print("-" * 70)
    print(f"Total checks: {len(results)}  |  Failed: {len(failed)}  |  Overall: {'PASS' if overall_passed else 'FAIL'}")

    return 0 if overall_passed else 1


if __name__ == "__main__":
    sys.exit(main())
