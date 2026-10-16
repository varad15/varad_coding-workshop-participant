"""
Silver stage orchestration + data quality report.

The DQ report reconciles every entity:  bronze_rows == silver_rows + quarantined_rows + duplicates_dropped
and is written to _meta/dq_reports/run_id=<id>/report.json and _meta/dq_reports/latest/report.json.
A reconciliation failure fails the stage.
"""

from __future__ import annotations

import json

from pyspark.sql import SparkSession

from common.lineage import RunContext
from common.logger import get_logger
from common.storage import DataLake
from silver.clean_events import clean_learning_events
from silver.clean_hr import clean_hr_tables
from silver.reference import build_course_competency_map

log = get_logger("silver")


class ReconciliationError(RuntimeError):
    """Rows were lost or invented between Bronze and Silver."""


def reconcile(stats: dict[str, dict[str, object]]) -> dict[str, bool]:
    """Check bronze == silver + quarantined + duplicates for each entity."""
    results = {}
    for entity, s in stats.items():
        if "bronze_rows" not in s:
            continue
        results[entity] = s["bronze_rows"] == s["silver_rows"] + s["quarantined_rows"] + s["duplicates_dropped"]
    return results


def run_silver(spark: SparkSession, lake: DataLake, run: RunContext) -> dict[str, dict[str, object]]:
    """Build every Silver dataset and publish the data quality report."""
    stats = clean_hr_tables(spark, lake, run)
    stats["learning_events"] = clean_learning_events(spark, lake, run)
    stats["course_competency_map"] = build_course_competency_map(spark, lake, run)

    checks = reconcile(stats)
    report = {"run_id": run.run_id, "entities": stats, "reconciliation": checks}
    body = json.dumps(report, indent=2, default=str).encode()
    lake.put_bytes(f"_meta/dq_reports/run_id={run.run_id}/report.json", body, "application/json")
    lake.put_bytes("_meta/dq_reports/latest/report.json", body, "application/json")
    log.info("dq_report_written", extra={"reconciliation": checks})

    failed = [e for e, ok in checks.items() if not ok]
    if failed:
        raise ReconciliationError(f"Bronze/Silver reconciliation failed for {failed}")
    return stats
