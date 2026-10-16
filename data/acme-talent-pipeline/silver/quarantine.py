"""
Quarantine (dead-letter) helpers - data-engineer.md section 3.

Every rejected record is written to `quarantine/<entity>/` with one standard
schema, so all rejects can be inspected together:

    entity, reason, record_json, source_file, line_number, batch_id, run_id, quarantined_at

Quarantine is rebuilt on each run from Bronze (overwrite), so reruns never
duplicate rejects. Whole files rejected for schema drift are also copied to
`quarantine/files/...` untouched, next to a reason file.
"""

from __future__ import annotations

import json

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common.lineage import RunContext, utc_now
from common.logger import get_logger
from common.spark import to_arrow
from common.storage import DataLake

log = get_logger("quarantine")

QUARANTINE_PREFIX = "quarantine"
QUARANTINE_COLUMNS = [
    "entity",
    "reason",
    "record_json",
    "source_file",
    "line_number",
    "batch_id",
    "run_id",
    "quarantined_at",
]


def to_quarantine_rows(
    df: DataFrame,
    entity: str,
    reason_col: str,
    run: RunContext,
    record_col: str | None = None,
    record_fields: list[str] | None = None,
) -> DataFrame:
    """
    Shape rejected rows into the standard quarantine schema.

    Args:
        df: Rejected rows.
        entity: Entity name, e.g. 'employees'.
        reason_col: Column holding the rejection reason.
        run: Current run (for run_id).
        record_col: Column already holding the raw record (e.g. raw JSON line).
        record_fields: Otherwise, fields serialised to JSON as the record.
    """
    record = F.col(record_col) if record_col else F.to_json(F.struct(*[F.col(c) for c in record_fields or []]))
    cols = set(df.columns)
    return df.select(
        F.lit(entity).alias("entity"),
        F.col(reason_col).alias("reason"),
        record.alias("record_json"),
        (F.col("_source_file") if "_source_file" in cols else F.col("_source_table")).alias("source_file"),
        (F.col("_line_number").cast("int") if "_line_number" in cols else F.lit(None).cast("int")).alias("line_number"),
        F.col("_batch_id").alias("batch_id"),
        F.lit(run.run_id).alias("run_id"),
        F.lit(utc_now()).cast("timestamp").alias("quarantined_at"),
    )


def write_quarantine(lake: DataLake, entity: str, rows: DataFrame) -> int:
    """Overwrite quarantine/<entity>/ with the current rejects; return the count."""
    table = to_arrow(rows.select(*QUARANTINE_COLUMNS))
    lake.write_table(table, f"{QUARANTINE_PREFIX}/{entity}")
    if table.num_rows:
        log.warning("records_quarantined", extra={"entity": entity, "rows": table.num_rows})
    return table.num_rows


def quarantine_file(lake: DataLake, source_key: str, reason: str, run: RunContext) -> str:
    """Copy a whole source file into quarantine/files/ and write its reason."""
    target = f"{QUARANTINE_PREFIX}/files/{source_key}"
    lake.client.copy_object(Bucket=lake.bucket, Key=target, CopySource={"Bucket": lake.bucket, "Key": source_key})
    lake.put_bytes(
        target + ".reason.json",
        json.dumps({"source": lake.uri(source_key), "reason": reason, "run_id": run.run_id}, indent=2).encode(),
        "application/json",
    )
    log.error(
        "file_quarantined_notify_support",
        extra={"source_file": lake.uri(source_key), "quarantined_to": lake.uri(target), "reason": reason},
    )
    return target
