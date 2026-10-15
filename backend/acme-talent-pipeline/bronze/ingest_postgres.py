"""
Ingest the PostgreSQL HR tables into Bronze.

Layout (immutable snapshots, never overwritten):
    bronze/hr/<table>/ingest_date=YYYY-MM-DD/batch_id=<content-hash>/part-0.parquet

* Source column types are preserved exactly (no cleaning in Bronze).
* batch_id is a hash of the table content, so re-running against unchanged
  source data finds the existing snapshot and skips it (idempotent), while a
  changed source produces a new snapshot (history kept, nothing overwritten).
* Lineage columns are appended to every row:
    _source_system, _source_table, _batch_id, _ingested_at, _run_id
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime

import pyarrow as pa

from common.lineage import RunContext
from common.logger import get_logger
from common.postgres import Postgres
from common.storage import DataLake
from contracts.hr import HR_TABLES

log = get_logger("bronze.hr")

BRONZE_HR_PREFIX = "bronze/hr"


def content_hash(table: pa.Table) -> str:
    """Deterministic short hash of a table's rows (order-sensitive)."""
    digest = hashlib.sha256()
    digest.update(json.dumps(table.schema.names).encode())
    for row in table.to_pylist():
        digest.update(json.dumps(row, default=str, sort_keys=True).encode())
    return digest.hexdigest()[:16]


def add_lineage(table: pa.Table, source_table: str, batch_id: str, ingested_at: datetime, run_id: str) -> pa.Table:
    """Append Bronze lineage columns to a source table."""
    n = table.num_rows
    return (
        table.append_column("_source_system", pa.array(["postgresql"] * n, pa.string()))
        .append_column("_source_table", pa.array([source_table] * n, pa.string()))
        .append_column("_batch_id", pa.array([batch_id] * n, pa.string()))
        .append_column("_ingested_at", pa.array([ingested_at] * n, pa.timestamp("s", tz="UTC")))
        .append_column("_run_id", pa.array([run_id] * n, pa.string()))
    )


def ingest_postgres_tables(pg: Postgres, lake: DataLake, run: RunContext) -> None:
    """Snapshot every HR table from PostgreSQL into Bronze."""
    for table_name, (_, pk) in HR_TABLES.items():
        target = f"{BRONZE_HR_PREFIX}/{table_name}"
        with run.step(f"bronze.{table_name}", source=f"postgresql://{table_name}", target=lake.uri(target), critical=True) as m:
            data = pg.read_table(table_name, order_by=pk)
            m.rows_read = data.num_rows
            batch_id = content_hash(data)
            m.notes["batch_id"] = batch_id

            existing = [o.key for o in lake.list_objects(target + "/") if f"/batch_id={batch_id}/" in o.key]
            if existing:
                log.info("bronze_snapshot_unchanged_skip", extra={"table": table_name, "batch_id": batch_id})
                m.status = "skipped"
                m.notes["existing_snapshot"] = existing[0].rsplit("/", 1)[0]
                continue

            ingested_at = run.started_at
            prefix = f"{target}/ingest_date={ingested_at.date().isoformat()}/batch_id={batch_id}"
            lake.write_immutable(add_lineage(data, table_name, batch_id, ingested_at, run.run_id), prefix)
            m.rows_written = data.num_rows
            if m.rows_written != m.rows_read:
                raise RuntimeError(f"bronze.{table_name}: read {m.rows_read} rows but wrote {m.rows_written}")
