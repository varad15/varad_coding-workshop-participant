"""
Bootstrap the two source systems from the sample dataset.

This simulates the upstream systems; it is not part of the medallion flow.

1. publish_landing(): uploads the dataset to S3
     landing/learning_activity/year=/month=/day=/*.jsonl  <- S3 source system
     landing/postgres_seed/*.csv + schema.sql             <- seed files for PostgreSQL
   Runs wherever backend/_source exists (VDI). Skips files already uploaded
   with the same size, then verifies every S3 object size equals the source
   file size (data-engineer.md section 6, Bronze checklist).

2. seed_postgres(): loads the CSVs from S3 into PostgreSQL (local or Aurora).
   Reading from S3 means the same code works inside the EKS pod, which can
   reach Aurora inside the VPC. Idempotent: if every table already holds the
   expected row count nothing is touched; otherwise all tables are truncated
   and reloaded in one transaction (schema.sql load order).
"""

from __future__ import annotations

import csv
import io
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from psycopg import sql

from common.lineage import RunContext
from common.logger import get_logger
from common.postgres import Postgres
from common.storage import DataLake
from contracts.hr import HR_TABLES

log = get_logger("seed")

LANDING_EVENTS_PREFIX = "landing/learning_activity/"
LANDING_SEED_PREFIX = "landing/postgres_seed/"


class SizeMismatchError(RuntimeError):
    """An uploaded object's size differs from its source file."""


def _local_files(source_dir: Path) -> dict[str, Path]:
    """Map destination S3 key -> local file for everything we publish."""
    files: dict[str, Path] = {}
    for path in sorted((source_dir / "learning_activity").rglob("*.jsonl")):
        rel = path.relative_to(source_dir / "learning_activity").as_posix()
        files[LANDING_EVENTS_PREFIX + rel] = path
    for name in [*HR_TABLES.keys()]:
        files[f"{LANDING_SEED_PREFIX}{name}.csv"] = source_dir / f"{name}.csv"
    files[f"{LANDING_SEED_PREFIX}schema.sql"] = source_dir / "schema.sql"
    return files


def publish_landing(lake: DataLake, source_dir: Path, run: RunContext) -> None:
    """Upload the sample dataset to the S3 landing zone and verify sizes."""
    files = _local_files(source_dir)
    with run.step("landing.publish", source=str(source_dir), target=lake.uri("landing/")) as m:
        existing = {o.key: o.size for o in lake.list_objects("landing/")}
        m.rows_read = len(files)
        with ThreadPoolExecutor(max_workers=16) as pool:
            uploaded = list(pool.map(lambda kv: lake.upload_file(kv[1], kv[0], existing), files.items()))
        m.rows_written = sum(uploaded)
        m.notes["skipped_already_present"] = len(files) - m.rows_written

        # Verify: every landing object size equals its source file size.
        after = {o.key: o.size for o in lake.list_objects("landing/")}
        mismatches = [k for k, p in files.items() if after.get(k) != p.stat().st_size]
        m.notes["size_check"] = "passed" if not mismatches else f"failed: {mismatches[:5]}"
        if mismatches:
            raise SizeMismatchError(f"{len(mismatches)} landing objects differ from source: {mismatches[:5]}")
        if m.rows_written == 0:
            m.status = "skipped"


def _csv_rows(raw: bytes) -> int:
    """Count data rows in a CSV (header excluded, quoted newlines handled)."""
    return sum(1 for _ in csv.reader(io.StringIO(raw.decode("utf-8")))) - 1


def _existing_tables(cur) -> set[str]:
    """Return which HR tables already exist in the public schema."""
    cur.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = ANY(%s)",
        (list(HR_TABLES.keys()),),
    )
    return {r[0] for r in cur.fetchall()}


def seed_postgres(pg: Postgres, lake: DataLake, run: RunContext) -> None:
    """Load the HR CSVs from S3 into PostgreSQL, idempotently."""
    with run.step("seed.postgres", source=lake.uri(LANDING_SEED_PREFIX), target="postgresql://hr") as m:
        csv_bytes = {t: lake.get_bytes(f"{LANDING_SEED_PREFIX}{t}.csv") for t in HR_TABLES}
        expected = {t: _csv_rows(b) for t, b in csv_bytes.items()}
        m.rows_read = sum(expected.values())

        with pg.connect() as conn, conn.cursor() as cur:
            present = _existing_tables(cur)
            if not present:
                cur.execute(lake.get_bytes(f"{LANDING_SEED_PREFIX}schema.sql").decode())
                log.info("schema_created", extra={"tables": len(HR_TABLES)})
            elif present != set(HR_TABLES):
                raise RuntimeError(f"Partial HR schema found ({sorted(present)}); fix the database manually")

            actual = {}
            for t in HR_TABLES:
                cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(t)))
                actual[t] = cur.fetchone()[0]
            if actual == expected:
                log.info("seed_already_loaded_skip", extra={"tables": actual})
                m.status = "skipped"
                conn.rollback()
                return

            names = sql.SQL(", ").join(sql.Identifier(t) for t in HR_TABLES)
            cur.execute(sql.SQL("TRUNCATE {} CASCADE").format(names))
            for table in HR_TABLES:  # dict order == schema.sql load order (FK safe)
                copy_sql = sql.SQL("COPY {} FROM STDIN WITH (FORMAT csv, HEADER true)").format(sql.Identifier(table))
                with cur.copy(copy_sql) as copy:
                    copy.write(csv_bytes[table])
            conn.commit()

        loaded = {t: pg.row_count(t) for t in HR_TABLES}
        m.rows_written = sum(loaded.values())
        m.notes["tables"] = loaded
        if loaded != expected:
            raise RuntimeError(f"Row counts after load {loaded} != CSV counts {expected}")
