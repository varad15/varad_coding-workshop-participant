"""
Ingest learning-activity JSONL files from the S3 landing zone into Bronze.

Layout (immutable, same Hive partitions as the source, one folder per source file):
    bronze/learning_activity/year=YYYY/month=MM/day=DD/<source-file-stem>/part-0.parquet

* Every line is stored as raw text (`raw_line`), so even truncated or
  malformed JSON is preserved for Silver to diagnose. Nothing is parsed or
  dropped here.
* Incremental, per source file: a landing file already present in Bronze is
  not re-ingested; new files (including a second file for the same day, e.g.
  events-001.jsonl) are picked up on the next run. Existing Bronze data is
  never overwritten (immutable raw zone).
* Lineage columns on every row:
    _source_file (s3 URI), _source_size_bytes, _line_number, _batch_id,
    _ingested_at, _run_id
"""

from __future__ import annotations

import re
from concurrent.futures import ThreadPoolExecutor

import pyarrow as pa

from common.lineage import RunContext
from common.logger import get_logger
from common.storage import DataLake, ObjectInfo
from seed.load_sources import LANDING_EVENTS_PREFIX

log = get_logger("bronze.events")

BRONZE_EVENTS_PREFIX = "bronze/learning_activity"
_PARTITION_RE = re.compile(r"year=(\d{4})/month=(\d{2})/day=(\d{2})/")

RAW_SCHEMA = pa.schema(
    [
        ("raw_line", pa.string()),
        ("_line_number", pa.int32()),
        ("_source_file", pa.string()),
        ("_source_size_bytes", pa.int64()),
        ("_batch_id", pa.string()),
        ("_ingested_at", pa.timestamp("s", tz="UTC")),
        ("_run_id", pa.string()),
    ]
)


def partition_of(key: str) -> str | None:
    """Return 'year=YYYY/month=MM/day=DD' for a landing key, or None."""
    match = _PARTITION_RE.search(key)
    return f"year={match.group(1)}/month={match.group(2)}/day={match.group(3)}" if match else None


def bronze_key_of(landing_key: str) -> str | None:
    """Return the Bronze folder for a landing file: '<partition>/<file stem>'."""
    partition = partition_of(landing_key)
    if partition is None:
        return None
    stem = landing_key.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    return f"{partition}/{stem}"


def _ingested_files(bronze_keys: list[str]) -> set[str]:
    """Return '<partition>/<file stem>' for every source file already in Bronze."""
    done = set()
    for key in bronze_keys:
        match = _PARTITION_RE.search(key)
        if match:
            rest = key[match.end():].split("/", 1)[0]
            done.add(f"{match.group(0)}{rest}")
    return done


def lines_to_table(raw: bytes, source_uri: str, size: int, run: RunContext) -> pa.Table:
    """Split a JSONL file into one raw row per non-empty line (line numbers kept)."""
    text = raw.decode("utf-8", errors="replace")
    rows = [(i, line) for i, line in enumerate(text.splitlines(), start=1) if line.strip()]
    n = len(rows)
    return pa.Table.from_arrays(
        [
            pa.array([line for _, line in rows], pa.string()),
            pa.array([i for i, _ in rows], pa.int32()),
            pa.array([source_uri] * n, pa.string()),
            pa.array([size] * n, pa.int64()),
            pa.array([run.run_id] * n, pa.string()),
            pa.array([run.started_at] * n, pa.timestamp("s", tz="UTC")),
            pa.array([run.run_id] * n, pa.string()),
        ],
        schema=RAW_SCHEMA,
    )


def ingest_learning_events(lake: DataLake, run: RunContext) -> None:
    """Copy new landing JSONL day-partitions into Bronze."""
    with run.step(
        "bronze.learning_activity",
        source=lake.uri(LANDING_EVENTS_PREFIX),
        target=lake.uri(BRONZE_EVENTS_PREFIX),
    ) as m:
        landing = [o for o in lake.list_objects(LANDING_EVENTS_PREFIX) if o.key.endswith(".jsonl")]
        ingested = _ingested_files([o.key for o in lake.list_objects(BRONZE_EVENTS_PREFIX + "/")])
        bad_paths = [o.key for o in landing if bronze_key_of(o.key) is None]
        if bad_paths:
            log.warning("landing_files_without_partition_ignored", extra={"files": bad_paths[:10]})

        todo = [o for o in landing if (b := bronze_key_of(o.key)) and b not in ingested]
        m.notes.update(landing_files=len(landing), already_ingested=len(landing) - len(todo) - len(bad_paths))

        def _ingest(obj: ObjectInfo) -> int:
            table = lines_to_table(lake.get_bytes(obj.key), lake.uri(obj.key), obj.size, run)
            lake.write_immutable(table, f"{BRONZE_EVENTS_PREFIX}/{bronze_key_of(obj.key)}")
            return table.num_rows

        with ThreadPoolExecutor(max_workers=16) as pool:
            counts = list(pool.map(_ingest, todo))

        m.rows_read = m.rows_written = sum(counts)
        m.notes["files_ingested"] = len(todo)
        if not todo:
            log.info("no_new_landing_files", extra={"landing_files": len(landing)})
            m.status = "skipped"
