"""
Bronze -> Silver for learning-activity events.

Rules are applied in order; the first rule a row fails decides its reason:

  1. Malformed / truncated JSON line      -> quarantine the row        (section 7: malformed row)
  2. Schema drift (missing / extra keys)  -> quarantine the WHOLE file  (section 7: schema drift)
                                            + copy file to quarantine/files/, ERROR log (notify support)
  3. Contract violation (contracts/learning.py), e.g. null employee_id,
     unparseable event_date               -> quarantine the row
  4. Exact duplicate event                -> dropped (first occurrence kept), counted
  5. employee_id not in Silver employees  -> quarantine the row (referential integrity)

Silver output: silver/learning_events/event_year=YYYY/event_month=MM/

Source -> target mapping:
  raw_line.event_id     -> event_id     (string)
  raw_line.employee_id  -> employee_id  (string -> int)
  raw_line.event_date   -> event_ts     (ISO string -> timestamp, UTC) and event_date (date)
  raw_line.event_type   -> event_type
  raw_line.course_name  -> course_name
  raw_line.text         -> event_text   (renamed: 'text' is too generic)
  raw_line.score        -> score        (int, quiz events only)
  partition year/month/day of source file -> _source_partition_date
"""

from __future__ import annotations

import json
from typing import Iterator

import pyarrow as pa
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from bronze.ingest_events import BRONZE_EVENTS_PREFIX
from common.lineage import RunContext
from common.logger import get_logger
from common.spark import to_arrow, to_spark
from common.storage import DataLake
from contracts.learning import EXPECTED_EVENT_KEYS, LearningEvent
from contracts.validation import REASON_COL, VALID_COL, validate_dataframe
from silver.checks import assert_silver_invariants
from silver.quarantine import quarantine_file, to_quarantine_rows, write_quarantine

log = get_logger("silver.events")

SILVER_EVENTS_PREFIX = "silver/learning_events"
ENTITY = "learning_events"
FIELDS = list(LearningEvent.model_fields.keys())
_PARSED_COLS = ["_parse_error", "_keys", *FIELDS]


def _parse_lines(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
    """Parse raw JSON lines into string columns; record parse errors and key sets."""
    for batch in batches:
        raw = batch.column(batch.schema.get_field_index("raw_line")).to_pylist()
        out: dict[str, list[str | None]] = {c: [] for c in _PARSED_COLS}
        for line in raw:
            try:
                record = json.loads(line)
                if not isinstance(record, dict):
                    raise ValueError("JSON value is not an object")
                out["_parse_error"].append(None)
                out["_keys"].append(",".join(sorted(record.keys())))
                for f in FIELDS:
                    value = record.get(f)
                    out[f].append(None if value is None else (value if isinstance(value, str) else json.dumps(value)))
            except ValueError as exc:  # json.JSONDecodeError is a ValueError
                out["_parse_error"].append(f"malformed JSON: {exc}")
                out["_keys"].append(None)
                for f in FIELDS:
                    out[f].append(None)
        arrays = [batch.column(i) for i in range(batch.num_columns)]
        arrays += [pa.array(out[c], pa.string()) for c in _PARSED_COLS]
        yield pa.RecordBatch.from_arrays(arrays, names=batch.schema.names + _PARSED_COLS)


def parse_raw(bronze: DataFrame) -> DataFrame:
    """Add parsed string columns (+ _parse_error, _keys) to Bronze raw lines."""
    schema = StructType(bronze.schema.fields + [StructField(c, StringType(), True) for c in _PARSED_COLS])
    return bronze.mapInArrow(_parse_lines, schema)


def drift_reason(keys_csv: str) -> str | None:
    """Describe schema drift for a record's key set, or None if it matches the contract."""
    keys = set(keys_csv.split(",")) if keys_csv else set()
    extra, missing = sorted(keys - EXPECTED_EVENT_KEYS), sorted(EXPECTED_EVENT_KEYS - keys)
    if not extra and not missing:
        return None
    return f"schema drift: unexpected keys {extra}, missing keys {missing}"


def clean_learning_events(spark: SparkSession, lake: DataLake, run: RunContext) -> dict[str, object]:
    """Run Bronze -> Silver for learning events; return DQ stats."""
    with run.step(
        "silver.learning_events",
        source=lake.uri(BRONZE_EVENTS_PREFIX),
        target=lake.uri(SILVER_EVENTS_PREFIX),
        critical=True,
    ) as m:
        bronze = to_spark(spark, lake.read_table(BRONZE_EVENTS_PREFIX))
        bronze = bronze.withColumn(
            "_source_partition_date",
            F.to_date(F.format_string("%04d-%02d-%02d", F.col("year").cast("int"), F.col("month").cast("int"), F.col("day").cast("int"))),
        ).drop("year", "month", "day")
        parsed = parse_raw(bronze).cache()
        m.rows_read = parsed.count()

        # 1. Malformed JSON rows.
        malformed = parsed.filter(F.col("_parse_error").isNotNull()).withColumn(REASON_COL, F.col("_parse_error"))
        ok = parsed.filter(F.col("_parse_error").isNull())

        # 2. Schema drift -> whole file.
        drift_udf = F.udf(drift_reason, StringType())
        ok = ok.withColumn("_drift", drift_udf("_keys"))
        drift_files = {
            r["_source_file"]: r["_drift"]
            for r in ok.filter(F.col("_drift").isNotNull()).groupBy("_source_file").agg(F.first("_drift").alias("_drift")).collect()
        }
        for source_uri, reason in drift_files.items():
            quarantine_file(lake, source_uri.split(f"{lake.bucket}/", 1)[1], f"{reason} (entire file quarantined)", run)
        in_drift_file = F.col("_source_file").isin(list(drift_files)) if drift_files else F.lit(False)
        drifted = ok.filter(in_drift_file).withColumn(
            REASON_COL, F.concat(F.lit("file quarantined for schema drift: "), F.coalesce(F.col("_drift"), F.lit("other records in file drifted")))
        )
        ok = ok.filter(~in_drift_file)

        # 3. Contract validation (Pydantic, distributed via mapInArrow).
        checked = validate_dataframe(ok, LearningEvent, FIELDS)
        invalid = checked.filter(~F.col(VALID_COL)).drop(VALID_COL)
        valid = checked.filter(F.col(VALID_COL)).drop(VALID_COL, REASON_COL)

        # Cast to conformed types (safe: every row passed the contract).
        typed = valid.select(
            F.col("event_id"),
            F.col("employee_id").cast("int").alias("employee_id"),
            F.to_timestamp("event_date").alias("event_ts"),
            F.to_date(F.to_timestamp("event_date")).alias("event_date"),
            F.col("event_type"),
            F.col("course_name"),
            F.col("text").alias("event_text"),
            F.col("score").cast("int").alias("score"),
            "_source_partition_date",
            "_source_file",
            "_line_number",
            "_batch_id",
            "_ingested_at",
            "raw_line",
        )

        # 4. Exact duplicates: keep the first occurrence by source position.
        content = ["event_id", "employee_id", "event_ts", "event_type", "course_name", "event_text", "score"]
        first = Window.partitionBy(*content).orderBy("_source_file", "_line_number")
        ranked = typed.withColumn("_rn", F.row_number().over(first))
        duplicates = ranked.filter(F.col("_rn") > 1).count()
        deduped = ranked.filter(F.col("_rn") == 1).drop("_rn")
        # Same event_id with different content is a conflict, not a duplicate.
        per_id = Window.partitionBy("event_id")
        deduped = deduped.withColumn("_id_count", F.count(F.lit(1)).over(per_id))
        conflicting = deduped.filter(F.col("_id_count") > 1).withColumn(
            REASON_COL, F.lit("duplicate event_id with conflicting values")
        ).drop("_id_count")
        deduped = deduped.filter(F.col("_id_count") == 1).drop("_id_count")

        # 5. Referential integrity against Silver employees.
        employees = to_spark(spark, lake.read_table("silver/hr/employees")).select(F.col("employee_id").alias("_emp"))
        orphans = deduped.join(employees, deduped["employee_id"] == employees["_emp"], "left_anti").withColumn(
            REASON_COL, F.concat(F.lit("foreign key employee_id="), F.col("employee_id").cast("string"), F.lit(" not found in employees"))
        )
        clean = deduped.join(employees, deduped["employee_id"] == employees["_emp"], "left_semi").cache()

        rejects = (
            to_quarantine_rows(malformed, ENTITY, REASON_COL, run, record_col="raw_line")
            .unionByName(to_quarantine_rows(drifted, ENTITY, REASON_COL, run, record_col="raw_line"))
            .unionByName(to_quarantine_rows(invalid, ENTITY, REASON_COL, run, record_col="raw_line"))
            .unionByName(to_quarantine_rows(conflicting, ENTITY, REASON_COL, run, record_col="raw_line"))
            .unionByName(to_quarantine_rows(orphans, ENTITY, REASON_COL, run, record_col="raw_line"))
        ).cache()
        m.rows_rejected = write_quarantine(lake, ENTITY, rejects)
        m.rows_duplicate = duplicates

        output = clean.drop("raw_line").withColumn("event_year", F.year("event_date")).withColumn(
            "event_month", F.format_string("%02d", F.month("event_date"))
        )
        assert_silver_invariants(output, ENTITY, "event_id", ["employee_id", "event_ts", "event_date", "event_type", "course_name"])

        mismatched_partitions = output.filter(F.col("event_date") != F.col("_source_partition_date")).count()
        if mismatched_partitions:
            log.warning("event_date_differs_from_source_partition", extra={"rows": mismatched_partitions})

        m.rows_valid = output.count()
        lake.delete_prefix(SILVER_EVENTS_PREFIX + "/")  # full rebuild from Bronze -> idempotent
        m.rows_written = lake.write_table(to_arrow(output), SILVER_EVENTS_PREFIX, partition_cols=["event_year", "event_month"])

        reasons = {}
        for r in rejects.withColumn("reason_group", F.regexp_replace("reason", r"(: line \d+ column \d+.*|=\d+ not found)", "")).groupBy("reason_group").count().collect():
            reasons[r["reason_group"]] = r["count"]
        m.notes.update(drift_files=list(drift_files), partition_date_mismatches=mismatched_partitions)
        return {
            "bronze_rows": m.rows_read,
            "silver_rows": m.rows_written,
            "quarantined_rows": m.rows_rejected,
            "duplicates_dropped": duplicates,
            "rejection_reasons": reasons,
            "quarantined_files": list(drift_files),
        }
