"""Unit tests: Bronze helpers and Silver cleaning rules on small hand-made data."""

from __future__ import annotations

from datetime import date

import pyarrow as pa
import pytest
from pyspark.sql import functions as F

from bronze.ingest_events import _ingested_files, bronze_key_of, lines_to_table, partition_of
from bronze.ingest_postgres import add_lineage, content_hash
from common.lineage import RunContext
from contracts.validation import REASON_COL
from silver.checks import SilverCheckError, assert_silver_invariants
from silver.clean_events import drift_reason, parse_raw
from silver.clean_hr import split_duplicates, split_orphans


# ------------------------------------------------------------------ bronze
def test_partition_of_extracts_hive_path():
    assert partition_of("landing/learning_activity/year=2025/month=01/day=10/events-000.jsonl") == "year=2025/month=01/day=10"
    assert partition_of("landing/learning_activity/misc/file.jsonl") is None


def test_two_files_on_same_day_are_tracked_separately():
    base = "landing/learning_activity/year=2025/month=01/day=10/"
    assert bronze_key_of(base + "events-000.jsonl") == "year=2025/month=01/day=10/events-000"
    assert bronze_key_of(base + "events-001.jsonl") == "year=2025/month=01/day=10/events-001"
    done = _ingested_files(["bronze/learning_activity/year=2025/month=01/day=10/events-000/part-0.parquet"])
    assert done == {"year=2025/month=01/day=10/events-000"}  # events-001 still to ingest


def test_lines_to_table_keeps_raw_text_and_line_numbers():
    run = RunContext.start()
    raw = b'{"a": 1}\n\n{"broken": \n{"b": 2}\n'
    t = lines_to_table(raw, "s3://b/k", 30, run)
    assert t.column("raw_line").to_pylist() == ['{"a": 1}', '{"broken": ', '{"b": 2}']  # malformed kept
    assert t.column("_line_number").to_pylist() == [1, 3, 4]  # blank line 2 skipped, numbering preserved
    assert set(t.column("_source_file").to_pylist()) == {"s3://b/k"}


def test_content_hash_is_deterministic_and_content_sensitive():
    a = pa.table({"id": [1, 2], "v": ["x", "y"]})
    assert content_hash(a) == content_hash(pa.table({"id": [1, 2], "v": ["x", "y"]}))
    assert content_hash(a) != content_hash(pa.table({"id": [1, 2], "v": ["x", "z"]}))


def test_add_lineage_appends_metadata_columns():
    run = RunContext.start()
    t = add_lineage(pa.table({"id": [1]}), "teams", "abc", run.started_at, run.run_id)
    assert t.column_names == ["id", "_source_system", "_source_table", "_batch_id", "_ingested_at", "_run_id"]


# ------------------------------------------------------------- silver: events
def test_parse_raw_flags_malformed_json(spark):
    df = spark.createDataFrame([('{"event_id": "EV0000000001", "score": 5}',), ('{"event_id": "EV00',)], "raw_line string")
    rows = {r["raw_line"]: r for r in parse_raw(df).collect()}
    good = rows['{"event_id": "EV0000000001", "score": 5}']
    assert good["_parse_error"] is None and good["event_id"] == "EV0000000001" and good["score"] == "5"
    assert rows['{"event_id": "EV00']["_parse_error"].startswith("malformed JSON")


def test_drift_reason_detects_renamed_field():
    keys = "course_name,employee_id,event_date,event_id,event_type,notes,score"
    reason = drift_reason(keys)
    assert "unexpected keys ['notes']" in reason and "missing keys ['text']" in reason
    assert drift_reason("course_name,employee_id,event_date,event_id,event_type,score,text") is None


# --------------------------------------------------------------- silver: hr
def test_split_duplicates_drops_exact_and_quarantines_conflicts(spark):
    df = spark.createDataFrame(
        [(1, "a"), (1, "a"), (2, "b"), (2, "c"), (3, "d")], "id int, v string"
    )
    clean, conflicting, exact = split_duplicates(df, "id", ["id", "v"])
    assert exact == 1
    assert sorted(r["id"] for r in clean.collect()) == [1, 3]
    assert sorted(r["v"] for r in conflicting.collect()) == ["b", "c"]
    assert all("conflicting" in r[REASON_COL] for r in conflicting.collect())


def test_split_orphans_rejects_missing_foreign_keys(spark):
    teams = spark.createDataFrame([(1,), (2,)], "team_id int")
    employees = spark.createDataFrame([(10, 1), (11, 99)], "employee_id int, team_id int")
    clean, orphans = split_orphans(employees, "employees", {"teams": teams})
    assert [r["employee_id"] for r in clean.collect()] == [10]
    reason = orphans.collect()[0][REASON_COL]
    assert reason == "foreign key team_id=99 not found in teams"


def test_silver_invariants(spark):
    ok = spark.createDataFrame([(1, date(2025, 1, 1)), (2, date(2025, 1, 2))], "id int, hire_date date")
    assert_silver_invariants(ok, "t", "id", ["hire_date"])
    with pytest.raises(SilverCheckError, match="duplicate"):
        assert_silver_invariants(ok.unionByName(ok), "t", "id", ["hire_date"])
    with pytest.raises(SilverCheckError, match="nulls"):
        assert_silver_invariants(ok.withColumn("hire_date", F.lit(None).cast("date")), "t", "id", ["hire_date"])
