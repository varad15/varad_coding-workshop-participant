"""Integration tests: real read/write behaviour against S3 and PostgreSQL."""

from __future__ import annotations

import os
from datetime import date
from decimal import Decimal

import psycopg
import pyarrow as pa
import pytest

from common.config import load_settings
from common.postgres import Postgres

pytestmark = pytest.mark.integration


# --------------------------------------------------------------------- S3
def test_write_and_read_parquet_round_trip(lake):
    table = pa.table({"id": [1, 2], "name": ["a", "b"]})
    assert lake.write_table(table, "silver/t") == 2
    assert lake.read_table("silver/t").to_pylist() == table.to_pylist()


def test_unpartitioned_write_replaces_previous_data(lake):
    lake.write_table(pa.table({"id": [1, 2, 3]}), "gold/t")
    lake.write_table(pa.table({"id": [9]}), "gold/t")  # rerun with new content
    assert lake.read_table("gold/t").column("id").to_pylist() == [9]


def test_partitioned_write_replaces_only_matching_partitions(lake):
    lake.write_table(pa.table({"v": [1, 2], "year": ["2024", "2025"]}), "silver/p", partition_cols=["year"])
    lake.write_table(pa.table({"v": [3], "year": ["2025"]}), "silver/p", partition_cols=["year"])
    rows = sorted(lake.read_table("silver/p").select(["v"]).column("v").to_pylist())
    assert rows == [1, 3]  # 2024 untouched, 2025 replaced (no duplicates)


def test_immutable_write_never_overwrites(lake):
    assert lake.write_immutable(pa.table({"v": [1]}), "bronze/x/batch_id=1") is True
    assert lake.write_immutable(pa.table({"v": [2]}), "bronze/x/batch_id=1") is False
    assert lake.read_table("bronze/x").column("v").to_pylist() == [1]


def test_upload_skips_identical_size_and_verifies(lake, tmp_path):
    f = tmp_path / "a.jsonl"
    f.write_text('{"a": 1}\n')
    assert lake.upload_file(f, "landing/a.jsonl") is True
    existing = {o.key: o.size for o in lake.list_objects("landing/")}
    assert lake.upload_file(f, "landing/a.jsonl", existing) is False
    assert existing["landing/a.jsonl"] == f.stat().st_size


def test_delete_prefix(lake):
    lake.put_bytes("tmp/a", b"1")
    lake.put_bytes("tmp/b", b"2")
    assert lake.delete_prefix("tmp/") == 2
    assert not lake.exists("tmp/")


def test_missing_bucket_is_fatal_in_cloud_mode(lake_env):
    from common.storage import DataLake

    cloud = DataLake(load_settings({**os.environ, **lake_env, "IS_LOCAL": "false", "DATALAKE_BUCKET": "coding-workshop-datalake-missing"}))
    with pytest.raises(RuntimeError, match="does not exist"):
        cloud.ensure_bucket()


# ------------------------------------------------------------- PostgreSQL
def test_postgres_read_preserves_source_types(pg_env):
    settings = load_settings({**os.environ, **pg_env})
    with psycopg.connect(settings.postgres.conninfo(), autocommit=True) as conn:
        conn.execute("DROP TABLE IF EXISTS type_probe")
        conn.execute("CREATE TABLE type_probe (id INTEGER PRIMARY KEY, score NUMERIC(5,2), d DATE, note TEXT)")
        conn.execute("INSERT INTO type_probe VALUES (2, 41.93, '2025-01-02', NULL), (1, 56.87, '2025-01-01', 'x')")
    table = Postgres(settings).read_table("type_probe", order_by="id")
    assert table.schema.field("id").type == pa.int32()
    assert table.schema.field("score").type == pa.decimal128(5, 2)
    assert table.schema.field("d").type == pa.date32()
    assert table.to_pylist() == [
        {"id": 1, "score": Decimal("56.87"), "d": date(2025, 1, 1), "note": "x"},
        {"id": 2, "score": Decimal("41.93"), "d": date(2025, 1, 2), "note": None},
    ]


def test_postgres_connection_failure_is_retried_then_raised():
    settings = load_settings({"POSTGRES_HOST": "127.0.0.1", "POSTGRES_PORT": "1", "RETRY_ATTEMPTS": "2"})
    with pytest.raises(psycopg.OperationalError):
        Postgres(settings).row_count("teams")
