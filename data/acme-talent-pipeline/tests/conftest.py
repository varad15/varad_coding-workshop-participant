"""
Shared test fixtures.

* `spark`        - one local SparkSession for the whole test session.
* `s3_endpoint`  - an S3 endpoint: TEST_S3_ENDPOINT if set (e.g. LocalStack on
                   the VDI), otherwise an in-process moto server.
* `pg_env`       - connection env for a throwaway PostgreSQL database
                   (acme_test); integration tests are skipped if PostgreSQL
                   is not reachable.
* `pipeline_env` - full environment for running job.py against both.
"""

from __future__ import annotations

import os
import socket
import sys
import uuid
from pathlib import Path
from typing import Iterator

import pytest

PIPELINE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIPELINE_DIR))

os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")
os.environ.setdefault("AWS_REGION", "us-east-1")


@pytest.fixture(scope="session")
def spark():
    """Local SparkSession shared by all tests."""
    from pyspark.sql import SparkSession

    os.environ.pop("SPARK_HOME", None)
    session = (
        SparkSession.builder.master("local[2]")
        .appName("acme-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def s3_endpoint() -> Iterator[str]:
    """Return an S3 endpoint URL (LocalStack if configured, else moto)."""
    configured = os.environ.get("TEST_S3_ENDPOINT")
    if configured:
        yield configured
        return
    from moto.server import ThreadedMotoServer

    port = _free_port()
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=port)
    server.start()
    yield f"http://127.0.0.1:{port}"
    server.stop()


def _pg_base() -> dict[str, str]:
    return {
        "POSTGRES_HOST": os.environ.get("TEST_POSTGRES_HOST", "localhost"),
        "POSTGRES_PORT": os.environ.get("TEST_POSTGRES_PORT", "5432"),
        "POSTGRES_USER": os.environ.get("TEST_POSTGRES_USER", "postgres"),
        "POSTGRES_PASS": os.environ.get("TEST_POSTGRES_PASS", "postgres123"),
    }


@pytest.fixture(scope="session")
def pg_env() -> Iterator[dict[str, str]]:
    """Create (and later drop) a dedicated test database."""
    import psycopg

    base = _pg_base()
    admin = (
        f"host={base['POSTGRES_HOST']} port={base['POSTGRES_PORT']} user={base['POSTGRES_USER']} "
        f"password={base['POSTGRES_PASS']} dbname=postgres connect_timeout=3"
    )
    try:
        conn = psycopg.connect(admin, autocommit=True)
    except psycopg.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    db = "acme_test"
    conn.execute(f"DROP DATABASE IF EXISTS {db}")
    conn.execute(f"CREATE DATABASE {db}")
    yield {**base, "POSTGRES_NAME": db}
    conn.execute(f"DROP DATABASE IF EXISTS {db} WITH (FORCE)")
    conn.close()


@pytest.fixture
def lake_env(s3_endpoint: str) -> dict[str, str]:
    """Environment pointing at a fresh, uniquely named bucket."""
    return {
        "IS_LOCAL": "true",
        "AWS_ENDPOINT_URL_S3": s3_endpoint,
        "DATALAKE_BUCKET": f"coding-workshop-datalake-test-{uuid.uuid4().hex[:8]}",
        "RETRY_ATTEMPTS": "2",
    }


@pytest.fixture
def lake(lake_env: dict[str, str]):
    """A DataLake bound to a fresh bucket."""
    from common.config import load_settings
    from common.storage import DataLake

    dl = DataLake(load_settings({**os.environ, **lake_env}))
    dl.ensure_bucket()
    return dl


@pytest.fixture
def run_ctx():
    """A RunContext for step tracking in tests."""
    from common.lineage import RunContext

    return RunContext.start()
