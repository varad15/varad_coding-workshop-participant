"""Unit tests: configuration, retry/backoff and lineage tracking."""

from __future__ import annotations

import json

import pytest

from common.config import load_settings
from common.lineage import RunContext, ZeroRowsError, new_run_id
from common.retry import backoff_delays, with_retry


# ----------------------------------------------------------------- config
def test_local_defaults_match_workshop_repo():
    s = load_settings({})
    assert s.is_local is True
    assert (s.postgres.host, s.postgres.port, s.postgres.user, s.postgres.dbname) == ("localhost", 5432, "postgres", "postgres")
    assert s.postgres.sslmode == "disable"
    assert s.s3.endpoint_url == "http://s3.localhost.localstack.cloud:4566"
    assert s.s3.bucket == "coding-workshop-datalake-abcd1234"


def test_cloud_mode_requires_ssl_and_uses_real_s3():
    s = load_settings({"IS_LOCAL": "false", "POSTGRES_HOST": "aurora.example", "APP_ID": "xyz123"})
    assert s.postgres.sslmode == "require"
    assert s.s3.endpoint_url is None
    assert s.s3.bucket == "coding-workshop-datalake-xyz123"


def test_secrets_never_in_describe():
    s = load_settings({"POSTGRES_PASS": "s3cret"})
    assert "s3cret" not in json.dumps(s.describe())
    assert "s3cret" not in repr(s.postgres)


# ------------------------------------------------------------------ retry
def test_backoff_is_exponential_and_capped():
    assert backoff_delays(5, base=1.0, cap=5.0) == [1.0, 2.0, 4.0, 5.0]


def test_retry_recovers_from_transient_failures():
    calls = {"n": 0}

    @with_retry((ConnectionError,), attempts=4, sleep=lambda _: None)
    def flaky() -> str:
        calls["n"] += 1
        if calls["n"] < 3:
            raise ConnectionError("transient")
        return "ok"

    assert flaky() == "ok"
    assert calls["n"] == 3


def test_retry_gives_up_after_max_attempts():
    calls = {"n": 0}

    @with_retry((ConnectionError,), attempts=3, sleep=lambda _: None)
    def always_down() -> None:
        calls["n"] += 1
        raise ConnectionError("down")

    with pytest.raises(ConnectionError):
        always_down()
    assert calls["n"] == 3


def test_non_transient_errors_are_not_retried():
    calls = {"n": 0}

    @with_retry((ConnectionError,), attempts=5, sleep=lambda _: None)
    def bad_input() -> None:
        calls["n"] += 1
        raise ValueError("not transient")

    with pytest.raises(ValueError):
        bad_input()
    assert calls["n"] == 1


# ---------------------------------------------------------------- lineage
def test_step_records_counts_timing_and_status():
    run = RunContext.start()
    with run.step("bronze.x", source="a", target="b") as m:
        m.rows_read = m.rows_written = 10
    step = run.steps[0]
    assert step.status == "succeeded" and step.rows_written == 10
    assert step.started_at and step.ended_at and step.duration_s >= 0


def test_failed_step_is_marked_and_reraised():
    run = RunContext.start()
    with pytest.raises(RuntimeError):
        with run.step("silver.x", source="a", target="b"):
            raise RuntimeError("boom")
    assert run.steps[0].status == "failed"
    assert "boom" in run.steps[0].error


def test_zero_rows_fails_critical_step_only():
    run = RunContext.start()
    with run.step("optional", source="a", target="b"):
        pass  # zero rows -> warning only
    assert run.steps[0].status == "succeeded"
    with pytest.raises(ZeroRowsError):
        with run.step("critical", source="a", target="b", critical=True):
            pass


def test_skipped_step_with_zero_rows_is_not_an_error():
    run = RunContext.start()
    with run.step("bronze.x", source="a", target="b", critical=True) as m:
        m.status = "skipped"
    assert run.steps[0].status == "skipped"


def test_run_record_serialises():
    run = RunContext.start()
    with run.step("s", source="a", target="b") as m:
        m.rows_written = 1
    run.finish("succeeded")
    record = json.loads(run.to_json())
    assert record["status"] == "succeeded" and record["steps"][0]["step"] == "s"
    assert run.steps_table().num_rows == 1


def test_run_ids_are_unique_and_sortable():
    ids = {new_run_id() for _ in range(50)}
    assert len(ids) == 50
    assert all(len(i.split("-")[0]) == 16 for i in ids)
