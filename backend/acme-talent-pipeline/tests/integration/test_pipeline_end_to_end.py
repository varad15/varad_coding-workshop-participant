"""
End-to-end integration, reconciliation and idempotency tests.

Runs the real job (python job.py, as Kubernetes would) against PostgreSQL and
S3 on the full sample dataset, twice, then checks:
  * source ingestion: Bronze row counts equal the source counts;
  * transformation + data quality: every known data issue is quarantined;
  * reconciliation: bronze == silver + quarantined + duplicates, per entity;
  * Gold: aggregates equal Silver figures;
  * idempotency: the second run changes no Silver/Gold/quarantine output.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from common.config import load_settings
from common.storage import DataLake

pytestmark = pytest.mark.integration

PIPELINE_DIR = Path(__file__).resolve().parents[2]
SOURCE_DIR = PIPELINE_DIR.parent / "_source"

# Known facts about the sample dataset (profiled independently of the pipeline).
SOURCE_COUNTS = {
    "teams": 100, "competencies": 40, "employees": 2000, "projects": 33,
    "performance_reviews": 4000, "competency_assessments": 4000, "talent_review": 2000,
}
RAW_EVENT_LINES = 29868
EXPECTED_QUARANTINE = {"malformed JSON": 13, "schema drift": 39, "employee_id": 76, "event_date": 54}
EXPECTED_DUPLICATES = 150
OUTPUTS = [
    "silver/hr/employees", "silver/learning_events", "quarantine/learning_events",
    "gold/employee_performance_history", "gold/employee_skill_gaps", "gold/promotion_pipeline",
    "gold/employee_course_progress", "gold/employee_learning_summary", "gold/attrition_risk",
    "gold/team_performance_outcomes", "gold/skill_distribution",
]


def _run_job(env: dict[str, str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "job.py"], cwd=PIPELINE_DIR, env=env, capture_output=True, text=True, timeout=1800
    )


def _steps(stdout: str) -> dict[str, dict]:
    steps = {}
    for line in stdout.splitlines():
        if line.startswith("{"):
            rec = json.loads(line)
            if rec["event"] == "step_finished":
                steps[rec["step"]] = rec
    return steps


def _fingerprint(lake: DataLake) -> dict[str, tuple[int, str]]:
    volatile = {"run_id", "quarantined_at", "_ingested_at", "_batch_id"}
    out = {}
    for prefix in OUTPUTS:
        df = lake.read_table(prefix).to_pandas()
        df = df.drop(columns=[c for c in df.columns if c in volatile]).astype(str)
        df = df.sort_values(list(df.columns)).reset_index(drop=True)
        out[prefix] = (len(df), hashlib.sha256(df.to_csv(index=False).encode()).hexdigest())
    return out


@pytest.fixture(scope="module")
def two_runs(s3_endpoint, pg_env, tmp_path_factory):
    """Run the whole pipeline twice on a fresh bucket + database."""
    env = {
        **os.environ,
        **pg_env,
        "IS_LOCAL": "true",
        "AWS_ENDPOINT_URL_S3": s3_endpoint,
        "DATALAKE_BUCKET": "coding-workshop-datalake-e2etest",
        "SOURCE_DATA_DIR": str(SOURCE_DIR),
        "SPARK_SHUFFLE_PARTITIONS": "4",
    }
    lake = DataLake(load_settings(env))
    first = _run_job(env)
    fp1 = _fingerprint(lake) if first.returncode == 0 else {}
    second = _run_job(env)
    fp2 = _fingerprint(lake) if second.returncode == 0 else {}
    return {"env": env, "lake": lake, "first": first, "second": second, "fp1": fp1, "fp2": fp2}


def test_both_runs_succeed(two_runs):
    for name in ("first", "second"):
        proc = two_runs[name]
        assert proc.returncode == 0, proc.stdout[-3000:] + proc.stderr[-3000:]


def test_source_ingestion_counts_match_bronze(two_runs):
    steps = _steps(two_runs["first"].stdout)
    for table, count in SOURCE_COUNTS.items():
        assert steps[f"bronze.{table}"]["rows_written"] == count
    assert steps["bronze.learning_activity"]["rows_written"] == RAW_EVENT_LINES
    assert steps["landing.publish"]["notes"]["size_check"] == "passed"


def test_bronze_partitions_follow_source_layout(two_runs):
    keys = [o.key for o in two_runs["lake"].list_objects("bronze/learning_activity/")]
    assert len(keys) == 783
    assert all("/year=" in k and "/month=" in k and "/day=" in k for k in keys)


def test_known_data_issues_are_quarantined(two_runs):
    report = json.loads(two_runs["lake"].get_bytes("_meta/dq_reports/latest/report.json"))
    events = report["entities"]["learning_events"]
    reasons = events["rejection_reasons"]
    for key, expected in EXPECTED_QUARANTINE.items():
        assert sum(v for r, v in reasons.items() if key in r) == expected, (key, reasons)
    assert events["duplicates_dropped"] == EXPECTED_DUPLICATES
    assert events["quarantined_files"] == [
        "s3://coding-workshop-datalake-e2etest/landing/learning_activity/year=2024/month=09/day=03/events-000.jsonl"
    ]
    assert two_runs["lake"].exists("quarantine/files/landing/learning_activity/year=2024/month=09/day=03/")


def test_reconciliation_bronze_equals_silver_plus_rejects(two_runs):
    report = json.loads(two_runs["lake"].get_bytes("_meta/dq_reports/latest/report.json"))
    assert report["reconciliation"] and all(report["reconciliation"].values()), report["reconciliation"]
    e = report["entities"]["learning_events"]
    assert e["bronze_rows"] == RAW_EVENT_LINES == e["silver_rows"] + e["quarantined_rows"] + e["duplicates_dropped"]


def test_silver_has_no_null_keys_or_duplicates(two_runs):
    events = two_runs["lake"].read_table("silver/learning_events").to_pandas()
    assert events["event_id"].notna().all() and events["event_id"].is_unique
    assert events["employee_id"].notna().all() and events["event_ts"].notna().all()


def test_gold_aggregates_match_silver(two_runs):
    lake = two_runs["lake"]
    gold = lambda t: lake.read_table(f"gold/{t}").to_pandas()  # noqa: E731
    assert gold("team_performance_outcomes")["headcount"].sum() == SOURCE_COUNTS["employees"]
    assert gold("team_performance_outcomes")["project_count"].sum() == SOURCE_COUNTS["projects"]
    assert gold("skill_distribution")["assessments"].sum() == SOURCE_COUNTS["competency_assessments"]
    assert len(gold("employee_performance_history")) == SOURCE_COUNTS["performance_reviews"]
    assert len(gold("promotion_pipeline")) == len(gold("attrition_risk")) == SOURCE_COUNTS["employees"]
    silver_events = lake.read_table("silver/learning_events").to_pandas()
    summary = gold("employee_learning_summary")
    assert summary["quiz_attempts"].sum() == (silver_events["event_type"] == "quiz_attempted").sum()


def test_second_run_is_idempotent(two_runs):
    steps = _steps(two_runs["second"].stdout)
    for name, rec in steps.items():
        if name.startswith(("landing.", "seed.", "bronze.")):
            assert rec["status"] == "skipped", name  # nothing re-ingested, raw data untouched
    assert two_runs["fp1"] and two_runs["fp1"] == two_runs["fp2"]


def test_lineage_records_written_per_run(two_runs):
    lake = two_runs["lake"]
    runs = [o.key for o in lake.list_objects("_meta/pipeline_runs/")]
    assert len(runs) == 2
    record = json.loads(lake.get_bytes(runs[0]))
    assert record["status"] == "succeeded" and record["duration_s"] is not None
    assert {"step", "source", "target", "rows_read", "rows_written", "status", "duration_s"} <= set(record["steps"][0])


def test_failure_exits_non_zero_with_error_logs(two_runs):
    env = {**two_runs["env"], "POSTGRES_PORT": "1", "RETRY_ATTEMPTS": "2"}
    proc = subprocess.run(
        [sys.executable, "job.py", "--stages", "bronze"], cwd=PIPELINE_DIR, env=env, capture_output=True, text=True, timeout=300
    )
    assert proc.returncode == 1
    events = [json.loads(line) for line in proc.stdout.splitlines() if line.startswith("{")]
    assert any(e["event"] == "retrying" for e in events)
    assert any(e["event"] == "job_finished" and e["status"] == "failed" for e in events)
