"""
Performance tests - data-engineer.md "Performance Testing":

1. Batch throughput   - records processed per minute (Bronze + Silver events).
2. Scale test         - 1x, 3x, 6x input; time per record must not grow non-linearly.
3. Resource efficiency- peak memory and average CPU of the whole job process tree.
4. External dependency- transient S3 failures are retried with backoff, then succeed.
5. SLA verification   - full end-to-end run (1x) finishes within PIPELINE_SLA_SECONDS.

Larger inputs are generated from the real sample: each copy k rewrites
event_id 'EV0...' -> 'EV{k}...' (still 10 digits, still unique) and is stored as
events-00k.jsonl in the same day folder, so data quality issues scale too.

Results are written to tests/performance/results/latest.json as test evidence.
Run: pytest -m performance
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path

import psutil
import pytest
from botocore.exceptions import EndpointConnectionError

pytestmark = [pytest.mark.performance, pytest.mark.integration]

PIPELINE_DIR = Path(__file__).resolve().parents[2]
SOURCE_DIR = PIPELINE_DIR.parent / "_source"
RESULTS = Path(__file__).resolve().parent / "results" / "latest.json"
SCALES = [1, 3, 6]
SLA_SECONDS = int(os.environ.get("PIPELINE_SLA_SECONDS", "600"))
MAX_PER_RECORD_SLOWDOWN = 2.0  # 6x data may cost at most 2x time *per record*

_results: dict[str, object] = {}


def _make_scaled_source(scale: int, root: Path) -> Path:
    """Copy the sample dataset with the event files multiplied `scale` times."""
    target = root / f"source_x{scale}"
    if target.exists():
        return target
    shutil.copytree(SOURCE_DIR, target, ignore=shutil.ignore_patterns("learning_activity"))
    for src in (SOURCE_DIR / "learning_activity").rglob("*.jsonl"):
        day_dir = target / "learning_activity" / src.parent.relative_to(SOURCE_DIR / "learning_activity")
        day_dir.mkdir(parents=True, exist_ok=True)
        text = src.read_text()
        for k in range(scale):
            (day_dir / f"events-{k:03d}.jsonl").write_text(text if k == 0 else text.replace('"EV0', f'"EV{k}'))
    return target


class _ResourceSampler:
    """Sample RSS and CPU of a process and all its children (incl. the JVM)."""

    def __init__(self, pid: int) -> None:
        self.proc = psutil.Process(pid)
        self.peak_rss = 0
        self.cpu_samples: list[float] = []
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                procs = [self.proc, *self.proc.children(recursive=True)]
                # PSS (proportional set size) counts shared pages once - close to
                # what the container's memory cgroup is charged.
                self.peak_rss = max(self.peak_rss, sum(p.memory_full_info().pss for p in procs))
                self.cpu_samples.append(sum(p.cpu_percent(interval=None) for p in procs))
            except psutil.Error:
                pass
            time.sleep(0.5)

    def __enter__(self) -> "_ResourceSampler":
        self._thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self._stop.set()
        self._thread.join()


def _run(env: dict[str, str], stages: str) -> tuple[subprocess.CompletedProcess, _ResourceSampler, float]:
    start = time.perf_counter()
    proc = subprocess.Popen(
        [sys.executable, "job.py", "--stages", stages], cwd=PIPELINE_DIR, env=env,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    with _ResourceSampler(proc.pid) as sampler:
        out, err = proc.communicate(timeout=3600)
    elapsed = time.perf_counter() - start
    return subprocess.CompletedProcess(proc.args, proc.returncode, out, err), sampler, elapsed


def _steps(stdout: str) -> dict[str, dict]:
    return {
        r["step"]: r
        for r in (json.loads(line) for line in stdout.splitlines() if line.startswith("{"))
        if r["event"] == "step_finished"
    }


@pytest.fixture(scope="module")
def scale_runs(s3_endpoint, pg_env, tmp_path_factory):
    """Run landing+seed+bronze+silver at each scale on its own bucket; collect metrics."""
    root = tmp_path_factory.mktemp("scaled")
    runs = {}
    for scale in SCALES:
        env = {
            **os.environ, **pg_env, "IS_LOCAL": "true", "AWS_ENDPOINT_URL_S3": s3_endpoint,
            "DATALAKE_BUCKET": f"coding-workshop-datalake-perf-x{scale}",
            "SOURCE_DATA_DIR": str(_make_scaled_source(scale, root)),
            "SPARK_MASTER": "local[2]", "SPARK_DRIVER_MEMORY": "512m",  # cloud settings (infra/locals.tf)
        }
        proc, sampler, elapsed = _run(env, "landing,seed,bronze,silver")
        assert proc.returncode == 0, proc.stdout[-2000:] + proc.stderr[-2000:]
        steps = _steps(proc.stdout)
        records = steps["bronze.learning_activity"]["rows_written"]
        event_seconds = steps["bronze.learning_activity"]["duration_s"] + steps["silver.learning_events"]["duration_s"]
        runs[scale] = {
            "raw_event_lines": records,
            "silver_events": steps["silver.learning_events"]["rows_written"],
            "event_pipeline_seconds": round(event_seconds, 2),
            "records_per_minute": round(records / event_seconds * 60),
            "seconds_per_10k_records": round(event_seconds / records * 10_000, 3),
            "job_wall_seconds": round(elapsed, 1),
            "peak_pss_mb": round(sampler.peak_rss / 1024**2),
            "avg_cpu_percent": round(sum(sampler.cpu_samples) / max(len(sampler.cpu_samples), 1)),
        }
    _results["scale"] = runs
    return runs


def test_batch_throughput(scale_runs):
    base = scale_runs[1]
    assert base["raw_event_lines"] == 29868
    assert base["records_per_minute"] > 10_000  # floor; actual figure recorded in results


def test_scale_is_roughly_linear(scale_runs):
    for scale in SCALES[1:]:
        assert scale_runs[scale]["raw_event_lines"] == 29868 * scale
    ratio = scale_runs[SCALES[-1]]["seconds_per_10k_records"] / scale_runs[1]["seconds_per_10k_records"]
    _results["per_record_slowdown_6x_vs_1x"] = round(ratio, 2)
    assert ratio <= MAX_PER_RECORD_SLOWDOWN


def test_resource_usage_fits_eks_job_limit(scale_runs):
    # Helm job limit: 3Gi memory (infra/helm/values.yaml); keep >= 10% headroom.
    assert scale_runs[1]["peak_pss_mb"] < 3072 * 0.9


def test_external_dependency_retry_backoff(lake, monkeypatch):
    """Two simulated S3 outages, then success: the write must still land."""
    import pyarrow as pa

    real_put = lake.client.put_object
    failures = {"left": 2}

    def flaky_put(**kwargs):
        if failures["left"]:
            failures["left"] -= 1
            raise EndpointConnectionError(endpoint_url="http://s3")
        return real_put(**kwargs)

    monkeypatch.setattr(lake.client, "put_object", flaky_put)
    monkeypatch.setattr(lake, "_attempts", 4)  # 2 failures must be absorbed
    monkeypatch.setattr("common.retry.time.sleep", lambda _: None)
    lake.put_bytes("_meta/probe.json", b"{}")
    assert failures["left"] == 0 and lake.get_bytes("_meta/probe.json") == b"{}"
    lake.write_table(pa.table({"v": [1]}), "silver/probe")
    assert lake.read_table("silver/probe").num_rows == 1


def test_end_to_end_sla(s3_endpoint, pg_env):
    env = {
        **os.environ, **pg_env, "IS_LOCAL": "true", "AWS_ENDPOINT_URL_S3": s3_endpoint,
        "DATALAKE_BUCKET": "coding-workshop-datalake-perf-sla", "SOURCE_DATA_DIR": str(SOURCE_DIR),
        "SPARK_MASTER": "local[2]", "SPARK_DRIVER_MEMORY": "512m",
    }
    proc, sampler, elapsed = _run(env, "landing,seed,bronze,silver,gold")
    assert proc.returncode == 0, proc.stdout[-2000:]
    _results["sla"] = {
        "sla_seconds": SLA_SECONDS,
        "end_to_end_seconds": round(elapsed, 1),
        "peak_pss_mb": round(sampler.peak_rss / 1024**2),
    }
    assert elapsed < SLA_SECONDS


def teardown_module() -> None:
    """Persist measured figures as test evidence."""
    if _results:
        RESULTS.parent.mkdir(parents=True, exist_ok=True)
        RESULTS.write_text(json.dumps(_results, indent=2))
