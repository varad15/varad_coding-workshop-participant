"""
Run-level and step-level lineage / observability.

Implements data-engineer.md section 4:
* rows read, processed (valid), rejected and written at each step;
* job and step start time, end time, duration and exit status;
* source -> target recorded for every step.

Each run writes one JSON record to `_meta/pipeline_runs/run_id=<id>/run.json`
and its step metrics to `_meta/step_metrics/` (Parquet), so lineage is
queryable from Jupyter next to the Gold tables.
"""

from __future__ import annotations

import json
import time
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Iterator

import pyarrow as pa

from common.logger import get_logger

log = get_logger("lineage")


def utc_now() -> datetime:
    """Return the current UTC time (second precision)."""
    return datetime.now(timezone.utc).replace(microsecond=0)


def new_run_id(now: datetime | None = None) -> str:
    """Return a sortable, unique run id like 20261015T023001Z-1a2b3c."""
    now = now or utc_now()
    return f"{now.strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:6]}"


class ZeroRowsError(RuntimeError):
    """Raised when a critical step writes zero rows (data-engineer.md section 7)."""


@dataclass
class StepMetrics:
    """Counts and timings for one pipeline step."""

    step: str
    source: str
    target: str
    rows_read: int = 0
    rows_valid: int = 0
    rows_rejected: int = 0
    rows_duplicate: int = 0
    rows_written: int = 0
    status: str = "running"
    started_at: str = ""
    ended_at: str = ""
    duration_s: float = 0.0
    error: str | None = None
    notes: dict[str, object] = field(default_factory=dict)


@dataclass
class RunContext:
    """State for one end-to-end pipeline run."""

    run_id: str
    started_at: datetime
    steps: list[StepMetrics] = field(default_factory=list)
    status: str = "running"
    ended_at: datetime | None = None

    @classmethod
    def start(cls) -> "RunContext":
        """Begin a new run."""
        now = utc_now()
        ctx = cls(run_id=new_run_id(now), started_at=now)
        log.info("job_started", extra={"run_id": ctx.run_id, "started_at": now.isoformat()})
        return ctx

    @contextmanager
    def step(self, name: str, source: str, target: str, critical: bool = False) -> Iterator[StepMetrics]:
        """
        Track one step: timing, counts and status.

        Args:
            name: Step name, e.g. 'bronze.employees'.
            source: Where data is read from.
            target: Where data is written to.
            critical: If True, writing zero rows fails the step (section 7).
        """
        metrics = StepMetrics(step=name, source=source, target=target, started_at=utc_now().isoformat())
        self.steps.append(metrics)
        clock = time.perf_counter()
        log.info("step_started", extra={"step": name, "source": source, "target": target})
        try:
            yield metrics
            if metrics.rows_written == 0 and metrics.status != "skipped":
                if critical:
                    raise ZeroRowsError(f"{name} wrote zero rows to {target}")
                log.warning("zero_rows_written", extra={"step": name, "target": target})
            if metrics.status == "running":
                metrics.status = "succeeded"
        except Exception as exc:
            metrics.status = "failed"
            metrics.error = repr(exc)
            raise
        finally:
            metrics.ended_at = utc_now().isoformat()
            metrics.duration_s = round(time.perf_counter() - clock, 3)
            level = log.error if metrics.status == "failed" else log.info
            level("step_finished", extra=asdict(metrics))

    def finish(self, status: str) -> None:
        """Mark the run finished and log the job summary."""
        self.status = status
        self.ended_at = utc_now()
        log.info(
            "job_finished",
            extra={
                "run_id": self.run_id,
                "status": status,
                "started_at": self.started_at.isoformat(),
                "ended_at": self.ended_at.isoformat(),
                "duration_s": (self.ended_at - self.started_at).total_seconds(),
                "steps": len(self.steps),
            },
        )

    def to_json(self) -> bytes:
        """Serialise the run record."""
        record = {
            "run_id": self.run_id,
            "status": self.status,
            "started_at": self.started_at.isoformat(),
            "ended_at": self.ended_at.isoformat() if self.ended_at else None,
            "duration_s": (self.ended_at - self.started_at).total_seconds() if self.ended_at else None,
            "steps": [asdict(s) for s in self.steps],
        }
        return json.dumps(record, indent=2, default=str).encode()

    def steps_table(self) -> pa.Table:
        """Return step metrics as an Arrow table (notes serialised as JSON)."""
        rows = []
        for s in self.steps:
            row = asdict(s)
            row["notes"] = json.dumps(row["notes"], default=str)
            row["run_id"] = self.run_id
            rows.append(row)
        return pa.Table.from_pylist(rows)
