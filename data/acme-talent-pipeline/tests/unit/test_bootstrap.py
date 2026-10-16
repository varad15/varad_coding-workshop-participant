"""job.py dual-mode behaviour.

The stock EKS chart (infra/helm/templates/python-job.yaml) ships job.py into
the pod alone, so job.py bootstraps when its sibling packages are missing and
runs the pipeline directly when they are present.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

PIPELINE_DIR = Path(__file__).resolve().parents[2]


def test_lone_job_py_enters_bootstrap_and_fails_cleanly(tmp_path):
    """A lone job.py (stock-chart pod layout) without a service-account token
    must exit non-zero with an actionable message, before touching pip or S3."""
    shutil.copy(PIPELINE_DIR / "job.py", tmp_path / "job.py")
    result = subprocess.run(
        [sys.executable, str(tmp_path / "job.py")],
        capture_output=True,
        text=True,
        timeout=60,
        env={"PATH": "/usr/bin:/bin", "PIPELINE_SA_DIR": str(tmp_path / "no-such-sa")},
    )
    assert result.returncode != 0
    combined = result.stdout + result.stderr
    assert "bootstrap" in combined
    assert "service-account token" in combined


def test_full_checkout_skips_bootstrap(tmp_path):
    """From the checkout (bronze/ present) job.py must go straight to the
    pipeline argument parser - no bootstrap, no Kubernetes lookups."""
    result = subprocess.run(
        [sys.executable, str(PIPELINE_DIR / "job.py"), "--stages", "not-a-stage"],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 2  # argparse usage error from pipeline mode
    assert "unknown stages" in result.stderr
    assert "bootstrap" not in result.stdout + result.stderr
