"""
ACME Inc. Performance & Talent Management - batch ETL job.

Entry point created from data/_examples/python-job/job.py; the skeleton's
functions are kept:

    create_spark_session() -> ingest_into_bronze() -> transform_to_silver() -> aggregate_to_gold()

Two source bootstrap stages run first (they simulate the upstream systems):
  landing - publish backend/_source files to the S3 landing zone (skipped when
            the folder is absent, e.g. inside the EKS pod, where the prereqs
            script has already synced the files);
  seed    - load the HR CSVs from S3 into PostgreSQL (Aurora starts empty).

This file runs in two modes:

  * Pipeline mode - a full checkout (or the unpacked source bundle): the
    sibling packages bronze/, silver/, gold/ exist next to this file and the
    pipeline below runs directly.  Local usage per docs/data-engineer.md:

        pip install -r ../data/acme-talent-pipeline/requirements.txt
        python ../data/acme-talent-pipeline/job.py

  * Bootstrap mode - the stock EKS chart (infra/helm/templates/python-job.yaml)
    ships exactly two files into the pod, this job.py and requirements.txt,
    mounted at /app, with no environment variables, no AWS identity and no
    database settings.  When the sibling packages are missing, this file
    therefore (1) reads the Kubernetes Secret written by
    deploy/cloud-prereqs.sh through the in-cluster API and exports it as the
    job environment, (2) builds an isolated virtualenv in /tmp/venv (the
    image's conda NumPy 1.x clashes with our pinned stack), (3) downloads the
    full pipeline source bundle the prereqs script uploaded to the data lake,
    and (4) re-executes itself from that bundle in pipeline mode.

Usage:
    python job.py                         # all stages
    python job.py --stages bronze,silver  # selected stages
Exit code is 0 on success and 1 on failure (Kubernetes Job status).
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent

# --------------------------------------------------------------------------- #
# Bootstrap mode (stock EKS chart: only job.py + requirements.txt in the pod) #
# --------------------------------------------------------------------------- #
if not (_HERE / "bronze").is_dir():
    import base64
    import json
    import ssl
    import subprocess
    import tarfile
    import urllib.request

    _SA = Path(os.environ.get("PIPELINE_SA_DIR", "/var/run/secrets/kubernetes.io/serviceaccount"))
    _VENV = "/tmp/venv"
    _SRC_DIR = "/tmp/app"

    def _blog(msg: str) -> None:
        print(f"[bootstrap] {msg}", flush=True)

    def _load_secret_env() -> None:
        """Export the job Secret as environment variables via the in-cluster API.

        The stock chart gives the pod no env vars, but every pod gets its
        service-account token mounted. deploy/cloud-prereqs.sh grants the
        default service account read access to this one named Secret.
        """
        if not (_SA / "token").exists():
            raise SystemExit(
                f"[bootstrap] no service-account token at {_SA} - bootstrap mode only runs "
                "inside the EKS pod; locally, run job.py from a full checkout instead"
            )
        secret = os.environ.get("PIPELINE_SECRET_NAME", "acme-talent-pipeline-secrets")
        namespace = "default"
        ns_file = _SA / "namespace"
        if ns_file.exists():
            namespace = ns_file.read_text().strip()
        token = (_SA / "token").read_text().strip()
        ctx = ssl.create_default_context(cafile=str(_SA / "ca.crt"))
        url = f"https://kubernetes.default.svc/api/v1/namespaces/{namespace}/secrets/{secret}"
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
        try:
            with urllib.request.urlopen(req, context=ctx, timeout=30) as resp:
                data = json.load(resp).get("data", {})
        except urllib.error.HTTPError as exc:  # noqa: PERF203
            raise SystemExit(
                f"[bootstrap] cannot read Secret '{secret}' ({exc.code}): "
                "run data/acme-talent-pipeline/deploy/cloud-prereqs.sh before deploying"
            ) from exc
        for key, value in data.items():
            os.environ.setdefault(key, base64.b64decode(value).decode())
        _blog(f"loaded {len(data)} environment keys from Secret {namespace}/{secret}")

    def _apply_profile() -> None:
        """Defaults for the stock chart's 2 GiB / 1 CPU pod (Secret values win).

        Measured peak RSS with this profile: ~1.7 GiB (docs/test-evidence.md).
        """
        for key, value in {
            "IS_LOCAL": "false",
            "SPARK_MASTER": "local[1]",
            "SPARK_DRIVER_MEMORY": "512m",
            "SPARK_SHUFFLE_PARTITIONS": "4",
            "SPARK_EXTRA_CONF": "spark.python.worker.reuse=false",
        }.items():
            os.environ.setdefault(key, value)
        os.environ.pop("PYTHONPATH", None)  # the image's conda paths must not leak in
        os.environ["PYSPARK_PYTHON"] = f"{_VENV}/bin/python"
        os.environ["PYSPARK_DRIVER_PYTHON"] = f"{_VENV}/bin/python"

    def _build_venv() -> None:
        reqs = Path("/app/requirements.txt")
        if not reqs.exists():
            reqs = _HERE / "requirements.txt"
        if not Path(_VENV, "bin", "python").exists():
            _blog("creating /tmp/venv")
            subprocess.run([sys.executable, "-m", "venv", _VENV], check=True)
        _blog(f"installing pinned dependencies from {reqs}")
        subprocess.run(
            [f"{_VENV}/bin/pip", "install", "--quiet", "--no-cache-dir", "-r", str(reqs)],
            check=True,
        )

    def _fetch_bundle() -> None:
        bucket = os.environ.get("DATALAKE_BUCKET")
        if not bucket:
            raise SystemExit(
                "[bootstrap] DATALAKE_BUCKET missing from the Secret: "
                "re-run data/acme-talent-pipeline/deploy/cloud-prereqs.sh"
            )
        key = os.environ.get("PIPELINE_SRC_KEY", "_deploy/src.tar.gz")
        _blog(f"downloading s3://{bucket}/{key}")
        subprocess.run(
            [
                f"{_VENV}/bin/python",
                "-c",
                "import boto3,sys; boto3.client('s3').download_file(sys.argv[1], sys.argv[2], '/tmp/src.tar.gz')",
                bucket,
                key,
            ],
            check=True,
        )
        Path(_SRC_DIR).mkdir(exist_ok=True)
        with tarfile.open("/tmp/src.tar.gz") as tar:
            try:
                tar.extractall(_SRC_DIR, filter="data")
            except TypeError:  # Python < 3.12 has no extract filters
                tar.extractall(_SRC_DIR)  # noqa: S202 - bundle built by our own prereqs script
        _blog(f"source bundle unpacked to {_SRC_DIR}")

    def _bootstrap() -> None:
        _blog("sibling packages missing - stock-chart pod detected")
        _load_secret_env()
        _apply_profile()
        _build_venv()
        _fetch_bundle()
        # Spark's Python workers are separate processes: they do not inherit the
        # driver's sys.path, only its environment and working directory. Both
        # must point at the bundle or the workers fail to import contracts/ etc.
        os.chdir(_SRC_DIR)
        os.environ["PYTHONPATH"] = _SRC_DIR
        target = f"{_SRC_DIR}/job.py"
        _blog(f"exec {_VENV}/bin/python {target}")
        os.execv(f"{_VENV}/bin/python", [f"{_VENV}/bin/python", target, *sys.argv[1:]])

    _bootstrap()
    raise SystemExit(1)  # unreachable: execv does not return

# --------------------------------------------------------------------------- #
# Pipeline mode (full checkout or unpacked bundle)                            #
# --------------------------------------------------------------------------- #
import argparse  # noqa: E402

# Make sibling packages (common/, contracts/, ...) importable regardless of CWD.
sys.path.insert(0, str(_HERE))

from pyspark.sql import SparkSession  # noqa: E402

from bronze.ingest_events import ingest_learning_events  # noqa: E402
from bronze.ingest_postgres import ingest_postgres_tables  # noqa: E402
from common import spark as spark_factory  # noqa: E402
from common.config import Settings, load_settings  # noqa: E402
from common.lineage import RunContext  # noqa: E402
from common.logger import bind_context, configure_logging, get_logger  # noqa: E402
from common.postgres import Postgres  # noqa: E402
from common.storage import DataLake  # noqa: E402
from seed.load_sources import publish_landing, seed_postgres  # noqa: E402

ALL_STAGES = ("landing", "seed", "bronze", "silver", "gold")
log = get_logger("job")


def create_spark_session(settings: Settings) -> SparkSession:
    """Create and configure the Spark session."""
    return spark_factory.create_spark_session(settings)


def publish_sources(settings: Settings, lake: DataLake, run: RunContext) -> None:
    """Publish the sample dataset to the S3 landing zone (S3 source system)."""
    lake.ensure_bucket()
    if settings.source_dir.exists():
        publish_landing(lake, settings.source_dir, run)
    else:
        log.info("source_dir_absent_skip_publish", extra={"source_dir": str(settings.source_dir)})


def seed_sources(pg: Postgres, lake: DataLake, run: RunContext) -> None:
    """Load the PostgreSQL source system from the landing zone."""
    seed_postgres(pg, lake, run)


def ingest_into_bronze(pg: Postgres, lake: DataLake, run: RunContext) -> None:
    """Ingest raw data into the bronze layer."""
    ingest_postgres_tables(pg, lake, run)
    ingest_learning_events(lake, run)


def transform_to_silver(spark: SparkSession, lake: DataLake, run: RunContext) -> None:
    """Transform data from the bronze layer to the silver layer."""
    from silver.run import run_silver

    run_silver(spark, lake, run)


def aggregate_to_gold(spark: SparkSession, lake: DataLake, run: RunContext) -> None:
    """Aggregate data from the silver layer to the gold layer."""
    from gold.run import run_gold

    run_gold(spark, lake, run)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--stages", default=",".join(ALL_STAGES), help=f"comma list of {ALL_STAGES}")
    args = parser.parse_args(argv)
    args.stages = [s.strip() for s in args.stages.split(",") if s.strip()]
    unknown = set(args.stages) - set(ALL_STAGES)
    if unknown:
        parser.error(f"unknown stages: {sorted(unknown)}")
    return args


def main(argv: list[str] | None = None) -> int:
    """Run the ETL job steps in sequence; return the process exit code."""
    args = parse_args(argv)
    settings = load_settings()
    configure_logging(settings.log_level)
    run = RunContext.start()
    bind_context(run_id=run.run_id)
    log.info("settings_loaded", extra={"settings": settings.describe(), "stages": args.stages})

    pg, lake = Postgres(settings), DataLake(settings)
    spark: SparkSession | None = None
    status = "failed"
    try:
        if "landing" in args.stages and settings.seed_sources:
            publish_sources(settings, lake, run)
        if "seed" in args.stages and settings.seed_sources:
            seed_sources(pg, lake, run)
        if "bronze" in args.stages:
            ingest_into_bronze(pg, lake, run)
        if "silver" in args.stages or "gold" in args.stages:
            # Create the Spark session
            spark = create_spark_session(settings)
        if "silver" in args.stages:
            transform_to_silver(spark, lake, run)
        if "gold" in args.stages:
            aggregate_to_gold(spark, lake, run)
        status = "succeeded"
    except Exception:
        log.exception("job_failed")
    finally:
        run.finish(status)
        try:
            lake.put_bytes(f"_meta/pipeline_runs/run_id={run.run_id}/run.json", run.to_json(), "application/json")
            lake.write_table(run.steps_table(), f"_meta/step_metrics/run_id={run.run_id}")
        except Exception:
            log.exception("lineage_write_failed")
        # Stop the Spark session
        if spark is not None:
            spark.stop()
    return 0 if status == "succeeded" else 1


if __name__ == "__main__":
    sys.exit(main())
