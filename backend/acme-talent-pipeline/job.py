"""
ACME Inc. Performance & Talent Management - batch ETL job.

Entry point created from data/_examples/python-job/job.py; the skeleton's
functions are kept:

    create_spark_session() -> ingest_into_bronze() -> transform_to_silver() -> aggregate_to_gold()

Two source bootstrap stages run first (they simulate the upstream systems):
  landing - publish backend/_source files to the S3 landing zone (skipped when
            the folder is absent, e.g. inside the EKS pod, where Terraform has
            already synced the files);
  seed    - load the HR CSVs from S3 into PostgreSQL (Aurora starts empty).

Usage:
    python job.py                         # all stages
    python job.py --stages bronze,silver  # selected stages
Exit code is 0 on success and 1 on failure (Kubernetes Job status).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Make sibling packages (common/, contracts/, ...) importable regardless of CWD.
sys.path.insert(0, str(Path(__file__).resolve().parent))

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
