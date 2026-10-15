"""SparkSession factory shared by every stage."""

from __future__ import annotations

import os

import pyarrow as pa
from pyspark.sql import DataFrame, SparkSession

from common.config import Settings


def create_spark_session(settings: Settings, app_name: str = "acme-talent-pipeline") -> SparkSession:
    """
    Create (or reuse) the SparkSession used by the pipeline.

    Notes:
        * Arrow is enabled so data moves between Spark and the S3/PostgreSQL
          adapters as columnar Arrow batches.
        * All timestamps are handled in UTC.
        * Shuffle partitions are kept small: the dataset is ~14 MB and runs on
          one t3.medium node in EKS (see docs/limitations.md).
        * The repo skeleton's Iceberg catalog is intentionally not configured:
          Iceberg is not required by data-engineer.md and its runtime JAR must
          be downloaded from Maven at start-up (version-fragile).
    """
    # A pip-installed PySpark must not pick up a different Spark install
    # (e.g. /usr/local/spark 3.5 inside the jupyter/all-spark-notebook image).
    if os.environ.get("PIPELINE_USE_PIP_SPARK", "true").lower() == "true":
        os.environ.pop("SPARK_HOME", None)

    builder = (
        SparkSession.builder.appName(app_name)
        .master(settings.spark_master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "1g"))
        # Cap JVM memory outside the heap so the job fits the EKS pod limit
        # (measured: docs/test-evidence.md). Serial GC suits a 2-core node.
        .config(
            "spark.driver.extraJavaOptions",
            os.environ.get(
                "SPARK_DRIVER_JAVA_OPTS",
                "-XX:+UseSerialGC -XX:MaxMetaspaceSize=256m -XX:MaxDirectMemorySize=256m -XX:ReservedCodeCacheSize=128m -Xss512k",
            ),
        )
        .config("spark.ui.enabled", "false")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


class ArrowConversionError(RuntimeError):
    """Spark did not receive every row of an Arrow table."""


def to_spark(spark: SparkSession, table: pa.Table) -> DataFrame:
    """
    Convert an Arrow table to a Spark DataFrame (schema preserved).

    Chunks are merged first: PySpark 4.1.2 silently dropped ~1/3 of the rows
    when given the 783-chunk Bronze events table (one chunk per source file).
    The row count is then verified so a silent loss can never reach Silver.
    """
    df = spark.createDataFrame(table.combine_chunks())
    received = df.count()
    if received != table.num_rows:
        raise ArrowConversionError(f"Spark received {received} of {table.num_rows} rows")
    return df


def to_arrow(df: DataFrame) -> pa.Table:
    """Collect a Spark DataFrame as an Arrow table."""
    return df.toArrow()
