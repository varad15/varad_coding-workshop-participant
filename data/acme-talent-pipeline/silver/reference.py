"""
Reference data: course -> competency mapping.

The learning platform has no competency_id, only free-text course names, so
linking training to skill gaps needs a curated mapping. It is versioned in
this repo (reference/course_competency_map.csv) and validated here:
  * every mapped competency_id must exist in Silver competencies;
  * every course seen in Silver events should be mapped (WARN otherwise).
"""

from __future__ import annotations

from pathlib import Path

import pyarrow.csv as pacsv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common.lineage import RunContext
from common.logger import get_logger
from common.spark import to_arrow, to_spark
from common.storage import DataLake

log = get_logger("silver.reference")

MAP_FILE = Path(__file__).resolve().parents[1] / "reference" / "course_competency_map.csv"
SILVER_MAP_PREFIX = "silver/reference/course_competency_map"


def build_course_competency_map(spark: SparkSession, lake: DataLake, run: RunContext) -> dict[str, object]:
    """Validate and publish the course -> competency mapping to Silver."""
    with run.step("silver.course_competency_map", source=str(MAP_FILE), target=lake.uri(SILVER_MAP_PREFIX), critical=True) as m:
        mapping = to_spark(spark, pacsv.read_csv(MAP_FILE))
        m.rows_read = mapping.count()
        competencies = to_spark(spark, lake.read_table("silver/hr/competencies")).select("competency_id", "name", "category")

        unknown = mapping.join(competencies, "competency_id", "left_anti").count()
        if unknown:
            raise ValueError(f"{unknown} mapped competency_id values do not exist in competencies")

        events = to_spark(spark, lake.read_table("silver/learning_events")).select("course_name").distinct()
        unmapped = [r["course_name"] for r in events.join(mapping, "course_name", "left_anti").collect()]
        if unmapped:
            log.warning("courses_without_competency_mapping", extra={"courses": unmapped})

        enriched = mapping.join(competencies.withColumnRenamed("name", "competency_name"), "competency_id").select(
            "course_name", "competency_id", "competency_name", F.col("category").alias("competency_category")
        )
        m.rows_written = lake.write_table(to_arrow(enriched), SILVER_MAP_PREFIX)
        return {"mapped_courses": m.rows_written, "unmapped_courses": unmapped}
