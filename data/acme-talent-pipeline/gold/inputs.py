"""
Shared Silver inputs and dimensions for Gold.

Determinism: Gold never uses the wall clock. The analysis date (`as_of_date`)
is the latest date present in the data, so re-running on the same Silver
input always produces identical Gold output (idempotency).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from common.spark import to_spark
from common.storage import DataLake

RATING_LABELS = {1: "Unsatisfactory", 2: "Needs Improvement", 3: "Meets Expectations", 4: "Exceeds Expectations", 5: "Outstanding"}


def rating_label(col: str) -> F.Column:
    """Map a 1-5 rating to its label."""
    expr = F.when(F.lit(False), F.lit(None))
    for value, label in RATING_LABELS.items():
        expr = expr.when(F.col(col) == value, F.lit(label))
    return expr


@dataclass
class SilverInputs:
    """All Silver datasets Gold reads, loaded once."""

    teams: DataFrame
    competencies: DataFrame
    employees: DataFrame
    projects: DataFrame
    reviews: DataFrame
    assessments: DataFrame
    talent: DataFrame
    events: DataFrame
    course_map: DataFrame
    as_of_date: date

    @classmethod
    def load(cls, spark: SparkSession, lake: DataLake) -> "SilverInputs":
        """Read every Silver dataset (lineage columns dropped)."""

        def read(prefix: str) -> DataFrame:
            df = to_spark(spark, lake.read_table(prefix))
            return df.drop(*[c for c in df.columns if c.startswith("_")]).cache()

        events = read("silver/learning_events")
        assessments = read("silver/hr/competency_assessments")
        reviews = read("silver/hr/performance_reviews")
        as_of = (
            events.select(F.max("event_date").alias("d"))
            .unionByName(assessments.select(F.max("assessed_date").alias("d")))
            .unionByName(reviews.select(F.max("submitted_date").alias("d")))
            .agg(F.max("d"))
            .first()[0]
        )
        return cls(
            teams=read("silver/hr/teams"),
            competencies=read("silver/hr/competencies"),
            employees=read("silver/hr/employees"),
            projects=read("silver/hr/projects"),
            reviews=reviews,
            assessments=assessments,
            talent=read("silver/hr/talent_review"),
            events=events,
            course_map=read("silver/reference/course_competency_map"),
            as_of_date=as_of,
        )

    def employee_dim(self) -> DataFrame:
        """One row per employee with team, department and tenure."""
        teams = self.teams.select("team_id", F.col("name").alias("team_name"), "department")
        return self.employees.join(teams, "team_id").select(
            "employee_id",
            F.concat_ws(" ", "first_name", "last_name").alias("full_name"),
            "email",
            "job_level",
            "team_id",
            "team_name",
            "department",
            "hire_date",
            F.round(F.months_between(F.lit(self.as_of_date), "hire_date") / 12, 1).alias("tenure_years"),
        )

    def review_trend(self) -> DataFrame:
        """
        Per employee: latest and previous rating and the change between them.

        Review periods sort correctly as text ('2024-H2' < '2025-H1').
        """
        ordered = Window.partitionBy("employee_id").orderBy("review_period")
        latest_first = Window.partitionBy("employee_id").orderBy(F.desc("review_period"))
        ranked = self.reviews.withColumn("previous_rating", F.lag("rating").over(ordered)).withColumn(
            "_rn", F.row_number().over(latest_first)
        )
        stats = self.reviews.groupBy("employee_id").agg(
            F.round(F.avg("rating"), 2).alias("avg_rating"), F.count("*").alias("review_count")
        )
        latest = ranked.filter(F.col("_rn") == 1).select(
            "employee_id",
            F.col("review_period").alias("latest_review_period"),
            F.col("rating").alias("latest_rating"),
            "previous_rating",
            (F.col("rating") - F.col("previous_rating")).alias("rating_change"),
            F.col("manager_comment").alias("latest_manager_comment"),
        )
        return latest.join(stats, "employee_id").withColumn(
            "rating_trend",
            F.when(F.col("rating_change").isNull(), "Single review")
            .when(F.col("rating_change") < 0, "Declining")
            .when(F.col("rating_change") > 0, "Improving")
            .otherwise("Stable"),
        )
