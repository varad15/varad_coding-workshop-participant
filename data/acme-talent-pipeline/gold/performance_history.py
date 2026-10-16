"""
Q1 - What are the performance ratings and review history for each employee?

gold/employee_performance_history  (grain: one row per employee per review period)
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from gold.inputs import SilverInputs, rating_label

TABLE = "employee_performance_history"
GRAIN = ("employee_id", "review_period")


def build(s: SilverInputs) -> DataFrame:
    """Every review with employee context and the change versus the previous review."""
    by_period = Window.partitionBy("employee_id").orderBy("review_period")
    latest_first = Window.partitionBy("employee_id").orderBy(F.desc("review_period"))
    reviews = (
        s.reviews.withColumn("review_sequence", F.row_number().over(by_period))
        .withColumn("previous_rating", F.lag("rating").over(by_period))
        .withColumn("rating_change", F.col("rating") - F.col("previous_rating"))
        .withColumn("is_latest_review", F.row_number().over(latest_first) == 1)
        .withColumn("rating_label", rating_label("rating"))
    )
    return reviews.join(s.employee_dim(), "employee_id").select(
        "employee_id",
        "full_name",
        "department",
        "team_name",
        "job_level",
        "review_id",
        "review_period",
        "review_sequence",
        "rating",
        "rating_label",
        "previous_rating",
        "rating_change",
        "is_latest_review",
        "manager_comment",
        "submitted_date",
    )
