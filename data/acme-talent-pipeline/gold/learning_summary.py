"""
Q4 - What training and development activities have been completed by each employee?

gold/employee_course_progress   (grain: one row per employee per course)
gold/employee_learning_summary  (grain: one row per employee, including employees with no activity)
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from gold.inputs import SilverInputs

PROGRESS_TABLE = "employee_course_progress"
PROGRESS_GRAIN = ("employee_id", "course_name")
SUMMARY_TABLE = "employee_learning_summary"
SUMMARY_GRAIN = ("employee_id",)


def _first_date(event_type: str) -> F.Column:
    """Earliest date of a given event type within the group."""
    return F.min(F.when(F.col("event_type") == event_type, F.col("event_date")))


def build_progress(s: SilverInputs) -> DataFrame:
    """Per employee and course: milestones, quiz results and overall status."""
    is_type = lambda t: (F.col("event_type") == t).cast("int")  # noqa: E731
    progress = s.events.groupBy("employee_id", "course_name").agg(
        _first_date("enrollment").alias("enrolled_on"),
        F.min("event_date").alias("first_activity_on"),
        F.max("event_date").alias("last_activity_on"),
        F.sum(is_type("module_completed")).alias("modules_completed"),
        F.sum(is_type("quiz_attempted")).alias("quiz_attempts"),
        F.sum(is_type("quiz_passed")).alias("quizzes_passed"),
        F.max(F.when(F.col("event_type").isin("quiz_attempted", "quiz_passed"), F.col("score"))).alias("best_quiz_score"),
        _first_date("course_completed").alias("completed_on"),
        _first_date("certification_earned").alias("certified_on"),
        F.sum((F.col("event_type") != "enrollment").cast("int")).alias("_activity_events"),
    )
    # Status = furthest milestone reached; any non-enrollment event means work has started.
    status = (
        F.when(F.col("certified_on").isNotNull(), "Certified")
        .when(F.col("completed_on").isNotNull(), "Completed")
        .when(F.col("_activity_events") > 0, "In progress")
        .otherwise("Enrolled")
    )
    return (
        progress.withColumn("status", status)
        .withColumn("is_completed", F.col("completed_on").isNotNull() | F.col("certified_on").isNotNull())
        .join(s.course_map, "course_name", "left")
        .join(s.employee_dim().select("employee_id", "full_name", "department", "team_name"), "employee_id")
        .select(
            "employee_id",
            "full_name",
            "department",
            "team_name",
            "course_name",
            "competency_id",
            "competency_name",
            "status",
            "is_completed",
            "enrolled_on",
            "first_activity_on",
            "last_activity_on",
            "completed_on",
            "certified_on",
            "modules_completed",
            "quiz_attempts",
            "quizzes_passed",
            "best_quiz_score",
        )
    )


def build_summary(s: SilverInputs, progress: DataFrame) -> DataFrame:
    """Per employee totals; employees with no learning activity get zeros."""
    quiz = s.events.filter(F.col("event_type").isin("quiz_attempted", "quiz_passed"))
    quiz_stats = quiz.groupBy("employee_id").agg(F.round(F.avg("score"), 1).alias("avg_quiz_score"))
    totals = progress.groupBy("employee_id").agg(
        F.count("*").alias("courses_started"),
        F.sum(F.col("is_completed").cast("int")).alias("courses_completed"),
        F.sum(F.col("certified_on").isNotNull().cast("int")).alias("certifications"),
        F.sum("quiz_attempts").alias("quiz_attempts"),
        F.sum("quizzes_passed").alias("quizzes_passed"),
        F.max("last_activity_on").alias("last_activity_on"),
        F.sort_array(F.collect_set(F.when(F.col("is_completed"), F.col("course_name")))).alias("completed_courses"),
    )
    counts = ["courses_started", "courses_completed", "certifications", "quiz_attempts", "quizzes_passed"]
    return (
        s.employee_dim()
        .select("employee_id", "full_name", "department", "team_name", "job_level")
        .join(totals, "employee_id", "left")
        .join(quiz_stats, "employee_id", "left")
        .fillna({c: 0 for c in counts})
        .withColumn("completed_courses", F.coalesce("completed_courses", F.array().cast("array<string>")))
        .withColumn("has_learning_activity", F.col("courses_started") > 0)
    )
