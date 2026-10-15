"""
Q6 - How does employee performance correlate with team and project outcomes?

gold/team_performance_outcomes  (grain: one row per team)

Projects link to teams (not to employees), so the analysis is at team level:
team performance (from members' latest ratings) next to project outcomes.
Teams without projects are kept (has_projects = false) so headcount totals
reconcile; the correlation itself is computed in the notebook on teams with
projects (27 of 100 teams, 33 projects - a small sample, see limitations).
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from gold.inputs import SilverInputs

TABLE = "team_performance_outcomes"
GRAIN = ("team_id",)


def build(s: SilverInputs) -> DataFrame:
    """Team performance metrics joined to project outcome metrics."""
    members = s.employee_dim().join(s.review_trend(), "employee_id", "left")
    perf = members.groupBy("team_id", "team_name", "department").agg(
        F.count("*").alias("headcount"),
        F.round(F.avg("latest_rating"), 2).alias("avg_latest_rating"),
        F.round(F.avg((F.col("latest_rating") >= 4).cast("double")) * 100, 1).alias("pct_high_performers"),
        F.round(F.avg((F.col("latest_rating") <= 2).cast("double")) * 100, 1).alias("pct_low_performers"),
        F.round(F.avg("rating_change"), 2).alias("avg_rating_change"),
    )
    proj = s.projects.groupBy("team_id").agg(
        F.count("*").alias("project_count"),
        F.sum((F.col("outcome_status") == "on-time").cast("int")).alias("projects_on_time"),
        F.sum((F.col("outcome_status") == "delayed").cast("int")).alias("projects_delayed"),
        F.sum((F.col("outcome_status") == "at-risk").cast("int")).alias("projects_at_risk"),
        F.sum((F.col("outcome_status") == "cancelled").cast("int")).alias("projects_cancelled"),
        F.round(F.avg(F.col("outcome_score").cast("double")), 2).alias("avg_outcome_score"),
    )
    return (
        perf.join(proj, "team_id", "left")
        .fillna({c: 0 for c in ["project_count", "projects_on_time", "projects_delayed", "projects_at_risk", "projects_cancelled"]})
        .withColumn("has_projects", F.col("project_count") > 0)
        .withColumn(
            "on_time_rate_pct",
            F.when(F.col("project_count") > 0, F.round(F.col("projects_on_time") / F.col("project_count") * 100, 1)),
        )
    )
