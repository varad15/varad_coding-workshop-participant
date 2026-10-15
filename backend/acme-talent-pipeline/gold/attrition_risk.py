"""
Q5 - Which employees are at risk of attrition based on performance trends?

gold/attrition_risk  (grain: one row per employee)

Two views side by side:
  * hr_attrition_risk     - the manager-assessed flag from talent_review;
  * derived_risk_level    - computed from performance signals in the data:

        +2  rating declined between the last two reviews
        +2  latest rating <= 2
        +1  latest manager comment is negative (see NEGATIVE_COMMENT_PATTERNS)
        +1  no learning activity in the 180 days before the as-of date
        score >= 3 -> High, 1-2 -> Medium, 0 -> Low

The rule weights are transparent assumptions (docs/limitations.md); with only
two review periods, "trend" means a single period-over-period change.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from gold.inputs import SilverInputs

TABLE = "attrition_risk"
GRAIN = ("employee_id",)
INACTIVITY_DAYS = 180
NEGATIVE_COMMENT_PATTERNS = ["trended down", "missed", "struggled", "needs support", "follow-up plan"]


def build(s: SilverInputs) -> DataFrame:
    """Per-employee attrition signals, derived risk and HR risk."""
    recent_cutoff = F.date_sub(F.lit(s.as_of_date), INACTIVITY_DAYS)
    recent = (
        s.events.filter(F.col("event_date") > recent_cutoff)
        .groupBy("employee_id")
        .agg(F.count("*").alias("learning_events_last_180d"))
    )
    negative = F.lower(F.coalesce(F.col("latest_manager_comment"), F.lit("")))
    negative_flag = F.lit(False)
    for pattern in NEGATIVE_COMMENT_PATTERNS:
        negative_flag = negative_flag | negative.contains(pattern)

    df = (
        s.employee_dim()
        .join(s.talent.select("employee_id", F.col("attrition_risk").alias("hr_attrition_risk"), "potential_rating"), "employee_id")
        .join(s.review_trend(), "employee_id", "left")
        .join(recent, "employee_id", "left")
        .fillna({"learning_events_last_180d": 0})
        .withColumn("signal_rating_declined", F.coalesce(F.col("rating_change") < 0, F.lit(False)))
        .withColumn("signal_low_rating", F.coalesce(F.col("latest_rating") <= 2, F.lit(False)))
        .withColumn("signal_negative_comment", negative_flag)
        .withColumn("signal_no_recent_learning", F.col("learning_events_last_180d") == 0)
    )
    score = (
        F.col("signal_rating_declined").cast("int") * 2
        + F.col("signal_low_rating").cast("int") * 2
        + F.col("signal_negative_comment").cast("int")
        + F.col("signal_no_recent_learning").cast("int")
    )
    level = F.when(F.col("derived_risk_score") >= 3, "High").when(F.col("derived_risk_score") >= 1, "Medium").otherwise("Low")
    return (
        df.withColumn("derived_risk_score", score)
        .withColumn("derived_risk_level", level)
        .withColumn("risk_views_agree", F.col("derived_risk_level") == F.col("hr_attrition_risk"))
        .withColumn(
            "is_at_risk",
            (F.col("derived_risk_level") == "High") | (F.col("hr_attrition_risk") == "High"),
        )
        .withColumn("as_of_date", F.lit(s.as_of_date))
        .select(
            "employee_id",
            "full_name",
            "department",
            "team_name",
            "job_level",
            "tenure_years",
            "potential_rating",
            "previous_rating",
            "latest_rating",
            "rating_change",
            "rating_trend",
            "latest_manager_comment",
            "learning_events_last_180d",
            "signal_rating_declined",
            "signal_low_rating",
            "signal_negative_comment",
            "signal_no_recent_learning",
            "derived_risk_score",
            "derived_risk_level",
            "hr_attrition_risk",
            "risk_views_agree",
            "is_at_risk",
            "as_of_date",
        )
    )
