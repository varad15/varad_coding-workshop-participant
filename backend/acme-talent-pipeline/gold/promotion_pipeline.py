"""
Q3 - Who are the high-potential employees ready for promotion?

gold/promotion_pipeline  (grain: one row per employee)

Combines the talent review (potential, readiness) with performance, using the
industry-standard 9-box grid (performance x potential):

  performance band: rating 1-2 Low, 3 Moderate, 4-5 High (latest review)
  potential band:   talent_review.potential_rating (Low / Medium / High)

is_promotion_candidate = potential High AND readiness 'Ready Now' AND latest rating >= 4
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from gold.inputs import SilverInputs

TABLE = "promotion_pipeline"
GRAIN = ("employee_id",)

# (performance band, potential) -> 9-box label
NINE_BOX = {
    ("High", "High"): "Star",
    ("Moderate", "High"): "High Potential",
    ("Low", "High"): "Enigma",
    ("High", "Medium"): "High Performer",
    ("Moderate", "Medium"): "Core Player",
    ("Low", "Medium"): "Inconsistent Player",
    ("High", "Low"): "Trusted Professional",
    ("Moderate", "Low"): "Effective Contributor",
    ("Low", "Low"): "Underperformer",
}


def build(s: SilverInputs) -> DataFrame:
    """Per-employee talent profile with 9-box placement and candidate flag."""
    band = F.when(F.col("latest_rating") >= 4, "High").when(F.col("latest_rating") == 3, "Moderate").otherwise("Low")
    nine_box = F.when(F.lit(False), F.lit(None))
    for (perf, pot), label in NINE_BOX.items():
        nine_box = nine_box.when((F.col("performance_band") == perf) & (F.col("potential_rating") == pot), F.lit(label))

    critical_gaps = s.assessments.groupBy("employee_id").agg(
        F.sum(((F.col("required_level") - F.col("proficiency_level")) >= 2).cast("int")).alias("critical_gap_count")
    )
    certs = (
        s.events.filter(F.col("event_type") == "certification_earned")
        .groupBy("employee_id")
        .agg(F.countDistinct("course_name").alias("certifications"))
    )

    df = (
        s.employee_dim()
        .join(s.talent, "employee_id")
        .join(s.review_trend(), "employee_id", "left")
        .join(critical_gaps, "employee_id", "left")
        .join(certs, "employee_id", "left")
        .fillna({"critical_gap_count": 0, "certifications": 0})
        .withColumn("performance_band", band)
        .withColumn("nine_box", nine_box)
        .withColumn("is_high_potential", F.col("potential_rating") == "High")
        .withColumn("is_ready_now", F.col("promotion_readiness") == "Ready Now")
        .withColumn(
            "is_promotion_candidate",
            F.col("is_high_potential") & F.col("is_ready_now") & (F.col("latest_rating") >= 4),
        )
    )
    return df.select(
        "employee_id",
        "full_name",
        "department",
        "team_name",
        "job_level",
        "tenure_years",
        "review_cycle",
        "potential_rating",
        "promotion_readiness",
        "latest_rating",
        "avg_rating",
        "rating_trend",
        "performance_band",
        "nine_box",
        "critical_gap_count",
        "certifications",
        "is_high_potential",
        "is_ready_now",
        "is_promotion_candidate",
    )
