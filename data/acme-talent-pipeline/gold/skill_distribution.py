"""
Q7 - What is the distribution of skills across the organization?

gold/skill_distribution  (grain: one row per competency per department)
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from gold.inputs import SilverInputs

TABLE = "skill_distribution"
GRAIN = ("competency_id", "department")


def build(s: SilverInputs) -> DataFrame:
    """Proficiency spread and requirement coverage by competency and department."""
    comp = s.competencies.select("competency_id", F.col("name").alias("competency_name"), F.col("category").alias("competency_category"))
    level = lambda n: F.sum((F.col("proficiency_level") == n).cast("int")).alias(f"level_{n}_count")  # noqa: E731
    return (
        s.assessments.join(s.employee_dim().select("employee_id", "department"), "employee_id")
        .join(comp, "competency_id")
        .groupBy("competency_id", "competency_name", "competency_category", "department")
        .agg(
            F.count("*").alias("assessments"),
            F.countDistinct("employee_id").alias("employees_assessed"),
            F.round(F.avg("proficiency_level"), 2).alias("avg_proficiency"),
            F.round(F.avg("required_level"), 2).alias("avg_required_level"),
            F.round(F.avg((F.col("proficiency_level") >= F.col("required_level")).cast("double")) * 100, 1).alias(
                "pct_meeting_requirement"
            ),
            F.sum(((F.col("required_level") - F.col("proficiency_level")) >= 2).cast("int")).alias("critical_gaps"),
            *[level(n) for n in range(1, 6)],
        )
    )
