"""
Q2 - Which employees have critical skill gaps in key competencies?

gold/employee_skill_gaps  (grain: one row per competency assessment)

gap = required_level - proficiency_level
  gap <= 0 -> 'No gap'      (meets or exceeds the requirement)
  gap == 1 -> 'Minor gap'
  gap >= 2 -> 'Critical gap'   (assumption, see docs/limitations.md: two or
                                more levels short of the role requirement)
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from gold.inputs import SilverInputs

TABLE = "employee_skill_gaps"
GRAIN = ("assessment_id",)
CRITICAL_GAP = 2


def build(s: SilverInputs) -> DataFrame:
    """Each assessment with gap size and severity."""
    comp = s.competencies.select("competency_id", F.col("name").alias("competency_name"), F.col("category").alias("competency_category"))
    gap = F.col("required_level") - F.col("proficiency_level")
    return (
        s.assessments.join(comp, "competency_id")
        .join(s.employee_dim(), "employee_id")
        .withColumn("gap", gap)
        .withColumn(
            "gap_severity",
            F.when(gap >= CRITICAL_GAP, "Critical gap").when(gap == 1, "Minor gap").otherwise("No gap"),
        )
        .withColumn("is_critical_gap", gap >= CRITICAL_GAP)
        .select(
            "assessment_id",
            "employee_id",
            "full_name",
            "department",
            "team_name",
            "job_level",
            "competency_id",
            "competency_name",
            "competency_category",
            "proficiency_level",
            "required_level",
            "gap",
            "gap_severity",
            "is_critical_gap",
            "assessed_date",
        )
    )
