"""
Gold stage orchestration with the data-engineer.md section 6 Gold checklist:

  * grain/cardinality: the declared grain columns are unique and non-null;
  * cross-validation: Gold row counts / aggregate totals equal Silver counts,
    so no record is unexpectedly dropped or duplicated by joins.

Every Gold table is fully rebuilt from Silver (overwrite) -> idempotent.
"""

from __future__ import annotations

from typing import Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common.lineage import RunContext
from common.logger import get_logger
from common.spark import to_arrow
from common.storage import DataLake
from gold import attrition_risk, learning_summary, performance_history, promotion_pipeline, skill_distribution, skill_gaps, team_outcomes
from gold.inputs import SilverInputs

log = get_logger("gold")
GOLD_PREFIX = "gold"


class GoldCheckError(RuntimeError):
    """A Gold table failed a grain or reconciliation check."""


def assert_grain(df: DataFrame, table: str, grain: tuple[str, ...]) -> int:
    """Fail if the grain columns are null or not unique; return the row count."""
    total = df.count()
    nulls = df.filter(F.greatest(*[F.col(c).isNull() for c in grain]) if len(grain) > 1 else F.col(grain[0]).isNull()).count()
    distinct = df.select(*grain).distinct().count()
    if nulls or distinct != total:
        raise GoldCheckError(f"{table}: grain {grain} violated (rows={total}, distinct={distinct}, null keys={nulls})")
    return total


def assert_equal(table: str, check: str, gold_value: int, silver_value: int) -> None:
    """Fail if a Gold figure does not match its Silver source figure."""
    if gold_value != silver_value:
        raise GoldCheckError(f"{table}: {check} gold={gold_value} silver={silver_value}")


def run_gold(spark: SparkSession, lake: DataLake, run: RunContext) -> dict[str, int]:
    """Build, check and publish every Gold table."""
    s = SilverInputs.load(spark, lake)
    n_employees = s.employees.count()
    log.info("gold_as_of_date", extra={"as_of_date": s.as_of_date.isoformat()})

    progress = learning_summary.build_progress(s).cache()
    tables: list[tuple[str, tuple[str, ...], Callable[[], DataFrame], Callable[[DataFrame], None]]] = [
        (
            performance_history.TABLE,
            performance_history.GRAIN,
            lambda: performance_history.build(s),
            lambda df: assert_equal(performance_history.TABLE, "rows vs silver reviews", df.count(), s.reviews.count()),
        ),
        (
            skill_gaps.TABLE,
            skill_gaps.GRAIN,
            lambda: skill_gaps.build(s),
            lambda df: assert_equal(skill_gaps.TABLE, "rows vs silver assessments", df.count(), s.assessments.count()),
        ),
        (
            promotion_pipeline.TABLE,
            promotion_pipeline.GRAIN,
            lambda: promotion_pipeline.build(s),
            lambda df: assert_equal(promotion_pipeline.TABLE, "rows vs silver talent_review", df.count(), s.talent.count()),
        ),
        (
            learning_summary.PROGRESS_TABLE,
            learning_summary.PROGRESS_GRAIN,
            lambda: progress,
            lambda df: assert_equal(
                learning_summary.PROGRESS_TABLE,
                "rows vs distinct employee/course in silver events",
                df.count(),
                s.events.select("employee_id", "course_name").distinct().count(),
            ),
        ),
        (
            learning_summary.SUMMARY_TABLE,
            learning_summary.SUMMARY_GRAIN,
            lambda: learning_summary.build_summary(s, progress),
            lambda df: (
                assert_equal(learning_summary.SUMMARY_TABLE, "rows vs silver employees", df.count(), n_employees),
                assert_equal(
                    learning_summary.SUMMARY_TABLE,
                    "sum(quiz_attempts) vs silver quiz_attempted events",
                    int(df.agg(F.sum("quiz_attempts")).first()[0]),
                    s.events.filter(F.col("event_type") == "quiz_attempted").count(),
                ),
            ),
        ),
        (
            attrition_risk.TABLE,
            attrition_risk.GRAIN,
            lambda: attrition_risk.build(s),
            lambda df: assert_equal(attrition_risk.TABLE, "rows vs silver talent_review", df.count(), s.talent.count()),
        ),
        (
            team_outcomes.TABLE,
            team_outcomes.GRAIN,
            lambda: team_outcomes.build(s),
            lambda df: (
                assert_equal(team_outcomes.TABLE, "sum(headcount) vs silver employees", int(df.agg(F.sum("headcount")).first()[0]), n_employees),
                assert_equal(
                    team_outcomes.TABLE, "sum(project_count) vs silver projects", int(df.agg(F.sum("project_count")).first()[0]), s.projects.count()
                ),
            ),
        ),
        (
            skill_distribution.TABLE,
            skill_distribution.GRAIN,
            lambda: skill_distribution.build(s),
            lambda df: assert_equal(
                skill_distribution.TABLE, "sum(assessments) vs silver assessments", int(df.agg(F.sum("assessments")).first()[0]), s.assessments.count()
            ),
        ),
    ]

    written: dict[str, int] = {}
    frames: dict[str, object] = {}
    for name, grain, build, reconcile in tables:
        with run.step(f"gold.{name}", source=lake.uri("silver/"), target=lake.uri(f"{GOLD_PREFIX}/{name}"), critical=True) as m:
            df = build().cache()
            m.rows_read = m.rows_valid = assert_grain(df, name, grain)
            reconcile(df)
            m.notes.update(grain=list(grain), as_of_date=s.as_of_date.isoformat(), checks="passed")
            table = to_arrow(df)
            m.rows_written = lake.write_table(table, f"{GOLD_PREFIX}/{name}")
            frames[name] = table.to_pandas()
            written[name] = m.rows_written

    # Chart-ready JSON summaries for the insights dashboard (backend/insights-api).
    from gold.api_export import API_PREFIX, export_api  # local import: pandas-only module

    with run.step("gold.api_export", source=lake.uri(GOLD_PREFIX), target=lake.uri(API_PREFIX)) as m:
        m.rows_read = len(frames)
        m.rows_written = export_api(lake, frames, s.as_of_date, run)
    return written
