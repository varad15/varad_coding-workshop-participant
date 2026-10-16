"""Unit tests: Pydantic contracts mirror schema.sql and the event rules."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from contracts.hr import CompetencyAssessment, Employee, PerformanceReview, Project, TalentReview, Team
from contracts.learning import LearningEvent
from contracts.validation import REASON_COL, VALID_COL, validate_dataframe, validate_record, validate_records

GOOD_EVENT = {
    "event_id": "EV0000013454",
    "employee_id": "373",
    "event_date": "2025-01-10T10:41:00",
    "event_type": "quiz_passed",
    "course_name": "Cloud Architecture Fundamentals",
    "text": "Cleared the assessment.",
    "score": "82",
}


def test_valid_event_passes_and_coerces_types():
    event = LearningEvent.model_validate(GOOD_EVENT)
    assert event.employee_id == 373 and event.score == 82


@pytest.mark.parametrize(
    "patch, expected_field",
    [
        ({"employee_id": None}, "employee_id"),  # 76 such rows in the dataset
        ({"event_date": "yesterday"}, "event_date"),  # junk dates
        ({"event_date": ""}, "event_date"),
        ({"event_date": "N/A"}, "event_date"),
        ({"event_type": "lesson_skipped"}, "event_type"),
        ({"event_id": "X1"}, "event_id"),
        ({"score": "101"}, "score"),
        ({"course_name": ""}, "course_name"),
    ],
)
def test_invalid_events_are_rejected_with_field_reason(patch, expected_field):
    reason = validate_record(LearningEvent, {**GOOD_EVENT, **patch})
    assert reason is not None and reason.startswith(expected_field)


def test_score_required_only_for_quiz_events():
    assert "score is required" in validate_record(LearningEvent, {**GOOD_EVENT, "score": None})
    assert "score must be null" in validate_record(LearningEvent, {**GOOD_EVENT, "event_type": "enrollment"})
    assert validate_record(LearningEvent, {**GOOD_EVENT, "event_type": "enrollment", "score": None}) is None


def test_extra_field_is_schema_drift():
    reason = validate_record(LearningEvent, {**GOOD_EVENT, "notes": "drifted"})
    assert "notes" in reason


VALID_HR = {
    Team: {"team_id": 1, "name": "Team A", "department": "Sales"},
    Employee: {
        "employee_id": 1, "first_name": "A", "last_name": "B", "email": "a.b@acme.example",
        "team_id": 1, "job_level": "IC1", "hire_date": date(2020, 1, 1),
    },
    Project: {"project_id": 1, "name": "P", "team_id": 1, "outcome_status": "on-time", "outcome_score": Decimal("56.87")},
    PerformanceReview: {
        "review_id": 1, "employee_id": 1, "review_period": "2025-H1", "rating": 4,
        "manager_comment": None, "submitted_date": date(2026, 2, 27),
    },
    CompetencyAssessment: {
        "assessment_id": 1, "employee_id": 1, "competency_id": 1,
        "proficiency_level": 3, "required_level": 4, "assessed_date": date(2026, 1, 1),
    },
    TalentReview: {
        "employee_id": 1, "review_cycle": "2025-H1", "potential_rating": "High",
        "promotion_readiness": "Ready Now", "attrition_risk": "Low",
    },
}


@pytest.mark.parametrize("model", list(VALID_HR))
def test_valid_hr_rows_pass(model):
    assert validate_record(model, VALID_HR[model]) is None


@pytest.mark.parametrize(
    "model, patch",
    [
        (PerformanceReview, {"rating": 6}),  # CHECK rating BETWEEN 1 AND 5
        (PerformanceReview, {"rating": 0}),
        (PerformanceReview, {"review_period": "2025-Q1"}),
        (CompetencyAssessment, {"proficiency_level": 0}),  # CHECK BETWEEN 1 AND 5
        (CompetencyAssessment, {"required_level": 9}),
        (Project, {"outcome_status": "done"}),  # CHECK IN (...)
        (Project, {"outcome_score": Decimal("120.00")}),
        (TalentReview, {"potential_rating": "Very High"}),
        (TalentReview, {"promotion_readiness": "Soon"}),
        (Employee, {"email": "not-an-email"}),
        (Employee, {"first_name": None}),  # NOT NULL
        (Team, {"department": ""}),
    ],
)
def test_hr_constraints_from_schema_sql(model, patch):
    assert validate_record(model, {**VALID_HR[model], **patch}) is not None


def test_validate_records_splits_and_keeps_reason():
    valid, rejected = validate_records(Team, [VALID_HR[Team], {**VALID_HR[Team], "team_id": -1}])
    assert len(valid) == 1 and len(rejected) == 1
    assert rejected[0][REASON_COL].startswith("team_id")


def test_validate_dataframe_runs_inside_spark(spark):
    rows = [VALID_HR[Team], {**VALID_HR[Team], "team_id": 2, "department": ""}]
    from pyspark.sql import functions as F

    df = spark.createDataFrame(rows).withColumn("_lineage", F.lit("x"))
    out = validate_dataframe(df, Team).collect()
    by_id = {r["team_id"]: r for r in out}
    assert by_id[1][VALID_COL] is True and by_id[1][REASON_COL] is None
    assert by_id[2][VALID_COL] is False and "department" in by_id[2][REASON_COL]
    assert by_id[2]["_lineage"] == "x"  # non-contract columns carried through
