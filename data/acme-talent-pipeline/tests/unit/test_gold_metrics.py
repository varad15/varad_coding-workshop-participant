"""
Unit tests: Gold business metrics on a tiny hand-made dataset.

Every expected value below is computed by hand, so these tests pin the
business logic (critical metric calculations, target >= 95% coverage).
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

import pytest

from gold import attrition_risk, learning_summary, performance_history, promotion_pipeline, skill_distribution, skill_gaps, team_outcomes
from gold.inputs import SilverInputs


@pytest.fixture(scope="module")
def silver(spark) -> SilverInputs:
    """Three employees in two teams with known reviews, skills, events, projects."""
    c = spark.createDataFrame
    return SilverInputs(
        teams=c([(1, "Team A", "Sales"), (2, "Team B", "Design")], "team_id int, name string, department string"),
        competencies=c([(1, "Python", "Technical"), (2, "Negotiation", "Domain")], "competency_id int, name string, category string"),
        employees=c(
            [
                (1, "Ann", "Lee", "ann@x.io", 1, "IC2", date(2020, 9, 2)),
                (2, "Bob", "Ray", "bob@x.io", 1, "IC1", date(2024, 9, 2)),
                (3, "Cy", "Dun", "cy@x.io", 2, "Manager", date(2018, 9, 2)),
            ],
            "employee_id int, first_name string, last_name string, email string, team_id int, job_level string, hire_date date",
        ),
        projects=c(
            [(1, "P1", 1, "on-time", Decimal("80.00")), (2, "P2", 1, "delayed", Decimal("40.00"))],
            "project_id int, name string, team_id int, outcome_status string, outcome_score decimal(5,2)",
        ),
        reviews=c(
            [
                (1, 1, "2024-H2", 4, "Solid.", date(2025, 8, 1)),
                (2, 1, "2025-H1", 5, "Strong upward trend.", date(2026, 2, 1)),
                (3, 2, "2024-H2", 4, "Solid.", date(2025, 8, 1)),
                (4, 2, "2025-H1", 2, "Missed several key deliverables this cycle; needs support.", date(2026, 2, 1)),
                (5, 3, "2024-H2", 3, "Met expectations.", date(2025, 8, 1)),
                (6, 3, "2025-H1", 3, "Met expectations.", date(2026, 2, 1)),
            ],
            "review_id int, employee_id int, review_period string, rating int, manager_comment string, submitted_date date",
        ),
        assessments=c(
            [
                (1, 1, 1, 4, 4, date(2026, 1, 1)),  # gap 0
                (2, 2, 1, 1, 4, date(2026, 1, 1)),  # gap 3 -> critical
                (3, 2, 2, 2, 3, date(2026, 1, 1)),  # gap 1 -> minor
                (4, 3, 2, 5, 3, date(2026, 1, 1)),  # gap -2 -> none
            ],
            "assessment_id int, employee_id int, competency_id int, proficiency_level int, required_level int, assessed_date date",
        ),
        talent=c(
            [
                (1, "2025-H1", "High", "Ready Now", "Low"),
                (2, "2025-H1", "High", "Ready Now", "High"),
                (3, "2025-H1", "Low", "Not Ready", "Medium"),
            ],
            "employee_id int, review_cycle string, potential_rating string, promotion_readiness string, attrition_risk string",
        ),
        events=c(
            [
                ("EV1", 1, datetime(2026, 8, 1, 9), date(2026, 8, 1), "enrollment", "Advanced Python for Data Engineers", "t", None),
                ("EV2", 1, datetime(2026, 8, 2, 9), date(2026, 8, 2), "quiz_attempted", "Advanced Python for Data Engineers", "t", 60),
                ("EV3", 1, datetime(2026, 8, 3, 9), date(2026, 8, 3), "quiz_passed", "Advanced Python for Data Engineers", "t", 90),
                ("EV4", 1, datetime(2026, 8, 4, 9), date(2026, 8, 4), "course_completed", "Advanced Python for Data Engineers", "t", None),
                ("EV5", 1, datetime(2026, 8, 5, 9), date(2026, 8, 5), "certification_earned", "Advanced Python for Data Engineers", "t", None),
                ("EV6", 3, datetime(2026, 9, 1, 9), date(2026, 9, 1), "enrollment", "Negotiation Tactics", "t", None),
                ("EV7", 2, datetime(2025, 1, 1, 9), date(2025, 1, 1), "module_started", "Negotiation Tactics", "t", None),
            ],
            "event_id string, employee_id int, event_ts timestamp, event_date date, event_type string, course_name string, event_text string, score int",
        ),
        course_map=c(
            [("Advanced Python for Data Engineers", 1, "Python", "Technical"), ("Negotiation Tactics", 2, "Negotiation", "Domain")],
            "course_name string, competency_id int, competency_name string, competency_category string",
        ),
        as_of_date=date(2026, 9, 2),
    )


def _by(df, key="employee_id"):
    return {r[key]: r for r in df.collect()}


def test_q1_performance_history_rating_change(silver):
    rows = {(r["employee_id"], r["review_period"]): r for r in performance_history.build(silver).collect()}
    assert len(rows) == 6
    latest_bob = rows[(2, "2025-H1")]
    assert latest_bob["previous_rating"] == 4 and latest_bob["rating_change"] == -2 and latest_bob["is_latest_review"]
    assert rows[(2, "2024-H2")]["rating_change"] is None and not rows[(2, "2024-H2")]["is_latest_review"]
    assert latest_bob["rating_label"] == "Needs Improvement"
    assert latest_bob["full_name"] == "Bob Ray" and latest_bob["department"] == "Sales"


def test_q2_skill_gap_severity(silver):
    rows = _by(skill_gaps.build(silver), "assessment_id")
    assert [rows[i]["gap"] for i in (1, 2, 3, 4)] == [0, 3, 1, -2]
    assert [rows[i]["gap_severity"] for i in (1, 2, 3, 4)] == ["No gap", "Critical gap", "Minor gap", "No gap"]
    assert [rows[i]["is_critical_gap"] for i in (1, 2, 3, 4)] == [False, True, False, False]


def test_q3_promotion_candidates_and_nine_box(silver):
    rows = _by(promotion_pipeline.build(silver))
    # Ann: High potential, Ready Now, latest 5 -> candidate, Star.
    assert rows[1]["is_promotion_candidate"] and rows[1]["nine_box"] == "Star"
    # Bob: High potential, Ready Now but latest rating 2 -> not a candidate, Enigma.
    assert not rows[2]["is_promotion_candidate"] and rows[2]["nine_box"] == "Enigma"
    # Cy: Low potential, rating 3 -> Effective Contributor.
    assert rows[3]["nine_box"] == "Effective Contributor"
    assert rows[2]["critical_gap_count"] == 1 and rows[1]["certifications"] == 1
    assert rows[1]["tenure_years"] == 6.0


def test_q4_course_progress_and_summary(silver):
    progress = learning_summary.build_progress(silver)
    p = {(r["employee_id"], r["course_name"]): r for r in progress.collect()}
    ann = p[(1, "Advanced Python for Data Engineers")]
    assert ann["status"] == "Certified" and ann["is_completed"]
    assert (ann["quiz_attempts"], ann["quizzes_passed"], ann["best_quiz_score"]) == (1, 1, 90)
    assert ann["competency_name"] == "Python"
    assert p[(3, "Negotiation Tactics")]["status"] == "Enrolled"
    assert p[(2, "Negotiation Tactics")]["status"] == "In progress"

    summary = _by(learning_summary.build_summary(silver, progress))
    assert len(summary) == 3  # every employee present
    assert (summary[1]["courses_completed"], summary[1]["certifications"], summary[1]["avg_quiz_score"]) == (1, 1, 75.0)
    assert summary[1]["completed_courses"] == ["Advanced Python for Data Engineers"]
    assert summary[3]["courses_completed"] == 0 and summary[3]["completed_courses"] == []


def test_q5_attrition_scoring(silver):
    rows = _by(attrition_risk.build(silver))
    bob = rows[2]
    # declined (+2), latest <= 2 (+2), negative comment (+1), last activity 2025-01-01 > 180 days ago (+1) = 6
    assert (bob["signal_rating_declined"], bob["signal_low_rating"], bob["signal_negative_comment"], bob["signal_no_recent_learning"]) == (
        True, True, True, True,
    )
    assert bob["derived_risk_score"] == 6 and bob["derived_risk_level"] == "High" and bob["is_at_risk"]
    ann = rows[1]
    assert ann["derived_risk_score"] == 0 and ann["derived_risk_level"] == "Low" and ann["rating_trend"] == "Improving"
    cy = rows[3]  # stable 3, recent learning -> 0, but HR says Medium
    assert cy["derived_risk_level"] == "Low" and cy["hr_attrition_risk"] == "Medium" and not cy["risk_views_agree"]


def test_q6_team_outcomes(silver):
    rows = _by(team_outcomes.build(silver), "team_id")
    a = rows[1]
    assert a["headcount"] == 2 and a["avg_latest_rating"] == 3.5 and a["pct_high_performers"] == 50.0
    assert (a["project_count"], a["projects_on_time"], a["projects_delayed"]) == (2, 1, 1)
    assert a["avg_outcome_score"] == 60.0 and a["on_time_rate_pct"] == 50.0
    b = rows[2]
    assert not b["has_projects"] and b["project_count"] == 0 and b["on_time_rate_pct"] is None


def test_q7_skill_distribution(silver):
    rows = {(r["competency_id"], r["department"]): r for r in skill_distribution.build(silver).collect()}
    python_sales = rows[(1, "Sales")]
    assert python_sales["assessments"] == 2 and python_sales["avg_proficiency"] == 2.5
    assert python_sales["pct_meeting_requirement"] == 50.0 and python_sales["critical_gaps"] == 1
    assert (python_sales["level_1_count"], python_sales["level_4_count"]) == (1, 1)
    assert rows[(2, "Design")]["pct_meeting_requirement"] == 100.0
